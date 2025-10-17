package natsfix

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/quickfixgo/quickfix"
)

type natsInitiatorSession struct {
	*quickfix.Session

	conn       *nats.Conn
	inSubject  string
	outSubject string

	initiator *Initiator

	msgIn       chan quickfix.FixIn
	msgOut      chan []byte
	writeLoopWg sync.WaitGroup
	connected   atomic.Bool
}

type Initiator struct {
	sessionFactory quickfix.SessionFactory

	app       quickfix.Application
	settings  *quickfix.Settings
	globalLog quickfix.Log

	conn              *nats.Conn
	logger            *slog.Logger
	sessions          map[quickfix.SessionID]*natsInitiatorSession
	sessionGroup      sync.WaitGroup
	subscriptions     []*nats.Subscription
	subscriptionGroup sync.WaitGroup

	ctx    context.Context
	cancel context.CancelFunc
}

func NewInitiator(
	ctx context.Context,
	app quickfix.Application,
	storeFactory quickfix.MessageStoreFactory,
	settings *quickfix.Settings,
	logFactory quickfix.LogFactory,
	logger *slog.Logger,
) (*Initiator, error) {
	ctx, cancel := context.WithCancel(ctx)

	i := &Initiator{
		sessionFactory: quickfix.SessionFactory{BuildInitiators: true},
		app:            app,
		settings:       settings,
		logger:         logger,
		sessions:       make(map[quickfix.SessionID]*natsInitiatorSession),
		ctx:            ctx,
		cancel:         cancel,
	}

	var err error
	if i.globalLog, err = logFactory.Create(); err != nil {
		cancel()
		return nil, fmt.Errorf("failed to create global log: %w", err)
	}

	seenSubjects := make(map[string]quickfix.SessionID)

	for sessionID, sessionSettings := range settings.SessionSettings() {
		session, err := i.createSession(sessionID, storeFactory, sessionSettings, logFactory, app)
		if err != nil {
			cancel()
			return nil, fmt.Errorf("failed to create session %s: %w", sessionID.String(), err)
		}

		if existingSessionID, exists := seenSubjects[session.inSubject]; exists {
			cancel()
			return nil, fmt.Errorf("duplicate subject %s: inbound for session %s conflicts with session %s", session.inSubject, sessionID.String(), existingSessionID.String())
		}
		seenSubjects[session.inSubject] = sessionID

		if existingSessionID, exists := seenSubjects[session.outSubject]; exists {
			cancel()
			return nil, fmt.Errorf("duplicate subject %s: outbound for session %s conflicts with session %s", session.outSubject, sessionID.String(), existingSessionID.String())
		}
		seenSubjects[session.outSubject] = sessionID

		i.sessions[sessionID] = session
		logger.InfoContext(ctx, "Pre-initialized session", "sessionID", sessionID.String(), "inSubject", session.inSubject, "outSubject", session.outSubject)
	}

	return i, nil
}

func (i *Initiator) Start() error {
	natsURL, err := i.settings.GlobalSettings().Setting("NATSUrl")
	if err != nil {
		return fmt.Errorf("NATSUrl not configured in global settings: %w", err)
	}

	var opts []nats.Option
	if i.settings.GlobalSettings().HasSetting("NATSCredsFile") {
		credsFile, _ := i.settings.GlobalSettings().Setting("NATSCredsFile")
		opts = append(opts, nats.UserCredentials(credsFile))
	}

	conn, err := nats.Connect(natsURL, opts...)
	if err != nil {
		return fmt.Errorf("failed to connect to NATS at %s: %w", natsURL, err)
	}
	i.conn = conn

	for sessionID, session := range i.sessions {
		session.conn = conn

		i.sessionGroup.Add(1)
		go func() {
			defer i.sessionGroup.Done()
			session.Run()
		}()

		is := i.sessions[sessionID]
		i.logger.InfoContext(i.ctx, "Subscribing to initiator session", "sessionID", sessionID.String(), "subject", is.inSubject)
		sub, err := i.conn.Subscribe(session.inSubject, is.handleMessage)
		if err != nil {
			return fmt.Errorf("failed to subscribe to %s: %w", session.inSubject, err)
		}

		i.subscriptionGroup.Add(1)
		sub.SetClosedHandler(func(subject string) {
			i.logger.InfoContext(context.Background(), "Subscription closed", "subject", subject)
			i.subscriptionGroup.Done()
		})
		i.subscriptions = append(i.subscriptions, sub)

		i.sessionGroup.Add(1)
		go func() {
			defer i.sessionGroup.Done()
			is.connectLoop()
		}()
	}

	return nil
}

func (i *Initiator) Stop() {
	i.logger.InfoContext(context.Background(), "Stopping NATS initiator")

	// Unsubscribe from all subscriptions and wait for handlers to complete
	for _, sub := range i.subscriptions {
		if err := sub.Unsubscribe(); err != nil {
			i.logger.ErrorContext(context.Background(), "Failed to unsubscribe", "error", err)
		}
	}
	i.subscriptionGroup.Wait()
	i.logger.InfoContext(context.Background(), "All subscriptions closed")

	// Cancel context and close sessions
	i.cancel()
	for _, s := range i.sessions {
		if s != nil {
			s.Close()
		}
	}
	i.sessionGroup.Wait()

	for sid := range i.sessions {
		quickfix.UnregisterSession(sid)
		delete(i.sessions, sid)
	}

	if i.conn != nil {
		i.conn.Close()
	}
	i.logger.InfoContext(context.Background(), "NATS initiator stopped")
}

func (i *Initiator) createSession(sessionID quickfix.SessionID, storeFactory quickfix.MessageStoreFactory, sessionSettings *quickfix.SessionSettings, logFactory quickfix.LogFactory, app quickfix.Application) (*natsInitiatorSession, error) {
	i.logger.InfoContext(i.ctx, "Creating session", "sessionID", sessionID.String())

	session, err := i.sessionFactory.CreateSession(sessionID, storeFactory, sessionSettings, logFactory, app)
	if err != nil {
		return nil, fmt.Errorf("failed to create quickfix session: %w", err)
	}

	inTemplate, err := sessionSettings.Setting("NATSInboundSubject")
	if err != nil {
		return nil, err
	}
	inSubject := ExpandSubjectTemplate(inTemplate, sessionID)

	outTemplate, err := sessionSettings.Setting("NATSOutboundSubject")
	if err != nil {
		return nil, err
	}
	outSubject := ExpandSubjectTemplate(outTemplate, sessionID)

	is := &natsInitiatorSession{
		Session:    session,
		inSubject:  inSubject,
		outSubject: outSubject,
		initiator:  i,
	}

	return is, nil
}

func (is *natsInitiatorSession) connectLoop() {
	defer func() {
		if err := recover(); err != nil {
			is.initiator.globalLog.OnEventf("Connect loop panic: %s", debug.Stack())
		}
	}()

	for {
		if !is.waitForInSessionTime() {
			return
		}

		is.msgIn = make(chan quickfix.FixIn)
		is.msgOut = make(chan []byte)
		if err := is.Session.Connect(is.msgIn, is.msgOut); err != nil {
			is.initiator.logger.ErrorContext(is.initiator.ctx, "Failed to connect session", "error", err)
			return
		}
		is.writeLoopWg.Add(1)
		go func() {
			defer is.writeLoopWg.Done()
			is.writeLoop()
		}()
		is.connected.Store(true)

		select {
		case <-is.initiator.ctx.Done():
			return
		case <-is.Session.DisconnectedC():
		}
		is.connected.Store(false)
		close(is.msgIn)
		is.writeLoopWg.Wait()
		is.Session.OnEventf("Reconnecting in %v", is.Session.ReconnectInterval)
		if !is.waitForReconnectInterval(is.Session.ReconnectInterval) {
			return
		}
	}
}

func (is *natsInitiatorSession) handleMessage(natsMsg *nats.Msg) {
	defer func() {
		if err := recover(); err != nil {
			is.initiator.globalLog.OnEventf("Message handling panic: %s", debug.Stack())
		}
	}()

	if !is.connected.Load() {
		is.initiator.logger.WarnContext(is.initiator.ctx, "Received message but session is not connected", "subject", natsMsg.Subject)
		return
	}

	select {
	case <-is.Session.DisconnectedC():
		is.initiator.logger.WarnContext(is.initiator.ctx, "Received message but session is disconnected", "subject", natsMsg.Subject)
		return
	case is.msgIn <- quickfix.NewFixIn(bytes.NewBuffer(natsMsg.Data), time.Now()):
		is.initiator.logger.InfoContext(is.initiator.ctx, "Received message", "subject", natsMsg.Subject, "length", len(natsMsg.Data))
	}
}

func (is *natsInitiatorSession) waitForInSessionTime() bool {
	inSessionTime := make(chan interface{})
	go func() {
		is.Session.WaitForInSessionTime()
		close(inSessionTime)
	}()

	select {
	case <-inSessionTime:
	case <-is.initiator.ctx.Done():
		return false
	}

	return true
}

func (is *natsInitiatorSession) waitForReconnectInterval(reconnectInterval time.Duration) bool {
	select {
	case <-time.After(reconnectInterval):
	case <-is.initiator.ctx.Done():
		return false
	}

	return true
}

func (is *natsInitiatorSession) writeLoop() {
	for {
		msg, ok := <-is.msgOut
		if !ok {
			return
		}
		is.initiator.logger.InfoContext(is.initiator.ctx, "Sending message", "subject", is.outSubject, "length", len(msg))
		if err := is.conn.Publish(is.outSubject, msg); err != nil {
			is.initiator.globalLog.OnEvent(err.Error())
		}
	}
}

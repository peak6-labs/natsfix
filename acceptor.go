package natsfix

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"runtime/debug"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/quickfixgo/quickfix"
)

type natsSession struct {
	*quickfix.Session

	msgIn  chan quickfix.FixIn
	msgOut chan []byte

	conn       *nats.Conn
	inSubject  string
	outSubject string

	acceptor *Acceptor

	connected bool
	doConnect sync.Once
}

type Acceptor struct {
	sessionFactory quickfix.SessionFactory

	app       quickfix.Application
	settings  *quickfix.Settings
	globalLog quickfix.Log

	conn              *nats.Conn
	logger            *slog.Logger
	sessions          map[quickfix.SessionID]*natsSession
	sessionGroup      sync.WaitGroup
	subscriptions     []*nats.Subscription
	subscriptionGroup sync.WaitGroup

	ctx    context.Context
	cancel context.CancelFunc
}

func NewAcceptor(
	ctx context.Context,
	app quickfix.Application,
	storeFactory quickfix.MessageStoreFactory,
	settings *quickfix.Settings,
	logFactory quickfix.LogFactory,
	logger *slog.Logger,
) (*Acceptor, error) {
	ctx, cancel := context.WithCancel(ctx)

	a := &Acceptor{
		sessionFactory: quickfix.SessionFactory{BuildInitiators: false},
		app:            app,
		settings:       settings,
		logger:         logger,
		sessions:       make(map[quickfix.SessionID]*natsSession),
		ctx:            ctx,
		cancel:         cancel,
	}

	var err error
	if a.globalLog, err = logFactory.Create(); err != nil {
		cancel()
		return nil, fmt.Errorf("failed to create global log: %w", err)
	}

	seenSubjects := make(map[string]quickfix.SessionID)

	for sessionID, sessionSettings := range settings.SessionSettings() {
		session, err := a.createSession(sessionID, storeFactory, sessionSettings, logFactory, app)
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

		a.sessions[sessionID] = session
		logger.InfoContext(ctx, "Pre-initialized session", "sessionID", sessionID.String(), "inSubject", session.inSubject, "outSubject", session.outSubject)
	}

	return a, nil
}

func (a *Acceptor) Start() error {
	natsURL, err := a.settings.GlobalSettings().Setting("NATSUrl")
	if err != nil {
		return fmt.Errorf("NATSUrl not configured in global settings: %w", err)
	}

	var opts []nats.Option
	if a.settings.GlobalSettings().HasSetting("NATSCredsFile") {
		credsFile, _ := a.settings.GlobalSettings().Setting("NATSCredsFile")
		opts = append(opts, nats.UserCredentials(credsFile))
	}

	conn, err := nats.Connect(natsURL, opts...)
	if err != nil {
		return fmt.Errorf("failed to connect to NATS at %s: %w", natsURL, err)
	}
	a.conn = conn

	for sessionID, session := range a.sessions {
		session.conn = conn

		a.sessionGroup.Add(1)
		go func() {
			defer a.sessionGroup.Done()
			session.Run()
		}()

		ns := a.sessions[sessionID]
		a.logger.InfoContext(a.ctx, "Subscribing to configured session", "sessionID", sessionID.String(), "subject", ns.inSubject)
		sub, err := a.conn.Subscribe(session.inSubject, ns.handleMessage)
		if err != nil {
			return fmt.Errorf("failed to subscribe to %s: %w", session.inSubject, err)
		}

		a.subscriptionGroup.Add(1)
		sub.SetClosedHandler(func(subject string) {
			a.logger.InfoContext(context.Background(), "Subscription closed", "subject", subject)
			a.subscriptionGroup.Done()
		})
		a.subscriptions = append(a.subscriptions, sub)
	}

	return nil
}

func (a *Acceptor) Stop() {
	a.logger.InfoContext(context.Background(), "Stopping NATS acceptor")

	// Unsubscribe from all subscriptions and wait for handlers to complete
	// TODO: Do we want to make "Draining" configurable?
	for _, sub := range a.subscriptions {
		if err := sub.Drain(); err != nil {
			a.logger.ErrorContext(context.Background(), "Failed to unsubscribe", "error", err)
		}
	}
	a.subscriptionGroup.Wait()
	a.logger.InfoContext(context.Background(), "All subscriptions closed")

	// Cancel context and close sessions
	a.cancel()
	for _, s := range a.sessions {
		if s != nil {
			s.Close()
		}
	}
	a.sessionGroup.Wait()

	// Drain NATS connection to ensure all messages are sent
	if a.conn != nil {
		if err := a.conn.Drain(); err != nil {
			a.logger.ErrorContext(context.Background(), "Failed to drain NATS connection", "error", err)
		}
	}
	a.logger.InfoContext(context.Background(), "NATS acceptor stopped")
}

func (a *Acceptor) createSession(sessionID quickfix.SessionID, storeFactory quickfix.MessageStoreFactory, sessionSettings *quickfix.SessionSettings, logFactory quickfix.LogFactory, app quickfix.Application) (*natsSession, error) {
	a.logger.InfoContext(a.ctx, "Creating session", "sessionID", sessionID.String())

	session, err := a.sessionFactory.CreateSession(sessionID, storeFactory, sessionSettings, logFactory, app)
	if err != nil {
		a.logger.ErrorContext(a.ctx, "Failed to create session", "sessionID", sessionID.String(), "error", err)
		return nil, fmt.Errorf("failed to create session: %w", err)
	}

	inTemplate, err := sessionSettings.Setting("NATSInboundSubject")
	if err != nil {
		return nil, fmt.Errorf("NATSInboundSubject is required but not configured: %w", err)
	}
	inSubject := ExpandSubjectTemplate(inTemplate, sessionID)

	outTemplate, err := sessionSettings.Setting("NATSOutboundSubject")
	if err != nil {
		return nil, fmt.Errorf("NATSOutboundSubject is required but not configured: %w", err)
	}
	outSubject := ExpandSubjectTemplate(outTemplate, sessionID)

	ns := &natsSession{
		Session:    session,
		msgIn:      make(chan quickfix.FixIn),
		msgOut:     make(chan []byte),
		inSubject:  inSubject,
		outSubject: outSubject,
		acceptor:   a,
	}

	return ns, nil
}

func (ns *natsSession) handleMessage(natsMsg *nats.Msg) {
	defer func() {
		if err := recover(); err != nil {
			ns.acceptor.globalLog.OnEventf("Message handling panic: %s", debug.Stack())
		}
	}()

	ns.doConnect.Do(func() {
		if err := ns.Session.Connect(ns.msgIn, ns.msgOut); err != nil {
			ns.acceptor.logger.ErrorContext(ns.acceptor.ctx, "Failed to connect session", "error", err)
			return
		}
		ns.connected = true

		ns.acceptor.sessionGroup.Add(1)
		go func() {
			defer ns.acceptor.sessionGroup.Done()
			ns.writeLoop()
		}()
	})
	if !ns.connected {
		ns.acceptor.logger.WarnContext(ns.acceptor.ctx, "Received message for disconnected session", "subject", natsMsg.Subject)
		return
	}
	ns.msgIn <- quickfix.NewFixIn(bytes.NewBuffer(natsMsg.Data), time.Now())
}

func (ns *natsSession) writeLoop() {
	for {
		select {
		case <-ns.acceptor.ctx.Done():
			return
		case msg, ok := <-ns.msgOut:
			if !ok {
				return
			}
			if err := ns.conn.Publish(ns.outSubject, msg); err != nil {
				ns.acceptor.globalLog.OnEvent(err.Error())
			}
		}
	}
}

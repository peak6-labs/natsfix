package natsfix

import (
	"bytes"
	"fmt"
	"log/slog"
	"runtime/debug"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/quickfixgo/quickfix"
)

type natsAcceptorSession struct {
	*quickfix.Session

	conn       *nats.Conn
	inSubject  string
	outSubject string

	acceptor *Acceptor

	msgIn       chan quickfix.FixIn
	msgOut      chan []byte
	writeLoopWg sync.WaitGroup
	connected   bool
}

type Acceptor struct {
	sessionFactory quickfix.SessionFactory

	app       quickfix.Application
	settings  *quickfix.Settings
	globalLog quickfix.Log

	conn              *nats.Conn
	logger            *slog.Logger
	sessions          map[quickfix.SessionID]*natsAcceptorSession
	sessionGroup      sync.WaitGroup
	subscriptions     []*nats.Subscription
	subscriptionGroup sync.WaitGroup
}

func NewAcceptor(
	app quickfix.Application,
	storeFactory quickfix.MessageStoreFactory,
	settings *quickfix.Settings,
	logFactory quickfix.LogFactory,
	logger *slog.Logger,
) (*Acceptor, error) {
	a := &Acceptor{
		sessionFactory: quickfix.SessionFactory{BuildInitiators: false},
		app:            app,
		settings:       settings,
		logger:         logger,
		sessions:       make(map[quickfix.SessionID]*natsAcceptorSession),
	}

	var err error
	if a.globalLog, err = logFactory.Create(); err != nil {
		return nil, fmt.Errorf("failed to create global log: %w", err)
	}

	seenSubjects := make(map[string]quickfix.SessionID)

	for sessionID, sessionSettings := range settings.SessionSettings() {
		session, err := a.createSession(sessionID, storeFactory, sessionSettings, logFactory, app)
		if err != nil {
			return nil, fmt.Errorf("failed to create session %s: %w", sessionID.String(), err)
		}

		if existingSessionID, exists := seenSubjects[session.inSubject]; exists {
			return nil, fmt.Errorf("duplicate subject %s: inbound for session %s conflicts with session %s", session.inSubject, sessionID.String(), existingSessionID.String())
		}
		seenSubjects[session.inSubject] = sessionID

		if existingSessionID, exists := seenSubjects[session.outSubject]; exists {
			return nil, fmt.Errorf("duplicate subject %s: outbound for session %s conflicts with session %s", session.outSubject, sessionID.String(), existingSessionID.String())
		}
		seenSubjects[session.outSubject] = sessionID

		a.sessions[sessionID] = session
		logger.Info("Pre-initialized session", "sessionID", sessionID.String(), "inSubject", session.inSubject, "outSubject", session.outSubject)
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
		a.logger.Info("Subscribing to configured session", "sessionID", sessionID.String(), "subject", ns.inSubject)
		sub, err := a.conn.Subscribe(session.inSubject, ns.handleMessage)
		if err != nil {
			return fmt.Errorf("failed to subscribe to %s: %w", session.inSubject, err)
		}

		a.subscriptionGroup.Add(1)
		sub.SetClosedHandler(func(subject string) {
			a.logger.Info("Subscription closed", "subject", subject)
			a.subscriptionGroup.Done()
		})
		a.subscriptions = append(a.subscriptions, sub)
	}

	return nil
}

func (a *Acceptor) Stop() {
	a.logger.Info("Stopping NATS acceptor")

	// Cancel context and close sessions
	for _, s := range a.sessions {
		if s != nil {
			s.Close()
		}
	}
	a.sessionGroup.Wait()
	a.logger.Info("All sessions stopped")

	for sid := range a.sessions {
		quickfix.UnregisterSession(sid)
		delete(a.sessions, sid)
	}

	// Unsubscribe from all subscriptions and wait for handlers to complete
	for _, sub := range a.subscriptions {
		if err := sub.Unsubscribe(); err != nil {
			a.logger.Error("Failed to unsubscribe", "error", err)
		}
	}
	a.subscriptionGroup.Wait()
	a.logger.Info("All subscriptions closed")

	if a.conn != nil {
		a.conn.Close()
	}
	a.logger.Info("NATS acceptor stopped")
}

func (a *Acceptor) createSession(sessionID quickfix.SessionID, storeFactory quickfix.MessageStoreFactory, sessionSettings *quickfix.SessionSettings, logFactory quickfix.LogFactory, app quickfix.Application) (*natsAcceptorSession, error) {
	a.logger.Info("Creating session", "sessionID", sessionID.String())

	session, err := a.sessionFactory.CreateSession(sessionID, storeFactory, sessionSettings, logFactory, app)
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

	ns := &natsAcceptorSession{
		Session:    session,
		inSubject:  inSubject,
		outSubject: outSubject,
		acceptor:   a,
		msgIn:      make(chan quickfix.FixIn),
	}

	return ns, nil
}

func (ns *natsAcceptorSession) handleMessage(natsMsg *nats.Msg) {
	defer func() {
		if err := recover(); err != nil {
			ns.acceptor.globalLog.OnEventf("Message handling panic: %s", debug.Stack())
		}
	}()

	// Unlike QuickFIX over TCP, we can't just leave the session disconnected because the client
	// has no way to reconnect. Instead, we'll watch for the next message from the subject and attempt to reconnect.
	for {
		if !ns.connected {
			ns.msgOut = make(chan []byte)
			if err := ns.Session.Connect(ns.msgIn, ns.msgOut); err != nil {
				ns.acceptor.logger.Error("Failed to connect session", "error", err)
				return
			}
			ns.writeLoopWg.Add(1)
			go func() {
				defer ns.writeLoopWg.Done()
				ns.writeLoop()
			}()
			ns.connected = true
		}

		// Check the state of session before attempting to send message
		// If the session is disconnected, loop to reconnect
		// If we successfully send the message, return out of the handler to wait for the next message
		select {
		case <-ns.Session.DisconnectedC():
			ns.connected = false
			ns.writeLoopWg.Wait()
			continue
		case ns.msgIn <- quickfix.NewFixIn(bytes.NewBuffer(natsMsg.Data), time.Now()):
			ns.acceptor.logger.Info("Received message", "subject", natsMsg.Subject, "length", len(natsMsg.Data))
			return
		}
	}
}

func (ns *natsAcceptorSession) writeLoop() {
	for {
		msg, ok := <-ns.msgOut
		if !ok {
			return
		}
		if err := ns.conn.Publish(ns.outSubject, msg); err != nil {
			ns.acceptor.globalLog.OnEvent(err.Error())
		}
	}
}

package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/nats-io/nats.go"
	"github.com/peak6-labs/natsfix"
	natsfixconfig "github.com/peak6-labs/natsfix/config"
	"github.com/quickfixgo/quickfix"
	"github.com/quickfixgo/quickfix/config"
	"github.com/quickfixgo/quickfix/log/screen"
	"github.com/spf13/pflag"
)

type Application struct {
	log *slog.Logger
}

func (a Application) OnCreate(sessionID quickfix.SessionID) {
	a.log.Info("Session created", "sessionID", sessionID.String())
}

func (a Application) OnLogon(sessionID quickfix.SessionID) {
	a.log.Info("Session logged on", "sessionID", sessionID.String())
}

func (a Application) OnLogout(sessionID quickfix.SessionID) {
	a.log.Info("Session logged out", "sessionID", sessionID.String())
}

func (a Application) ToAdmin(message *quickfix.Message, sessionID quickfix.SessionID) {
	msgType, _ := message.Header.GetString(35)
	a.log.Debug("ToAdmin", "sessionID", sessionID.String(), "msgType", msgType)
}

func (a Application) ToApp(message *quickfix.Message, sessionID quickfix.SessionID) error {
	msgType, _ := message.Header.GetString(35)
	a.log.Info("ToApp", "sessionID", sessionID.String(), "msgType", msgType)
	return nil
}

func (a Application) FromAdmin(message *quickfix.Message, sessionID quickfix.SessionID) quickfix.MessageRejectError {
	msgType, _ := message.Header.GetString(35)
	a.log.Debug("FromAdmin", "sessionID", sessionID.String(), "msgType", msgType)
	return nil
}

func (a Application) FromApp(message *quickfix.Message, sessionID quickfix.SessionID) quickfix.MessageRejectError {
	msgType, _ := message.Header.GetString(35)
	a.log.Info("FromApp", "sessionID", sessionID.String(), "msgType", msgType)
	return nil
}

func main() {
	natsURL := pflag.String("nats", nats.DefaultURL, "NATS server URL")
	natsCredsFile := pflag.String("creds", "", "NATS credentials file (optional)")
	senderCompID := pflag.String("sender", "GATEWAY", "SenderCompID")
	targetCompID := pflag.String("target", "CLIENT1", "TargetCompID")
	pflag.Parse()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))
	slog.SetDefault(logger)

	settings := quickfix.NewSettings()
	globalSettings := settings.GlobalSettings()
	globalSettings.Set(config.BeginString, "FIXT.1.1")
	globalSettings.Set(config.DefaultApplVerID, "9") // FIX.5.0SP2
	globalSettings.Set(natsfixconfig.NATSUrl, *natsURL)
	if *natsCredsFile != "" {
		globalSettings.Set(natsfixconfig.NATSCredsFile, *natsCredsFile)
	}

	sessionSettings := quickfix.NewSessionSettings()
	sessionSettings.Set(config.SenderCompID, *senderCompID)
	sessionSettings.Set(config.TargetCompID, *targetCompID)
	sessionSettings.Set(config.CheckLatency, "N")
	sessionSettings.Set(config.HeartBtInt, "30")
	sessionSettings.Set(config.ResetOnLogon, "Y")
	sessionSettings.Set(config.ResetOnLogout, "Y")
	sessionSettings.Set(config.ResetOnDisconnect, "Y")
	sessionSettings.Set(natsfixconfig.NATSInboundSubject, "fix.{BeginString}.{TargetCompID}.{SenderCompID}")
	sessionSettings.Set(natsfixconfig.NATSOutboundSubject, "fix.{BeginString}.{SenderCompID}.{TargetCompID}")
	settings.AddSession(sessionSettings)

	ctx := context.Background()
	app := Application{log: logger.With("component", "Application")}
	storeFactory := quickfix.NewMemoryStoreFactory()
	logFactory := screen.NewLogFactory()

	acceptor, err := natsfix.NewAcceptor(
		ctx,
		app,
		storeFactory,
		settings,
		logFactory,
		logger.With("component", "Acceptor"),
	)
	if err != nil {
		logger.Error("Failed to create acceptor", "error", err)
		os.Exit(1)
	}

	if err := acceptor.Start(); err != nil {
		logger.Error("Failed to start acceptor", "error", err)
		os.Exit(1)
	}

	logger.Info("Acceptor started", "natsURL", *natsURL, "sender", *senderCompID, "target", *targetCompID)

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	<-sigChan

	logger.Info("Shutting down acceptor")
	acceptor.Stop()
}

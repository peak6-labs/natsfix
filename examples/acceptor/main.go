package main

import (
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

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

	if msgType == "D" {
		if err := a.handleNewOrderSingle(message, sessionID); err != nil {
			a.log.Error("Failed to handle NewOrderSingle", "error", err)
		}
	}

	return nil
}

func (a Application) handleNewOrderSingle(nos *quickfix.Message, sessionID quickfix.SessionID) error {
	clOrdID, err := nos.Body.GetString(11)
	if err != nil {
		return fmt.Errorf("missing ClOrdID: %w", err)
	}

	symbol, err := nos.Body.GetString(55)
	if err != nil {
		return fmt.Errorf("missing Symbol: %w", err)
	}

	side, err := nos.Body.GetString(54)
	if err != nil {
		return fmt.Errorf("missing Side: %w", err)
	}

	orderQty, err := nos.Body.GetString(38)
	if err != nil {
		return fmt.Errorf("missing OrderQty: %w", err)
	}

	price := ""
	if p, err := nos.Body.GetString(44); err == nil {
		price = p
	}

	execReport := quickfix.NewMessage()
	execReport.Header.SetField(quickfix.Tag(35), quickfix.FIXString("8"))

	execReport.Body.SetField(quickfix.Tag(11), quickfix.FIXString(clOrdID))
	execReport.Body.SetField(quickfix.Tag(37), quickfix.FIXString(fmt.Sprintf("ORD-%d", time.Now().UnixNano())))
	execReport.Body.SetField(quickfix.Tag(17), quickfix.FIXString(fmt.Sprintf("EXEC-%d", time.Now().UnixNano())))
	execReport.Body.SetField(quickfix.Tag(150), quickfix.FIXString("0"))
	execReport.Body.SetField(quickfix.Tag(39), quickfix.FIXString("0"))
	execReport.Body.SetField(quickfix.Tag(55), quickfix.FIXString(symbol))
	execReport.Body.SetField(quickfix.Tag(54), quickfix.FIXString(side))
	execReport.Body.SetField(quickfix.Tag(38), quickfix.FIXString(orderQty))
	execReport.Body.SetField(quickfix.Tag(32), quickfix.FIXString("0"))
	execReport.Body.SetField(quickfix.Tag(151), quickfix.FIXString(orderQty))
	execReport.Body.SetField(quickfix.Tag(14), quickfix.FIXString("0"))
	if price != "" {
		execReport.Body.SetField(quickfix.Tag(44), quickfix.FIXString(price))
	}
	execReport.Body.SetField(quickfix.Tag(60), quickfix.FIXString(time.Now().UTC().Format("20060102-15:04:05.000")))

	a.log.Info("Sending ExecutionReport", "clOrdID", clOrdID, "symbol", symbol)
	return quickfix.SendToTarget(execReport, sessionID)
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

	app := Application{log: logger.With("component", "Application")}
	storeFactory := quickfix.NewMemoryStoreFactory()
	logFactory := screen.NewLogFactory()

	acceptor, err := natsfix.NewAcceptor(
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

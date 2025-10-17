package main

import (
	"bufio"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
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
	return nil
}

func main() {
	natsURL := pflag.String("nats", nats.DefaultURL, "NATS server URL")
	natsCredsFile := pflag.String("creds", "", "NATS credentials file (optional)")
	senderCompID := pflag.String("sender", "CLIENT1", "SenderCompID")
	targetCompID := pflag.String("target", "GATEWAY", "TargetCompID")
	pflag.Parse()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))
	slog.SetDefault(logger)

	settings := quickfix.NewSettings()
	globalSettings := settings.GlobalSettings()
	globalSettings.Set(config.BeginString, "FIXT.1.1")
	globalSettings.Set(config.DefaultApplVerID, "9")
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
	sessionSettings.Set(config.ReconnectInterval, "5")
	sessionSettings.Set(natsfixconfig.NATSInboundSubject, "fix.{BeginString}.{TargetCompID}.{SenderCompID}")
	sessionSettings.Set(natsfixconfig.NATSOutboundSubject, "fix.{BeginString}.{SenderCompID}.{TargetCompID}")
	settings.AddSession(sessionSettings)

	app := Application{log: logger.With("component", "Application")}
	storeFactory := quickfix.NewMemoryStoreFactory()
	logFactory := screen.NewLogFactory()

	initiator, err := natsfix.NewInitiator(
		app,
		storeFactory,
		settings,
		logFactory,
		logger.With("component", "Initiator"),
	)
	if err != nil {
		logger.Error("Failed to create initiator", "error", err)
		os.Exit(1)
	}

	if err := initiator.Start(); err != nil {
		logger.Error("Failed to start initiator", "error", err)
		os.Exit(1)
	}

	logger.Info("Initiator started", "natsURL", *natsURL, "sender", *senderCompID, "target", *targetCompID)
	logger.Info("Commands: /exit - quit, /NewOrderSingle - send new order")

	sessionID := quickfix.SessionID{
		BeginString:  "FIXT.1.1",
		SenderCompID: *senderCompID,
		TargetCompID: *targetCompID,
	}

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	go func() {
		scanner := bufio.NewScanner(os.Stdin)
		for scanner.Scan() {
			line := strings.TrimSpace(scanner.Text())
			if line == "" {
				continue
			}

			switch line {
			case "/exit":
				logger.Info("Exit command received")
				sigChan <- syscall.SIGTERM
				return
			case "/NewOrderSingle":
				msg := createNewOrderSingle()
				if err := quickfix.SendToTarget(msg, sessionID); err != nil {
					logger.Error("Failed to send NewOrderSingle", "error", err)
				} else {
					logger.Info("NewOrderSingle sent")
				}
			default:
				logger.Warn("Unknown command", "command", line)
			}
		}
	}()

	<-sigChan

	logger.Info("Shutting down initiator")
	initiator.Stop()
}

func createNewOrderSingle() *quickfix.Message {
	msg := quickfix.NewMessage()
	msg.Header.SetField(quickfix.Tag(35), quickfix.FIXString("D"))
	msg.Body.SetField(quickfix.Tag(11), quickfix.FIXString(fmt.Sprintf("ORDER-%d", time.Now().Unix())))
	msg.Body.SetField(quickfix.Tag(21), quickfix.FIXString("1"))
	msg.Body.SetField(quickfix.Tag(55), quickfix.FIXString("AAPL"))
	msg.Body.SetField(quickfix.Tag(54), quickfix.FIXString("1"))
	msg.Body.SetField(quickfix.Tag(40), quickfix.FIXString("2"))
	msg.Body.SetField(quickfix.Tag(38), quickfix.FIXInt(100))
	msg.Body.SetField(quickfix.Tag(44), quickfix.FIXString("150.50"))
	msg.Body.SetField(quickfix.Tag(60), quickfix.FIXUTCTimestamp{Time: time.Now(), Precision: quickfix.Millis})
	msg.Body.SetField(quickfix.Tag(100), quickfix.FIXString("NYSE"))
	return msg
}

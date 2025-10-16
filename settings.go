package natsfix

import (
	"fmt"

	"github.com/quickfixgo/quickfix"
)

const (
	BeginStringFIXT11 = "FIXT.1.1"
	DefaultApplVerID  = "FIX.5.0SP2"
)

func ValidateFIXTSettings(settings *quickfix.SessionSettings) error {
	var beginString string
	var err error

	if beginString, err = settings.Setting("BeginString"); err != nil {
		return fmt.Errorf("BeginString not configured: %w", err)
	}

	if beginString != BeginStringFIXT11 {
		return fmt.Errorf("BeginString must be %s, got %s", BeginStringFIXT11, beginString)
	}

	if !settings.HasSetting("DefaultApplVerID") {
		return fmt.Errorf("DefaultApplVerID is required for FIXT.1.1")
	}

	return nil
}

func CreateFIXTSettings(senderCompID, targetCompID string) *quickfix.Settings {
	settings := quickfix.NewSettings()
	
	globalSettings := settings.GlobalSettings()
	globalSettings.Set("BeginString", BeginStringFIXT11)
	globalSettings.Set("DefaultApplVerID", DefaultApplVerID)

	sessionSettings := quickfix.NewSessionSettings()
	sessionSettings.Set("BeginString", BeginStringFIXT11)
	sessionSettings.Set("SenderCompID", senderCompID)
	sessionSettings.Set("TargetCompID", targetCompID)
	sessionSettings.Set("DefaultApplVerID", DefaultApplVerID)
	sessionSettings.Set("HeartBtInt", "30")
	sessionSettings.Set("ResetOnLogon", "Y")
	sessionSettings.Set("ResetOnLogout", "Y")
	sessionSettings.Set("ResetOnDisconnect", "Y")

	settings.AddSession(sessionSettings)

	return settings
}

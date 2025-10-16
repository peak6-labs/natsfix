package natsfix

import (
	"testing"

	"github.com/quickfixgo/quickfix"
)

func TestValidateFIXTSettings(t *testing.T) {
	tests := []struct {
		name      string
		setupFn   func() *quickfix.SessionSettings
		wantError bool
	}{
		{
			name: "valid FIXT settings",
			setupFn: func() *quickfix.SessionSettings {
				s := quickfix.NewSessionSettings()
				s.Set("BeginString", BeginStringFIXT11)
				s.Set("DefaultApplVerID", DefaultApplVerID)
				return s
			},
			wantError: false,
		},
		{
			name: "missing BeginString",
			setupFn: func() *quickfix.SessionSettings {
				s := quickfix.NewSessionSettings()
				s.Set("DefaultApplVerID", DefaultApplVerID)
				return s
			},
			wantError: true,
		},
		{
			name: "wrong BeginString",
			setupFn: func() *quickfix.SessionSettings {
				s := quickfix.NewSessionSettings()
				s.Set("BeginString", "FIX.4.4")
				s.Set("DefaultApplVerID", DefaultApplVerID)
				return s
			},
			wantError: true,
		},
		{
			name: "missing DefaultApplVerID",
			setupFn: func() *quickfix.SessionSettings {
				s := quickfix.NewSessionSettings()
				s.Set("BeginString", BeginStringFIXT11)
				return s
			},
			wantError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			settings := tt.setupFn()
			err := ValidateFIXTSettings(settings)

			if tt.wantError && err == nil {
				t.Errorf("expected error but got none")
			}
			if !tt.wantError && err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
	}
}

func TestCreateFIXTSettings(t *testing.T) {
	settings := CreateFIXTSettings("GATEWAY", "CLIENT1")

	globalSettings := settings.GlobalSettings()

	beginString, _ := globalSettings.Setting("BeginString")
	if beginString != BeginStringFIXT11 {
		t.Errorf("BeginString = %s, want %s", beginString, BeginStringFIXT11)
	}

	applVerID, _ := globalSettings.Setting("DefaultApplVerID")
	if applVerID != DefaultApplVerID {
		t.Errorf("DefaultApplVerID = %s, want %s", applVerID, DefaultApplVerID)
	}

	sessionID := quickfix.SessionID{
		BeginString:  BeginStringFIXT11,
		SenderCompID: "GATEWAY",
		TargetCompID: "CLIENT1",
	}

	sessionSettings, ok := settings.SessionSettings()[sessionID]
	if !ok {
		t.Fatalf("session not found for %v", sessionID)
	}

	senderCompID, _ := sessionSettings.Setting("SenderCompID")
	if senderCompID != "GATEWAY" {
		t.Errorf("SenderCompID = %s, want GATEWAY", senderCompID)
	}

	targetCompID, _ := sessionSettings.Setting("TargetCompID")
	if targetCompID != "CLIENT1" {
		t.Errorf("TargetCompID = %s, want CLIENT1", targetCompID)
	}
}

package natsfix

import (
	"testing"

	"github.com/quickfixgo/quickfix"
)

func TestNormalizeToken(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"FIXT.1.1", "FIXT_1_1"},
		{"CLIENT 1", "CLIENT_1"},
		{"NORMAL", "NORMAL"},
		{"A>B", "A_B"},
		{"A*B", "A_B"},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got := normalizeToken(tt.input)
			if got != tt.want {
				t.Errorf("normalizeToken(%v) = %v, want %v", tt.input, got, tt.want)
			}
		})
	}
}

func TestExpandSubjectTemplate(t *testing.T) {
	sessionID := quickfix.SessionID{
		BeginString:      "FIXT.1.1",
		SenderCompID:     "GATEWAY",
		SenderSubID:      "GSUB",
		SenderLocationID: "GLOC",
		TargetCompID:     "CLIENT1",
		TargetSubID:      "SUB1",
		TargetLocationID: "LOC1",
		Qualifier:        "TRADE",
	}

	tests := []struct {
		name     string
		template string
		want     string
	}{
		{
			name:     "simple template",
			template: "fix.{SenderCompID}.{TargetCompID}",
			want:     "fix.GATEWAY.CLIENT1",
		},
		{
			name:     "template with all fields",
			template: "{BeginString}.{SenderCompID}.{SenderSubID}.{SenderLocationID}.{TargetCompID}.{TargetSubID}.{TargetLocationID}.{Qualifier}",
			want:     "FIXT_1_1.GATEWAY.GSUB.GLOC.CLIENT1.SUB1.LOC1.TRADE",
		},
		{
			name:     "template with literal text",
			template: "apex.fix.inbound.{SenderCompID}.to.{TargetCompID}",
			want:     "apex.fix.inbound.GATEWAY.to.CLIENT1",
		},
		{
			name:     "empty fields in template",
			template: "fix.{Qualifier}.msgs",
			want:     "fix.TRADE.msgs",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ExpandSubjectTemplate(tt.template, sessionID)
			if got != tt.want {
				t.Errorf("ExpandSubjectTemplate() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestExpandSubjectTemplateWithEmptyFields(t *testing.T) {
	sessionID := quickfix.SessionID{
		BeginString:  "FIXT.1.1",
		SenderCompID: "GATEWAY",
		TargetCompID: "CLIENT1",
	}

	template := "fix.{SenderCompID}.{SenderSubID}.{TargetCompID}"
	got := ExpandSubjectTemplate(template, sessionID)
	want := "fix.GATEWAY..CLIENT1"

	if got != want {
		t.Errorf("ExpandSubjectTemplate() = %v, want %v", got, want)
	}
}

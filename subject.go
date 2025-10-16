package natsfix

import (
	"strings"

	"github.com/quickfixgo/quickfix"
)

const (
	SubjectPrefix = "fix"
)

func normalizeToken(s string) string {
	s = strings.ReplaceAll(s, ".", "_")
	s = strings.ReplaceAll(s, " ", "_")
	s = strings.ReplaceAll(s, ">", "_")
	s = strings.ReplaceAll(s, "*", "_")
	return s
}

func ExpandSubjectTemplate(template string, sessionID quickfix.SessionID) string {
	result := template
	result = strings.ReplaceAll(result, "{BeginString}", normalizeToken(sessionID.BeginString))
	result = strings.ReplaceAll(result, "{SenderCompID}", normalizeToken(sessionID.SenderCompID))
	result = strings.ReplaceAll(result, "{SenderSubID}", normalizeToken(sessionID.SenderSubID))
	result = strings.ReplaceAll(result, "{SenderLocationID}", normalizeToken(sessionID.SenderLocationID))
	result = strings.ReplaceAll(result, "{TargetCompID}", normalizeToken(sessionID.TargetCompID))
	result = strings.ReplaceAll(result, "{TargetSubID}", normalizeToken(sessionID.TargetSubID))
	result = strings.ReplaceAll(result, "{TargetLocationID}", normalizeToken(sessionID.TargetLocationID))
	result = strings.ReplaceAll(result, "{Qualifier}", normalizeToken(sessionID.Qualifier))
	return result
}

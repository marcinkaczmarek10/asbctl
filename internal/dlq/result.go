package dlq

import "github.com/marcinkaczmarek10/asbctl/internal/export"

// Per-message statuses.
const (
	StatusResent                 = "resent"
	StatusPurged                 = "purged"
	StatusFailed                 = "failed"
	StatusSentNotCompleted       = "sent_not_completed"
	StatusSkippedDuplicateWindow = "skipped_duplicate_window"
)

// Result is the outcome for one selected message.
type Result struct {
	SequenceNumber int64
	MessageID      string
	Status         string
	Err            error
}

// ResendSummary closes a resend run. NotReached counts selected messages that the run never received, for
// instance because they sat behind more messages than one lock duration could read through, or because the run
// reached the hold cap, which ScanLimitReached says.
type ResendSummary struct {
	Schema           int    `json:"schema"`
	Type             string `json:"type"`
	Resent           int    `json:"resent"`
	Failed           int    `json:"failed"`
	SentNotCompleted int    `json:"sent_not_completed"`
	Skipped          int    `json:"skipped"`
	Bounced          int    `json:"bounced"`
	StoppedOnBounce  bool   `json:"stopped_on_bounce"`
	NotReached       int    `json:"not_reached"`
	ScanLimitReached bool   `json:"scan_limit_reached"`
	Snapshot         string `json:"snapshot"`
}

// PurgeSummary closes a purge run.
type PurgeSummary struct {
	Schema           int    `json:"schema"`
	Type             string `json:"type"`
	Purged           int    `json:"purged"`
	Failed           int    `json:"failed"`
	NotReached       int    `json:"not_reached"`
	ScanLimitReached bool   `json:"scan_limit_reached"`
	Snapshot         string `json:"snapshot"`
}

func newResendSummary(snapshot string) ResendSummary {
	return ResendSummary{Schema: export.Schema, Type: "summary", Snapshot: snapshot}
}

func newPurgeSummary(snapshot string) PurgeSummary {
	return PurgeSummary{Schema: export.Schema, Type: "summary", Snapshot: snapshot}
}

// ExitCode is 0 when every selected message was resent and none bounced, 1 when none was resent, and 3 otherwise.
func (s ResendSummary) ExitCode(selected int) int {
	switch {
	case selected == 0:
		return 0
	case s.Resent == 0:
		return 1
	case s.Resent == selected && s.Bounced == 0 && !s.StoppedOnBounce:
		return 0
	default:
		return 3
	}
}

// ExitCode is 0 when every selected message was purged, 1 when none was, and 3 otherwise.
func (s PurgeSummary) ExitCode(selected int) int {
	switch {
	case selected == 0 || s.Purged == selected:
		return 0
	case s.Purged == 0:
		return 1
	default:
		return 3
	}
}

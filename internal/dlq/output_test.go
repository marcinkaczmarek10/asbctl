package dlq

import (
	"bytes"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/marcinkaczmarek10/asbctl/internal/bus"
	"github.com/marcinkaczmarek10/asbctl/internal/export"
)

var planSelection = Selection{Entries: []Entry{
	{SequenceNumber: 1, MessageID: "a", PayloadTypeID: "Order", Reason: "Poison"},
	{SequenceNumber: 2, MessageID: "a", PayloadTypeID: "Order", Reason: "Poison"},
	{SequenceNumber: 3, MessageID: "b", PayloadTypeID: "", Reason: "Expired"},
	{SequenceNumber: 4, MessageID: "c", PayloadTypeID: "Order", Reason: "Poison"},
}}

func resendPlan(params ResendParams, target Location, dryRun bool) Plan {
	return NewResendPlan(ResendPlanInput{
		Source: bus.Target{Queue: "orders", DeadLetter: true}, Target: target, Selection: planSelection,
		Snapshot: "snap.ndjson", Params: params, DryRun: dryRun,
	})
}

func TestNewResendPlanCountsWhatTheGuardWouldSkip(t *testing.T) {
	params := ResendParams{Dedupe: bus.DuplicateDetection{Enabled: true, Window: 10 * time.Minute}, Recent: map[string]struct{}{"c": {}, "b": {}}}

	plan := resendPlan(params, Location{Queue: "orders"}, false)

	want := DedupePlan{Enabled: true, WindowSeconds: 600, RepeatedInSelection: 2, InLedger: 2}
	if *plan.Dedupe != want {
		t.Errorf("Dedupe = %+v, want %+v", *plan.Dedupe, want)
	}
	if plan.Selected != 4 || plan.Groups[0] != (Group{"Order", "Poison", 3}) {
		t.Errorf("plan = %+v, want 4 selected and the Order/Poison group first", plan)
	}
}

func TestNewResendPlanDoesNotCountSkipsWhenTheGuardIsOffOrIdsAreReplaced(t *testing.T) {
	off := resendPlan(ResendParams{Recent: map[string]struct{}{"c": {}}}, Location{Queue: "orders"}, false)
	replaced := resendPlan(ResendParams{Dedupe: bus.DuplicateDetection{Enabled: true, Window: time.Minute}, NewMessageID: func() string { return "x" }}, Location{Queue: "orders"}, false)

	if off.Dedupe.Enabled || off.Dedupe.InLedger != 0 || replaced.Dedupe.RepeatedInSelection != 0 {
		t.Errorf("off = %+v, replaced = %+v, want no skips counted", *off.Dedupe, *replaced.Dedupe)
	}
}

func TestNewResendPlanWarnings(t *testing.T) {
	tests := []struct {
		name   string
		params ResendParams
		target Location
		want   []string
		none   []string
	}{
		{name: "same queue has no warning", params: ResendParams{}, target: Location{Queue: "orders"}, none: []string{"topic", "assumed", "bounce", "--new-message-id"}},
		{name: "topic target", params: ResendParams{}, target: Location{Topic: "events"}, want: []string{"reaches every matching subscription", "bounce detection reads the source"}},
		{name: "another queue", params: ResendParams{}, target: Location{Queue: "other"}, want: []string{"bounce detection reads the source"}, none: []string{"matching subscription"}},
		{name: "assumed detection", params: ResendParams{Dedupe: bus.AssumedDuplicateDetection()}, target: Location{Queue: "orders"}, want: []string{"assumed on with a window of 10m0s"}},
		{
			name:   "new ids with detection on",
			params: ResendParams{Dedupe: bus.DuplicateDetection{Enabled: true, Window: time.Minute}, NewMessageID: func() string { return "x" }},
			target: Location{Queue: "orders"}, want: []string{"will process each message twice"},
		},
		{name: "new ids with detection off", params: ResendParams{NewMessageID: func() string { return "x" }}, target: Location{Queue: "orders"}, none: []string{"twice"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			joined := strings.Join(resendPlan(tc.params, tc.target, false).Warnings, "|")

			for _, want := range tc.want {
				if !strings.Contains(joined, want) {
					t.Errorf("warnings = %q, want one containing %q", joined, want)
				}
			}
			for _, none := range tc.none {
				if strings.Contains(joined, none) {
					t.Errorf("warnings = %q, want none containing %q", joined, none)
				}
			}
		})
	}
}

func TestWritePlanTable(t *testing.T) {
	params := ResendParams{Dedupe: bus.DuplicateDetection{Enabled: true, Window: 10 * time.Minute}}
	var out bytes.Buffer

	err := WritePlan(&out, export.FormatTable, resendPlan(params, Location{Topic: "events"}, false))

	if err != nil {
		t.Fatalf("WritePlan() error = %v", err)
	}
	for _, want := range []string{
		"command:   dlq resend", "source:    queue orders (dead-letter queue)", "target:    topic events", "selected:  4",
		"snapshot:  snap.ndjson (written when the command runs)", "duplicate detection:  on, window 10m0s: 2 repeat a MessageId",
		"COUNT  PAYLOAD_TYPE  REASON", "3      Order         Poison", "1      (none)        Expired",
		"Warning: a topic send reaches every matching subscription", "Nothing was changed. Pass --yes to run it.",
	} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("plan = \n%s\nwant it to contain %q", out.String(), want)
		}
	}
}

func TestWritePlanTableForADryRunAndAPurge(t *testing.T) {
	var out bytes.Buffer

	_ = WritePlan(&out, export.FormatTable, NewPurgePlan(bus.Target{Topic: "events", Subscription: "billing", DeadLetter: true}, planSelection, "snap.ndjson", true))

	for _, want := range []string{"command:   dlq purge", "topic events, subscription billing (dead-letter queue)", "snapshot:  snap.ndjson (not written in a dry run)", "Dry run: nothing was changed."} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("plan = \n%s\nwant it to contain %q", out.String(), want)
		}
	}
	if strings.Contains(out.String(), "target:") || strings.Contains(out.String(), "duplicate detection") {
		t.Errorf("plan = \n%s\nwant no target or duplicate detection lines for a purge", out.String())
	}
}

func TestWritePlanJSON(t *testing.T) {
	var out bytes.Buffer

	_ = WritePlan(&out, export.FormatJSON, NewPurgePlan(bus.Target{Queue: "orders", DeadLetter: true}, Selection{}, "s.ndjson", false))

	want := `{"schema":1,"type":"plan","command":"dlq purge","source":{"queue":"orders"},"selected":0,"groups":[],"snapshot":"s.ndjson","warnings":[],"dry_run":false}` + "\n"
	if out.String() != want {
		t.Errorf("plan = %s, want %s", out.String(), want)
	}
}

func TestWriteResult(t *testing.T) {
	tests := []struct {
		name   string
		format export.Format
		result Result
		want   string
	}{
		{name: "json resent", format: export.FormatJSON, result: Result{SequenceNumber: 5, MessageID: "m", Status: StatusResent}, want: `{"schema":1,"sequence_number":5,"message_id":"m","status":"resent"}` + "\n"},
		{name: "json failed", format: export.FormatJSON, result: Result{SequenceNumber: 6, Status: StatusFailed, Err: errors.New("boom")}, want: `{"schema":1,"sequence_number":6,"status":"failed","error":"boom"}` + "\n"},
		{name: "table says nothing for resent", format: export.FormatTable, result: Result{Status: StatusResent}, want: ""},
		{name: "table says nothing for purged", format: export.FormatTable, result: Result{Status: StatusPurged}, want: ""},
		{name: "table names a skip", format: export.FormatTable, result: Result{SequenceNumber: 7, MessageID: "m", Status: StatusSkippedDuplicateWindow}, want: "sequence 7 m: skipped_duplicate_window\n"},
		{name: "table names a failure", format: export.FormatTable, result: Result{SequenceNumber: 8, MessageID: "m", Status: StatusFailed, Err: errors.New("boom")}, want: "sequence 8 m: failed: boom\n"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var out bytes.Buffer

			if err := WriteResult(&out, tc.format, tc.result); err != nil || out.String() != tc.want {
				t.Errorf("WriteResult() = %q, %v, want %q", out.String(), err, tc.want)
			}
		})
	}
}

func TestWriteSummaries(t *testing.T) {
	resend := ResendSummary{Schema: 1, Type: "summary", Resent: 8, Failed: 1, SentNotCompleted: 2, Skipped: 3, Bounced: 4, StoppedOnBounce: true, NotReached: 5, Snapshot: "snap"}
	purge := PurgeSummary{Schema: 1, Type: "summary", Purged: 8, Failed: 1, NotReached: 2, Snapshot: "snap"}
	tests := []struct {
		name  string
		write func(*bytes.Buffer) error
		want  []string
	}{
		{
			name: "resend json", write: func(b *bytes.Buffer) error { return WriteResendSummary(b, export.FormatJSON, resend) },
			want: []string{`{"schema":1,"type":"summary","resent":8,"failed":1,"sent_not_completed":2,"skipped":3,"bounced":4,"stopped_on_bounce":true,"not_reached":5,"snapshot":"snap"}`},
		},
		{
			name: "resend table", write: func(b *bytes.Buffer) error { return WriteResendSummary(b, export.FormatTable, resend) },
			want: []string{"Resent 8, failed 1, sent but not completed 2, skipped 3, bounced 4.", "Stopped before the next wave", "5 selected messages were not reached.", "were sent already", "Snapshot: snap"},
		},
		{
			name: "purge json", write: func(b *bytes.Buffer) error { return WritePurgeSummary(b, export.FormatJSON, purge) },
			want: []string{`{"schema":1,"type":"summary","purged":8,"failed":1,"not_reached":2,"snapshot":"snap"}`},
		},
		{
			name: "purge table", write: func(b *bytes.Buffer) error { return WritePurgeSummary(b, export.FormatTable, purge) },
			want: []string{"Purged 8, failed 1.", "2 selected messages were not reached.", "Snapshot: snap"},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var out bytes.Buffer

			if err := tc.write(&out); err != nil {
				t.Fatalf("write error = %v", err)
			}
			for _, want := range tc.want {
				if !strings.Contains(out.String(), want) {
					t.Errorf("output = %q, want it to contain %q", out.String(), want)
				}
			}
		})
	}
}

func TestWriteResendSummaryTableOmitsTheQuietLines(t *testing.T) {
	var out bytes.Buffer

	_ = WriteResendSummary(&out, export.FormatTable, ResendSummary{Resent: 3, Snapshot: "snap"})

	for _, unwanted := range []string{"Stopped", "not reached", "sent already"} {
		if strings.Contains(out.String(), unwanted) {
			t.Errorf("summary = %q, want no %q line for a clean run", out.String(), unwanted)
		}
	}
}

func TestLocationString(t *testing.T) {
	tests := []struct {
		location Location
		want     string
	}{
		{Location{Queue: "q"}, "queue q"},
		{Location{Topic: "t"}, "topic t"},
		{Location{Topic: "t", Subscription: "s"}, "topic t, subscription s"},
	}
	for _, tc := range tests {
		if got := tc.location.String(); got != tc.want {
			t.Errorf("Location%+v.String() = %q, want %q", tc.location, got, tc.want)
		}
	}
}

func TestWritePlanTableExactOutput(t *testing.T) {
	tests := []struct {
		name string
		plan Plan
		want string
	}{
		{
			name: "a purge plan with groups and no warnings",
			plan: NewPurgePlan(bus.Target{Queue: "orders", DeadLetter: true}, Selection{Entries: []Entry{{PayloadTypeID: "Order", Reason: "Poison"}}}, "snap.ndjson", false),
			want: "command:   dlq purge\nsource:    queue orders (dead-letter queue)\nselected:  1\n" +
				"snapshot:  snap.ndjson (written when the command runs)\n\n" +
				"COUNT  PAYLOAD_TYPE  REASON\n1      Order         Poison\n\nNothing was changed. Pass --yes to run it.\n",
		},
		{
			name: "an empty selection has no table",
			plan: NewPurgePlan(bus.Target{Queue: "orders", DeadLetter: true}, Selection{}, "snap.ndjson", true),
			want: "command:   dlq purge\nsource:    queue orders (dead-letter queue)\nselected:  0\n" +
				"snapshot:  snap.ndjson (not written in a dry run)\n\nDry run: nothing was changed.\n",
		},
		{
			name: "a warning follows the table on its own line",
			plan: Plan{Command: "dlq purge", Source: Location{Queue: "q"}, Warnings: []string{"careful"}},
			want: "command:   dlq purge\nsource:    queue q (dead-letter queue)\nselected:  0\n\nWarning: careful.\n\nNothing was changed. Pass --yes to run it.\n",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var out bytes.Buffer

			if err := WritePlan(&out, export.FormatTable, tc.plan); err != nil || out.String() != tc.want {
				t.Errorf("WritePlan() =\n%q\nwant\n%q", out.String(), tc.want)
			}
		})
	}
}

func TestDedupeText(t *testing.T) {
	tests := []struct {
		name string
		plan DedupePlan
		want string
	}{
		{name: "off", plan: DedupePlan{}, want: "off"},
		{name: "on", plan: DedupePlan{Enabled: true, WindowSeconds: 600}, want: "on, window 10m0s"},
		{name: "assumed", plan: DedupePlan{Enabled: true, WindowSeconds: 600, Assumed: true}, want: "on, window 10m0s (assumed)"},
		{name: "only repeats", plan: DedupePlan{Enabled: true, WindowSeconds: 60, RepeatedInSelection: 2}, want: "on, window 1m0s: 2 repeat a MessageId in the selection and 0 were resent inside the window, and will be skipped"},
		{name: "only ledger hits", plan: DedupePlan{Enabled: true, WindowSeconds: 60, InLedger: 1}, want: "on, window 1m0s: 0 repeat a MessageId in the selection and 1 were resent inside the window, and will be skipped"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := dedupeText(tc.plan); got != tc.want {
				t.Errorf("dedupeText() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestWritePurgeSummaryTableWithoutAnythingNotReached(t *testing.T) {
	var out bytes.Buffer

	_ = WritePurgeSummary(&out, export.FormatTable, PurgeSummary{Purged: 3, Snapshot: "snap"})

	if want := "Purged 3, failed 0.\nSnapshot: snap\n"; out.String() != want {
		t.Errorf("summary = %q, want %q", out.String(), want)
	}
}

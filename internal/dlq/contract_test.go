package dlq

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/marcinkaczmarek10/asbctl/internal/bus"
	"github.com/marcinkaczmarek10/asbctl/internal/export"
)

// The files in testdata/contract are the output shapes that the Python adapter parses. This test pins them to
// what the writers produce. Run it with UPDATE_CONTRACT=1 to rewrite them after an intended change.
const contractDir = "../../testdata/contract"

func contractFiles(t *testing.T) map[string]string {
	t.Helper()
	write := func(buffer *bytes.Buffer, err error) string {
		t.Helper()
		if err != nil {
			t.Fatalf("write error = %v", err)
		}
		return buffer.String()
	}

	resendPlanJSON := new(bytes.Buffer)
	params := ResendParams{
		Dedupe: bus.DuplicateDetection{Enabled: true, Window: 10 * time.Minute},
		Recent: map[string]struct{}{"c": {}},
	}
	plan := NewResendPlan(ResendPlanInput{
		Source: bus.Target{Queue: "orders", DeadLetter: true}, Target: Location{Queue: "orders"}, Selection: planSelection,
		Snapshot: "asbctl-snapshot-orders-20261105T143005Z.ndjson", Params: params,
	})
	resendPlan := write(resendPlanJSON, WritePlan(resendPlanJSON, export.FormatJSON, plan))

	purgePlanJSON := new(bytes.Buffer)
	purgePlan := write(purgePlanJSON, WritePlan(purgePlanJSON, export.FormatJSON,
		NewPurgePlan(bus.Target{Topic: "events", Subscription: "billing", DeadLetter: true}, planSelection, "snap.ndjson", true)))

	resendResults := new(bytes.Buffer)
	for _, result := range []Result{
		{SequenceNumber: 1042, MessageID: "msg-1", Status: StatusResent},
		{SequenceNumber: 1043, MessageID: "msg-2", Status: StatusFailed, Err: errors.New("the message is too large")},
		{SequenceNumber: 1044, MessageID: "msg-3", Status: StatusSentNotCompleted, Err: errors.New("lock lost")},
		{SequenceNumber: 1045, MessageID: "msg-4", Status: StatusSkippedDuplicateWindow},
	} {
		if err := WriteResult(resendResults, export.FormatJSON, result); err != nil {
			t.Fatalf("WriteResult() error = %v", err)
		}
	}
	summary := ResendSummary{Schema: 1, Type: "summary", Resent: 1, Failed: 1, SentNotCompleted: 1, Skipped: 1, Bounced: 1, StoppedOnBounce: true, NotReached: 2, ScanLimitReached: true, Snapshot: "snap.ndjson"}
	if err := WriteResendSummary(resendResults, export.FormatJSON, summary); err != nil {
		t.Fatalf("WriteResendSummary() error = %v", err)
	}

	purgeResults := new(bytes.Buffer)
	for _, result := range []Result{
		{SequenceNumber: 2001, MessageID: "p-1", Status: StatusPurged},
		{SequenceNumber: 2002, MessageID: "p-2", Status: StatusFailed, Err: errors.New("lock lost")},
	} {
		if err := WriteResult(purgeResults, export.FormatJSON, result); err != nil {
			t.Fatalf("WriteResult() error = %v", err)
		}
	}
	if err := WritePurgeSummary(purgeResults, export.FormatJSON, PurgeSummary{Schema: 1, Type: "summary", Purged: 1, Failed: 1, NotReached: 0, Snapshot: "snap.ndjson"}); err != nil {
		t.Fatalf("WritePurgeSummary() error = %v", err)
	}

	return map[string]string{
		"dlq_resend_plan.json":     resendPlan,
		"dlq_purge_plan.json":      purgePlan,
		"dlq_resend_result.ndjson": resendResults.String(),
		"dlq_purge_result.ndjson":  purgeResults.String(),
	}
}

func TestContractFilesMatchTheWriters(t *testing.T) {
	for name, got := range contractFiles(t) {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(contractDir, name)
			if os.Getenv("UPDATE_CONTRACT") != "" {
				if err := os.WriteFile(path, []byte(got), 0o644); err != nil {
					t.Fatalf("write %s: %v", path, err)
				}
			}

			want, err := os.ReadFile(path)
			if err != nil {
				t.Fatalf("read %s: %v", path, err)
			}
			if got != string(want) {
				t.Errorf("%s differs from the writer output.\ngot:\n%s\nwant:\n%s", name, got, want)
			}
		})
	}
}

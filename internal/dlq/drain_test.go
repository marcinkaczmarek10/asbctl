package dlq

import (
	"bytes"
	"context"
	"log/slog"
	"reflect"
	"strings"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
)

func noiseQueue(noise int64, wanted ...string) *fakeQueue {
	queue := newFakeQueue()
	for seq := int64(1); seq <= noise; seq++ {
		queue.add(dlMessage(seq, "noise", "Noise", "r"))
	}
	for i, id := range wanted {
		queue.add(dlMessage(noise+int64(i)+1, id, "Order", "r"))
	}
	return queue
}

func discardProcess(context.Context, []candidate, func(*azservicebus.ReceivedMessage)) (bool, error) {
	return false, nil
}

func TestDrainStopsAtTheHoldCapAndReportsTheRestAsNotHandled(t *testing.T) {
	queue := noiseQueue(250, "wanted")
	selected := selectionOf(t, queue, Filter{PayloadTypeID: "Order"}).MessageIDs()

	result, err := drain(context.Background(), queue, drainOptions{BatchSize: 100, MaxHeld: 100, Logger: discardLogger}, selected, discardProcess)

	if err != nil {
		t.Fatalf("drain() error = %v", err)
	}
	if result.Handled != 0 || !result.ScanLimited {
		t.Errorf("result = %+v, want nothing handled and the scan limit reached", result)
	}
	if !reflect.DeepEqual(queue.abandoned, seqs(1, 100)) {
		t.Errorf("abandoned %d messages, want the 100 held ones released at the end", len(queue.abandoned))
	}
	if queue.receives != 1 {
		t.Errorf("receives = %d, want the run to stop after the batch that hit the cap", queue.receives)
	}
}

func TestDrainDoesNotFlagTheCapWhenEverySelectedMessageWasHandled(t *testing.T) {
	queue := noiseQueue(99, "wanted")
	selected := selectionOf(t, queue, Filter{PayloadTypeID: "Order"}).MessageIDs()

	result, _ := drain(context.Background(), queue, drainOptions{BatchSize: 100, MaxHeld: 99, Logger: discardLogger}, selected, discardProcess)

	if result.Handled != 1 || result.ScanLimited {
		t.Errorf("result = %+v, want the selected message handled and no scan limit: the cap was hit with nothing left to find", result)
	}
}

func TestDrainWithoutACapReadsThroughEverything(t *testing.T) {
	queue := noiseQueue(250, "wanted")
	selected := selectionOf(t, queue, Filter{PayloadTypeID: "Order"}).MessageIDs()

	result, _ := drain(context.Background(), queue, drainOptions{BatchSize: 100, Logger: discardLogger}, selected, discardProcess)

	if result.Handled != 1 || result.ScanLimited {
		t.Errorf("result = %+v, want the selected message found behind 250 others", result)
	}
}

func TestReleaseHeldCountsAbandonFailuresAndLogsThemOnce(t *testing.T) {
	queue := newFakeQueue(dlMessage(1, "a", "", "r"), dlMessage(2, "b", "", "r"), dlMessage(3, "c", "", "r"))
	queue.abandonErr[1], queue.abandonErr[3] = errBoom, errBoom
	var logs bytes.Buffer

	releaseHeld(context.Background(), queue, slog.New(slog.NewTextHandler(&logs, nil)), queue.messages)

	if got := strings.Count(logs.String(), "level=WARN"); got != 1 {
		t.Fatalf("warnings = %d in %q, want exactly one", got, logs.String())
	}
	for _, want := range []string{"failed=2", "held=3"} {
		if !strings.Contains(logs.String(), want) {
			t.Errorf("log = %q, want it to contain %q", logs.String(), want)
		}
	}
	if !reflect.DeepEqual(queue.abandoned, []int64{2}) {
		t.Errorf("abandoned = %v, want the one that could be released", queue.abandoned)
	}
}

func TestReleaseHeldLogsNothingWhenEveryAbandonSucceeds(t *testing.T) {
	queue := newFakeQueue(dlMessage(1, "a", "", "r"))
	var logs bytes.Buffer

	releaseHeld(context.Background(), queue, slog.New(slog.NewTextHandler(&logs, nil)), queue.messages)

	if logs.Len() != 0 {
		t.Errorf("log = %q, want nothing", logs.String())
	}
}

func TestResendReportsTheScanLimitInTheSummary(t *testing.T) {
	queue := noiseQueue(250, "wanted")
	f := newResendFixture(t, queue, Filter{PayloadTypeID: "Order"})
	f.params.MaxHeld = 100

	summary := f.run(t)

	if !summary.ScanLimitReached || summary.NotReached != 1 || summary.Resent != 0 {
		t.Errorf("summary = %+v, want the scan limit reached with 1 not reached", summary)
	}
	if code := summary.ExitCode(1); code != 1 {
		t.Errorf("ExitCode() = %d, want 1 because nothing was resent", code)
	}
}

func TestPurgeReportsTheScanLimitInTheSummary(t *testing.T) {
	queue := noiseQueue(250, "wanted")
	selection := selectionOf(t, queue, Filter{PayloadTypeID: "Order"})

	summary, err := Purge(context.Background(), queue, selection, "snap", PurgeOptions{MaxHeld: 100, Logger: discardLogger}, func(Result) error { return nil })

	if err != nil || !summary.ScanLimitReached || summary.NotReached != 1 {
		t.Errorf("Purge() = %+v, %v, want the scan limit reached with 1 not reached", summary, err)
	}
}

func TestDrainDoesNotCallProcessForABatchWithNothingSelected(t *testing.T) {
	queue := noiseQueue(150, "wanted")
	selected := selectionOf(t, queue, Filter{PayloadTypeID: "Order"}).MessageIDs()
	calls := 0

	_, err := drain(context.Background(), queue, drainOptions{BatchSize: 100, Logger: discardLogger}, selected,
		func(ctx context.Context, candidates []candidate, hold func(*azservicebus.ReceivedMessage)) (bool, error) {
			calls++
			return false, nil
		})

	if err != nil || calls != 1 {
		t.Errorf("process calls = %d, error = %v, want 1: the first batch of 100 held none of the selected messages", calls, err)
	}
}

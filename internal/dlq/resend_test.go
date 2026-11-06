package dlq

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
	"github.com/marcinkaczmarek10/asbctl/internal/bus"
	"github.com/marcinkaczmarek10/asbctl/internal/export"
)

func seqs(from, to int64) []int64 {
	var out []int64
	for seq := from; seq <= to; seq++ {
		out = append(out, seq)
	}
	return out
}

func TestResendSendsThenCompletesAndKeepsTheOtherProperties(t *testing.T) {
	queue := newFakeQueue(dlMessage(1, "a", "Order", "r"), dlMessage(2, "b", "Order", "r"), dlMessage(3, "c", "Order", "r"))
	f := newResendFixture(t, queue, Filter{})

	summary := f.run(t)

	if summary.Resent != 3 || summary.Failed+summary.SentNotCompleted+summary.Skipped != 0 || summary.NotReached != 0 || summary.Snapshot != "snap.ndjson" {
		t.Errorf("summary = %+v, want 3 resent and nothing else", summary)
	}
	if !reflect.DeepEqual(queue.completed, []int64{1, 2, 3}) || len(queue.abandoned) != 0 {
		t.Errorf("completed %v, abandoned %v, want 1 2 3 completed and none abandoned", queue.completed, queue.abandoned)
	}
	if len(f.sender.sent) != 3 {
		t.Fatalf("sent %d messages, want 3", len(f.sender.sent))
	}
	properties := f.sender.sent[0].Message.ApplicationProperties
	if _, hasReason := properties["DeadLetterReason"]; hasReason {
		t.Errorf("properties = %v, want the dead-letter reason dropped", properties)
	}
	if _, hasDescription := properties["DeadLetterErrorDescription"]; hasDescription {
		t.Errorf("properties = %v, want the dead-letter description dropped", properties)
	}
	if properties["kept"] != "yes" || properties["PayloadTypeId"] != "Order" {
		t.Errorf("properties = %v, want the other application properties kept", properties)
	}
	if *f.sender.sent[0].Message.MessageID != "a" {
		t.Errorf("MessageId = %q, want the original id", *f.sender.sent[0].Message.MessageID)
	}
	if got, want := f.statuses(), []string{"1:resent", "2:resent", "3:resent"}; !reflect.DeepEqual(got, want) {
		t.Errorf("results = %v, want %v", got, want)
	}
}

func TestResendRecordsEachSendInTheLedger(t *testing.T) {
	queue := newFakeQueue(dlMessage(1, "a", "", "r"), dlMessage(2, "b", "", "r"))
	f := newResendFixture(t, queue, Filter{})

	f.run(t)

	got, err := RecentFromFile(f.path, "ns.servicebus.windows.net", "queue:orders", time.Hour, runStart)
	if err != nil || !reflect.DeepEqual(got, map[string]struct{}{"a": {}, "b": {}}) {
		t.Errorf("ledger = %v, %v, want a and b", got, err)
	}
}

func TestResendReadsPastNonSelectedMessagesInsteadOfAbandoningThemAtOnce(t *testing.T) {
	queue := newFakeQueue()
	for seq := int64(1); seq <= 250; seq++ {
		queue.add(dlMessage(seq, "noise", "Noise", "r"))
	}
	queue.add(dlMessage(251, "wanted-1", "Order", "r"))
	queue.add(dlMessage(252, "wanted-2", "Order", "r"))
	f := newResendFixture(t, queue, Filter{PayloadTypeID: "Order"})

	summary := f.run(t)

	if summary.Resent != 2 || summary.NotReached != 0 {
		t.Errorf("summary = %+v, want both selected messages reached behind 250 others", summary)
	}
	if !reflect.DeepEqual(queue.completed, []int64{251, 252}) {
		t.Errorf("completed = %v, want 251 and 252", queue.completed)
	}
	if !reflect.DeepEqual(queue.abandoned, seqs(1, 250)) {
		t.Errorf("abandoned %d messages, want the 250 others abandoned at the end", len(queue.abandoned))
	}
	if queue.receives > 4 {
		t.Errorf("receives = %d, want about 3: each batch must move past the locked ones", queue.receives)
	}
}

func TestResendAbandonsAFailedSendAtTheEndAndReportsIt(t *testing.T) {
	queue := newFakeQueue(dlMessage(1, "a", "", "r"), dlMessage(2, "b", "", "r"))
	f := newResendFixture(t, queue, Filter{})
	f.sender.failing[1] = errBoom

	summary := f.run(t)

	if summary.Resent != 1 || summary.Failed != 1 {
		t.Errorf("summary = %+v, want 1 resent and 1 failed", summary)
	}
	if !reflect.DeepEqual(queue.completed, []int64{2}) || !reflect.DeepEqual(queue.abandoned, []int64{1}) {
		t.Errorf("completed %v, abandoned %v, want 2 completed and 1 abandoned", queue.completed, queue.abandoned)
	}
	if f.got[0].Status != StatusFailed || !errors.Is(f.got[0].Err, errBoom) {
		t.Errorf("first result = %+v, want failed with the send error", f.got[0])
	}
	recent, _ := RecentFromFile(f.path, "ns.servicebus.windows.net", "queue:orders", time.Hour, runStart)
	if _, wrote := recent["a"]; wrote {
		t.Error("a failed send was written to the ledger")
	}
}

func TestResendNeverHidesAFailedComplete(t *testing.T) {
	queue := newFakeQueue(dlMessage(1, "a", "", "r"), dlMessage(2, "b", "", "r"))
	queue.completeErr[1] = errBoom
	f := newResendFixture(t, queue, Filter{})

	summary := f.run(t)

	if summary.Resent != 1 || summary.SentNotCompleted != 1 {
		t.Errorf("summary = %+v, want 1 resent and 1 sent_not_completed", summary)
	}
	if f.got[0].Status != StatusSentNotCompleted || !errors.Is(f.got[0].Err, errBoom) {
		t.Errorf("first result = %+v, want sent_not_completed with the complete error", f.got[0])
	}
	if len(f.sender.sent) != 2 {
		t.Errorf("sent %d, want both sent: the copy exists even when Complete failed", len(f.sender.sent))
	}
	recent, _ := RecentFromFile(f.path, "ns.servicebus.windows.net", "queue:orders", time.Hour, runStart)
	if _, wrote := recent["a"]; !wrote {
		t.Error("a sent message with a failed Complete is missing from the ledger")
	}
}

func TestResendSkipsMessagesThatDuplicateDetectionWouldDrop(t *testing.T) {
	queue := newFakeQueue(
		dlMessage(1, "twin", "", "r"), dlMessage(2, "twin", "", "r"), dlMessage(3, "solo", "", "r"), dlMessage(4, "again", "", "r"),
	)
	f := newResendFixture(t, queue, Filter{})
	f.params.Dedupe = bus.DuplicateDetection{Enabled: true, Window: 10 * time.Minute}
	f.params.Recent = map[string]struct{}{"again": {}}

	summary := f.run(t)

	if summary.Resent != 1 || summary.Skipped != 3 {
		t.Errorf("summary = %+v, want solo resent and twin, twin and again skipped", summary)
	}
	if got, want := f.statuses(), []string{"1:skipped_duplicate_window", "2:skipped_duplicate_window", "4:skipped_duplicate_window", "3:resent"}; !reflect.DeepEqual(got, want) {
		t.Errorf("results = %v, want %v", got, want)
	}
	if len(f.sender.sent) != 1 || f.sender.sent[0].MessageID != "solo" {
		t.Errorf("sent = %v, want only solo", f.sender.sent)
	}
	if !reflect.DeepEqual(queue.abandoned, []int64{1, 2, 4}) {
		t.Errorf("abandoned = %v, want the skipped messages left in the queue", queue.abandoned)
	}
}

func TestResendSkipsNothingWhenDuplicateDetectionIsOffOrIdsAreReplaced(t *testing.T) {
	tests := []struct {
		name  string
		setup func(p *ResendParams)
	}{
		{name: "detection off", setup: func(p *ResendParams) { p.Dedupe = bus.DuplicateDetection{} }},
		{name: "new ids", setup: func(p *ResendParams) {
			p.Dedupe = bus.DuplicateDetection{Enabled: true, Window: time.Hour}
			p.NewMessageID = func() string { return "fresh" }
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			queue := newFakeQueue(dlMessage(1, "twin", "", "r"), dlMessage(2, "twin", "", "r"))
			f := newResendFixture(t, queue, Filter{})
			f.params.Recent = map[string]struct{}{"twin": {}}
			tc.setup(&f.params)

			summary := f.run(t)

			if summary.Resent != 2 || summary.Skipped != 0 {
				t.Errorf("summary = %+v, want both resent", summary)
			}
		})
	}
}

func TestResendWithNewIDsKeepsTheOriginalInAProperty(t *testing.T) {
	queue := newFakeQueue(dlMessage(1, "original", "Order", "r"))
	f := newResendFixture(t, queue, Filter{})
	f.params.NewMessageID = func() string { return "new-id" }

	f.run(t)

	message := f.sender.sent[0].Message
	if *message.MessageID != "new-id" || message.ApplicationProperties[export.OriginalMessageIDProperty] != "original" {
		t.Errorf("message = id %q, properties %v, want new-id and the original in a property", *message.MessageID, message.ApplicationProperties)
	}
}

func TestSkipsDuplicate(t *testing.T) {
	enabled := bus.DuplicateDetection{Enabled: true, Window: time.Minute}
	duplicates := map[string]struct{}{"twin": {}}
	tests := []struct {
		name   string
		params ResendParams
		entry  Entry
		want   bool
	}{
		{name: "repeats in the selection", params: ResendParams{Dedupe: enabled}, entry: Entry{MessageID: "twin"}, want: true},
		{name: "in the ledger", params: ResendParams{Dedupe: enabled, Recent: map[string]struct{}{"x": {}}}, entry: Entry{MessageID: "x"}, want: true},
		{name: "neither", params: ResendParams{Dedupe: enabled}, entry: Entry{MessageID: "fine"}, want: false},
		{name: "detection off", params: ResendParams{}, entry: Entry{MessageID: "twin"}, want: false},
		{name: "new ids", params: ResendParams{Dedupe: enabled, NewMessageID: func() string { return "n" }}, entry: Entry{MessageID: "twin"}, want: false},
		{name: "no message id", params: ResendParams{Dedupe: enabled, Recent: map[string]struct{}{"": {}}}, entry: Entry{}, want: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.params.SkipsDuplicate(tc.entry, duplicates); got != tc.want {
				t.Errorf("SkipsDuplicate() = %v, want %v", got, tc.want)
			}
		})
	}
}

func bounceOf(seq int64, id string) *azservicebus.ReceivedMessage {
	message := dlMessage(seq, id, "", "MaxDeliveryCountExceeded")
	enqueued := runStart.Add(time.Minute)
	message.EnqueuedTime = &enqueued
	return message
}

// bounceWhenSent makes every message in a sent batch that matches pick come back to the queue.
func bounceWhenSent(queue *fakeQueue, pick func(line int) bool) func([]bus.Outgoing) {
	return func(batch []bus.Outgoing) {
		for _, outgoing := range batch {
			if pick(outgoing.Line) {
				queue.add(bounceOf(1000+int64(outgoing.Line), outgoing.MessageID))
			}
		}
	}
}

func sixMessageQueue() *fakeQueue {
	queue := newFakeQueue()
	for seq := int64(1); seq <= 6; seq++ {
		queue.add(dlMessage(seq, "m"+string(rune('0'+seq)), "", "r"))
	}
	return queue
}

func TestResendStopsBeforeTheNextWaveWhenTooManyBounce(t *testing.T) {
	queue := sixMessageQueue()
	f := newResendFixture(t, queue, Filter{})
	f.params.Wave, f.params.BatchSize, f.params.StopOnBounce = 2, 2, 50
	f.sender.onSend = bounceWhenSent(queue, func(line int) bool { return line == 3 || line == 4 })

	summary := f.run(t)

	if !summary.StoppedOnBounce || summary.Resent != 4 || summary.Bounced != 2 || summary.NotReached != 2 {
		t.Errorf("summary = %+v, want stopped after 4 resent with 2 bounced and 2 not reached", summary)
	}
	if len(f.sender.sent) != 4 {
		t.Errorf("sent %d, want the third wave never sent", len(f.sender.sent))
	}
	if code := summary.ExitCode(6); code != 3 {
		t.Errorf("ExitCode() = %d, want 3", code)
	}
}

func TestResendKeepsGoingWhenStopOnBounceIsOff(t *testing.T) {
	queue := sixMessageQueue()
	f := newResendFixture(t, queue, Filter{})
	f.params.Wave, f.params.BatchSize, f.params.StopOnBounce = 2, 2, 0
	f.sender.onSend = bounceWhenSent(queue, func(line int) bool { return line == 3 || line == 4 })

	summary := f.run(t)

	if summary.StoppedOnBounce || summary.Resent != 6 || summary.Bounced != 2 {
		t.Errorf("summary = %+v, want all 6 resent and 2 bounced counted", summary)
	}
	if code := summary.ExitCode(6); code != 3 {
		t.Errorf("ExitCode() = %d, want 3 because some bounced", code)
	}
}

func TestResendDoesNotMistakeTheOldCopyOfAFailedCompleteForABounce(t *testing.T) {
	queue := newFakeQueue(dlMessage(1, "a", "", "r"))
	queue.completeErr[1] = errBoom
	f := newResendFixture(t, queue, Filter{})
	f.params.Wave = 1

	summary := f.run(t)

	if summary.Bounced != 0 {
		t.Errorf("Bounced = %d, want 0: the old copy was enqueued before the run began", summary.Bounced)
	}
}

func TestResendWatchesForBouncesAfterTheLastWave(t *testing.T) {
	queue := newFakeQueue(dlMessage(1, "a", "", "r"), dlMessage(2, "b", "", "r"))
	f := newResendFixture(t, queue, Filter{})
	f.params.Watch = 12 * time.Second
	f.params.StopOnBounce = 0
	f.clock.onSleep = func() {
		if len(f.clock.sleeps) == 2 {
			queue.add(bounceOf(500, "b"))
		}
	}

	summary := f.run(t)

	want := []time.Duration{5 * time.Second, 5 * time.Second, 2 * time.Second}
	if !reflect.DeepEqual(f.clock.sleeps, want) {
		t.Errorf("sleeps = %v, want %v", f.clock.sleeps, want)
	}
	if summary.Bounced != 1 {
		t.Errorf("Bounced = %d, want the bounce that arrived during the watch", summary.Bounced)
	}
}

func TestResendWithoutWatchDoesNotSleep(t *testing.T) {
	queue := newFakeQueue(dlMessage(1, "a", "", "r"))
	f := newResendFixture(t, queue, Filter{})

	f.run(t)

	if len(f.clock.sleeps) != 0 {
		t.Errorf("sleeps = %v, want none without --watch", f.clock.sleeps)
	}
}

func TestResendCountsSelectedMessagesThatNeverShowUp(t *testing.T) {
	queue := newFakeQueue(dlMessage(1, "a", "", "r"))
	f := newResendFixture(t, queue, Filter{})
	f.params.Selection.Entries = append(f.params.Selection.Entries, Entry{SequenceNumber: 9, MessageID: "ghost"})

	summary := f.run(t)

	if summary.Resent != 1 || summary.NotReached != 1 {
		t.Errorf("summary = %+v, want 1 resent and 1 not reached", summary)
	}
	if code := summary.ExitCode(2); code != 3 {
		t.Errorf("ExitCode() = %d, want 3", code)
	}
}

func TestResendRefusesABodyOfSeveralDataSections(t *testing.T) {
	multi := dlMessage(1, "multi", "", "r")
	multi.Body = nil
	multi.RawAMQPMessage = &azservicebus.AMQPAnnotatedMessage{Body: azservicebus.AMQPAnnotatedMessageBody{Data: [][]byte{[]byte("a"), []byte("b")}}}
	queue := newFakeQueue(multi, dlMessage(2, "plain", "", "r"))
	f := newResendFixture(t, queue, Filter{})

	summary := f.run(t)

	if summary.Failed != 1 || summary.Resent != 1 {
		t.Errorf("summary = %+v, want the multi-section body failed and the other resent", summary)
	}
	if f.got[0].Err == nil || !strings.Contains(f.got[0].Err.Error(), "data sections") {
		t.Errorf("first result = %+v, want an error about data sections", f.got[0])
	}
	if len(f.sender.sent) != 1 {
		t.Errorf("sent %d messages, want the multi-section message never sent", len(f.sender.sent))
	}
}

func TestResendStopsOnALedgerWriteFailureAfterCompletingTheBatchInHand(t *testing.T) {
	queue := newFakeQueue(dlMessage(1, "a", "", "r"), dlMessage(2, "b", "", "r"))
	f := newResendFixture(t, queue, Filter{})
	_ = f.ledger.Close()

	_, err := Resend(context.Background(), f.deps, f.params, func(Result) error { return nil })

	if err == nil || !strings.Contains(err.Error(), "ledger") {
		t.Errorf("Resend() error = %v, want a ledger error", err)
	}
	if !reflect.DeepEqual(queue.completed, []int64{1, 2}) {
		t.Errorf("completed = %v, want both completed: they were sent already", queue.completed)
	}
}

func TestResendReleasesHeldMessagesWhenTheContextIsCancelled(t *testing.T) {
	queue := newFakeQueue(dlMessage(1, "a", "", "r"), dlMessage(2, "b", "", "r"))
	f := newResendFixture(t, queue, Filter{})
	ctx, cancel := context.WithCancel(context.Background())
	queue.onReceive = cancel
	f.deps.Pacer = bus.NewPacer(1)

	_, err := Resend(ctx, f.deps, f.params, func(Result) error { return nil })

	if !errors.Is(err, context.Canceled) {
		t.Errorf("Resend() error = %v, want context.Canceled", err)
	}
	if len(queue.completed) != 0 || len(f.sender.sent) != 0 {
		t.Errorf("completed %v, sent %d, want nothing done after the cancel", queue.completed, len(f.sender.sent))
	}
	if !reflect.DeepEqual(queue.abandoned, []int64{1, 2}) {
		t.Errorf("abandoned = %v, want both released on a fresh context", queue.abandoned)
	}
}

func TestResendStopsWhenTheResultCannotBeWritten(t *testing.T) {
	queue := newFakeQueue(dlMessage(1, "a", "", "r"), dlMessage(2, "b", "", "r"))
	f := newResendFixture(t, queue, Filter{})

	_, err := Resend(context.Background(), f.deps, f.params, func(Result) error { return errBoom })

	if !errors.Is(err, errBoom) || len(queue.completed) != 1 {
		t.Errorf("Resend() error = %v, completed %v, want %v after the first message", err, queue.completed, errBoom)
	}
}

func TestResendStopsWhenAReceiveFails(t *testing.T) {
	queue := newFakeQueue(dlMessage(1, "a", "", "r"))
	queue.receiveErr = errBoom
	f := newResendFixture(t, queue, Filter{})

	_, err := Resend(context.Background(), f.deps, f.params, func(Result) error { return nil })

	if !errors.Is(err, errBoom) {
		t.Errorf("Resend() error = %v, want %v", err, errBoom)
	}
}

func TestResendOfNothingReceivesNothingUseful(t *testing.T) {
	queue := newFakeQueue()
	f := newResendFixture(t, queue, Filter{})

	summary := f.run(t)

	if summary.Resent != 0 || summary.NotReached != 0 || queue.receives != 0 {
		t.Errorf("summary = %+v after %d receives, want nothing to do and no receive", summary, queue.receives)
	}
}

func TestResendExitCode(t *testing.T) {
	tests := []struct {
		name     string
		summary  ResendSummary
		selected int
		want     int
	}{
		{name: "nothing selected", summary: ResendSummary{}, selected: 0, want: 0},
		{name: "all resent", summary: ResendSummary{Resent: 5}, selected: 5, want: 0},
		{name: "none resent", summary: ResendSummary{Failed: 5}, selected: 5, want: 1},
		{name: "some resent", summary: ResendSummary{Resent: 3, Failed: 2}, selected: 5, want: 3},
		{name: "all resent but one bounced", summary: ResendSummary{Resent: 5, Bounced: 1}, selected: 5, want: 3},
		{name: "all resent but stopped", summary: ResendSummary{Resent: 5, StoppedOnBounce: true}, selected: 5, want: 3},
		{name: "sent but not completed counts as not resent", summary: ResendSummary{Resent: 4, SentNotCompleted: 1}, selected: 5, want: 3},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.summary.ExitCode(tc.selected); got != tc.want {
				t.Errorf("ExitCode(%d) = %d, want %d", tc.selected, got, tc.want)
			}
		})
	}
}

func TestResendLooksForBouncesInTheLastPartialWave(t *testing.T) {
	queue := newFakeQueue(dlMessage(1, "a", "", "r"), dlMessage(2, "b", "", "r"))
	f := newResendFixture(t, queue, Filter{})
	f.params.StopOnBounce = 0
	f.sender.onSend = bounceWhenSent(queue, func(line int) bool { return line == 2 })

	summary := f.run(t)

	if summary.Bounced != 1 {
		t.Errorf("Bounced = %d, want the bounce from a wave that never filled up", summary.Bounced)
	}
}

func TestResendScansOncePerWaveAndNotAgainAfterAFullWave(t *testing.T) {
	queue := newFakeQueue(dlMessage(1, "a", "", "r"), dlMessage(2, "b", "", "r"))
	f := newResendFixture(t, queue, Filter{})
	f.params.Wave, f.params.BatchSize = 2, 2
	queue.peeks = 0

	f.run(t)

	if queue.peeks != 1 {
		t.Errorf("peeks = %d, want 1: the full wave was scanned and nothing was left over", queue.peeks)
	}
}

func TestResendEndsAWaveAsSoonAsItIsFull(t *testing.T) {
	queue := sixMessageQueue()
	f := newResendFixture(t, queue, Filter{})
	f.params.Wave, f.params.BatchSize, f.params.StopOnBounce = 2, 2, 50
	f.sender.onSend = bounceWhenSent(queue, func(line int) bool { return line <= 2 })

	summary := f.run(t)

	if !summary.StoppedOnBounce || summary.Resent != 2 || summary.NotReached != 4 {
		t.Errorf("summary = %+v, want the run stopped after the first full wave of 2", summary)
	}
}

func TestResendStartsTheBounceScanAfterWhatTheSelectionRead(t *testing.T) {
	queue := noiseQueue(5, "wanted")
	f := newResendFixture(t, queue, Filter{PayloadTypeID: "Order"})
	queue.peekFroms = nil

	f.run(t)

	if len(queue.peekFroms) == 0 || queue.peekFroms[0] != 7 {
		t.Errorf("bounce scan peeked from %v, want it to start at 7, past the 6 messages the selection read", queue.peekFroms)
	}
}

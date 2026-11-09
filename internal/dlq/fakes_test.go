package dlq

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"path/filepath"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
	"github.com/marcinkaczmarek10/asbctl/internal/bus"
)

var runStart = time.Date(2026, 11, 5, 12, 0, 0, 0, time.UTC)

func ptr[T any](v T) *T { return &v }

// dlMessage is a dead-lettered message with the dead-letter properties the service adds.
func dlMessage(seq int64, id, payloadTypeID, reason string) *azservicebus.ReceivedMessage {
	properties := map[string]any{"DeadLetterReason": reason, "DeadLetterErrorDescription": "boom", "kept": "yes"}
	if payloadTypeID != "" {
		properties["PayloadTypeId"] = payloadTypeID
	}
	enqueued := runStart.Add(-time.Hour)
	return &azservicebus.ReceivedMessage{
		SequenceNumber:        &seq,
		MessageID:             id,
		EnqueuedTime:          &enqueued,
		DeadLetterReason:      &reason,
		ApplicationProperties: properties,
		Body:                  []byte("body-" + id),
	}
}

// fakeQueue is a dead-letter queue. Receive hands out unlocked messages in sequence order and locks them,
// Abandon unlocks, Complete removes, and Peek reads everything that is still in the queue.
//
// Receive models the SDK's link credit: a request tops the open credit up to its size and every delivered
// message uses one. A message abandoned while credit is open is redelivered into it, as the broker does, and
// stays locked: those are recorded in stuck.
type fakeQueue struct {
	messages    []*azservicebus.ReceivedMessage
	locked      map[int64]bool
	completed   []int64
	abandoned   []int64
	completeErr map[int64]error
	abandonErr  map[int64]error
	receives    int
	requests    []int
	credits     int
	stuck       []int64
	peeks       int
	peekFroms   []int64
	receiveErr  error
	onReceive   func()
}

func newFakeQueue(messages ...*azservicebus.ReceivedMessage) *fakeQueue {
	return &fakeQueue{messages: messages, locked: map[int64]bool{}, completeErr: map[int64]error{}, abandonErr: map[int64]error{}}
}

func (q *fakeQueue) add(message *azservicebus.ReceivedMessage) {
	q.messages = append(q.messages, message)
}

func (q *fakeQueue) ReceiveMessages(_ context.Context, maxMessages int, _ *azservicebus.ReceiveMessagesOptions) ([]*azservicebus.ReceivedMessage, error) {
	q.receives++
	if q.onReceive != nil {
		q.onReceive()
	}
	if q.receiveErr != nil {
		return nil, q.receiveErr
	}
	q.requests = append(q.requests, maxMessages)
	q.credits = max(q.credits, maxMessages)
	var batch []*azservicebus.ReceivedMessage
	for _, message := range q.messages {
		if len(batch) == q.credits {
			break
		}
		if !q.locked[*message.SequenceNumber] {
			q.locked[*message.SequenceNumber] = true
			batch = append(batch, message)
		}
	}
	q.credits -= len(batch)
	return batch, nil
}

func (q *fakeQueue) CompleteMessage(_ context.Context, message *azservicebus.ReceivedMessage, _ *azservicebus.CompleteMessageOptions) error {
	seq := *message.SequenceNumber
	if err := q.completeErr[seq]; err != nil {
		return err
	}
	q.completed = append(q.completed, seq)
	q.messages = removeSequence(q.messages, seq)
	delete(q.locked, seq)
	return nil
}

func (q *fakeQueue) AbandonMessage(_ context.Context, message *azservicebus.ReceivedMessage, _ *azservicebus.AbandonMessageOptions) error {
	seq := *message.SequenceNumber
	if err := q.abandonErr[seq]; err != nil {
		return err
	}
	q.abandoned = append(q.abandoned, seq)
	if q.credits > 0 {
		q.credits--
		q.stuck = append(q.stuck, seq)
		return nil
	}
	delete(q.locked, seq)
	return nil
}

func (q *fakeQueue) PeekMessages(_ context.Context, count int, options *azservicebus.PeekMessagesOptions) ([]*azservicebus.ReceivedMessage, error) {
	q.peeks++
	from := int64(0)
	if options.FromSequenceNumber != nil {
		from = *options.FromSequenceNumber
	}
	q.peekFroms = append(q.peekFroms, from)
	var page []*azservicebus.ReceivedMessage
	for _, message := range q.messages {
		if len(page) == count {
			break
		}
		if *message.SequenceNumber >= from {
			page = append(page, message)
		}
	}
	return page, nil
}

func removeSequence(messages []*azservicebus.ReceivedMessage, seq int64) []*azservicebus.ReceivedMessage {
	kept := messages[:0:0]
	for _, message := range messages {
		if *message.SequenceNumber != seq {
			kept = append(kept, message)
		}
	}
	return kept
}

// fakeSender records what was sent and fails the messages whose sequence number (Outgoing.Line) is listed.
type fakeSender struct {
	sent    []bus.Outgoing
	failing map[int]error
	onSend  func([]bus.Outgoing)
}

func (s *fakeSender) SendMessages(_ context.Context, messages []bus.Outgoing) []error {
	errs := make([]error, len(messages))
	for i, message := range messages {
		errs[i] = s.failing[message.Line]
		if errs[i] == nil {
			s.sent = append(s.sent, message)
		}
	}
	if s.onSend != nil {
		s.onSend(messages)
	}
	return errs
}

// fakeClock advances only when something sleeps.
type fakeClock struct {
	now     time.Time
	sleeps  []time.Duration
	onSleep func()
}

func (c *fakeClock) Now() time.Time { return c.now }

func (c *fakeClock) Sleep(_ context.Context, d time.Duration) error {
	c.sleeps = append(c.sleeps, d)
	c.now = c.now.Add(d)
	if c.onSleep != nil {
		c.onSleep()
	}
	return nil
}

// selectionOf builds a selection from the messages of a queue that pass the filter.
func selectionOf(t *testing.T, queue *fakeQueue, filter Filter) Selection {
	t.Helper()
	selection, err := Select(context.Background(), queue, filter, 0, func(*azservicebus.ReceivedMessage) error { return nil })
	if err != nil {
		t.Fatalf("Select() error = %v", err)
	}
	return selection
}

func openTestLedger(t *testing.T) (*Ledger, string) {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "state")
	ledger, err := OpenLedger(dir, runStart)
	if err != nil {
		t.Fatalf("OpenLedger() error = %v", err)
	}
	t.Cleanup(func() { _ = ledger.Close() })
	return ledger, dir
}

// resendFixture wires a queue, a sender and a ledger to a Resend call.
type resendFixture struct {
	queue  *fakeQueue
	sender *fakeSender
	clock  *fakeClock
	ledger *Ledger
	dir    string
	deps   ResendDeps
	params ResendParams
	got    []Result
}

func newResendFixture(t *testing.T, queue *fakeQueue, filter Filter) *resendFixture {
	t.Helper()
	ledger, dir := openTestLedger(t)
	f := &resendFixture{queue: queue, sender: &fakeSender{failing: map[int]error{}}, clock: &fakeClock{now: runStart}, ledger: ledger, dir: dir}
	f.deps = ResendDeps{Receiver: queue, Sender: f.sender, Peeker: queue, Ledger: ledger, Now: f.clock.Now, Sleep: f.clock.Sleep, Logger: discardLogger}
	f.params = ResendParams{
		Selection: selectionOf(t, queue, filter), Snapshot: "snap.ndjson", Namespace: "ns.servicebus.windows.net", Target: "queue:orders",
		Wave: 100, BatchSize: 100, StopOnBounce: 5,
	}
	return f
}

func (f *resendFixture) run(t *testing.T) ResendSummary {
	t.Helper()
	summary, err := Resend(context.Background(), f.deps, f.params, func(result Result) error {
		f.got = append(f.got, result)
		return nil
	})
	if err != nil {
		t.Fatalf("Resend() error = %v", err)
	}
	return summary
}

func (f *resendFixture) statuses() []string {
	statuses := make([]string, len(f.got))
	for i, result := range f.got {
		statuses[i] = fmt.Sprintf("%d:%s", result.SequenceNumber, result.Status)
	}
	return statuses
}

var errBoom = errors.New("boom")

var discardLogger = slog.New(slog.DiscardHandler)

package bus

import (
	"context"
	"errors"
	"iter"
	"reflect"
	"strings"
	"testing"
	"time"
)

type fakeMessageSender struct {
	batches [][]int
	failing map[int]error
	onSend  func()
}

func (f *fakeMessageSender) SendMessages(_ context.Context, messages []Outgoing) []error {
	lines := make([]int, len(messages))
	errs := make([]error, len(messages))
	for i, message := range messages {
		lines[i] = message.Line
		errs[i] = f.failing[message.Line]
	}
	f.batches = append(f.batches, lines)
	if f.onSend != nil {
		f.onSend()
	}
	return errs
}

func outgoings(count int) iter.Seq2[Outgoing, error] {
	return func(yield func(Outgoing, error) bool) {
		for line := 1; line <= count; line++ {
			if !yield(Outgoing{Line: line, MessageID: "m"}, nil) {
				return
			}
		}
	}
}

func collect(results *[]SendResult) func(SendResult) error {
	return func(result SendResult) error {
		*results = append(*results, result)
		return nil
	}
}

func TestSendGroupsMessagesIntoBatches(t *testing.T) {
	sender := &fakeMessageSender{}
	var results []SendResult

	counts, err := Send(context.Background(), sender, nil, 3, outgoings(7), collect(&results))

	if err != nil {
		t.Fatalf("Send() error = %v", err)
	}
	wantBatches := [][]int{{1, 2, 3}, {4, 5, 6}, {7}}
	if !reflect.DeepEqual(sender.batches, wantBatches) {
		t.Errorf("batches = %v, want %v", sender.batches, wantBatches)
	}
	if counts != (SendCounts{Sent: 7}) || len(results) != 7 {
		t.Errorf("counts = %+v with %d results, want 7 sent and 7 results", counts, len(results))
	}
	for i, result := range results {
		if result.Line != i+1 || result.Err != nil {
			t.Errorf("result %d = %+v, want line %d and no error", i, result, i+1)
		}
	}
}

func TestSendReportsEachFailureAndKeepsGoing(t *testing.T) {
	boom := errors.New("too large")
	sender := &fakeMessageSender{failing: map[int]error{2: boom, 5: boom}}
	var results []SendResult

	counts, err := Send(context.Background(), sender, nil, 3, outgoings(5), collect(&results))

	if err != nil {
		t.Fatalf("Send() error = %v", err)
	}
	if counts != (SendCounts{Sent: 3, Failed: 2}) {
		t.Errorf("counts = %+v, want 3 sent and 2 failed", counts)
	}
	var failedLines []int
	for _, result := range results {
		if result.Err != nil {
			failedLines = append(failedLines, result.Line)
		}
	}
	if !reflect.DeepEqual(failedLines, []int{2, 5}) {
		t.Errorf("failed lines = %v, want [2 5]", failedLines)
	}
}

func TestSendOfNothingSendsNothing(t *testing.T) {
	sender := &fakeMessageSender{}

	counts, err := Send(context.Background(), sender, nil, 3, outgoings(0), collect(new([]SendResult)))

	if err != nil || counts != (SendCounts{}) || len(sender.batches) != 0 {
		t.Errorf("Send() = %+v, %v with %d batches, want zero counts, nil and no batches", counts, err, len(sender.batches))
	}
}

func TestSendStopsOnAnItemError(t *testing.T) {
	sender := &fakeMessageSender{}
	parseErr := errors.New("line 3: bad record")
	items := func(yield func(Outgoing, error) bool) {
		_ = yield(Outgoing{Line: 1}, nil) && yield(Outgoing{Line: 2}, nil) && yield(Outgoing{}, parseErr)
	}

	counts, err := Send(context.Background(), sender, nil, 10, items, collect(new([]SendResult)))

	if !errors.Is(err, parseErr) {
		t.Errorf("Send() error = %v, want %v", err, parseErr)
	}
	if counts.Sent != 0 || len(sender.batches) != 0 {
		t.Errorf("sent %d in %d batches, want nothing sent when the input breaks mid-batch", counts.Sent, len(sender.batches))
	}
}

func TestSendStopsWhenTheContextIsCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	sender := &fakeMessageSender{onSend: cancel}
	var results []SendResult

	counts, err := Send(ctx, sender, nil, 2, outgoings(10), collect(&results))

	if !errors.Is(err, context.Canceled) {
		t.Errorf("Send() error = %v, want context.Canceled", err)
	}
	if counts.Sent != 2 || len(sender.batches) != 1 || len(results) != 2 {
		t.Errorf("counts = %+v, batches = %d, results = %d, want the first batch reported and no more", counts, len(sender.batches), len(results))
	}
}

func TestSendStopsWhenOnResultFails(t *testing.T) {
	sender := &fakeMessageSender{}
	writeErr := errors.New("broken pipe")

	_, err := Send(context.Background(), sender, nil, 5, outgoings(10), func(SendResult) error { return writeErr })

	if !errors.Is(err, writeErr) || len(sender.batches) != 1 {
		t.Errorf("Send() error = %v after %d batches, want %v after 1", err, len(sender.batches), writeErr)
	}
}

func TestBatchSize(t *testing.T) {
	tests := []struct{ rate, want int }{{0, 100}, {-5, 100}, {1, 1}, {20, 20}, {100, 100}, {5000, 100}}
	for _, tc := range tests {
		if got := BatchSize(tc.rate); got != tc.want {
			t.Errorf("BatchSize(%d) = %d, want %d", tc.rate, got, tc.want)
		}
	}
}

// fakeClock moves only when the pacer sleeps.
type fakeClock struct {
	now    time.Time
	sleeps []time.Duration
}

func (c *fakeClock) pacer(rate int) *Pacer {
	return &Pacer{
		rate: rate,
		now:  func() time.Time { return c.now },
		sleep: func(_ context.Context, d time.Duration) error {
			c.sleeps = append(c.sleeps, d)
			c.now = c.now.Add(d)
			return nil
		},
	}
}

func TestPacerSpacesBatchesByTheirSizeOverTheRate(t *testing.T) {
	clock := &fakeClock{now: time.Unix(1000, 0)}
	pacer := clock.pacer(10)

	for range 3 {
		if err := pacer.Wait(context.Background(), 5); err != nil {
			t.Fatalf("Wait() error = %v", err)
		}
	}

	want := []time.Duration{500 * time.Millisecond, 500 * time.Millisecond}
	if !reflect.DeepEqual(clock.sleeps, want) {
		t.Errorf("sleeps = %v, want %v: the first batch goes at once, then 5 messages per half second", clock.sleeps, want)
	}
}

func TestPacerDoesNotSaveUpIdleTime(t *testing.T) {
	clock := &fakeClock{now: time.Unix(1000, 0)}
	pacer := clock.pacer(10)
	_ = pacer.Wait(context.Background(), 10)

	clock.now = clock.now.Add(time.Hour)
	_ = pacer.Wait(context.Background(), 10)
	_ = pacer.Wait(context.Background(), 10)

	want := []time.Duration{time.Second}
	if !reflect.DeepEqual(clock.sleeps, want) {
		t.Errorf("sleeps = %v, want %v: after an idle hour the next batch goes at once and only the one after waits", clock.sleeps, want)
	}
}

func TestPacerReturnsTheContextError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if err := NewPacer(10).Wait(ctx, 1); !errors.Is(err, context.Canceled) {
		t.Errorf("Wait() on a cancelled context = %v, want context.Canceled", err)
	}
	if err := (*Pacer)(nil).Wait(ctx, 1); !errors.Is(err, context.Canceled) {
		t.Errorf("nil Pacer Wait() on a cancelled context = %v, want context.Canceled", err)
	}
	if NewPacer(0) != nil || NewPacer(-1) != nil {
		t.Error("NewPacer(0 or less) is not nil, want nil for unlimited")
	}
}

func TestPacerSleepStopsOnCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := sleepContext(ctx, time.Hour)

	if !errors.Is(err, context.Canceled) {
		t.Errorf("sleepContext() = %v, want context.Canceled", err)
	}
	if err := sleepContext(context.Background(), time.Millisecond); err != nil {
		t.Errorf("sleepContext(1ms) = %v, want nil", err)
	}
}

func TestNewMessageIDIsAVersion4UUID(t *testing.T) {
	first, second := NewMessageID(), NewMessageID()

	if first == second {
		t.Errorf("NewMessageID() repeated %q", first)
	}
	parts := strings.Split(first, "-")
	if len(first) != 36 || len(parts) != 5 || parts[2][0] != '4' || !strings.Contains("89ab", parts[3][:1]) {
		t.Errorf("NewMessageID() = %q, want a version 4 UUID", first)
	}
}

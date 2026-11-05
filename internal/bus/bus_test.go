package bus

import (
	"context"
	"errors"
	"slices"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
)

type peekCall struct {
	count        int
	fromSequence *int64
}

// fakePeeker serves messages numbered 1..total and honours the requested start and count.
type fakePeeker struct {
	total   int64
	calls   []peekCall
	onPeek  func()
	peekErr error
}

func (f *fakePeeker) PeekMessages(ctx context.Context, count int, options *azservicebus.PeekMessagesOptions) ([]*azservicebus.ReceivedMessage, error) {
	f.calls = append(f.calls, peekCall{count: count, fromSequence: options.FromSequenceNumber})
	if f.onPeek != nil {
		f.onPeek()
	}
	if f.peekErr != nil {
		return nil, f.peekErr
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	start := int64(1)
	if options.FromSequenceNumber != nil {
		start = *options.FromSequenceNumber
	}
	var page []*azservicebus.ReceivedMessage
	for seq := start; seq <= f.total && len(page) < count; seq++ {
		page = append(page, &azservicebus.ReceivedMessage{SequenceNumber: &seq})
	}
	return page, nil
}

func sequencesOf(pages [][]*azservicebus.ReceivedMessage) []int64 {
	var sequences []int64
	for _, page := range pages {
		for _, message := range page {
			sequences = append(sequences, *message.SequenceNumber)
		}
	}
	return sequences
}

func TestFullyQualifiedNamespace(t *testing.T) {
	tests := []struct {
		in   string
		want string
	}{
		{in: "contoso", want: "contoso.servicebus.windows.net"},
		{in: "contoso.servicebus.windows.net", want: "contoso.servicebus.windows.net"},
		{in: "contoso.servicebus.usgovcloudapi.net", want: "contoso.servicebus.usgovcloudapi.net"},
	}
	for _, tc := range tests {
		t.Run(tc.in, func(t *testing.T) {
			if got := FullyQualifiedNamespace(tc.in); got != tc.want {
				t.Errorf("FullyQualifiedNamespace(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

func TestPeekPagesThroughTheEntityAndStopsAtMax(t *testing.T) {
	tests := []struct {
		name          string
		total         int64
		options       PeekOptions
		wantCounts    []int
		wantFirstSeqs []int64
		wantPeeked    int
	}{
		{name: "max below one page asks for exactly max", total: 500, options: PeekOptions{Max: 30}, wantCounts: []int{30}, wantPeeked: 30},
		{name: "max above one page shrinks the last page", total: 500, options: PeekOptions{Max: 250}, wantCounts: []int{100, 100, 50}, wantPeeked: 250},
		{name: "entity smaller than max ends on an empty page", total: 120, options: PeekOptions{Max: 500}, wantCounts: []int{100, 100, 100}, wantPeeked: 120},
		{name: "empty entity makes one call", total: 0, options: PeekOptions{Max: 50}, wantCounts: []int{50}, wantPeeked: 0},
		{name: "from sequence starts there and pages on from the last sequence", total: 500, options: PeekOptions{Max: 150, FromSequence: 201}, wantCounts: []int{100, 50}, wantFirstSeqs: []int64{201, 301}, wantPeeked: 150},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fake := &fakePeeker{total: tc.total}
			var pages [][]*azservicebus.ReceivedMessage

			peeked, err := Peek(context.Background(), fake, tc.options, func(page []*azservicebus.ReceivedMessage) error {
				pages = append(pages, page)
				return nil
			})

			if err != nil {
				t.Fatalf("Peek() error = %v, want nil", err)
			}
			if peeked != tc.wantPeeked {
				t.Errorf("Peek() peeked = %d, want %d", peeked, tc.wantPeeked)
			}
			var gotCounts []int
			for _, call := range fake.calls {
				gotCounts = append(gotCounts, call.count)
			}
			if !slices.Equal(gotCounts, tc.wantCounts) {
				t.Errorf("peek page sizes = %v, want %v", gotCounts, tc.wantCounts)
			}
			for i, want := range tc.wantFirstSeqs {
				got := fake.calls[i].fromSequence
				if got == nil || *got != want {
					t.Errorf("peek call %d from sequence = %v, want %d", i, got, want)
				}
			}
			sequences := sequencesOf(pages)
			for i := 1; i < len(sequences); i++ {
				if sequences[i] != sequences[i-1]+1 {
					t.Fatalf("sequences %v have a gap or repeat at index %d", sequences, i)
				}
			}
		})
	}
}

func TestPeekStopsWhenTheContextIsCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	fake := &fakePeeker{total: 500, onPeek: cancel}
	pageCount := 0

	_, err := Peek(ctx, fake, PeekOptions{Max: 500}, func([]*azservicebus.ReceivedMessage) error {
		pageCount++
		return nil
	})

	if !errors.Is(err, context.Canceled) {
		t.Errorf("Peek() error = %v, want context.Canceled", err)
	}
	if len(fake.calls) != 1 {
		t.Errorf("peek calls after cancel = %d, want 1", len(fake.calls))
	}
	if pageCount != 0 {
		t.Errorf("pages delivered after cancel = %d, want 0", pageCount)
	}
}

func TestPeekStopsAfterTheFirstPageWhenTheContextIsCancelledBetweenPages(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	fake := &fakePeeker{total: 500}

	peeked, err := Peek(ctx, fake, PeekOptions{Max: 500}, func([]*azservicebus.ReceivedMessage) error {
		cancel()
		return nil
	})

	if !errors.Is(err, context.Canceled) {
		t.Errorf("Peek() error = %v, want context.Canceled", err)
	}
	if peeked != 100 || len(fake.calls) != 1 {
		t.Errorf("Peek() peeked %d in %d calls, want 100 in 1", peeked, len(fake.calls))
	}
}

func TestPeekFailsWhenAMessageHasNoSequenceNumber(t *testing.T) {
	peeker := peekerFunc(func() []*azservicebus.ReceivedMessage { return []*azservicebus.ReceivedMessage{{}} })

	_, err := Peek(context.Background(), peeker, PeekOptions{Max: 10}, func([]*azservicebus.ReceivedMessage) error { return nil })

	if err == nil {
		t.Error("Peek() error = nil, want an error for a message without a sequence number")
	}
}

type peekerFunc func() []*azservicebus.ReceivedMessage

func (f peekerFunc) PeekMessages(context.Context, int, *azservicebus.PeekMessagesOptions) ([]*azservicebus.ReceivedMessage, error) {
	return f(), nil
}

func TestPeekReturnsThePeekAndPageErrors(t *testing.T) {
	peekErr := errors.New("service unavailable")
	pageErr := errors.New("write failed")

	_, err := Peek(context.Background(), &fakePeeker{peekErr: peekErr}, PeekOptions{Max: 10}, func([]*azservicebus.ReceivedMessage) error { return nil })
	if !errors.Is(err, peekErr) {
		t.Errorf("Peek() error = %v, want it to wrap %v", err, peekErr)
	}

	calls := 0
	fake := &fakePeeker{total: 500}
	_, err = Peek(context.Background(), fake, PeekOptions{Max: 500}, func([]*azservicebus.ReceivedMessage) error {
		calls++
		return pageErr
	})
	if !errors.Is(err, pageErr) {
		t.Errorf("Peek() error = %v, want %v", err, pageErr)
	}
	if calls != 1 || len(fake.calls) != 1 {
		t.Errorf("page callbacks = %d, peek calls = %d, want 1 and 1", calls, len(fake.calls))
	}
}

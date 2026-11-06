package dlq

import (
	"context"
	"math"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
	"github.com/marcinkaczmarek10/asbctl/internal/bus"
	"github.com/marcinkaczmarek10/asbctl/internal/export"
)

// bounceClockSkew is how much earlier than the run start a dead-letter time may be and still count, because the
// service clock and the local clock need not agree.
const bounceClockSkew = time.Minute

// BounceScanner finds resent messages that came back to the dead-letter queue.
//
// It reads forward from a cursor, so each scan only looks at what is new. That relies on a message that is
// dead-lettered again getting a higher sequence number than everything already in the queue.
type BounceScanner struct {
	peeker  bus.Peeker
	since   time.Time
	cursor  int64
	resent  map[string]struct{}
	bounced map[string]struct{}
}

// NewBounceScanner returns a scanner that ignores messages enqueued before since, less the clock skew allowance, and
// starts reading at sequence number from: a bounce is dead-lettered after the selection read the queue, so it
// gets a higher sequence number than anything the selection saw.
func NewBounceScanner(peeker bus.Peeker, since time.Time, from int64) *BounceScanner {
	return &BounceScanner{
		peeker:  peeker,
		since:   since.Add(-bounceClockSkew),
		cursor:  from,
		resent:  make(map[string]struct{}),
		bounced: make(map[string]struct{}),
	}
}

// Track records a message that was resent. originalID is the id it had in the queue, and differs from sentID
// when a new id was assigned.
func (b *BounceScanner) Track(sentID, originalID string) {
	for _, id := range []string{sentID, originalID} {
		if id != "" {
			b.resent[id] = struct{}{}
		}
	}
}

// Bounced returns how many resent messages have come back so far.
func (b *BounceScanner) Bounced() int {
	return len(b.bounced)
}

// Scan reads the part of the queue that it has not read yet and returns how many new bounces it found.
func (b *BounceScanner) Scan(ctx context.Context) (int, error) {
	before := len(b.bounced)
	options := bus.PeekOptions{FromSequence: b.cursor, Max: math.MaxInt}
	_, err := bus.Peek(ctx, b.peeker, options, func(page []*azservicebus.ReceivedMessage) error {
		for _, message := range page {
			if message.SequenceNumber != nil {
				b.cursor = max(b.cursor, *message.SequenceNumber+1)
			}
			if id, ok := bounceID(message, b.resent, b.since); ok {
				b.bounced[id] = struct{}{}
			}
		}
		return nil
	})
	return len(b.bounced) - before, err
}

// bounceID reports whether a message in the dead-letter queue is one that was resent, and under which id. It is
// one when it was enqueued since the run began and its MessageId, or the original id it carries, was resent.
func bounceID(message *azservicebus.ReceivedMessage, resent map[string]struct{}, since time.Time) (string, bool) {
	if message.EnqueuedTime == nil || message.EnqueuedTime.Before(since) {
		return "", false
	}
	if _, ok := resent[message.MessageID]; ok {
		return message.MessageID, true
	}
	original, _ := message.ApplicationProperties[export.OriginalMessageIDProperty].(string)
	if _, ok := resent[original]; ok && original != "" {
		return original, true
	}
	return "", false
}

// BounceShare is the percentage of resent messages that came back.
func BounceShare(bounced, resent int) float64 {
	if resent == 0 {
		return 0
	}
	return float64(bounced) * 100 / float64(resent)
}

// ShouldStopOnBounce is true when stopAtPercent is set and the bounce share has reached it.
func ShouldStopOnBounce(stopAtPercent float64, bounced, resent int) bool {
	return stopAtPercent > 0 && resent > 0 && BounceShare(bounced, resent) >= stopAtPercent
}

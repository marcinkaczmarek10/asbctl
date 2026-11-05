package bus

import (
	"context"
	"crypto/rand"
	"fmt"
	"iter"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
)

const (
	// MaxBatchSize is the most messages one send call carries.
	MaxBatchSize = 100

	sendTimeout = 60 * time.Second
)

// Outgoing is one message to send. Exactly one of Message and Annotated is set: Message for a data body,
// Annotated for an AMQP value or sequence body, which Message cannot carry.
type Outgoing struct {
	Line          int
	MessageID     string
	PayloadTypeID string
	Message       *azservicebus.Message
	Annotated     *azservicebus.AMQPAnnotatedMessage
}

func (o Outgoing) addTo(batch *azservicebus.MessageBatch) error {
	if o.Annotated != nil {
		return batch.AddAMQPAnnotatedMessage(o.Annotated, nil)
	}
	return batch.AddMessage(o.Message, nil)
}

// MessageSender sends messages and reports one error per message, nil for a message that was sent.
type MessageSender interface {
	SendMessages(ctx context.Context, messages []Outgoing) []error
}

var _ MessageSender = (*Sender)(nil)

// Sender sends to one queue or topic.
type Sender struct {
	sender *azservicebus.Sender
}

// NewSender opens a sender on a queue or a topic.
func (c *Client) NewSender(queueOrTopic string) (*Sender, error) {
	sender, err := c.client.NewSender(queueOrTopic, nil)
	if err != nil {
		return nil, err
	}
	return &Sender{sender: sender}, nil
}

// Close closes the sender.
func (s *Sender) Close(ctx context.Context) error {
	return s.sender.Close(ctx)
}

// SendMessages sends the messages in as few batches as the service size limit allows. A batch that fails
// fails every message in it, and a message too large for any batch fails alone.
func (s *Sender) SendMessages(ctx context.Context, messages []Outgoing) []error {
	errs := make([]error, len(messages))
	for start := 0; start < len(messages); {
		batch, err := s.sender.NewMessageBatch(ctx, nil)
		if err != nil {
			failFrom(errs, start, fmt.Errorf("create batch: %w", err))
			return errs
		}

		end := start
		for ; end < len(messages); end++ {
			if err := messages[end].addTo(batch); err != nil {
				if end == start {
					errs[end] = err
					end++
				}
				break
			}
		}

		// A batch holds messages [start, end) except when its first message alone was rejected.
		if batch.NumMessages() > 0 {
			if err := s.sender.SendMessageBatch(ctx, batch, nil); err != nil {
				for i := start; i < end; i++ {
					if errs[i] == nil {
						errs[i] = fmt.Errorf("send batch: %w", err)
					}
				}
			}
		}
		start = end
	}
	return errs
}

func failFrom(errs []error, start int, err error) {
	for i := start; i < len(errs); i++ {
		errs[i] = err
	}
}

// NewMessageID returns a random version 4 UUID in its text form.
func NewMessageID() string {
	var id [16]byte
	_, _ = rand.Read(id[:]) // crypto/rand.Read does not return an error since Go 1.24.
	id[6] = id[6]&0x0f | 0x40
	id[8] = id[8]&0x3f | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", id[0:4], id[4:6], id[6:8], id[8:10], id[10:16])
}

// BatchSize returns how many messages to send per call. At a rate it is at most one second of messages.
func BatchSize(rate int) int {
	if rate <= 0 {
		return MaxBatchSize
	}
	return min(MaxBatchSize, rate)
}

// SendResult is the outcome of one message. A nil Err means it was sent.
type SendResult struct {
	Line      int
	MessageID string
	Err       error
}

// SendCounts counts what a Send run did.
type SendCounts struct {
	Sent   int
	Failed int
}

// Send sends the items in batches of batchSize, waits on the pacer before each batch, and reports each
// message to onResult. It stops at the first error from items or onResult, and when ctx ends.
func Send(
	ctx context.Context,
	sender MessageSender,
	pacer *Pacer,
	batchSize int,
	items iter.Seq2[Outgoing, error],
	onResult func(SendResult) error,
) (SendCounts, error) {
	var counts SendCounts
	batch := make([]Outgoing, 0, batchSize)

	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		if err := pacer.Wait(ctx, len(batch)); err != nil {
			return err
		}

		sendCtx, cancel := context.WithTimeout(ctx, sendTimeout)
		errs := sender.SendMessages(sendCtx, batch)
		cancel()

		for i, outgoing := range batch {
			result := SendResult{Line: outgoing.Line, MessageID: outgoing.MessageID, Err: errs[i]}
			if result.Err == nil {
				counts.Sent++
			} else {
				counts.Failed++
			}
			if err := onResult(result); err != nil {
				return err
			}
		}
		batch = batch[:0]
		return nil
	}

	for outgoing, err := range items {
		if err != nil {
			return counts, err
		}
		if err := ctx.Err(); err != nil {
			return counts, err
		}

		batch = append(batch, outgoing)
		if len(batch) == batchSize {
			if err := flush(); err != nil {
				return counts, err
			}
		}
	}
	return counts, flush()
}

// Pacer spaces sends so that they average a given number of messages per second. A nil Pacer does not wait.
type Pacer struct {
	rate  int
	next  time.Time
	now   func() time.Time
	sleep func(ctx context.Context, d time.Duration) error
}

// NewPacer returns a pacer for rate messages per second, or nil when rate is zero or less.
func NewPacer(rate int) *Pacer {
	if rate <= 0 {
		return nil
	}
	return &Pacer{rate: rate, now: time.Now, sleep: sleepContext}
}

// Wait blocks until n messages may be sent, or until ctx ends. Idle time is not saved up, so a pause
// does not allow a later burst.
func (p *Pacer) Wait(ctx context.Context, n int) error {
	if p == nil {
		return ctx.Err()
	}

	now := p.now()
	if p.next.Before(now) {
		p.next = now
	}
	wait := p.next.Sub(now)
	p.next = p.next.Add(time.Duration(n) * time.Second / time.Duration(p.rate))

	if wait <= 0 {
		return ctx.Err()
	}
	return p.sleep(ctx, wait)
}

func sleepContext(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

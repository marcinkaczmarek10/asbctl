// Package bus builds Service Bus clients and receivers and runs the peek loop.
package bus

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
)

const (
	// MaxPageSize is the largest message count one peek call asks for.
	MaxPageSize = 100

	namespaceSuffix = ".servicebus.windows.net"
	peekTimeout     = 30 * time.Second
)

// Target names the entity to read: a queue, or a topic subscription, optionally its dead-letter sub-queue.
type Target struct {
	Queue        string
	Topic        string
	Subscription string
	DeadLetter   bool
}

// Peeker is the one receiver method the peek loop calls.
type Peeker interface {
	PeekMessages(ctx context.Context, maxMessageCount int, options *azservicebus.PeekMessagesOptions) ([]*azservicebus.ReceivedMessage, error)
}

var _ Peeker = (*azservicebus.Receiver)(nil)

// Client wraps an azservicebus client that was opened for one command.
type Client struct {
	client *azservicebus.Client
}

// FullyQualifiedNamespace appends the public cloud suffix to a short namespace name.
func FullyQualifiedNamespace(namespace string) string {
	if strings.Contains(namespace, ".") {
		return namespace
	}
	return namespace + namespaceSuffix
}

// NewClient opens a client with the connection string when one is given, and with DefaultAzureCredential otherwise.
func NewClient(namespace, connectionString string) (*Client, error) {
	if connectionString != "" {
		client, err := azservicebus.NewClientFromConnectionString(connectionString, nil)
		if err != nil {
			return nil, fmt.Errorf("create client from connection string: %w", err)
		}
		return &Client{client: client}, nil
	}

	credential, err := azidentity.NewDefaultAzureCredential(nil)
	if err != nil {
		return nil, fmt.Errorf("create credential: %w", err)
	}
	client, err := azservicebus.NewClient(FullyQualifiedNamespace(namespace), credential, nil)
	if err != nil {
		return nil, fmt.Errorf("create client: %w", err)
	}
	return &Client{client: client}, nil
}

// NewReceiver opens a peek-lock receiver on the target. It never uses ReceiveAndDelete.
func (c *Client) NewReceiver(target Target) (*azservicebus.Receiver, error) {
	options := &azservicebus.ReceiverOptions{ReceiveMode: azservicebus.ReceiveModePeekLock}
	if target.DeadLetter {
		options.SubQueue = azservicebus.SubQueueDeadLetter
	}

	if target.Queue != "" {
		return c.client.NewReceiverForQueue(target.Queue, options)
	}
	return c.client.NewReceiverForSubscription(target.Topic, target.Subscription, options)
}

// Close closes the underlying client.
func (c *Client) Close(ctx context.Context) error {
	return c.client.Close(ctx)
}

// PeekOptions limits a peek run. A FromSequence of zero starts at the head of the entity.
type PeekOptions struct {
	FromSequence int64
	Max          int
}

// Peek reads up to options.Max messages in pages of at most MaxPageSize and hands each page to onPage.
// It stops at Max, at an empty page, on a context error or on an error from onPage.
func Peek(ctx context.Context, peeker Peeker, options PeekOptions, onPage func([]*azservicebus.ReceivedMessage) error) (int, error) {
	var fromSequence *int64
	if options.FromSequence > 0 {
		fromSequence = &options.FromSequence
	}

	peeked := 0
	for peeked < options.Max {
		if err := ctx.Err(); err != nil {
			return peeked, err
		}

		page, err := peekPage(ctx, peeker, min(MaxPageSize, options.Max-peeked), fromSequence)
		if err != nil {
			return peeked, err
		}
		if len(page) == 0 {
			return peeked, nil
		}

		peeked += len(page)
		if err := onPage(page); err != nil {
			return peeked, err
		}

		next, err := nextSequence(page)
		if err != nil {
			return peeked, err
		}
		fromSequence = &next
	}
	return peeked, nil
}

func peekPage(ctx context.Context, peeker Peeker, count int, fromSequence *int64) ([]*azservicebus.ReceivedMessage, error) {
	ctx, cancel := context.WithTimeout(ctx, peekTimeout)
	defer cancel()

	page, err := peeker.PeekMessages(ctx, count, &azservicebus.PeekMessagesOptions{FromSequenceNumber: fromSequence})
	if err != nil {
		return nil, fmt.Errorf("peek messages: %w", err)
	}
	return page, nil
}

func nextSequence(page []*azservicebus.ReceivedMessage) (int64, error) {
	last := page[len(page)-1].SequenceNumber
	if last == nil {
		return 0, errors.New("peeked message has no sequence number")
	}
	return *last + 1, nil
}

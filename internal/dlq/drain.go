package dlq

import (
	"context"
	"log/slog"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
	"github.com/marcinkaczmarek10/asbctl/internal/bus"
)

const (
	settleTimeout  = 30 * time.Second
	releaseTimeout = 60 * time.Second
)

// candidate is a locked message that the selection asked for.
type candidate struct {
	Entry   Entry
	Message *azservicebus.ReceivedMessage
}

// drainOptions tune a drain. MaxHeld caps how many messages a run holds locked, and zero means no cap.
type drainOptions struct {
	BatchSize int
	MaxHeld   int
	Logger    *slog.Logger
}

// drainResult says how many selected messages a drain handled, and whether it stopped at the hold cap.
type drainResult struct {
	Handled     int
	ScanLimited bool
}

// drain receives the dead-letter queue under peek-lock and hands the selected messages to process, batch by batch.
//
// A message that is not acted on stays locked until the run ends, and is only then abandoned. Abandoning at once
// would put it back at the head of the queue, and the next receive would return it again before any message
// behind it. The lock expires by itself after the lock duration, which bounds how far one run can read through
// messages it skips. The hold cap bounds the memory: the SDK keeps the whole received message behind a lock, body
// included, and offers no way to drop it. drain stops when every selected message is handled, when a receive
// returns nothing, when a receive returns only messages it has seen before, or when the hold cap is reached.
func drain(
	ctx context.Context,
	receiver bus.SettlingReceiver,
	options drainOptions,
	selected map[string][]Entry,
	process func(ctx context.Context, candidates []candidate, hold func(*azservicebus.ReceivedMessage)) (stop bool, err error),
) (result drainResult, err error) {
	var held []*azservicebus.ReceivedMessage
	defer func() { releaseHeld(ctx, receiver, options.Logger, held) }()
	hold := func(message *azservicebus.ReceivedMessage) { held = append(held, message) }

	total := 0
	for _, entries := range selected {
		total += len(entries)
	}
	matched := make(map[string]int)
	for result.Handled < total {
		batch, err := bus.ReceiveBatch(ctx, receiver, options.BatchSize)
		if err != nil {
			return result, err
		}

		candidates, sawNew := sortBatch(batch, selected, matched, hold)
		if !sawNew {
			return result, nil
		}
		result.Handled += len(candidates)

		if len(candidates) > 0 {
			stop, err := process(ctx, candidates, hold)
			if err != nil || stop {
				return result, err
			}
		}
		if options.MaxHeld > 0 && len(held) >= options.MaxHeld && result.Handled < len(selected) {
			result.ScanLimited = true
			return result, nil
		}
	}
	return result, nil
}

// sortBatch splits a received batch into the selected messages, which it returns, and the rest, which it holds.
// Matching is by MessageId because PeekMessages and ReceiveMessages return different sequence numbers on a
// dead-letter sub-queue. sawNew is true when any message was new.
func sortBatch(
	batch []*azservicebus.ReceivedMessage,
	selected map[string][]Entry,
	matched map[string]int,
	hold func(*azservicebus.ReceivedMessage),
) (candidates []candidate, sawNew bool) {
	for _, message := range batch {
		entry := EntryFromMessage(message)
		sawNew = true

		entries, wanted := selected[entry.MessageID]
		if !wanted || matched[entry.MessageID] >= len(entries) {
			hold(message)
			continue
		}
		selEntry := entries[matched[entry.MessageID]]
		matched[entry.MessageID]++
		candidates = append(candidates, candidate{Entry: selEntry, Message: message})
	}
	return candidates, sawNew
}

// releaseHeld abandons what a run held back. A lock that already expired makes Abandon fail, which is fine: the
// message is available again either way, so the failures are counted and logged once. It runs on its own deadline
// so that a cancelled run still releases.
func releaseHeld(ctx context.Context, receiver bus.SettlingReceiver, logger *slog.Logger, held []*azservicebus.ReceivedMessage) {
	releaseCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), releaseTimeout)
	defer cancel()

	failed := 0
	for _, message := range held {
		if releaseCtx.Err() != nil || receiver.AbandonMessage(releaseCtx, message, nil) != nil {
			failed++
		}
	}
	if failed > 0 {
		logger.WarnContext(ctx, "held messages were not abandoned and return when their locks expire", "failed", failed, "held", len(held))
	}
}

func complete(ctx context.Context, receiver bus.SettlingReceiver, message *azservicebus.ReceivedMessage) error {
	completeCtx, cancel := context.WithTimeout(ctx, settleTimeout)
	defer cancel()
	return receiver.CompleteMessage(completeCtx, message, nil)
}

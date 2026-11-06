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

// drainOptions tune a drain. MaxHeld caps how many messages a run holds locked, and zero means no cap. Budget is
// the most messages a run receives in total, and zero means no limit.
type drainOptions struct {
	BatchSize int
	MaxHeld   int
	Budget    int
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
// returns nothing, when a receive returns only messages it has seen before, when the hold cap is reached, or
// when it has received its budget.
//
// A receive never asks for more than the budget still allows. The SDK keeps the unused part of a request open on
// the link, and the broker redelivers the messages this run abandons into that open request; the ones in flight
// when the receiver closes stay locked for the whole lock duration, so a rerun finds nothing.
func drain(
	ctx context.Context,
	receiver bus.SettlingReceiver,
	options drainOptions,
	selected map[int64]Entry,
	process func(ctx context.Context, candidates []candidate, hold func(*azservicebus.ReceivedMessage)) (stop bool, err error),
) (result drainResult, err error) {
	var held []*azservicebus.ReceivedMessage
	defer func() { releaseHeld(ctx, receiver, options.Logger, held) }()
	hold := func(message *azservicebus.ReceivedMessage) { held = append(held, message) }

	seen := make(map[int64]struct{})
	received := 0
	for result.Handled < len(selected) {
		count := options.BatchSize
		if options.Budget > 0 {
			if received >= options.Budget {
				return result, nil
			}
			count = min(count, options.Budget-received)
		}
		batch, err := bus.ReceiveBatch(ctx, receiver, count)
		if err != nil {
			return result, err
		}
		received += len(batch)

		candidates, sawNew := sortBatch(batch, selected, seen, hold)
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

// sortBatch splits a received batch into the selected messages seen for the first time, which it returns, and
// the rest, which it holds. sawNew is true when any message was new, selected or not.
func sortBatch(
	batch []*azservicebus.ReceivedMessage,
	selected map[int64]Entry,
	seen map[int64]struct{},
	hold func(*azservicebus.ReceivedMessage),
) (candidates []candidate, sawNew bool) {
	for _, message := range batch {
		entry := EntryFromMessage(message)
		if _, again := seen[entry.SequenceNumber]; again {
			hold(message)
			continue
		}
		seen[entry.SequenceNumber] = struct{}{}
		sawNew = true

		if _, wanted := selected[entry.SequenceNumber]; !wanted {
			hold(message)
			continue
		}
		candidates = append(candidates, candidate{Entry: entry, Message: message})
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

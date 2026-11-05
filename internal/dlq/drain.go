package dlq

import (
	"context"
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

// drain receives the dead-letter queue under peek-lock and hands the selected messages to process, batch by batch.
//
// A message that is not acted on stays locked until the run ends, and is only then abandoned. Abandoning at once
// would put it back at the head of the queue, and the next receive would return it again before any message
// behind it. The lock expires by itself after the lock duration, which bounds how far one run can read through
// messages it skips. drain stops when every selected message is handled, when a receive returns nothing, or when
// a receive returns only messages it has seen before.
func drain(
	ctx context.Context,
	receiver bus.SettlingReceiver,
	batchSize int,
	selected map[int64]Entry,
	process func(ctx context.Context, candidates []candidate, hold func(*azservicebus.ReceivedMessage)) (stop bool, err error),
) (handled int, err error) {
	var held []*azservicebus.ReceivedMessage
	defer func() { releaseHeld(ctx, receiver, held) }()
	hold := func(message *azservicebus.ReceivedMessage) { held = append(held, message) }

	seen := make(map[int64]struct{})
	for handled < len(selected) {
		batch, err := bus.ReceiveBatch(ctx, receiver, batchSize)
		if err != nil {
			return handled, err
		}

		candidates, unseen := sortBatch(batch, selected, seen, hold)
		if unseen == 0 {
			return handled, nil
		}
		if len(candidates) == 0 {
			continue
		}
		handled += len(candidates)

		stop, err := process(ctx, candidates, hold)
		if err != nil || stop {
			return handled, err
		}
	}
	return handled, nil
}

// sortBatch splits a received batch into the selected messages seen for the first time, which it returns, and
// the rest, which it holds. unseen counts the messages that were new, selected or not.
func sortBatch(
	batch []*azservicebus.ReceivedMessage,
	selected map[int64]Entry,
	seen map[int64]struct{},
	hold func(*azservicebus.ReceivedMessage),
) (candidates []candidate, unseen int) {
	for _, message := range batch {
		entry := EntryFromMessage(message)
		if _, again := seen[entry.SequenceNumber]; again {
			hold(message)
			continue
		}
		seen[entry.SequenceNumber] = struct{}{}
		unseen++

		if _, wanted := selected[entry.SequenceNumber]; !wanted {
			hold(message)
			continue
		}
		candidates = append(candidates, candidate{Entry: entry, Message: message})
	}
	return candidates, unseen
}

// releaseHeld abandons what a run held back. A lock that already expired makes Abandon fail, which is fine: the
// message is available again either way. It runs on its own deadline so that a cancelled run still releases.
func releaseHeld(ctx context.Context, receiver bus.SettlingReceiver, held []*azservicebus.ReceivedMessage) {
	releaseCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), releaseTimeout)
	defer cancel()
	for _, message := range held {
		if releaseCtx.Err() != nil {
			return
		}
		_ = receiver.AbandonMessage(releaseCtx, message, nil) // the lock expires by itself if this fails.
	}
}

func complete(ctx context.Context, receiver bus.SettlingReceiver, message *azservicebus.ReceivedMessage) error {
	completeCtx, cancel := context.WithTimeout(ctx, settleTimeout)
	defer cancel()
	return receiver.CompleteMessage(completeCtx, message, nil)
}

package dlq

import (
	"context"

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
	"github.com/marcinkaczmarek10/asbctl/internal/bus"
)

// Purge completes each selected message under peek-lock, which removes it from the dead-letter queue. It never
// receives in delete mode: a message is gone only after its own Complete succeeded.
func Purge(
	ctx context.Context,
	receiver bus.SettlingReceiver,
	selection Selection,
	snapshot string,
	onResult func(Result) error,
) (PurgeSummary, error) {
	summary := newPurgeSummary(snapshot)
	selected := selection.Sequences()

	handled, err := drain(ctx, receiver, bus.MaxBatchSize, selected,
		func(ctx context.Context, candidates []candidate, hold func(*azservicebus.ReceivedMessage)) (bool, error) {
			for _, c := range candidates {
				result := Result{SequenceNumber: c.Entry.SequenceNumber, MessageID: c.Entry.MessageID, Status: StatusPurged}
				if err := complete(ctx, receiver, c.Message); err != nil {
					hold(c.Message)
					result.Status, result.Err = StatusFailed, err
					summary.Failed++
				} else {
					summary.Purged++
				}
				if err := onResult(result); err != nil {
					return true, err
				}
			}
			return false, nil
		})

	summary.NotReached = len(selected) - handled
	return summary, err
}

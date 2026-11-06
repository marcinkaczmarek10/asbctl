package dlq

import (
	"context"
	"log/slog"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
	"github.com/marcinkaczmarek10/asbctl/internal/bus"
)

// PurgeOptions tune a purge. MaxHeld is the hold cap of drain, and zero means no cap. ReleaseGrace is the wait
// after releasing held messages.
type PurgeOptions struct {
	MaxHeld      int
	ReleaseGrace time.Duration
	Logger       *slog.Logger
}

// Purge completes each selected message under peek-lock, which removes it from the dead-letter queue. It never
// receives in delete mode: a message is gone only after its own Complete succeeded.
func Purge(
	ctx context.Context,
	receiver bus.SettlingReceiver,
	selection Selection,
	snapshot string,
	options PurgeOptions,
	onResult func(Result) error,
) (PurgeSummary, error) {
	summary := newPurgeSummary(snapshot)
	selected := selection.Sequences()

	drained, err := drain(ctx, receiver, drainOptions{BatchSize: bus.MaxBatchSize, MaxHeld: options.MaxHeld, ReleaseGrace: options.ReleaseGrace, Logger: options.Logger}, selected,
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

	summary.NotReached = len(selected) - drained.Handled
	summary.ScanLimitReached = drained.ScanLimited
	return summary, err
}

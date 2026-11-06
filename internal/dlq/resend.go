package dlq

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
	"github.com/marcinkaczmarek10/asbctl/internal/bus"
	"github.com/marcinkaczmarek10/asbctl/internal/export"
)

const (
	watchInterval = 5 * time.Second
	sendTimeout   = 60 * time.Second
)

// ResendDeps are the I/O boundaries of a resend.
type ResendDeps struct {
	// Receiver is the peek-lock receiver on the source dead-letter queue.
	Receiver bus.SettlingReceiver
	// Sender sends to the target entity.
	Sender bus.MessageSender
	// Peeker reads the source dead-letter queue to look for bounces.
	Peeker bus.Peeker
	Ledger *Ledger
	Pacer  *bus.Pacer
	Now    func() time.Time
	Sleep  func(ctx context.Context, d time.Duration) error
	Logger *slog.Logger
}

// ResendParams configure a resend. Recent holds the MessageIds the ledger has for the target inside the
// duplicate detection window.
type ResendParams struct {
	Selection    Selection
	Snapshot     string
	Namespace    string
	Target       string
	Wave         int
	BatchSize    int
	MaxHeld      int
	StopOnBounce float64
	Watch        time.Duration
	Dedupe       bus.DuplicateDetection
	NewMessageID func() string
	Recent       map[string]struct{}
}

// SkipsDuplicate is true when the duplicate detection guard refuses a message: the target drops a repeated
// MessageId, the ids are being kept, and the id repeats in the selection or was resent inside the window.
func (p ResendParams) SkipsDuplicate(entry Entry, duplicateIDs map[string]struct{}) bool {
	if !p.Dedupe.Enabled || p.NewMessageID != nil || entry.MessageID == "" {
		return false
	}
	if _, repeated := duplicateIDs[entry.MessageID]; repeated {
		return true
	}
	_, resent := p.Recent[entry.MessageID]
	return resent
}

// Resend sends each selected message back, one wave at a time, and completes its dead-letter copy only after
// the send succeeded. After a wave it looks for bounces and stops before the next wave when too many came back.
func Resend(ctx context.Context, deps ResendDeps, params ResendParams, onResult func(Result) error) (ResendSummary, error) {
	run := &resendRun{
		deps:         deps,
		params:       params,
		duplicateIDs: params.Selection.DuplicateIDs(),
		scanner:      NewBounceScanner(deps.Peeker, deps.Now()),
		onResult:     onResult,
		summary:      newResendSummary(params.Snapshot),
	}

	selected := params.Selection.Sequences()
	drained, err := drain(ctx, deps.Receiver, drainOptions{BatchSize: params.BatchSize, MaxHeld: params.MaxHeld, Logger: deps.Logger}, selected, run.processBatch)
	if err == nil && run.waveSent > 0 && !run.summary.StoppedOnBounce {
		err = run.endWave(ctx)
	}
	if err == nil && !run.summary.StoppedOnBounce {
		err = run.watch(ctx)
	}

	run.summary.NotReached = len(selected) - drained.Handled
	run.summary.ScanLimitReached = drained.ScanLimited
	return run.summary, err
}

type resendRun struct {
	deps         ResendDeps
	params       ResendParams
	duplicateIDs map[string]struct{}
	scanner      *BounceScanner
	onResult     func(Result) error
	summary      ResendSummary
	waveSent     int
}

func (r *resendRun) report(entry Entry, status string, err error) error {
	switch status {
	case StatusResent:
		r.summary.Resent++
		r.waveSent++
	case StatusFailed:
		r.summary.Failed++
	case StatusSentNotCompleted:
		r.summary.SentNotCompleted++
	case StatusSkippedDuplicateWindow:
		r.summary.Skipped++
	}
	return r.onResult(Result{SequenceNumber: entry.SequenceNumber, MessageID: entry.MessageID, Status: status, Err: err})
}

type pending struct {
	candidate candidate
	outgoing  bus.Outgoing
}

func (r *resendRun) processBatch(ctx context.Context, candidates []candidate, hold func(*azservicebus.ReceivedMessage)) (bool, error) {
	var toSend []pending
	for _, c := range candidates {
		if r.params.SkipsDuplicate(c.Entry, r.duplicateIDs) {
			hold(c.Message)
			if err := r.report(c.Entry, StatusSkippedDuplicateWindow, nil); err != nil {
				return true, err
			}
			continue
		}

		outgoing, err := export.OutgoingFromReceived(int(c.Entry.SequenceNumber), c.Message, r.params.NewMessageID)
		if err != nil {
			hold(c.Message)
			if err := r.report(c.Entry, StatusFailed, err); err != nil {
				return true, err
			}
			continue
		}
		toSend = append(toSend, pending{candidate: c, outgoing: outgoing})
	}
	return r.sendBatch(ctx, toSend, hold)
}

// sendBatch sends, then completes what was sent, one message after the other. A send failure holds the message
// for abandoning at the end of the run, and a failed Complete is reported as sent_not_completed: the copy exists
// and the dead-letter message will come back, which is never hidden.
func (r *resendRun) sendBatch(ctx context.Context, toSend []pending, hold func(*azservicebus.ReceivedMessage)) (bool, error) {
	if len(toSend) == 0 {
		return r.afterBatch(ctx)
	}
	if err := r.deps.Pacer.Wait(ctx, len(toSend)); err != nil {
		for _, p := range toSend {
			hold(p.candidate.Message)
		}
		return true, err
	}

	outgoings := make([]bus.Outgoing, len(toSend))
	for i, p := range toSend {
		outgoings[i] = p.outgoing
	}
	sendCtx, cancel := context.WithTimeout(ctx, sendTimeout)
	errs := r.deps.Sender.SendMessages(sendCtx, outgoings)
	cancel()

	var ledgerErr error
	for i, p := range toSend {
		entry, message := p.candidate.Entry, p.candidate.Message
		if errs[i] != nil {
			hold(message)
			if err := r.report(entry, StatusFailed, errs[i]); err != nil {
				return true, err
			}
			continue
		}

		r.scanner.Track(p.outgoing.MessageID, entry.MessageID)
		sent := LedgerEntry{Namespace: r.params.Namespace, Target: r.params.Target, MessageID: p.outgoing.MessageID, SentAt: r.deps.Now()}
		if err := r.deps.Ledger.Append(sent); err != nil && ledgerErr == nil {
			ledgerErr = err
		}

		status, completeErr := StatusResent, complete(ctx, r.deps.Receiver, message)
		if completeErr != nil {
			status = StatusSentNotCompleted
		}
		if err := r.report(entry, status, completeErr); err != nil {
			return true, err
		}
	}
	if ledgerErr != nil {
		return true, ledgerErr
	}
	return r.afterBatch(ctx)
}

// afterBatch ends the wave when it is full. It stops the run when the wave showed too many bounces.
func (r *resendRun) afterBatch(ctx context.Context) (bool, error) {
	if r.waveSent < r.params.Wave {
		return false, nil
	}
	err := r.endWave(ctx)
	return r.summary.StoppedOnBounce, err
}

func (r *resendRun) endWave(ctx context.Context) error {
	if _, err := r.scanner.Scan(ctx); err != nil {
		return fmt.Errorf("look for bounces: %w", err)
	}
	r.summary.Bounced = r.scanner.Bounced()
	r.waveSent = 0
	r.summary.StoppedOnBounce = ShouldStopOnBounce(r.params.StopOnBounce, r.summary.Bounced, r.summary.Resent)
	return nil
}

func (r *resendRun) watch(ctx context.Context) error {
	deadline := r.deps.Now().Add(r.params.Watch)
	for {
		remaining := deadline.Sub(r.deps.Now())
		if remaining <= 0 {
			return nil
		}
		if err := r.deps.Sleep(ctx, min(watchInterval, remaining)); err != nil {
			return err
		}
		if _, err := r.scanner.Scan(ctx); err != nil {
			return fmt.Errorf("look for bounces: %w", err)
		}
		r.summary.Bounced = r.scanner.Bounced()
	}
}

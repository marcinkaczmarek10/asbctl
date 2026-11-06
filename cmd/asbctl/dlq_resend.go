package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
	"github.com/marcinkaczmarek10/asbctl/internal/bus"
	"github.com/marcinkaczmarek10/asbctl/internal/dlq"
	"github.com/marcinkaczmarek10/asbctl/internal/export"
)

const (
	defaultWave         = 100
	defaultStopOnBounce = 5.0
)

const resendUsageText = `Usage:
  asbctl dlq resend --namespace <name|fqdn> (--queue Q | --topic T --subscription S) [flags]

Send dead-lettered messages back, then complete their dead-letter copies. A copy is completed only after its
send succeeded.

The target is the source queue by default. The dead-letter queue of a subscription has no
default: pass --to-topic or --to-queue. A send to a topic reaches every matching subscription, not only the one the
messages came from.

Selection: --payload-type-id and --reason pick messages, --max caps how many. Before anything changes the selected
messages are written to a snapshot file, in the format of "asbctl export". Without --yes, or with --dry-run, the
command prints the plan (the count per payload type and reason, the target, the snapshot path and the duplicate
detection status) and changes nothing.

Duplicate detection: when the target drops a repeated MessageId inside a time window, a message is refused and
left in the dead-letter queue if its MessageId repeats in the selection or was resent inside the window, which the
ledger in $XDG_STATE_HOME/asbctl (or ~/.local/state/asbctl) records. When the target settings cannot be read, duplicate
detection is assumed on with a window of 10 minutes. With --new-message-id each message gets a new id and its original
is kept in the application property asbctl-original-message-id, but a consumer that deduplicates on MessageId will
then process the message twice.

Pacing and bounces: --rate caps sends per second. Messages go out in waves of --wave. After each wave the command
looks in the source dead-letter queue for resent messages that came back, and stops before the next wave when
--stop-on-bounce percent of the resent messages did (0 turns this off). --watch keeps watching for that long after the
last wave. Bounce detection reads the source dead-letter queue only.

A run holds the messages it skips locked until it ends. --max-scan caps how many (default 50000), and a selection behind
more messages than that is reported as not reached; run the command again for the rest.

Exit code 0 means every selected message was resent and none came back. 3 means some were not, or some came back, or
the run stopped on bounces. 1 means nothing was resent.

Authentication uses DefaultAzureCredential (AZURE_TOKEN_CREDENTIALS=dev narrows it to az and azd).
The environment variable ASBCTL_CONNECTION_STRING, when set, is used instead and --namespace is optional.

Flags:
`

type resendConfig struct {
	selectionConfig
	toQueue      string
	toTopic      string
	rate         int
	wave         int
	watch        time.Duration
	stopOnBounce float64
	newMessageID bool
}

// destination returns where the messages are sent. It is the source queue unless --to-queue or --to-topic say otherwise.
func (c resendConfig) destination() dlq.Location {
	switch {
	case c.toQueue != "":
		return dlq.Location{Queue: c.toQueue}
	case c.toTopic != "":
		return dlq.Location{Topic: c.toTopic}
	default:
		return dlq.Location{Queue: c.target.Queue}
	}
}

// ledgerTarget names the destination in the resend ledger.
func (c resendConfig) ledgerTarget() string {
	if destination := c.destination(); destination.Queue != "" {
		return "queue:" + destination.Queue
	}
	return "topic:" + c.destination().Topic
}

func runDLQResend(ctx context.Context, args []string, stdout, stderr io.Writer, logger *slog.Logger) int {
	connectionString := os.Getenv(connectionStringEnv)

	cfg, err := parseResendArgs(args, stdout, connectionString != "", time.Now())
	switch {
	case errors.Is(err, flag.ErrHelp):
		return exitOK
	case err != nil:
		return reportUsageError(stderr, cfg.format, err, "run 'asbctl dlq resend -h' for usage")
	}

	code, err := resend(ctx, cfg, connectionString, stdout, stderr, logger)
	if err != nil {
		export.WriteError(stderr, cfg.format, dlqErrorCode(ctx, err, "resend_failed"), err)
		return exitFailure
	}
	return code
}

func resend(ctx context.Context, cfg resendConfig, connectionString string, stdout, stderr io.Writer, logger *slog.Logger) (int, error) {
	client, err := bus.NewClient(cfg.namespace, connectionString)
	if err != nil {
		return 0, err
	}
	defer closeAfter(ctx, logger, "close client", client.Close)

	receiver, err := client.NewReceiver(cfg.target)
	if err != nil {
		return 0, fmt.Errorf("create receiver: %w", err)
	}
	defer closeAfter(ctx, logger, "close receiver", receiver.Close)

	lockDuration, dedupe, err := readResendSettings(ctx, client, cfg, logger)
	if err != nil {
		return 0, err
	}

	now := time.Now()
	selection, err := selectMessages(ctx, receiver, cfg.selectionConfig)
	if err != nil {
		return 0, err
	}

	ledgerDir := dlq.DefaultLedgerDir(os.Getenv, homeDir())
	namespace := namespaceKey(cfg.namespace, connectionString)
	params := dlq.ResendParams{
		Selection:    selection,
		Snapshot:     cfg.snapshot,
		Namespace:    namespace,
		Target:       cfg.ledgerTarget(),
		Wave:         cfg.wave,
		BatchSize:    min(cfg.wave, bus.ReceiveBatchSize(lockDuration, cfg.rate)),
		MaxHeld:      cfg.maxScan,
		StopOnBounce: cfg.stopOnBounce,
		Watch:        cfg.watch,
		Dedupe:       dedupe,
	}
	if cfg.newMessageID {
		params.NewMessageID = bus.NewMessageID
	}
	if dedupe.Enabled && !cfg.newMessageID {
		params.Recent, err = dlq.RecentFromDir(ledgerDir, namespace, params.Target, dedupe.Window, now)
		if err != nil {
			return 0, err
		}
	}

	if !cfg.acts() || len(selection.Entries) == 0 {
		plan := dlq.NewResendPlan(dlq.ResendPlanInput{
			Source: cfg.target, Target: cfg.destination(), Selection: selection, Snapshot: cfg.snapshot, Params: params, DryRun: cfg.dryRun,
		})
		return exitOK, dlq.WritePlan(stdout, cfg.format, plan)
	}
	logger.InfoContext(ctx, "snapshot written", "file", cfg.snapshot, "messages", len(selection.Entries))

	return sendSelection(ctx, cfg, client, receiver, params, ledgerDir, stdout, stderr, logger)
}

func sendSelection(
	ctx context.Context,
	cfg resendConfig,
	client *bus.Client,
	receiver *azservicebus.Receiver,
	params dlq.ResendParams,
	ledgerDir string,
	stdout, stderr io.Writer,
	logger *slog.Logger,
) (int, error) {
	destination := cfg.destination()
	sender, err := client.NewSender(destination.Queue + destination.Topic)
	if err != nil {
		return 0, fmt.Errorf("create sender: %w", err)
	}
	defer closeAfter(ctx, logger, "close sender", sender.Close)

	ledger, err := dlq.OpenLedger(ledgerDir, time.Now())
	if err != nil {
		return 0, err
	}
	defer ledger.Close()

	deps := dlq.ResendDeps{
		Receiver: receiver, Sender: sender, Peeker: receiver, Ledger: ledger,
		Pacer: bus.NewPacer(cfg.rate), Now: time.Now, Sleep: sleepContext, Logger: logger,
	}
	summary, err := dlq.Resend(ctx, deps, params, func(result dlq.Result) error {
		return dlq.WriteResult(stdout, cfg.format, result)
	})
	if err != nil {
		return 0, err
	}
	if err := dlq.WriteResendSummary(stdout, cfg.format, summary); err != nil {
		return 0, err
	}

	code := summary.ExitCode(len(params.Selection.Entries))
	switch {
	case summary.StoppedOnBounce:
		export.WriteError(stderr, cfg.format, "stopped_on_bounce", fmt.Errorf(
			"stopped before the next wave: %d of %d resent messages came back to the dead-letter queue", summary.Bounced, summary.Resent))
	case code == exitPartial:
		export.WriteError(stderr, cfg.format, "partial", errors.New("not every selected message was resent, or some came back"))
	case code == exitFailure:
		export.WriteError(stderr, cfg.format, "resend_failed", errors.New("no message was resent"))
	}
	return code, nil
}

// readResendSettings reads the lock duration of the source and the duplicate detection of the target. A
// permission error falls back to the assumed values, and any other error fails the run.
func readResendSettings(ctx context.Context, client *bus.Client, cfg resendConfig, logger *slog.Logger) (time.Duration, bus.DuplicateDetection, error) {
	facts, err := bus.ReadEntityFacts(ctx, client.Admin(), cfg.target)
	switch {
	case errors.Is(err, bus.ErrEntityFactsForbidden):
		logger.WarnContext(ctx, "entity details unavailable, assuming a lock duration", "lock_duration", bus.DefaultLockDuration, "error", err)
	case err != nil:
		return 0, bus.DuplicateDetection{}, err
	}

	destination := cfg.destination()
	dedupe, err := bus.ReadSendTarget(ctx, client.Admin(), destination.Queue+destination.Topic, destination.Topic != "")
	switch {
	case errors.Is(err, bus.ErrEntityFactsForbidden):
		logger.WarnContext(ctx, "target details unavailable, assuming duplicate detection is on", "window", bus.AssumedDuplicateWindow, "error", err)
	case err != nil:
		return 0, bus.DuplicateDetection{}, err
	}
	return bus.LockDurationFrom(facts), dedupe, nil
}

func homeDir() string {
	home, err := os.UserHomeDir()
	if err != nil {
		return "."
	}
	return home
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

// parseResendArgs turns the command line into a validated config. Help text goes to helpOut.
func parseResendArgs(args []string, helpOut io.Writer, hasConnectionString bool, now time.Time) (resendConfig, error) {
	var cfg resendConfig
	var output string

	fs := flag.NewFlagSet("dlq resend", flag.ContinueOnError)
	bindSelectionFlags(fs, &cfg.selectionConfig, &output)
	fs.StringVar(&cfg.toQueue, "to-queue", "", "send to this queue instead of the source queue")
	fs.StringVar(&cfg.toTopic, "to-topic", "", "send to this topic; it reaches every matching subscription, not only the source")
	fs.IntVar(&cfg.rate, "rate", 0, "send at most this many messages per second (0 is unlimited)")
	fs.IntVar(&cfg.wave, "wave", defaultWave, "send this many messages, then look for bounces before the next wave")
	fs.DurationVar(&cfg.watch, "watch", 0, "keep looking for bounces for this long after the last wave, such as 2m")
	fs.Float64Var(&cfg.stopOnBounce, "stop-on-bounce", defaultStopOnBounce, "stop before the next wave when this percent of resent messages came back (0 disables)")
	fs.BoolVar(&cfg.newMessageID, "new-message-id", false, "give each message a new MessageId and keep the original in asbctl-original-message-id")

	if err := parseFlagSet(fs, args, resendUsageText, helpOut); err != nil {
		return cfg, err
	}
	if fs.NArg() > 0 {
		return cfg, fmt.Errorf("unexpected argument %q", fs.Arg(0))
	}
	if err := finishSelectionConfig(&cfg.selectionConfig, output, now, hasConnectionString); err != nil {
		return cfg, err
	}
	return cfg, validateResendConfig(cfg)
}

func validateResendConfig(cfg resendConfig) error {
	switch {
	case cfg.toQueue != "" && cfg.toTopic != "":
		return errors.New("--to-queue cannot be combined with --to-topic")
	case cfg.target.Queue == "" && cfg.toQueue == "" && cfg.toTopic == "":
		return errors.New("a subscription's dead-letter queue has no default target: pass --to-topic or --to-queue")
	case pointsAtSubscription(cfg.toQueue) || pointsAtSubscription(cfg.toTopic):
		return errors.New("--to-queue and --to-topic cannot point at a subscription")
	case cfg.rate < 0:
		return fmt.Errorf("--rate is %d, want 0 or more", cfg.rate)
	case cfg.wave < 1:
		return fmt.Errorf("--wave is %d, want at least 1", cfg.wave)
	case cfg.watch < 0:
		return fmt.Errorf("--watch is %s, want 0 or more", cfg.watch)
	case cfg.stopOnBounce < 0 || cfg.stopOnBounce > 100:
		return fmt.Errorf("--stop-on-bounce is %v, want a percent from 0 to 100", cfg.stopOnBounce)
	}
	return nil
}

func pointsAtSubscription(name string) bool {
	return strings.Contains(strings.ToLower(name), "/subscriptions/")
}

package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"time"

	"github.com/marcinkaczmarek10/asbctl/internal/bus"
	"github.com/marcinkaczmarek10/asbctl/internal/dlq"
	"github.com/marcinkaczmarek10/asbctl/internal/export"
)

const purgeUsageText = `Usage:
  asbctl dlq purge --namespace <name|fqdn> (--queue Q | --topic T --subscription S) [flags]

Remove dead-lettered messages for good. Each message is received under peek-lock and completed, never in delete
mode, so a message is gone only after its own Complete succeeded.

Selection: --payload-type-id and --reason pick messages, --max caps how many. Before anything is removed the
selected messages are written to a snapshot file, in the format of "asbctl export", and "asbctl send" can send
them back. Without --yes, or with --dry-run, the command prints the plan (the count per payload type and reason and
the snapshot path) and changes nothing.

A run holds the messages it skips locked until it ends. --max-scan caps how many (default 50000), and a selection behind
more messages than that is reported as not reached; run the command again for the rest.

Exit code 0 means every selected message was purged, 3 means some were not, and 1 means none were.

Authentication uses DefaultAzureCredential (AZURE_TOKEN_CREDENTIALS=dev narrows it to az and azd).
The environment variable ASBCTL_CONNECTION_STRING, when set, is used instead and --namespace is optional.

Flags:
`

func runDLQPurge(ctx context.Context, args []string, stdout, stderr io.Writer, logger *slog.Logger) int {
	connectionString := os.Getenv(connectionStringEnv)

	cfg, err := parsePurgeArgs(args, stdout, connectionString != "", time.Now())
	switch {
	case errors.Is(err, flag.ErrHelp):
		return exitOK
	case err != nil:
		return reportUsageError(stderr, cfg.format, err, "run 'asbctl dlq purge -h' for usage")
	}

	code, err := purge(ctx, cfg, connectionString, stdout, stderr, logger)
	if err != nil {
		export.WriteError(stderr, cfg.format, dlqErrorCode(ctx, err, "purge_failed"), err)
		return exitFailure
	}
	return code
}

func purge(ctx context.Context, cfg selectionConfig, connectionString string, stdout, stderr io.Writer, logger *slog.Logger) (int, error) {
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

	selection, err := selectMessages(ctx, receiver, cfg)
	if err != nil {
		return 0, err
	}
	if !cfg.acts() || len(selection.Entries) == 0 {
		return exitOK, dlq.WritePlan(stdout, cfg.format, dlq.NewPurgePlan(cfg.target, selection, cfg.snapshot, cfg.dryRun))
	}
	logger.InfoContext(ctx, "snapshot written", "file", cfg.snapshot, "messages", len(selection.Entries))

	summary, err := dlq.Purge(ctx, receiver, selection, cfg.snapshot, dlq.PurgeOptions{MaxHeld: cfg.maxScan, ReleaseGrace: dlq.DefaultReleaseGrace, Logger: logger}, func(result dlq.Result) error {
		return dlq.WriteResult(stdout, cfg.format, result)
	})
	if err != nil {
		return 0, err
	}
	if err := dlq.WritePurgeSummary(stdout, cfg.format, summary); err != nil {
		return 0, err
	}

	code := summary.ExitCode(len(selection.Entries))
	switch code {
	case exitPartial:
		export.WriteError(stderr, cfg.format, "partial", errors.New("not every selected message was purged"))
	case exitFailure:
		export.WriteError(stderr, cfg.format, "purge_failed", errors.New("no message was purged"))
	}
	return code, nil
}

// parsePurgeArgs turns the command line into a validated config. Help text goes to helpOut.
func parsePurgeArgs(args []string, helpOut io.Writer, hasConnectionString bool, now time.Time) (selectionConfig, error) {
	var cfg selectionConfig
	var output string

	fs := flag.NewFlagSet("dlq purge", flag.ContinueOnError)
	bindSelectionFlags(fs, &cfg, &output)

	if err := parseFlagSet(fs, args, purgeUsageText, helpOut); err != nil {
		return cfg, err
	}
	if fs.NArg() > 0 {
		return cfg, fmt.Errorf("unexpected argument %q", fs.Arg(0))
	}
	return cfg, finishSelectionConfig(&cfg, output, now, hasConnectionString)
}

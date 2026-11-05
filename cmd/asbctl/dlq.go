package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
	"github.com/marcinkaczmarek10/asbctl/internal/bus"
	"github.com/marcinkaczmarek10/asbctl/internal/export"
)

const defaultSummaryMax = 10000

const dlqUsageText = `asbctl dlq works with dead-letter queues.

Usage:
  asbctl dlq <command> [flags]

Commands:
  summary  group the dead-lettered messages by payload type and reason
`

const summaryUsageText = `Usage:
  asbctl dlq summary --namespace <name|fqdn> (--queue Q | --topic T --subscription S) [flags]

Group the messages of a dead-letter queue by PayloadTypeId and dead-letter reason, and show the entity's
counts and settings. Messages are peeked, never locked or removed.

Reading the entity's counts and settings needs Manage rights. Without them the summary still runs and
the entity details are marked unavailable.

Authentication uses DefaultAzureCredential (AZURE_TOKEN_CREDENTIALS=dev narrows it to az and azd).
The environment variable ASBCTL_CONNECTION_STRING, when set, is used instead and --namespace is optional.

Flags:
`

type summaryConfig struct {
	target    bus.Target
	namespace string
	max       int
	format    export.Format
}

func runDLQ(ctx context.Context, args []string, stdout, stderr io.Writer, logger *slog.Logger) int {
	if len(args) == 0 {
		fmt.Fprintln(stderr, "asbctl dlq: no command given")
		fmt.Fprint(stderr, dlqUsageText)
		return exitUsage
	}

	name, rest := args[0], args[1:]
	switch name {
	case "summary":
		return runDLQSummary(ctx, rest, stdout, stderr, logger)
	case "-h", "-help", "--help":
		fmt.Fprint(stdout, dlqUsageText)
		return exitOK
	default:
		fmt.Fprintf(stderr, "asbctl dlq: unknown command %q\n", name)
		fmt.Fprint(stderr, dlqUsageText)
		return exitUsage
	}
}

func runDLQSummary(ctx context.Context, args []string, stdout, stderr io.Writer, logger *slog.Logger) int {
	connectionString := os.Getenv(connectionStringEnv)

	cfg, err := parseSummaryArgs(args, stdout, connectionString != "")
	switch {
	case errors.Is(err, flag.ErrHelp):
		return exitOK
	case err != nil:
		return reportUsageError(stderr, cfg.format, err, "run 'asbctl dlq summary -h' for usage")
	}

	if err := summarise(ctx, cfg, connectionString, stdout, logger); err != nil {
		code := "summary_failed"
		if ctx.Err() != nil {
			code = "canceled"
		}
		export.WriteError(stderr, cfg.format, code, err)
		return exitFailure
	}
	return exitOK
}

func summarise(ctx context.Context, cfg summaryConfig, connectionString string, stdout io.Writer, logger *slog.Logger) error {
	client, err := bus.NewClient(cfg.namespace, connectionString)
	if err != nil {
		return err
	}
	defer closeAfter(ctx, logger, "close client", client.Close)

	facts, err := bus.ReadEntityFacts(ctx, client.Admin(), cfg.target)
	switch {
	case errors.Is(err, bus.ErrEntityFactsForbidden):
		logger.WarnContext(ctx, "entity details unavailable, grouping only", "error", err)
	case err != nil:
		return err
	}

	receiver, err := client.NewReceiver(cfg.target)
	if err != nil {
		return fmt.Errorf("create receiver: %w", err)
	}
	defer closeAfter(ctx, logger, "close receiver", receiver.Close)

	aggregator := export.NewAggregator()
	_, err = bus.Peek(ctx, receiver, bus.PeekOptions{Max: cfg.max}, func(page []*azservicebus.ReceivedMessage) error {
		aggregator.Add(page)
		return nil
	})
	if err != nil {
		return err
	}
	return export.WriteSummary(stdout, cfg.format, aggregator.Summary(facts, cfg.max))
}

// parseSummaryArgs turns the command line into a validated config. Help text goes to helpOut.
func parseSummaryArgs(args []string, helpOut io.Writer, hasConnectionString bool) (summaryConfig, error) {
	var cfg summaryConfig
	var output string

	fs := flag.NewFlagSet("dlq summary", flag.ContinueOnError)
	bindEntityFlags(fs, &cfg.namespace, &cfg.target)
	fs.IntVar(&cfg.max, "max", defaultSummaryMax, "peek at most this many dead-lettered messages")
	fs.StringVar(&output, "output", string(export.FormatTable), "output format: table or json")

	if err := parseFlagSet(fs, args, summaryUsageText, helpOut); err != nil {
		return cfg, err
	}
	if fs.NArg() > 0 {
		return cfg, fmt.Errorf("unexpected argument %q", fs.Arg(0))
	}

	format, err := export.ParseFormat(output)
	if err != nil {
		return cfg, err
	}
	cfg.format = format
	cfg.target.DeadLetter = true

	if err := validateEntity(cfg.namespace, cfg.target, hasConnectionString); err != nil {
		return cfg, err
	}
	if cfg.max < 1 {
		return cfg, fmt.Errorf("--max is %d, want at least 1", cfg.max)
	}
	return cfg, nil
}

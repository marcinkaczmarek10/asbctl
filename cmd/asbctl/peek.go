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

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
	"github.com/marcinkaczmarek10/asbctl/internal/bus"
	"github.com/marcinkaczmarek10/asbctl/internal/export"
)

const (
	connectionStringEnv = "ASBCTL_CONNECTION_STRING"
	closeTimeout        = 10 * time.Second
	defaultPeekMax      = 50
)

const peekUsageText = `Usage:
  asbctl peek --namespace <name|fqdn> (--queue Q | --topic T --subscription S) [flags]

Peek messages without locking or removing them.

Authentication uses DefaultAzureCredential (AZURE_TOKEN_CREDENTIALS=dev narrows it to az and azd).
The environment variable ASBCTL_CONNECTION_STRING, when set, is used instead and --namespace is optional.

Flags:
`

type peekConfig struct {
	target        bus.Target
	namespace     string
	max           int
	fromSequence  int64
	payloadTypeID string
	format        export.Format
}

func runPeek(ctx context.Context, args []string, stdout, stderr io.Writer, logger *slog.Logger) int {
	connectionString := os.Getenv(connectionStringEnv)

	cfg, err := parsePeekArgs(args, stdout, connectionString != "")
	switch {
	case errors.Is(err, flag.ErrHelp):
		return exitOK
	case err != nil:
		export.WriteError(stderr, cfg.format, "usage", err)
		if cfg.format != export.FormatJSON {
			fmt.Fprintln(stderr, "run 'asbctl peek -h' for usage")
		}
		return exitUsage
	}

	if err := peek(ctx, cfg, connectionString, stdout, logger); err != nil {
		code := "peek_failed"
		if ctx.Err() != nil {
			code = "canceled"
		}
		export.WriteError(stderr, cfg.format, code, err)
		return exitFailure
	}
	return exitOK
}

func peek(ctx context.Context, cfg peekConfig, connectionString string, stdout io.Writer, logger *slog.Logger) error {
	client, err := bus.NewClient(cfg.namespace, connectionString)
	if err != nil {
		return err
	}
	defer closeAfter(ctx, logger, "close client", client.Close)

	receiver, err := client.NewReceiver(cfg.target)
	if err != nil {
		return fmt.Errorf("create receiver: %w", err)
	}
	defer closeAfter(ctx, logger, "close receiver", receiver.Close)

	writer := export.NewWriter(stdout, export.Options{
		Format:        cfg.format,
		DeadLetter:    cfg.target.DeadLetter,
		PayloadTypeID: cfg.payloadTypeID,
	})
	peeked, err := bus.Peek(ctx, receiver, bus.PeekOptions{FromSequence: cfg.fromSequence, Max: cfg.max}, func(page []*azservicebus.ReceivedMessage) error {
		_, err := writer.WriteMessages(page)
		return err
	})
	logger.DebugContext(ctx, "peek finished", "peeked", peeked)
	return err
}

// closeAfter closes on a fresh deadline, because ctx is usually already cancelled when a run was interrupted.
func closeAfter(ctx context.Context, logger *slog.Logger, message string, closeFn func(context.Context) error) {
	closeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), closeTimeout)
	defer cancel()
	if err := closeFn(closeCtx); err != nil {
		logger.WarnContext(ctx, message, "error", err)
	}
}

// parsePeekArgs turns the command line into a validated config. help text goes to helpOut.
func parsePeekArgs(args []string, helpOut io.Writer, hasConnectionString bool) (peekConfig, error) {
	var cfg peekConfig
	var output string

	fs := flag.NewFlagSet("peek", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	fs.StringVar(&cfg.namespace, "namespace", "", "Service Bus namespace: a short name or a fully qualified domain name")
	fs.StringVar(&cfg.target.Queue, "queue", "", "queue to peek")
	fs.StringVar(&cfg.target.Topic, "topic", "", "topic to peek, with --subscription")
	fs.StringVar(&cfg.target.Subscription, "subscription", "", "subscription of --topic")
	fs.BoolVar(&cfg.target.DeadLetter, "dlq", false, "peek the dead-letter sub-queue")
	fs.IntVar(&cfg.max, "max", defaultPeekMax, "peek at most this many messages")
	fs.Int64Var(&cfg.fromSequence, "from-sequence", 0, "start at this sequence number (0 starts at the head)")
	fs.StringVar(&cfg.payloadTypeID, "payload-type-id", "", "only output messages with this PayloadTypeId property;\nthe filter runs after peeking, so skipped messages still count toward --max")
	fs.StringVar(&output, "output", string(export.FormatTable), "output format: table or json")

	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			fmt.Fprint(helpOut, peekUsageText)
			fs.SetOutput(helpOut)
			fs.PrintDefaults()
		}
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

	return cfg, validatePeekConfig(cfg, hasConnectionString)
}

func validatePeekConfig(cfg peekConfig, hasConnectionString bool) error {
	target := cfg.target
	switch {
	case cfg.namespace == "" && !hasConnectionString:
		return errors.New("--namespace is required")
	case target.Queue == "" && target.Topic == "":
		return errors.New("one of --queue or --topic with --subscription is required")
	case target.Queue != "" && (target.Topic != "" || target.Subscription != ""):
		return errors.New("--queue cannot be combined with --topic or --subscription")
	case target.Queue == "" && target.Subscription == "":
		return errors.New("--topic needs --subscription")
	case cfg.max < 1:
		return fmt.Errorf("--max is %d, want at least 1", cfg.max)
	case cfg.fromSequence < 0:
		return fmt.Errorf("--from-sequence is %d, want 0 or more", cfg.fromSequence)
	}
	return nil
}

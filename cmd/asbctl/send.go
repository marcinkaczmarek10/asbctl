package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"

	"github.com/marcinkaczmarek10/asbctl/internal/bus"
	"github.com/marcinkaczmarek10/asbctl/internal/export"
)

const sendUsageText = `Usage:
  asbctl send --namespace <name|fqdn> (--queue Q | --topic T) [flags]

Send NDJSON records, the format that "asbctl export" writes, to a queue or a topic. Records come from --file,
or from stdin when --file is absent or "-". The whole input is checked before anything is sent, and a record
that does not parse is a usage error that names its line.

Without --yes the command prints the plan (the count per payload type and the target) and sends nothing.
--dry-run prints the same plan and never sends, even with --yes.

A message keeps its MessageId, its application properties and its other fields. Fields that the broker sets
(sequence number, enqueued time, delivery count, dead-letter fields) are ignored. With --new-message-id a
consumer that deduplicates on MessageId will process a resent message twice, because the new id is not the one
it saw before. The original id is kept in the application property asbctl-original-message-id.

With --output json each message is reported as one NDJSON line. The exit code is 0 when every message was
sent, 3 when some were, and 1 when none were.

Authentication uses DefaultAzureCredential (AZURE_TOKEN_CREDENTIALS=dev narrows it to az and azd).
The environment variable ASBCTL_CONNECTION_STRING, when set, is used instead and --namespace is optional.

Flags:
`

type sendConfig struct {
	namespace    string
	queue        string
	topic        string
	file         string
	rate         int
	newMessageID bool
	dryRun       bool
	yes          bool
	format       export.Format
}

func (c sendConfig) target() export.SendTarget {
	return export.SendTarget{Queue: c.queue, Topic: c.topic}
}

// entityName is the queue name, or the topic name when no queue is set.
func (c sendConfig) entityName() string {
	if c.queue != "" {
		return c.queue
	}
	return c.topic
}

func (c sendConfig) readsStdin() bool {
	return c.file == "" || c.file == "-"
}

func runSend(ctx context.Context, args []string, stdin io.Reader, stdout, stderr io.Writer, logger *slog.Logger) int {
	connectionString := os.Getenv(connectionStringEnv)

	cfg, err := parseSendArgs(args, stdout, connectionString != "")
	switch {
	case errors.Is(err, flag.ErrHelp):
		return exitOK
	case err != nil:
		return reportUsageError(stderr, cfg.format, err, "run 'asbctl send -h' for usage")
	}
	if cfg.readsStdin() && isTerminal(stdin) {
		return reportUsageError(stderr, cfg.format, errors.New("no input: pass --file or pipe NDJSON records on stdin"), "run 'asbctl send -h' for usage")
	}

	path, cleanup, err := inputPath(cfg, stdin)
	if err != nil {
		export.WriteError(stderr, cfg.format, "send_failed", err)
		return exitFailure
	}
	defer cleanup()

	counts, err := countByPayloadType(path)
	if err != nil {
		var lineErr *export.LineError
		if errors.As(err, &lineErr) {
			return reportUsageError(stderr, cfg.format, err, "run 'asbctl send -h' for the record format")
		}
		export.WriteError(stderr, cfg.format, "send_failed", err)
		return exitFailure
	}

	plan := export.NewSendPlan(cfg.target(), counts, cfg.dryRun)
	if !cfg.yes || cfg.dryRun || plan.Total == 0 {
		if err := export.WriteSendPlan(stdout, cfg.format, plan); err != nil {
			export.WriteError(stderr, cfg.format, "send_failed", err)
			return exitFailure
		}
		return exitOK
	}

	sendCounts, err := sendFile(ctx, cfg, connectionString, path, stdout, logger)
	return finishSend(ctx, cfg.format, sendCounts, plan.Total, err, stdout, stderr)
}

// finishSend reports the end of a send and picks the exit code from what was sent.
func finishSend(ctx context.Context, format export.Format, counts bus.SendCounts, total int, sendErr error, stdout, stderr io.Writer) int {
	export.WriteSendSummary(stdout, format, counts, total)

	code := sendExitCode(counts.Sent, total)
	switch {
	case sendErr != nil && ctx.Err() != nil:
		export.WriteError(stderr, format, "canceled", fmt.Errorf("stopped after %d of %d messages: %w", counts.Sent, total, sendErr))
	case sendErr != nil:
		export.WriteError(stderr, format, "send_failed", sendErr)
	case code == exitPartial:
		export.WriteError(stderr, format, "partial", fmt.Errorf("%d of %d messages were not sent", total-counts.Sent, total))
	case code == exitFailure:
		export.WriteError(stderr, format, "send_failed", fmt.Errorf("none of %d messages was sent", total))
	}
	return code
}

// sendExitCode is 0 when everything was sent, 3 when some of it was, and 1 when none of it was.
func sendExitCode(sent, total int) int {
	switch {
	case sent >= total:
		return exitOK
	case sent > 0:
		return exitPartial
	default:
		return exitFailure
	}
}

func sendFile(ctx context.Context, cfg sendConfig, connectionString, path string, stdout io.Writer, logger *slog.Logger) (bus.SendCounts, error) {
	client, err := bus.NewClient(cfg.namespace, connectionString)
	if err != nil {
		return bus.SendCounts{}, err
	}
	defer closeAfter(ctx, logger, "close client", client.Close)

	sender, err := client.NewSender(cfg.entityName())
	if err != nil {
		return bus.SendCounts{}, fmt.Errorf("create sender: %w", err)
	}
	defer closeAfter(ctx, logger, "close sender", sender.Close)

	input, err := os.Open(path)
	if err != nil {
		return bus.SendCounts{}, fmt.Errorf("open input: %w", err)
	}
	defer input.Close()

	var newMessageID func() string
	if cfg.newMessageID {
		newMessageID = bus.NewMessageID
	}
	return bus.Send(ctx, sender, bus.NewPacer(cfg.rate), bus.BatchSize(cfg.rate), export.Outgoings(input, newMessageID),
		func(result bus.SendResult) error { return export.WriteSendResult(stdout, cfg.format, result) })
}

// countByPayloadType reads the whole input once, so that a bad record stops the run before anything is sent.
func countByPayloadType(path string) (map[string]int, error) {
	input, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open input: %w", err)
	}
	defer input.Close()

	counts := make(map[string]int)
	for outgoing, err := range export.Outgoings(input, nil) {
		if err != nil {
			return nil, err
		}
		counts[outgoing.PayloadTypeID]++
	}
	return counts, nil
}

// inputPath returns a file that can be read twice, once to check it and once to send it. Stdin is copied to a
// private temporary file for that, which cleanup removes.
func inputPath(cfg sendConfig, stdin io.Reader) (path string, cleanup func(), err error) {
	if !cfg.readsStdin() {
		return cfg.file, func() {}, nil
	}

	spool, err := os.CreateTemp("", "asbctl-send-*.ndjson")
	if err != nil {
		return "", nil, fmt.Errorf("create temporary input file: %w", err)
	}
	cleanup = func() { _ = os.Remove(spool.Name()) }

	if _, err := io.Copy(spool, stdin); err != nil {
		cleanup()
		return "", nil, errors.Join(fmt.Errorf("read stdin: %w", err), spool.Close())
	}
	if err := spool.Close(); err != nil {
		cleanup()
		return "", nil, fmt.Errorf("write temporary input file: %w", err)
	}
	return spool.Name(), cleanup, nil
}

// isTerminal reports whether r is an interactive terminal, where waiting for records would hang.
func isTerminal(r io.Reader) bool {
	file, ok := r.(*os.File)
	if !ok {
		return false
	}
	info, err := file.Stat()
	return err == nil && info.Mode()&os.ModeCharDevice != 0
}

// parseSendArgs turns the command line into a validated config. Help text goes to helpOut.
func parseSendArgs(args []string, helpOut io.Writer, hasConnectionString bool) (sendConfig, error) {
	var cfg sendConfig
	var output string

	fs := flag.NewFlagSet("send", flag.ContinueOnError)
	fs.StringVar(&cfg.namespace, "namespace", "", "Service Bus namespace: a short name or a fully qualified domain name")
	fs.StringVar(&cfg.queue, "queue", "", "queue to send to")
	fs.StringVar(&cfg.topic, "topic", "", "topic to send to")
	fs.StringVar(&cfg.file, "file", "", "read the records from this file, or from stdin when empty or -")
	fs.IntVar(&cfg.rate, "rate", 0, "send at most this many messages per second (0 is unlimited)")
	fs.BoolVar(&cfg.newMessageID, "new-message-id", false, "give each message a new MessageId and keep the original in asbctl-original-message-id")
	fs.BoolVar(&cfg.dryRun, "dry-run", false, "print the plan and never send, even with --yes")
	fs.BoolVar(&cfg.yes, "yes", false, "send, instead of printing the plan")
	fs.StringVar(&output, "output", string(export.FormatTable), "output format: table or json")

	if err := parseFlagSet(fs, args, sendUsageText, helpOut); err != nil {
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

	return cfg, validateSendConfig(cfg, hasConnectionString)
}

func validateSendConfig(cfg sendConfig, hasConnectionString bool) error {
	switch {
	case cfg.namespace == "" && !hasConnectionString:
		return errors.New("--namespace is required")
	case cfg.queue == "" && cfg.topic == "":
		return errors.New("one of --queue or --topic is required")
	case cfg.queue != "" && cfg.topic != "":
		return errors.New("--queue cannot be combined with --topic")
	case cfg.rate < 0:
		return fmt.Errorf("--rate is %d, want 0 or more", cfg.rate)
	}
	return nil
}

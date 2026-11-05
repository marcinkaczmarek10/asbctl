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

const (
	defaultExportMax = 10000
	progressEvery    = 1000
)

const exportUsageText = `Usage:
  asbctl export --namespace <name|fqdn> (--queue Q | --topic T --subscription S) [flags]

Write the messages of an entity as NDJSON records, in the format described in docs/output-format.md.
Messages are peeked, never locked or removed.

Records go to --file, or to stdout when --file is absent or "-". With --file the result is printed on stdout:
one JSON object with --output json, one sentence with --output table (the default). Without --file the
records are JSON and --output table is a usage error. The file is written as <file>.tmp and renamed when
the export succeeds. An existing file is an error unless --force is given.

Authentication uses DefaultAzureCredential (AZURE_TOKEN_CREDENTIALS=dev narrows it to az and azd).
The environment variable ASBCTL_CONNECTION_STRING, when set, is used instead and --namespace is optional.

Flags:
`

type exportConfig struct {
	target        bus.Target
	namespace     string
	max           int
	payloadTypeID string
	file          string
	force         bool
	format        export.Format
}

func (c exportConfig) writesToStdout() bool {
	return c.file == "" || c.file == "-"
}

func runExport(ctx context.Context, args []string, stdout, stderr io.Writer, logger *slog.Logger) int {
	connectionString := os.Getenv(connectionStringEnv)

	cfg, err := parseExportArgs(args, stdout, connectionString != "")
	switch {
	case errors.Is(err, flag.ErrHelp):
		return exitOK
	case err != nil:
		return reportUsageError(stderr, cfg.format, err, "run 'asbctl export -h' for usage")
	}

	var result export.MessagesResult
	writeRecords := func(w io.Writer) error {
		var err error
		result, err = exportRecords(ctx, cfg, connectionString, w, logger)
		return err
	}
	if cfg.writesToStdout() {
		err = writeRecords(stdout)
	} else {
		err = export.WriteFile(cfg.file, cfg.force, writeRecords)
	}
	if err != nil {
		export.WriteError(stderr, cfg.format, exportErrorCode(ctx, err), err)
		return exitFailure
	}

	logger.InfoContext(ctx, "export finished", "exported", result.Exported, "file", cfg.file)
	if cfg.writesToStdout() {
		return exitOK
	}
	err = export.WriteExportResult(stdout, cfg.format, export.ExportResult{
		Schema: export.Schema, Exported: result.Exported, File: cfg.file, Partial: result.Partial,
	})
	if err != nil {
		export.WriteError(stderr, cfg.format, "export_failed", err)
		return exitFailure
	}
	return exitOK
}

func exportErrorCode(ctx context.Context, err error) string {
	switch {
	case errors.Is(err, export.ErrFileExists):
		return "file_exists"
	case ctx.Err() != nil:
		return "canceled"
	default:
		return "export_failed"
	}
}

func exportRecords(ctx context.Context, cfg exportConfig, connectionString string, w io.Writer, logger *slog.Logger) (export.MessagesResult, error) {
	client, err := bus.NewClient(cfg.namespace, connectionString)
	if err != nil {
		return export.MessagesResult{}, err
	}
	defer closeAfter(ctx, logger, "close client", client.Close)

	receiver, err := client.NewReceiver(cfg.target)
	if err != nil {
		return export.MessagesResult{}, fmt.Errorf("create receiver: %w", err)
	}
	defer closeAfter(ctx, logger, "close receiver", receiver.Close)

	loggedPeeked := 0
	return export.Messages(ctx, receiver, export.MessagesOptions{Max: cfg.max, PayloadTypeID: cfg.payloadTypeID}, w,
		func(peeked, exported int) {
			if crossedProgressMark(loggedPeeked, peeked) {
				logger.InfoContext(ctx, "export progress", "peeked", peeked, "exported", exported)
			}
			loggedPeeked = peeked
		})
}

// crossedProgressMark is true when the peek count moved past a multiple of progressEvery.
func crossedProgressMark(previous, current int) bool {
	return current/progressEvery > previous/progressEvery
}

// parseExportArgs turns the command line into a validated config. Help text goes to helpOut.
func parseExportArgs(args []string, helpOut io.Writer, hasConnectionString bool) (exportConfig, error) {
	var cfg exportConfig
	var output string

	fs := flag.NewFlagSet("export", flag.ContinueOnError)
	bindEntityFlags(fs, &cfg.namespace, &cfg.target)
	fs.BoolVar(&cfg.target.DeadLetter, "dlq", false, "export the dead-letter sub-queue")
	fs.IntVar(&cfg.max, "max", defaultExportMax, "peek at most this many messages")
	fs.StringVar(&cfg.payloadTypeID, "payload-type-id", "", "only write messages with this PayloadTypeId property;\nthe filter runs after peeking, so skipped messages still count toward --max")
	fs.StringVar(&cfg.file, "file", "", "write the records to this file, or to stdout when empty or -")
	fs.BoolVar(&cfg.force, "force", false, "overwrite --file when it exists")
	fs.StringVar(&output, "output", "", "result format with --file: table (default) or json; records without --file are always json")

	if err := parseFlagSet(fs, args, exportUsageText, helpOut); err != nil {
		return cfg, err
	}
	if fs.NArg() > 0 {
		return cfg, fmt.Errorf("unexpected argument %q", fs.Arg(0))
	}

	format, err := resolveExportFormat(output, cfg.writesToStdout())
	if err != nil {
		return cfg, err
	}
	cfg.format = format

	if err := validateEntity(cfg.namespace, cfg.target, hasConnectionString); err != nil {
		return cfg, err
	}
	switch {
	case cfg.max < 1:
		return cfg, fmt.Errorf("--max is %d, want at least 1", cfg.max)
	case cfg.force && cfg.writesToStdout():
		return cfg, errors.New("--force needs --file")
	}
	return cfg, nil
}

// resolveExportFormat picks the format of what goes to stdout. Records are always json, so a table is only
// possible for the result line that follows a --file export.
func resolveExportFormat(output string, writesToStdout bool) (export.Format, error) {
	if output == "" {
		if writesToStdout {
			return export.FormatJSON, nil
		}
		return export.FormatTable, nil
	}

	format, err := export.ParseFormat(output)
	if err != nil {
		return "", err
	}
	if writesToStdout && format == export.FormatTable {
		return "", errors.New("--output table cannot be used when the records go to stdout, use --file or --output json")
	}
	return format, nil
}

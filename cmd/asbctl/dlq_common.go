package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"strings"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
	"github.com/marcinkaczmarek10/asbctl/internal/bus"
	"github.com/marcinkaczmarek10/asbctl/internal/dlq"
	"github.com/marcinkaczmarek10/asbctl/internal/export"
)

// defaultMaxScan is how many messages a run holds locked while it reads past the ones it skips.
const defaultMaxScan = 50000

// selectionConfig holds what dlq resend and dlq purge share: where the messages are, which of them, and whether
// to act.
type selectionConfig struct {
	namespace string
	target    bus.Target
	filter    dlq.Filter
	max       int
	maxScan   int
	snapshot  string
	dryRun    bool
	yes       bool
	format    export.Format
}

// acts is true when the command changes messages. Without --yes, or with --dry-run, it only prints the plan.
func (c selectionConfig) acts() bool {
	return c.yes && !c.dryRun
}

// bindSelectionFlags registers the flags that both dead-letter commands take. output holds the --output value.
func bindSelectionFlags(fs *flag.FlagSet, cfg *selectionConfig, output *string) {
	bindEntityFlags(fs, &cfg.namespace, &cfg.target)
	fs.StringVar(&cfg.filter.PayloadTypeID, "payload-type-id", "", "only act on messages with this PayloadTypeId property")
	fs.StringVar(&cfg.filter.Reason, "reason", "", "only act on messages with this dead-letter reason")
	fs.IntVar(&cfg.max, "max", 0, "act on at most this many messages (0 is all that match)")
	fs.IntVar(&cfg.maxScan, "max-scan", defaultMaxScan, "stop after holding this many skipped messages locked while reading to the selected ones; the rest are reported as not reached (0 is no limit)")
	fs.StringVar(&cfg.snapshot, "snapshot", "", "write the snapshot to this path (default asbctl-snapshot-<entity>-<UTC time>.ndjson)")
	fs.BoolVar(&cfg.dryRun, "dry-run", false, "print the plan and change nothing, even with --yes; writes no snapshot")
	fs.BoolVar(&cfg.yes, "yes", false, "act, instead of printing the plan")
	fs.StringVar(output, "output", string(export.FormatTable), "output format: table or json")
}

// finishSelectionConfig parses --output, fills in the snapshot path and checks the shared flags.
func finishSelectionConfig(cfg *selectionConfig, output string, now time.Time, hasConnectionString bool) error {
	format, err := export.ParseFormat(output)
	if err != nil {
		return err
	}
	cfg.format = format
	cfg.target.DeadLetter = true
	if cfg.snapshot == "" {
		cfg.snapshot = dlq.DefaultSnapshotPath(cfg.target, now)
	}

	if err := validateEntity(cfg.namespace, cfg.target, hasConnectionString); err != nil {
		return err
	}
	switch {
	case cfg.max < 0:
		return fmt.Errorf("--max is %d, want 0 or more", cfg.max)
	case cfg.maxScan < 0:
		return fmt.Errorf("--max-scan is %d, want 0 or more", cfg.maxScan)
	}
	return nil
}

// selectMessages peeks the dead-letter queue and applies the filter. When the command acts it writes the
// snapshot in the same pass, before any message is touched.
func selectMessages(ctx context.Context, peeker bus.Peeker, cfg selectionConfig) (dlq.Selection, error) {
	if cfg.acts() {
		return dlq.SelectWithSnapshot(ctx, peeker, cfg.filter, cfg.max, cfg.snapshot)
	}
	return dlq.Select(ctx, peeker, cfg.filter, cfg.max, func(*azservicebus.ReceivedMessage) error { return nil })
}

// namespaceKey names the namespace in the resend ledger: the flag when there is one, and the host in the
// connection string otherwise.
func namespaceKey(namespace, connectionString string) string {
	if namespace != "" {
		return strings.ToLower(bus.FullyQualifiedNamespace(namespace))
	}
	for part := range strings.SplitSeq(connectionString, ";") {
		if value, found := strings.CutPrefix(part, "Endpoint=sb://"); found {
			return strings.ToLower(strings.TrimSuffix(value, "/"))
		}
	}
	return ""
}

// dlqErrorCode names a failed dead-letter run for the error object.
func dlqErrorCode(ctx context.Context, err error, failed string) string {
	switch {
	case errors.Is(err, export.ErrFileExists):
		return "file_exists"
	case ctx.Err() != nil:
		return "canceled"
	default:
		return failed
	}
}

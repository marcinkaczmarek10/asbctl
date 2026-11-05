package main

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"strings"
	"testing"
	"time"

	"github.com/marcinkaczmarek10/asbctl/internal/bus"
	"github.com/marcinkaczmarek10/asbctl/internal/dlq"
	"github.com/marcinkaczmarek10/asbctl/internal/export"
)

var testNow = time.Date(2026, 11, 5, 14, 30, 5, 0, time.UTC)

func TestParseResendArgs(t *testing.T) {
	tests := []struct {
		name                string
		args                []string
		hasConnectionString bool
		check               func(t *testing.T, cfg resendConfig)
		wantErr             string
	}{
		{
			name: "queue with defaults resends to itself",
			args: []string{"--namespace", "n", "--queue", "orders"},
			check: func(t *testing.T, cfg resendConfig) {
				if cfg.destination() != (dlq.Location{Queue: "orders"}) || cfg.ledgerTarget() != "queue:orders" {
					t.Errorf("destination = %+v, ledger target %q, want the source queue", cfg.destination(), cfg.ledgerTarget())
				}
				if cfg.wave != 100 || cfg.stopOnBounce != 5 || cfg.rate != 0 || cfg.watch != 0 || cfg.max != 0 || cfg.newMessageID || cfg.acts() {
					t.Errorf("config = %+v, want the documented defaults and no action without --yes", cfg)
				}
				if !cfg.target.DeadLetter || cfg.snapshot != "asbctl-snapshot-orders-20261105T143005Z.ndjson" || cfg.format != export.FormatTable {
					t.Errorf("config = %+v, want the dead-letter queue, a default snapshot path and table output", cfg)
				}
			},
		},
		{
			name: "every flag",
			args: []string{
				"--namespace", "n", "--topic", "t", "--subscription", "s", "--to-topic", "t2", "--payload-type-id", "P", "--reason", "R",
				"--max", "50", "--rate", "20", "--wave", "10", "--watch", "2m", "--stop-on-bounce", "2.5", "--new-message-id",
				"--snapshot", "snap.ndjson", "--dry-run", "--yes", "--output", "json",
			},
			check: func(t *testing.T, cfg resendConfig) {
				if cfg.destination() != (dlq.Location{Topic: "t2"}) || cfg.ledgerTarget() != "topic:t2" {
					t.Errorf("destination = %+v, want topic t2", cfg.destination())
				}
				if cfg.filter != (dlq.Filter{PayloadTypeID: "P", Reason: "R"}) || cfg.max != 50 || cfg.rate != 20 || cfg.wave != 10 ||
					cfg.watch != 2*time.Minute || cfg.stopOnBounce != 2.5 || !cfg.newMessageID || cfg.snapshot != "snap.ndjson" ||
					!cfg.dryRun || !cfg.yes || cfg.format != export.FormatJSON {
					t.Errorf("config = %+v, want every flag read", cfg)
				}
				if cfg.acts() {
					t.Error("acts() = true with --dry-run, want false even with --yes")
				}
			},
		},
		{
			name: "yes alone acts",
			args: []string{"--namespace", "n", "--queue", "q", "--yes"},
			check: func(t *testing.T, cfg resendConfig) {
				if !cfg.acts() {
					t.Error("acts() = false with --yes, want true")
				}
			},
		},
		{
			name: "subscription with a target queue",
			args: []string{"--namespace", "n", "--topic", "t", "--subscription", "s", "--to-queue", "other"},
			check: func(t *testing.T, cfg resendConfig) {
				if cfg.destination() != (dlq.Location{Queue: "other"}) || cfg.ledgerTarget() != "queue:other" {
					t.Errorf("destination = %+v, want queue other", cfg.destination())
				}
			},
		},
		{
			name:                "connection string makes the namespace optional",
			args:                []string{"--queue", "q"},
			hasConnectionString: true,
			check:               func(*testing.T, resendConfig) {},
		},
		{name: "a subscription needs a target", args: []string{"--namespace", "n", "--topic", "t", "--subscription", "s"}, wantErr: "has no default target"},
		{name: "queue with topic target is fine but not both targets", args: []string{"--namespace", "n", "--queue", "q", "--to-queue", "a", "--to-topic", "b"}, wantErr: "cannot be combined"},
		{name: "target pointing at a subscription", args: []string{"--namespace", "n", "--queue", "q", "--to-topic", "events/Subscriptions/billing"}, wantErr: "cannot point at a subscription"},
		{name: "queue target pointing at a subscription in any case", args: []string{"--namespace", "n", "--queue", "q", "--to-queue", "t/SUBSCRIPTIONS/s"}, wantErr: "cannot point at a subscription"},
		{name: "namespace is required", args: []string{"--queue", "q"}, wantErr: "--namespace is required"},
		{name: "an entity is required", args: []string{"--namespace", "n"}, wantErr: "--queue or --topic"},
		{name: "rate cannot be negative", args: []string{"--namespace", "n", "--queue", "q", "--rate", "-1"}, wantErr: "--rate is -1"},
		{name: "wave must be positive", args: []string{"--namespace", "n", "--queue", "q", "--wave", "0"}, wantErr: "--wave is 0"},
		{name: "watch cannot be negative", args: []string{"--namespace", "n", "--queue", "q", "--watch", "-1s"}, wantErr: "--watch is -1s"},
		{name: "stop on bounce above 100", args: []string{"--namespace", "n", "--queue", "q", "--stop-on-bounce", "101"}, wantErr: "--stop-on-bounce is 101"},
		{name: "stop on bounce below 0", args: []string{"--namespace", "n", "--queue", "q", "--stop-on-bounce", "-1"}, wantErr: "--stop-on-bounce is -1"},
		{name: "stop on bounce of 100 is allowed", args: []string{"--namespace", "n", "--queue", "q", "--stop-on-bounce", "100"}, check: func(*testing.T, resendConfig) {}},
		{name: "max cannot be negative", args: []string{"--namespace", "n", "--queue", "q", "--max", "-1"}, wantErr: "--max is -1"},
		{name: "unknown output", args: []string{"--namespace", "n", "--queue", "q", "--output", "yaml"}, wantErr: `unknown output format "yaml"`},
		{name: "positional argument", args: []string{"--namespace", "n", "--queue", "q", "extra"}, wantErr: `unexpected argument "extra"`},
		{name: "there is no dlq flag", args: []string{"--namespace", "n", "--queue", "q", "--dlq"}, wantErr: "flag provided but not defined"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var help bytes.Buffer

			cfg, err := parseResendArgs(tc.args, &help, tc.hasConnectionString, testNow)

			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("parseResendArgs(%q) error = %v, want it to contain %q", tc.args, err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("parseResendArgs(%q) error = %v, want nil", tc.args, err)
			}
			tc.check(t, cfg)
		})
	}
}

func TestParseResendArgsHelp(t *testing.T) {
	var help bytes.Buffer

	_, err := parseResendArgs([]string{"-h"}, &help, false, testNow)

	if !errors.Is(err, flag.ErrHelp) {
		t.Fatalf("parseResendArgs(-h) error = %v, want flag.ErrHelp", err)
	}
	for _, want := range []string{
		"asbctl dlq resend --namespace", "reaches every matching subscription", "deduplicates on MessageId", "assumed on with a window of 10 minutes",
		"-stop-on-bounce", "-to-topic", "-wave", "-watch", "-snapshot", "Bounce detection reads the source dead-letter queue only",
	} {
		if !strings.Contains(help.String(), want) {
			t.Errorf("help does not contain %q", want)
		}
	}
}

func TestParsePurgeArgs(t *testing.T) {
	var help bytes.Buffer

	cfg, err := parsePurgeArgs([]string{"--namespace", "n", "--topic", "t", "--subscription", "s", "--reason", "R", "--max", "5", "--yes"}, &help, false, testNow)

	if err != nil {
		t.Fatalf("parsePurgeArgs() error = %v", err)
	}
	if cfg.target != (bus.Target{Topic: "t", Subscription: "s", DeadLetter: true}) || cfg.filter.Reason != "R" || cfg.max != 5 || !cfg.acts() ||
		cfg.snapshot != "asbctl-snapshot-t-s-20261105T143005Z.ndjson" {
		t.Errorf("config = %+v, want the subscription dead-letter queue, the filter, max 5 and a default snapshot", cfg)
	}
	for _, args := range [][]string{{"--queue", "q"}, {"--namespace", "n"}, {"--namespace", "n", "--queue", "q", "extra"}, {"--namespace", "n", "--queue", "q", "--max", "-1"}, {"--namespace", "n", "--queue", "q", "--to-queue", "x"}} {
		if _, err := parsePurgeArgs(args, &help, false, testNow); err == nil {
			t.Errorf("parsePurgeArgs(%q) error = nil, want an error", args)
		}
	}
}

func TestParsePurgeArgsHelp(t *testing.T) {
	var help bytes.Buffer

	_, err := parsePurgeArgs([]string{"-h"}, &help, false, testNow)

	if !errors.Is(err, flag.ErrHelp) || !strings.Contains(help.String(), "never in delete") || !strings.Contains(help.String(), "-snapshot") {
		t.Errorf("parsePurgeArgs(-h) = %v, help %q, want ErrHelp and the safety text", err, help.String())
	}
}

func TestNamespaceKey(t *testing.T) {
	tests := []struct {
		name             string
		namespace        string
		connectionString string
		want             string
	}{
		{name: "short name", namespace: "Contoso", want: "contoso.servicebus.windows.net"},
		{name: "fqdn", namespace: "Contoso.ServiceBus.Windows.net", want: "contoso.servicebus.windows.net"},
		{name: "from the connection string", connectionString: "Endpoint=sb://Contoso.servicebus.windows.net/;SharedAccessKeyName=k;SharedAccessKey=secret", want: "contoso.servicebus.windows.net"},
		{name: "the flag wins", namespace: "a", connectionString: "Endpoint=sb://b.servicebus.windows.net/;SharedAccessKey=x", want: "a.servicebus.windows.net"},
		{name: "nothing to go on", want: ""},
		{name: "a connection string without an endpoint", connectionString: "SharedAccessKey=secret", want: ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := namespaceKey(tc.namespace, tc.connectionString)

			if got != tc.want {
				t.Errorf("namespaceKey() = %q, want %q", got, tc.want)
			}
			if strings.Contains(got, "secret") {
				t.Errorf("namespaceKey() = %q leaked part of the connection string", got)
			}
		})
	}
}

func TestDLQErrorCode(t *testing.T) {
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	tests := []struct {
		name string
		ctx  context.Context
		err  error
		want string
	}{
		{name: "snapshot exists", ctx: context.Background(), err: errors.Join(errors.New("snapshot"), export.ErrFileExists), want: "file_exists"},
		{name: "cancelled", ctx: cancelled, err: errors.New("stopped"), want: "canceled"},
		{name: "other", ctx: context.Background(), err: errors.New("boom"), want: "resend_failed"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := dlqErrorCode(tc.ctx, tc.err, "resend_failed"); got != tc.want {
				t.Errorf("dlqErrorCode() = %q, want %q", got, tc.want)
			}
		})
	}
}

package main

import (
	"bytes"
	"errors"
	"flag"
	"reflect"
	"strings"
	"testing"

	"github.com/marcinkaczmarek10/asbctl/internal/bus"
	"github.com/marcinkaczmarek10/asbctl/internal/export"
)

func TestParseSummaryArgs(t *testing.T) {
	tests := []struct {
		name                string
		args                []string
		hasConnectionString bool
		want                summaryConfig
		wantErr             string
	}{
		{
			name: "queue with defaults reads the dead-letter queue",
			args: []string{"--namespace", "contoso", "--queue", "orders"},
			want: summaryConfig{namespace: "contoso", target: bus.Target{Queue: "orders", DeadLetter: true}, max: 10000, format: export.FormatTable},
		},
		{
			name: "subscription with every flag",
			args: []string{"--namespace", "n", "--topic", "t", "--subscription", "s", "--max", "25", "--output", "json"},
			want: summaryConfig{namespace: "n", target: bus.Target{Topic: "t", Subscription: "s", DeadLetter: true}, max: 25, format: export.FormatJSON},
		},
		{
			name:                "connection string makes the namespace optional",
			args:                []string{"--queue", "orders"},
			hasConnectionString: true,
			want:                summaryConfig{target: bus.Target{Queue: "orders", DeadLetter: true}, max: 10000, format: export.FormatTable},
		},
		{name: "namespace is required without a connection string", args: []string{"--queue", "q"}, wantErr: "--namespace is required"},
		{name: "an entity is required", args: []string{"--namespace", "n"}, wantErr: "--queue or --topic"},
		{name: "topic needs a subscription", args: []string{"--namespace", "n", "--topic", "t"}, wantErr: "--topic needs --subscription"},
		{name: "max must be positive", args: []string{"--namespace", "n", "--queue", "q", "--max", "0"}, wantErr: "--max is 0"},
		{
			name: "max of one is accepted",
			args: []string{"--namespace", "n", "--queue", "q", "--max", "1"},
			want: summaryConfig{namespace: "n", target: bus.Target{Queue: "q", DeadLetter: true}, max: 1, format: export.FormatTable},
		},
		{name: "unknown output format", args: []string{"--namespace", "n", "--queue", "q", "--output", "yaml"}, wantErr: `unknown output format "yaml"`},
		{name: "dlq flag does not exist because the target is always the dead-letter queue", args: []string{"--dlq"}, wantErr: "flag provided but not defined"},
		{name: "positional argument", args: []string{"--namespace", "n", "--queue", "q", "extra"}, wantErr: `unexpected argument "extra"`},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var help bytes.Buffer

			got, err := parseSummaryArgs(tc.args, &help, tc.hasConnectionString)

			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("parseSummaryArgs(%q) error = %v, want it to contain %q", tc.args, err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("parseSummaryArgs(%q) error = %v, want nil", tc.args, err)
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("parseSummaryArgs(%q) = %+v, want %+v", tc.args, got, tc.want)
			}
		})
	}
}

func TestParseSummaryArgsHelpWritesUsageToTheHelpWriter(t *testing.T) {
	var help bytes.Buffer

	_, err := parseSummaryArgs([]string{"-h"}, &help, false)

	if !errors.Is(err, flag.ErrHelp) {
		t.Fatalf("parseSummaryArgs(-h) error = %v, want flag.ErrHelp", err)
	}
	for _, want := range []string{"asbctl dlq summary --namespace", "Manage rights", "-max"} {
		if !strings.Contains(help.String(), want) {
			t.Errorf("help = %q, want it to contain %q", help.String(), want)
		}
	}
}

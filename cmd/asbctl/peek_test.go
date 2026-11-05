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

func TestParsePeekArgs(t *testing.T) {
	tests := []struct {
		name                string
		args                []string
		hasConnectionString bool
		want                peekConfig
		wantErr             string
	}{
		{
			name: "queue with defaults",
			args: []string{"--namespace", "contoso", "--queue", "orders"},
			want: peekConfig{namespace: "contoso", target: bus.Target{Queue: "orders"}, max: 50, format: export.FormatTable},
		},
		{
			name: "subscription dead-letter with every flag",
			args: []string{"--namespace", "contoso.servicebus.windows.net", "--topic", "t", "--subscription", "s", "--dlq", "--max", "7", "--from-sequence", "99", "--payload-type-id", "Order", "--output", "json"},
			want: peekConfig{
				namespace: "contoso.servicebus.windows.net", target: bus.Target{Topic: "t", Subscription: "s", DeadLetter: true},
				max: 7, fromSequence: 99, payloadTypeID: "Order", format: export.FormatJSON,
			},
		},
		{
			name:                "connection string makes the namespace optional",
			args:                []string{"--queue", "orders"},
			hasConnectionString: true,
			want:                peekConfig{target: bus.Target{Queue: "orders"}, max: 50, format: export.FormatTable},
		},
		{
			name: "max of one is accepted",
			args: []string{"--namespace", "n", "--queue", "q", "--max", "1"},
			want: peekConfig{namespace: "n", target: bus.Target{Queue: "q"}, max: 1, format: export.FormatTable},
		},
		{name: "namespace is required without a connection string", args: []string{"--queue", "q"}, wantErr: "--namespace is required"},
		{name: "an entity is required", args: []string{"--namespace", "n"}, wantErr: "--queue or --topic"},
		{name: "queue excludes topic", args: []string{"--namespace", "n", "--queue", "q", "--topic", "t"}, wantErr: "cannot be combined"},
		{name: "queue excludes subscription", args: []string{"--namespace", "n", "--queue", "q", "--subscription", "s"}, wantErr: "cannot be combined"},
		{name: "topic needs a subscription", args: []string{"--namespace", "n", "--topic", "t"}, wantErr: "--topic needs --subscription"},
		{name: "subscription alone is not an entity", args: []string{"--namespace", "n", "--subscription", "s"}, wantErr: "--queue or --topic"},
		{name: "max must be positive", args: []string{"--namespace", "n", "--queue", "q", "--max", "0"}, wantErr: "--max is 0"},
		{name: "from sequence cannot be negative", args: []string{"--namespace", "n", "--queue", "q", "--from-sequence", "-1"}, wantErr: "--from-sequence is -1"},
		{name: "unknown output format", args: []string{"--namespace", "n", "--queue", "q", "--output", "yaml"}, wantErr: `unknown output format "yaml"`},
		{name: "unknown flag", args: []string{"--nope"}, wantErr: "flag provided but not defined"},
		{name: "positional argument", args: []string{"--namespace", "n", "--queue", "q", "extra"}, wantErr: `unexpected argument "extra"`},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var help bytes.Buffer

			got, err := parsePeekArgs(tc.args, &help, tc.hasConnectionString)

			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("parsePeekArgs(%q) error = %v, want it to contain %q", tc.args, err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("parsePeekArgs(%q) error = %v, want nil", tc.args, err)
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("parsePeekArgs(%q) = %+v, want %+v", tc.args, got, tc.want)
			}
		})
	}
}

func TestParsePeekArgsHelpWritesUsageToTheHelpWriter(t *testing.T) {
	var help bytes.Buffer

	_, err := parsePeekArgs([]string{"-h"}, &help, false)

	if !errors.Is(err, flag.ErrHelp) {
		t.Fatalf("parsePeekArgs(-h) error = %v, want flag.ErrHelp", err)
	}
	for _, want := range []string{"asbctl peek --namespace", "-payload-type-id", "skipped messages still count toward --max"} {
		if !strings.Contains(help.String(), want) {
			t.Errorf("help = %q, want it to contain %q", help.String(), want)
		}
	}
}

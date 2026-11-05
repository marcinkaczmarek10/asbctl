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

func TestParseExportArgs(t *testing.T) {
	tests := []struct {
		name                string
		args                []string
		hasConnectionString bool
		want                exportConfig
		wantErr             string
	}{
		{
			name: "records to stdout are json by default",
			args: []string{"--namespace", "contoso", "--queue", "orders"},
			want: exportConfig{namespace: "contoso", target: bus.Target{Queue: "orders"}, max: 10000, format: export.FormatJSON},
		},
		{
			name: "a dash means stdout too",
			args: []string{"--namespace", "n", "--queue", "q", "--file", "-"},
			want: exportConfig{namespace: "n", target: bus.Target{Queue: "q"}, max: 10000, file: "-", format: export.FormatJSON},
		},
		{
			name: "a file result is a table by default",
			args: []string{"--namespace", "n", "--topic", "t", "--subscription", "s", "--dlq", "--max", "50", "--payload-type-id", "P", "--file", "out.ndjson", "--force"},
			want: exportConfig{
				namespace: "n", target: bus.Target{Topic: "t", Subscription: "s", DeadLetter: true}, max: 50,
				payloadTypeID: "P", file: "out.ndjson", force: true, format: export.FormatTable,
			},
		},
		{
			name: "a file result can be json",
			args: []string{"--namespace", "n", "--queue", "q", "--file", "out", "--output", "json"},
			want: exportConfig{namespace: "n", target: bus.Target{Queue: "q"}, max: 10000, file: "out", format: export.FormatJSON},
		},
		{
			name:                "connection string makes the namespace optional",
			args:                []string{"--queue", "q"},
			hasConnectionString: true,
			want:                exportConfig{target: bus.Target{Queue: "q"}, max: 10000, format: export.FormatJSON},
		},
		{name: "table output is refused for records on stdout", args: []string{"--namespace", "n", "--queue", "q", "--output", "table"}, wantErr: "--output table cannot be used"},
		{name: "table output is refused for a dash", args: []string{"--namespace", "n", "--queue", "q", "--file", "-", "--output", "table"}, wantErr: "--output table cannot be used"},
		{name: "unknown output", args: []string{"--namespace", "n", "--queue", "q", "--output", "yaml"}, wantErr: `unknown output format "yaml"`},
		{name: "force needs a file", args: []string{"--namespace", "n", "--queue", "q", "--force"}, wantErr: "--force needs --file"},
		{name: "force needs a file that is not a dash", args: []string{"--namespace", "n", "--queue", "q", "--file", "-", "--force"}, wantErr: "--force needs --file"},
		{name: "namespace is required", args: []string{"--queue", "q"}, wantErr: "--namespace is required"},
		{name: "an entity is required", args: []string{"--namespace", "n"}, wantErr: "--queue or --topic"},
		{name: "max must be positive", args: []string{"--namespace", "n", "--queue", "q", "--max", "0"}, wantErr: "--max is 0"},
		{name: "positional argument", args: []string{"--namespace", "n", "--queue", "q", "extra"}, wantErr: `unexpected argument "extra"`},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var help bytes.Buffer

			got, err := parseExportArgs(tc.args, &help, tc.hasConnectionString)

			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("parseExportArgs(%q) error = %v, want it to contain %q", tc.args, err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("parseExportArgs(%q) error = %v, want nil", tc.args, err)
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("parseExportArgs(%q) = %+v, want %+v", tc.args, got, tc.want)
			}
		})
	}
}

func TestParseExportArgsHelp(t *testing.T) {
	var help bytes.Buffer

	_, err := parseExportArgs([]string{"-h"}, &help, false)

	if !errors.Is(err, flag.ErrHelp) {
		t.Fatalf("parseExportArgs(-h) error = %v, want flag.ErrHelp", err)
	}
	for _, want := range []string{"asbctl export --namespace", "<file>.tmp", "-force", "-file"} {
		if !strings.Contains(help.String(), want) {
			t.Errorf("help = %q, want it to contain %q", help.String(), want)
		}
	}
}

func TestCrossedProgressMark(t *testing.T) {
	tests := []struct {
		previous, current int
		want              bool
	}{
		{0, 100, false},
		{900, 999, false},
		{900, 1000, true},
		{999, 1100, true},
		{1000, 1100, false},
		{1900, 2100, true},
		{0, 3000, true},
	}
	for _, tc := range tests {
		if got := crossedProgressMark(tc.previous, tc.current); got != tc.want {
			t.Errorf("crossedProgressMark(%d, %d) = %v, want %v", tc.previous, tc.current, got, tc.want)
		}
	}
}

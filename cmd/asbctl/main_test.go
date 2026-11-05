package main

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"
)

func TestRun(t *testing.T) {
	tests := []struct {
		name           string
		args           []string
		wantCode       int
		wantStdout     bool
		wantStderrPart string
	}{
		{name: "version prints the version and succeeds", args: []string{"version"}, wantCode: 0, wantStdout: true},
		{name: "help prints usage and succeeds", args: []string{"help"}, wantCode: 0, wantStdout: true},
		{name: "dash h prints usage and succeeds", args: []string{"-h"}, wantCode: 0, wantStdout: true},
		{name: "no command is a usage error", args: nil, wantCode: 2, wantStderrPart: "no command"},
		{name: "unknown command is a usage error that names the command", args: []string{"nope"}, wantCode: 2, wantStderrPart: "nope"},
		{name: "peek help prints usage and succeeds", args: []string{"peek", "-h"}, wantCode: 0, wantStdout: true},
		{name: "peek without an entity is a usage error", args: []string{"peek", "--namespace", "n"}, wantCode: 2, wantStderrPart: "--queue or --topic"},
		{name: "peek with a bad flag is a usage error", args: []string{"peek", "--nope"}, wantCode: 2, wantStderrPart: "flag provided but not defined: -nope"},
		{name: "dlq without a command is a usage error", args: []string{"dlq"}, wantCode: 2, wantStderrPart: "no command given"},
		{name: "dlq with an unknown command is a usage error", args: []string{"dlq", "nope"}, wantCode: 2, wantStderrPart: "nope"},
		{name: "dlq help prints usage and succeeds", args: []string{"dlq", "-h"}, wantCode: 0, wantStdout: true},
		{name: "dlq summary help prints usage and succeeds", args: []string{"dlq", "summary", "-h"}, wantCode: 0, wantStdout: true},
		{name: "dlq summary without an entity is a usage error", args: []string{"dlq", "summary", "--namespace", "n"}, wantCode: 2, wantStderrPart: "--queue or --topic"},
		{name: "unknown flag on version is a usage error", args: []string{"version", "-x"}, wantCode: 2, wantStderrPart: "-x"},
		{name: "extra argument on version is a usage error", args: []string{"version", "extra"}, wantCode: 2, wantStderrPart: "extra"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			logger := slog.New(slog.DiscardHandler)

			got := run(context.Background(), tc.args, &stdout, &stderr, logger)

			if got != tc.wantCode {
				t.Errorf("run(%q) = %d, want %d", tc.args, got, tc.wantCode)
			}
			if tc.wantStdout && stdout.Len() == 0 {
				t.Errorf("run(%q) wrote nothing to stdout, want output", tc.args)
			}
			if !tc.wantStdout && stdout.Len() != 0 {
				t.Errorf("run(%q) stdout = %q, want empty", tc.args, stdout.String())
			}
			if !strings.Contains(stderr.String(), tc.wantStderrPart) {
				t.Errorf("run(%q) stderr = %q, want it to contain %q", tc.args, stderr.String(), tc.wantStderrPart)
			}
		})
	}
}

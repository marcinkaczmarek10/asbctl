package main

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"log/slog"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/marcinkaczmarek10/asbctl/internal/bus"
	"github.com/marcinkaczmarek10/asbctl/internal/export"
)

func TestParseSendArgs(t *testing.T) {
	tests := []struct {
		name                string
		args                []string
		hasConnectionString bool
		want                sendConfig
		wantErr             string
	}{
		{
			name: "queue with defaults",
			args: []string{"--namespace", "contoso", "--queue", "orders"},
			want: sendConfig{namespace: "contoso", queue: "orders", format: export.FormatTable},
		},
		{
			name: "topic with every flag",
			args: []string{"--namespace", "n", "--topic", "t", "--file", "in.ndjson", "--rate", "50", "--new-message-id", "--dry-run", "--yes", "--output", "json"},
			want: sendConfig{namespace: "n", topic: "t", file: "in.ndjson", rate: 50, newMessageID: true, dryRun: true, yes: true, format: export.FormatJSON},
		},
		{
			name:                "connection string makes the namespace optional",
			args:                []string{"--queue", "q"},
			hasConnectionString: true,
			want:                sendConfig{queue: "q", format: export.FormatTable},
		},
		{name: "namespace is required", args: []string{"--queue", "q"}, wantErr: "--namespace is required"},
		{name: "a target is required", args: []string{"--namespace", "n"}, wantErr: "--queue or --topic is required"},
		{name: "queue excludes topic", args: []string{"--namespace", "n", "--queue", "q", "--topic", "t"}, wantErr: "cannot be combined"},
		{name: "subscription is not a send flag", args: []string{"--namespace", "n", "--topic", "t", "--subscription", "s"}, wantErr: "flag provided but not defined"},
		{name: "rate cannot be negative", args: []string{"--namespace", "n", "--queue", "q", "--rate", "-1"}, wantErr: "--rate is -1"},
		{name: "rate of zero is unlimited", args: []string{"--namespace", "n", "--queue", "q", "--rate", "0"}, want: sendConfig{namespace: "n", queue: "q", format: export.FormatTable}},
		{name: "unknown output", args: []string{"--namespace", "n", "--queue", "q", "--output", "yaml"}, wantErr: `unknown output format "yaml"`},
		{name: "positional argument", args: []string{"--namespace", "n", "--queue", "q", "extra"}, wantErr: `unexpected argument "extra"`},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var help bytes.Buffer

			got, err := parseSendArgs(tc.args, &help, tc.hasConnectionString)

			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("parseSendArgs(%q) error = %v, want it to contain %q", tc.args, err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("parseSendArgs(%q) error = %v, want nil", tc.args, err)
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("parseSendArgs(%q) = %+v, want %+v", tc.args, got, tc.want)
			}
		})
	}
}

func TestParseSendArgsHelp(t *testing.T) {
	var help bytes.Buffer

	_, err := parseSendArgs([]string{"-h"}, &help, false)

	if !errors.Is(err, flag.ErrHelp) {
		t.Fatalf("parseSendArgs(-h) error = %v, want flag.ErrHelp", err)
	}
	for _, want := range []string{"asbctl send --namespace", "deduplicates on MessageId", "asbctl-original-message-id", "-new-message-id", "-yes"} {
		if !strings.Contains(help.String(), want) {
			t.Errorf("help = %q, want it to contain %q", help.String(), want)
		}
	}
}

func TestSendExitCode(t *testing.T) {
	tests := []struct{ sent, total, want int }{
		{10, 10, 0},
		{0, 0, 0},
		{9, 10, 3},
		{1, 10, 3},
		{0, 10, 1},
	}
	for _, tc := range tests {
		if got := sendExitCode(tc.sent, tc.total); got != tc.want {
			t.Errorf("sendExitCode(%d, %d) = %d, want %d", tc.sent, tc.total, got, tc.want)
		}
	}
}

const sendInput = `{"schema":1,"message_id":"m1","application_properties":{"PayloadTypeId":"OrderPlaced"},"body":"eA=="}` + "\n" +
	`{"schema":1,"message_id":"m2","application_properties":{"PayloadTypeId":"OrderPlaced"},"body":"eQ=="}` + "\n" +
	`{"schema":1,"message_id":"m3","body":""}` + "\n"

func runSendWith(t *testing.T, stdin string, args ...string) (code int, stdout, stderr string) {
	t.Helper()
	var out, errOut bytes.Buffer
	code = run(context.Background(), append([]string{"send"}, args...), strings.NewReader(stdin), &out, &errOut, slog.New(slog.DiscardHandler))
	return code, out.String(), errOut.String()
}

func TestSendPrintsThePlanAndSendsNothingWithoutYes(t *testing.T) {
	code, stdout, stderr := runSendWith(t, sendInput, "--namespace", "n", "--queue", "orders")

	if code != 0 || stderr != "" {
		t.Fatalf("send exit code = %d, stderr = %q, want 0 and no stderr", code, stderr)
	}
	for _, want := range []string{"target:    queue orders", "messages:  3", "2      OrderPlaced", "1      (none)", "Nothing was sent. Pass --yes to send."} {
		if !strings.Contains(stdout, want) {
			t.Errorf("plan = %q, want it to contain %q", stdout, want)
		}
	}
}

func TestSendDryRunNeverSendsEvenWithYes(t *testing.T) {
	code, stdout, _ := runSendWith(t, sendInput, "--namespace", "n", "--topic", "events", "--yes", "--dry-run", "--output", "json")

	want := `{"schema":1,"target":{"topic":"events"},"total":3,"payload_types":[{"payload_type_id":"OrderPlaced","count":2},{"payload_type_id":"","count":1}],"dry_run":true}` + "\n"
	if code != 0 || stdout != want {
		t.Errorf("send = %d, %q, want 0 and %q", code, stdout, want)
	}
}

func TestSendReadsAFileAndPlansWithoutConnecting(t *testing.T) {
	path := filepath.Join(t.TempDir(), "in.ndjson")
	if err := os.WriteFile(path, []byte(sendInput), 0o600); err != nil {
		t.Fatal(err)
	}

	code, stdout, _ := runSendWith(t, "", "--namespace", "n", "--queue", "q", "--file", path)

	if code != 0 || !strings.Contains(stdout, "messages:  3") {
		t.Errorf("send = %d, %q, want 0 and a plan of 3 messages", code, stdout)
	}
}

func TestSendWithNoMessagesPlansEvenWithYes(t *testing.T) {
	code, stdout, _ := runSendWith(t, "\n\n", "--namespace", "n", "--queue", "q", "--yes")

	if code != 0 || !strings.Contains(stdout, "messages:  0") {
		t.Errorf("send = %d, %q, want 0 and a plan of 0 messages without connecting", code, stdout)
	}
}

func TestSendValidatesTheWholeInputBeforeSending(t *testing.T) {
	input := `{"schema":1,"body":""}` + "\n" + `{"schema":1,"body":""}` + "\n" + `not json` + "\n"

	code, stdout, stderr := runSendWith(t, input, "--namespace", "n", "--queue", "q", "--yes")

	if code != 2 || stdout != "" {
		t.Errorf("send = %d, stdout %q, want a usage error and no output", code, stdout)
	}
	if !strings.Contains(stderr, "line 3") {
		t.Errorf("stderr = %q, want it to name line 3", stderr)
	}
}

func TestSendReportsABadLineAsJSONInJSONMode(t *testing.T) {
	code, _, stderr := runSendWith(t, `{"schema":9,"body":""}`+"\n", "--namespace", "n", "--queue", "q", "--output", "json")

	want := `{"schema":1,"error":{"code":"usage","message":"line 1: schema is 9, want 1"}}` + "\n"
	if code != 2 || stderr != want {
		t.Errorf("send = %d, stderr %q, want 2 and %q", code, stderr, want)
	}
}

func TestSendFailsWhenTheFileIsMissing(t *testing.T) {
	code, _, stderr := runSendWith(t, "", "--namespace", "n", "--queue", "q", "--file", filepath.Join(t.TempDir(), "missing"))

	if code != 1 || !strings.Contains(stderr, "open input") {
		t.Errorf("send = %d, stderr %q, want 1 and an open error", code, stderr)
	}
}

func TestSendRemovesItsTemporaryCopyOfStdin(t *testing.T) {
	t.Setenv("TMPDIR", t.TempDir())

	runSendWith(t, sendInput, "--namespace", "n", "--queue", "q")

	entries, _ := os.ReadDir(os.TempDir())
	if len(entries) != 0 {
		t.Errorf("temporary directory has %d entries after send, want none", len(entries))
	}
}

func TestFinishSend(t *testing.T) {
	tests := []struct {
		name       string
		format     export.Format
		counts     bus.SendCounts
		total      int
		sendErr    error
		wantCode   int
		wantStdout string
		wantStderr string
	}{
		{name: "all sent in table mode", format: export.FormatTable, counts: bus.SendCounts{Sent: 3}, total: 3, wantCode: 0, wantStdout: "Sent 3 of 3 messages, 0 failed.\n"},
		{name: "all sent in json mode is silent", format: export.FormatJSON, counts: bus.SendCounts{Sent: 3}, total: 3, wantCode: 0},
		{
			name: "partial", format: export.FormatJSON, counts: bus.SendCounts{Sent: 2, Failed: 1}, total: 3, wantCode: 3,
			wantStderr: `{"schema":1,"error":{"code":"partial","message":"1 of 3 messages were not sent"}}` + "\n",
		},
		{
			name: "none sent", format: export.FormatJSON, counts: bus.SendCounts{Failed: 3}, total: 3, wantCode: 1,
			wantStderr: `{"schema":1,"error":{"code":"send_failed","message":"none of 3 messages was sent"}}` + "\n",
		},
		{
			name: "an error from the send", format: export.FormatJSON, counts: bus.SendCounts{Sent: 1}, total: 3, sendErr: errors.New("read input: gone"), wantCode: 3,
			wantStderr: `{"schema":1,"error":{"code":"send_failed","message":"read input: gone"}}` + "\n",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var out, errOut bytes.Buffer

			code := finishSend(context.Background(), tc.format, tc.counts, tc.total, tc.sendErr, &out, &errOut)

			if code != tc.wantCode || out.String() != tc.wantStdout || errOut.String() != tc.wantStderr {
				t.Errorf("finishSend() = %d, stdout %q, stderr %q, want %d, %q, %q", code, out.String(), errOut.String(), tc.wantCode, tc.wantStdout, tc.wantStderr)
			}
		})
	}
}

func TestFinishSendNamesACancelledRun(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var out, errOut bytes.Buffer

	code := finishSend(ctx, export.FormatJSON, bus.SendCounts{Sent: 40}, 100, context.Canceled, &out, &errOut)

	want := `{"schema":1,"error":{"code":"canceled","message":"stopped after 40 of 100 messages: context canceled"}}` + "\n"
	if code != 3 || errOut.String() != want {
		t.Errorf("finishSend() = %d, stderr %q, want 3 and %q", code, errOut.String(), want)
	}
}

func TestIsTerminalIsFalseForANonFileReader(t *testing.T) {
	if isTerminal(strings.NewReader("")) {
		t.Error("isTerminal(strings.Reader) = true, want false")
	}
	file, err := os.Open(filepath.Join(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	if isTerminal(file) {
		t.Error("isTerminal(directory) = true, want false")
	}
}

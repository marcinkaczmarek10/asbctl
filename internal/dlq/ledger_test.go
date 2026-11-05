package dlq

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestLedgerRecentReturnsTheIDsSentInsideTheWindowToTheSameTarget(t *testing.T) {
	ledger, _ := openTestLedger(t)
	now := runStart
	entries := []LedgerEntry{
		{"ns", "queue:orders", "fresh", now.Add(-time.Minute)},
		{"ns", "queue:orders", "edge-inside", now.Add(-10*time.Minute + time.Second)},
		{"ns", "queue:orders", "edge-outside", now.Add(-10 * time.Minute)},
		{"ns", "queue:orders", "old", now.Add(-time.Hour)},
		{"ns", "queue:other", "other-target", now.Add(-time.Minute)},
		{"other-ns", "queue:orders", "other-namespace", now.Add(-time.Minute)},
		{"ns", "queue:orders", "future", now.Add(time.Hour)},
	}
	for _, entry := range entries {
		if err := ledger.Append(entry); err != nil {
			t.Fatalf("Append() error = %v", err)
		}
	}

	got, err := ledger.Recent("ns", "queue:orders", 10*time.Minute, now)

	if err != nil {
		t.Fatalf("Recent() error = %v", err)
	}
	want := map[string]struct{}{"fresh": {}, "edge-inside": {}, "future": {}}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("Recent() = %v, want %v", got, want)
	}
}

func TestLedgerRecentOfAMissingFileIsEmpty(t *testing.T) {
	got, err := RecentFromFile(filepath.Join(t.TempDir(), "none.ndjson"), "ns", "t", time.Hour, runStart)

	if err != nil || len(got) != 0 {
		t.Errorf("RecentFromFile() = %v, %v, want an empty set and no error", got, err)
	}
}

func TestLedgerRecentSkipsLinesThatDoNotParse(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ledger.ndjson")
	content := "not json\n\n" +
		`{"namespace":"ns","target":"t","message_id":"good","sent_at":"2026-11-05T11:59:00Z"}` + "\n" +
		`{"namespace":"ns","target":"t","message_id":"torn","sent_at":"2026-11-05T11:`
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}

	got, err := RecentFromFile(path, "ns", "t", time.Hour, runStart)

	if err != nil || !reflect.DeepEqual(got, map[string]struct{}{"good": {}}) {
		t.Errorf("RecentFromFile() = %v, %v, want only the good line", got, err)
	}
}

func TestLedgerAppendsAcrossOpens(t *testing.T) {
	ledger, path := openTestLedger(t)
	_ = ledger.Append(LedgerEntry{"ns", "t", "first", runStart})
	_ = ledger.Close()

	again, err := OpenLedger(path)
	if err != nil {
		t.Fatalf("OpenLedger() error = %v", err)
	}
	t.Cleanup(func() { _ = again.Close() })
	_ = again.Append(LedgerEntry{"ns", "t", "second", runStart})

	data, _ := os.ReadFile(path)
	if lines := strings.Count(string(data), "\n"); lines != 2 {
		t.Errorf("ledger has %d lines, want 2 after two opens", lines)
	}
	info, _ := os.Stat(path)
	if info.Mode().Perm() != 0o600 {
		t.Errorf("ledger mode = %v, want 0600", info.Mode().Perm())
	}
}

func TestOpenLedgerFailsWhenTheDirectoryCannotBeCreated(t *testing.T) {
	blocker := filepath.Join(t.TempDir(), "file")
	if err := os.WriteFile(blocker, nil, 0o600); err != nil {
		t.Fatal(err)
	}

	if _, err := OpenLedger(filepath.Join(blocker, "sub", "ledger.ndjson")); err == nil {
		t.Error("OpenLedger() error = nil, want an error when a parent is a file")
	}
}

func TestDefaultLedgerPath(t *testing.T) {
	tests := []struct {
		name string
		env  map[string]string
		home string
		want string
	}{
		{name: "xdg state home", env: map[string]string{"XDG_STATE_HOME": "/state"}, home: "/home/me", want: "/state/asbctl/resend-ledger.ndjson"},
		{name: "fallback", env: map[string]string{}, home: "/home/me", want: "/home/me/.local/state/asbctl/resend-ledger.ndjson"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := DefaultLedgerPath(func(key string) string { return tc.env[key] }, tc.home)

			if got != tc.want {
				t.Errorf("DefaultLedgerPath() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestWithinWindow(t *testing.T) {
	now := runStart
	tests := []struct {
		name   string
		sentAt time.Time
		want   bool
	}{
		{name: "just sent", sentAt: now, want: true},
		{name: "inside", sentAt: now.Add(-9 * time.Minute), want: true},
		{name: "exactly the window ago", sentAt: now.Add(-10 * time.Minute), want: false},
		{name: "outside", sentAt: now.Add(-11 * time.Minute), want: false},
		{name: "in the future", sentAt: now.Add(time.Minute), want: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := withinWindow(tc.sentAt, now, 10*time.Minute); got != tc.want {
				t.Errorf("withinWindow() = %v, want %v", got, tc.want)
			}
		})
	}
}

package dlq

import (
	"encoding/json"
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
	got, err := RecentFromDir(filepath.Join(t.TempDir(), "none"), "ns", "t", time.Hour, runStart)

	if err != nil || len(got) != 0 {
		t.Errorf("RecentFromDir() = %v, %v, want an empty set and no error", got, err)
	}
}

func TestLedgerRecentSkipsLinesThatDoNotParse(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "resend-ledger-2026-11-05.ndjson")
	content := "not json\n\n" +
		`{"namespace":"ns","target":"t","message_id":"good","sent_at":"2026-11-05T11:59:00Z"}` + "\n" +
		`{"namespace":"ns","target":"t","message_id":"torn","sent_at":"2026-11-05T11:`
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}

	got, err := RecentFromDir(dir, "ns", "t", time.Hour, runStart)

	if err != nil || !reflect.DeepEqual(got, map[string]struct{}{"good": {}}) {
		t.Errorf("RecentFromDir() = %v, %v, want only the good line", got, err)
	}
}

// writeLedgerFile writes entries as NDJSON to dir/name.
func writeLedgerFile(t *testing.T, dir, name string, entries ...LedgerEntry) string {
	t.Helper()
	var content []byte
	for _, entry := range entries {
		line, err := json.Marshal(entry)
		if err != nil {
			t.Fatal(err)
		}
		content = append(append(content, line...), '\n')
	}
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, content, 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestLedgerAppendWritesEachEntryToTheFileOfItsUTCDay(t *testing.T) {
	ledger, dir := openTestLedger(t)
	zone := time.FixedZone("plus2", 2*60*60)
	entries := []LedgerEntry{
		{"ns", "t", "same-day", runStart},
		{"ns", "t", "next-day-late-utc", time.Date(2026, 11, 5, 23, 59, 59, 0, time.UTC)},
		{"ns", "t", "next-day", time.Date(2026, 11, 6, 0, 0, 0, 0, time.UTC)},
		{"ns", "t", "zone-shifted", time.Date(2026, 11, 6, 1, 30, 0, 0, zone)},
	}
	for _, entry := range entries {
		if err := ledger.Append(entry); err != nil {
			t.Fatalf("Append() error = %v", err)
		}
	}

	want := map[string]int{"resend-ledger-2026-11-05.ndjson": 3, "resend-ledger-2026-11-06.ndjson": 1}
	for name, lines := range want {
		data, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil || strings.Count(string(data), "\n") != lines {
			t.Errorf("%s = %q, %v, want %d lines", name, data, err, lines)
		}
	}
}

func TestLedgerAppendAfterCloseFails(t *testing.T) {
	ledger, dir := openTestLedger(t)
	_ = ledger.Close()

	err := ledger.Append(LedgerEntry{"ns", "t", "late", runStart})

	if err == nil {
		t.Error("Append() error = nil, want an error after Close")
	}
	if data, _ := os.ReadFile(filepath.Join(dir, "resend-ledger-2026-11-05.ndjson")); len(data) != 0 {
		t.Errorf("ledger file = %q, want nothing written after Close", data)
	}
}

func TestRecentFromDirReadsTheDayFilesFromTheDayOfTheWindowStartOn(t *testing.T) {
	dir := t.TempDir()
	now := time.Date(2026, 11, 5, 0, 5, 0, 0, time.UTC)
	entry := func(id string, sentAt time.Time) LedgerEntry { return LedgerEntry{"ns", "t", id, sentAt} }
	writeLedgerFile(t, dir, "resend-ledger-2026-11-03.ndjson", entry("two-days-ago", now.Add(-time.Minute)))
	writeLedgerFile(t, dir, "resend-ledger-2026-11-04.ndjson",
		entry("yesterday-inside", now.Add(-time.Minute)), entry("yesterday-outside", now.Add(-time.Hour)))
	writeLedgerFile(t, dir, "resend-ledger-2026-11-05.ndjson", entry("today", now.Add(-time.Second)))
	writeLedgerFile(t, dir, "resend-ledger-2026-11-06.ndjson", entry("tomorrow", now.Add(24*time.Hour)))
	writeLedgerFile(t, dir, "resend-ledger-notes.ndjson", entry("not-a-day-file", now))
	writeLedgerFile(t, dir, "resend-ledger.ndjson", entry("legacy", now.Add(-time.Minute)))

	got, err := RecentFromDir(dir, "ns", "t", 10*time.Minute, now)

	if err != nil {
		t.Fatalf("RecentFromDir() error = %v", err)
	}
	want := map[string]struct{}{"yesterday-inside": {}, "today": {}, "tomorrow": {}, "legacy": {}}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("RecentFromDir() = %v, want %v", got, want)
	}
}

func TestRecentFromDirFailsWhenTheDirectoryCannotBeRead(t *testing.T) {
	blocker := filepath.Join(t.TempDir(), "file")
	if err := os.WriteFile(blocker, nil, 0o600); err != nil {
		t.Fatal(err)
	}

	if _, err := RecentFromDir(blocker, "ns", "t", time.Hour, runStart); err == nil {
		t.Error("RecentFromDir() error = nil, want an error when the directory is a file")
	}
}

func TestOpenLedgerDeletesTheFilesOlderThanTheRetention(t *testing.T) {
	dir := t.TempDir()
	entry := LedgerEntry{"ns", "t", "id", runStart}
	oldest := writeLedgerFile(t, dir, "resend-ledger-2026-10-28.ndjson", entry)
	expired := writeLedgerFile(t, dir, "resend-ledger-2026-10-27.ndjson", entry)
	recent := writeLedgerFile(t, dir, "resend-ledger-2026-11-04.ndjson", entry)
	unrelated := writeLedgerFile(t, dir, "notes.txt", entry)
	staleLegacy := writeLedgerFile(t, dir, "resend-ledger.ndjson", entry)
	if err := os.Chtimes(staleLegacy, runStart, runStart.Add(-9*24*time.Hour)); err != nil {
		t.Fatal(err)
	}

	ledger, err := OpenLedger(dir, runStart)
	if err != nil {
		t.Fatalf("OpenLedger() error = %v", err)
	}
	t.Cleanup(func() { _ = ledger.Close() })

	for path, wantKept := range map[string]bool{oldest: true, expired: false, recent: true, unrelated: true, staleLegacy: false} {
		_, err := os.Stat(path)
		if kept := err == nil; kept != wantKept {
			t.Errorf("%s kept = %v, want %v", filepath.Base(path), kept, wantKept)
		}
	}
}

func TestOpenLedgerKeepsALegacyFileThatIsStillFreshAndDoesNotRewriteIt(t *testing.T) {
	dir := t.TempDir()
	legacy := writeLedgerFile(t, dir, "resend-ledger.ndjson", LedgerEntry{"ns", "t", "id", runStart})
	if err := os.Chtimes(legacy, runStart, runStart.Add(-7*24*time.Hour)); err != nil {
		t.Fatal(err)
	}
	before, _ := os.ReadFile(legacy)

	ledger, err := OpenLedger(dir, runStart)
	if err != nil {
		t.Fatalf("OpenLedger() error = %v", err)
	}
	t.Cleanup(func() { _ = ledger.Close() })
	_ = ledger.Append(LedgerEntry{"ns", "t", "new", runStart})

	if after, err := os.ReadFile(legacy); err != nil || string(after) != string(before) {
		t.Errorf("legacy file = %q, %v, want it kept as it was", after, err)
	}
}

func TestDayFromLedgerFileName(t *testing.T) {
	tests := []struct {
		name   string
		file   string
		want   time.Time
		wantOK bool
	}{
		{name: "day file", file: "resend-ledger-2026-11-05.ndjson", want: time.Date(2026, 11, 5, 0, 0, 0, 0, time.UTC), wantOK: true},
		{name: "legacy file", file: "resend-ledger.ndjson"},
		{name: "wrong prefix", file: "ledger-2026-11-05.ndjson"},
		{name: "wrong suffix", file: "resend-ledger-2026-11-05.json"},
		{name: "not a date", file: "resend-ledger-notes.ndjson"},
		{name: "impossible date", file: "resend-ledger-2026-13-45.ndjson"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := dayFromLedgerFileName(tc.file)

			if ok != tc.wantOK || !got.Equal(tc.want) {
				t.Errorf("dayFromLedgerFileName(%q) = %v, %v, want %v, %v", tc.file, got, ok, tc.want, tc.wantOK)
			}
		})
	}
}

func TestLedgerAppendsAcrossOpens(t *testing.T) {
	ledger, dir := openTestLedger(t)
	path := filepath.Join(dir, "resend-ledger-2026-11-05.ndjson")
	_ = ledger.Append(LedgerEntry{"ns", "t", "first", runStart})
	_ = ledger.Close()

	again, err := OpenLedger(dir, runStart)
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

	if _, err := OpenLedger(filepath.Join(blocker, "sub"), runStart); err == nil {
		t.Error("OpenLedger() error = nil, want an error when a parent is a file")
	}
}

func TestDefaultLedgerDir(t *testing.T) {
	tests := []struct {
		name string
		env  map[string]string
		home string
		want string
	}{
		{name: "xdg state home", env: map[string]string{"XDG_STATE_HOME": "/state"}, home: "/home/me", want: "/state/asbctl"},
		{name: "fallback", env: map[string]string{}, home: "/home/me", want: "/home/me/.local/state/asbctl"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := DefaultLedgerDir(func(key string) string { return tc.env[key] }, tc.home)

			if got != tc.want {
				t.Errorf("DefaultLedgerDir() = %q, want %q", got, tc.want)
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

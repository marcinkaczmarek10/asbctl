package dlq

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const (
	// ledgerRetention is the age after which a ledger file is deleted. The longest duplicate detection window
	// of Service Bus is 7 days.
	ledgerRetention = 8 * 24 * time.Hour
	ledgerDayFormat = "2006-01-02"
	ledgerPrefix    = "resend-ledger-"
	ledgerSuffix    = ".ndjson"
	// legacyLedgerName is the single file that earlier versions appended to.
	legacyLedgerName = "resend-ledger.ndjson"
)

// LedgerEntry records one successful resend.
type LedgerEntry struct {
	Namespace string    `json:"namespace"`
	Target    string    `json:"target"`
	MessageID string    `json:"message_id"`
	SentAt    time.Time `json:"sent_at"`
}

// DefaultLedgerDir returns $XDG_STATE_HOME/asbctl, or ~/.local/state/asbctl without it.
func DefaultLedgerDir(getenv func(string) string, homeDir string) string {
	stateDir := getenv("XDG_STATE_HOME")
	if stateDir == "" {
		stateDir = filepath.Join(homeDir, ".local", "state")
	}
	return filepath.Join(stateDir, "asbctl")
}

// Ledger is the append-only record of what resend sent, kept so that a later run can refuse a MessageId that
// duplicate detection would drop. It writes one file per UTC day of the send, named resend-ledger-YYYY-MM-DD.ndjson.
type Ledger struct {
	dir    string
	file   *os.File
	day    time.Time
	closed bool
}

// OpenLedger opens the ledger in dir for appending, creating the directory when it is missing. It first deletes
// the ledger files that are older than the retention, as a best effort.
func OpenLedger(dir string, now time.Time) (*Ledger, error) {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, fmt.Errorf("create ledger directory: %w", err)
	}
	pruneLedger(dir, now)
	day := utcDay(now)
	file, err := openLedgerFile(dir, day)
	if err != nil {
		return nil, err
	}
	return &Ledger{dir: dir, file: file, day: day}, nil
}

func openLedgerFile(dir string, day time.Time) (*os.File, error) {
	file, err := os.OpenFile(filepath.Join(dir, ledgerFileName(day)), os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open ledger: %w", err)
	}
	return file, nil
}

// Append writes one entry to the file of its UTC day. It is one write call per line, so that a crash leaves at
// most a torn last line.
func (l *Ledger) Append(entry LedgerEntry) error {
	if l.closed {
		return errors.New("append to ledger: ledger is closed")
	}
	line, err := json.Marshal(entry)
	if err != nil {
		return fmt.Errorf("encode ledger entry: %w", err)
	}
	if day := utcDay(entry.SentAt); !day.Equal(l.day) {
		next, err := openLedgerFile(l.dir, day)
		if err != nil {
			return err
		}
		_ = l.file.Close() // every line went out in its own write call, so the close has nothing left to lose
		l.file, l.day = next, day
	}
	if _, err := l.file.Write(append(line, '\n')); err != nil {
		return fmt.Errorf("append to ledger: %w", err)
	}
	return nil
}

// Close closes the ledger file. An Append after it fails.
func (l *Ledger) Close() error {
	l.closed = true
	return l.file.Close()
}

// Recent returns the MessageIds that were resent to the target inside the window before now. See RecentFromDir.
func (l *Ledger) Recent(namespace, target string, window time.Duration, now time.Time) (map[string]struct{}, error) {
	return RecentFromDir(l.dir, namespace, target, window, now)
}

// RecentFromDir returns the MessageIds that were resent to the target inside the window before now. It reads the
// day files dated from the day of now minus the window on, which includes a send dated in the future, and the
// legacy single file. A missing ledger is empty, and a line that does not parse is skipped, because a torn last
// line must not block every later run.
func RecentFromDir(dir, namespace, target string, window time.Duration, now time.Time) (map[string]struct{}, error) {
	recent := make(map[string]struct{})
	entries, err := os.ReadDir(dir)
	if errors.Is(err, fs.ErrNotExist) {
		return recent, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read ledger directory: %w", err)
	}
	paths := []string{filepath.Join(dir, legacyLedgerName)}
	oldest := utcDay(now.Add(-window))
	for _, entry := range entries {
		if day, ok := dayFromLedgerFileName(entry.Name()); ok && !day.Before(oldest) {
			paths = append(paths, filepath.Join(dir, entry.Name()))
		}
	}
	for _, path := range paths {
		if err := collectRecent(recent, path, namespace, target, window, now); err != nil {
			return nil, err
		}
	}
	return recent, nil
}

func collectRecent(recent map[string]struct{}, path, namespace, target string, window time.Duration, now time.Time) error {
	file, err := os.Open(path)
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("open ledger: %w", err)
	}
	defer file.Close()

	reader := bufio.NewReader(file)
	for {
		line, readErr := reader.ReadBytes('\n')
		var entry LedgerEntry
		if len(bytes.TrimSpace(line)) > 0 && json.Unmarshal(line, &entry) == nil &&
			entry.Namespace == namespace && entry.Target == target && withinWindow(entry.SentAt, now, window) {
			recent[entry.MessageID] = struct{}{}
		}
		if readErr == nil {
			continue
		}
		if errors.Is(readErr, io.EOF) {
			return nil
		}
		return fmt.Errorf("read ledger: %w", readErr)
	}
}

// pruneLedger deletes the day files dated before the day of now minus the retention, and the legacy file when
// it was last written before then. A failure is ignored: the ledger still works with old files in it.
func pruneLedger(dir string, now time.Time) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return
	}
	oldest := utcDay(now.Add(-ledgerRetention))
	for _, entry := range entries {
		expired := false
		if day, ok := dayFromLedgerFileName(entry.Name()); ok {
			expired = day.Before(oldest)
		} else if entry.Name() == legacyLedgerName {
			info, err := entry.Info()
			expired = err == nil && info.ModTime().Before(now.Add(-ledgerRetention))
		}
		if expired {
			_ = os.Remove(filepath.Join(dir, entry.Name()))
		}
	}
}

func utcDay(t time.Time) time.Time {
	year, month, day := t.UTC().Date()
	return time.Date(year, month, day, 0, 0, 0, 0, time.UTC)
}

func ledgerFileName(day time.Time) string {
	return ledgerPrefix + day.Format(ledgerDayFormat) + ledgerSuffix
}

// dayFromLedgerFileName returns the day in a day file name, and false for any other name.
func dayFromLedgerFileName(name string) (time.Time, bool) {
	rest, ok := strings.CutPrefix(name, ledgerPrefix)
	if !ok {
		return time.Time{}, false
	}
	date, ok := strings.CutSuffix(rest, ledgerSuffix)
	if !ok {
		return time.Time{}, false
	}
	day, err := time.Parse(ledgerDayFormat, date)
	return day, err == nil
}

// withinWindow is true for a send that is not older than the window. A send dated in the future counts as recent.
func withinWindow(sentAt, now time.Time, window time.Duration) bool {
	return now.Sub(sentAt) < window
}

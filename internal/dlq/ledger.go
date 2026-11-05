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
	"time"
)

// LedgerEntry records one successful resend.
type LedgerEntry struct {
	Namespace string    `json:"namespace"`
	Target    string    `json:"target"`
	MessageID string    `json:"message_id"`
	SentAt    time.Time `json:"sent_at"`
}

// DefaultLedgerPath returns $XDG_STATE_HOME/asbctl/resend-ledger.ndjson, or ~/.local/state/asbctl/ without it.
func DefaultLedgerPath(getenv func(string) string, homeDir string) string {
	stateDir := getenv("XDG_STATE_HOME")
	if stateDir == "" {
		stateDir = filepath.Join(homeDir, ".local", "state")
	}
	return filepath.Join(stateDir, "asbctl", "resend-ledger.ndjson")
}

// Ledger is the append-only record of what resend sent, kept so that a later run can refuse a MessageId that
// duplicate detection would drop.
type Ledger struct {
	path string
	file *os.File
}

// OpenLedger opens the ledger for appending, creating it and its directory when they are missing.
func OpenLedger(path string) (*Ledger, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, fmt.Errorf("create ledger directory: %w", err)
	}
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open ledger: %w", err)
	}
	return &Ledger{path: path, file: file}, nil
}

// Append writes one entry. It is one write call per line, so that a crash leaves at most a torn last line.
func (l *Ledger) Append(entry LedgerEntry) error {
	line, err := json.Marshal(entry)
	if err != nil {
		return fmt.Errorf("encode ledger entry: %w", err)
	}
	if _, err := l.file.Write(append(line, '\n')); err != nil {
		return fmt.Errorf("append to ledger: %w", err)
	}
	return nil
}

// Close closes the ledger file.
func (l *Ledger) Close() error {
	return l.file.Close()
}

// Recent returns the MessageIds that were resent to the target inside the window before now. A missing ledger
// is empty, and a line that does not parse is skipped, because a torn last line must not block every later run.
func (l *Ledger) Recent(namespace, target string, window time.Duration, now time.Time) (map[string]struct{}, error) {
	return RecentFromFile(l.path, namespace, target, window, now)
}

// RecentFromFile is Recent for a ledger that is not open for appending.
func RecentFromFile(path, namespace, target string, window time.Duration, now time.Time) (map[string]struct{}, error) {
	recent := make(map[string]struct{})
	file, err := os.Open(path)
	if errors.Is(err, fs.ErrNotExist) {
		return recent, nil
	}
	if err != nil {
		return nil, fmt.Errorf("open ledger: %w", err)
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
			return recent, nil
		}
		return nil, fmt.Errorf("read ledger: %w", readErr)
	}
}

// withinWindow is true for a send that is not older than the window. A send dated in the future counts as recent.
func withinWindow(sentAt, now time.Time, window time.Duration) bool {
	return now.Sub(sentAt) < window
}

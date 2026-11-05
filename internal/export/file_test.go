package export

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
)

func readFile(t *testing.T, path string) string {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return string(data)
}

func writeText(text string) func(io.Writer) error {
	return func(w io.Writer) error {
		_, err := io.WriteString(w, text)
		return err
	}
}

func TestWriteFileWritesThroughATemporaryFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "out.ndjson")
	var duringWrite []string

	err := WriteFile(path, false, func(w io.Writer) error {
		_, _ = io.WriteString(w, "one\n")
		entries, _ := os.ReadDir(filepath.Dir(path))
		for _, entry := range entries {
			duringWrite = append(duringWrite, entry.Name())
		}
		return nil
	})

	if err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}
	if len(duringWrite) != 1 || duringWrite[0] != "out.ndjson.tmp" {
		t.Errorf("files during the write = %v, want only out.ndjson.tmp", duringWrite)
	}
	if got := readFile(t, path); got != "one\n" {
		t.Errorf("file = %q, want %q", got, "one\n")
	}
	if _, err := os.Stat(path + ".tmp"); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("temporary file still exists after success: %v", err)
	}
}

func TestWriteFileRefusesAnExistingFileWithoutForce(t *testing.T) {
	path := filepath.Join(t.TempDir(), "out.ndjson")
	if err := os.WriteFile(path, []byte("keep"), 0o600); err != nil {
		t.Fatal(err)
	}
	called := false

	err := WriteFile(path, false, func(io.Writer) error { called = true; return nil })

	if !errors.Is(err, ErrFileExists) {
		t.Errorf("WriteFile() error = %v, want ErrFileExists", err)
	}
	if called {
		t.Error("write was called for an existing file, want the refusal before any work")
	}
	if got := readFile(t, path); got != "keep" {
		t.Errorf("existing file = %q, want it untouched", got)
	}
}

func TestWriteFileDoesNotOverwriteAFileThatAppearsDuringTheWrite(t *testing.T) {
	path := filepath.Join(t.TempDir(), "out.ndjson")

	err := WriteFile(path, false, func(w io.Writer) error {
		_, _ = io.WriteString(w, "mine")
		return os.WriteFile(path, []byte("theirs"), 0o600)
	})

	if !errors.Is(err, ErrFileExists) {
		t.Errorf("WriteFile() error = %v, want ErrFileExists", err)
	}
	if got := readFile(t, path); got != "theirs" {
		t.Errorf("file = %q, want the file that appeared during the write to survive", got)
	}
	if _, statErr := os.Stat(path + ".tmp"); !errors.Is(statErr, os.ErrNotExist) {
		t.Errorf("temporary file left behind: %v", statErr)
	}
}

func TestWriteFileOverwritesWithForce(t *testing.T) {
	path := filepath.Join(t.TempDir(), "out.ndjson")
	if err := os.WriteFile(path, []byte("old"), 0o600); err != nil {
		t.Fatal(err)
	}

	if err := WriteFile(path, true, writeText("new")); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	if got := readFile(t, path); got != "new" {
		t.Errorf("file = %q, want %q", got, "new")
	}
}

func TestWriteFileLeavesNothingWhenTheWriteFails(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "out.ndjson")
	boom := errors.New("interrupted")

	err := WriteFile(path, false, func(w io.Writer) error {
		_, _ = io.WriteString(w, "half")
		return boom
	})

	if !errors.Is(err, boom) {
		t.Errorf("WriteFile() error = %v, want %v", err, boom)
	}
	entries, _ := os.ReadDir(dir)
	if len(entries) != 0 {
		t.Errorf("directory has %d entries after a failed write, want none", len(entries))
	}
}

func TestWriteFileKeepsAnExistingFileWhenAForcedWriteFails(t *testing.T) {
	path := filepath.Join(t.TempDir(), "out.ndjson")
	if err := os.WriteFile(path, []byte("old"), 0o600); err != nil {
		t.Fatal(err)
	}

	_ = WriteFile(path, true, func(io.Writer) error { return errors.New("interrupted") })

	if got := readFile(t, path); got != "old" {
		t.Errorf("file = %q, want the old content after a failed forced write", got)
	}
}

func TestWriteFileFailsWhenTheDirectoryDoesNotExist(t *testing.T) {
	err := WriteFile(filepath.Join(t.TempDir(), "missing", "out.ndjson"), false, writeText("x"))

	if err == nil {
		t.Error("WriteFile() error = nil, want an error for a missing directory")
	}
}

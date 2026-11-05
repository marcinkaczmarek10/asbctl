package export

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
)

// ErrFileExists is returned when the destination exists and overwriting was not asked for.
var ErrFileExists = errors.New("file already exists")

// WriteFile writes the output of write to path through path+".tmp" and renames it into place when write
// succeeds, so that a failed or interrupted run never leaves a half file under the final name. Without force an
// existing file is an error, which the check at the start and the rename at the end both enforce.
func WriteFile(path string, force bool, write func(io.Writer) error) (err error) {
	if !force {
		if _, statErr := os.Stat(path); statErr == nil {
			return fmt.Errorf("%w: %s", ErrFileExists, path)
		}
	}

	tempPath := path + ".tmp"
	file, err := os.OpenFile(tempPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return fmt.Errorf("create %s: %w", tempPath, err)
	}
	defer func() {
		if err != nil {
			err = errors.Join(err, file.Close(), os.Remove(tempPath))
		}
	}()

	buffered := bufio.NewWriter(file)
	if err := write(buffered); err != nil {
		return err
	}
	if err := buffered.Flush(); err != nil {
		return fmt.Errorf("write %s: %w", tempPath, err)
	}
	if err := file.Sync(); err != nil {
		return fmt.Errorf("sync %s: %w", tempPath, err)
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("close %s: %w", tempPath, err)
	}

	if err := moveIntoPlace(tempPath, path, force); err != nil {
		// file is closed already, so only the temporary file is left to remove.
		return errors.Join(err, os.Remove(tempPath))
	}
	return nil
}

// moveIntoPlace renames over the destination with force, and otherwise links, which fails if the destination exists.
func moveIntoPlace(tempPath, path string, force bool) error {
	if force {
		if err := os.Rename(tempPath, path); err != nil {
			return fmt.Errorf("rename %s: %w", tempPath, err)
		}
		return nil
	}

	if err := os.Link(tempPath, path); err != nil {
		if errors.Is(err, fs.ErrExist) {
			return fmt.Errorf("%w: %s", ErrFileExists, path)
		}
		return fmt.Errorf("link %s: %w", path, err)
	}
	if err := os.Remove(tempPath); err != nil {
		return fmt.Errorf("remove %s: %w", tempPath, err)
	}
	return nil
}

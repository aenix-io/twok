// Package atomicfile replaces small files so that a crash leaves either the
// old or the new content on disk, never a mix or an empty file.
package atomicfile

import (
	"fmt"
	"os"
	"path/filepath"
)

// Write replaces path with data. It writes a temporary file in the same
// directory, syncs it, renames it over path and syncs the directory, so
// that the rename itself is durable when Write returns.
func Write(path string, data []byte, perm os.FileMode) (err error) {
	dir, base := filepath.Split(path)
	if dir == "" {
		dir = "."
	}
	tmp, err := os.CreateTemp(dir, "."+base+".tmp-*")
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			_ = tmp.Close()
			_ = os.Remove(tmp.Name())
		}
	}()
	if err := tmp.Chmod(perm); err != nil {
		return err
	}
	if _, err := tmp.Write(data); err != nil {
		return err
	}
	if err := tmp.Sync(); err != nil {
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp.Name(), path); err != nil {
		return err
	}
	return SyncDir(dir)
}

// SyncDir makes the entries of dir durable: a rename or a create is on disk
// only once its directory is synced.
func SyncDir(dir string) error {
	d, err := os.Open(dir) // #nosec G304 -- the directory of a file the caller chose
	if err != nil {
		return err
	}
	if err := d.Sync(); err != nil {
		_ = d.Close()
		return fmt.Errorf("sync %s: %w", dir, err)
	}
	return d.Close()
}

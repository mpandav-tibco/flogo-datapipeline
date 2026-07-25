// Package fileutil provides small filesystem helpers shared across the engine.
package fileutil

import (
	"os"
	"path/filepath"
)

// WriteFileAtomic writes data to path atomically AND durably. It writes to a
// temp file in the same directory, fsyncs the temp file so its contents reach
// stable storage, renames it over path (an atomic operation on POSIX), and then
// best-effort fsyncs the parent directory so the rename survives a power loss.
//
// os.WriteFile followed by os.Rename gives atomicity with respect to concurrent
// readers, but NOT durability: without the fsync the data can sit in the OS page
// cache and be lost on a power failure or kernel crash — which defeats the point
// of a checkpoint/watermark store whose job is to survive a crash.
func WriteFileAtomic(path string, data []byte, perm os.FileMode) (err error) {
	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, "."+filepath.Base(path)+".tmp-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	// On any failure before a successful rename, remove the temp file. After a
	// successful rename tmpName no longer exists, so this is a harmless no-op.
	renamed := false
	defer func() {
		if !renamed {
			_ = tmp.Close()
			_ = os.Remove(tmpName)
		}
	}()

	if err := tmp.Chmod(perm); err != nil {
		return err
	}
	if _, err := tmp.Write(data); err != nil {
		return err
	}
	if err := tmp.Sync(); err != nil { // flush data to stable storage
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpName, path); err != nil {
		return err
	}
	renamed = true

	// Persist the rename itself. Best-effort: some filesystems do not support
	// directory fsync, and if it fails the data is still durable and the rename
	// is visible to the running system — at worst a power loss resumes from the
	// previous durable state, which is safe (at-least-once replay).
	if d, derr := os.Open(dir); derr == nil {
		_ = d.Sync()
		_ = d.Close()
	}
	return nil
}

//go:build linux || darwin

package objstore

import "syscall"

// diskFree reports the bytes available to unprivileged users and the size of
// the filesystem holding dir.
func diskFree(dir string) (free, total uint64, ok bool) {
	var st syscall.Statfs_t
	if err := syscall.Statfs(dir, &st); err != nil {
		return 0, 0, false
	}
	bs := uint64(st.Bsize) //nolint:gosec,unconvert // block size is positive; its type differs per OS
	return st.Bavail * bs, st.Blocks * bs, true
}

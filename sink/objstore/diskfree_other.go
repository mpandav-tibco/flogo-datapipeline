//go:build !linux && !darwin

package objstore

func diskFree(string) (free, total uint64, ok bool) { return 0, 0, false }

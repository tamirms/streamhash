//go:build !linux && !darwin

package streamhash

import "golang.org/x/sys/unix"

// preallocFile extends the file to the given size.
// Fallback for platforms without fallocate: uses ftruncate to set file size
// so pwrite has allocated blocks to write into.
func preallocFile(fd int, size int64) error {
	return unix.Ftruncate(fd, size)
}

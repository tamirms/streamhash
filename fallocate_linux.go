//go:build linux

package streamhash

import (
	"os"

	"golang.org/x/sys/unix"
)

// fallocateFile pre-allocates disk blocks to prevent SIGBUS on disk full.
// On Linux, uses the fallocate syscall for efficient space reservation.
func fallocateFile(file *os.File, size int64) error {
	err := unix.Fallocate(int(file.Fd()), 0, 0, size)
	if err != nil {
		// Fallback to ftruncate if fallocate fails (e.g., NFS, some filesystems)
		return unix.Ftruncate(int(file.Fd()), size)
	}
	// Fallocate allocates blocks but doesn't set file size - must also truncate
	return unix.Ftruncate(int(file.Fd()), size)
}

//go:build darwin

package streamhash

import (
	"os"

	"golang.org/x/sys/unix"
)

// fallocateFile pre-allocates disk blocks to prevent SIGBUS on disk full.
// On macOS, uses fcntl F_PREALLOCATE for space reservation.
func fallocateFile(file *os.File, size int64) error {
	// F_PREALLOCATE with F_ALLOCATEALL - allocate all requested space or fail
	fst := unix.Fstore_t{
		Flags:   unix.F_ALLOCATEALL,
		Posmode: unix.F_PEOFPOSMODE,
		Offset:  0,
		Length:  size,
	}

	err := unix.FcntlFstore(file.Fd(), unix.F_PREALLOCATE, &fst)
	if err != nil {
		// Fallback to ftruncate if F_PREALLOCATE fails
		return unix.Ftruncate(int(file.Fd()), size)
	}

	// Set the file size (F_PREALLOCATE only reserves space, doesn't set size)
	return unix.Ftruncate(int(file.Fd()), size)
}

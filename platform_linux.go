//go:build linux

package streamhash

import "golang.org/x/sys/unix"

// preallocFile pre-allocates disk blocks and extends the file to the given size.
func preallocFile(fd int, size int64) error {
	return unix.Fallocate(fd, 0, 0, size)
}

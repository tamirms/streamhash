//go:build linux

package streamhash

import "golang.org/x/sys/unix"

// fadviseSequential hints to the kernel that the file will be read
// sequentially. Applied before read-back of partition files.
// Best-effort: errors are silently ignored.
func fadviseSequential(fd int, offset, length int64) {
	_ = unix.Fadvise(fd, offset, length, unix.FADV_SEQUENTIAL)
}

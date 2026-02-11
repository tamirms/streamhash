//go:build linux

package streamhash

import "golang.org/x/sys/unix"

// MADV_POPULATE_WRITE was added in Linux 5.14.
// On older kernels, madvise returns EINVAL which we ignore.
const madvPopulateWrite = 23

// prefaultRegion asks the kernel to prefault pages for writing.
// On Linux 5.14+, this uses MADV_POPULATE_WRITE for efficient prefaulting.
// On older kernels, madvise returns EINVAL which is silently ignored.
func prefaultRegion(data []byte) {
	if len(data) == 0 {
		return
	}
	// Best-effort: ignore all errors (EINVAL on old kernels, or other failures)
	_ = unix.Madvise(data, madvPopulateWrite)
}

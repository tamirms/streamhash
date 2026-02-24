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

// hintHugePages requests transparent huge pages for the given mmap region.
// Must be called BEFORE prefaultRegion: sets VM_HUGEPAGE so subsequent page
// faults allocate 2MB compound pages directly. If called after prefaulting,
// pages are already 4KB and khugepaged may never promote them.
// Best-effort: no-op on systems without THP or on filesystems that don't
// support file-backed huge pages (effective on tmpfs).
func hintHugePages(data []byte) {
	if len(data) == 0 {
		return
	}
	_ = unix.Madvise(data, unix.MADV_HUGEPAGE)
}

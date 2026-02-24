//go:build !linux

package streamhash

// prefaultRegion is a no-op on non-Linux platforms.
// MADV_POPULATE_WRITE is Linux 5.14+ specific.
func prefaultRegion(data []byte) {
	// No-op: no efficient prefaulting available on this platform
}

// hintHugePages is a no-op on non-Linux platforms.
// MADV_HUGEPAGE is Linux-specific.
func hintHugePages(data []byte) {
	// No-op
}

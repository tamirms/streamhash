//go:build !linux

package streamhash

// fadviseSequential is a no-op on non-Linux platforms.
// FADV_SEQUENTIAL is Linux-specific.
func fadviseSequential(fd int, offset, length int64) {
	// No-op
}

//go:build !linux && !darwin

package streamhash

import "os"

// fallocateFile pre-allocates disk blocks to prevent SIGBUS on disk full.
// On platforms without native fallocate, uses Truncate as a fallback.
// Note: This sets file size but may not reserve actual disk blocks on all filesystems.
func fallocateFile(file *os.File, size int64) error {
	return file.Truncate(size)
}

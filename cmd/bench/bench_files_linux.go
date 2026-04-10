//go:build linux

package main

import (
	"fmt"
	"os"

	"golang.org/x/sys/unix"
)

func evictFromCache(files []string) {
	for _, path := range files {
		f, err := os.Open(path)
		if err != nil {
			continue
		}
		unix.Fadvise(int(f.Fd()), 0, 0, unix.FADV_DONTNEED)
		f.Close()
	}
	fmt.Println("Evicted files from page cache")
}

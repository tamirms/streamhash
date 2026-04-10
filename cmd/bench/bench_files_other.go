//go:build !linux

package main

import "fmt"

func evictFromCache(files []string) {
	fmt.Println("Cache eviction not supported on this platform")
}

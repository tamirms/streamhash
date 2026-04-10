package main

import (
	"bufio"
	"bytes"
	"crypto/rand"
	"encoding/binary"
	"flag"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"sync"
	"sync/atomic"
	"time"
)

// entrySize matches benchEntrySize in bench_files.go.
// Each entry is 16 bytes key + 4 bytes payload = 20 bytes.
const entrySize = benchEntrySize

func mainGen() {
	outDir := flag.String("out", "", "output directory (required)")
	numFiles := flag.Int("files", 1000, "number of files to generate")
	minEntries := flag.Int("min", 3_500_000, "minimum entries per file")
	maxEntries := flag.Int("max", 5_000_000, "maximum entries per file")
	workers := flag.Int("workers", runtime.NumCPU(), "number of parallel workers")
	sorted := flag.Bool("sorted", true, "sort entries within each file (false = random order)")
	flag.Parse()

	if *outDir == "" {
		fmt.Fprintf(os.Stderr, "Usage: bench gen -out <directory> [-files N] [-min N] [-max N] [-workers N] [-sorted=false]\n")
		os.Exit(1)
	}

	if err := os.MkdirAll(*outDir, 0755); err != nil {
		fmt.Fprintf(os.Stderr, "mkdir %s: %v\n", *outDir, err)
		os.Exit(1)
	}

	order := "sorted"
	if !*sorted {
		order = "unsorted"
	}
	fmt.Printf("Generating %d %s files with %d-%d entries of %d bytes each\n",
		*numFiles, order, *minEntries, *maxEntries, entrySize)
	fmt.Printf("Output: %s, Workers: %d\n", *outDir, *workers)

	start := time.Now()
	var completed atomic.Int64
	var totalEntries atomic.Int64
	var totalBytes atomic.Int64

	rangeSize := int64(*maxEntries - *minEntries + 1)

	var wg sync.WaitGroup
	work := make(chan int, *numFiles)
	for i := range *numFiles {
		work <- i
	}
	close(work)

	for range *workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for fileIdx := range work {
				n, err := generateFile(*outDir, fileIdx, *minEntries, rangeSize, *sorted)
				if err != nil {
					fmt.Fprintf(os.Stderr, "Error generating file %d: %v\n", fileIdx, err)
					os.Exit(1)
				}
				totalEntries.Add(int64(n))
				totalBytes.Add(int64(n) * entrySize)
				done := completed.Add(1)
				if done%50 == 0 || done == int64(*numFiles) {
					elapsed := time.Since(start)
					fmt.Printf("  %d/%d files done (%.1fs elapsed)\n", done, *numFiles, elapsed.Seconds())
				}
			}
		}()
	}

	wg.Wait()
	elapsed := time.Since(start)
	fmt.Printf("\nDone in %.1fs\n", elapsed.Seconds())
	fmt.Printf("Total entries: %d\n", totalEntries.Load())
	fmt.Printf("Total size: %.2f GB\n", float64(totalBytes.Load())/(1<<30))
}

func generateFile(outDir string, idx, minEntries int, rangeSize int64, sorted bool) (int, error) {
	bigN, err := rand.Int(rand.Reader, big.NewInt(rangeSize))
	if err != nil {
		return 0, fmt.Errorf("random count: %w", err)
	}
	n := minEntries + int(bigN.Int64())

	data := make([]byte, n*entrySize)
	if _, err := rand.Read(data); err != nil {
		return 0, fmt.Errorf("crypto/rand: %w", err)
	}

	var entries [][]byte
	if sorted {
		entries = make([][]byte, n)
		for i := range n {
			entries[i] = data[i*entrySize : (i+1)*entrySize]
		}
		slices.SortFunc(entries, bytes.Compare)
	}

	path := filepath.Join(outDir, fmt.Sprintf("%04d.bin", idx))
	f, err := os.Create(path)
	if err != nil {
		return 0, fmt.Errorf("create %s: %w", path, err)
	}
	defer f.Close()

	w := bufio.NewWriterSize(f, 4*1024*1024)
	var header [8]byte
	binary.LittleEndian.PutUint64(header[:], uint64(n))
	if _, err := w.Write(header[:]); err != nil {
		return 0, err
	}
	if sorted {
		for _, e := range entries {
			if _, err := w.Write(e); err != nil {
				return 0, err
			}
		}
	} else {
		if _, err := w.Write(data); err != nil {
			return 0, err
		}
	}
	if err := w.Flush(); err != nil {
		return 0, err
	}

	return n, nil
}

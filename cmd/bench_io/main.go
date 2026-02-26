// bench_io compares I/O patterns for unsorted key routing:
//
//  1. "mmap": Random writes to per-block regions in an mmap'd file (current approach)
//  2. "partition": Buffered writes with partition flush to P sequential files (proposed)
//
// Each mode writes N entries of 16 bytes to simulated block positions,
// then reads them back. Entry blockIDs are uniform random.
//
// Usage:
//
//	go run ./cmd/bench_io -size 2 -blocks 16000
//	go run ./cmd/bench_io -size 10 -blocks 160000 -mode partition
//	go run ./cmd/bench_io -size 50 -blocks 800000
//
// To simulate memory pressure (data exceeding page cache):
//
//	sudo systemd-run --scope -p MemoryMax=4G --uid=$(id -u) \
//	  go run ./cmd/bench_io -size 10 -blocks 160000
package main

import (
	"bufio"
	"encoding/binary"
	"flag"
	"fmt"
	"math"
	"math/rand/v2"
	"os"
	"path/filepath"
	"runtime"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
)

const entrySize = 16 // k0 (8 bytes) + k1 (8 bytes)

func main() {
	sizeGB := flag.Float64("size", 2.0, "total data size in GB")
	numBlocks := flag.Int("blocks", 16000, "number of blocks")
	numPartitions := flag.Int("partitions", 0, "partitions for partition mode (0 = auto)")
	bufferMB := flag.Int("buffer", 256, "flush buffer size in MB for partition mode")
	mode := flag.String("mode", "both", "mode: mmap, partition, sweep, or both")
	tmpDir := flag.String("dir", "", "temp directory (default: os.TempDir())")
	flag.Parse()

	totalBytes := int64(*sizeGB * 1024 * 1024 * 1024)
	numEntries := totalBytes / entrySize

	if *tmpDir == "" {
		*tmpDir = os.TempDir()
	}

	// Auto-select partition count: target ~4GB per partition
	if *numPartitions == 0 {
		*numPartitions = max(4, int(math.Ceil(*sizeGB/4.0)))
	}

	fmt.Printf("Configuration:\n")
	fmt.Printf("  Data size:    %.1f GB (%d entries × %d bytes)\n", *sizeGB, numEntries, entrySize)
	fmt.Printf("  Blocks:       %d\n", *numBlocks)
	fmt.Printf("  Buffer:       %d MB\n", *bufferMB)
	fmt.Printf("  Temp dir:     %s\n", *tmpDir)
	fmt.Printf("  GOMAXPROCS:   %d\n", runtime.GOMAXPROCS(0))
	fmt.Println()

	if *mode == "sweep" {
		// Write-only sweep across partition counts
		fmt.Println("=== partition count sweep (write-only) ===")
		for _, p := range []int{4, 32, 128, 512, 800, 2000} {
			benchPartitionWriteOnly(*tmpDir, numEntries, *numBlocks, p, *bufferMB)
		}
		return
	}

	fmt.Printf("  Partitions:   %d (%.1f GB each)\n", *numPartitions, *sizeGB/float64(*numPartitions))
	fmt.Println()

	if *mode == "mmap" || *mode == "both" {
		fmt.Println("=== mmap random write (current approach) ===")
		benchMmap(*tmpDir, numEntries, *numBlocks)
		fmt.Println()
	}

	if *mode == "partition" || *mode == "both" {
		fmt.Printf("=== partitioned sequential write (proposed approach) ===\n")
		benchPartition(*tmpDir, numEntries, *numBlocks, *numPartitions, *bufferMB)
		fmt.Println()
	}
}

// benchMmap simulates the current approach: random writes to per-block mmap regions.
func benchMmap(dir string, numEntries int64, numBlocks int) {
	// Compute region layout (matching streamhash's unsortedBuffer)
	avgPerBlock := float64(numEntries) / float64(numBlocks)
	marginFactor := 1.0 + 7.0/math.Sqrt(avgPerBlock) // 7-sigma margin
	regionCapacity := int(math.Ceil(avgPerBlock * marginFactor))
	regionSize := int64(regionCapacity) * entrySize
	totalSize := int64(numBlocks) * regionSize

	fmt.Printf("  Region capacity: %d entries/block, region: %d KB, total mmap: %.1f GB\n",
		regionCapacity, regionSize/1024, float64(totalSize)/(1024*1024*1024))

	// Create temp file
	tmpFile, err := os.CreateTemp(dir, "bench-mmap-*.tmp")
	if err != nil {
		fmt.Printf("  ERROR: create temp file: %v\n", err)
		return
	}
	tmpPath := tmpFile.Name()
	defer func() { _ = os.Remove(tmpPath) }()

	// Fallocate
	if err := syscall.Fallocate(int(tmpFile.Fd()), 0, 0, totalSize); err != nil {
		fmt.Printf("  ERROR: fallocate: %v\n", err)
		_ = tmpFile.Close()
		return
	}

	// mmap
	data, err := unix.Mmap(int(tmpFile.Fd()), 0, int(totalSize),
		unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED)
	if err != nil {
		fmt.Printf("  ERROR: mmap: %v\n", err)
		_ = tmpFile.Close()
		return
	}
	_ = unix.Madvise(data, unix.MADV_RANDOM)

	counter := make([]uint32, numBlocks)
	rng := rand.New(rand.NewPCG(42, 0))

	// Write phase: random writes to per-block regions
	writeStart := time.Now()
	for range numEntries {
		blockID := rng.Uint32N(uint32(numBlocks))
		pos := int64(blockID)*regionSize + int64(counter[blockID])*entrySize
		binary.LittleEndian.PutUint64(data[pos:], rng.Uint64())
		binary.LittleEndian.PutUint64(data[pos+8:], rng.Uint64())
		counter[blockID]++
	}
	writeDur := time.Since(writeStart)
	writeRate := float64(numEntries) / writeDur.Seconds() / 1e6

	// Sync to measure true write cost
	syncStart := time.Now()
	_ = unix.Msync(data, unix.MS_SYNC)
	syncDur := time.Since(syncStart)

	fmt.Printf("  Write:  %6.2fs (%6.2f M entries/sec)\n", writeDur.Seconds(), writeRate)
	fmt.Printf("  Sync:   %6.2fs\n", syncDur.Seconds())

	// Read phase: sequential read per block
	readStart := time.Now()
	var checksum uint64
	for b := range numBlocks {
		count := counter[b]
		for j := range count {
			pos := int64(b)*regionSize + int64(j)*entrySize
			checksum += binary.LittleEndian.Uint64(data[pos:])
		}
	}
	readDur := time.Since(readStart)
	readRate := float64(numEntries) / readDur.Seconds() / 1e6

	fmt.Printf("  Read:   %6.2fs (%6.2f M entries/sec) [checksum=%x]\n", readDur.Seconds(), readRate, checksum)
	fmt.Printf("  Total:  %6.2fs\n", (writeDur + syncDur + readDur).Seconds())

	// Cleanup
	_ = unix.Munmap(data)
	_ = tmpFile.Close()
}

type bufferedEntry struct {
	k0      uint64
	k1      uint64
	blockID uint32
}

// benchPartition simulates the proposed approach: buffered + partitioned sequential writes.
func benchPartition(dir string, numEntries int64, numBlocks, numPartitions, bufferMB int) {
	blocksPerPartition := (numBlocks + numPartitions - 1) / numPartitions
	bufferCap := (bufferMB * 1024 * 1024) / 24 // 24 bytes per bufferedEntry (approx)

	fmt.Printf("  Partitions: %d (%d blocks each), buffer: %d entries\n",
		numPartitions, blocksPerPartition, bufferCap)

	// Create partition files with buffered writers
	partDir, err := os.MkdirTemp(dir, "bench-part-*")
	if err != nil {
		fmt.Printf("  ERROR: create partition dir: %v\n", err)
		return
	}
	defer func() { _ = os.RemoveAll(partDir) }()

	type partFile struct {
		f *os.File
		w *bufio.Writer
	}
	parts := make([]partFile, numPartitions)
	for i := range parts {
		f, err := os.Create(filepath.Join(partDir, fmt.Sprintf("part-%04d.bin", i)))
		if err != nil {
			fmt.Printf("  ERROR: create partition file %d: %v\n", i, err)
			return
		}
		defer func() { _ = f.Close() }()
		parts[i] = partFile{f: f, w: bufio.NewWriterSize(f, 256*1024)}
	}

	buffer := make([]bufferedEntry, 0, bufferCap)
	// Scratch buffer for partitioned flush (reused across flushes)
	scratch := make([]bufferedEntry, bufferCap)
	// Per-partition counts for counting sort
	partCounts := make([]int, numPartitions)
	partOffsets := make([]int, numPartitions)
	// Encode buffer for writing (16 bytes per entry)
	writeBuf := make([]byte, 16)

	rng := rand.New(rand.NewPCG(42, 0))

	flush := func() {
		if len(buffer) == 0 {
			return
		}
		// Count entries per partition
		clear(partCounts)
		for i := range buffer {
			p := int(buffer[i].blockID) / blocksPerPartition
			if p >= numPartitions {
				p = numPartitions - 1
			}
			partCounts[p]++
		}
		// Prefix sum for offsets
		partOffsets[0] = 0
		for i := 1; i < numPartitions; i++ {
			partOffsets[i] = partOffsets[i-1] + partCounts[i-1]
		}
		// Distribute entries to scratch in partition order
		offsets := make([]int, numPartitions)
		copy(offsets, partOffsets)
		for i := range buffer {
			p := int(buffer[i].blockID) / blocksPerPartition
			if p >= numPartitions {
				p = numPartitions - 1
			}
			scratch[offsets[p]] = buffer[i]
			offsets[p]++
		}
		// Write each partition's chunk sequentially
		for p := range numPartitions {
			start := partOffsets[p]
			end := start + partCounts[p]
			if start == end {
				continue
			}
			for j := start; j < end; j++ {
				binary.LittleEndian.PutUint64(writeBuf[0:], scratch[j].k0)
				binary.LittleEndian.PutUint64(writeBuf[8:], scratch[j].k1)
				_, _ = parts[p].w.Write(writeBuf)
			}
		}
		buffer = buffer[:0]
	}

	// Write phase
	writeStart := time.Now()
	for range numEntries {
		blockID := rng.Uint32N(uint32(numBlocks))
		buffer = append(buffer, bufferedEntry{
			k0:      rng.Uint64(),
			k1:      rng.Uint64(),
			blockID: blockID,
		})
		if len(buffer) >= bufferCap {
			flush()
		}
	}
	flush() // flush remaining
	writeDur := time.Since(writeStart)
	writeRate := float64(numEntries) / writeDur.Seconds() / 1e6

	// Sync all partition files
	syncStart := time.Now()
	for i := range parts {
		_ = parts[i].w.Flush()
		_ = parts[i].f.Sync()
	}
	syncDur := time.Since(syncStart)

	fmt.Printf("  Write:  %6.2fs (%6.2f M entries/sec)\n", writeDur.Seconds(), writeRate)
	fmt.Printf("  Sync:   %6.2fs\n", syncDur.Seconds())

	// Read phase: read each partition sequentially
	readStart := time.Now()
	var checksum uint64
	readBuf := make([]byte, 256*1024)
	for i := range parts {
		// Seek to beginning
		_, _ = parts[i].f.Seek(0, 0)
		// Advise sequential read
		info, _ := parts[i].f.Stat()
		_ = unix.Fadvise(int(parts[i].f.Fd()), 0, info.Size(), unix.FADV_SEQUENTIAL)

		for {
			n, err := parts[i].f.Read(readBuf)
			if n > 0 {
				for off := 0; off+16 <= n; off += 16 {
					checksum += binary.LittleEndian.Uint64(readBuf[off:])
				}
			}
			if err != nil {
				break
			}
		}
		// Advise dontneed after reading
		info, _ = parts[i].f.Stat()
		_ = unix.Fadvise(int(parts[i].f.Fd()), 0, info.Size(), unix.FADV_DONTNEED)
	}
	readDur := time.Since(readStart)
	readRate := float64(numEntries) / readDur.Seconds() / 1e6

	fmt.Printf("  Read:   %6.2fs (%6.2f M entries/sec) [checksum=%x]\n", readDur.Seconds(), readRate, checksum)
	fmt.Printf("  Total:  %6.2fs\n", (writeDur + syncDur + readDur).Seconds())
}

// benchPartitionWriteOnly measures only the write phase for a given partition count.
// Uses direct write() from a contiguous encode buffer instead of per-partition bufio.Writers.
func benchPartitionWriteOnly(dir string, numEntries int64, numBlocks, numPartitions, bufferMB int) {
	blocksPerPartition := (numBlocks + numPartitions - 1) / numPartitions
	bufferCap := (bufferMB * 1024 * 1024) / 24

	// Create partition files (no bufio — we write directly)
	partDir, err := os.MkdirTemp(dir, "bench-part-*")
	if err != nil {
		fmt.Printf("  P=%4d  ERROR: create partition dir: %v\n", numPartitions, err)
		return
	}
	defer func() { _ = os.RemoveAll(partDir) }()

	files := make([]*os.File, numPartitions)
	for i := range files {
		f, err := os.Create(filepath.Join(partDir, fmt.Sprintf("part-%04d.bin", i)))
		if err != nil {
			fmt.Printf("  P=%4d  ERROR: create partition file %d: %v\n", numPartitions, i, err)
			return
		}
		defer func() { _ = f.Close() }()
		files[i] = f
	}

	buffer := make([]bufferedEntry, 0, bufferCap)
	scratch := make([]bufferedEntry, bufferCap)
	partCounts := make([]int, numPartitions)
	partOffsets := make([]int, numPartitions)
	// Single contiguous encode buffer, reused across partitions within each flush.
	// Sized for largest possible partition chunk: all entries in one partition.
	encodeBuf := make([]byte, bufferCap*entrySize)

	rng := rand.New(rand.NewPCG(42, 0))

	flush := func() {
		if len(buffer) == 0 {
			return
		}
		clear(partCounts)
		for i := range buffer {
			p := int(buffer[i].blockID) / blocksPerPartition
			if p >= numPartitions {
				p = numPartitions - 1
			}
			partCounts[p]++
		}
		partOffsets[0] = 0
		for i := 1; i < numPartitions; i++ {
			partOffsets[i] = partOffsets[i-1] + partCounts[i-1]
		}
		offsets := make([]int, numPartitions)
		copy(offsets, partOffsets)
		for i := range buffer {
			p := int(buffer[i].blockID) / blocksPerPartition
			if p >= numPartitions {
				p = numPartitions - 1
			}
			scratch[offsets[p]] = buffer[i]
			offsets[p]++
		}
		// Encode each partition's chunk into a contiguous byte slice, write() once
		for p := range numPartitions {
			start := partOffsets[p]
			end := start + partCounts[p]
			if start == end {
				continue
			}
			n := (end - start) * entrySize
			buf := encodeBuf[:n]
			for j := start; j < end; j++ {
				off := (j - start) * entrySize
				binary.LittleEndian.PutUint64(buf[off:], scratch[j].k0)
				binary.LittleEndian.PutUint64(buf[off+8:], scratch[j].k1)
			}
			_, _ = files[p].Write(buf)
		}
		buffer = buffer[:0]
	}

	writeStart := time.Now()
	for range numEntries {
		blockID := rng.Uint32N(uint32(numBlocks))
		buffer = append(buffer, bufferedEntry{
			k0:      rng.Uint64(),
			k1:      rng.Uint64(),
			blockID: blockID,
		})
		if len(buffer) >= bufferCap {
			flush()
		}
	}
	flush()
	writeDur := time.Since(writeStart)
	writeRate := float64(numEntries) / writeDur.Seconds() / 1e6

	fmt.Printf("  P=%4d  Write: %6.2fs (%6.2f M entries/sec)\n",
		numPartitions, writeDur.Seconds(), writeRate)
}

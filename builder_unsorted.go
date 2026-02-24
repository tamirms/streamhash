package streamhash

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"os"

	streamerrors "github.com/tamirms/streamhash/errors"
	"golang.org/x/sys/unix"
)

const (
	// unsortedMarginSigmas is the number of standard deviations of margin
	// for unsorted mode block regions. For Poisson distribution with mean μ,
	// we allocate μ + k*√μ slots where k = unsortedMarginSigmas.
	// 7 sigmas provides ~10^-12 overflow probability per block (Q(7) ≈ 1.28×10^-12).
	unsortedMarginSigmas = 7.0

	// defaultWriteBufferBytes is the total memory budget for per-block write buffers.
	// Entries are buffered in memory per-block and flushed sequentially to the mmap
	// when a block's buffer fills. This converts scattered random mmap writes into
	// sequential burst writes, improving cache/TLB locality.
	defaultWriteBufferBytes = 16 << 20 // 16MB
)

// unsortedBuffer encapsulates the temp file, mmap, and per-block counters
// used to buffer entries in unsorted mode. Entries are buffered in memory
// per-block and flushed to the mmap when a block's buffer fills, converting
// scattered random writes into sequential burst writes. Entries are later
// read back in block order for building.
type unsortedBuffer struct {
	tempFile       *os.File
	tempData       []byte
	tempPath       string
	counter        []uint32 // flushed entries per block (written to mmap)
	regionSize     uint64
	regionCapacity uint32
	entrySize      int
	cfg            *buildConfig
	numBlocks      uint32

	// Per-block write buffers: entries are accumulated here before flushing
	// sequentially to the mmap region. Total entries for a block =
	// counter[blockID] + len(writeBuffers[blockID]).
	writeBuffers   [][]routedEntry
	flushThreshold int // entries per block before flush
}

// newUnsortedBuffer creates an unsortedBuffer with mmap'd temp file.
func newUnsortedBuffer(cfg *buildConfig, numBlocks uint32) (*unsortedBuffer, error) {
	u := &unsortedBuffer{
		cfg:       cfg,
		numBlocks: numBlocks,
		entrySize: minKeySize + cfg.fingerprintSize + cfg.payloadSize,
	}

	// Compute expected keys per block.
	// Keys are distributed by prefix hash, following Poisson distribution.
	avgKeysPerBlock := float64(cfg.totalKeys) / float64(numBlocks)

	// Compute region capacity with dynamic safety margin.
	// For Poisson with mean μ, variance = μ, so σ = √μ.
	// We allocate μ + k*σ slots where k = unsortedMarginSigmas (7 sigmas ≈ 10^-12).
	// This scales correctly for any block size: large blocks need ~15% margin,
	// small blocks need proportionally more (e.g., 100% for μ=49 keys).
	marginFactor := 1.0 + unsortedMarginSigmas/math.Sqrt(avgKeysPerBlock)
	u.regionCapacity = uint32(math.Ceil(avgKeysPerBlock * marginFactor))
	u.regionSize = uint64(u.regionCapacity) * uint64(u.entrySize)

	// Total temp file size with overflow check
	// Max safe size is math.MaxInt64 to avoid overflow when casting to int64
	if u.regionSize > 0 && uint64(numBlocks) > uint64(math.MaxInt64)/u.regionSize {
		return nil, fmt.Errorf("temp file size overflow: %d blocks × %d bytes/region exceeds int64", numBlocks, u.regionSize)
	}
	totalSize := int64(numBlocks) * int64(u.regionSize)

	// Create temp file
	if err := u.createTempFile(cfg.tempDir); err != nil {
		return nil, fmt.Errorf("create temp file: %w", err)
	}

	// Pre-allocate disk blocks (prevents SIGBUS on disk full)
	if err := fallocateFile(u.tempFile, totalSize); err != nil {
		primaryErr := fmt.Errorf("pre-allocate temp file: %w", err)
		return nil, errors.Join(primaryErr, u.cleanup())
	}

	// mmap the temp file
	data, err := unix.Mmap(int(u.tempFile.Fd()), 0, int(totalSize),
		unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED)
	if err != nil {
		primaryErr := fmt.Errorf("mmap temp file: %w", err)
		return nil, errors.Join(primaryErr, u.cleanup())
	}
	u.tempData = data

	// Hint for random write pattern during routing phase
	_ = unix.Madvise(u.tempData, unix.MADV_RANDOM)

	// Request transparent huge pages (must precede prefault for 2MB direct allocation)
	hintHugePages(u.tempData)
	// Pre-fault all pages to eliminate minor faults during AddKey writes
	prefaultRegion(u.tempData)

	// Allocate counter array
	u.counter = make([]uint32, numBlocks)

	// Initialize per-block write buffers.
	// Budget is in terms of routedEntry structs (32 bytes each: 3×uint64 + uint32 + padding).
	const routedEntrySize = 32
	u.flushThreshold = max(1, defaultWriteBufferBytes/(int(numBlocks)*routedEntrySize))
	u.writeBuffers = make([][]routedEntry, numBlocks)
	for i := range u.writeBuffers {
		u.writeBuffers[i] = make([]routedEntry, 0, u.flushThreshold)
	}

	return u, nil
}

// createTempFile creates a temp file for unsorted mode.
// Tries O_TMPFILE on Linux for auto-cleanup, falls back to regular temp file.
func (u *unsortedBuffer) createTempFile(tempDir string) error {
	if tempDir == "" {
		tempDir = os.TempDir()
	}

	// Try O_TMPFILE first (Linux 3.11+)
	// O_TMPFILE creates an anonymous file that is automatically deleted on close
	f, err := openTmpFile(tempDir)
	if err == nil {
		u.tempFile = f
		u.tempPath = "" // Anonymous file - no path to remove
		return nil
	}

	// Fallback to regular temp file
	f, err = os.CreateTemp(tempDir, "streamhash-*.tmp")
	if err != nil {
		return err
	}
	u.tempFile = f
	u.tempPath = f.Name()
	return nil
}

// openTmpFile attempts to create an O_TMPFILE anonymous temp file.
// Returns an error if O_TMPFILE is not supported.
func openTmpFile(dir string) (*os.File, error) {
	// O_TMPFILE is Linux-specific (kernel 3.11+)
	// On other platforms, this will return an error
	const oTmpFile = 0o20000000 //nolint:revive // Linux O_TMPFILE flag

	fd, err := unix.Open(dir, unix.O_RDWR|oTmpFile, 0600)
	if err != nil {
		return nil, err
	}
	return os.NewFile(uintptr(fd), ""), nil
}

// addKey buffers an entry for the given block. When the block's buffer fills,
// it is flushed sequentially to the mmap region.
func (u *unsortedBuffer) addKey(k0, k1 uint64, payload uint64, fingerprint uint32, blockID uint32) error {
	// Early overflow detection: counter (flushed) + buffer (pending) + 1 (this entry)
	if u.counter[blockID]+uint32(len(u.writeBuffers[blockID]))+1 > u.regionCapacity {
		return fmt.Errorf("%w: block %d overflow (capacity %d)",
			streamerrors.ErrRegionOverflow, blockID, u.regionCapacity)
	}
	u.writeBuffers[blockID] = append(u.writeBuffers[blockID],
		routedEntry{k0: k0, k1: k1, payload: payload, fingerprint: fingerprint})
	if len(u.writeBuffers[blockID]) >= u.flushThreshold {
		u.flushBlock(blockID)
	}
	return nil
}

// flushBlock writes buffered entries sequentially to the block's mmap region.
func (u *unsortedBuffer) flushBlock(blockID uint32) {
	buf := u.writeBuffers[blockID]
	for _, e := range buf {
		pos := uint64(blockID)*u.regionSize + uint64(u.counter[blockID])*uint64(u.entrySize)
		binary.LittleEndian.PutUint64(u.tempData[pos:], e.k0)
		binary.LittleEndian.PutUint64(u.tempData[pos+8:], e.k1)
		if u.cfg.fingerprintSize > 0 {
			packFingerprintToBytes(u.tempData[pos+minKeySize:], e.fingerprint, u.cfg.fingerprintSize)
		}
		if u.cfg.payloadSize > 0 {
			packPayloadToBytes(u.tempData[pos+minKeySize+uint64(u.cfg.fingerprintSize):],
				e.payload, u.cfg.payloadSize)
		}
		u.counter[blockID]++
	}
	u.writeBuffers[blockID] = buf[:0]
}

// flushAll flushes all non-empty write buffers to the mmap. Called by
// prepareForRead before the replay phase.
func (u *unsortedBuffer) flushAll() {
	for blockID := range u.numBlocks {
		if len(u.writeBuffers[blockID]) > 0 {
			u.flushBlock(blockID)
		}
	}
	u.writeBuffers = nil // free buffer memory before replay
}

// blockCount returns the number of entries stored for the given block.
func (u *unsortedBuffer) blockCount(blockID uint32) uint32 {
	return u.counter[blockID]
}

// readEntry reads an entry from the temp file and returns a routedEntry.
//
// Optimized to read values directly from tempData without intermediate buffer allocation.
// Entry format: [first 16 bytes of key][fingerprint (fpSize bytes)][payload (payloadSize bytes)]
func (u *unsortedBuffer) readEntry(blockID, index uint32) routedEntry {
	pos := uint64(blockID)*u.regionSize + uint64(index)*uint64(u.entrySize)

	// Read first 16 bytes of key data directly
	data := u.tempData[pos : pos+minKeySize]

	// k0, k1 are the first 16 bytes as little-endian uint64s
	k0 := binary.LittleEndian.Uint64(data[0:8])
	k1 := binary.LittleEndian.Uint64(data[8:16])

	// Read fingerprint directly from tempData (at offset minKeySize)
	fingerprint := unpackFingerprintFromBytes(u.tempData[pos+minKeySize:], u.cfg.fingerprintSize)

	// Read payload
	var payload uint64
	if u.cfg.payloadSize > 0 {
		payloadPos := pos + minKeySize + uint64(u.cfg.fingerprintSize)
		payload = unpackPayloadFromBytes(u.tempData[payloadPos:], u.cfg.payloadSize)
	}

	return routedEntry{
		k0:          k0,
		k1:          k1,
		payload:     payload,
		fingerprint: fingerprint,
	}
}

// prepareForRead flushes write buffers and switches to sequential read hints.
// Called before iterating entries in block order.
//
// No MADV_DONTNEED: dirty MAP_SHARED pages are already in the page cache from
// the write phase. MADV_DONTNEED would force the kernel to write them to disk
// and evict them, then the replay would fault them all back in from disk.
func (u *unsortedBuffer) prepareForRead() {
	u.flushAll()
	_ = unix.Madvise(u.tempData, unix.MADV_SEQUENTIAL)
}

// cleanup releases all temp file resources. Idempotent: nil-checks all fields
// before operating and nils them after cleanup. Safe to call multiple times
// (error paths + Close()).
func (u *unsortedBuffer) cleanup() error {
	var errs []error

	// Step 1: Unmap first (required before close on some platforms)
	if u.tempData != nil {
		if err := unix.Munmap(u.tempData); err != nil {
			errs = append(errs, fmt.Errorf("munmap: %w", err))
		}
		u.tempData = nil
	}

	// Step 2: Close file (O_TMPFILE auto-deletes here)
	if u.tempFile != nil {
		if err := u.tempFile.Close(); err != nil {
			errs = append(errs, fmt.Errorf("close temp file: %w", err))
		}
		u.tempFile = nil
	}

	// Step 3: Remove only if using fallback (O_TMPFILE auto-deleted on close)
	if u.tempPath != "" {
		if err := os.Remove(u.tempPath); err != nil && !os.IsNotExist(err) {
			errs = append(errs, fmt.Errorf("remove temp file: %w", err))
		}
		u.tempPath = ""
	}

	u.counter = nil
	u.writeBuffers = nil

	return errors.Join(errs...)
}

// finishUnsorted builds blocks directly from the mmap'd temp file data.
// Unlike the old replay-through-sorted-pipeline approach, this iterates
// blocks directly and feeds entries to the block builder (single-threaded)
// or dispatches block work to parallel workers, sharing the downstream
// worker infrastructure with sorted mode.
//
// Correctness is validated by TestBuildModeEquivalence.
func (b *Builder) finishUnsorted() error {
	b.unsortedBuf.prepareForRead()

	if b.workers > 1 {
		return b.finishUnsortedParallel()
	}
	return b.finishUnsortedSingleThreaded()
}

// finishUnsortedSingleThreaded builds blocks directly from mmap data.
// Iterates all numBlocks blocks (0..numBlocks-1), so no trailing empties
// are needed — iw.finalize() only adds the sentinel RAM index entry.
func (b *Builder) finishUnsortedSingleThreaded() error {
	numBlocks := b.unsortedBuf.numBlocks
	for blockID := range numBlocks {
		// Periodic context check (per-block, not per-entry)
		if blockID%64 == 0 {
			select {
			case <-b.ctx.Done():
				return errors.Join(b.ctx.Err(), b.unsortedBuf.cleanup(), b.cleanup())
			default:
			}
		}

		count := b.unsortedBuf.blockCount(blockID)
		if count == 0 {
			b.commitEmptyBlock()
			continue
		}
		b.builder.Reset()
		for i := range count {
			entry := b.unsortedBuf.readEntry(blockID, i)
			b.builder.AddKey(entry.k0, entry.k1, entry.payload, entry.fingerprint)
		}
		if err := b.buildBlockZeroCopy(); err != nil {
			return errors.Join(err, b.unsortedBuf.cleanup(), b.cleanup())
		}
	}

	// Cleanup temp file, then finalize output
	if err := b.unsortedBuf.cleanup(); err != nil {
		return errors.Join(err, b.cleanup())
	}
	b.unsortedBuf = nil
	return b.iw.finalize()
}

// finishUnsortedParallel dispatches blocks directly to parallel workers.
// Bulk-reads entries from each block's mmap region into pooled slices,
// then dispatches via dispatchBlockWork. All numBlocks blocks are dispatched
// in the loop, so no trailing empties are needed.
func (b *Builder) finishUnsortedParallel() error {
	b.initParallelWorkers()

	numBlocks := b.unsortedBuf.numBlocks
	for blockID := range numBlocks {
		// Periodic context + writer-error check
		if blockID%64 == 0 {
			select {
			case <-b.ctx.Done():
				return errors.Join(b.ctx.Err(), b.unsortedBuf.cleanup(), b.cleanup())
			default:
			}
			if b.writerDone != nil {
				select {
				case err := <-b.writerDone:
					b.writerErr = err
					return errors.Join(err, b.unsortedBuf.cleanup(), b.cleanup())
				default:
				}
			}
		}

		count := b.unsortedBuf.blockCount(blockID)
		if count == 0 {
			if err := b.dispatchEmptyBlock(blockID); err != nil {
				return errors.Join(err, b.unsortedBuf.cleanup(), b.cleanup())
			}
			continue
		}

		entries := b.getEntrySlice()
		if cap(entries) < int(count) {
			b.putEntrySlice(entries)
			entries = make([]routedEntry, 0, count)
		}
		for i := range count {
			entries = append(entries, b.unsortedBuf.readEntry(blockID, i))
		}
		if err := b.dispatchBlockWork(blockID, entries); err != nil {
			return errors.Join(err, b.unsortedBuf.cleanup(), b.cleanup())
		}
	}

	// Safe to cleanup temp file: all entries have been copied from the mmap
	// into pooled []routedEntry slices. Workers read from slices, not the mmap.
	if err := b.unsortedBuf.cleanup(); err != nil {
		return errors.Join(err, b.cleanup())
	}
	b.unsortedBuf = nil

	// Drain pipeline directly — all numBlocks blocks were dispatched, no trailing empties.
	return b.drainParallelPipeline()
}

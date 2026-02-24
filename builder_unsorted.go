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
)

// unsortedBuffer encapsulates the temp file, mmap, and per-block counters
// used to buffer entries in unsorted mode. Entries are written in arrival
// order within each block's region and later read back in block order
// for replay through the sorted pipeline.
type unsortedBuffer struct {
	tempFile       *os.File
	tempData       []byte
	tempPath       string
	counter        []uint32
	regionSize     uint64
	regionCapacity uint32
	entrySize      int
	cfg            *buildConfig
	numBlocks      uint32
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

	// Allocate counter array
	u.counter = make([]uint32, numBlocks)

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

// addKey writes an entry to the temp file in the appropriate block region.
func (u *unsortedBuffer) addKey(key []byte, k0, k1 uint64, payload uint64, fingerprint uint32, blockID uint32) error {
	// Bounds check - prevent writing beyond region
	if u.counter[blockID] >= u.regionCapacity {
		return fmt.Errorf("%w: block %d has %d entries (capacity %d)",
			streamerrors.ErrRegionOverflow, blockID, u.counter[blockID], u.regionCapacity)
	}

	// Calculate position in mmap
	pos := uint64(blockID)*u.regionSize + uint64(u.counter[blockID])*uint64(u.entrySize)

	// Write entry: [key[0:16]][fingerprint][payload]
	// Copy first 16 bytes of key (k0 and k1 for bucket/slot computation)
	copy(u.tempData[pos:pos+minKeySize], key[:minKeySize])

	// Write fingerprint (pre-computed using hybrid extraction)
	if u.cfg.fingerprintSize > 0 {
		packFingerprintToBytes(u.tempData[pos+minKeySize:], fingerprint, u.cfg.fingerprintSize)
	}

	// Write payload
	if u.cfg.payloadSize > 0 {
		payloadPos := pos + minKeySize + uint64(u.cfg.fingerprintSize)
		packPayloadToBytes(u.tempData[payloadPos:], payload, u.cfg.payloadSize)
	}

	u.counter[blockID]++
	return nil
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

// prepareForRead resets page state and switches to sequential read hints.
// Called before the replay phase reads entries in block order.
func (u *unsortedBuffer) prepareForRead() {
	// NOTE: No msync needed - both phases access same mmap, data visible in page cache
	_ = unix.Madvise(u.tempData, unix.MADV_DONTNEED)   // Reset page state after random writes
	_ = unix.Madvise(u.tempData, unix.MADV_SEQUENTIAL) // Enable readahead for sequential scan
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

	return errors.Join(errs...)
}

// finishUnsorted replays buffered entries through the sorted pipeline
// (addKeySingleThreaded/addKeyParallel) rather than building blocks directly.
// This eliminates ~55 lines of duplicated block-building logic and ensures
// sorted and unsorted modes use identical code paths for block construction,
// which is validated by TestUnsortedReplay_BitForBitParity.
func (b *Builder) finishUnsorted() error {
	b.unsortedBuf.prepareForRead()

	// Parallel workers must not be initialized before this point.
	// NewBuilder skips initParallelWorkers for unsorted mode; we init here.
	if b.workers > 1 {
		b.initParallelWorkers()
		b.pendingEntries = b.getEntrySlice()
	}

	// Explicitly reset sorted pipeline state for replay.
	// These are already in initial state from construction, but making
	// the dependency explicit prevents subtle bugs if Builder init changes.
	b.firstKey = true
	b.nextBlockToWrite = 0
	b.keysBefore = 0
	b.currentBlockIdx = 0

	// Replay entries through sorted pipeline in block order.
	// INVARIANT: entries are replayed in strictly ascending block order
	// (0, 1, 2, ...). This is guaranteed by the outer loop structure
	// and is required for addKeySingleThreaded/addKeyParallel's
	// block-transition and gap-filling logic to work correctly.
	replayCounter := 0
	numBlocks := b.unsortedBuf.numBlocks
	for blockID := range numBlocks {
		count := b.unsortedBuf.blockCount(blockID)
		for i := range count {
			// Periodic context + writer-error check during replay.
			// Replay bypasses AddKey, so we check here instead.
			replayCounter++
			if replayCounter >= contextCheckInterval {
				replayCounter = 0
				select {
				case <-b.ctx.Done():
					return errors.Join(b.ctx.Err(), b.unsortedBuf.cleanup(), b.cleanup())
				default:
				}
				if b.workers > 1 && b.writerDone != nil {
					select {
					case err := <-b.writerDone:
						b.writerErr = err
						return errors.Join(err, b.unsortedBuf.cleanup(), b.cleanup())
					default:
					}
				}
			}

			entry := b.unsortedBuf.readEntry(blockID, i)
			var err error
			if b.workers > 1 {
				err = b.addKeyParallel(entry.k0, entry.k1, entry.payload,
					entry.fingerprint, blockID)
			} else {
				err = b.addKeySingleThreaded(entry.k0, entry.k1, entry.payload,
					entry.fingerprint, blockID)
			}
			if err != nil {
				return errors.Join(err, b.unsortedBuf.cleanup(), b.cleanup())
			}
		}
	}

	// Free temp file resources before finalization
	if err := b.unsortedBuf.cleanup(); err != nil {
		return errors.Join(err, b.cleanup())
	}
	b.unsortedBuf = nil

	// Finish via sorted pipeline (handles final block + trailing empties)
	if b.workers > 1 {
		return b.finishParallel()
	}
	return b.finishSingleThreaded()
}

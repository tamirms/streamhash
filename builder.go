package streamhash

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math/bits"
	"os"
	"sync"

	streamerrors "github.com/tamirms/streamhash/errors"
	intbits "github.com/tamirms/streamhash/internal/bits"
	"golang.org/x/sync/errgroup"
)

const (
	// contextCheckInterval is how often to check for context cancellation during AddKey.
	contextCheckInterval = 10000

	// maxKeyLength is the maximum key length accepted by AddKey.
	maxKeyLength = 65535

	// maxKeys is the maximum number of keys (~1.1 trillion). The limit comes
	// from the 40-bit cumulative key counter in the RAM index (see header.go).
	maxKeys = uint64(1) << 40
)

// Builder provides an AddKey-style API for building indexes.
// Supports both sorted and unsorted input modes.
//
// Usage (sorted mode - default):
//
//	builder, err := streamhash.NewBuilder(ctx, "index.idx", totalKeys, opts...)
//	if err != nil { return err }
//	defer builder.Close() // Clean up on error
//
//	for key, payload := range sortedData {
//	    if err := builder.AddKey(key, payload); err != nil { return err }
//	}
//	return builder.Finish()
//
// Usage (unsorted mode):
//
//	builder, err := streamhash.NewBuilder(ctx, "index.idx", totalKeys, WithUnsortedInput(), opts...)
//	if err != nil { return err }
//	defer builder.Close()
//
//	for key, payload := range anyOrderData {
//	    if err := builder.AddKey(key, payload); err != nil { return err }
//	}
//	return builder.Finish()
//
// For parallel building with N workers:
//
//	builder, err := streamhash.NewBuilder(ctx, "index.idx", totalKeys,
//	    WithPayload(4), WithWorkers(8))
//	if err != nil { return err }
//	defer builder.Close()
//
//	for key, payload := range sortedData {
//	    if err := builder.AddKey(key, payload); err != nil { return err }
//	}
//	return builder.Finish()
type Builder struct {
	ctx              context.Context
	cfg              *buildConfig
	numBlocks        uint32
	iw               *indexWriter
	builder          blockBuilder        // Block builder (also used directly in single-threaded mode)
	output           string
	nextBlockToWrite uint32
	currentBlockIdx  uint32
	firstKey         bool
	keyCounter       int
	closed bool

	// Parallel mode fields (when workers > 1)
	workers         int
	workChan        chan blockWork
	resultChan      chan blockResult
	workerGroup     *errgroup.Group
	workerCtx       context.Context
	workerCancel    context.CancelFunc // Cancels workerCtx to unblock stuck workers
	writerDone      chan error
	writerErr       error
	workersShutDown bool // True after worker goroutines have been shut down

	// Block accumulation for parallel mode
	pendingEntries []routedEntry // Current block being accumulated
	keysBefore     uint64        // Cumulative keys before current block
	entryPool      sync.Pool     // Pool of []routedEntry slices for reuse
	metadataPool   sync.Pool     // Pool of []byte slices for metadata buffers

	// Sorted mode validation
	lastBlockID int64 // Last seen block ID (-1 initially), for detecting out-of-order keys

	// Key count tracking
	totalKeysAdded uint64 // Number of keys added via AddKey

	// Unsorted mode buffer (nil in sorted mode)
	unsortedBuf *unsortedBuffer
}

// NewBuilder creates a new builder for building indexes.
// By default, keys must be added in sorted order by block index.
// Use WithUnsortedInput() to enable unsorted input mode.
//
// totalKeys must be the exact number of keys that will be added.
//
// Use WithWorkers(N) to enable parallel building with N workers.
// With workers > 1, blocks are built in parallel while maintaining
// the streaming API and O(W × block_size) memory.
func NewBuilder(ctx context.Context, output string, totalKeys uint64, opts ...BuildOption) (*Builder, error) {
	if totalKeys == 0 {
		return nil, streamerrors.ErrEmptyIndex
	}

	if totalKeys > maxKeys {
		return nil, streamerrors.ErrTooManyKeys
	}

	cfg := defaultBuildConfig()
	for _, opt := range opts {
		opt(cfg)
	}
	cfg.totalKeys = totalKeys

	if cfg.payloadSize < 0 || cfg.payloadSize > maxPayloadSize {
		return nil, streamerrors.ErrPayloadTooLarge
	}
	if cfg.fingerprintSize < 0 || cfg.fingerprintSize > maxFingerprintSize {
		return nil, streamerrors.ErrFingerprintTooLarge
	}

	// Get the block builder for building
	bldr, err := newBlockBuilder(cfg.algorithm, totalKeys, cfg.globalSeed, cfg.payloadSize, cfg.fingerprintSize)
	if err != nil {
		return nil, err
	}

	numBlocks := bldr.NumBlocks()

	iw, err := newIndexWriter(output, cfg, numBlocks, bldr)
	if err != nil {
		return nil, fmt.Errorf("create index writer: %w", err)
	}

	// Determine worker count
	workers := cfg.workers
	if workers <= 0 {
		workers = 1 // Default to single-threaded
	}
	if workers > int(numBlocks) {
		workers = int(numBlocks)
	}

	b := &Builder{
		ctx:          ctx,
		cfg:          cfg,
		numBlocks:    numBlocks,
		iw:           iw,
		builder:      bldr,
		output:       output,
		firstKey:    true,
		workers:     workers,
		lastBlockID: -1,
	}

	if cfg.unsortedInput {
		// Unsorted mode: initialize partition flush buffer
		buf, err := newUnsortedBuffer(cfg, numBlocks)
		if err != nil {
			primaryErr := fmt.Errorf("init unsorted mode: %w", err)
			return nil, errors.Join(primaryErr, iw.close(), os.Remove(output))
		}
		b.unsortedBuf = buf
	} else if workers > 1 {
		// Sorted parallel mode: initialize channels, pools, and workers
		b.initParallelWorkers()
		b.pendingEntries = b.getEntrySlice()
	}
	// Single-threaded sorted mode uses bldr directly (already set above)

	return b, nil
}

// AddKey adds a key-payload pair to the index.
// In sorted mode, keys must be added in sorted order by block index.
// In unsorted mode, keys can be added in any order.
// The payload is passed as uint64; for smaller sizes (e.g., 4 bytes), pass uint64(yourUint32).
func (b *Builder) AddKey(key []byte, payload uint64) error {
	if b.closed {
		return streamerrors.ErrBuilderClosed
	}

	// Validate key length
	if len(key) > maxKeyLength {
		return streamerrors.ErrKeyTooLong
	}

	// Validate key is at least 16 bytes (required for routing and Mix function)
	if len(key) < minKeySize {
		return streamerrors.ErrKeyTooShort
	}

	// Validate payload fits in configured PayloadSize
	// For PayloadSize < 8, check that value doesn't exceed max for that byte width
	if ps := b.cfg.payloadSize; ps > 0 && ps < 8 {
		maxValue := uint64(1)<<(ps*8) - 1
		if payload > maxValue {
			return streamerrors.ErrPayloadOverflow
		}
	}

	// Check context periodically (applies to both sorted and unsorted modes)
	b.keyCounter++
	if b.keyCounter >= contextCheckInterval {
		b.keyCounter = 0
		select {
		case <-b.ctx.Done():
			return b.ctx.Err()
		default:
		}
		// Check for writer errors in parallel mode
		if b.workers > 1 && b.writerDone != nil {
			select {
			case err := <-b.writerDone:
				b.writerErr = err
				return err
			default:
			}
		}
	}

	// Parse key once: extract k0, k1, compute prefix and blockIdx
	k0 := binary.LittleEndian.Uint64(key[0:8])
	k1 := binary.LittleEndian.Uint64(key[8:16])
	prefix := bits.ReverseBytes64(k0) // Big-endian for monotonic routing
	blockIdx := intbits.FastRange32(prefix, b.numBlocks)

	if b.unsortedBuf != nil {
		if b.unsortedBuf.addKey(k0, k1, payload, blockIdx) {
			if err := b.unsortedBuf.flush(); err != nil {
				return err
			}
		}
		// Track key count only after successful add
		b.totalKeysAdded++
		return nil
	}

	// Validate monotonicity (sorted mode requires keys in block order)
	if int64(blockIdx) < b.lastBlockID {
		return streamerrors.ErrUnsortedInput
	}
	b.lastBlockID = int64(blockIdx)

	// Call the appropriate add function.
	// Fingerprint is computed here for single-threaded mode; parallel mode
	// defers fingerprint computation to workers (not stored in routedEntry).
	var err error
	if b.workers > 1 {
		err = b.addKeyParallel(k0, k1, payload, blockIdx)
	} else {
		fingerprint := extractFingerprint(k0, k1, b.cfg.fingerprintSize)
		err = b.addKeySingleThreaded(k0, k1, payload, fingerprint, blockIdx)
	}

	if err != nil {
		return err
	}
	// Track key count only after successful add
	b.totalKeysAdded++
	return nil
}

// addKeySingleThreaded handles AddKey in single-threaded sorted mode.
// Parameters are pre-parsed in AddKey for efficiency.
func (b *Builder) addKeySingleThreaded(k0, k1 uint64, payload uint64, fingerprint uint32, blockIdx uint32) error {
	if b.firstKey {
		// Emit empty blocks for indices 0 to blockIdx-1 using zero-copy
		for b.nextBlockToWrite < blockIdx {
			b.commitEmptyBlock()
			b.nextBlockToWrite++
		}
		// Reset builder after gap emission leaves it in post-build state
		b.resetCurrentBuilder()
		b.currentBlockIdx = blockIdx
		b.firstKey = false
	} else if blockIdx != b.currentBlockIdx {
		// Emit current block using zero-copy path
		if b.currentBlockKeysAdded() > 0 {
			if err := b.buildBlockZeroCopy(); err != nil {
				return err
			}
			b.nextBlockToWrite++
		}

		// Emit empty blocks for gaps using zero-copy
		for b.nextBlockToWrite < blockIdx {
			b.commitEmptyBlock()
			b.nextBlockToWrite++
		}
		// Reset builder after gap emission leaves it in post-build state
		b.resetCurrentBuilder()

		b.currentBlockIdx = blockIdx
	}

	// Add key to block builder (values already parsed in AddKey)
	b.builder.AddKey(k0, k1, payload, fingerprint)
	return nil
}

// currentBlockKeysAdded returns the number of keys added to the current block builder.
func (b *Builder) currentBlockKeysAdded() int {
	return b.builder.KeysAdded()
}

// resetCurrentBuilder resets the current block builder for reuse.
func (b *Builder) resetCurrentBuilder() {
	b.builder.Reset()
}

// commitEmptyBlock writes an empty block's metadata directly to the index.
// Resets the builder, builds empty metadata, and commits.
func (b *Builder) commitEmptyBlock() {
	b.builder.Reset()
	metadataDst := b.iw.getMetadataRegion(b.estimateMetadataSize())
	metadataLen, _, _, _ := b.builder.BuildSeparatedInto(metadataDst, nil)
	b.iw.commitBlock(metadataLen, 0)
}

// estimateMetadataSize returns max metadata size for the current block.
func (b *Builder) estimateMetadataSize() int {
	return b.builder.MaxMetadataSizeForCurrentBlock()
}

// Finish completes the index and writes it to disk.
// After calling Finish, the builder cannot be used again.
func (b *Builder) Finish() error {
	if b.closed {
		return streamerrors.ErrBuilderClosed
	}
	b.closed = true

	// Validate key count matches declared total
	if b.totalKeysAdded != b.cfg.totalKeys {
		primaryErr := fmt.Errorf("%w: expected %d, got %d", streamerrors.ErrKeyCountMismatch, b.cfg.totalKeys, b.totalKeysAdded)
		if b.unsortedBuf != nil {
			return errors.Join(primaryErr, b.unsortedBuf.cleanup(), b.cleanup())
		}
		return errors.Join(primaryErr, b.cleanup())
	}

	if b.unsortedBuf != nil {
		return b.finishUnsorted()
	}

	if b.workers > 1 {
		return b.finishParallel()
	}
	return b.finishSingleThreaded()
}

// finishSingleThreaded completes the build in single-threaded sorted mode.
func (b *Builder) finishSingleThreaded() error {
	// Emit final block using zero-copy path
	if b.currentBlockKeysAdded() > 0 {
		if err := b.buildBlockZeroCopy(); err != nil {
			return errors.Join(err, b.cleanup())
		}
	}

	// Emit trailing empty blocks
	for b.iw.blockCount < b.numBlocks {
		b.commitEmptyBlock()
	}

	return b.iw.finalize()
}

// buildBlockZeroCopy builds the current block directly into mmap (zero-copy).
func (b *Builder) buildBlockZeroCopy() error {
	numKeys := b.currentBlockKeysAdded()
	entrySize := b.cfg.payloadSize + b.cfg.fingerprintSize

	// Get mmap regions for direct writes
	payloadOffset := b.iw.keyCount * uint64(entrySize)
	payloadsDst := b.iw.getPayloadRegion(payloadOffset, uint64(numKeys*entrySize))
	metadataDst := b.iw.getMetadataRegion(b.estimateMetadataSize())

	// Build directly into mmap regions (no intermediate allocation)
	metadataLen, _, builtNumKeys, err := b.builder.BuildSeparatedInto(metadataDst, payloadsDst)
	if err != nil {
		return err
	}

	// Update bookkeeping (no copy needed!)
	b.iw.commitBlock(metadataLen, builtNumKeys)
	return nil
}

// Close aborts the build and cleans up resources.
// Call this if an error occurs during AddKey calls.
// Safe to call after Finish() — ensures worker goroutines are shut down
// even if Finish() returned an error.
func (b *Builder) Close() error {
	if b.closed {
		// Finish() was called but may have failed with workers still running.
		// Shut them down if they haven't been already.
		// cleanup() handles shutdown, but iw may already be closed — call
		// shutdownWorkers directly for the post-Finish path.
		b.shutdownWorkers()
		return nil
	}
	b.closed = true

	var errs []error

	// Cleanup temp file if in unsorted mode
	if b.unsortedBuf != nil {
		if err := b.unsortedBuf.cleanup(); err != nil {
			errs = append(errs, err)
		}
		b.unsortedBuf = nil
	}

	// cleanup() shuts down workers before unmapping iw.data
	if err := b.cleanup(); err != nil {
		errs = append(errs, err)
	}
	return errors.Join(errs...)
}

// cleanup shuts down any running worker goroutines and removes the output file.
// Workers must be stopped before unmapping iw.data, which they may still be
// accessing via writePayloadsDirect.
func (b *Builder) cleanup() error {
	b.shutdownWorkers()
	return errors.Join(b.iw.close(), os.Remove(b.output))
}


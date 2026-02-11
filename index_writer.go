package streamhash

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"os"

	"github.com/cespare/xxhash/v2"
	"github.com/edsrzf/mmap-go"
)

// indexWriter handles writing index data to disk using mmap-based zero-copy writes.
// File layout: [Header 64B][UserMetaLen 4B][UserMeta][AlgoConfigLen 4B][AlgoConfig][RAM Index (N+1)×10B][Payload Region][Metadata Region][Footer 32B]
type indexWriter struct {
	file *os.File
	mmap mmap.MMap // Memory-mapped region
	data []byte    // View into mmap for direct writes

	// Region offsets (computed upfront for separated layout)
	payloadRegionOffset  uint64
	metadataRegionOffset uint64

	// Variable section offsets
	userMetadataOffset uint64
	algoConfigOffset   uint64
	ramIndexOffset     uint64

	// Write tracking
	metadataWriteOffset uint64 // Current write position in metadata region

	// RAM index (built incrementally)
	ramIndex []ramIndexEntry

	// Streaming hashers for incremental hash computation
	// These hash data while it's hot in CPU cache during writes
	payloadHasher  *xxhash.Digest // Hash-of-hashes: folds per-block payload hashes in order
	metadataHasher *xxhash.Digest // Streaming hash of metadata as it's written

	// Config
	header        header
	cfg           *buildConfig
	numBlocks     uint32
	estimatedSize uint64 // Pre-allocated file size
	algo          blockBuilder
	userMetadata  []byte

	// State
	keyCount   uint64
	blockCount uint32
}

// newIndexWriter creates a new mmap-based index writer using separated layout.
// The file is pre-allocated and memory-mapped for zero-copy writes.
func newIndexWriter(path string, cfg *buildConfig, numBlocks uint32, algo blockBuilder) (*indexWriter, error) {
	// Compute variable section sizes
	userMetadataLen := len(cfg.userMetadata)
	algoConfigLen := algo.GlobalConfigSize()

	// Estimate total file size for pre-allocation
	estimatedSize := estimateFileSizeForAlgo(cfg, numBlocks, algo, userMetadataLen, algoConfigLen)

	file, err := os.Create(path)
	if err != nil {
		return nil, fmt.Errorf("failed to create index file: %w", err)
	}

	entrySize := cfg.payloadSize + cfg.fingerprintSize

	// Compute region offsets for separated layout with variable sections
	// Layout: [Header][UserMetaLen 4B][UserMeta][AlgoConfigLen 4B][AlgoConfig][RAM Index]...
	userMetadataOffset := uint64(headerSize)
	algoConfigOffset := userMetadataOffset + 4 + uint64(userMetadataLen)
	ramIndexOffset := algoConfigOffset + 4 + uint64(algoConfigLen)
	ramIndexSize := uint64(numBlocks+1) * uint64(ramIndexEntrySize)
	payloadRegionOffset := ramIndexOffset + ramIndexSize
	payloadRegionSize := cfg.totalKeys * uint64(entrySize)
	metadataRegionOffset := payloadRegionOffset + payloadRegionSize

	// Pre-allocate disk blocks to prevent SIGBUS on disk full
	if err := fallocateFile(file, int64(estimatedSize)); err != nil {
		primaryErr := fmt.Errorf("failed to allocate disk space: %w", err)
		return nil, errors.Join(primaryErr, file.Close())
	}

	// Memory map the file for zero-copy writes
	mm, err := mmap.MapRegion(file, int(estimatedSize), mmap.RDWR, 0, 0)
	if err != nil {
		primaryErr := fmt.Errorf("failed to mmap file: %w", err)
		return nil, errors.Join(primaryErr, file.Close())
	}

	iw := &indexWriter{
		file:                 file,
		mmap:                 mm,
		data:                 []byte(mm),
		payloadRegionOffset:  payloadRegionOffset,
		metadataRegionOffset: metadataRegionOffset,
		userMetadataOffset:   userMetadataOffset,
		algoConfigOffset:     algoConfigOffset,
		ramIndexOffset:       ramIndexOffset,
		metadataWriteOffset:  metadataRegionOffset,
		ramIndex:             make([]ramIndexEntry, 0, numBlocks+1),
		payloadHasher:        xxhash.New(),
		metadataHasher:       xxhash.New(),
		cfg:                  cfg,
		numBlocks:            numBlocks,
		estimatedSize:        estimatedSize,
		algo:                 algo,
		userMetadata:         cfg.userMetadata,
	}

	// Prefault the payload region for better parallel write performance.
	// On Linux 5.14+, uses MADV_POPULATE_WRITE. No-op on other platforms.
	prefaultRegion(iw.data[payloadRegionOffset:metadataRegionOffset])

	// Initialize header (spec §5.2)
	// Note: TotalBuckets, BucketsPerBlock, PrefixBits are algorithm-internal
	// UserMetadata and AlgoConfig are stored in variable-length sections after header
	// Compute R (bits needed to index blocks) for header
	var r uint32
	if numBlocks > 1 {
		r = uint32(math.Ceil(math.Log2(float64(numBlocks))))
	}

	iw.header = header{
		Magic:           magic,
		Version:         version,
		TotalKeys:       cfg.totalKeys, // Set upfront for separated layout
		NumBlocks:       numBlocks,
		RAMBits:         r,
		PayloadSize:     uint32(cfg.payloadSize),
		FingerprintSize: uint8(cfg.fingerprintSize),
		Seed:            cfg.globalSeed,
		BlockAlgorithm:  cfg.algorithm,
	}

	return iw, nil
}

// writePayloadsDirect writes payload data directly to the mmap payload region.
// offset is relative to the start of the payload region.
// This is thread-safe for non-overlapping regions, enabling parallel payload writes.
func (iw *indexWriter) writePayloadsDirect(data []byte, offset uint64) error {
	if len(data) == 0 {
		return nil
	}

	absoluteOffset := iw.payloadRegionOffset + offset
	// Defensive bounds check to prevent overwriting metadata region
	if absoluteOffset+uint64(len(data)) > iw.metadataRegionOffset {
		return fmt.Errorf("writePayloadsDirect: write exceeds payload region boundary")
	}
	copy(iw.data[absoluteOffset:], data)
	return nil
}

// foldPayloadHash folds a per-block payload hash into the streaming payload hasher.
// This must be called in block order (block 0, 1, 2, ...) to produce deterministic results.
// Workers compute xxhash.Sum64(payloads) for their block, then the writer goroutine
// calls this method in order to fold those hashes into the final payload region hash.
func (iw *indexWriter) foldPayloadHash(blockPayloadHash uint64) {
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], blockPayloadHash)
	if _, err := iw.payloadHasher.Write(buf[:]); err != nil {
		panic("hash.Hash.Write returned unexpected error: " + err.Error())
	}
}

// writeMetadata writes metadata for a block to the mmap metadata region.
// Blocks must be written in order (blockID 0, 1, 2, ...).
// This is used with writePayloadsDirect for parallel builds where payloads
// are written directly by workers and only metadata goes through the queue.
func (iw *indexWriter) writeMetadata(metadata []byte, numKeys int) {
	// Record RAM index entry
	metadataRelOffset := iw.metadataWriteOffset - iw.metadataRegionOffset
	iw.ramIndex = append(iw.ramIndex, ramIndexEntry{
		KeysBefore:     iw.keyCount,
		MetadataOffset: metadataRelOffset,
	})

	// Write metadata directly to mmap region and update streaming hash
	if len(metadata) > 0 {
		copy(iw.data[iw.metadataWriteOffset:], metadata)
		if _, err := iw.metadataHasher.Write(metadata); err != nil {
			panic("hash.Hash.Write returned unexpected error: " + err.Error())
		}
		iw.metadataWriteOffset += uint64(len(metadata))
	}

	iw.keyCount += uint64(numKeys)
	iw.blockCount++
}

// finalize completes the index file using mmap-based zero-copy writes.
// On error, delegates to close() for idempotent cleanup.
// On success, nils mmap/file so that close() is a safe no-op.
func (iw *indexWriter) finalize() error {
	// Validate we didn't exceed estimated size
	actualSize := iw.metadataWriteOffset + footerSize
	if actualSize > iw.estimatedSize {
		primaryErr := fmt.Errorf("actual size %d exceeds estimated size %d", actualSize, iw.estimatedSize)
		return errors.Join(primaryErr, iw.close())
	}

	// Add sentinel entry for RAM index (spec §5.3)
	metadataRelOffset := iw.metadataWriteOffset - iw.metadataRegionOffset
	iw.ramIndex = append(iw.ramIndex, ramIndexEntry{
		KeysBefore:     iw.keyCount,
		MetadataOffset: metadataRelOffset,
	})

	// Get final hashes from streaming hashers (no re-read of regions needed!)
	// PayloadRegionHash: hash-of-hashes computed by folding per-block hashes in order
	// MetadataRegionHash: streaming hash computed as metadata was written
	payloadRegionHash := iw.payloadHasher.Sum64()
	metadataRegionHash := iw.metadataHasher.Sum64()

	// Update header
	iw.header.TotalKeys = iw.keyCount
	iw.header.NumBlocks = iw.blockCount

	// Write header to mmap at offset 0
	iw.header.encodeTo(iw.data[0:headerSize])

	// Write variable-length sections
	// UserMetadata: [length 4B][data]
	binary.LittleEndian.PutUint32(iw.data[iw.userMetadataOffset:], uint32(len(iw.userMetadata)))
	copy(iw.data[iw.userMetadataOffset+4:], iw.userMetadata)

	// AlgoConfig: [length 4B][data]
	algoConfigLen := iw.algo.GlobalConfigSize()
	binary.LittleEndian.PutUint32(iw.data[iw.algoConfigOffset:], uint32(algoConfigLen))
	if algoConfigLen > 0 {
		iw.algo.EncodeGlobalConfigInto(iw.data[iw.algoConfigOffset+4:])
	}

	// Write RAM index entries to mmap (after variable sections)
	for i, entry := range iw.ramIndex {
		offset := iw.ramIndexOffset + uint64(i)*ramIndexEntrySize
		encodeRAMIndexEntryTo(entry, iw.data[offset:])
	}

	// Write footer to mmap
	ftr := footer{
		PayloadRegionHash:  payloadRegionHash,
		MetadataRegionHash: metadataRegionHash,
	}
	ftr.encodeTo(iw.data[iw.metadataWriteOffset:])

	// Flush dirty pages to file (ensures writes visible before unmap)
	if err := iw.mmap.Flush(); err != nil {
		primaryErr := fmt.Errorf("mmap flush failed: %w", err)
		return errors.Join(primaryErr, iw.close())
	}

	// Unmap before truncate (required order).
	// Nil mmap regardless of outcome to prevent close() from retrying.
	unmapErr := iw.mmap.Unmap()
	iw.mmap = nil
	if unmapErr != nil {
		primaryErr := fmt.Errorf("mmap unmap failed: %w", unmapErr)
		return errors.Join(primaryErr, iw.close())
	}

	// Shrink to actual size
	if err := iw.file.Truncate(int64(actualSize)); err != nil {
		primaryErr := fmt.Errorf("truncate failed: %w", err)
		return errors.Join(primaryErr, iw.close())
	}

	closeErr := iw.file.Close()
	iw.file = nil
	return closeErr
}

// close closes the writer without finalizing (for error cleanup).
// Idempotent: safe to call multiple times.
func (iw *indexWriter) close() error {
	var unmapErr error
	if iw.mmap != nil {
		unmapErr = iw.mmap.Unmap()
		iw.mmap = nil
	}
	var closeErr error
	if iw.file != nil {
		closeErr = iw.file.Close()
		iw.file = nil
	}
	return errors.Join(unmapErr, closeErr)
}

// getPayloadRegion returns a slice into the mmap for direct payload writes.
// offset is in bytes from start of payload region.
// Panics if the requested region exceeds the metadata region boundary.
func (iw *indexWriter) getPayloadRegion(offset, length uint64) []byte {
	start := iw.payloadRegionOffset + offset
	end := start + length
	if end > iw.metadataRegionOffset {
		panic("getPayloadRegion: requested region exceeds payload region boundary")
	}
	return iw.data[start:end]
}

// getMetadataRegion returns a slice for the next block's metadata.
// Panics if the requested region exceeds the estimated file size.
func (iw *indexWriter) getMetadataRegion(maxSize int) []byte {
	end := iw.metadataWriteOffset + uint64(maxSize)
	if end > iw.estimatedSize {
		panic("getMetadataRegion: requested region exceeds estimated file size")
	}
	return iw.data[iw.metadataWriteOffset:end]
}

// commitBlock records that a block was written with given metadata size.
// This is used by single-threaded builds where payloads are written directly to mmap.
// It computes the payload hash from the mmap region and folds it into the streaming hasher.
func (iw *indexWriter) commitBlock(metadataLen, numKeys int) {
	// Record RAM index entry
	metadataRelOffset := iw.metadataWriteOffset - iw.metadataRegionOffset
	iw.ramIndex = append(iw.ramIndex, ramIndexEntry{
		KeysBefore:     iw.keyCount,
		MetadataOffset: metadataRelOffset,
	})

	// Update metadata hasher with the written metadata
	if metadataLen > 0 {
		if _, err := iw.metadataHasher.Write(iw.data[iw.metadataWriteOffset : iw.metadataWriteOffset+uint64(metadataLen)]); err != nil {
			panic("hash.Hash.Write returned unexpected error: " + err.Error())
		}
	}

	// Compute and fold payload hash for this block's payloads
	entrySize := iw.cfg.payloadSize + iw.cfg.fingerprintSize
	if entrySize > 0 && numKeys > 0 {
		payloadOffset := iw.payloadRegionOffset + iw.keyCount*uint64(entrySize)
		payloadLen := uint64(numKeys * entrySize)
		// Defensive bounds check
		if payloadOffset+payloadLen > iw.metadataRegionOffset {
			panic("commitBlock: payload region overflow")
		}
		blockPayloadHash := xxhash.Sum64(iw.data[payloadOffset : payloadOffset+payloadLen])
		iw.foldPayloadHash(blockPayloadHash)
	} else {
		// Empty block or MPHF-only: fold hash of empty slice (deterministic)
		iw.foldPayloadHash(xxhash.Sum64(nil))
	}

	iw.metadataWriteOffset += uint64(metadataLen)
	iw.keyCount += uint64(numKeys)
	iw.blockCount++
}

// estimateFileSizeForAlgo estimates file size based on algorithm.
func estimateFileSizeForAlgo(cfg *buildConfig, numBlocks uint32, algo blockBuilder, userMetadataLen, algoConfigLen int) uint64 {
	// Variable section sizes
	variableSectionSize := uint64(4 + userMetadataLen + 4 + algoConfigLen)
	ramIndexSize := uint64(numBlocks+1) * ramIndexEntrySize
	entrySize := cfg.payloadSize + cfg.fingerprintSize
	payloadRegionSize := cfg.totalKeys * uint64(entrySize)

	// Estimate metadata size using algorithm's worst-case max.
	// Add 16-byte safety margin per block for alignment/padding variance.
	metadataPerBlock := uint64(algo.MaxIndexMetadataSize()) + 16
	estimatedMetadataSize := uint64(numBlocks) * metadataPerBlock
	return headerSize + variableSectionSize + ramIndexSize + payloadRegionSize + estimatedMetadataSize + footerSize
}

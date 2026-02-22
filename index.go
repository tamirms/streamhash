package streamhash

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"sync/atomic"

	"github.com/cespare/xxhash/v2"
	"github.com/edsrzf/mmap-go"
	streamerrors "github.com/tamirms/streamhash/errors"
	intbits "github.com/tamirms/streamhash/internal/bits"
)

const (
	// minFileSize is a conservative lower bound for valid index files.
	// Derived from: headerSize(64) + variableSections(8 min) + ramIndex(30 min for 2 blocks + sentinel)
	// + footerSize(32) = 134 bytes minimum structure. 304 adds margin for metadata region.
	minFileSize = 304
)

// Index is a read-only StreamHash index for querying.
//
// Thread Safety:
// - Query, QueryPayload, and other read methods are safe for concurrent use
// - Close is NOT safe to call concurrently with queries
// - Close must only be called after all queries have completed
// - After Close returns, no methods may be called on the Index
type Index struct {
	// Memory map (no file handle needed after mmap)
	mmap mmap.MMap
	data []byte

	// Parsed header
	header *header

	// Variable-length data from file
	userMetadata []byte

	// RAM index for block lookup
	ramIndex []ramIndexEntry

	// Separated layout region offsets (computed from header)
	payloadRegionOffset  uint64
	metadataRegionOffset uint64

	// Computed values
	entrySize int // FingerprintSize + PayloadSize

	// Algorithm decoder (created at Open time, reused for all queries)
	decoder blockDecoder

	closed atomic.Bool // Atomic for lock-free close check
}

// Stats holds index statistics.
type Stats struct {
	NumKeys      uint64
	NumBlocks    uint32
	BitsPerKey   float64
	PayloadSize  int
	Fingerprints bool
	IndexSize    int64
}

// Open opens a StreamHash index file for querying.
// It opens the file, memory-maps it, and closes the file descriptor.
func Open(path string) (*Index, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open index file: %w", err)
	}
	defer file.Close()
	return OpenFile(file)
}

// OpenFile opens a StreamHash index by memory-mapping the given file.
// The caller is responsible for closing f. Per POSIX mmap(2), f may be
// closed immediately after OpenFile returns.
func OpenFile(f *os.File) (*Index, error) {
	stat, err := f.Stat()
	if err != nil {
		return nil, fmt.Errorf("stat index file: %w", err)
	}
	fileSize := stat.Size()

	if fileSize < int64(minFileSize) {
		return nil, streamerrors.ErrTruncatedFile
	}

	mm, err := mmap.Map(f, mmap.RDONLY, 0)
	if err != nil {
		return nil, fmt.Errorf("mmap index file: %w", err)
	}

	idx := &Index{
		mmap: mm,
		data: []byte(mm),
	}
	if err := idx.initFromData(); err != nil {
		return nil, errors.Join(err, idx.Close())
	}
	return idx, nil
}

// OpenBytes creates a StreamHash index from an in-memory byte slice.
// No file is opened or memory-mapped; Close is a no-op.
// The caller must ensure data is not modified while the Index is in use.
func OpenBytes(data []byte) (*Index, error) {
	if len(data) < minFileSize {
		return nil, streamerrors.ErrTruncatedFile
	}
	idx := &Index{
		data: data,
	}
	if err := idx.initFromData(); err != nil {
		return nil, err
	}
	return idx, nil
}

// initFromData parses header, variable sections, and RAM index from idx.data.
// Footer decoding is deferred to Verify() — Open() only touches the
// contiguous prefix (header + variable sections + RAM index).
func (idx *Index) initFromData() error {
	fileSize := uint64(len(idx.data))

	// Parse header
	hdr, err := decodeHeader(idx.data[:headerSize])
	if err != nil {
		return err
	}
	idx.header = hdr

	// Read variable-length sections after header
	// Layout: [Header 64B][UserMetaLen 4B][UserMeta][AlgoConfigLen 4B][AlgoConfig][RAM Index]...
	offset := uint64(headerSize)

	// Read userMetadata
	if offset+4 > fileSize {
		return streamerrors.ErrTruncatedFile
	}
	userMetadataLen := binary.LittleEndian.Uint32(idx.data[offset:])
	offset += 4
	if offset+uint64(userMetadataLen) > fileSize {
		return streamerrors.ErrTruncatedFile
	}
	idx.userMetadata = idx.data[offset : offset+uint64(userMetadataLen)]
	offset += uint64(userMetadataLen)

	// Read algoConfig
	if offset+4 > fileSize {
		return streamerrors.ErrTruncatedFile
	}
	algoConfigLen := binary.LittleEndian.Uint32(idx.data[offset:])
	offset += 4
	if offset+uint64(algoConfigLen) > fileSize {
		return streamerrors.ErrTruncatedFile
	}
	algoConfig := idx.data[offset : offset+uint64(algoConfigLen)]
	offset += uint64(algoConfigLen)

	// Calculate RAM index position (after variable sections)
	numRAMEntries := hdr.NumBlocks + 1 // includes sentinel
	ramIndexStart := offset
	ramIndexSize := uint64(numRAMEntries) * uint64(ramIndexEntrySize)
	ramIndexEnd := ramIndexStart + ramIndexSize

	// fileSize >= minFileSize > footerSize, so no underflow.
	if ramIndexEnd > fileSize-uint64(footerSize) {
		return streamerrors.ErrCorruptedIndex
	}

	// Load RAM index
	idx.ramIndex = make([]ramIndexEntry, numRAMEntries)
	for i := uint32(0); i < numRAMEntries; i++ {
		entryOffset := ramIndexStart + uint64(i)*uint64(ramIndexEntrySize)
		idx.ramIndex[i] = decodeRAMIndexEntry(idx.data[entryOffset : entryOffset+uint64(ramIndexEntrySize)])
	}

	idx.entrySize = hdr.entrySize()

	// Compute separated layout region offsets
	idx.payloadRegionOffset = ramIndexEnd
	payloadRegionSize := hdr.TotalKeys * uint64(idx.entrySize)
	idx.metadataRegionOffset = idx.payloadRegionOffset + payloadRegionSize

	// Create algorithm decoder for query-time slot computation
	idx.decoder, err = newBlockDecoder(hdr.BlockAlgorithm, algoConfig, hdr.Seed)
	if err != nil {
		return err
	}

	return nil
}

// Close closes the index and releases resources.
func (idx *Index) Close() error {
	if idx.closed.Swap(true) {
		return nil // Already closed
	}

	if idx.mmap != nil {
		return idx.mmap.Unmap()
	}
	return nil
}

// Query returns the rank (0-based index) for a key.
// This is the core MPHF operation.
// Returns streamerrors.ErrKeyTooShort if key is less than 16 bytes.
func (idx *Index) Query(key []byte) (uint64, error) {
	if idx.closed.Load() {
		return 0, streamerrors.ErrIndexClosed
	}

	// Validate key length (must be at least 16 bytes for routing)
	if len(key) < minKeySize {
		return 0, streamerrors.ErrKeyTooShort
	}

	return idx.queryInternal(key)
}

// verifyFingerprintSeparated checks if the fingerprint matches.
// Reads from the separated payload region.
// Uses hybrid extraction: keys > 16 bytes use trailing bytes, 16-byte keys use unified mixer.
// Preconditions:
//   - idx.header.hasFingerprint() (callers must check)
//   - len(key) >= 16 (guaranteed by Query)
func (idx *Index) verifyFingerprintSeparated(key []byte, k0, k1 uint64, globalRank uint64) (bool, error) {
	fpSize := idx.header.fingerprintSizeInt()

	// Get stored fingerprint from payload region
	payloadOffset := idx.payloadRegionOffset + globalRank*uint64(idx.entrySize)

	if payloadOffset+uint64(fpSize) > uint64(len(idx.data)) {
		return false, streamerrors.ErrCorruptedIndex
	}

	storedFP := unpackFingerprintFromBytes(idx.data[payloadOffset:], fpSize)

	// Hybrid fingerprint extraction:
	// - Keys with enough trailing bytes (len >= 16 + fpSize): use key end
	// - Keys without enough trailing bytes: use unified mixer
	var expectedFP uint32
	extraBytes := len(key) - 16
	if extraBytes >= fpSize {
		// Use trailing bytes beyond k0/k1 - completely independent
		expectedFP = unpackFingerprintFromBytes(key[len(key)-fpSize:], fpSize)
	} else {
		// Use unified mixer for 16-byte keys (or insufficient trailing bytes)
		expectedFP = extractFingerprint(k0, k1, fpSize)
	}

	return storedFP == expectedFP, nil
}

// queryInternal is the internal query implementation.
// Uses the algorithm-specific decoder to compute slots.
func (idx *Index) queryInternal(key []byte) (uint64, error) {
	// Step 1: Parse key and route to block
	// k0, k1 are little-endian for hash computation
	// prefix is big-endian for monotonic block routing
	k0 := binary.LittleEndian.Uint64(key[0:8])
	k1 := binary.LittleEndian.Uint64(key[8:16])
	prefix := binary.BigEndian.Uint64(key[0:8])
	blockIdx := intbits.FastRange32(prefix, idx.header.NumBlocks)

	if int(blockIdx) >= len(idx.ramIndex)-1 {
		return 0, streamerrors.ErrNotFound
	}

	// Step 2: Get block info from RAM index
	entry := idx.ramIndex[blockIdx]
	nextEntry := idx.ramIndex[blockIdx+1]
	baseRank := entry.KeysBefore
	keysInBlock := int(nextEntry.KeysBefore - entry.KeysBefore)

	if keysInBlock == 0 {
		return 0, streamerrors.ErrNotFound
	}

	// Step 3: Get metadata for block
	metaStart := idx.metadataRegionOffset + entry.MetadataOffset
	metaEnd := idx.metadataRegionOffset + nextEntry.MetadataOffset

	if metaStart >= metaEnd || metaEnd > uint64(len(idx.data)) {
		return 0, streamerrors.ErrCorruptedIndex
	}

	metadataData := idx.data[metaStart:metaEnd]

	// Step 4: Use decoder to compute local slot
	localSlot, err := idx.decoder.QuerySlot(k0, k1, metadataData, keysInBlock)
	if err != nil {
		return 0, err
	}

	// Step 5: Compute global rank and verify fingerprint
	globalRank := baseRank + uint64(localSlot)

	if idx.header.hasFingerprint() {
		ok, err := idx.verifyFingerprintSeparated(key, k0, k1, globalRank)
		if err != nil {
			return 0, err
		}
		if !ok {
			return 0, streamerrors.ErrFingerprintMismatch
		}
	}

	return globalRank, nil
}

// QueryPayload returns the payload for a key as a uint64.
// Payloads are stored in little-endian format and can be 1-8 bytes.
// Returns streamerrors.ErrNoPayload if the index has no payload data.
// Returns streamerrors.ErrKeyTooShort if key is less than 16 bytes.
func (idx *Index) QueryPayload(key []byte) (uint64, error) {
	if idx.closed.Load() {
		return 0, streamerrors.ErrIndexClosed
	}

	if idx.header.PayloadSize == 0 {
		return 0, streamerrors.ErrNoPayload
	}

	// Validate key length (must be at least 16 bytes for routing)
	if len(key) < minKeySize {
		return 0, streamerrors.ErrKeyTooShort
	}

	// Get the rank first (includes fingerprint verification if present)
	rank, err := idx.queryInternal(key)
	if err != nil {
		return 0, err
	}

	// Read payload from payload region
	fpSize := idx.header.fingerprintSizeInt()
	payloadSize := idx.header.payloadSizeInt()

	payloadOffset := idx.payloadRegionOffset + rank*uint64(idx.entrySize) + uint64(fpSize)

	if payloadOffset+uint64(payloadSize) > uint64(len(idx.data)) {
		return 0, streamerrors.ErrCorruptedIndex
	}

	return unpackPayloadFromBytes(idx.data[payloadOffset:], payloadSize), nil
}

// NumKeys returns the total number of keys in the index.
func (idx *Index) NumKeys() uint64 {
	return idx.header.TotalKeys
}

// NumBlocks returns the number of blocks in the index.
func (idx *Index) NumBlocks() uint32 {
	return idx.header.NumBlocks
}

// HasPayload returns whether the index stores payloads.
func (idx *Index) HasPayload() bool {
	return idx.header.hasPayload()
}

// PayloadSize returns the payload size per key.
func (idx *Index) PayloadSize() int {
	return idx.header.payloadSizeInt()
}

// UserMetadata returns the variable-length user-defined metadata.
// The returned slice is backed by the memory-mapped file data.
func (idx *Index) UserMetadata() []byte {
	return idx.userMetadata
}

// GetStats returns statistics for an index file.
func GetStats(path string) (*Stats, error) {
	idx, err := Open(path)
	if err != nil {
		return nil, err
	}

	return idx.Stats(), idx.Close()
}

// Stats returns statistics for the index.
func (idx *Index) Stats() *Stats {
	totalSize := int64(len(idx.data))

	bitsPerKey := float64(0)
	if idx.header.TotalKeys > 0 {
		bitsPerKey = float64(totalSize*8) / float64(idx.header.TotalKeys)
	}

	return &Stats{
		NumKeys:      idx.header.TotalKeys,
		NumBlocks:    idx.header.NumBlocks,
		BitsPerKey:   bitsPerKey,
		PayloadSize:  idx.header.payloadSizeInt(),
		Fingerprints: idx.header.hasFingerprint(),
		IndexSize:    totalSize,
	}
}

// Verify checks the integrity of the entire index.
// For separated layout, this verifies:
// 1. PayloadRegionHash (hash-of-hashes: H(H(b0) || H(b1) || ...))
// 2. MetadataRegionHash (streaming hash of metadata region)
//
// The footer (last 32 bytes) is decoded on each Verify call rather than at Open time,
// so Open() only touches the contiguous prefix and avoids a scattered page fault.
//
// The hash-of-hashes approach matches the streaming hash computation during build,
// where workers compute per-block payload hashes that are folded in order.
func (idx *Index) Verify() error {
	if idx.closed.Load() {
		return streamerrors.ErrIndexClosed
	}

	// Lazy footer decode — only touched by Verify, not Open.
	fileSize := uint64(len(idx.data))
	ft, err := decodeFooter(idx.data[fileSize-footerSize:])
	if err != nil {
		return err
	}

	numBlocks := int(idx.header.NumBlocks)

	// Verify payload region hash using hash-of-hashes
	// This matches the build-time computation: fold per-block hashes in order
	payloadHasher := xxhash.New()
	for blockID := 0; blockID < numBlocks; blockID++ {
		// Get this block's payload range from RAM index
		startKey := idx.ramIndex[blockID].KeysBefore
		endKey := idx.ramIndex[blockID+1].KeysBefore
		if startKey > endKey {
			return streamerrors.ErrCorruptedIndex
		}
		numKeys := endKey - startKey

		// Compute hash of this block's payloads
		var blockHash uint64
		if numKeys > 0 && idx.entrySize > 0 {
			payloadStart := idx.payloadRegionOffset + startKey*uint64(idx.entrySize)
			payloadEnd := idx.payloadRegionOffset + endKey*uint64(idx.entrySize)
			if payloadEnd > uint64(len(idx.data)) {
				return streamerrors.ErrCorruptedIndex
			}
			blockPayloads := idx.data[payloadStart:payloadEnd]
			blockHash = xxhash.Sum64(blockPayloads)
		} else {
			// Empty block or MPHF-only: hash of empty slice
			blockHash = xxhash.Sum64(nil)
		}

		// Fold block hash into streaming hasher (little-endian)
		var buf [8]byte
		binary.LittleEndian.PutUint64(buf[:], blockHash)
		if _, err := payloadHasher.Write(buf[:]); err != nil {
			panic("hash.Hash.Write returned unexpected error: " + err.Error())
		}
	}

	if payloadHasher.Sum64() != ft.PayloadRegionHash {
		return streamerrors.ErrChecksumFailed
	}

	// Verify metadata region hash (streaming hash of entire region)
	footerOffset := fileSize - footerSize
	if footerOffset < idx.metadataRegionOffset {
		return streamerrors.ErrCorruptedIndex
	}
	metadataRegionSize := footerOffset - idx.metadataRegionOffset
	if metadataRegionSize > 0 {
		metadataRegion := idx.data[idx.metadataRegionOffset:footerOffset]
		actualMetaHash := xxhash.Sum64(metadataRegion)
		if actualMetaHash != ft.MetadataRegionHash {
			return streamerrors.ErrChecksumFailed
		}
	}

	return nil
}

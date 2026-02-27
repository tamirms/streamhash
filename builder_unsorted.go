package streamhash

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"math/bits"
	"os"
	"sync"
	"syscall"

	intbits "github.com/tamirms/streamhash/internal/bits"
)

const (
	// maxFlushBufferBytes caps the flush buffer at 4MB. Smaller buffers have
	// better cache behavior for addKey writes (~75MB buffer at 64MB exceeds L3,
	// causing cache misses). Double-buffering fully hides flush I/O regardless
	// of buffer size. Profiling shows 4MB is the sweet spot: fits in L3,
	// ~100ms faster than 64MB at 50M keys, and ~180MB less peak heap.
	maxFlushBufferBytes = 4 << 20

	// defaultUnsortedMemoryBudget is the default peak RAM budget for unsorted
	// builds. Handles up to ~20B keys with P <= 5000.
	defaultUnsortedMemoryBudget int64 = 256 << 20

	// readBufSize is the streaming read buffer size for partition files.
	// 4MB provides good throughput without excess memory (benchmark #12).
	readBufSize = 4 << 20

	// bufferedEntrySize is the in-memory size of a bufferedEntry struct.
	bufferedEntrySize = 24 // 3×uint64 (k0, k1, payload)

	// maxPartitions limits the number of partition files to prevent fd exhaustion.
	maxPartitions = 5000

	// superblockSize is the number of blocks per superblock for readback grouping.
	// S=500 is optimal for high block counts (50K-100K blocks/partition):
	// cache-friendly per-SB sort with counts/offsets fitting in L1 cache (4KB each).
	superblockSize = 500

	// superblockMarginSigmas is the margin for Poisson overflow protection.
	// 6-sigma gives P(overflow per SB) ~ exp(-18) ~ 1.5e-8 for large lambda.
	superblockMarginSigmas = 6.0

	// readCostPerEntry is the per-entry memory cost during the read phase.
	// Derivation: routedEntry(24B) + blockID(4B) = 28B per entry, times 2
	// for pipelining = 56B, times 1.03 superblock margin ≈ 58B.
	readCostPerEntry = 58

	// fdReserve is the number of file descriptors reserved for non-partition use
	// (stdin/stdout/stderr, the output file, Go runtime internals, etc.).
	fdReserve = 100

	// blockContextCheckInterval is how often to check for context cancellation
	// during block-level iteration in Finish.
	blockContextCheckInterval = 64
)

// bufferedEntry is the in-memory representation during the write phase.
// blockID is computed transiently for partition routing, not stored.
// Fingerprint is computed from k0/k1 at index-write time.
type bufferedEntry struct {
	k0, k1  uint64
	payload uint64
}

// unsortedBuffer implements partition flush for unsorted mode.
//
// Write phase: entries are appended directly to per-partition buffers
// (no sorting needed), then written sequentially to P partition files.
// Partition files are opened at build start and immediately unlinked
// (crash-safe: kernel frees data on process exit).
//
// Read phase: each partition file is streamed, entries scattered to
// superblock regions in a flat buffer, then in-place per-superblock
// counting sort groups entries by blockID.
//
// Total I/O: 2N (all sequential). Scales to hundreds of billions of keys.
type unsortedBuffer struct {
	cfg       *buildConfig
	numBlocks uint32
	entrySize int // on-disk: minKeySize + payloadSize (no fingerprint)

	// Partition layout
	partDir          string     // temp directory (empty after files unlinked)
	partFiles        []*os.File // persistent fds (nil'd after read+close)
	numPartitions    int
	blocksPerPart    int
	blocksPerPartU32 uint32 // same as blocksPerPart, stored as uint32 for fast 32-bit division in addKey

	// Write-phase double buffer (nil'd after prepareForRead).
	// Two flat arrays, each divided into P regions. addKey writes to the
	// active buffer; flush swaps buffers and writes the previous one in
	// the background, overlapping I/O with the next batch of addKey calls.
	flatBufs      [2][]bufferedEntry // two flat buffers for double-buffering
	cursorSets    [2][]int           // per-partition cursors for each buffer
	activeBuf     int                // 0 or 1: which buffer addKey writes to
	regionCap     int                // entries per partition region (with headroom for hash variance)
	totalBuffered int                // total entries in the active buffer
	bufferCap     int                // flush threshold (= target entries before flush)
	encodeBuf     []byte             // reused encode buffer (only used by flush goroutine)
	flushWg       sync.WaitGroup     // waits for background flush to complete
	flushErr      error              // error from background flush (read after Wait)

	// Read-phase pools (allocated in prepareForRead, reused across partitions)
	readFlatBufs        [2][]routedEntry   // totalRegion each, alternated for pipelining
	readBlockIDs        [2][]uint32        // totalRegion each, one per pipeline flatBuf
	readScratch         []routedEntry      // max SB region size
	readScratchIDs      []uint32           // max SB region size
	readBlockEntries    [2][][]routedEntry // blocksPerPart each, one per pipeline flatBuf
	readSBCursors       []int              // numSB
	readSBCounts        []int              // S
	readSBOffsets       []int              // S
	readBuf             []byte             // readBufSize, reused across partitions
	readSBRegionOffsets []int              // max numSB+1, reused across partitions

	flushed bool // true after first flush to disk
}

// newUnsortedBuffer creates an unsortedBuffer with partition flush.
func newUnsortedBuffer(cfg *buildConfig, numBlocks uint32) (*unsortedBuffer, error) {
	entrySize := minKeySize + cfg.payloadSize

	memoryBudget := defaultUnsortedMemoryBudget

	// Derive partition file target size from memory budget and read-phase cost.
	partFileTarget := max(int64(memoryBudget)*int64(entrySize)/readCostPerEntry, 1)

	totalDataSize := int64(cfg.totalKeys) * int64(entrySize)
	numPartitions := max(int(math.Ceil(float64(totalDataSize)/float64(partFileTarget))), 1)

	// Cap at fd limit
	maxP := maxPartitions
	var rlim syscall.Rlimit
	if err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &rlim); err == nil {
		fdLimit := int(rlim.Cur) - fdReserve
		if fdLimit > 0 && fdLimit < maxP {
			maxP = fdLimit
		}
	}
	if numPartitions > maxP {
		numPartitions = maxP
	}
	// Cannot have more partitions than blocks (each partition needs >= 1 block)
	if numPartitions > int(numBlocks) {
		numPartitions = int(numBlocks)
	}

	blocksPerPart := max(int(math.Ceil(float64(numBlocks)/float64(numPartitions))), 1)

	// Derive flush buffer size
	flushBufferBytes := max(min(memoryBudget/4, maxFlushBufferBytes), int64(bufferedEntrySize))
	bufferCap := int(flushBufferBytes / bufferedEntrySize)

	// Each partition region gets bufferCap/P + 12.5% headroom for hash variance.
	regionCap := max(bufferCap/numPartitions+bufferCap/(numPartitions*8), 1)

	flatSize := regionCap * numPartitions
	u := &unsortedBuffer{
		cfg:              cfg,
		numBlocks:        numBlocks,
		entrySize:        entrySize,
		numPartitions:    numPartitions,
		blocksPerPart:    blocksPerPart,
		blocksPerPartU32: uint32(blocksPerPart),
		bufferCap:        bufferCap,
		regionCap:        regionCap,
		flatBufs: [2][]bufferedEntry{
			make([]bufferedEntry, flatSize),
			make([]bufferedEntry, flatSize),
		},
		cursorSets: [2][]int{
			make([]int, numPartitions),
			make([]int, numPartitions),
		},
	}

	// Create partition directory eagerly to validate the temp dir early.
	if err := u.createPartDir(cfg.unsortedTempDir); err != nil {
		return nil, fmt.Errorf("create partition directory: %w", err)
	}

	return u, nil
}

// createPartDir creates the temp directory and opens persistent partition file fds.
// Files are immediately unlinked (crash-safe: kernel frees data on last fd close).
func (u *unsortedBuffer) createPartDir(tempDir string) error {
	if tempDir == "" {
		tempDir = os.TempDir()
	}
	dir, err := os.MkdirTemp(tempDir, "streamhash-parts-*")
	if err != nil {
		return err
	}
	u.partDir = dir

	// Open all partition files and immediately unlink them.
	u.partFiles = make([]*os.File, u.numPartitions)
	for i := range u.partFiles {
		path := fmt.Sprintf("%s/part-%05d", dir, i)
		f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0600)
		if err != nil {
			u.closePartFiles(i)
			os.RemoveAll(dir)
			u.partDir = ""
			return fmt.Errorf("create partition file %d: %w", i, err)
		}
		// Unlink immediately — data persists via fd. Near-crash-safe:
		// non-zero window between OpenFile and Remove (microseconds).
		if err := os.Remove(path); err != nil {
			f.Close()
			u.closePartFiles(i)
			os.RemoveAll(dir)
			u.partDir = ""
			return fmt.Errorf("unlink partition file %d: %w", i, err)
		}
		u.partFiles[i] = f
	}
	return nil
}

// closePartFiles closes partition fds [0, n).
func (u *unsortedBuffer) closePartFiles(n int) {
	for _, f := range u.partFiles[:n] {
		if f != nil {
			f.Close()
		}
	}
}

// addKey writes an entry directly to the active buffer's partition region.
// Returns true if the buffer is full and needs flushing. The caller must
// call flush() when addKey returns true.
func (u *unsortedBuffer) addKey(k0, k1 uint64, payload uint64, blockID uint32) bool {
	// partitionID inlined. Uses uint32 division (DIVL) — 2-3x faster than int division.
	p := int(blockID / u.blocksPerPartU32)
	a := u.activeBuf
	c := u.cursorSets[a][p]
	u.flatBufs[a][p*u.regionCap+c] = bufferedEntry{k0: k0, k1: k1, payload: payload}
	u.cursorSets[a][p] = c + 1
	u.totalBuffered++
	return u.totalBuffered >= u.bufferCap || c+1 >= u.regionCap
}


// flush swaps the active buffer and launches background I/O for the filled buffer.
func (u *unsortedBuffer) flush() error {
	if u.totalBuffered == 0 {
		return nil
	}

	// Wait for any previous background flush to complete.
	u.flushWg.Wait()
	if u.flushErr != nil {
		return u.flushErr
	}

	u.flushed = true

	flushBuf := u.activeBuf
	u.activeBuf = 1 - flushBuf
	clear(u.cursorSets[u.activeBuf])
	u.totalBuffered = 0

	u.flushWg.Go(func() {
		u.flushErr = u.doFlush(flushBuf)
	})

	return nil
}

// doFlush encodes and writes entries from the specified buffer to partition files.
// Writes to persistent fds (file offset advances automatically via f.Write).
func (u *unsortedBuffer) doFlush(bufIdx int) error {
	cursors := u.cursorSets[bufIdx]
	buf := u.flatBufs[bufIdx]

	// Find max partition entry count for encode buffer sizing
	maxCount := 0
	for p := range u.numPartitions {
		n := cursors[p]
		if n > maxCount {
			maxCount = n
		}
	}

	encodeBufNeeded := maxCount * u.entrySize
	if cap(u.encodeBuf) < encodeBufNeeded {
		u.encodeBuf = make([]byte, encodeBufNeeded)
	}

	for p := range u.numPartitions {
		n := cursors[p]
		if n == 0 {
			continue
		}

		base := p * u.regionCap
		encLen := u.encodeEntries(buf[base : base+n])

		// Write to persistent fd (offset advances automatically)
		f := u.partFiles[p]
		if _, err := f.Write(u.encodeBuf[:encLen]); err != nil {
			return fmt.Errorf("write partition %d: %w", p, err)
		}
	}

	return nil
}

// encodeEntries encodes a slice of bufferedEntries into u.encodeBuf.
// On-disk format per entry: k0(8) + k1(8) + payload(payloadSize).
// No fingerprint on disk (computed from k0/k1 at index-write time).
func (u *unsortedBuffer) encodeEntries(entries []bufferedEntry) int {
	buf := u.encodeBuf
	pos := 0
	payloadSize := u.cfg.payloadSize

	for i := range entries {
		e := &entries[i]
		binary.LittleEndian.PutUint64(buf[pos:], e.k0)
		binary.LittleEndian.PutUint64(buf[pos+8:], e.k1)
		if payloadSize > 0 {
			packPayloadToBytes(buf[pos+minKeySize:], e.payload, payloadSize)
		}
		pos += u.entrySize
	}
	return pos
}

// computeSBRegions computes the total region size and max individual SB region
// for a given entry count and block count, using superblock margin allocation.
// If offsets is non-nil (len >= numSB+1), it is filled with cumulative region offsets.
func computeSBRegions(totalEntries, localBlocks, S int, offsets []int) (totalRegion, maxSBRegion int) {
	numSB := (localBlocks + S - 1) / S
	if len(offsets) > 0 {
		offsets[0] = 0
	}
	for sb := range numSB {
		blocksInSB := min(S, localBlocks-sb*S)
		expected := (totalEntries*blocksInSB + localBlocks - 1) / localBlocks
		margin := int(superblockMarginSigmas * math.Sqrt(float64(expected)))
		region := expected + margin
		totalRegion += region
		if region > maxSBRegion {
			maxSBRegion = region
		}
		if len(offsets) > sb+1 {
			offsets[sb+1] = totalRegion
		}
	}
	return
}

// sortSuperblock performs in-place counting sort for a single superblock.
// Each SB is independent, enabling parallel sorting across goroutines.
func sortSuperblock(sb, S, localBlocks int, sbRegionOffsets, sbCursors []int,
	flatBuf []routedEntry, blockIDs []uint32, blockEntries [][]routedEntry,
	scratch []routedEntry, scratchIDs []uint32, sbCounts, sbOffsets []int) {

	sbStart := sbRegionOffsets[sb]
	sbN := sbCursors[sb]
	blocksInSB := min(S, localBlocks-sb*S)

	// Copy to scratch
	copy(scratch[:sbN], flatBuf[sbStart:sbStart+sbN])
	copy(scratchIDs[:sbN], blockIDs[sbStart:sbStart+sbN])

	// Count by blockID within SB
	clear(sbCounts[:blocksInSB])
	for i := range sbN {
		sbCounts[scratchIDs[i]-uint32(sb*S)]++
	}

	// Prefix sum -> offsets within flatBuf
	sbOffsets[0] = sbStart
	for j := 1; j < blocksInSB; j++ {
		sbOffsets[j] = sbOffsets[j-1] + sbCounts[j-1]
	}

	// Build result slices BEFORE scatter (offsets are correct positions)
	for j := range blocksInSB {
		blockEntries[sb*S+j] = flatBuf[sbOffsets[j] : sbOffsets[j]+sbCounts[j]]
	}

	// Scatter from scratch -> flatBuf (sorted by block within SB)
	for i := range sbN {
		localInSB := scratchIDs[i] - uint32(sb*S)
		flatBuf[sbOffsets[localInSB]] = scratch[i]
		sbOffsets[localInSB]++
	}
}

// readPartition reads a partition file and returns entries grouped by block.
// Uses superblock scatter + in-place per-SB counting sort.
// The pipelineIdx (0 or 1) selects which pipeline buffer set to use.
func (u *unsortedBuffer) readPartition(p, pipelineIdx int, flatBuf []routedEntry, blockIDs []uint32) ([][]routedEntry, error) {
	partStartBlock := p * u.blocksPerPart
	partEndBlock := min(partStartBlock+u.blocksPerPart, int(u.numBlocks))
	localBlocks := partEndBlock - partStartBlock
	if localBlocks <= 0 {
		return nil, nil
	}

	f := u.partFiles[p]
	if f == nil {
		return make([][]routedEntry, localBlocks), nil
	}

	fi, err := f.Stat()
	if err != nil {
		return nil, fmt.Errorf("stat partition %d: %w", p, err)
	}
	fileSize := fi.Size()
	if fileSize == 0 {
		f.Close()
		u.partFiles[p] = nil
		return make([][]routedEntry, localBlocks), nil
	}

	totalEntries := int(fileSize) / u.entrySize
	if int64(totalEntries)*int64(u.entrySize) != fileSize {
		return nil, fmt.Errorf("partition %d: file size %d not a multiple of entry size %d", p, fileSize, u.entrySize)
	}

	// Seek to beginning for reading
	if _, err := f.Seek(0, 0); err != nil {
		return nil, fmt.Errorf("seek partition %d: %w", p, err)
	}

	// Per-SB proportional region allocation
	S := superblockSize
	numSB := (localBlocks + S - 1) / S
	sbRegionOffsets := u.readSBRegionOffsets[:numSB+1]
	totalRegion, _ := computeSBRegions(totalEntries, localBlocks, S, sbRegionOffsets)

	if len(flatBuf) < totalRegion || len(blockIDs) < totalRegion {
		return nil, fmt.Errorf("flatBuf/blockIDs too small: have %d/%d, need %d", len(flatBuf), len(blockIDs), totalRegion)
	}

	sbCursors := u.readSBCursors[:numSB]
	clear(sbCursors)

	// Single pass: decode file -> scatter to SB regions in flatBuf.
	// Leftover bytes from partial entries are kept at the start of readBuf
	// to avoid per-read allocations from append.
	readBuf := u.readBuf
	payloadSize := u.cfg.payloadSize
	entrySize := u.entrySize
	leftoverN := 0 // number of leftover bytes at start of readBuf

	for {
		nr, readErr := f.Read(readBuf[leftoverN:])
		if nr == 0 && readErr != nil {
			if readErr == io.EOF {
				break
			}
			return nil, fmt.Errorf("read partition %d: %w", p, readErr)
		}

		data := readBuf[:leftoverN+nr]
		leftoverN = 0

		for len(data) >= entrySize {
			k0 := binary.LittleEndian.Uint64(data[0:8])
			k1 := binary.LittleEndian.Uint64(data[8:16])
			var payload uint64
			if payloadSize > 0 {
				payload = unpackPayloadFromBytes(data[minKeySize:], payloadSize)
			}

			prefix := bits.ReverseBytes64(k0)
			globalBlockID := int(intbits.FastRange32(prefix, u.numBlocks))

			if globalBlockID < partStartBlock || globalBlockID >= partEndBlock {
				return nil, fmt.Errorf("partition %d: blockID %d outside range [%d, %d)",
					p, globalBlockID, partStartBlock, partEndBlock)
			}

			localIdx := globalBlockID - partStartBlock
			sb := localIdx / S

			cursor := sbCursors[sb]
			regionCap := sbRegionOffsets[sb+1] - sbRegionOffsets[sb]
			if cursor >= regionCap {
				return nil, fmt.Errorf("superblock %d overflow: non-uniform input", sb)
			}

			pos := sbRegionOffsets[sb] + cursor
			flatBuf[pos] = routedEntry{k0: k0, k1: k1, payload: payload}
			blockIDs[pos] = uint32(localIdx)
			sbCursors[sb] = cursor + 1

			data = data[entrySize:]
		}

		if len(data) > 0 {
			leftoverN = copy(readBuf, data)
		}

		if readErr != nil {
			if readErr != io.EOF {
				return nil, fmt.Errorf("read partition %d: %w", p, readErr)
			}
			break
		}
	}

	// In-place per-SB counting sort using scratch buffer
	scratch := u.readScratch
	scratchIDs := u.readScratchIDs
	sbCounts := u.readSBCounts[:S]
	sbOffsets := u.readSBOffsets[:S]
	blockEntries := u.readBlockEntries[pipelineIdx][:localBlocks]

	for sb := range numSB {
		sortSuperblock(sb, S, localBlocks, sbRegionOffsets, sbCursors,
			flatBuf, blockIDs, blockEntries, scratch, scratchIDs, sbCounts, sbOffsets)
	}

	// Close fd (file already unlinked, kernel frees pages on last fd close).
	f.Close()
	u.partFiles[p] = nil

	return blockEntries, nil
}

// maxPartitionEntries returns the maximum entry count across all partitions,
// computed by statting the persistent fds.
func (u *unsortedBuffer) maxPartitionEntries() (int, error) {
	maxEntries := 0
	for _, f := range u.partFiles {
		if f == nil {
			continue
		}
		fi, err := f.Stat()
		if err != nil {
			return 0, fmt.Errorf("stat partition file: %w", err)
		}
		n := int(fi.Size()) / u.entrySize
		if n > maxEntries {
			maxEntries = n
		}
	}
	return maxEntries, nil
}

// prepareForRead flushes remaining entries, nils write-phase buffers,
// and pre-allocates read-phase pools.
func (u *unsortedBuffer) prepareForRead() error {
	if u.totalBuffered > 0 {
		if err := u.flush(); err != nil {
			return err
		}
	}
	u.flushWg.Wait()
	if u.flushErr != nil {
		return u.flushErr
	}
	// Free write-phase allocations
	u.flatBufs = [2][]bufferedEntry{}
	u.cursorSets = [2][]int{}
	u.encodeBuf = nil

	// Pre-allocate read-phase pools
	maxEntries, err := u.maxPartitionEntries()
	if err != nil {
		return err
	}
	if maxEntries == 0 {
		return nil
	}

	// Compute totalRegion and maxSBRegion as max over both blocksPerPart
	// and the last partition's actual block count.
	totalRegion1, maxSBRegion1 := computeSBRegions(maxEntries, u.blocksPerPart, superblockSize, nil)
	lastPartBlocks := int(u.numBlocks) - (u.numPartitions-1)*u.blocksPerPart
	totalRegion2, maxSBRegion2 := computeSBRegions(maxEntries, lastPartBlocks, superblockSize, nil)
	totalRegion := max(totalRegion1, totalRegion2)
	maxSBRegion := max(maxSBRegion1, maxSBRegion2)

	u.readFlatBufs = [2][]routedEntry{
		make([]routedEntry, totalRegion),
		make([]routedEntry, totalRegion),
	}
	u.readBlockIDs = [2][]uint32{
		make([]uint32, totalRegion),
		make([]uint32, totalRegion),
	}
	u.readScratch = make([]routedEntry, maxSBRegion)
	u.readScratchIDs = make([]uint32, maxSBRegion)

	maxBlocksPerPart := u.blocksPerPart
	u.readBlockEntries = [2][][]routedEntry{
		make([][]routedEntry, maxBlocksPerPart),
		make([][]routedEntry, maxBlocksPerPart),
	}

	numSB := (u.blocksPerPart + superblockSize - 1) / superblockSize
	u.readSBCursors = make([]int, numSB)
	u.readSBCounts = make([]int, superblockSize)
	u.readSBOffsets = make([]int, superblockSize)
	u.readBuf = make([]byte, readBufSize)
	u.readSBRegionOffsets = make([]int, numSB+1)

	return nil
}

// cleanup closes all remaining partition fds and removes temp directory. Idempotent.
func (u *unsortedBuffer) cleanup() error {
	// Wait for any background flush to complete before closing fds
	u.flushWg.Wait()

	// Close any remaining non-nil fds
	for i, f := range u.partFiles {
		if f != nil {
			f.Close()
			u.partFiles[i] = nil
		}
	}

	// Remove temp directory (now empty since files were unlinked)
	if u.partDir != "" {
		err := os.RemoveAll(u.partDir)
		u.partDir = ""
		u.partFiles = nil
		u.flatBufs = [2][]bufferedEntry{}
		u.cursorSets = [2][]int{}
		u.encodeBuf = nil
		if err != nil {
			return fmt.Errorf("remove partition directory: %w", err)
		}
	}
	return nil
}

// finishUnsorted builds blocks from partition files.
// Correctness is validated by TestBuildModeEquivalence.
func (b *Builder) finishUnsorted() error {
	u := b.unsortedBuf

	// Fast path: if all entries are still in buffer (never flushed to disk),
	// sort in-memory and build blocks directly without file I/O.
	if !u.flushed {
		return b.finishUnsortedFastPath()
	}

	// Normal path: flush remaining entries, free write-phase buffers.
	if err := u.prepareForRead(); err != nil {
		return errors.Join(err, u.cleanup(), b.cleanup())
	}

	if b.workers > 1 {
		return b.finishUnsortedParallel()
	}
	return b.finishUnsortedSingleThreaded()
}

// finishUnsortedFastPath handles small datasets that fit entirely in memory.
// The buffer was never flushed to disk, so we sort by blockID and build directly.
func (b *Builder) finishUnsortedFastPath() error {
	u := b.unsortedBuf

	// Collect all entries from the active buffer's partition regions
	// and compute blockIDs.
	a := u.activeBuf
	entries := make([]routedEntry, 0, u.totalBuffered)
	entryBlockIDs := make([]uint32, 0, u.totalBuffered)
	for p := range u.numPartitions {
		n := u.cursorSets[a][p]
		if n > 0 {
			base := p * u.regionCap
			for i := range n {
				e := u.flatBufs[a][base+i]
				entries = append(entries, routedEntry{k0: e.k0, k1: e.k1, payload: e.payload})
				prefix := bits.ReverseBytes64(e.k0)
				entryBlockIDs = append(entryBlockIDs, intbits.FastRange32(prefix, u.numBlocks))
			}
		}
	}

	// Check context before doing any work
	select {
	case <-b.ctx.Done():
		return errors.Join(b.ctx.Err(), u.cleanup(), b.cleanup())
	default:
	}

	// Counting sort by blockID
	numBlocks := int(u.numBlocks)
	counts := make([]int, numBlocks)
	for _, bid := range entryBlockIDs {
		counts[bid]++
	}
	offsets := make([]int, numBlocks)
	for i := 1; i < numBlocks; i++ {
		offsets[i] = offsets[i-1] + counts[i-1]
	}
	sorted := make([]routedEntry, len(entries))
	cursors := make([]int, numBlocks)
	copy(cursors, offsets)
	for i, e := range entries {
		bid := entryBlockIDs[i]
		sorted[cursors[bid]] = e
		cursors[bid]++
	}

	if b.workers > 1 {
		b.initParallelWorkers()
	}

	fpSize := b.cfg.fingerprintSize
	for blockID := uint32(0); blockID < u.numBlocks; blockID++ {
		// Periodic context check
		if blockID%blockContextCheckInterval == 0 {
			select {
			case <-b.ctx.Done():
				return errors.Join(b.ctx.Err(), u.cleanup(), b.cleanup())
			default:
			}
		}

		start := offsets[blockID]
		n := counts[blockID]

		if n == 0 {
			if b.workers > 1 {
				if err := b.dispatchEmptyBlock(blockID); err != nil {
					return errors.Join(err, u.cleanup(), b.cleanup())
				}
			} else {
				b.commitEmptyBlock()
			}
			continue
		}

		blockEntries := sorted[start : start+n]
		if b.workers > 1 {
			// Fresh allocation, not from pool or flatBuf
			dst := make([]routedEntry, n)
			copy(dst, blockEntries)
			if err := b.dispatchBlockWork(blockID, dst, false); err != nil {
				return errors.Join(err, u.cleanup(), b.cleanup())
			}
		} else {
			b.builder.Reset()
			for _, e := range blockEntries {
				b.builder.AddKey(e.k0, e.k1, e.payload, extractFingerprint(e.k0, e.k1, fpSize))
			}
			if err := b.buildBlockZeroCopy(); err != nil {
				return errors.Join(err, u.cleanup(), b.cleanup())
			}
		}
	}

	if err := u.cleanup(); err != nil {
		return errors.Join(err, b.cleanup())
	}
	b.unsortedBuf = nil

	if b.workers > 1 {
		return b.drainParallelPipeline()
	}
	return b.iw.finalize()
}

// finishUnsortedSingleThreaded reads partitions sequentially and builds blocks.
// Uses pipelined partition processing: reads partition P+1 in background
// while building blocks from partition P (benchmark #10: 11%+ gain).
func (b *Builder) finishUnsortedSingleThreaded() error {
	u := b.unsortedBuf
	numParts := u.numPartitions
	fpSize := b.cfg.fingerprintSize

	type readResult struct {
		entries [][]routedEntry
		err     error
	}

	var nextResult *readResult

	// Start reading partition 0
	if numParts > 0 {
		entries, err := u.readPartition(0, 0, u.readFlatBufs[0], u.readBlockIDs[0])
		nextResult = &readResult{entries, err}
	}

	for p := range numParts {
		currentResult := nextResult
		if currentResult.err != nil {
			return errors.Join(currentResult.err, u.cleanup(), b.cleanup())
		}

		// Start reading next partition in background (pipelining).
		var wg sync.WaitGroup
		if p+1 < numParts {
			nextResult = &readResult{}
			wg.Add(1)
			result := nextResult
			nextPart := p + 1
			nextPIdx := nextPart % 2
			go func() {
				defer wg.Done()
				result.entries, result.err = u.readPartition(nextPart, nextPIdx, u.readFlatBufs[nextPIdx], u.readBlockIDs[nextPIdx])
			}()
		}

		// Process current partition: build blocks in ascending order
		partStartBlock := uint32(p * u.blocksPerPart)
		for localIdx, entries := range currentResult.entries {
			blockID := partStartBlock + uint32(localIdx)

			if blockID%blockContextCheckInterval == 0 {
				select {
				case <-b.ctx.Done():
					wg.Wait()
					return errors.Join(b.ctx.Err(), u.cleanup(), b.cleanup())
				default:
				}
			}

			if len(entries) == 0 {
				b.commitEmptyBlock()
				continue
			}
			b.builder.Reset()
			for _, e := range entries {
				b.builder.AddKey(e.k0, e.k1, e.payload, extractFingerprint(e.k0, e.k1, fpSize))
			}
			if err := b.buildBlockZeroCopy(); err != nil {
				wg.Wait()
				return errors.Join(err, u.cleanup(), b.cleanup())
			}
		}

		wg.Wait()
	}

	if err := u.cleanup(); err != nil {
		return errors.Join(err, b.cleanup())
	}
	b.unsortedBuf = nil
	return b.iw.finalize()
}

// finishUnsortedParallel reads partitions and dispatches blocks to workers.
// Uses pipelined partition processing: reads partition P+1 in background
// while dispatching blocks from partition P.
func (b *Builder) finishUnsortedParallel() error {
	b.initParallelWorkers()

	u := b.unsortedBuf
	numParts := u.numPartitions

	type readResult struct {
		entries [][]routedEntry
		err     error
	}

	var nextResult *readResult

	// Per-pipeline-buffer fences: workers signal Done() after reading entries,
	// allowing the flatBuf to be safely reused for the next partition read.
	// This eliminates the need to copy entries from flatBuf to pool slices.
	var bufFence [2]sync.WaitGroup

	// Start reading partition 0
	if numParts > 0 {
		entries, err := u.readPartition(0, 0, u.readFlatBufs[0], u.readBlockIDs[0])
		nextResult = &readResult{entries, err}
	}

	for p := range numParts {
		currentResult := nextResult
		if currentResult.err != nil {
			return errors.Join(currentResult.err, u.cleanup(), b.cleanup())
		}

		pIdx := p % 2

		// Start reading next partition in background.
		// Fence: wait for workers to finish reading from the flatBuf before reusing it.
		var wg sync.WaitGroup
		if p+1 < numParts {
			nextResult = &readResult{}
			wg.Add(1)
			result := nextResult
			nextPart := p + 1
			nextPIdx := nextPart % 2
			fence := &bufFence[nextPIdx]
			go func() {
				defer wg.Done()
				// Wait for workers using this flatBuf to finish reading entries.
				// For the first use of each buffer, the fence count is 0 (instant).
				fence.Wait()
				result.entries, result.err = u.readPartition(nextPart, nextPIdx, u.readFlatBufs[nextPIdx], u.readBlockIDs[nextPIdx])
			}()
		}

		// Dispatch blocks from current partition directly from flatBuf.
		// Workers signal bufFence[pIdx] after reading entries, allowing
		// flatBuf[pIdx] to be reused when the next same-parity partition reads.
		partStartBlock := uint32(p * u.blocksPerPart)
		for localIdx, entries := range currentResult.entries {
			blockID := partStartBlock + uint32(localIdx)

			if blockID%blockContextCheckInterval == 0 {
				select {
				case <-b.ctx.Done():
					wg.Wait()
					return errors.Join(b.ctx.Err(), u.cleanup(), b.cleanup())
				default:
				}
				if b.writerDone != nil {
					select {
					case err := <-b.writerDone:
						b.writerErr = err
						wg.Wait()
						return errors.Join(err, u.cleanup(), b.cleanup())
					default:
					}
				}
			}

			if len(entries) == 0 {
				if err := b.dispatchEmptyBlock(blockID); err != nil {
					wg.Wait()
					return errors.Join(err, u.cleanup(), b.cleanup())
				}
				continue
			}

			// Dispatch entries directly from flatBuf (zero-copy).
			// Worker signals bufFence[pIdx].Done() after reading entries.
			bufFence[pIdx].Add(1)
			work := blockWork{
				blockID:    blockID,
				entries:    entries,
				fenceWg:    &bufFence[pIdx],
				keysBefore: b.keysBefore,
			}
			b.keysBefore += uint64(len(entries))
			b.nextBlockToWrite = blockID + 1

			select {
			case b.workChan <- work:
			case <-b.workerCtx.Done():
				wg.Wait()
				return errors.Join(b.workerCtx.Err(), u.cleanup(), b.cleanup())
			case err := <-b.writerDone:
				b.writerErr = err
				wg.Wait()
				return errors.Join(err, u.cleanup(), b.cleanup())
			}
		}

		wg.Wait()
	}

	// Wait for all remaining workers to finish with flatBuf data before cleanup.
	bufFence[0].Wait()
	bufFence[1].Wait()

	if err := u.cleanup(); err != nil {
		return errors.Join(err, b.cleanup())
	}
	b.unsortedBuf = nil

	return b.drainParallelPipeline()
}

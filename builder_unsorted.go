package streamhash

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"math/bits"
	"os"
	"sort"
	"sync"

	intbits "github.com/tamirms/streamhash/internal/bits"
)

const (
	// maxFlushBufferBytes caps the flush buffer at 64MB. Benchmark #7 showed
	// throughput is flat from 32MB-512MB, so 64MB wastes nothing while keeping
	// write-phase memory bounded.
	maxFlushBufferBytes = 64 << 20

	// defaultUnsortedMemoryBudget is the default peak RAM budget for unsorted
	// builds. Handles up to ~20B keys with P <= 5000.
	defaultUnsortedMemoryBudget int64 = 256 << 20

	// readBufSize is the streaming read buffer size for partition files.
	// 4MB provides good throughput without excess memory (benchmark #12).
	readBufSize = 4 << 20

	// bufferedEntrySize is the in-memory size of a bufferedEntry struct.
	bufferedEntrySize = 32 // 2×uint64 + uint64 + uint32 + uint32

	// maxPartitions limits the number of partition files to prevent fd exhaustion.
	maxPartitions = 5000
)

// bufferedEntry is the in-memory representation during the write phase.
// blockID is used for partition routing but not stored on disk.
type bufferedEntry struct {
	k0, k1      uint64
	payload     uint64
	fingerprint uint32
	blockID     uint32 // partition routing only, not written to disk
}

// unsortedBuffer implements partition flush for unsorted mode.
//
// Write phase: entries are appended directly to per-partition buffers
// (no sorting needed), then written sequentially to P partition files.
//
// Read phase: each partition file is streamed, entries decoded and grouped
// by blockID (recomputed from k0), then fed to block builders.
//
// Total I/O: 2N (all sequential). Scales to hundreds of billions of keys.
type unsortedBuffer struct {
	cfg       *buildConfig
	numBlocks uint32
	entrySize int // on-disk: minKeySize + fpSize + payloadSize

	// Partition layout
	partDir       string   // "" if fast path (no files needed)
	partPaths     []string // paths for open/close per flush
	numPartitions int
	blocksPerPart int
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

	// Per-block entry counts accumulated across flushes.
	// Used by readPartition for exact pre-allocation (zero growth waste).
	blockCounts []int

	flushed bool // true after first flush to disk
}

// newUnsortedBuffer creates an unsortedBuffer with partition flush.
func newUnsortedBuffer(cfg *buildConfig, numBlocks uint32) (*unsortedBuffer, error) {
	entrySize := minKeySize + cfg.fingerprintSize + cfg.payloadSize

	memoryBudget := cfg.unsortedMemoryBudget
	if memoryBudget <= 0 {
		memoryBudget = defaultUnsortedMemoryBudget
	}

	// Derive partition file target size.
	// Read phase decodes entries from partition file into routedEntry structs (32 bytes each).
	// With pipelining, two partitions are decoded concurrently.
	// Target: partFileTarget = memoryBudget * entrySize / 64
	// (accounts for decode expansion: routedEntry=32B, 2 concurrent pipelined partitions)
	partFileTarget := int64(memoryBudget) * int64(entrySize) / 64
	if partFileTarget < 1 {
		partFileTarget = 1
	}

	totalDataSize := int64(cfg.totalKeys) * int64(entrySize)
	numPartitions := int(math.Ceil(float64(totalDataSize) / float64(partFileTarget)))
	if numPartitions < 1 {
		numPartitions = 1
	}
	if numPartitions > maxPartitions {
		numPartitions = maxPartitions
	}
	// Cannot have more partitions than blocks (each partition needs >= 1 block)
	if numPartitions > int(numBlocks) {
		numPartitions = int(numBlocks)
	}

	blocksPerPart := int(math.Ceil(float64(numBlocks) / float64(numPartitions)))
	if blocksPerPart < 1 {
		blocksPerPart = 1
	}

	// Derive flush buffer size
	flushBufferBytes := memoryBudget / 4
	if flushBufferBytes > maxFlushBufferBytes {
		flushBufferBytes = maxFlushBufferBytes
	}
	if flushBufferBytes < int64(bufferedEntrySize) {
		flushBufferBytes = int64(bufferedEntrySize)
	}
	bufferCap := int(flushBufferBytes / bufferedEntrySize)

	// Each partition region gets bufferCap/P + 12.5% headroom for hash variance.
	// With hash-distributed keys, the max deviation is ~6σ ≈ 6*sqrt(N/P),
	// well within the 12.5% headroom for any practical N/P ratio.
	regionCap := bufferCap/numPartitions + bufferCap/(numPartitions*8)
	if regionCap < 1 {
		regionCap = 1
	}

	flatSize := regionCap * numPartitions
	u := &unsortedBuffer{
		cfg:           cfg,
		numBlocks:     numBlocks,
		entrySize:     entrySize,
		numPartitions:    numPartitions,
		blocksPerPart:    blocksPerPart,
		blocksPerPartU32: uint32(blocksPerPart),
		bufferCap:     bufferCap,
		regionCap:     regionCap,
		flatBufs: [2][]bufferedEntry{
			make([]bufferedEntry, flatSize),
			make([]bufferedEntry, flatSize),
		},
		cursorSets: [2][]int{
			make([]int, numPartitions),
			make([]int, numPartitions),
		},
		blockCounts: make([]int, numBlocks),
	}

	// Create partition directory eagerly to validate the temp dir early.
	// For fast-path datasets (all entries fit in buffer), the directory
	// will be empty and removed at cleanup.
	if err := u.createPartDir(cfg.tempDir); err != nil {
		return nil, fmt.Errorf("create partition directory: %w", err)
	}

	return u, nil
}

// createPartDir creates the temp directory and partition file paths.
func (u *unsortedBuffer) createPartDir(tempDir string) error {
	if tempDir == "" {
		tempDir = os.TempDir()
	}
	dir, err := os.MkdirTemp(tempDir, "streamhash-parts-*")
	if err != nil {
		return err
	}
	u.partDir = dir
	u.partPaths = make([]string, u.numPartitions)
	for i := range u.partPaths {
		u.partPaths[i] = fmt.Sprintf("%s/part-%05d", dir, i)
	}
	return nil
}

// addKey writes an entry directly to the active buffer's partition region.
// Returns true if the buffer is full and needs flushing. The caller must
// call flush() when addKey returns true.
func (u *unsortedBuffer) addKey(k0, k1 uint64, payload uint64, fingerprint uint32, blockID uint32) bool {
	// partitionID inlined. The clamping check (p >= numPartitions) is omitted
	// because blocksPerPart = ceil(numBlocks/numPartitions) guarantees
	// (numBlocks-1)/blocksPerPart < numPartitions for all valid blockIDs.
	// Uses uint32 division (DIVL) instead of int division (DIVQ) — 2-3x faster.
	p := int(blockID / u.blocksPerPartU32)
	a := u.activeBuf
	c := u.cursorSets[a][p]
	u.flatBufs[a][p*u.regionCap+c] = bufferedEntry{k0: k0, k1: k1, payload: payload, fingerprint: fingerprint, blockID: blockID}
	u.cursorSets[a][p] = c + 1
	u.totalBuffered++
	return u.totalBuffered >= u.bufferCap || c+1 >= u.regionCap
}

// partitionID returns the partition for a given blockID.
func (u *unsortedBuffer) partitionID(blockID uint32) int {
	p := int(blockID) / u.blocksPerPart
	if p >= u.numPartitions {
		p = u.numPartitions - 1
	}
	return p
}

// flush swaps the active buffer and launches background I/O for the filled buffer.
// The next batch of addKey calls writes to the other buffer concurrently.
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

	// Capture which buffer to flush and its cursor state.
	flushBuf := u.activeBuf
	// Switch to the other buffer for the next batch of addKey calls.
	u.activeBuf = 1 - flushBuf
	clear(u.cursorSets[u.activeBuf])
	u.totalBuffered = 0

	// Launch background flush of the filled buffer.
	u.flushWg.Add(1)
	go func() {
		defer u.flushWg.Done()
		u.flushErr = u.doFlush(flushBuf)
	}()

	return nil
}

// doFlush encodes and writes entries from the specified buffer to partition files.
// Runs in a background goroutine. Only accesses flatBufs[bufIdx] and cursorSets[bufIdx]
// (the inactive buffer), plus shared state protected by the WaitGroup.
func (u *unsortedBuffer) doFlush(bufIdx int) error {
	cursors := u.cursorSets[bufIdx]
	buf := u.flatBufs[bufIdx]

	// Accumulate per-block counts (used by readPartition for exact pre-allocation).
	maxCount := 0
	for p := range u.numPartitions {
		n := cursors[p]
		if n == 0 {
			continue
		}
		if n > maxCount {
			maxCount = n
		}
		base := p * u.regionCap
		for i := range n {
			u.blockCounts[buf[base+i].blockID]++
		}
	}

	// Size encodeBuf from actual max partition entry count
	encodeBufNeeded := maxCount * u.entrySize
	if cap(u.encodeBuf) < encodeBufNeeded {
		u.encodeBuf = make([]byte, encodeBufNeeded)
	}

	// Write each partition's entries
	for p := range u.numPartitions {
		n := cursors[p]
		if n == 0 {
			continue
		}

		// Encode entries from this partition's region
		base := p * u.regionCap
		encLen := u.encodeEntries(buf[base : base+n])

		// Open, write, close (overhead <1% even at P=3200, benchmark #4)
		f, err := os.OpenFile(u.partPaths[p], os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0600)
		if err != nil {
			return fmt.Errorf("open partition %d: %w", p, err)
		}
		_, err = f.Write(u.encodeBuf[:encLen])
		closeErr := f.Close()
		if err != nil {
			return fmt.Errorf("write partition %d: %w", p, err)
		}
		if closeErr != nil {
			return fmt.Errorf("close partition %d: %w", p, closeErr)
		}
	}

	return nil
}

// encodeEntries encodes a slice of bufferedEntries into u.encodeBuf.
// On-disk format per entry: k0(8) + k1(8) + fp(fpSize) + payload(payloadSize).
// Returns the number of bytes encoded.
func (u *unsortedBuffer) encodeEntries(entries []bufferedEntry) int {
	buf := u.encodeBuf
	pos := 0
	fpSize := u.cfg.fingerprintSize
	payloadSize := u.cfg.payloadSize

	for i := range entries {
		e := &entries[i]
		binary.LittleEndian.PutUint64(buf[pos:], e.k0)
		binary.LittleEndian.PutUint64(buf[pos+8:], e.k1)
		if fpSize > 0 {
			packFingerprintToBytes(buf[pos+minKeySize:], e.fingerprint, fpSize)
		}
		if payloadSize > 0 {
			packPayloadToBytes(buf[pos+minKeySize+fpSize:], e.payload, payloadSize)
		}
		pos += u.entrySize
	}
	return pos
}

// readPartition reads a partition file and returns entries grouped by block.
// Returns [][]routedEntry indexed by local block offset within partition
// (0..blocksPerPart-1 or less for the last partition).
//
// If flatBuf is non-nil, entries are decoded into it (reusing memory).
// Otherwise a new flat array is allocated. Uses exact per-block counts
// accumulated during flush() for zero-waste sub-slicing.
func (u *unsortedBuffer) readPartition(p int, flatBuf []routedEntry) ([][]routedEntry, error) {
	partStartBlock := p * u.blocksPerPart
	partEndBlock := partStartBlock + u.blocksPerPart
	if partEndBlock > int(u.numBlocks) {
		partEndBlock = int(u.numBlocks)
	}
	localBlocks := partEndBlock - partStartBlock
	if localBlocks <= 0 {
		return nil, nil
	}

	// Handle empty/non-existent partition files gracefully
	path := u.partPaths[p]
	fi, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return make([][]routedEntry, localBlocks), nil
		}
		return nil, fmt.Errorf("stat partition %d: %w", p, err)
	}
	fileSize := fi.Size()
	if fileSize == 0 {
		return make([][]routedEntry, localBlocks), nil
	}

	totalEntries := int(fileSize) / u.entrySize
	if int64(totalEntries)*int64(u.entrySize) != fileSize {
		return nil, fmt.Errorf("partition %d: file size %d not a multiple of entry size %d", p, fileSize, u.entrySize)
	}

	// Use provided flat buffer or allocate.
	flat := flatBuf
	if len(flat) < totalEntries {
		flat = make([]routedEntry, totalEntries)
	} else {
		flat = flat[:totalEntries]
	}

	// Compute per-block offsets into the flat buffer using exact counts from flush().
	// Use cursor-based writes (8-byte cursor vs 24-byte slice header per block)
	// for better L2 cache behavior during decode grouping.
	blockOffsets := make([]int, localBlocks+1)
	for i := range localBlocks {
		blockOffsets[i+1] = blockOffsets[i] + u.blockCounts[partStartBlock+i]
	}
	blockCursors := make([]int, localBlocks)

	// Open and hint for sequential read
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open partition %d: %w", p, err)
	}
	defer f.Close()
	fadviseSequential(int(f.Fd()), 0, fileSize)

	// Stream file in readBufSize chunks
	readBuf := make([]byte, readBufSize)
	fpSize := u.cfg.fingerprintSize
	payloadSize := u.cfg.payloadSize
	entrySize := u.entrySize

	// leftover tracks a partial entry from the previous read
	var leftover []byte

	for {
		nr, readErr := f.Read(readBuf)
		if nr == 0 && readErr != nil {
			if readErr == io.EOF {
				break
			}
			return nil, fmt.Errorf("read partition %d: %w", p, readErr)
		}

		// Prepend any leftover from previous iteration
		data := readBuf[:nr]
		if len(leftover) > 0 {
			data = append(leftover, data...)
			leftover = nil
		}

		// Decode complete entries
		for len(data) >= entrySize {
			k0 := binary.LittleEndian.Uint64(data[0:8])
			k1 := binary.LittleEndian.Uint64(data[8:16])
			fp := unpackFingerprintFromBytes(data[minKeySize:], fpSize)
			var payload uint64
			if payloadSize > 0 {
				payload = unpackPayloadFromBytes(data[minKeySize+fpSize:], payloadSize)
			}

			// Recompute blockID from k0 (benchmark #3: saves 20% I/O)
			prefix := bits.ReverseBytes64(k0)
			blockID := int(intbits.FastRange32(prefix, u.numBlocks))

			// Validate blockID falls within partition's block range
			if blockID < partStartBlock || blockID >= partEndBlock {
				return nil, fmt.Errorf("partition %d: blockID %d outside range [%d, %d)",
					p, blockID, partStartBlock, partEndBlock)
			}

			localIdx := blockID - partStartBlock
			pos := blockOffsets[localIdx] + blockCursors[localIdx]
			flat[pos] = routedEntry{
				k0:          k0,
				k1:          k1,
				payload:     payload,
				fingerprint: fp,
			}
			blockCursors[localIdx]++

			data = data[entrySize:]
		}

		// Save leftover partial entry
		if len(data) > 0 {
			leftover = make([]byte, len(data))
			copy(leftover, data)
		}

		if readErr != nil {
			break
		}
	}

	// Remove partition file to free disk immediately
	_ = os.Remove(path)

	// Construct result slices from flat buffer using offsets
	blockEntries := make([][]routedEntry, localBlocks)
	for i := range localBlocks {
		start := blockOffsets[i]
		end := start + blockCursors[i]
		blockEntries[i] = flat[start:end]
	}

	return blockEntries, nil
}

// prepareForRead flushes remaining entries and nils write-phase buffers.
// This ensures peak memory = max(write phase, read phase), not their sum.
func (u *unsortedBuffer) prepareForRead() error {
	if u.totalBuffered > 0 {
		if err := u.flush(); err != nil {
			return err
		}
	}
	// Wait for any background flush to complete
	u.flushWg.Wait()
	if u.flushErr != nil {
		return u.flushErr
	}
	// Free write-phase allocations
	u.flatBufs = [2][]bufferedEntry{}
	u.cursorSets = [2][]int{}
	u.encodeBuf = nil
	return nil
}

// maxPartitionEntries returns the maximum entry count across all partitions,
// computed from the accumulated blockCounts.
func (u *unsortedBuffer) maxPartitionEntries() int {
	maxEntries := 0
	for p := range u.numPartitions {
		startBlock := p * u.blocksPerPart
		endBlock := startBlock + u.blocksPerPart
		if endBlock > int(u.numBlocks) {
			endBlock = int(u.numBlocks)
		}
		count := 0
		for b := startBlock; b < endBlock; b++ {
			count += u.blockCounts[b]
		}
		if count > maxEntries {
			maxEntries = count
		}
	}
	return maxEntries
}

// cleanup removes all temp files and directories. Idempotent.
func (u *unsortedBuffer) cleanup() error {
	if u.partDir != "" {
		err := os.RemoveAll(u.partDir)
		u.partDir = ""
		u.flushWg.Wait()
		u.partPaths = nil
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

	// Collect all entries from the active buffer's partition regions.
	a := u.activeBuf
	buf := make([]bufferedEntry, 0, u.totalBuffered)
	for p := range u.numPartitions {
		n := u.cursorSets[a][p]
		if n > 0 {
			base := p * u.regionCap
			buf = append(buf, u.flatBufs[a][base:base+n]...)
		}
	}

	// Check context before doing any work
	select {
	case <-b.ctx.Done():
		return errors.Join(b.ctx.Err(), u.cleanup(), b.cleanup())
	default:
	}

	// Sort buffer by blockID in-place
	sort.Slice(buf, func(i, j int) bool {
		return buf[i].blockID < buf[j].blockID
	})

	if b.workers > 1 {
		b.initParallelWorkers()
	}

	numBlocks := u.numBlocks
	bufIdx := 0
	for blockID := uint32(0); blockID < numBlocks; blockID++ {
		// Periodic context check
		if blockID%64 == 0 {
			select {
			case <-b.ctx.Done():
				return errors.Join(b.ctx.Err(), u.cleanup(), b.cleanup())
			default:
			}
		}

		// Collect entries for this block
		start := bufIdx
		for bufIdx < len(buf) && buf[bufIdx].blockID == blockID {
			bufIdx++
		}

		if start == bufIdx {
			// Empty block
			if b.workers > 1 {
				if err := b.dispatchEmptyBlock(blockID); err != nil {
					return errors.Join(err, u.cleanup(), b.cleanup())
				}
			} else {
				b.commitEmptyBlock()
			}
			continue
		}

		if b.workers > 1 {
			entries := make([]routedEntry, bufIdx-start)
			for i, e := range buf[start:bufIdx] {
				entries[i] = routedEntry{k0: e.k0, k1: e.k1, payload: e.payload, fingerprint: e.fingerprint}
			}
			if err := b.dispatchBlockWork(blockID, entries); err != nil {
				return errors.Join(err, u.cleanup(), b.cleanup())
			}
		} else {
			b.builder.Reset()
			for _, e := range buf[start:bufIdx] {
				b.builder.AddKey(e.k0, e.k1, e.payload, e.fingerprint)
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
//
// Pre-allocates two flat buffers and alternates between them to avoid
// GC pressure from per-partition allocations.
func (b *Builder) finishUnsortedSingleThreaded() error {
	u := b.unsortedBuf
	numParts := u.numPartitions

	// Pre-allocate two reusable flat buffers for pipelining.
	maxEntries := u.maxPartitionEntries()
	flatBufs := [2][]routedEntry{
		make([]routedEntry, maxEntries),
		make([]routedEntry, maxEntries),
	}

	type readResult struct {
		entries [][]routedEntry
		err     error
	}

	var nextResult *readResult

	// Start reading partition 0
	if numParts > 0 {
		entries, err := u.readPartition(0, flatBufs[0])
		nextResult = &readResult{entries, err}
	}

	for p := 0; p < numParts; p++ {
		currentResult := nextResult
		if currentResult.err != nil {
			return errors.Join(currentResult.err, u.cleanup(), b.cleanup())
		}

		// Start reading next partition in background (pipelining).
		// Alternates flat buffers: current uses flatBufs[p%2], next uses flatBufs[(p+1)%2].
		var wg sync.WaitGroup
		if p+1 < numParts {
			nextResult = &readResult{}
			wg.Add(1)
			result := nextResult
			nextPart := p + 1
			buf := flatBufs[nextPart%2]
			go func() {
				defer wg.Done()
				result.entries, result.err = u.readPartition(nextPart, buf)
			}()
		}

		// Process current partition: build blocks in ascending order
		partStartBlock := uint32(p * u.blocksPerPart)
		for localIdx, entries := range currentResult.entries {
			blockID := partStartBlock + uint32(localIdx)

			// Periodic context check
			if blockID%64 == 0 {
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
				b.builder.AddKey(e.k0, e.k1, e.payload, e.fingerprint)
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
//
// Pre-allocates two flat buffers and alternates between them to avoid
// GC pressure from per-partition allocations.
func (b *Builder) finishUnsortedParallel() error {
	b.initParallelWorkers()

	u := b.unsortedBuf
	numParts := u.numPartitions

	// Pre-allocate two reusable flat buffers for pipelining.
	maxEntries := u.maxPartitionEntries()
	flatBufs := [2][]routedEntry{
		make([]routedEntry, maxEntries),
		make([]routedEntry, maxEntries),
	}

	type readResult struct {
		entries [][]routedEntry
		err     error
	}

	var nextResult *readResult

	// Start reading partition 0
	if numParts > 0 {
		entries, err := u.readPartition(0, flatBufs[0])
		nextResult = &readResult{entries, err}
	}

	for p := 0; p < numParts; p++ {
		currentResult := nextResult
		if currentResult.err != nil {
			return errors.Join(currentResult.err, u.cleanup(), b.cleanup())
		}

		// Start reading next partition in background.
		// Alternates flat buffers: current uses flatBufs[p%2], next uses flatBufs[(p+1)%2].
		var wg sync.WaitGroup
		if p+1 < numParts {
			nextResult = &readResult{}
			wg.Add(1)
			result := nextResult
			nextPart := p + 1
			buf := flatBufs[nextPart%2]
			go func() {
				defer wg.Done()
				result.entries, result.err = u.readPartition(nextPart, buf)
			}()
		}

		// Dispatch blocks from current partition
		partStartBlock := uint32(p * u.blocksPerPart)
		for localIdx, entries := range currentResult.entries {
			blockID := partStartBlock + uint32(localIdx)

			// Periodic context + writer-error check
			if blockID%64 == 0 {
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

			if err := b.dispatchBlockWork(blockID, entries); err != nil {
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

	return b.drainParallelPipeline()
}

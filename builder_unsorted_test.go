// builder_unsorted_test.go tests the unsorted mode builder: partition flush,
// superblock readback, buffer management, crash safety, and memory budgets.
package streamhash

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"math/bits"
	randv2 "math/rand/v2"
	"os"
	"runtime"
	"testing"

	intbits "github.com/tamirms/streamhash/internal/bits"
)

// =============================================================================
// Test helpers
// =============================================================================

// partitionID returns the partition for a given blockID. Test-only helper;
// production code inlines this computation in addKey for performance.
func (u *unsortedBuffer) partitionID(blockID uint32) int {
	p := int(blockID) / u.blocksPerPart
	if p >= u.numPartitions {
		p = u.numPartitions - 1
	}
	return p
}

// testEntry holds a key-payload pair with its pre-computed blockID for testing.
type testEntry struct {
	k0, k1  uint64
	payload uint64
	blockID uint32
}

// generateTestEntry generates a random test entry with the given payload and numBlocks.
func generateTestEntry(rng *randv2.Rand, payload uint64, numBlocks uint32) testEntry {
	k0 := rng.Uint64()
	k1 := rng.Uint64()
	prefix := bits.ReverseBytes64(k0)
	blockID := intbits.FastRange32(prefix, numBlocks)
	return testEntry{k0: k0, k1: k1, payload: payload, blockID: blockID}
}

// =============================================================================
// UnsortedBuffer tests
// =============================================================================

// TestUnsortedBuffer_FlushAndReadPartition tests the partition flush round-trip:
// addKey -> flush -> readPartition -> verify all entries are recovered.
func TestUnsortedBuffer_FlushAndReadPartition(t *testing.T) {
	rng := newTestRNG(t)
	configs := []struct {
		name    string
		payload int
		fp      int
	}{
		{"payload4_fp0", 4, 0},
		{"payload0_fp0", 0, 0},
		{"payload4_fp2", 4, 2},
		{"payload8_fp1", 8, 1},
	}

	for _, tc := range configs {
		t.Run(tc.name, func(t *testing.T) {
			numBlocks, err := numBlocksForAlgo(AlgoBijection, 1000, tc.payload, tc.fp)
			if err != nil {
				t.Fatalf("numBlocksForAlgo: %v", err)
			}

			cfg := &buildConfig{
				totalKeys:       1000,
				payloadSize:     tc.payload,
				fingerprintSize: tc.fp,
				unsortedTempDir: t.TempDir(),
			}

			u, err := newUnsortedBuffer(cfg, numBlocks)
			if err != nil {
				t.Fatalf("newUnsortedBuffer: %v", err)
			}
			defer u.cleanup()

			var entries []testEntry
			for i := range 100 {
				var payload uint64
				if tc.payload > 0 && tc.payload < 8 {
					payload = uint64(i) & ((1 << (tc.payload * 8)) - 1)
				} else if tc.payload == 8 {
					payload = uint64(i)
				}
				e := generateTestEntry(rng, payload, numBlocks)
				entries = append(entries, e)
			}

			for _, e := range entries {
				if u.addKey(e.k0, e.k1, e.payload, e.blockID) {
					if err := u.flush(); err != nil {
						t.Fatalf("flush: %v", err)
					}
				}
			}

			// Flush and read back via partitions
			if err := u.prepareForRead(); err != nil {
				t.Fatalf("prepareForRead: %v", err)
			}

			// Read all partitions and collect entries by blockID
			recovered := make(map[uint32][]routedEntry)
			for p := range u.numPartitions {
				blockEntries, err := u.readPartition(p, p%2, u.readFlatBufs[p%2], u.readBlockIDs[p%2])
				if err != nil {
					t.Fatalf("readPartition(%d): %v", p, err)
				}
				partStartBlock := p * u.blocksPerPart
				for localIdx, entries := range blockEntries {
					blockID := uint32(partStartBlock + localIdx)
					recovered[blockID] = append(recovered[blockID], entries...)
				}
			}

			// Verify all entries are recovered
			for _, e := range entries {
				found := false
				for _, got := range recovered[e.blockID] {
					if got.k0 == e.k0 && got.k1 == e.k1 {
						if got.payload != e.payload {
							t.Errorf("payload mismatch: got %d, want %d", got.payload, e.payload)
						}
						found = true
						break
					}
				}
				if !found {
					t.Errorf("entry not found: k0=%x k1=%x blockID=%d", e.k0, e.k1, e.blockID)
				}
			}
		})
	}
}

// TestUnsortedBuffer_FastPath tests that small datasets that fit entirely
// in the flush buffer skip file I/O (fast path).
func TestUnsortedBuffer_FastPath(t *testing.T) {
	numKeys := uint64(50) // Very small, fits in any flush buffer
	numBlocks, err := numBlocksForAlgo(AlgoBijection, numKeys, 4, 0)
	if err != nil {
		t.Fatalf("numBlocksForAlgo: %v", err)
	}

	cfg := &buildConfig{
		totalKeys:       numKeys,
		payloadSize:     4,
		unsortedTempDir: t.TempDir(),
	}

	u, err := newUnsortedBuffer(cfg, numBlocks)
	if err != nil {
		t.Fatalf("newUnsortedBuffer: %v", err)
	}
	defer u.cleanup()

	rng := newTestRNG(t)
	for i := range int(numKeys) {
		key := make([]byte, 16)
		fillFromRNG(rng, key)
		k0 := binary.LittleEndian.Uint64(key[0:8])
		k1 := binary.LittleEndian.Uint64(key[8:16])
		prefix := extractPrefix(key)
		blockID := blockIndexFromPrefix(prefix, numBlocks)
		if u.addKey(k0, k1, uint64(i), blockID) {
			if err := u.flush(); err != nil {
				t.Fatalf("flush %d: %v", i, err)
			}
		}
	}

	// After adding all keys, flushed should still be false (fast path)
	if u.flushed {
		t.Error("expected flushed=false for small dataset (fast path)")
	}
	// partDir should be empty (files are only created on flush)
	if u.partDir != "" {
		entries, err := os.ReadDir(u.partDir)
		if err != nil {
			t.Fatalf("ReadDir: %v", err)
		}
		if len(entries) > 0 {
			t.Errorf("expected empty partDir (fast path), got %d files", len(entries))
		}
	}
}

// TestUnsortedBuffer_LastPartitionBoundary tests off-by-one handling
// for the last partition when numBlocks is not evenly divisible by blocksPerPart.
func TestUnsortedBuffer_LastPartitionBoundary(t *testing.T) {
	numKeys := uint64(5000)
	numBlocks, err := numBlocksForAlgo(AlgoBijection, numKeys, 0, 0)
	if err != nil {
		t.Fatalf("numBlocksForAlgo: %v", err)
	}

	cfg := &buildConfig{
		totalKeys:       numKeys,
		unsortedTempDir: t.TempDir(),
	}

	u, err := newUnsortedBuffer(cfg, numBlocks)
	if err != nil {
		t.Fatalf("newUnsortedBuffer: %v", err)
	}
	defer u.cleanup()

	// Verify partition layout covers all blocks
	lastPartStart := (u.numPartitions - 1) * u.blocksPerPart
	lastPartEnd := lastPartStart + u.blocksPerPart
	if lastPartEnd < int(numBlocks) {
		t.Errorf("last partition ends at block %d but numBlocks=%d", lastPartEnd, numBlocks)
	}

	// Verify partitionID for last block maps to last partition
	lastBlockPartID := u.partitionID(numBlocks - 1)
	if lastBlockPartID != u.numPartitions-1 {
		t.Errorf("last block (ID=%d) maps to partition %d, expected %d",
			numBlocks-1, lastBlockPartID, u.numPartitions-1)
	}
}

// TestUnsortedBuffer_EmptyPartitions tests handling of partition files
// that have no entries (empty or non-existent).
func TestUnsortedBuffer_EmptyPartitions(t *testing.T) {
	numKeys := uint64(5000)
	numBlocks, err := numBlocksForAlgo(AlgoBijection, numKeys, 0, 0)
	if err != nil {
		t.Fatalf("numBlocksForAlgo: %v", err)
	}

	cfg := &buildConfig{
		totalKeys:       numKeys,
		unsortedTempDir: t.TempDir(),
	}

	u, err := newUnsortedBuffer(cfg, numBlocks)
	if err != nil {
		t.Fatalf("newUnsortedBuffer: %v", err)
	}
	defer u.cleanup()

	// Don't add any keys, just flush (creating partition dir and files)
	if err := u.flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}

	// Prepare for reading (allocate read-phase buffers)
	if err := u.prepareForRead(); err != nil {
		t.Fatalf("prepareForRead: %v", err)
	}

	// Read back — all partitions should return empty block slices
	for p := range u.numPartitions {
		blockEntries, err := u.readPartition(p, p%2, u.readFlatBufs[p%2], u.readBlockIDs[p%2])
		if err != nil {
			t.Fatalf("readPartition(%d): %v", p, err)
		}
		for localIdx, entries := range blockEntries {
			if len(entries) > 0 {
				t.Errorf("partition %d block %d: expected 0 entries, got %d",
					p, localIdx, len(entries))
			}
		}
	}
}

// TestUnsortedBuffer_CorruptionDetection verifies that readPartition detects
// entries whose recomputed blockID falls outside the partition's block range.
func TestUnsortedBuffer_CorruptionDetection(t *testing.T) {
	numKeys := uint64(10000)
	numBlocks, err := numBlocksForAlgo(AlgoBijection, numKeys, 0, 0)
	if err != nil {
		t.Fatalf("numBlocksForAlgo: %v", err)
	}

	cfg := &buildConfig{
		totalKeys:       numKeys,
		unsortedTempDir: t.TempDir(),
	}
	u, err := newUnsortedBuffer(cfg, numBlocks)
	if err != nil {
		t.Fatalf("newUnsortedBuffer: %v", err)
	}
	defer u.cleanup()

	if u.numPartitions < 2 {
		t.Skip("need at least 2 partitions for corruption test")
	}

	// Force flush to create partition files
	if err := u.flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}

	// Find a key that routes to the last partition's block range
	rng := newTestRNG(t)
	lastPartStart := (u.numPartitions - 1) * u.blocksPerPart
	var corruptK0, corruptK1 uint64
	found := false
	for range 100000 {
		key := make([]byte, 16)
		fillFromRNG(rng, key)
		k0 := binary.LittleEndian.Uint64(key[0:8])
		prefix := extractPrefix(key)
		blockID := blockIndexFromPrefix(prefix, numBlocks)
		if int(blockID) >= lastPartStart {
			corruptK0 = k0
			corruptK1 = binary.LittleEndian.Uint64(key[8:16])
			found = true
			break
		}
	}
	if !found {
		t.Fatal("could not find a key routing to the last partition")
	}

	// Write this entry into partition 0's file via fd (files are unlinked)
	entryBuf := make([]byte, u.entrySize)
	binary.LittleEndian.PutUint64(entryBuf[0:8], corruptK0)
	binary.LittleEndian.PutUint64(entryBuf[8:16], corruptK1)
	f := u.partFiles[0]
	if _, err := f.Seek(0, 0); err != nil {
		t.Fatalf("seek partition 0: %v", err)
	}
	if err := f.Truncate(0); err != nil {
		t.Fatalf("truncate partition 0: %v", err)
	}
	if _, err := f.Write(entryBuf); err != nil {
		t.Fatalf("write corrupt partition: %v", err)
	}

	// Prepare for reading
	if err := u.prepareForRead(); err != nil {
		t.Fatalf("prepareForRead: %v", err)
	}

	// readPartition should detect the misplaced entry
	_, err = u.readPartition(0, 0, u.readFlatBufs[0], u.readBlockIDs[0])
	if err == nil {
		t.Fatal("expected error for corrupt partition (blockID outside range), got nil")
	}
	if !bytes.Contains([]byte(err.Error()), []byte("outside")) {
		t.Errorf("expected 'outside' in error, got: %v", err)
	}
}

func TestUnsortedBuffer_CleanupIdempotent(t *testing.T) {
	numBlocks, err := numBlocksForAlgo(AlgoBijection, 1000, 4, 0)
	if err != nil {
		t.Fatalf("numBlocksForAlgo: %v", err)
	}

	cfg := &buildConfig{totalKeys: 1000, payloadSize: 4, unsortedTempDir: t.TempDir()}
	u, err := newUnsortedBuffer(cfg, numBlocks)
	if err != nil {
		t.Fatalf("newUnsortedBuffer: %v", err)
	}

	// Force a flush to create partition directory and files
	if err := u.flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}

	if err := u.cleanup(); err != nil {
		t.Fatalf("first cleanup: %v", err)
	}

	if err := u.cleanup(); err != nil {
		t.Fatalf("second cleanup: %v", err)
	}
}

func TestUnsortedBuffer_CleanupRemovesPartDir(t *testing.T) {
	tmpDir := t.TempDir()
	numBlocks, err := numBlocksForAlgo(AlgoBijection, 1000, 4, 0)
	if err != nil {
		t.Fatalf("numBlocksForAlgo: %v", err)
	}

	cfg := &buildConfig{totalKeys: 1000, payloadSize: 4, unsortedTempDir: tmpDir}
	u, err := newUnsortedBuffer(cfg, numBlocks)
	if err != nil {
		t.Fatalf("newUnsortedBuffer: %v", err)
	}

	// Force flush to create partition directory and files
	if err := u.flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}

	partDir := u.partDir
	if partDir == "" {
		t.Fatal("expected partDir to be set after flush")
	}

	if err := u.cleanup(); err != nil {
		t.Fatalf("cleanup: %v", err)
	}

	if _, err := os.Stat(partDir); !os.IsNotExist(err) {
		t.Errorf("partition directory still exists after cleanup: %s", partDir)
	}

	if u.partDir != "" {
		t.Error("partDir should be empty after cleanup")
	}
}

// =============================================================================
// Superblock readback tests
// =============================================================================

// TestComputeSBRegions_SumOfAllocations verifies that the total region size
// returned by computeSBRegions is always >= totalEntries for various configs.
// This ensures no entry is left without room.
func TestComputeSBRegions_SumOfAllocations(t *testing.T) {
	configs := []struct {
		totalEntries int
		localBlocks  int
		S            int
	}{
		{1000, 10, 500},
		{10000, 100, 500},
		{100000, 1000, 500},
		{1000000, 50000, 500},
		{5000000, 100000, 500},
		{100, 1, 500},      // Single block
		{100, 501, 500},    // blocksPerPart = S+1
		{100000, 499, 500}, // Just under S
		{100000, 500, 500}, // Exactly S
		{100000, 502, 500}, // Last SB has 2 blocks
		{50, 2, 500},       // Very small
	}

	for _, tc := range configs {
		totalRegion, maxSBRegion := computeSBRegions(tc.totalEntries, tc.localBlocks, tc.S, nil)
		if totalRegion < tc.totalEntries {
			t.Errorf("totalEntries=%d localBlocks=%d S=%d: totalRegion=%d < totalEntries",
				tc.totalEntries, tc.localBlocks, tc.S, totalRegion)
		}
		if maxSBRegion <= 0 && tc.totalEntries > 0 {
			t.Errorf("totalEntries=%d localBlocks=%d S=%d: maxSBRegion=%d should be > 0",
				tc.totalEntries, tc.localBlocks, tc.S, maxSBRegion)
		}
	}
}

// TestPoissonMarginSimulation generates random Poisson-distributed samples
// for various lambda values and verifies that the violation rate is negligible.
// This validates the 6-sigma margin used in superblock readback.
//
// For large lambda (>=50), the Gaussian approximation is tight and we expect
// zero violations. For small lambda (<50), the discrete Poisson tail can
// exceed the continuous Gaussian bound at very low rates; we allow up to
// 1e-4 violation rate for these cases.
func TestPoissonMarginSimulation(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping Poisson margin simulation in short mode")
	}

	lambdas := []float64{50, 100, 1000, 10000, 50000, 100000}
	const trials = 10_000_000

	for _, lambda := range lambdas {
		threshold := lambda + superblockMarginSigmas*math.Sqrt(lambda)
		rng := newTestRNG(t)

		violations := 0
		for trial := range trials {
			// Generate Poisson(lambda) sample.
			// For large lambda, use normal approximation (CLT).
			var sample float64
			if lambda < 500 {
				// Direct method for small lambda (Knuth).
				// Boundary at 500 (not 1000) because math.Exp(-lambda) underflows
				// to 0.0 in float64 for lambda >= 750, causing infinite loops.
				L := math.Exp(-lambda)
				k := 0
				p := 1.0
				for {
					k++
					p *= rng.Float64()
					if p <= L {
						break
					}
				}
				sample = float64(k - 1)
			} else {
				// Normal approximation for large lambda
				// Box-Muller transform
				u1 := rng.Float64()
				u2 := rng.Float64()
				z := math.Sqrt(-2*math.Log(u1)) * math.Cos(2*math.Pi*u2)
				sample = math.Round(lambda + math.Sqrt(lambda)*z)
				if sample < 0 {
					sample = 0
				}
			}

			if sample > threshold {
				violations++
				if violations == 1 {
					t.Logf("lambda=%.0f: first violation at trial %d sample=%.0f threshold=%.1f",
						lambda, trial, sample, threshold)
				}
			}
		}
		rate := float64(violations) / float64(trials)
		t.Logf("lambda=%.0f: %d/%d violations (%.2e rate)", lambda, violations, trials, rate)
		if violations > 0 {
			t.Errorf("lambda=%.0f: %d violations (rate %.2e), expected 0 for large lambda",
				lambda, violations, rate)
		}
	}
}

// TestReadPartition_SmallPartition creates a small partition file with known entries,
// calls readPartition, and verifies entries are correctly grouped by blockID.
func TestReadPartition_SmallPartition(t *testing.T) {
	numKeys := uint64(10000)
	numBlocks, err := numBlocksForAlgo(AlgoBijection, numKeys, 4, 0)
	if err != nil {
		t.Fatalf("numBlocksForAlgo: %v", err)
	}

	cfg := &buildConfig{
		totalKeys:       numKeys,
		payloadSize:     4,
		unsortedTempDir: t.TempDir(),
	}
	u, err := newUnsortedBuffer(cfg, numBlocks)
	if err != nil {
		t.Fatalf("newUnsortedBuffer: %v", err)
	}
	defer u.cleanup()

	// Add 1000 entries and flush
	rng := newTestRNG(t)
	var entries []testEntry
	for i := range 1000 {
		key := make([]byte, 16)
		fillFromRNG(rng, key)
		k0 := binary.LittleEndian.Uint64(key[0:8])
		k1 := binary.LittleEndian.Uint64(key[8:16])
		payload := uint64(i) & 0xFFFFFFFF
		prefix := extractPrefix(key)
		blockID := blockIndexFromPrefix(prefix, numBlocks)

		entries = append(entries, testEntry{k0, k1, payload, blockID})
		if u.addKey(k0, k1, payload, blockID) {
			if err := u.flush(); err != nil {
				t.Fatalf("flush: %v", err)
			}
		}
	}

	if err := u.prepareForRead(); err != nil {
		t.Fatalf("prepareForRead: %v", err)
	}

	// Build expected: group entries by blockID
	expected := make(map[uint32][]testEntry)
	for _, e := range entries {
		expected[e.blockID] = append(expected[e.blockID], e)
	}

	// Read all partitions and verify
	recovered := make(map[uint32][]routedEntry)
	for p := range u.numPartitions {
		blockEntries, err := u.readPartition(p, p%2, u.readFlatBufs[p%2], u.readBlockIDs[p%2])
		if err != nil {
			t.Fatalf("readPartition(%d): %v", p, err)
		}
		partStartBlock := p * u.blocksPerPart
		for localIdx, entries := range blockEntries {
			blockID := uint32(partStartBlock + localIdx)
			recovered[blockID] = append(recovered[blockID], entries...)
		}
	}

	// Verify every entry is recovered in the correct block
	for _, e := range entries {
		found := false
		for _, got := range recovered[e.blockID] {
			if got.k0 == e.k0 && got.k1 == e.k1 && got.payload == e.payload {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("entry not found: k0=%x blockID=%d", e.k0, e.blockID)
		}
	}

	// Verify no extra entries
	totalRecovered := 0
	for _, re := range recovered {
		totalRecovered += len(re)
	}
	if totalRecovered != len(entries) {
		t.Errorf("recovered %d entries, want %d", totalRecovered, len(entries))
	}
}

// TestSuperblock_LastSBFewBlocks tests edge cases where the last superblock
// has fewer than S blocks, including blocksPerPart = S+1 (last SB has 1 block).
func TestSuperblock_LastSBFewBlocks(t *testing.T) {
	configs := []struct {
		name        string
		localBlocks int
	}{
		{"S+1 (last SB has 1 block)", superblockSize + 1},
		{"S+2 (last SB has 2 blocks)", superblockSize + 2},
		{"single SB (50 blocks)", 50},
		{"single SB (1 block)", 1},
		{"exactly S", superblockSize},
		{"2*S+1 (last SB has 1)", 2*superblockSize + 1},
	}

	for _, tc := range configs {
		t.Run(tc.name, func(t *testing.T) {
			totalEntries := tc.localBlocks * 100 // ~100 entries per block
			totalRegion, maxSBRegion := computeSBRegions(totalEntries, tc.localBlocks, superblockSize, nil)

			// totalRegion must be >= totalEntries
			if totalRegion < totalEntries {
				t.Errorf("totalRegion=%d < totalEntries=%d", totalRegion, totalEntries)
			}

			// maxSBRegion must be positive
			if maxSBRegion <= 0 {
				t.Errorf("maxSBRegion=%d should be > 0", maxSBRegion)
			}

			// Verify margin is reasonable (< 10% of total for large regions)
			margin := totalRegion - totalEntries
			if totalEntries > 10000 {
				marginPct := float64(margin) / float64(totalEntries) * 100
				if marginPct > 10 {
					t.Errorf("margin %.1f%% is too large (expected < 10%%)", marginPct)
				}
			}
		})
	}
}

// TestSuperblock_UnbalancedLastPartition tests that readPartition correctly
// uses localBlocks (not blocksPerPart) as the denominator when the last
// partition has fewer blocks than blocksPerPart.
func TestSuperblock_UnbalancedLastPartition(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}

	// Use a key count that produces an unbalanced last partition.
	// 5.1M keys with AlgoBijection: numBlocks=1661, P=2, bpp=831, lastPB=830
	numKeys := uint64(5_100_000)
	numBlocks, err := numBlocksForAlgo(AlgoBijection, numKeys, 0, 0)
	if err != nil {
		t.Fatalf("numBlocksForAlgo: %v", err)
	}

	cfg := &buildConfig{
		totalKeys:       numKeys,
		unsortedTempDir: t.TempDir(),
	}
	u, err := newUnsortedBuffer(cfg, numBlocks)
	if err != nil {
		t.Fatalf("newUnsortedBuffer: %v", err)
	}
	defer u.cleanup()

	if u.numPartitions < 2 {
		t.Skip("need multiple partitions")
	}

	lastPartBlocks := int(numBlocks) - (u.numPartitions-1)*u.blocksPerPart
	if lastPartBlocks == u.blocksPerPart {
		t.Skip("last partition has same blocks as others (evenly divisible)")
	}

	t.Logf("numBlocks=%d numPartitions=%d blocksPerPart=%d lastPartBlocks=%d",
		numBlocks, u.numPartitions, u.blocksPerPart, lastPartBlocks)

	// Add entries, ensuring some land in the last partition
	rng := newTestRNG(t)
	added := 0
	for added < 100000 {
		key := make([]byte, 16)
		fillFromRNG(rng, key)
		k0 := binary.LittleEndian.Uint64(key[0:8])
		k1 := binary.LittleEndian.Uint64(key[8:16])
		prefix := extractPrefix(key)
		blockID := blockIndexFromPrefix(prefix, numBlocks)
		if u.addKey(k0, k1, uint64(added), blockID) {
			if err := u.flush(); err != nil {
				t.Fatalf("flush: %v", err)
			}
		}
		added++
	}

	if err := u.prepareForRead(); err != nil {
		t.Fatalf("prepareForRead: %v", err)
	}

	// Read the last partition — should succeed despite fewer blocks
	lastP := u.numPartitions - 1
	blockEntries, err := u.readPartition(lastP, 0, u.readFlatBufs[0], u.readBlockIDs[0])
	if err != nil {
		t.Fatalf("readPartition(%d) for unbalanced last partition: %v", lastP, err)
	}

	// Verify the returned slice length matches the last partition's block count
	if len(blockEntries) != lastPartBlocks {
		t.Errorf("blockEntries length=%d, want %d (lastPartBlocks)", len(blockEntries), lastPartBlocks)
	}
}

// TestSuperblock_ModerateSkew tests that the 6-sigma margin absorbs moderate
// skew (2x expected concentration in one superblock).
func TestSuperblock_ModerateSkew(t *testing.T) {
	numKeys := uint64(100000)
	numBlocks, err := numBlocksForAlgo(AlgoBijection, numKeys, 0, 0)
	if err != nil {
		t.Fatalf("numBlocksForAlgo: %v", err)
	}

	cfg := &buildConfig{
		totalKeys:       numKeys,
		unsortedTempDir: t.TempDir(),
	}
	u, err := newUnsortedBuffer(cfg, numBlocks)
	if err != nil {
		t.Fatalf("newUnsortedBuffer: %v", err)
	}
	defer u.cleanup()

	// Generate keys with moderate skew: preferentially route ~2x keys
	// into blocks in the first superblock of partition 0 by generating
	// random keys and keeping the ones that route where we want.
	rng := newTestRNG(t)
	firstPartEndBlock := min(uint32(u.blocksPerPart), numBlocks)
	firstSBEndBlock := uint32(min(superblockSize, int(firstPartEndBlock)))

	added := 0
	skewedAdded := 0
	normalAdded := 0
	target := 10000

	for added < target {
		key := make([]byte, 16)
		fillFromRNG(rng, key)
		k0 := binary.LittleEndian.Uint64(key[0:8])
		k1 := binary.LittleEndian.Uint64(key[8:16])
		prefix := extractPrefix(key)
		blockID := blockIndexFromPrefix(prefix, numBlocks)

		// Accept all keys, but generate extra for the first SB
		if blockID < firstSBEndBlock && skewedAdded < target/2 {
			if u.addKey(k0, k1, uint64(added), blockID) {
				if err := u.flush(); err != nil {
					t.Fatalf("flush: %v", err)
				}
			}
			added++
			skewedAdded++
		} else if normalAdded < target/2 {
			if u.addKey(k0, k1, uint64(added), blockID) {
				if err := u.flush(); err != nil {
					t.Fatalf("flush: %v", err)
				}
			}
			added++
			normalAdded++
		}
	}

	if err := u.prepareForRead(); err != nil {
		t.Fatalf("prepareForRead: %v", err)
	}

	// Read partition 0 — the skew should be absorbed by the margin
	_, err = u.readPartition(0, 0, u.readFlatBufs[0], u.readBlockIDs[0])
	if err != nil {
		t.Fatalf("readPartition(0) with moderate skew: %v", err)
	}
}

// TestSuperblock_OverflowError verifies that readPartition returns an error
// when blockIDs are non-uniform enough to overflow a superblock's region.
// We write directly to a partition file fd to inject entries that all route
// to the same block, overwhelming the superblock margin.
func TestSuperblock_OverflowError(t *testing.T) {
	// Use enough keys to get multiple partitions with many blocks
	numKeys := uint64(5_000_000)
	numBlocks, err := numBlocksForAlgo(AlgoBijection, numKeys, 0, 0)
	if err != nil {
		t.Fatalf("numBlocksForAlgo: %v", err)
	}

	cfg := &buildConfig{
		totalKeys:       numKeys,
		unsortedTempDir: t.TempDir(),
	}
	u, err := newUnsortedBuffer(cfg, numBlocks)
	if err != nil {
		t.Fatalf("newUnsortedBuffer: %v", err)
	}
	defer u.cleanup()

	if u.numPartitions < 2 {
		t.Skipf("need at least 2 partitions, got %d", u.numPartitions)
	}

	// Force flush to create partition files
	if err := u.flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}

	// Write a massive number of entries all routing to the same block
	// directly into partition 0's file. This simulates extreme non-uniformity.
	// All entries have blockID=0, which is in partition 0's first superblock.
	rng := newTestRNG(t)
	f := u.partFiles[0]
	if _, err := f.Seek(0, 0); err != nil {
		t.Fatalf("seek: %v", err)
	}
	if err := f.Truncate(0); err != nil {
		t.Fatalf("truncate: %v", err)
	}

	// Write 10000 entries all routing to block 0. With blocksPerPart blocks,
	// expected entries per SB = totalEntries * blocksInSB / localBlocks.
	// All entries in SB 0 will far exceed the expected + margin.
	numOverflow := 10000
	for range numOverflow {
		// Generate a key that routes to block 0
		for {
			key := make([]byte, 16)
			fillFromRNG(rng, key)
			k0 := binary.LittleEndian.Uint64(key[0:8])
			prefix := extractPrefix(key)
			blockID := blockIndexFromPrefix(prefix, numBlocks)
			if blockID == 0 {
				entryBuf := make([]byte, u.entrySize)
				binary.LittleEndian.PutUint64(entryBuf[0:8], k0)
				binary.LittleEndian.PutUint64(entryBuf[8:16], binary.LittleEndian.Uint64(key[8:16]))
				if _, err := f.Write(entryBuf); err != nil {
					t.Fatalf("write: %v", err)
				}
				break
			}
		}
	}

	if err := u.prepareForRead(); err != nil {
		t.Fatalf("prepareForRead: %v", err)
	}

	// readPartition should detect overflow
	_, err = u.readPartition(0, 0, u.readFlatBufs[0], u.readBlockIDs[0])
	if err == nil {
		t.Fatal("expected overflow error for extremely non-uniform input, got nil")
	}
	if !bytes.Contains([]byte(err.Error()), []byte("overflow")) {
		t.Errorf("expected 'overflow' in error, got: %v", err)
	}
}

// TestSuperblock_ProportionalAllocation tests that per-SB proportional
// allocation prevents overflow when blocksPerPart=S+1 (last SB has 1 block)
// with uniform keys.
func TestSuperblock_ProportionalAllocation(t *testing.T) {
	// We need a config where a partition has exactly S+1 blocks.
	// Use a large key count and find a suitable numBlocks.
	numKeys := uint64(200000)
	numBlocks, err := numBlocksForAlgo(AlgoBijection, numKeys, 0, 0)
	if err != nil {
		t.Fatalf("numBlocksForAlgo: %v", err)
	}

	cfg := &buildConfig{
		totalKeys:       numKeys,
		unsortedTempDir: t.TempDir(),
	}
	u, err := newUnsortedBuffer(cfg, numBlocks)
	if err != nil {
		t.Fatalf("newUnsortedBuffer: %v", err)
	}
	defer u.cleanup()

	remainder := u.blocksPerPart % superblockSize
	t.Logf("blocksPerPart=%d, remainder after S=%d", u.blocksPerPart, remainder)

	// Add entries uniformly and verify readback works
	rng := newTestRNG(t)
	for i := range 20000 {
		key := make([]byte, 16)
		fillFromRNG(rng, key)
		k0 := binary.LittleEndian.Uint64(key[0:8])
		k1 := binary.LittleEndian.Uint64(key[8:16])
		prefix := extractPrefix(key)
		blockID := blockIndexFromPrefix(prefix, numBlocks)
		if u.addKey(k0, k1, uint64(i), blockID) {
			if err := u.flush(); err != nil {
				t.Fatalf("flush: %v", err)
			}
		}
	}

	if err := u.prepareForRead(); err != nil {
		t.Fatalf("prepareForRead: %v", err)
	}

	// Read all partitions — last partition may have a different block count
	totalRecovered := 0
	for p := range u.numPartitions {
		blockEntries, err := u.readPartition(p, p%2, u.readFlatBufs[p%2], u.readBlockIDs[p%2])
		if err != nil {
			t.Fatalf("readPartition(%d): %v", p, err)
		}
		for _, be := range blockEntries {
			totalRecovered += len(be)
		}
	}

	if totalRecovered != 20000 {
		t.Errorf("total recovered entries=%d, want 20000", totalRecovered)
	}
}

// =============================================================================
// Partition count and file count verification
// =============================================================================

// TestUnsortedBuffer_PartitionCountDerivation verifies that the partition count
// scales correctly with the number of keys and entry sizes.
func TestUnsortedBuffer_PartitionCountDerivation(t *testing.T) {
	configs := []struct {
		name        string
		numKeys     uint64
		payloadSize int
		fpSize      int
		minP        int // expected minimum partitions
		maxP        int // expected maximum partitions
	}{
		// Small: fits in 1 partition
		{"1K_p0", 1000, 0, 0, 1, 1},
		{"10K_p0", 10_000, 0, 0, 1, 1},
		// Medium: a few partitions
		{"1M_p0", 1_000_000, 0, 0, 1, 3},
		{"1M_p4", 1_000_000, 4, 0, 1, 3},
		// Large: many partitions
		{"10M_p0", 10_000_000, 0, 0, 1, 10},
		{"10M_p4", 10_000_000, 4, 0, 1, 10},
		// Very large: scaling continues
		{"100M_p0", 100_000_000, 0, 0, 5, 50},
		{"100M_p4", 100_000_000, 4, 0, 5, 50},
		// Larger payload -> more data -> more partitions
		{"10M_p8", 10_000_000, 8, 0, 1, 15},
	}

	for _, tc := range configs {
		t.Run(tc.name, func(t *testing.T) {
			numBlocks, err := numBlocksForAlgo(AlgoBijection, tc.numKeys, tc.payloadSize, tc.fpSize)
			if err != nil {
				t.Fatalf("numBlocksForAlgo: %v", err)
			}

			cfg := &buildConfig{
				totalKeys:       tc.numKeys,
				payloadSize:     tc.payloadSize,
				fingerprintSize: tc.fpSize,
				unsortedTempDir: t.TempDir(),
			}

			u, err := newUnsortedBuffer(cfg, numBlocks)
			if err != nil {
				t.Fatalf("newUnsortedBuffer: %v", err)
			}
			defer u.cleanup()

			if u.numPartitions < tc.minP {
				t.Errorf("numPartitions=%d, expected >= %d", u.numPartitions, tc.minP)
			}
			if u.numPartitions > tc.maxP {
				t.Errorf("numPartitions=%d, expected <= %d", u.numPartitions, tc.maxP)
			}

			// Verify partition layout covers all blocks
			totalBlocks := (u.numPartitions - 1) * u.blocksPerPart
			lastPartBlocks := int(numBlocks) - totalBlocks
			if lastPartBlocks <= 0 {
				t.Errorf("last partition has %d blocks (should be > 0)", lastPartBlocks)
			}
			if lastPartBlocks > u.blocksPerPart {
				t.Errorf("last partition has %d blocks > blocksPerPart=%d", lastPartBlocks, u.blocksPerPart)
			}

			t.Logf("N=%d p=%d fp=%d: P=%d blocksPerPart=%d numBlocks=%d",
				tc.numKeys, tc.payloadSize, tc.fpSize, u.numPartitions, u.blocksPerPart, numBlocks)
		})
	}

	// Verify larger payload -> more partitions (monotonicity)
	t.Run("payload_monotonicity", func(t *testing.T) {
		numKeys := uint64(50_000_000)
		var lastP int
		for _, ps := range []int{0, 4, 8} {
			numBlocks, err := numBlocksForAlgo(AlgoBijection, numKeys, ps, 0)
			if err != nil {
				t.Fatalf("numBlocksForAlgo: %v", err)
			}
			cfg := &buildConfig{
				totalKeys:       numKeys,
				payloadSize:     ps,
				unsortedTempDir: t.TempDir(),
			}
			u, err := newUnsortedBuffer(cfg, numBlocks)
			if err != nil {
				t.Fatalf("newUnsortedBuffer: %v", err)
			}
			t.Logf("payload=%d: P=%d", ps, u.numPartitions)
			if lastP > 0 && u.numPartitions < lastP {
				t.Errorf("payload=%d: P=%d decreased from P=%d (larger payload should mean more partitions)",
					ps, u.numPartitions, lastP)
			}
			lastP = u.numPartitions
			u.cleanup()
		}
	})
}

// TestUnsortedBuffer_FileCountMatchesP verifies that the number of open
// partition file descriptors matches the partition count P.
// Partition fds are opened eagerly during newUnsortedBuffer (persistent fds
// + unlink-after-open design), so they exist before any flush.
func TestUnsortedBuffer_FileCountMatchesP(t *testing.T) {
	numKeys := uint64(500_000)
	numBlocks, err := numBlocksForAlgo(AlgoBijection, numKeys, 4, 0)
	if err != nil {
		t.Fatalf("numBlocksForAlgo: %v", err)
	}

	cfg := &buildConfig{
		totalKeys:       numKeys,
		payloadSize:     4,
		unsortedTempDir: t.TempDir(),
	}
	u, err := newUnsortedBuffer(cfg, numBlocks)
	if err != nil {
		t.Fatalf("newUnsortedBuffer: %v", err)
	}
	defer u.cleanup()

	// Partition fds are opened eagerly — all P fds should exist immediately
	nonNil := 0
	for _, f := range u.partFiles {
		if f != nil {
			nonNil++
		}
	}
	if nonNil != u.numPartitions {
		t.Errorf("open fds=%d after newUnsortedBuffer, want numPartitions=%d", nonNil, u.numPartitions)
	}

	// Add enough entries to trigger a flush
	rng := newTestRNG(t)
	for i := 0; i < u.bufferCap+1; i++ {
		key := make([]byte, 16)
		fillFromRNG(rng, key)
		k0 := binary.LittleEndian.Uint64(key[0:8])
		k1 := binary.LittleEndian.Uint64(key[8:16])
		prefix := extractPrefix(key)
		blockID := blockIndexFromPrefix(prefix, numBlocks)
		if u.addKey(k0, k1, uint64(i), blockID) {
			if err := u.flush(); err != nil {
				t.Fatalf("flush: %v", err)
			}
		}
	}

	if !u.flushed {
		t.Fatal("expected flushed=true after adding bufferCap+1 entries")
	}

	// After flushing: file count should still match P
	u.flushWg.Wait()
	nonNil = 0
	for _, f := range u.partFiles {
		if f != nil {
			nonNil++
		}
	}
	if nonNil != u.numPartitions {
		t.Errorf("open fds=%d after flush, want numPartitions=%d", nonNil, u.numPartitions)
	}

	t.Logf("P=%d, confirmed %d open fds", u.numPartitions, nonNil)
}

// TestUnsortedBuffer_FastPathNoFlush verifies that in fast path mode
// (no flushing), flushed remains false and partition files have zero data.
// Partition fds are opened eagerly (persistent fds + unlink-after-open)
// but no data is written in fast path mode.
func TestUnsortedBuffer_FastPathNoFlush(t *testing.T) {
	numKeys := uint64(50)
	numBlocks, err := numBlocksForAlgo(AlgoBijection, numKeys, 0, 0)
	if err != nil {
		t.Fatalf("numBlocksForAlgo: %v", err)
	}

	cfg := &buildConfig{
		totalKeys:       numKeys,
		unsortedTempDir: t.TempDir(),
	}
	u, err := newUnsortedBuffer(cfg, numBlocks)
	if err != nil {
		t.Fatalf("newUnsortedBuffer: %v", err)
	}
	defer u.cleanup()

	// Add keys (fewer than bufferCap, so no flush)
	rng := newTestRNG(t)
	for i := range int(numKeys) {
		key := make([]byte, 16)
		fillFromRNG(rng, key)
		k0 := binary.LittleEndian.Uint64(key[0:8])
		k1 := binary.LittleEndian.Uint64(key[8:16])
		prefix := extractPrefix(key)
		blockID := blockIndexFromPrefix(prefix, numBlocks)
		if u.addKey(k0, k1, uint64(i), blockID) {
			t.Fatal("addKey returned true (flush needed) for small dataset")
		}
	}

	if u.flushed {
		t.Error("expected flushed=false for small dataset")
	}

	// Partition fds are opened eagerly but should have zero data written
	for i, f := range u.partFiles {
		if f == nil {
			continue
		}
		fi, err := f.Stat()
		if err != nil {
			t.Errorf("partition %d: stat failed: %v", i, err)
			continue
		}
		if fi.Size() != 0 {
			t.Errorf("partition %d: expected zero-length file in fast path, got %d bytes", i, fi.Size())
		}
	}
}

// =============================================================================
// Crash safety tests
// =============================================================================

// TestUnsortedBuffer_PartitionFilesUnlinked verifies that partition files are
// unlinked from the filesystem immediately after creation (crash-safe design).
// The files should still be readable via their persistent fds.
func TestUnsortedBuffer_PartitionFilesUnlinked(t *testing.T) {
	numKeys := uint64(500_000)
	numBlocks, err := numBlocksForAlgo(AlgoBijection, numKeys, 0, 0)
	if err != nil {
		t.Fatalf("numBlocksForAlgo: %v", err)
	}

	cfg := &buildConfig{
		totalKeys:       numKeys,
		unsortedTempDir: t.TempDir(),
	}
	u, err := newUnsortedBuffer(cfg, numBlocks)
	if err != nil {
		t.Fatalf("newUnsortedBuffer: %v", err)
	}
	defer u.cleanup()

	// Trigger a flush to create partition files
	rng := newTestRNG(t)
	for i := 0; i < u.bufferCap+1; i++ {
		key := make([]byte, 16)
		fillFromRNG(rng, key)
		k0 := binary.LittleEndian.Uint64(key[0:8])
		k1 := binary.LittleEndian.Uint64(key[8:16])
		prefix := extractPrefix(key)
		blockID := blockIndexFromPrefix(prefix, numBlocks)
		if u.addKey(k0, k1, uint64(i), blockID) {
			if err := u.flush(); err != nil {
				t.Fatalf("flush: %v", err)
			}
		}
	}
	u.flushWg.Wait()

	if !u.flushed {
		t.Fatal("expected flushed=true")
	}

	// Partition directory should exist but be empty (files unlinked)
	if u.partDir == "" {
		t.Fatal("expected partDir to be set")
	}
	entries, err := os.ReadDir(u.partDir)
	if err != nil {
		t.Fatalf("ReadDir(%s): %v", u.partDir, err)
	}
	if len(entries) != 0 {
		t.Errorf("expected empty partDir (files unlinked), found %d entries", len(entries))
		for _, e := range entries {
			t.Logf("  found: %s", e.Name())
		}
	}

	// But the fds should still be valid — verify by stat'ing them
	for i, f := range u.partFiles {
		if f == nil {
			continue
		}
		fi, err := f.Stat()
		if err != nil {
			t.Errorf("partition %d: Stat() on unlinked fd failed: %v", i, err)
			continue
		}
		// File exists via fd even though directory entry is gone
		if fi.Size() < 0 {
			t.Errorf("partition %d: unexpected negative size", i)
		}
	}
}

// TestUnsortedBuffer_InterruptedBuildCleanup verifies that cancelling the context
// mid-build properly cleans up all resources (fds closed, temp dir removed).
func TestUnsortedBuffer_InterruptedBuildCleanup(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tmpDir := t.TempDir()
	output := tmpDir + "/interrupted.idx"
	numKeys := uint64(500_000)

	builder, err := NewBuilder(ctx, output, numKeys,
		WithUnsortedInput(TempDir(tmpDir)),
		WithPayload(4),
	)
	if err != nil {
		t.Fatalf("NewBuilder: %v", err)
	}

	// Add some keys to trigger flushing
	rng := newTestRNG(t)
	added := 0
	for added < 200_000 {
		key := make([]byte, 16)
		fillFromRNG(rng, key)
		hashed := PreHash(key)
		if err := builder.AddKey(hashed, uint64(added)); err != nil {
			t.Fatalf("AddKey: %v", err)
		}
		added++
	}

	// Cancel mid-build
	cancel()

	// Close should clean up everything
	if err := builder.Close(); err != nil {
		t.Logf("Close returned error (expected): %v", err)
	}

	// Output file should be removed
	if _, err := os.Stat(output); !os.IsNotExist(err) {
		t.Errorf("output file should be removed after Close, got err: %v", err)
	}

	// The unsortedBuf's partDir (if created) should be cleaned up.
	// We can't easily access it after Close since the builder nils it,
	// but we can verify no unexpected files remain in tmpDir.
	entries, err := os.ReadDir(tmpDir)
	if err != nil {
		t.Fatalf("ReadDir(%s): %v", tmpDir, err)
	}
	for _, e := range entries {
		// tmpDir itself is managed by t.TempDir(), but our partDir subdirectory
		// should have been removed by cleanup.
		if e.IsDir() {
			t.Errorf("unexpected directory remaining in tmpDir: %s", e.Name())
		}
	}
}

// =============================================================================
// Heap measurement tests
// =============================================================================

// measureHeapAlloc runs GC and returns the current heap allocation in bytes.
func measureHeapAlloc() uint64 {
	runtime.GC()
	runtime.GC() // Second GC to sweep finalizers
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return m.HeapAlloc
}

// addKeysAndFlush adds numKeys random entries to the unsorted buffer,
// flushing when full. Returns the number of flushes triggered.
func addKeysAndFlush(t *testing.T, u *unsortedBuffer, numKeys int, numBlocks uint32) int {
	t.Helper()
	rng := newTestRNG(t)
	flushCount := 0
	for i := range numKeys {
		key := make([]byte, 16)
		fillFromRNG(rng, key)
		k0 := binary.LittleEndian.Uint64(key[0:8])
		k1 := binary.LittleEndian.Uint64(key[8:16])
		prefix := extractPrefix(key)
		blockID := blockIndexFromPrefix(prefix, numBlocks)
		if u.addKey(k0, k1, uint64(i), blockID) {
			if err := u.flush(); err != nil {
				t.Fatalf("flush: %v", err)
			}
			flushCount++
		}
	}
	return flushCount
}

// TestUnsortedBuffer_WritePhaseMemory verifies that the write phase stays
// within its memory budget. With 4MB flush buffers, write-phase structural
// allocations are: two flat buffers (2 × bufferCap × 24B with headroom),
// two cursor arrays (2 × P × 8B), one encode buffer, and partition fds.
//
// Uses 10M keys (P=3) to exercise multi-partition behavior.
func TestUnsortedBuffer_WritePhaseMemory(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping memory measurement in short mode")
	}

	numKeys := uint64(10_000_000) // P=3 with p=4
	numBlocks, err := numBlocksForAlgo(AlgoPTRHash, numKeys, 4, 0)
	if err != nil {
		t.Fatalf("numBlocksForAlgo: %v", err)
	}

	baselineHeap := measureHeapAlloc()

	cfg := &buildConfig{
		totalKeys:       numKeys,
		payloadSize:     4,
		unsortedTempDir: t.TempDir(),
	}
	u, err := newUnsortedBuffer(cfg, numBlocks)
	if err != nil {
		t.Fatalf("newUnsortedBuffer: %v", err)
	}
	defer u.cleanup()

	if u.numPartitions < 2 {
		t.Fatalf("expected P >= 2 for 10M keys, got P=%d", u.numPartitions)
	}

	flushCount := addKeysAndFlush(t, u, int(numKeys), numBlocks)
	u.flushWg.Wait()

	writePhaseHeap := measureHeapAlloc()
	writePhaseUsage := writePhaseHeap - baselineHeap

	// Write-phase structural allocations with 4MB flush buffer:
	//   bufferCap = 4MB / 24B = ~174K entries
	//   regionCap = bufferCap/P + bufferCap/(P*8) per partition
	//   Two flat buffers: 2 × P × regionCap × 24B ≈ 2 × 4.5MB = ~9MB
	//   Encode buffer: maxRegionEntries × entrySize ≈ ~1.3MB
	//   Cursors, fd overhead, struct: < 1MB
	// Total: ~12MB. Limit set at 25MB to catch 2x regressions.
	const writePhaseLimit = 25 << 20 // 25MB
	t.Logf("write phase: delta=%dMB (baseline=%dMB, after=%dMB), flushes=%d, P=%d",
		writePhaseUsage>>20, baselineHeap>>20, writePhaseHeap>>20, flushCount, u.numPartitions)

	if writePhaseUsage > writePhaseLimit {
		t.Errorf("write phase heap usage %dMB exceeds %dMB limit",
			writePhaseUsage>>20, writePhaseLimit>>20)
	}
}

// TestUnsortedBuffer_ReadPhaseMemory verifies that the read phase stays within
// the 256MB memory budget. Takes a baseline before newUnsortedBuffer, then
// measures after prepareForRead. Since prepareForRead nils write-phase buffers,
// GC collects them, and the resulting delta captures read-phase allocations.
//
// Uses 10M keys (P=3) to exercise multi-partition behavior. Read-phase
// allocations are sized to the largest partition (totalKeys/P entries).
func TestUnsortedBuffer_ReadPhaseMemory(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping memory measurement in short mode")
	}

	numKeys := uint64(10_000_000)
	numBlocks, err := numBlocksForAlgo(AlgoPTRHash, numKeys, 4, 0)
	if err != nil {
		t.Fatalf("numBlocksForAlgo: %v", err)
	}

	baselineHeap := measureHeapAlloc()

	cfg := &buildConfig{
		totalKeys:       numKeys,
		payloadSize:     4,
		unsortedTempDir: t.TempDir(),
	}
	u, err := newUnsortedBuffer(cfg, numBlocks)
	if err != nil {
		t.Fatalf("newUnsortedBuffer: %v", err)
	}
	defer u.cleanup()

	if u.numPartitions < 2 {
		t.Fatalf("expected P >= 2, got P=%d", u.numPartitions)
	}

	addKeysAndFlush(t, u, int(numKeys), numBlocks)

	// prepareForRead flushes any remaining entries (safe even if totalBuffered > 0),
	// nils write-phase buffers, then allocates read-phase buffers.
	if err := u.prepareForRead(); err != nil {
		t.Fatalf("prepareForRead: %v", err)
	}

	// measureHeapAlloc runs GC which collects the nil'd write-phase buffers.
	// The delta from baseline captures read-phase allocations only.
	readPhaseHeap := measureHeapAlloc()
	readPhaseDelta := readPhaseHeap - baselineHeap

	// Read-phase allocations for 10M keys with P=3:
	//   maxPartEntries ≈ 10M/3 ≈ 3.3M
	//   totalRegion ≈ 3.3M × 1.03 ≈ 3.4M (with superblock margin)
	//   2 flatBufs: 2 × 3.4M × 24B ≈ 163MB
	//   2 blockIDs: 2 × 3.4M × 4B  ≈ 27MB
	//   scratch + readBuf + sbRegionOffsets ≈ ~5-10MB
	//   Total ≈ ~200MB, within 280MB budget.
	//   280MB limit accounts for Go allocator overhead (page alignment,
	//   size class rounding, ~5% above theoretical).
	const readPhaseLimit = 280 << 20 // 280MB
	t.Logf("read phase: delta=%dMB (baseline=%dMB, after=%dMB), P=%d, numBlocks=%d",
		readPhaseDelta>>20, baselineHeap>>20, readPhaseHeap>>20, u.numPartitions, numBlocks)

	if readPhaseDelta > readPhaseLimit {
		t.Errorf("read phase heap delta %dMB exceeds %dMB limit",
			readPhaseDelta>>20, readPhaseLimit>>20)
	}

	// Also verify reads don't leak — heap should stay flat across partition reads.
	for p := range u.numPartitions {
		_, err := u.readPartition(p, p%2, u.readFlatBufs[p%2], u.readBlockIDs[p%2])
		if err != nil {
			t.Fatalf("readPartition(%d): %v", p, err)
		}
	}

	readPhaseAfterReads := measureHeapAlloc()
	readsDelta := int64(readPhaseAfterReads) - int64(readPhaseHeap)
	t.Logf("after reading all partitions: heap=%dMB, delta from prepareForRead=%+dMB",
		readPhaseAfterReads>>20, readsDelta>>20)

	// Reading partitions should not grow heap significantly (buffers are reused).
	// Allow 10MB for minor allocations (readBuf, sbRegionOffsets reallocations).
	if readsDelta > 10<<20 {
		t.Errorf("reading partitions grew heap by %dMB (expected < 10MB)", readsDelta>>20)
	}
}

// TestUnsortedBuffer_MemoryScaling verifies that write-phase memory stays flat
// as key count increases, since the flush buffer is fixed-size (4MB).
//
// Uses key counts that produce different partition counts (P=1 to P=5)
// to verify that per-partition overhead doesn't dominate.
func TestUnsortedBuffer_MemoryScaling(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping memory scaling test in short mode")
	}

	// Key counts chosen to produce increasing P values:
	// 2M -> P=1, 5M -> P=2, 10M -> P=3, 20M -> P=5
	keyCounts := []uint64{2_000_000, 5_000_000, 10_000_000, 20_000_000}
	type measurement struct {
		numKeys    uint64
		writeHeap  uint64
		partitions int
	}
	var measurements []measurement

	for _, numKeys := range keyCounts {
		t.Run(fmt.Sprintf("N=%dM", numKeys/1_000_000), func(t *testing.T) {
			numBlocks, err := numBlocksForAlgo(AlgoPTRHash, numKeys, 4, 0)
			if err != nil {
				t.Fatalf("numBlocksForAlgo: %v", err)
			}

			baselineHeap := measureHeapAlloc()

			cfg := &buildConfig{
				totalKeys:       numKeys,
				payloadSize:     4,
				unsortedTempDir: t.TempDir(),
			}
			u, err := newUnsortedBuffer(cfg, numBlocks)
			if err != nil {
				t.Fatalf("newUnsortedBuffer: %v", err)
			}

			flushCount := addKeysAndFlush(t, u, int(numKeys), numBlocks)
			u.flushWg.Wait()

			writeHeap := measureHeapAlloc() - baselineHeap
			t.Logf("N=%dM: write_heap=%dMB P=%d flushes=%d",
				numKeys/1_000_000, writeHeap>>20, u.numPartitions, flushCount)

			measurements = append(measurements, measurement{numKeys, writeHeap, u.numPartitions})

			u.cleanup()
		})
	}

	if len(measurements) < 2 {
		return
	}

	// Verify partition counts are actually different (test validity check)
	first := measurements[0]
	last := measurements[len(measurements)-1]
	if first.partitions == last.partitions {
		t.Logf("warning: P=%d for both N=%dM and N=%dM — scaling test is less meaningful",
			first.partitions, first.numKeys/1_000_000, last.numKeys/1_000_000)
	}

	keyRatio := float64(last.numKeys) / float64(first.numKeys)
	heapRatio := float64(last.writeHeap) / float64(first.writeHeap)
	t.Logf("scaling: N ratio=%.0fx (P: %d->%d), heap ratio=%.1fx",
		keyRatio, first.partitions, last.partitions, heapRatio)

	// Write-phase memory is dominated by the fixed-size flush buffer (4MB),
	// not by key count or partition count. With 10x key increase (2M->20M)
	// and P going from 1 to 5, heap should grow at most 2x (for the slightly
	// larger cursor arrays and encode buffer).
	if heapRatio > 2.0 {
		t.Errorf("write-phase heap scaled %.1fx for %.0fx key increase (P: %d->%d) — expected < 2x",
			heapRatio, keyRatio, first.partitions, last.partitions)
	}
}

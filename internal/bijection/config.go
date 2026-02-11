// Package bijection implements the Bijection algorithm for StreamHash.
//
// This algorithm uses EF/GR encoding with 128-bucket checkpoint stride for queries.
// It provides good space efficiency with moderate query latency.
package bijection

import "math"

// Algorithm constants
const (
	// lambda is the average keys per bucket.
	lambda = 3.0

	// bucketsPerBlock is the number of buckets per block.
	bucketsPerBlock = 1024

	// blockBits is log2(bucketsPerBlock), used for compact fallback entry encoding.
	blockBits = 10

	// checkpointInterval is how often checkpoints are stored in metadata.
	checkpointInterval = 128

	// numCheckpoints is the number of checkpoint entries stored per block.
	// Derived from bucketsPerBlock / checkpointInterval - 1 (the first
	// segment needs no checkpoint since it starts at offset 0).
	numCheckpoints = bucketsPerBlock/checkpointInterval - 1

	// checkpointsSize is the size of the checkpoints section in bytes.
	// numCheckpoints × 2 bytes (uint16) for EF + numCheckpoints × 2 bytes for seeds.
	checkpointsSize = numCheckpoints * 2 * 2

	// splitThreshold is the bucket size above which splitting is used.
	splitThreshold = 8

	// fallbackMarker indicates a seed that needs fallback lookup.
	fallbackMarker = uint32(0xFFFFFFFF)

	// initialSlotCapacity is the initial capacity for the slot-tracking array
	// used during seed search. Resized dynamically if a bucket exceeds this.
	initialSlotCapacity = 256

	// blockOverflowSigmas controls the maximum keys-per-block safety margin.
	// The block builder uses uint16 for cumulative bucket counts. We set the
	// operational limit at expectedAvg + K×σ (Poisson model). 7 sigmas
	// matches the unsorted mode's region margin (~10^-12 per block).
	blockOverflowSigmas = 7.0
)

// numBlocks returns the number of blocks for a given key count.
func numBlocks(totalKeys uint64) uint32 {
	totalBuckets := uint64(math.Ceil(float64(totalKeys) / lambda))
	if totalBuckets == 0 {
		totalBuckets = 1
	}

	nb := uint32((totalBuckets + uint64(bucketsPerBlock) - 1) / uint64(bucketsPerBlock))
	// Minimum 2 blocks ensures the framework's block-routing (FastRange32)
	// has meaningful distribution.
	if nb < 2 {
		nb = 2
	}
	return nb
}

// estimateMetadataSize returns a safe upper bound on metadata size for a block.
// The actual size after building will be <= this estimate.
//
// SAFETY ANALYSIS - why these bounds are guaranteed sufficient:
//
// 1. Checkpoints: exactly 28 bytes (7 × uint16 for EF + 7 × uint16 for seeds)
//
//  2. Elias-Fano encoding (cumulative bucket sizes):
//     Formula: totalBits = n + (U >> lowBits) + n × lowBits
//     where n = numBuckets (1024), U = numKeys, lowBits = max(0, log2(U/n) - 1)
//
//     For any numKeys ≤ 65535 (uint16 max, since cumulative is uint16):
//     - lowBits ≤ log2(65535/1024) ≈ 6
//     - highBits = n + (U >> lowBits) ≤ 1024 + 1024 = 2048 bits
//     - lowBits total = n × 6 = 6144 bits
//     - Max total = 8192 bits = 1024 bytes
//     Bound: 1024 bytes (sufficient for any valid numKeys)
//
//  3. Seed stream (Golomb-Rice encoded):
//     Each bucket emits either:
//     - Nothing (size ≤ 1)
//     - Golomb-Rice code: q unary bits + k remainder bits, where q ≤ 15 and k ≤ 8 → max 24 bits
//     - Fallback marker: 16 ones (for size > 8 or large seeds)
//     Split buckets (size ≥ 8) emit TWO codes.
//
//     Worst case: all 1024 buckets are split, each emitting 2 × 24 bits = 48 bits
//     Max total = 1024 × 48 = 49152 bits = 6144 bytes
//     Bound: 6144 bytes (sufficient for pathological case)
//
// 4. Fallback list:
//   - 1 byte count + 1 byte validation XOR (max 255 entries)
//   - Each entry: 4 bytes (compactFallbackEntrySize)
//     Max total = 2 + 255 × 4 = 1022 bytes
//     Bound: 1024 bytes (rounded up)
//
// Total worst-case: 28 + 1024 + 6144 + 1024 = 8220 bytes
func estimateMetadataSize(numKeys int) int {
	if numKeys == 0 {
		// Empty block: checkpoints + EF high bits (1 bit per bucket) + seed terminator
		// EF with U=0: only high bits needed (1 per bucket), no low bits
		efHighBits := (bucketsPerBlock + 7) / 8 // 128 bytes for 1024 buckets
		return checkpointsSize + efHighBits + 1
	}

	// See SAFETY ANALYSIS above for derivation of these bounds
	const (
		efUpperBound       = 1024 // EF encoding max for cumulative uint16 values
		seedsUpperBound    = 6144 // Golomb-Rice worst case: all split buckets with max codes
		fallbackUpperBound = 1024 // 255 entries × 4 bytes + count byte, rounded
	)
	return checkpointsSize + efUpperBound + seedsUpperBound + fallbackUpperBound
}

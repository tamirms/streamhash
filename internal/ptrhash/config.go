// Package ptrhash implements the PTRHash algorithm for StreamHash.
//
// This algorithm uses 8-bit pilots (0-255) for slot assignment with cuckoo
// hashing for collision resolution.
package ptrhash

import "math"

// Algorithm constants
const (
	// lambda is the average keys per bucket.
	lambda = 3.16

	// alpha is the slot overflow factor.
	alpha = 0.99

	// bucketsPerBlock is the number of buckets per block.
	//
	// This was increased from 3,000 to 10,000 to make builds reliable at
	// extreme scale. The per-block failure probability depends on the largest
	// bucket (bucket-0 under CubicEps, which gets ~1.08% of keys). The failure
	// rate is exponentially sensitive to bucket size, so the tail of the
	// bucket-0 size distribution dominates:
	//
	//   - Mean bucket-0 size: 341 keys (for 31,600 keys/block)
	//   - P(all 256 pilots fail | k=341): ~2.5×10⁻²⁰
	//   - Integrated P(block fails) over bucket-0 tail: ~4.4×10⁻¹²
	//   - At 1T keys (~31.6M blocks): P(any block fails) ≈ 1.4×10⁻⁴
	//
	// Note: ErrIndistinguishableHashes is not retried (only errEvictionLimitExceeded
	// triggers retries). So the 1.4×10⁻⁴ figure is the actual build failure rate
	// at 1T keys — about 1 in 7,200 builds. This is acceptable for production use.
	//
	// This requires the 256 pilots to behave as approximately independent trials,
	// which is achieved by the SplitMix64 finalizer in pilotHash. Without it,
	// the pilots become correlated (only ~180-225 independent trials), and ~1 in
	// 30 builds at 1T keys would fail. See pilot.go.
	bucketsPerBlock = 10000

	// numPilotValues is the total number of pilot values to try (0-255 = 256 values).
	numPilotValues = 256

	// maxGlobalRetries is the maximum attempts with different random seeds.
	maxGlobalRetries = 10

	// blockOverflowSigmas controls the maximum keys-per-block safety margin.
	// The block builder uses uint16 for solver internals (slot indices,
	// cumulative bucket counts). We set the operational limit at
	// expectedAvg + K×σ (Poisson model). 7 sigmas matches the unsorted
	// mode's region margin (~10^-12 per block).
	blockOverflowSigmas = 7.0

	// solverMaxKeysMultiplier provides 20% headroom above expected average
	// to account for statistical variation across blocks. Used by NewBuilder
	// and MaxIndexMetadataSize to keep them in sync.
	solverMaxKeysMultiplier = 1.2

	// bucketPreallocMultiplier is the multiplier applied to lambda for
	// per-bucket pre-allocation. lambda * 3.5 ≈ 99.9th percentile of
	// Poisson(lambda), avoiding most runtime slice growth.
	bucketPreallocMultiplier = 3.5

	// metadataEstimateMargin is a conservative byte margin added to
	// MaxIndexMetadataSize to account for rounding and edge cases in
	// remap table size estimation.
	metadataEstimateMargin = 100
)

// computeNumSlots returns the number of slots for a given key count.
// numSlots = ceil(numKeys / alpha), but always at least numKeys.
func computeNumSlots(numKeys int) uint32 {
	n := uint32(math.Ceil(float64(numKeys) / alpha))
	if n < uint32(numKeys) {
		n = uint32(numKeys)
	}
	return n
}

// numBlocks returns the number of blocks for a given key count.
// This is an internal function called by Builder.
func numBlocks(totalKeys uint64) uint32 {
	totalBuckets := uint64(math.Ceil(float64(totalKeys) / lambda))
	if totalBuckets == 0 {
		totalBuckets = 1
	}

	nb := uint32((totalBuckets + uint64(bucketsPerBlock) - 1) / uint64(bucketsPerBlock))
	// Minimum 2 blocks ensures the prefix-based block routing
	// (blockIndexFromPrefix) always has at least two targets,
	// preventing degenerate single-block behavior.
	if nb < 2 {
		nb = 2
	}
	return nb
}

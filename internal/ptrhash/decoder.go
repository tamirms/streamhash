package ptrhash

import (
	"fmt"

	streamerrors "github.com/tamirms/streamhash/errors"
)

// Decoder handles query-time slot computation for PTRHash blocks.
type Decoder struct {
	globalSeed uint64
}

// NewDecoder creates a new PTRHash block decoder.
// globalConfig is algorithm-specific configuration (currently unused for ptrhash).
// Returns an error if globalConfig is unexpectedly non-empty.
func NewDecoder(globalConfig []byte, globalSeed uint64) (*Decoder, error) {
	if len(globalConfig) != 0 {
		return nil, fmt.Errorf("%w: ptrhash: unexpected non-empty global config (len=%d)", streamerrors.ErrCorruptedIndex, len(globalConfig))
	}
	return &Decoder{
		globalSeed: globalSeed,
	}, nil
}

// QuerySlot computes the slot for a key within a block.
// k0, k1: key representations (k1 is suffix for bucket assignment)
// metadata is the block's encoded metadata.
// keysInBlock is the number of keys in this block.
// Returns the local slot index (0-based within the block).
func (d *Decoder) QuerySlot(k0, k1 uint64, metadata []byte, keysInBlock int) (int, error) {
	// Use k1 (bytes 8-15 as little-endian) as the suffix for bucket assignment.
	// Compute local bucket index using suffix directly with CubicEps distribution.
	// This matches the builder's bucket assignment.
	localBucket := int(cubicEpsBucket(k1, bucketsPerBlock))

	return queryInternalHashes(metadata, bucketsPerBlock, keysInBlock, localBucket, k0, k1, d.globalSeed)
}

// queryInternalHashes is the fast-path query for PTRHash blocks using k0^k1 slot input.
// Returns the local slot within the block.
// Uses k0^k1 for slot computation to achieve 128-bit collision resistance.
func queryInternalHashes(metadataData []byte, numBuckets int, keysInBlock int, localBucket int, k0, k1 uint64, globalSeed uint64) (int, error) {
	numSlots := int(computeNumSlots(keysInBlock))

	if keysInBlock == 0 {
		return 0, streamerrors.ErrNotFound
	}

	// Pilot section: 8 bits per bucket (direct bytes)
	if len(metadataData) < numBuckets {
		return 0, streamerrors.ErrCorruptedIndex
	}

	// Extract pilot for this bucket and compute slot using k0^k1
	pilot := metadataData[localBucket]
	slot := pilotSlotFromHashes(k0, k1, pilot, uint32(numSlots), globalSeed)

	// If slot is in overflow range, use direct remap lookup
	if int(slot) >= keysInBlock {
		remapData := metadataData[numBuckets:]
		slot = lookupRemap(remapData, slot, uint32(keysInBlock))
	}

	return int(slot), nil
}

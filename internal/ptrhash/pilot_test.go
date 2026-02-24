package ptrhash

import (
	"math"
	"slices"
	"testing"
)

// encodeRemapTable is a test helper that wraps the production encodeRemapTableInto.
func encodeRemapTable(table []uint16) []byte {
	buf := make([]byte, remapTableHeaderSize+len(table)*remapTableEntrySize)
	encodeRemapTableInto(table, buf)
	return buf
}

func TestRemapLookup(t *testing.T) {
	// Setup: N=100 keys, S=103 slots (3 overflow slots)
	// table[0]=42 means slot 100 -> 42
	// table[1]=17 means slot 101 -> 17
	// table[2]=89 means slot 102 -> 89
	table := []uint16{42, 17, 89}
	encoded := encodeRemapTable(table)
	numKeys := uint32(100)

	// Test slots in valid range [0, N) - should return unchanged
	for slot := uint32(0); slot < numKeys; slot += 10 {
		result := lookupRemap(encoded, slot, numKeys)
		if result != slot {
			t.Errorf("slot %d: expected %d (unchanged), got %d", slot, slot, result)
		}
	}

	// Test overflow slots [N, S) - should return remapped values
	testCases := []struct {
		slot     uint32
		expected uint32
	}{
		{100, 42}, // table[0]
		{101, 17}, // table[1]
		{102, 89}, // table[2]
	}
	for _, tc := range testCases {
		result := lookupRemap(encoded, tc.slot, numKeys)
		if result != tc.expected {
			t.Errorf("slot %d: expected %d, got %d", tc.slot, tc.expected, result)
		}
	}
}

func TestRemapEmpty(t *testing.T) {
	encoded := encodeRemapTable(nil)
	if len(encoded) != remapTableHeaderSize {
		t.Errorf("expected %d bytes for empty table, got %d", remapTableHeaderSize, len(encoded))
	}

	// With empty table, slots should return unchanged
	result := lookupRemap(encoded, 50, 100)
	if result != 50 {
		t.Errorf("expected slot 50 unchanged, got %d", result)
	}
}

// TestSlotDistributionWithHPZero demonstrates why hp=0 is catastrophic.
// When hp=0, (h^(h>>32))*0 = 0 so ALL keys map to slot 0.
func TestSlotDistributionWithHPZero(t *testing.T) {
	numSlots := uint32(1000)
	numInputs := 10000

	// Generate random-ish inputs
	inputs := make([]uint64, numInputs)
	for i := range inputs {
		inputs[i] = uint64(i) * 0x123456789abcdef0
	}

	// Test with hp=0 (BAD - this is what happens without | 1)
	slotsWithHPZero := make(map[uint32]int)
	for _, h := range inputs {
		slot := pilotSlotFolded(h^(h>>32), 0, numSlots)
		slotsWithHPZero[slot]++
	}

	// Test with hp=1 (what | 1 produces when pilot^seed=0)
	slotsWithHP1 := make(map[uint32]int)
	for _, h := range inputs {
		slot := pilotSlotFolded(h^(h>>32), 1, numSlots)
		slotsWithHP1[slot]++
	}

	// Test with a normal hp value
	normalHP := pilotHash(42, 12345)
	slotsWithNormalHP := make(map[uint32]int)
	for _, h := range inputs {
		slot := pilotSlotFolded(h^(h>>32), normalHP, numSlots)
		slotsWithNormalHP[slot]++
	}

	uniqueWithHPZero := len(slotsWithHPZero)
	uniqueWithHP1 := len(slotsWithHP1)
	uniqueWithNormalHP := len(slotsWithNormalHP)

	// hp=0 maps everything to slot 0 (only 1 unique slot),
	// while hp=1 and normal hp produce ~1000 (limited by numSlots).
	if uniqueWithHPZero > uniqueWithHP1/2 {
		t.Errorf("hp=0 didn't show expected degradation: "+
			"uniqueWithHPZero=%d, uniqueWithHP1=%d, uniqueWithNormalHP=%d",
			uniqueWithHPZero, uniqueWithHP1, uniqueWithNormalHP)
	}
	// Normal hp should be at least as good as hp=1
	if uniqueWithNormalHP < uniqueWithHP1/2 {
		t.Errorf("normal hp unexpectedly worse than hp=1: "+
			"uniqueWithNormalHP=%d, uniqueWithHP1=%d",
			uniqueWithNormalHP, uniqueWithHP1)
	}
}

// TestPilotHashAlwaysOddNonZero verifies that pilotHash always returns
// an odd, non-zero value. hp=0 causes ~32% build failure rate (empty slot
// collisions); even hp breaks bijective multiplication on uint64.
func TestPilotHashAlwaysOddNonZero(t *testing.T) {
	rng := newTestRNG(t)
	const iterations = 1000

	// Random global seeds
	for i := range iterations {
		globalSeed := rng.Uint64()
		for pilot := range numPilotValues {
			hp := pilotHash(uint8(pilot), globalSeed)
			if hp == 0 {
				t.Fatalf("iter %d: pilotHash(%d, 0x%X) = 0", i, pilot, globalSeed)
			}
			if hp&1 != 1 {
				t.Fatalf("iter %d: pilotHash(%d, 0x%X) = 0x%X (even)", i, pilot, globalSeed, hp)
			}
		}
	}

	// Explicit critical seeds: 0 and 1 exercise low-entropy inputs to SplitMix64
	for _, seed := range []uint64{0, 1, 0xdeadbeef, math.MaxUint64} {
		for pilot := range numPilotValues {
			hp := pilotHash(uint8(pilot), seed)
			if hp == 0 {
				t.Errorf("pilotHash(%d, 0x%X) = 0", pilot, seed)
			}
			if hp&1 != 1 {
				t.Errorf("pilotHash(%d, 0x%X) = 0x%X (even)", pilot, seed, hp)
			}
		}
	}

	// Matching pilot-and-seed: when pilot value equals seed as uint64,
	// XOR zeroes out low bits — historically fragile.
	for pilot := range numPilotValues {
		seed := uint64(pilot)
		hp := pilotHash(uint8(pilot), seed)
		if hp == 0 {
			t.Errorf("pilotHash(%d, %d) = 0", pilot, seed)
		}
		if hp&1 != 1 {
			t.Errorf("pilotHash(%d, %d) = 0x%X (even)", pilot, seed, hp)
		}
	}
}

// TestCubicEpsBucketDistribution verifies that cubicEpsBucket produces the
// expected skewed distribution: bucket 0 gets >0.5% of keys, and no single
// bucket gets >2.0%. This validates the cubic-root skew that gives larger
// buckets to low-index buckets.
func TestCubicEpsBucketDistribution(t *testing.T) {
	numBuckets := uint32(10000)
	n := 100000
	counts := make([]int, numBuckets)
	rng := newTestRNG(t)
	for range n {
		b := cubicEpsBucket(rng.Uint64(), numBuckets)
		counts[b]++
	}

	// CubicEps gives bucket 0 ~1.08% of keys. With 100K samples,
	// expected ~1080. Check >0.5% (>500) with huge margin.
	bucket0Pct := float64(counts[0]) / float64(n) * 100
	if bucket0Pct < 0.5 {
		t.Errorf("bucket 0 got %.2f%% of keys (%d), expected >0.5%%", bucket0Pct, counts[0])
	}

	// No single bucket should dominate excessively
	for i, c := range counts {
		pct := float64(c) / float64(n) * 100
		if pct > 2.0 {
			t.Errorf("bucket %d got %.2f%% of keys (%d), expected <2%%", i, pct, c)
		}
	}
}

// TestCubicEpsBucketMonotonicity verifies that cubicEpsBucket is monotone:
// sorted inputs produce non-decreasing bucket indices.
func TestCubicEpsBucketMonotonicity(t *testing.T) {
	rng := newTestRNG(t)
	const iterations = 1000

	for i := range iterations {
		numBuckets := uint32(rng.Uint32N(math.MaxUint32-1)) + 1

		// Generate 100 sorted x values
		xs := make([]uint64, 100)
		for j := range xs {
			xs[j] = rng.Uint64()
		}
		slices.Sort(xs)

		prev := cubicEpsBucket(xs[0], numBuckets)
		for j := 1; j < len(xs); j++ {
			cur := cubicEpsBucket(xs[j], numBuckets)
			if cur < prev {
				t.Fatalf("iter %d: monotonicity violated: cubicEpsBucket(0x%X, %d)=%d > cubicEpsBucket(0x%X, %d)=%d",
					i, xs[j-1], numBuckets, prev, xs[j], numBuckets, cur)
			}
			prev = cur
		}
	}
}

// TestCubicEpsBucketRange verifies that the result is always in [0, numBuckets).
func TestCubicEpsBucketRange(t *testing.T) {
	rng := newTestRNG(t)
	const iterations = 10000

	for i := range iterations {
		numBuckets := uint32(rng.Uint32N(math.MaxUint32-1)) + 1
		x := rng.Uint64()

		got := cubicEpsBucket(x, numBuckets)
		if got >= numBuckets {
			t.Fatalf("iter %d: cubicEpsBucket(0x%X, %d) = %d >= numBuckets",
				i, x, numBuckets, got)
		}
	}
}

// TestCubicEpsBucketEdgeCases tests deterministic edge cases.
func TestCubicEpsBucketEdgeCases(t *testing.T) {
	// numBuckets=0 -> 0
	if got := cubicEpsBucket(42, 0); got != 0 {
		t.Errorf("cubicEpsBucket(42, 0) = %d, want 0", got)
	}

	// numBuckets=1 -> 0
	for _, x := range []uint64{0, 1, math.MaxUint64, 0xDEADBEEF} {
		if got := cubicEpsBucket(x, 1); got != 0 {
			t.Errorf("cubicEpsBucket(0x%X, 1) = %d, want 0", x, got)
		}
	}

	// x=0 -> 0
	for n := uint32(1); n <= 100; n++ {
		if got := cubicEpsBucket(0, n); got != 0 {
			t.Errorf("cubicEpsBucket(0, %d) = %d, want 0", n, got)
		}
	}

	// x=MaxUint64 -> numBuckets-1
	for n := uint32(2); n <= 100; n++ {
		got := cubicEpsBucket(math.MaxUint64, n)
		if got != n-1 {
			t.Errorf("cubicEpsBucket(MaxUint64, %d) = %d, want %d", n, got, n-1)
		}
	}
}

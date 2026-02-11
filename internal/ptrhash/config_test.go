package ptrhash

import (
	"math"
	"testing"
)

// TestComputeNumSlots verifies monotonicity, lower bound, and upper bound.
func TestComputeNumSlots(t *testing.T) {
	prev := computeNumSlots(1)

	for n := 1; n <= 10000; n++ {
		got := computeNumSlots(n)

		// LowerBound: computeNumSlots(n) >= n
		if got < uint32(n) {
			t.Errorf("computeNumSlots(%d) = %d < %d", n, got, n)
		}

		// UpperBound: computeNumSlots(n) <= ceil(n/0.99) + 1
		upperBound := uint32(math.Ceil(float64(n)/alpha)) + 1
		if got > upperBound {
			t.Errorf("computeNumSlots(%d) = %d > %d (ceil(%d/0.99)+1)", n, got, upperBound, n)
		}

		// Monotonicity: computeNumSlots(n+1) >= computeNumSlots(n)
		if n > 1 && got < prev {
			t.Errorf("monotonicity: computeNumSlots(%d)=%d < computeNumSlots(%d)=%d",
				n, got, n-1, prev)
		}
		prev = got
	}
}

// TestComputeNumSlotsPinnedValues verifies computeNumSlots returns specific
// expected values. These are pinned to detect any accidental formula changes.
func TestComputeNumSlotsPinnedValues(t *testing.T) {
	cases := []struct {
		n    int
		want uint32
	}{
		{1, 2},
		{10, 11},
		{100, 102},
		{1000, 1011},
		{10000, 10102},
	}
	for _, tc := range cases {
		got := computeNumSlots(tc.n)
		if got != tc.want {
			t.Errorf("computeNumSlots(%d) = %d, want %d", tc.n, got, tc.want)
		}
	}
}

// TestNumBlocksMonotonicity verifies that numBlocks is monotone.
// ptrhash has independent numBlocks with lambda=3.16, bucketsPerBlock=10000
// (vs bijection lambda=3.0, bucketsPerBlock=1024).
// Mirrored in internal/bijection/builder_test.go with bijection constants.
func TestNumBlocksMonotonicity(t *testing.T) {
	prev := numBlocks(1)
	for n := uint64(2); n <= 50000; n++ {
		got := numBlocks(n)
		if got < prev {
			t.Fatalf("monotonicity: numBlocks(%d)=%d < numBlocks(%d)=%d",
				n, got, n-1, prev)
		}
		prev = got
	}
}

// TestNumBlocksPinnedValues verifies numBlocks returns specific expected values.
// These are pinned to detect accidental formula changes.
// ptrhash: lambda=3.16, bucketsPerBlock=10000, minimum 2 blocks.
// Mirrored in internal/bijection/builder_test.go with bijection constants.
func TestNumBlocksPinnedValues(t *testing.T) {
	cases := []struct {
		n    uint64
		want uint32
	}{
		{1, 2},
		{100, 2},
		{1000, 2},
		{10000, 2},
		{100000, 4},
		{1000000, 32},
	}
	for _, tc := range cases {
		got := numBlocks(tc.n)
		if got != tc.want {
			t.Errorf("numBlocks(%d) = %d, want %d", tc.n, got, tc.want)
		}
	}
}

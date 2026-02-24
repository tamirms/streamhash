package ptrhash

import (
	"testing"
)

// ─── bucketHeap tests ─────────────────────────────────────────────────

// TestBucketHeapPopOrder tests named deterministic cases for max-heap ordering
// with ascending-index tie-breaking.
func TestBucketHeapPopOrder(t *testing.T) {
	type entry struct {
		idx  int
		size int
	}
	tests := []struct {
		name   string
		input  []entry
		expect []entry // expected pop order
	}{
		{
			name:   "distinct_sizes",
			input:  []entry{{0, 3}, {1, 7}, {2, 1}, {3, 5}},
			expect: []entry{{1, 7}, {3, 5}, {0, 3}, {2, 1}},
		},
		{
			name:   "all_same_size",
			input:  []entry{{0, 4}, {1, 4}, {2, 4}, {3, 4}, {4, 4}},
			expect: []entry{{0, 4}, {1, 4}, {2, 4}, {3, 4}, {4, 4}},
		},
		{
			name: "ties_mixed",
			input: []entry{
				{5, 10}, {2, 10}, {7, 3}, {0, 3}, {9, 10}, {4, 3},
			},
			expect: []entry{
				{2, 10}, {5, 10}, {9, 10},
				{0, 3}, {4, 3}, {7, 3},
			},
		},
		{
			name:   "descending_input",
			input:  []entry{{0, 8}, {1, 6}, {2, 4}, {3, 2}},
			expect: []entry{{0, 8}, {1, 6}, {2, 4}, {3, 2}},
		},
		{
			name:   "ascending_input",
			input:  []entry{{0, 2}, {1, 4}, {2, 6}, {3, 8}},
			expect: []entry{{3, 8}, {2, 6}, {1, 4}, {0, 2}},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			h := newBucketHeap(len(tc.input))
			for _, e := range tc.input {
				h.push(e.idx, e.size)
			}
			for i, want := range tc.expect {
				idx, size := h.pop()
				if idx != want.idx || size != want.size {
					t.Fatalf("pop[%d] = (idx=%d, size=%d), want (idx=%d, size=%d)",
						i, idx, size, want.idx, want.size)
				}
			}
			if h.len() != 0 {
				t.Fatalf("heap not empty after draining: len=%d", h.len())
			}
		})
	}
}

func TestBucketHeapClearAndReuse(t *testing.T) {
	h := newBucketHeap(16)
	for i := range 10 {
		h.push(i, i+1)
	}
	if h.len() != 10 {
		t.Fatalf("len after first push = %d, want 10", h.len())
	}

	h.clear()
	if h.len() != 0 {
		t.Fatalf("len after clear = %d, want 0", h.len())
	}

	// Push 5 new elements with distinct values.
	type entry struct{ idx, size int }
	input := []entry{{100, 5}, {101, 2}, {102, 9}, {103, 2}, {104, 7}}
	for _, e := range input {
		h.push(e.idx, e.size)
	}

	expected := []entry{{102, 9}, {104, 7}, {100, 5}, {101, 2}, {103, 2}}
	for i, want := range expected {
		idx, size := h.pop()
		if idx != want.idx || size != want.size {
			t.Fatalf("pop[%d] = (idx=%d, size=%d), want (idx=%d, size=%d)",
				i, idx, size, want.idx, want.size)
		}
	}
	if h.len() != 0 {
		t.Fatalf("heap not empty after draining: len=%d", h.len())
	}
}

func TestBucketHeapPushDuringDrain(t *testing.T) {
	h := newBucketHeap(8)

	// Initial push.
	h.push(0, 10)
	h.push(1, 5)
	h.push(2, 3)

	// Pop max -> expect (0, 10).
	idx, size := h.pop()
	if idx != 0 || size != 10 {
		t.Fatalf("first pop = (idx=%d, size=%d), want (idx=0, size=10)", idx, size)
	}

	// Simulate eviction: push new elements.
	h.push(3, 7)
	h.push(4, 2)

	// Drain remaining — expect correct max-ordering.
	type entry struct{ idx, size int }
	expected := []entry{{3, 7}, {1, 5}, {2, 3}, {4, 2}}
	for i, want := range expected {
		idx, size := h.pop()
		if idx != want.idx || size != want.size {
			t.Fatalf("pop[%d] = (idx=%d, size=%d), want (idx=%d, size=%d)",
				i, idx, size, want.idx, want.size)
		}
	}
	if h.len() != 0 {
		t.Fatalf("heap not empty after draining: len=%d", h.len())
	}
}

// buildBucketStarts converts bucket sizes to cumulative bucket starts.
func buildBucketStarts(sizes []int) []uint16 {
	starts := make([]uint16, len(sizes)+1)
	for i, s := range sizes {
		starts[i+1] = starts[i] + uint16(s)
	}
	return starts
}

// TestCountingSortOrder tests named deterministic cases for counting sort output order.
func TestCountingSortOrder(t *testing.T) {
	tests := []struct {
		name   string
		sizes  []int
		expect []uint16 // expected bucket indices in sorted order
	}{
		{
			name:   "distinct_sizes",
			sizes:  []int{3, 1, 4, 1, 5},
			expect: []uint16{4, 2, 0, 1, 3},
		},
		{
			name:   "all_same_size",
			sizes:  []int{3, 3, 3, 3, 3},
			expect: []uint16{0, 1, 2, 3, 4},
		},
		{
			name:   "all_empty",
			sizes:  []int{0, 0, 0, 0},
			expect: []uint16{0, 1, 2, 3},
		},
		{
			name:   "mixed_with_empty",
			sizes:  []int{0, 5, 0, 3, 0},
			expect: []uint16{1, 3, 0, 2, 4},
		},
		{
			name:   "single_bucket",
			sizes:  []int{7},
			expect: []uint16{0},
		},
		{
			name:   "descending_sizes",
			sizes:  []int{5, 4, 3, 2, 1},
			expect: []uint16{0, 1, 2, 3, 4},
		},
		{
			name:   "ascending_sizes",
			sizes:  []int{1, 2, 3, 4, 5},
			expect: []uint16{4, 3, 2, 1, 0},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			bucketStarts := buildBucketStarts(tc.sizes)
			n := len(tc.sizes)

			maxSize := 0
			for _, s := range tc.sizes {
				if s > maxSize {
					maxSize = s
				}
			}

			result := make([]uint16, n)
			counts := make([]int, maxSize+1)
			positions := make([]int, maxSize+1)

			countingSortBucketsInto(bucketStarts, result, counts, positions)

			for i := range n {
				if result[i] != tc.expect[i] {
					t.Fatalf("result[%d] = %d, want %d (full result: %v, expect: %v)",
						i, result[i], tc.expect[i], result[:n], tc.expect)
				}
			}
		})
	}
}

// TestCountingSortBucketsInto verifies sorting correctness with random data.
func TestCountingSortBucketsInto(t *testing.T) {
	rng := newTestRNG(t)
	const iterations = 1000

	for i := range iterations {
		numBuckets := 10 + int(rng.IntN(9991)) // 10-10000
		bucketStarts := make([]uint16, numBuckets+1)
		total := uint16(0)
		for j := range numBuckets {
			size := uint16(rng.IntN(16)) // 0-15
			bucketStarts[j] = total
			total += size // may wrap around uint16 — that's OK
		}
		bucketStarts[numBuckets] = total

		result := make([]uint16, numBuckets)
		counts := make([]int, 256)
		positions := make([]int, 256)
		countingSortBucketsInto(bucketStarts, result, counts, positions)

		// Verify: output is a permutation of [0, numBuckets)
		seen := make(map[uint16]bool)
		for _, idx := range result {
			if int(idx) >= numBuckets {
				t.Fatalf("iter %d: bucket index %d >= numBuckets %d", i, idx, numBuckets)
			}
			if seen[idx] {
				t.Fatalf("iter %d: duplicate bucket index %d", i, idx)
			}
			seen[idx] = true
		}
		if len(seen) != numBuckets {
			t.Fatalf("iter %d: got %d unique indices, want %d", i, len(seen), numBuckets)
		}

		// Verify: non-increasing by size.
		// Use uint16 subtraction (matching production heap.go) so modular
		// arithmetic gives the correct positive size even when cumulative
		// counts wrap around uint16.
		getSize := func(idx uint16) int {
			return int(bucketStarts[idx+1] - bucketStarts[idx])
		}
		for j := 1; j < numBuckets; j++ {
			if getSize(result[j]) > getSize(result[j-1]) {
				t.Fatalf("iter %d: not non-increasing at position %d: size[%d]=%d > size[%d]=%d",
					i, j, result[j], getSize(result[j]), result[j-1], getSize(result[j-1]))
			}
		}

		// Verify: within same-size groups, indices ascending (stability)
		for j := 1; j < numBuckets; j++ {
			if getSize(result[j]) == getSize(result[j-1]) && result[j] < result[j-1] {
				t.Fatalf("iter %d: not stable at position %d: same size %d, index %d < %d",
					i, j, getSize(result[j]), result[j], result[j-1])
			}
		}
	}
}

// TestBucketHeapConservation verifies that the heap conserves elements
// and returns them in the correct order.
func TestBucketHeapConservation(t *testing.T) {
	rng := newTestRNG(t)
	const iterations = 1000

	for i := range iterations {
		n := 1 + int(rng.IntN(100))
		h := newBucketHeap(n)

		// Push random (index, size) pairs
		type entry struct {
			idx, size int
		}
		pushed := make([]entry, n)
		for j := range n {
			idx := j
			size := int(rng.IntN(20))
			pushed[j] = entry{idx, size}
			h.push(idx, size)
		}

		if h.len() != n {
			t.Fatalf("iter %d: heap len = %d, want %d", i, h.len(), n)
		}

		// Pop all and verify ordering + conservation
		popped := make([]entry, 0, n)
		for h.len() > 0 {
			idx, size := h.pop()
			popped = append(popped, entry{idx, size})
		}

		if len(popped) != n {
			t.Fatalf("iter %d: popped %d, want %d", i, len(popped), n)
		}

		// Verify ordering: each pop returns max by size desc, index asc
		for j := 1; j < len(popped); j++ {
			prev := popped[j-1]
			cur := popped[j]
			if cur.size > prev.size {
				t.Fatalf("iter %d: not max-heap at %d: size %d > %d", i, j, cur.size, prev.size)
			}
			if cur.size == prev.size && cur.idx < prev.idx {
				t.Fatalf("iter %d: tie-break at %d: idx %d < %d with same size %d",
					i, j, cur.idx, prev.idx, cur.size)
			}
		}

		// Verify multiset conservation
		pushedMap := make(map[int]int) // idx -> size
		for _, e := range pushed {
			pushedMap[e.idx] = e.size
		}
		for _, e := range popped {
			if pushedMap[e.idx] != e.size {
				t.Fatalf("iter %d: conservation violated: pushed idx=%d size=%d, popped size=%d",
					i, e.idx, pushedMap[e.idx], e.size)
			}
			delete(pushedMap, e.idx)
		}
		if len(pushedMap) != 0 {
			t.Fatalf("iter %d: %d elements not popped", i, len(pushedMap))
		}
	}
}

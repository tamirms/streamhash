package ptrhash

// bucketHeap is a max-heap ordered by bucket size.
// Uses index-based heap for O(log n) push/pop.
type bucketHeap struct {
	indices []uint16 // Bucket indices
	sizes   []uint16 // Corresponding sizes (uint16 to handle CubicEps bucket 0 which can exceed 255)
}

func newBucketHeap(capacity int) *bucketHeap {
	return &bucketHeap{
		indices: make([]uint16, 0, capacity),
		sizes:   make([]uint16, 0, capacity),
	}
}

// clear resets the heap for reuse without allocation.
func (h *bucketHeap) clear() {
	h.indices = h.indices[:0]
	h.sizes = h.sizes[:0]
}

func (h *bucketHeap) len() int {
	return len(h.indices)
}

// push adds an element and maintains heap property. O(log n).
func (h *bucketHeap) push(idx int, size int) {
	h.indices = append(h.indices, uint16(idx))
	h.sizes = append(h.sizes, uint16(size))
	h.up(len(h.indices) - 1)
}

func (h *bucketHeap) pop() (int, int) {
	n := len(h.indices) - 1
	h.swap(0, n)
	h.down(0, n)
	idx := h.indices[n]
	size := h.sizes[n]
	h.indices = h.indices[:n]
	h.sizes = h.sizes[:n]
	return int(idx), int(size)
}

func (h *bucketHeap) swap(i, j int) {
	h.indices[i], h.indices[j] = h.indices[j], h.indices[i]
	h.sizes[i], h.sizes[j] = h.sizes[j], h.sizes[i]
}

func (h *bucketHeap) less(i, j int) bool {
	// Max-heap by size
	if h.sizes[i] != h.sizes[j] {
		return h.sizes[i] > h.sizes[j]
	}
	// Deterministic tie-break by index
	return h.indices[i] < h.indices[j]
}

func (h *bucketHeap) up(j int) {
	for {
		i := (j - 1) / 2 // parent
		if i == j || !h.less(j, i) {
			break
		}
		h.swap(i, j)
		j = i
	}
}

func (h *bucketHeap) down(i, n int) {
	for {
		j1 := 2*i + 1
		if j1 >= n || j1 < 0 {
			break
		}
		j := j1 // left child
		if j2 := j1 + 1; j2 < n && h.less(j2, j1) {
			j = j2 // right child
		}
		if !h.less(j, i) {
			break
		}
		h.swap(i, j)
		i = j
	}
}

// countingSortBucketsInto sorts buckets by size (largest first) using counting sort.
// Reuses the result slice and pre-allocated counts/positions buffers to avoid per-block allocations.
func countingSortBucketsInto(bucketStarts []uint16, result []uint16, counts []int, positions []int) {
	n := len(bucketStarts) - 1
	if n <= 0 {
		return
	}

	// Find max bucket size
	maxSize := 0
	for i := 0; i < n; i++ {
		size := int(bucketStarts[i+1] - bucketStarts[i])
		if size > maxSize {
			maxSize = size
		}
	}

	if maxSize == 0 {
		for i := range result[:n] {
			result[i] = uint16(i)
		}
		return
	}

	// Clear counts buffer up to maxSize+1
	for i := 0; i <= maxSize; i++ {
		counts[i] = 0
	}

	// Count occurrences of each size
	for i := 0; i < n; i++ {
		size := int(bucketStarts[i+1] - bucketStarts[i])
		counts[size]++
	}

	// Convert to positions (reverse order for largest first)
	pos := 0
	for size := maxSize; size >= 0; size-- {
		positions[size] = pos
		pos += counts[size]
	}

	// Place buckets in sorted order
	for i := 0; i < n; i++ {
		size := int(bucketStarts[i+1] - bucketStarts[i])
		result[positions[size]] = uint16(i)
		positions[size]++
	}
}

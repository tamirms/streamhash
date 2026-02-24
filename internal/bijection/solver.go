package bijection

// This file contains bucket solver functions for MPHF construction.
//
// Bucket solvers find a seed value that produces a bijection (no collisions)
// for all keys in a bucket. They try seeds from 0 to maxSeed-1 and return
// the first seed that works, or (0, true) if no valid seed is found.
//
// Specialized solvers for sizes 2-8 use unrolled loops and inline slot
// computation for performance. Larger buckets use bitmask or array tracking.

// solveBucket2 finds a seed for 2-entry buckets (most common case).
func solveBucket2(bucket []bucketEntry, maxSeed uint32) (uint32, bool) {
	// Extract mixParts into locals to avoid repeated bucket[] indexing
	// and bounds checks in the hot seed loop.
	p0 := bucket[0].mixParts
	p1 := bucket[1].mixParts
	// Early termination: identical mixParts means guaranteed collision for all seeds
	if p0 == p1 {
		return 0, true
	}
	for seed := range maxSeed {
		s0 := mixFromParts(p0, seed, 2)
		s1 := mixFromParts(p1, seed, 2)
		if s0 != s1 {
			return seed, false
		}
	}
	return 0, true
}

// solveBucket3 finds a seed for 3-entry buckets.
func solveBucket3(bucket []bucketEntry, maxSeed uint32) (uint32, bool) {
	p0 := bucket[0].mixParts
	p1 := bucket[1].mixParts
	p2 := bucket[2].mixParts
	// Early termination: any duplicate mixParts means guaranteed collision
	if p0 == p1 || p0 == p2 || p1 == p2 {
		return 0, true
	}
	for seed := range maxSeed {
		s0 := mixFromParts(p0, seed, 3)
		s1 := mixFromParts(p1, seed, 3)
		if s0 == s1 {
			continue
		}
		s2 := mixFromParts(p2, seed, 3)
		if s2 != s0 && s2 != s1 {
			return seed, false
		}
	}
	return 0, true
}

// solveBucket4 finds a seed for 4-entry buckets.
func solveBucket4(bucket []bucketEntry, maxSeed uint32) (uint32, bool) {
	p0 := bucket[0].mixParts
	p1 := bucket[1].mixParts
	p2 := bucket[2].mixParts
	p3 := bucket[3].mixParts
	// Early termination: any duplicate mixParts means guaranteed collision
	if p0 == p1 || p0 == p2 || p0 == p3 || p1 == p2 || p1 == p3 || p2 == p3 {
		return 0, true
	}
	for seed := range maxSeed {
		s0 := mixFromParts(p0, seed, 4)
		s1 := mixFromParts(p1, seed, 4)
		if s0 == s1 {
			continue
		}
		s2 := mixFromParts(p2, seed, 4)
		if s2 == s0 || s2 == s1 {
			continue
		}
		s3 := mixFromParts(p3, seed, 4)
		if s3 != s0 && s3 != s1 && s3 != s2 {
			return seed, false
		}
	}
	return 0, true
}

// solveBucket5 finds a seed for 5-entry buckets.
func solveBucket5(bucket []bucketEntry, maxSeed uint32) (uint32, bool) {
	p0, p1, p2, p3, p4 := bucket[0].mixParts, bucket[1].mixParts, bucket[2].mixParts, bucket[3].mixParts, bucket[4].mixParts
	// Early termination: any duplicate mixParts means guaranteed collision
	if p0 == p1 || p0 == p2 || p0 == p3 || p0 == p4 ||
		p1 == p2 || p1 == p3 || p1 == p4 ||
		p2 == p3 || p2 == p4 ||
		p3 == p4 {
		return 0, true
	}
	for seed := range maxSeed {
		s0 := mixFromParts(p0, seed, 5)
		s1 := mixFromParts(p1, seed, 5)
		if s0 == s1 {
			continue
		}
		s2 := mixFromParts(p2, seed, 5)
		if s2 == s0 || s2 == s1 {
			continue
		}
		s3 := mixFromParts(p3, seed, 5)
		if s3 == s0 || s3 == s1 || s3 == s2 {
			continue
		}
		s4 := mixFromParts(p4, seed, 5)
		if s4 != s0 && s4 != s1 && s4 != s2 && s4 != s3 {
			return seed, false
		}
	}
	return 0, true
}

// solveBucket6 finds a seed for 6-entry buckets.
func solveBucket6(bucket []bucketEntry, maxSeed uint32) (uint32, bool) {
	p0, p1, p2, p3, p4, p5 := bucket[0].mixParts, bucket[1].mixParts, bucket[2].mixParts, bucket[3].mixParts, bucket[4].mixParts, bucket[5].mixParts
	// Early termination: any duplicate mixParts means guaranteed collision
	if p0 == p1 || p0 == p2 || p0 == p3 || p0 == p4 || p0 == p5 ||
		p1 == p2 || p1 == p3 || p1 == p4 || p1 == p5 ||
		p2 == p3 || p2 == p4 || p2 == p5 ||
		p3 == p4 || p3 == p5 ||
		p4 == p5 {
		return 0, true
	}
	for seed := range maxSeed {
		s0 := mixFromParts(p0, seed, 6)
		s1 := mixFromParts(p1, seed, 6)
		if s0 == s1 {
			continue
		}
		s2 := mixFromParts(p2, seed, 6)
		if s2 == s0 || s2 == s1 {
			continue
		}
		s3 := mixFromParts(p3, seed, 6)
		if s3 == s0 || s3 == s1 || s3 == s2 {
			continue
		}
		s4 := mixFromParts(p4, seed, 6)
		if s4 == s0 || s4 == s1 || s4 == s2 || s4 == s3 {
			continue
		}
		s5 := mixFromParts(p5, seed, 6)
		if s5 != s0 && s5 != s1 && s5 != s2 && s5 != s3 && s5 != s4 {
			return seed, false
		}
	}
	return 0, true
}

// solveBucket7 finds a seed for 7-entry buckets using bitmask tracking.
func solveBucket7(bucket []bucketEntry, maxSeed uint32) (uint32, bool) {
	p0, p1, p2, p3 := bucket[0].mixParts, bucket[1].mixParts, bucket[2].mixParts, bucket[3].mixParts
	p4, p5, p6 := bucket[4].mixParts, bucket[5].mixParts, bucket[6].mixParts
	// Skip early termination check for size 7: pairwise duplicate detection cost exceeds benefit
	for seed := range maxSeed {
		var occupied uint8
		s0 := mixFromParts(p0, seed, 7)
		occupied |= 1 << s0
		s1 := mixFromParts(p1, seed, 7)
		if occupied&(1<<s1) != 0 {
			continue
		}
		occupied |= 1 << s1
		s2 := mixFromParts(p2, seed, 7)
		if occupied&(1<<s2) != 0 {
			continue
		}
		occupied |= 1 << s2
		s3 := mixFromParts(p3, seed, 7)
		if occupied&(1<<s3) != 0 {
			continue
		}
		occupied |= 1 << s3
		s4 := mixFromParts(p4, seed, 7)
		if occupied&(1<<s4) != 0 {
			continue
		}
		occupied |= 1 << s4
		s5 := mixFromParts(p5, seed, 7)
		if occupied&(1<<s5) != 0 {
			continue
		}
		occupied |= 1 << s5
		s6 := mixFromParts(p6, seed, 7)
		if occupied&(1<<s6) == 0 {
			return seed, false
		}
	}
	return 0, true
}

// solveBucket8 finds a seed for 8-entry buckets using bitmask tracking.
func solveBucket8(bucket []bucketEntry, maxSeed uint32) (uint32, bool) {
	p0, p1, p2, p3 := bucket[0].mixParts, bucket[1].mixParts, bucket[2].mixParts, bucket[3].mixParts
	p4, p5, p6, p7 := bucket[4].mixParts, bucket[5].mixParts, bucket[6].mixParts, bucket[7].mixParts
	// Skip early termination check for size 8: pairwise duplicate detection cost exceeds benefit
	for seed := range maxSeed {
		var occupied uint8
		s0 := mixFromParts(p0, seed, 8)
		occupied |= 1 << s0
		s1 := mixFromParts(p1, seed, 8)
		if occupied&(1<<s1) != 0 {
			continue
		}
		occupied |= 1 << s1
		s2 := mixFromParts(p2, seed, 8)
		if occupied&(1<<s2) != 0 {
			continue
		}
		occupied |= 1 << s2
		s3 := mixFromParts(p3, seed, 8)
		if occupied&(1<<s3) != 0 {
			continue
		}
		occupied |= 1 << s3
		s4 := mixFromParts(p4, seed, 8)
		if occupied&(1<<s4) != 0 {
			continue
		}
		occupied |= 1 << s4
		s5 := mixFromParts(p5, seed, 8)
		if occupied&(1<<s5) != 0 {
			continue
		}
		occupied |= 1 << s5
		s6 := mixFromParts(p6, seed, 8)
		if occupied&(1<<s6) != 0 {
			continue
		}
		occupied |= 1 << s6
		s7 := mixFromParts(p7, seed, 8)
		if occupied&(1<<s7) == 0 {
			return seed, false
		}
	}
	return 0, true
}

// solveBucketBitmask finds a seed for buckets up to 64 entries using bitmask tracking.
// Note: This function is only called for bucket sizes 9-64 in production.
// Sizes 2-8 use dedicated solveBucket2-8 functions.
func solveBucketBitmask(bucket []bucketEntry, size int, maxSeed uint32) (uint32, bool) {
	bucketSize := uint32(size)

	// Use pre-computed mixParts
	for seed := range maxSeed {
		var occupied uint64
		collision := false
		for _, entry := range bucket {
			slot := mixFromParts(entry.mixParts, seed, bucketSize)
			mask := uint64(1) << slot
			if occupied&mask != 0 {
				collision = true
				break
			}
			occupied |= mask
		}
		if !collision {
			return seed, false
		}
	}

	return 0, true
}

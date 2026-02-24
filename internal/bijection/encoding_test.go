package bijection

import (
	"slices"
	"testing"
)

// TestCheckpointsRoundTrip tests encode/decode of the checkpoints struct.
func TestCheckpointsRoundTrip(t *testing.T) {
	testCases := []struct {
		name string
		cp   checkpoints
	}{
		{
			name: "all_zeros",
			cp:   checkpoints{},
		},
		{
			name: "all_max",
			cp: func() checkpoints {
				var cp checkpoints
				for i := range cp.efBitPos {
					cp.efBitPos[i] = 0xFFFF
				}
				for i := range cp.seedBitPos {
					cp.seedBitPos[i] = 0xFFFF
				}
				return cp
			}(),
		},
		{
			name: "incremental",
			cp: func() checkpoints {
				var cp checkpoints
				for i := range cp.efBitPos {
					cp.efBitPos[i] = uint16((i + 1) * 100)
				}
				for i := range cp.seedBitPos {
					cp.seedBitPos[i] = uint16((i+1)*50 + 25)
				}
				return cp
			}(),
		},
		{
			name: "distinct_arrays",
			cp: func() checkpoints {
				var cp checkpoints
				for i := range cp.efBitPos {
					cp.efBitPos[i] = 0xAAAA
				}
				for i := range cp.seedBitPos {
					cp.seedBitPos[i] = 0x5555
				}
				return cp
			}(),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			buf := make([]byte, checkpointsSize)
			tc.cp.encode(buf)

			var decoded checkpoints
			decodeCheckpoints(buf, &decoded)

			for i := range numCheckpoints {
				if decoded.efBitPos[i] != tc.cp.efBitPos[i] {
					t.Errorf("efBitPos[%d]: got %d, want %d",
						i, decoded.efBitPos[i], tc.cp.efBitPos[i])
				}
				if decoded.seedBitPos[i] != tc.cp.seedBitPos[i] {
					t.Errorf("seedBitPos[%d]: got %d, want %d",
						i, decoded.seedBitPos[i], tc.cp.seedBitPos[i])
				}
			}
		})
	}
}

// TestGRSeedExhaustiveRoundtrip exhaustively verifies that for each
// bucket size 2-8, ALL seeds in [0, maxEncodableSeed) roundtrip through
// Golomb-Rice encode/decode.
func TestGRSeedExhaustiveRoundtrip(t *testing.T) {
	for size := 2; size <= 8; size++ {
		maxSeed := maxEncodableSeed(size)
		if maxSeed == 0 {
			continue
		}

		for seed := range maxSeed {
			enc := newSeedStreamEncoder()
			enc.encodeSeed(seed, size)
			data := enc.bytes()

			br := newBitReaderFast(data)
			got := decodeGRSeedFast(br, size)

			if got != seed {
				t.Fatalf("size=%d seed=%d: roundtrip got %d", size, seed, got)
			}
		}
	}
}

// TestGRSeedSequenceRoundtrip verifies encoding multiple seeds in
// sequence and decoding in order.
func TestGRSeedSequenceRoundtrip(t *testing.T) {
	rng := newTestRNG(t)
	const iterations = 1000

	for i := range iterations {
		// Generate random sequence of seeds for various bucket sizes
		n := 2 + rng.IntN(20)
		sizes := make([]int, n)
		seeds := make([]uint32, n)
		enc := newSeedStreamEncoder()

		for j := range sizes {
			sizes[j] = 2 + rng.IntN(7) // bucket size 2-8
			maxSeed := maxEncodableSeed(sizes[j])
			if maxSeed > 0 {
				seeds[j] = rng.Uint32N(maxSeed)
			}
			enc.encodeSeed(seeds[j], sizes[j])
		}

		data := enc.bytes()
		br := newBitReaderFast(data)

		for j := range sizes {
			got := decodeGRSeedFast(br, sizes[j])
			if got != seeds[j] {
				t.Fatalf("iter %d seq[%d]: size=%d seed=%d got %d",
					i, j, sizes[j], seeds[j], got)
			}
		}
	}
}

// TestGRSeedFallbackMarker verifies that the fallback marker
// encodes/decodes correctly and is distinct from seed=0.
func TestGRSeedFallbackMarker(t *testing.T) {
	for size := 2; size <= 8; size++ {
		// Encode fallback marker
		enc := newSeedStreamEncoder()
		enc.encodeFallbackMarker()
		data := enc.bytes()

		br := newBitReaderFast(data)
		got := decodeGRSeedFast(br, size)

		if got != fallbackMarker {
			t.Errorf("size=%d: fallback marker decoded as %d, want %d",
				size, got, fallbackMarker)
		}

		// Verify seed=0 is distinct from fallback
		enc2 := newSeedStreamEncoder()
		enc2.encodeSeed(0, size)
		data2 := enc2.bytes()

		br2 := newBitReaderFast(data2)
		got2 := decodeGRSeedFast(br2, size)

		if got2 != 0 {
			t.Errorf("size=%d: seed=0 decoded as %d", size, got2)
		}
	}
}

// TestGRSeedBounds verifies bounds and exact parameter values.
func TestGRSeedBounds(t *testing.T) {
	// Preserve exact regression table from removed TestGolombParameters
	expectedK := map[int]uint8{2: 1, 3: 2, 4: 3, 5: 4, 6: 5, 7: 7, 8: 8}
	for size, wantK := range expectedK {
		gotK := golombParameter(size)
		if gotK != wantK {
			t.Errorf("golombParameter(%d) = %d, want %d", size, gotK, wantK)
		}
	}

	// Exact maxEncodableSeed values (guards against silent formula changes)
	expectedMax := map[int]uint32{
		2: 16 << 1, // 32
		3: 16 << 2, // 64
		4: 16 << 3, // 128
		5: 16 << 4, // 256
		6: 16 << 5, // 512
		7: 16 << 7, // 2048
		8: 16 << 8, // 4096
	}
	for size, wantMax := range expectedMax {
		gotMax := maxEncodableSeed(size)
		if gotMax != wantMax {
			t.Errorf("maxEncodableSeed(%d) = %d, want %d", size, gotMax, wantMax)
		}
	}

	// maxEncodableSeed < maxExtendedSeedForB for all sizes
	for size := 2; size <= 8; size++ {
		if maxEncodableSeed(size) >= maxExtendedSeedForB(blockBits) {
			t.Errorf("size=%d: maxEncodableSeed(%d) >= maxExtendedSeedForB(%d)",
				size, maxEncodableSeed(size), maxExtendedSeedForB(blockBits))
		}
	}

	// Edge cases
	if maxEncodableSeed(0) != 0 {
		t.Errorf("maxEncodableSeed(0) = %d, want 0", maxEncodableSeed(0))
	}
	if maxEncodableSeed(1) != 0 {
		t.Errorf("maxEncodableSeed(1) = %d, want 0", maxEncodableSeed(1))
	}
}

// TestComputeEFLowBitsPinnedValues pins the Elias-Fano low-bits formula
// to known values. This catches formula bugs that round-trip tests would miss
// (since both encoder and decoder use the same formula, a consistent error
// produces a valid round-trip but incorrect space/structure).
func TestComputeEFLowBitsPinnedValues(t *testing.T) {
	cases := []struct {
		n, U int
		want int
	}{
		// Zero/degenerate cases: return 0 when U <= n or n == 0
		{0, 0, 0},
		{0, 100, 0},
		{1, 0, 0},
		{1, 1, 0},

		// Boundary: U == n returns 0 (triggers U <= n guard)
		{10, 10, 0},
		{1024, 1024, 0},

		// U > n: result is floor(log2(U/n))
		{1, 2, 1},     // floor(log2(2)) = 1
		{1, 100, 6},   // floor(log2(100)) = 6
		{10, 11, 0},   // floor(log2(1)) = 0
		{10, 20, 1},   // floor(log2(2)) = 1
		{10, 100, 3},  // floor(log2(10)) = 3
		{10, 1000, 6}, // floor(log2(100)) = 6

		// Production-scale values (bucketsPerBlock=1024)
		{1024, 2048, 1},  // floor(log2(2)) = 1
		{1024, 3072, 1},  // floor(log2(3)) = 1
		{1024, 10000, 3}, // floor(log2(9)) = 3
		{1024, 65535, 5}, // floor(log2(63)) = 5
	}
	for _, tc := range cases {
		got := computeEFLowBits(tc.n, tc.U)
		if got != tc.want {
			t.Errorf("computeEFLowBits(%d, %d) = %d, want %d", tc.n, tc.U, got, tc.want)
		}
	}
}

// TestEFDecodeRoundtrip verifies decodeEliasFanoUnrolled correctly recovers
// 1,000 random monotonic sequences from their Elias-Fano encoding.
func TestEFDecodeRoundtrip(t *testing.T) {
	rng := newTestRNG(t)
	const iterations = 1000

	for i := range iterations {
		n := 8 + rng.IntN(1017) // [8, 1024]
		U := n + rng.IntN(65536-n)

		// Generate random monotonic cumulative array
		cumulative := make([]uint16, n)
		for j := range cumulative {
			cumulative[j] = uint16(uint64(j) * uint64(U) / uint64(n))
		}
		// Add small random perturbation while keeping monotonicity
		for j := 1; j < n; j++ {
			gap := int(cumulative[j]) - int(cumulative[j-1])
			if gap > 1 {
				cumulative[j] -= uint16(rng.IntN(gap))
			}
		}
		// Re-sort to ensure monotonicity
		slices.Sort(cumulative)

		var buf []byte
		encoded := encodeEliasFanoInto(cumulative, uint32(U), &buf)

		// The unrolled decoder requires >= 8 bytes for its initial uint64 load.
		// Small-data cases are covered by TestEFSmallEncodedData.
		if len(encoded) < 8 {
			continue
		}

		var out [bucketsPerBlock]uint16
		decodeEliasFanoUnrolled(encoded, n, U, &out)

		for j := range n {
			if out[j] != cumulative[j] {
				t.Fatalf("iter %d: n=%d U=%d idx=%d: decoded=%d want=%d",
					i, n, U, j, out[j], cumulative[j])
			}
		}
	}
}

// TestEFSizeBound verifies eliasFanoSize >= actual encoded size.
func TestEFSizeBound(t *testing.T) {
	rng := newTestRNG(t)
	const iterations = 1000

	for i := range iterations {
		n := 1 + rng.IntN(1024)
		U := n + rng.IntN(65536-n)

		cumulative := make([]uint16, n)
		for j := range cumulative {
			cumulative[j] = uint16(uint64(j) * uint64(U) / uint64(n))
		}

		var buf []byte
		encoded := encodeEliasFanoInto(cumulative, uint32(U), &buf)

		estimate := eliasFanoSize(n, U)
		actual := len(encoded)
		if estimate < actual {
			t.Fatalf("iter %d: n=%d U=%d: eliasFanoSize=%d < actual=%d",
				i, n, U, estimate, actual)
		}
	}
}

// TestEFSmallEncodedData verifies EF correctness for sequences that produce
// fewer than 8 bytes of encoded data. The unrolled decoder and decodeEFRangeInline
// both bail out for < 8 bytes, so this test uses readEFLowBits + manual
// high-bit scanning as a reference decoder.
func TestEFSmallEncodedData(t *testing.T) {
	cases := []struct {
		name       string
		cumulative []uint16
		U          uint32
	}{
		{"n=1_U=0", []uint16{0}, 0},
		{"n=1_U=1", []uint16{0}, 1},
		{"n=2_U=2", []uint16{0, 1}, 2},
		{"n=3_U=3", []uint16{0, 1, 2}, 3},
		{"n=4_U=4", []uint16{0, 1, 2, 3}, 4},
		{"n=2_U=3", []uint16{0, 2}, 3},
		{"n=3_U=8", []uint16{0, 3, 5}, 8},
		{"n=1_U=5", []uint16{4}, 5},
		{"n=4_U=7", []uint16{0, 2, 4, 6}, 7},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var buf []byte
			encoded := encodeEliasFanoInto(tc.cumulative, tc.U, &buf)
			if len(encoded) == 0 {
				return
			}

			// Confirm this is a case the unrolled decoder would skip.
			if len(encoded) >= 8 {
				t.Skipf("encoded=%d bytes (>= 8), not a small-data case", len(encoded))
			}

			n := len(tc.cumulative)
			lowBits := computeEFLowBits(n, int(tc.U))
			lowMask := uint16(0)
			if lowBits > 0 {
				lowMask = (1 << lowBits) - 1
			}

			// Verify low bits via readEFLowBits
			for j := range n {
				expectedLow := tc.cumulative[j] & lowMask
				gotLow := readEFLowBits(encoded, j, lowBits)
				if gotLow != expectedLow {
					t.Errorf("low[%d]: got %d, want %d", j, gotLow, expectedLow)
				}
			}

			// Verify full values by scanning high bits manually (byte-by-byte).
			lowBitsTotal := n * lowBits
			bitPos := 0
			currentHigh := uint16(0)
			for j := range n {
				for {
					absBitPos := lowBitsTotal + bitPos
					byteIdx := absBitPos / 8
					bitInByte := absBitPos % 8
					if byteIdx >= len(encoded) {
						t.Fatalf("element %d: ran past encoded data scanning high bits", j)
					}
					if (encoded[byteIdx]>>bitInByte)&1 == 1 {
						bitPos++
						break
					}
					currentHigh++
					bitPos++
				}

				got := (currentHigh << lowBits) | readEFLowBits(encoded, j, lowBits)
				if got != tc.cumulative[j] {
					t.Errorf("element[%d]: got %d, want %d (high=%d, lowBits=%d)",
						j, got, tc.cumulative[j], currentHigh, lowBits)
				}
			}
		})
	}
}

// TestEFEdgeCases tests deterministic Elias-Fano edge cases.
func TestEFEdgeCases(t *testing.T) {
	cases := []struct {
		n, U int
	}{
		{0, 0},
		{1, 0},
		{1, 1},
		{1024, 0},
		{1024, 1},
		{1024, 65535},
	}

	for _, tc := range cases {
		cumulative := make([]uint16, tc.n)
		if tc.U > 0 {
			for j := range cumulative {
				cumulative[j] = uint16(uint64(j) * uint64(tc.U) / uint64(tc.n))
			}
		}

		var buf []byte
		encoded := encodeEliasFanoInto(cumulative, uint32(tc.U), &buf)

		estimate := eliasFanoSize(tc.n, tc.U)
		if estimate < len(encoded) {
			t.Errorf("n=%d U=%d: estimate=%d < actual=%d", tc.n, tc.U, estimate, len(encoded))
		}

		// The unrolled decoder requires >= 8 bytes for its initial uint64 load.
		// Cases that produce < 8 bytes (e.g. n=1) are covered by TestEFSmallEncodedData.
		if len(encoded) >= 8 {
			var out [bucketsPerBlock]uint16
			decodeEliasFanoUnrolled(encoded, tc.n, tc.U, &out)

			for j := 0; j < tc.n; j++ {
				if out[j] != cumulative[j] {
					t.Errorf("n=%d U=%d idx=%d: decoded=%d want=%d", tc.n, tc.U, j, out[j], cumulative[j])
				}
			}
		}
	}
}

package bijection

import (
	"testing"
)

// TestBitWriterReset tests that reset() clears state completely.
func TestBitWriterReset(t *testing.T) {
	bw := newBitWriterBuffer()

	// Write 100 bits of known data
	for i := 0; i < 100; i++ {
		bw.writeBit(1)
	}
	if bw.bitsWritten() != 100 {
		t.Fatalf("before reset: bitsWritten = %d, want 100", bw.bitsWritten())
	}

	bw.reset()
	if bw.bitsWritten() != 0 {
		t.Fatalf("after reset: bitsWritten = %d, want 0", bw.bitsWritten())
	}

	// Write 50 bits of different data
	bw.writeBits(0xABCDEF123, 36)
	bw.writeBits(0x3FFF, 14) // total = 50
	dataAfterReset := bw.flush()

	// Create a fresh writer with the same 50 bits
	bw2 := newBitWriterBuffer()
	bw2.writeBits(0xABCDEF123, 36)
	bw2.writeBits(0x3FFF, 14)
	dataFresh := bw2.flush()

	if len(dataAfterReset) != len(dataFresh) {
		t.Fatalf("length mismatch: reset=%d, fresh=%d", len(dataAfterReset), len(dataFresh))
	}
	for i := range dataAfterReset {
		if dataAfterReset[i] != dataFresh[i] {
			t.Errorf("byte %d: reset=0x%02X, fresh=0x%02X", i, dataAfterReset[i], dataFresh[i])
		}
	}
}

// TestBitsWritten tests that bitsWritten tracks correctly across all operations.
func TestBitsWritten(t *testing.T) {
	bw := newBitWriterBuffer()

	if bw.bitsWritten() != 0 {
		t.Fatalf("initial: got %d, want 0", bw.bitsWritten())
	}

	bw.writeBit(1)
	if bw.bitsWritten() != 1 {
		t.Errorf("after writeBit: got %d, want 1", bw.bitsWritten())
	}

	bw.writeBits(0xFF, 8)
	if bw.bitsWritten() != 9 {
		t.Errorf("after writeBits(8): got %d, want 9", bw.bitsWritten())
	}

	bw.writeOnes(20)
	if bw.bitsWritten() != 29 {
		t.Errorf("after writeOnes(20): got %d, want 29", bw.bitsWritten())
	}

	// Write enough to trigger a flushWord (total > 64)
	bw.writeBits(0, 35) // total = 64, triggers flush
	if bw.bitsWritten() != 64 {
		t.Errorf("at 64 bits: got %d, want 64", bw.bitsWritten())
	}

	bw.writeBits(0xAB, 8) // total = 72
	if bw.bitsWritten() != 72 {
		t.Errorf("after boundary: got %d, want 72", bw.bitsWritten())
	}

	// Write ones spanning a word boundary
	bw.writeOnes(100) // total = 172
	if bw.bitsWritten() != 172 {
		t.Errorf("after spanning writeOnes: got %d, want 172", bw.bitsWritten())
	}
}

// TestReadUnaryMax16 tests readUnaryMax16 which has complex boundary logic.
func TestReadUnaryMax16(t *testing.T) {
	t.Run("zero_ones", func(t *testing.T) {
		bw := newBitWriterBuffer()
		bw.writeBit(0) // zero bit immediately
		data := bw.flush()

		br := newBitReaderFast(data)
		got := br.readUnaryMax16()
		if got != 0 {
			t.Errorf("got %d, want 0", got)
		}
	})

	t.Run("small_count", func(t *testing.T) {
		bw := newBitWriterBuffer()
		bw.writeOnes(5)
		bw.writeBit(0)
		data := bw.flush()

		br := newBitReaderFast(data)
		got := br.readUnaryMax16()
		if got != 5 {
			t.Errorf("got %d, want 5", got)
		}
	})

	t.Run("exact_max", func(t *testing.T) {
		bw := newBitWriterBuffer()
		bw.writeOnes(16)
		bw.writeBit(0) // terminator after cap
		data := bw.flush()

		br := newBitReaderFast(data)
		got := br.readUnaryMax16()
		if got != 16 {
			t.Errorf("got %d, want 16", got)
		}
	})

	t.Run("exceeds_max", func(t *testing.T) {
		bw := newBitWriterBuffer()
		bw.writeOnes(20) // more than grMaxQuotient
		data := bw.flush()

		br := newBitReaderFast(data)
		got := br.readUnaryMax16()
		if got != 16 {
			t.Errorf("got %d, want 16 (capped)", got)
		}
	})

	t.Run("at_word_boundary", func(t *testing.T) {
		bw := newBitWriterBuffer()
		// Write 60 junk bits to position near word boundary
		bw.writeBits(0, 60)
		// Write 10 ones + 0-bit (spans the word boundary at bit 64)
		bw.writeOnes(10)
		bw.writeBit(0)
		data := bw.flush()

		br := newBitReaderFast(data)
		br.skipBits(60) // skip to where the unary data starts
		got := br.readUnaryMax16()
		if got != 10 {
			t.Errorf("got %d, want 10", got)
		}
	})

	t.Run("ones_span_word_boundary_hit_cap", func(t *testing.T) {
		bw := newBitWriterBuffer()
		// Write 50 junk bits, then 20 ones that span the boundary and exceed cap
		bw.writeBits(0, 50)
		bw.writeOnes(20)
		data := bw.flush()

		br := newBitReaderFast(data)
		br.skipBits(50)
		got := br.readUnaryMax16()
		if got != 16 {
			t.Errorf("got %d, want 16 (capped at boundary)", got)
		}
	})

	t.Run("verify_position_after_cap", func(t *testing.T) {
		bw := newBitWriterBuffer()
		// Write 20 ones (will be capped at 16), then write known bits
		bw.writeOnes(20)
		bw.writeBits(0xAB, 8)
		data := bw.flush()

		br := newBitReaderFast(data)
		got := br.readUnaryMax16()
		if got != 16 {
			t.Errorf("unary: got %d, want 16", got)
		}
		// After capping at 16, bitPos should be at bit 16 (consumed 16 ones).
		// The next bits are ones[16..19] (4 more ones), then 0xAB.
		// Read the 4 remaining ones + 8 bits of 0xAB = 12 bits total.
		remaining := br.readBits(4)
		if remaining != 0xF { // 4 remaining one-bits
			t.Errorf("remaining ones: got 0x%X, want 0xF", remaining)
		}
		val := br.readBits(8)
		if val != 0xAB {
			t.Errorf("value after cap: got 0x%X, want 0xAB", val)
		}
	})
}

// TestBitWriterReaderRandomSequences writes random (value, bitWidth)
// pairs and reads them back.
func TestBitWriterReaderRandomSequences(t *testing.T) {
	rng := newTestRNG(t)
	const iterations = 1000

	for i := 0; i < iterations; i++ {
		n := 10 + rng.IntN(41) // 10-50 pairs
		values := make([]uint64, n)
		widths := make([]int, n)

		bw := newBitWriterBuffer()
		for j := range values {
			widths[j] = 1 + rng.IntN(64) // 1-64 bits
			if widths[j] == 64 {
				values[j] = rng.Uint64()
			} else {
				values[j] = rng.Uint64() & ((1 << widths[j]) - 1)
			}
			bw.writeBits(values[j], widths[j])
		}

		data := bw.flush()
		br := newBitReaderFast(data)

		for j := range values {
			got := br.readBits(widths[j])
			if got != values[j] {
				t.Fatalf("iter %d seq[%d]: width=%d wrote=0x%X read=0x%X",
					i, j, widths[j], values[j], got)
			}
		}
	}
}

// TestBitWriterReaderBoundaries tests word boundary edge cases.
func TestBitWriterReaderBoundaries(t *testing.T) {
	// Exact 64-bit boundary
	t.Run("ExactWord", func(t *testing.T) {
		bw := newBitWriterBuffer()
		val := uint64(0xDEADBEEFCAFEBABE)
		bw.writeBits(val, 64)

		data := bw.flush()
		br := newBitReaderFast(data)
		got := br.readBits(64)
		if got != val {
			t.Errorf("exact word: got 0x%X, want 0x%X", got, val)
		}
	})

	// Spanning 64-bit boundary
	t.Run("SpanningBoundary", func(t *testing.T) {
		bw := newBitWriterBuffer()
		v1 := uint64(0xABCDEF0123456789)
		v2 := uint64(0x0F0F0F0F0F0F0F0F)
		bw.writeBits(v1, 40) // bits 0-39
		bw.writeBits(v2, 40) // bits 40-79 (spans boundary)

		data := bw.flush()
		br := newBitReaderFast(data)
		got1 := br.readBits(40)
		got2 := br.readBits(40)

		mask40 := uint64((1 << 40) - 1)
		if got1 != v1&mask40 {
			t.Errorf("spanning v1: got 0x%X, want 0x%X", got1, v1&mask40)
		}
		if got2 != v2&mask40 {
			t.Errorf("spanning v2: got 0x%X, want 0x%X", got2, v2&mask40)
		}
	})

	// Zero-width write is a no-op
	t.Run("ZeroWidth", func(t *testing.T) {
		bw := newBitWriterBuffer()
		bw.writeBits(0xFF, 8)
		bw.writeBits(0, 0) // should be no-op
		bw.writeBits(0xAA, 8)

		data := bw.flush()
		br := newBitReaderFast(data)
		got1 := br.readBits(8)
		got2 := br.readBits(8)
		if got1 != 0xFF || got2 != 0xAA {
			t.Errorf("zero width: got 0x%X, 0x%X, want 0xFF, 0xAA", got1, got2)
		}
	})
}

// TestBitWriterReaderOnes writes N ones and verifies all read back as 1.
func TestBitWriterReaderOnes(t *testing.T) {
	rng := newTestRNG(t)
	const iterations = 1000

	for i := 0; i < iterations; i++ {
		n := 1 + rng.IntN(200) // 1-200 ones
		bw := newBitWriterBuffer()
		bw.writeOnes(n)

		data := bw.flush()
		br := newBitReaderFast(data)

		for j := 0; j < n; j++ {
			got := br.readBits(1)
			if got != 1 {
				t.Fatalf("iter %d: bit[%d]=%d, want 1 (n=%d)", i, j, got, n)
			}
		}
	}
}

// TestBitReaderSkipBits writes values, skips random bits, reads, and verifies.
func TestBitReaderSkipBits(t *testing.T) {
	rng := newTestRNG(t)
	const iterations = 1000

	for i := 0; i < iterations; i++ {
		bw := newBitWriterBuffer()
		// Write 3 values with known widths
		w1, w2, w3 := 8+rng.IntN(20), 8+rng.IntN(20), 8+rng.IntN(20)
		v1 := rng.Uint64() & ((1 << w1) - 1)
		v2 := rng.Uint64() & ((1 << w2) - 1)
		v3 := rng.Uint64() & ((1 << w3) - 1)
		bw.writeBits(v1, w1)
		bw.writeBits(v2, w2)
		bw.writeBits(v3, w3)

		data := bw.flush()
		br := newBitReaderFast(data)

		// Read v1, skip v2, read v3
		got1 := br.readBits(w1)
		br.skipBits(w2)
		got3 := br.readBits(w3)

		if got1 != v1 {
			t.Fatalf("iter %d: v1 got=0x%X want=0x%X", i, got1, v1)
		}
		if got3 != v3 {
			t.Fatalf("iter %d: v3 got=0x%X want=0x%X (after skip %d bits)", i, got3, v3, w2)
		}
	}
}

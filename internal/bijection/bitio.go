package bijection

import (
	"encoding/binary"
	"math/bits"
)

// =============================================================================
// Bit Writer
// =============================================================================

type bitWriter struct {
	buf     []byte
	current uint64
	bitPos  int
}

func newBitWriterBuffer() *bitWriter {
	return &bitWriter{
		buf: make([]byte, 0, 4096),
	}
}

func (bw *bitWriter) flushWord() {
	var wordBuf [8]byte
	binary.LittleEndian.PutUint64(wordBuf[:], bw.current)
	bw.buf = append(bw.buf, wordBuf[:]...)
	bw.current = 0
	bw.bitPos = 0
}

func (bw *bitWriter) writeBit(bit uint8) {
	if bit != 0 {
		bw.current |= uint64(1) << bw.bitPos
	}
	bw.bitPos++
	if bw.bitPos == 64 {
		bw.flushWord()
	}
}

func (bw *bitWriter) writeBits(v uint64, n int) {
	if n == 0 {
		return
	}

	mask := (uint64(1) << n) - 1
	v &= mask

	if bw.bitPos+n <= 64 {
		bw.current |= v << bw.bitPos
		bw.bitPos += n
		if bw.bitPos == 64 {
			bw.flushWord()
		}
		return
	}

	bitsInCurrent := 64 - bw.bitPos
	bw.current |= (v & ((1 << bitsInCurrent) - 1)) << bw.bitPos
	bw.flushWord()

	bw.current = v >> bitsInCurrent
	bw.bitPos = n - bitsInCurrent
}

func (bw *bitWriter) writeOnes(n int) {
	if n == 0 {
		return
	}

	if bw.bitPos+n <= 64 {
		mask := (uint64(1) << n) - 1
		bw.current |= mask << bw.bitPos
		bw.bitPos += n
		if bw.bitPos == 64 {
			bw.flushWord()
		}
		return
	}

	remaining := 64 - bw.bitPos
	bw.current |= (^uint64(0)) << bw.bitPos
	bw.flushWord()
	n -= remaining

	for n >= 64 {
		bw.current = ^uint64(0)
		bw.flushWord()
		n -= 64
	}

	if n > 0 {
		bw.current = (uint64(1) << n) - 1
		bw.bitPos = n
	}
}

func (bw *bitWriter) flush() []byte {
	if bw.bitPos > 0 {
		numBytes := (bw.bitPos + 7) / 8
		var wordBuf [8]byte
		binary.LittleEndian.PutUint64(wordBuf[:], bw.current)
		bw.buf = append(bw.buf, wordBuf[:numBytes]...)
		bw.current = 0
		bw.bitPos = 0
	}
	return bw.buf
}

func (bw *bitWriter) reset() {
	bw.buf = bw.buf[:0]
	bw.current = 0
	bw.bitPos = 0
}

func (bw *bitWriter) bitsWritten() int {
	return len(bw.buf)*8 + bw.bitPos
}

// =============================================================================
// Bit Reader
// =============================================================================

type bitReaderFast struct {
	data    []byte
	current uint64
	bitPos  int
	bytePos int
}

func newBitReaderFast(data []byte) *bitReaderFast {
	br := &bitReaderFast{
		data: data,
	}
	br.refill()
	return br
}

func (br *bitReaderFast) refill() {
	remaining := len(br.data) - br.bytePos
	if remaining >= 8 {
		br.current = binary.LittleEndian.Uint64(br.data[br.bytePos:])
		br.bytePos += 8
	} else if remaining > 0 {
		br.current = 0
		for i := range remaining {
			br.current |= uint64(br.data[br.bytePos+i]) << (i * 8)
		}
		br.bytePos += remaining
	} else {
		br.current = 0
	}
	br.bitPos = 0
}

func (br *bitReaderFast) readBits(n int) uint64 {
	if n == 0 {
		return 0
	}
	if br.bitPos+n > 64 {
		bitsFromCurrent := 64 - br.bitPos
		lowBits := br.current >> br.bitPos

		br.refill()

		bitsFromNext := n - bitsFromCurrent
		mask := (uint64(1) << bitsFromNext) - 1
		highBits := br.current & mask
		br.bitPos = bitsFromNext

		return lowBits | (highBits << bitsFromCurrent)
	}

	mask := (uint64(1) << n) - 1
	result := (br.current >> br.bitPos) & mask
	br.bitPos += n
	return result
}

func (br *bitReaderFast) skipBits(n int) {
	for n > 0 {
		available := 64 - br.bitPos
		if n <= available {
			br.bitPos += n
			return
		}
		n -= available
		br.refill()
	}
}

// readUnaryMax16 reads unary-coded ones with a maximum of grMaxQuotient.
// Returns the count of ones (0-grMaxQuotient), capped at grMaxQuotient.
func (br *bitReaderFast) readUnaryMax16() uint32 {
	count := uint32(0)

	for {
		if br.bitPos >= 64 {
			br.refill()
		}

		remaining := br.current >> br.bitPos
		inverted := ^remaining

		zeros := bits.TrailingZeros64(inverted)

		availableBits := 64 - br.bitPos

		if zeros < availableBits {
			newCount := count + uint32(zeros)

			if newCount >= grMaxQuotient {
				br.bitPos += int(grMaxQuotient - count)
				return grMaxQuotient
			}

			br.bitPos += zeros + 1
			return newCount
		}

		oldCount := count
		count += uint32(availableBits)

		if count >= grMaxQuotient {
			br.bitPos += int(grMaxQuotient - oldCount)
			return grMaxQuotient
		}

		br.bitPos = 64
	}
}

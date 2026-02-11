package encoding

import (
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"math/rand/v2"
	"testing"
	"unsafe"
)

// Named seeds for deterministic reproduction.
const (
	testSeed1 = 0x1234567890ABCDEF
	testSeed2 = 0xFEDCBA9876543210
)

func newTestRNG(t testing.TB) *rand.Rand {
	t.Helper()
	h := fnv.New128a()
	h.Write([]byte(t.Name()))
	sum := h.Sum(nil)
	s1 := binary.LittleEndian.Uint64(sum[:8])
	s2 := binary.LittleEndian.Uint64(sum[8:])
	return rand.New(rand.NewPCG(testSeed1^s1, testSeed2^s2))
}

// randomFP generates a random fingerprint value that fits in fpSize bytes.
func randomFP(rng *rand.Rand, fpSize int) uint32 {
	if fpSize == 0 {
		return 0
	}
	if fpSize == 4 {
		return rng.Uint32()
	}
	return rng.Uint32() & (uint32(1)<<(fpSize*8) - 1)
}

// randomPayload generates a random payload value that fits in payloadSize bytes.
func randomPayload(rng *rand.Rand, payloadSize int) uint64 {
	if payloadSize == 0 {
		return 0
	}
	if payloadSize >= 8 {
		return rng.Uint64()
	}
	return rng.Uint64() & (uint64(1)<<(payloadSize*8) - 1)
}

// randomEntryParams generates valid random (fpSize, payloadSize, fp, payload) tuples.
// Ensures fpSize+payloadSize is in [1,12] and values are properly masked.
func randomEntryParams(t *testing.T, rng *rand.Rand) (fpSize, payloadSize int, fp uint32, payload uint64) {
	t.Helper()
	for {
		fpSize = int(rng.IntN(5))     // 0-4
		payloadSize = int(rng.IntN(9)) // 0-8
		entrySize := fpSize + payloadSize
		if entrySize >= 1 && entrySize <= 12 {
			break
		}
	}
	fp = randomFP(rng, fpSize)
	payload = randomPayload(rng, payloadSize)
	return
}

// TestWriteEntryBugRegression specifically tests the << 8 bug that
// was present when fpSize != 1 for entrySize=5.
func TestWriteEntryBugRegression(t *testing.T) {
	// entrySize=5 with fp=2 + payload=3: the old code used << 8 (hardcoded for fp=1)
	// which would corrupt the payload. The fix uses fpShift = fpSize * 8.
	buf := make([]byte, 5)
	basePtr := unsafe.Pointer(&buf[0])

	fp := uint32(0xBEEF)        // 2-byte fp
	payload := uint64(0xABCDEF) // 3-byte payload
	fpShift := uint(2 * 8)      // fpSize=2

	WriteEntry(basePtr, 0, 5, fp, payload, fpShift)

	gotFP := ReadFP(buf, 2)
	gotPayload := ReadPayload(buf[2:], 3)

	if gotFP != fp {
		t.Errorf("regression fp: got %x, want %x", gotFP, fp)
	}
	if gotPayload != payload {
		t.Errorf("regression payload: got %x, want %x (old bug would give wrong value)", gotPayload, payload)
	}
}

// TestWriteEntryGenericRoundtrip verifies that WriteEntryGeneric correctly
// round-trips fingerprint and payload values for random valid (fpSize, payloadSize)
// combinations, and that adjacent entries remain zero (no buffer overrun).
func TestWriteEntryGenericRoundtrip(t *testing.T) {
	rng := newTestRNG(t)
	const iterations = 1000

	for i := 0; i < iterations; i++ {
		fpSize, payloadSize, fp, payload := randomEntryParams(t, rng)
		entrySize := fpSize + payloadSize
		fpShift := uint(fpSize * 8)

		const numEntries = 3
		pos := 1 // write at middle position

		buf := make([]byte, numEntries*entrySize)
		basePtr := unsafe.Pointer(&buf[0])
		WriteEntryGeneric(basePtr, pos, entrySize, fp, payload, fpShift)

		offset := pos * entrySize
		gotFP := ReadFP(buf[offset:], fpSize)
		gotPayload := ReadPayload(buf[offset+fpSize:], payloadSize)

		if gotFP != fp {
			t.Fatalf("iter %d: fp mismatch: fpSize=%d paySize=%d fp=0x%X payload=0x%X: got fp 0x%X",
				i, fpSize, payloadSize, fp, payload, gotFP)
		}
		if gotPayload != payload {
			t.Fatalf("iter %d: payload mismatch: fpSize=%d paySize=%d fp=0x%X payload=0x%X: got payload 0x%X",
				i, fpSize, payloadSize, fp, payload, gotPayload)
		}

		// Verify no buffer overrun: entry 0 and entry 2 should remain zero
		for j := 0; j < entrySize; j++ {
			if buf[j] != 0 {
				t.Fatalf("iter %d: buffer overrun at buf[%d]=0x%X, want 0", i, j, buf[j])
			}
			if buf[2*entrySize+j] != 0 {
				t.Fatalf("iter %d: buffer overrun at buf[%d]=0x%X, want 0", i, 2*entrySize+j, buf[2*entrySize+j])
			}
		}
	}
}

// TestWriteEntrySpecializedParity verifies that for entry sizes 1, 4, 5, and 8,
// WriteEntry and WriteEntryGeneric produce byte-identical output.
func TestWriteEntrySpecializedParity(t *testing.T) {
	rng := newTestRNG(t)
	const iterations = 1000

	for _, entrySize := range []int{1, 4, 5, 8} {
		t.Run(fmt.Sprintf("entrySize=%d", entrySize), func(t *testing.T) {
			for i := 0; i < iterations; i++ {
				// Generate random valid (fpSize, payloadSize) for this entrySize
				fpSize := int(rng.IntN(min(5, entrySize+1)))
				payloadSize := entrySize - fpSize
				if payloadSize < 0 || payloadSize > 8 {
					continue // skip invalid combo
				}

				fpShift := uint(fpSize * 8)
				fp := randomFP(rng, fpSize)
				payload := randomPayload(rng, payloadSize)

				const numEntries = 3
				pos := 1

				specBuf := make([]byte, numEntries*entrySize)
				genBuf := make([]byte, numEntries*entrySize)

				WriteEntry(unsafe.Pointer(&specBuf[0]), pos, entrySize, fp, payload, fpShift)
				WriteEntryGeneric(unsafe.Pointer(&genBuf[0]), pos, entrySize, fp, payload, fpShift)

				offset := pos * entrySize
				for j := 0; j < entrySize; j++ {
					if specBuf[offset+j] != genBuf[offset+j] {
						t.Fatalf("iter %d: parity mismatch at byte %d: entrySize=%d fpSize=%d fp=0x%X payload=0x%X: spec=0x%X gen=0x%X",
							i, j, entrySize, fpSize, fp, payload, specBuf[offset:offset+entrySize], genBuf[offset:offset+entrySize])
					}
				}

				// Verify no buffer overrun: entries 0 and 2 should remain zero.
				for j := 0; j < entrySize; j++ {
					if specBuf[j] != 0 {
						t.Fatalf("iter %d: WriteEntry buffer overrun at buf[%d]=0x%X, want 0", i, j, specBuf[j])
					}
					if specBuf[2*entrySize+j] != 0 {
						t.Fatalf("iter %d: WriteEntry buffer overrun at buf[%d]=0x%X, want 0", i, 2*entrySize+j, specBuf[2*entrySize+j])
					}
					if genBuf[j] != 0 {
						t.Fatalf("iter %d: WriteEntryGeneric buffer overrun at buf[%d]=0x%X, want 0", i, j, genBuf[j])
					}
					if genBuf[2*entrySize+j] != 0 {
						t.Fatalf("iter %d: WriteEntryGeneric buffer overrun at buf[%d]=0x%X, want 0", i, 2*entrySize+j, genBuf[2*entrySize+j])
					}
				}
			}
		})
	}
}

// TestWriteEntryBoundaryValues tests deterministic boundary values
// ({0, 1, all-ones-max, mid-value}) for each valid (fpSize, payloadSize) combination.
func TestWriteEntryBoundaryValues(t *testing.T) {
	for entrySize := 1; entrySize <= 12; entrySize++ {
		for fpSize := 0; fpSize <= min(4, entrySize); fpSize++ {
			payloadSize := entrySize - fpSize
			if payloadSize < 0 || payloadSize > 8 {
				continue
			}

			name := fmt.Sprintf("entry%d_fp%d_pay%d", entrySize, fpSize, payloadSize)
			t.Run(name, func(t *testing.T) {
				fpShift := uint(fpSize * 8)

				fpValues := []uint32{0}
				if fpSize > 0 {
					var fpMax uint32
					if fpSize == 4 {
						fpMax = ^uint32(0)
					} else {
						fpMax = uint32(1)<<(fpSize*8) - 1
					}
					fpValues = append(fpValues, 1, fpMax/2, fpMax)
				}

				payValues := []uint64{0}
				if payloadSize > 0 {
					var payMax uint64
					if payloadSize >= 8 {
						payMax = ^uint64(0)
					} else {
						payMax = uint64(1)<<(payloadSize*8) - 1
					}
					payValues = append(payValues, 1, payMax/2, payMax)
				}

				for _, fp := range fpValues {
					for _, payload := range payValues {
						const numEntries = 3
						pos := 1

						// Test WriteEntryGeneric (all entry sizes)
						genBuf := make([]byte, numEntries*entrySize)
						WriteEntryGeneric(unsafe.Pointer(&genBuf[0]), pos, entrySize, fp, payload, fpShift)

						offset := pos * entrySize
						gotFP := ReadFP(genBuf[offset:], fpSize)
						gotPayload := ReadPayload(genBuf[offset+fpSize:], payloadSize)

						if gotFP != fp {
							t.Errorf("fp=0x%X payload=0x%X: generic got fp 0x%X",
								fp, payload, gotFP)
						}
						if gotPayload != payload {
							t.Errorf("fp=0x%X payload=0x%X: generic got payload 0x%X",
								fp, payload, gotPayload)
						}

						// Verify no buffer overrun on generic buffer
						for j := 0; j < entrySize; j++ {
							if genBuf[j] != 0 {
								t.Errorf("fp=0x%X payload=0x%X: generic overrun at buf[%d]=0x%X",
									fp, payload, j, genBuf[j])
							}
							if genBuf[2*entrySize+j] != 0 {
								t.Errorf("fp=0x%X payload=0x%X: generic overrun at buf[%d]=0x%X",
									fp, payload, 2*entrySize+j, genBuf[2*entrySize+j])
							}
						}

						// For sizes 4/5/8, also test WriteEntry (specialized path):
						// correctness, buffer overrun, and byte-level parity with generic.
						if entrySize == 4 || entrySize == 5 || entrySize == 8 {
							specBuf := make([]byte, numEntries*entrySize)
							WriteEntry(unsafe.Pointer(&specBuf[0]), pos, entrySize, fp, payload, fpShift)

							specFP := ReadFP(specBuf[offset:], fpSize)
							specPayload := ReadPayload(specBuf[offset+fpSize:], payloadSize)

							if specFP != fp {
								t.Errorf("fp=0x%X payload=0x%X: specialized got fp 0x%X",
									fp, payload, specFP)
							}
							if specPayload != payload {
								t.Errorf("fp=0x%X payload=0x%X: specialized got payload 0x%X",
									fp, payload, specPayload)
							}

							// Verify no buffer overrun on specialized buffer
							for j := 0; j < entrySize; j++ {
								if specBuf[j] != 0 {
									t.Errorf("fp=0x%X payload=0x%X: specialized overrun at buf[%d]=0x%X",
										fp, payload, j, specBuf[j])
								}
								if specBuf[2*entrySize+j] != 0 {
									t.Errorf("fp=0x%X payload=0x%X: specialized overrun at buf[%d]=0x%X",
										fp, payload, 2*entrySize+j, specBuf[2*entrySize+j])
								}
							}

							// Verify byte-level parity: specialized == generic
							for j := 0; j < entrySize; j++ {
								if specBuf[offset+j] != genBuf[offset+j] {
									t.Errorf("fp=0x%X payload=0x%X: parity mismatch at byte %d: spec=0x%X gen=0x%X",
										fp, payload, j, specBuf[offset:offset+entrySize], genBuf[offset:offset+entrySize])
									break
								}
							}
						}
					}
				}
			})
		}
	}
}

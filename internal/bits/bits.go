// Package bits provides low-level bit manipulation primitives.
package bits

import "math/bits"

// FastRange32 maps a 64-bit hash uniformly to [0, n) returning uint32.
// Uses the "fastrange" technique: multiply and take high bits.
// This is the standard way to map hashes to ranges without modulo bias.
func FastRange32(hash uint64, n uint32) uint32 {
	if n == 0 {
		return 0
	}
	hi, _ := bits.Mul64(hash, uint64(n))
	return uint32(hi)
}

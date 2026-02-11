// Package errors defines all exported error sentinels for the streamhash library.
//
// This is the single source of truth for error values. Both the top-level
// streamhash package and internal algorithm packages import from here,
// ensuring errors.Is checks work across package boundaries.
package errors

import "errors"

// Build errors
var (
	ErrBuilderClosed    = errors.New("streamhash: builder is closed")
	ErrEmptyIndex       = errors.New("streamhash: cannot build index with zero keys")
	ErrTooManyKeys      = errors.New("streamhash: key count exceeds maximum (2^40)")
	ErrKeyTooShort      = errors.New("streamhash: key is shorter than minimum required length")
	ErrKeyTooLong       = errors.New("streamhash: key exceeds maximum length (65535 bytes)")
	ErrPayloadOverflow  = errors.New("streamhash: payload value exceeds configured PayloadSize capacity")
	ErrDuplicateKey     = errors.New("streamhash: duplicate key detected")
	ErrUnsortedInput    = errors.New("streamhash: input keys are not sorted")
	ErrRegionOverflow   = errors.New("streamhash: block region capacity exceeded")
	ErrKeyCountMismatch = errors.New("streamhash: key count mismatch")
)

// Construction errors (spec §7.5)
var (
	ErrPayloadTooLarge             = errors.New("streamhash: PayloadSize exceeds maximum 8 bytes")
	ErrFingerprintTooLarge         = errors.New("streamhash: FingerprintSize exceeds maximum (4 bytes)")
	ErrSplitBucketSeedSearchFailed = errors.New("streamhash: split bucket seed search failed - retry with different globalSeed")
	ErrIndistinguishableHashes     = errors.New("streamhash: indistinguishable hashes in bucket - retry with different globalSeed")
)

// Index errors
var (
	ErrInvalidMagic     = errors.New("streamhash: invalid magic number")
	ErrInvalidVersion   = errors.New("streamhash: unsupported version")
	ErrChecksumFailed   = errors.New("streamhash: file checksum verification failed")
	ErrTruncatedFile    = errors.New("streamhash: index file is truncated")
	ErrCorruptedIndex   = errors.New("streamhash: index data is corrupted")
	ErrNotFound         = errors.New("streamhash: key not found")
)

// Query errors
var (
	ErrIndexClosed         = errors.New("streamhash: index is closed")
	ErrNoPayload           = errors.New("streamhash: index has no payload data")
	ErrFingerprintMismatch = errors.New("streamhash: fingerprint mismatch")
)

// Internal errors (used by algorithm implementations)
var (
	ErrInvalidGeometry = errors.New("streamhash: invalid geometry parameters")
	ErrBlockOverflow   = errors.New("streamhash: block overflow")
)

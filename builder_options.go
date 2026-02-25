package streamhash

const (
	maxPayloadSize     = 8
	maxFingerprintSize = 4
)

// BuildOption is a functional option for configuring builds.
type BuildOption func(*buildConfig)

type buildConfig struct {
	workers              int
	tempDir              string
	payloadSize          int
	fingerprintSize      int // in bytes
	globalSeed           uint64
	userMetadata         []byte
	unsortedInput        bool // true to enable unsorted input mode
	unsortedMemoryBudget int64
	algorithm            BlockAlgorithmID

	// Advanced options (spec §7.2)
	totalKeys uint64 // Pre-known key count for single-pass builds
}

func defaultBuildConfig() *buildConfig {
	return &buildConfig{
		workers:    0, // Default to single-threaded; use WithWorkers(n) to parallelize
		globalSeed: 0x1234567890abcdef, // Arbitrary default; overridden via WithGlobalSeed
	}
}

// WithWorkers sets the number of parallel workers.
func WithWorkers(n int) BuildOption {
	return func(c *buildConfig) {
		c.workers = n
	}
}

// WithTempDir sets the temporary directory for unsorted builds.
func WithTempDir(dir string) BuildOption {
	return func(c *buildConfig) {
		c.tempDir = dir
	}
}

// WithPayload configures payload storage.
func WithPayload(size int) BuildOption {
	return func(c *buildConfig) {
		c.payloadSize = size
	}
}

// WithFingerprint configures fingerprint verification (size in bytes).
func WithFingerprint(sizeBytes int) BuildOption {
	return func(c *buildConfig) {
		c.fingerprintSize = sizeBytes
	}
}

// WithGlobalSeed sets the global hash seed.
func WithGlobalSeed(seed uint64) BuildOption {
	return func(c *buildConfig) {
		c.globalSeed = seed
	}
}

// WithUserMetadata sets the variable-length user metadata.
// The metadata is copied, so the caller can reuse the slice after this call.
func WithUserMetadata(data []byte) BuildOption {
	return func(c *buildConfig) {
		c.userMetadata = append([]byte(nil), data...) // Copy slice
	}
}

// WithUnsortedInput enables unsorted input mode.
// In this mode, keys can be added in any order. The builder buffers entries
// in RAM, counting-sorts by partition, and writes sequentially to partition
// files. During Finish, partitions are read back and blocks are built.
//
// Requirements:
//   - Keys must be at least 16 bytes long
//   - Keys must be unique
//   - NVMe storage recommended (HDD will be very slow)
//
// Note: The AddKey phase is always single-threaded.
// WithWorkers only affects the build phase in Finish(), where blocks are
// built in parallel after reading from partition files.
//
// Use WithTempDir to specify where partition files are created.
// Use WithUnsortedMemoryBudget to control peak memory usage.
func WithUnsortedInput() BuildOption {
	return func(c *buildConfig) {
		c.unsortedInput = true
	}
}

// WithUnsortedMemoryBudget sets the peak memory budget for unsorted builds.
// Both write-phase buffers and read-phase decoded entries are sized to
// fit within this budget. With pipelining, two partitions are decoded
// concurrently, each using budget/2.
//
// Default: 256MB (handles up to ~20B keys without tuning).
// Minimum: totalKeys * 12.8 bytes (keeps partition count <= 5000).
// No performance benefit from larger budgets (file operation overhead
// is constant regardless of partition count).
//
// Note: actual process RSS will exceed this budget due to Go GC overhead
// (GOGC=100 allows ~2x live heap), goroutine stacks, and page cache for
// partition files. For strict memory control, set GOGC to a lower value.
//
// Temp disk space required equals total data size (totalKeys * entrySize).
// tempDir must point to a real filesystem (not tmpfs) with sufficient space.
func WithUnsortedMemoryBudget(bytes int64) BuildOption {
	return func(c *buildConfig) {
		c.unsortedMemoryBudget = bytes
	}
}

// WithAlgorithm sets the block construction algorithm.
// Default is AlgoBijection.
func WithAlgorithm(algo BlockAlgorithmID) BuildOption {
	return func(c *buildConfig) {
		c.algorithm = algo
	}
}

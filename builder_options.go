package streamhash

const (
	maxPayloadSize     = 8
	maxFingerprintSize = 4
)

// BuildOption is a functional option for configuring builds.
type BuildOption func(*buildConfig)

type buildConfig struct {
	workers         int
	tempDir         string
	payloadSize     int
	fingerprintSize int // in bytes
	globalSeed      uint64
	userMetadata    []byte
	unsortedInput   bool // true to enable unsorted input mode
	algorithm       BlockAlgorithmID

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
// In this mode, keys can be added in any order. The builder uses a temp file
// to buffer entries by block, then processes them in block order.
//
// Requirements:
//   - Keys must be at least 16 bytes long
//   - Keys must be unique
//   - NVMe storage recommended (HDD will be very slow)
//
// Note: The AddKey phase is always single-threaded (I/O bound to temp file).
// WithWorkers only affects the build phase in Finish(), where blocks are
// built in parallel after reading from the temp file.
//
// Use WithTempDir to specify where the temp file is created.
func WithUnsortedInput() BuildOption {
	return func(c *buildConfig) {
		c.unsortedInput = true
	}
}

// WithAlgorithm sets the block construction algorithm.
// Default is AlgoBijection.
func WithAlgorithm(algo BlockAlgorithmID) BuildOption {
	return func(c *buildConfig) {
		c.algorithm = algo
	}
}

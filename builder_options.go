package streamhash

const (
	maxPayloadSize     = 8
	maxFingerprintSize = 4
)

// BuildOption is a functional option for configuring builds.
type BuildOption func(*buildConfig)

// UnsortedOption is a functional option for configuring unsorted input mode.
type UnsortedOption func(*buildConfig)

type buildConfig struct {
	workers         int
	payloadSize     int
	fingerprintSize int // in bytes
	globalSeed      uint64
	userMetadata    []byte
	unsortedInput   bool   // true to enable unsorted input mode
	unsortedTempDir string // temp directory for unsorted partition files
	algorithm       BlockAlgorithmID

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

// TempDir sets the temporary directory for unsorted partition files.
// The directory must exist and be on a local filesystem (ext4, xfs, btrfs).
// NFS is not supported (uses "silly rename" instead of true unlink-while-open).
// tmpfs works but is not recommended at scale since it stores data in RAM/swap.
func TempDir(dir string) UnsortedOption {
	return func(c *buildConfig) {
		c.unsortedTempDir = dir
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
// in RAM, partitions them, and writes sequentially to partition files.
// During Finish, partitions are read back and blocks are built.
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
// Use TempDir to specify where partition files are created.
func WithUnsortedInput(opts ...UnsortedOption) BuildOption {
	return func(c *buildConfig) {
		c.unsortedInput = true
		for _, opt := range opts {
			opt(c)
		}
	}
}

// WithAlgorithm sets the block construction algorithm.
// Default is AlgoBijection.
func WithAlgorithm(algo BlockAlgorithmID) BuildOption {
	return func(c *buildConfig) {
		c.algorithm = algo
	}
}

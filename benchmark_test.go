package streamhash

import (
	"bytes"
	"cmp"
	"context"
	"fmt"
	"path/filepath"
	"runtime"
	"slices"
	"testing"
)

func benchmarkBuildN(b *testing.B, n int) {
	rng := newTestRNG(b)
	keys := generateRandomKeys(rng, n, 24)
	sortKeysByBlock(keys, uint64(n), nil)

	tempDir := b.TempDir()
	ctx := context.Background()

	indexPath := filepath.Join(tempDir, "bench.idx")

	b.ResetTimer()
	b.ReportAllocs()
	for range b.N {
		builder, err := NewBuilder(ctx, indexPath, uint64(n))
		if err != nil {
			b.Fatal(err)
		}
		for _, key := range keys {
			if err := builder.AddKey(key, 0); err != nil {
				builder.Close()
				b.Fatal(err)
			}
		}
		if err := builder.Finish(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkBuild1K(b *testing.B)   { benchmarkBuildN(b, 1000) }
func BenchmarkBuild10K(b *testing.B)  { benchmarkBuildN(b, 10000) }
func BenchmarkBuild100K(b *testing.B) { benchmarkBuildN(b, 100000) }

func benchmarkQueryN(b *testing.B, n int) {
	rng := newTestRNG(b)
	keys := generateRandomKeys(rng, n, 24)

	tempDir := b.TempDir()

	indexPath := filepath.Join(tempDir, "bench.idx")
	ctx := context.Background()
	if err := quickBuild(ctx, indexPath, keys); err != nil {
		b.Fatal(err)
	}

	idx, err := Open(indexPath)
	if err != nil {
		b.Fatal(err)
	}
	defer idx.Close()

	b.ResetTimer()
	b.ReportAllocs()
	for i := range b.N {
		_, _ = idx.Query(keys[i%n])
	}
}

func BenchmarkQuery1K(b *testing.B)   { benchmarkQueryN(b, 1000) }
func BenchmarkQuery10K(b *testing.B)  { benchmarkQueryN(b, 10000) }
func BenchmarkQuery100K(b *testing.B) { benchmarkQueryN(b, 100000) }

func benchmarkQueryPayloadN(b *testing.B, n int) {
	rng := newTestRNG(b)
	keys := generateRandomKeys(rng, n, 24)

	tempDir := b.TempDir()

	indexPath := filepath.Join(tempDir, "bench_fp.idx")
	ctx := context.Background()
	if err := quickBuild(ctx, indexPath, keys, WithPayload(4), WithFingerprint(2)); err != nil {
		b.Fatal(err)
	}

	idx, err := Open(indexPath)
	if err != nil {
		b.Fatal(err)
	}
	defer idx.Close()

	b.ResetTimer()
	b.ReportAllocs()
	for i := range b.N {
		_, _ = idx.QueryPayload(keys[i%n])
	}
}

func BenchmarkQueryPayload1K(b *testing.B)   { benchmarkQueryPayloadN(b, 1000) }
func BenchmarkQueryPayload10K(b *testing.B)  { benchmarkQueryPayloadN(b, 10000) }
func BenchmarkQueryPayload100K(b *testing.B) { benchmarkQueryPayloadN(b, 100000) }

func BenchmarkQueryParallel(b *testing.B) {
	n := 10000
	rng := newTestRNG(b)
	keys := generateRandomKeys(rng, n, 24)

	tempDir := b.TempDir()

	indexPath := filepath.Join(tempDir, "bench.idx")
	ctx := context.Background()
	if err := quickBuild(ctx, indexPath, keys); err != nil {
		b.Fatal(err)
	}

	idx, err := Open(indexPath)
	if err != nil {
		b.Fatal(err)
	}
	defer idx.Close()

	b.ResetTimer()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			_, _ = idx.Query(keys[i%n])
			i++
		}
	})
}

func BenchmarkHeaderEncode(b *testing.B) {
	h := &header{
		Magic:     magic,
		Version:   version,
		TotalKeys: 1000000,
		NumBlocks: 256,
		Seed:      0x1234567890abcdef,
	}

	buf := make([]byte, headerSize)

	b.ResetTimer()
	b.ReportAllocs()
	for b.Loop() {
		h.encodeTo(buf)
	}
}

func BenchmarkHeaderDecode(b *testing.B) {
	h := &header{
		Magic:     magic,
		Version:   version,
		TotalKeys: 1000000,
		NumBlocks: 256,
		Seed:      0x1234567890abcdef,
	}
	encoded := make([]byte, headerSize)
	h.encodeTo(encoded)

	b.ResetTimer()
	b.ReportAllocs()
	for b.Loop() {
		_, _ = decodeHeader(encoded)
	}
}

func BenchmarkComputeGeometry(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()
	for b.Loop() {
		numBlocksForAlgo(AlgoBijection, 1000000, 0, 0)
	}
}

// BenchmarkBuildSortedPreHash benchmarks sorted builds with pre-hashing enabled vs disabled.
func BenchmarkBuildSortedPreHash(b *testing.B) {
	sizes := []int{10000, 100000, 1000000}
	rng := newTestRNG(b)

	for _, n := range sizes {
		keys := generateRandomKeys(rng, n, 24)

		// Benchmark WITHOUT pre-hashing (keys sorted by original bytes)
		b.Run(fmt.Sprintf("NoPreHash_%dk", n/1000), func(b *testing.B) {
			// Sort by original key bytes for block index ordering
			sortedKeys := make([][]byte, len(keys))
			copy(sortedKeys, keys)
			sortByPrefix(sortedKeys)

			tempDir := b.TempDir()
			indexPath := filepath.Join(tempDir, "bench.idx")
			ctx := context.Background()

			b.ResetTimer()
			b.ReportAllocs()
			for range b.N {
				builder, err := NewBuilder(ctx, indexPath, uint64(n), WithWorkers(1))
				if err != nil {
					b.Fatal(err)
				}
				for _, k := range sortedKeys {
					if err := builder.AddKey(k, 0); err != nil {
						b.Fatal(err)
					}
				}
				if err := builder.Finish(); err != nil {
					b.Fatal(err)
				}
			}
			b.ReportMetric(float64(n)*float64(b.N)/b.Elapsed().Seconds(), "keys/sec")
		})

		// Benchmark WITH pre-hashing (keys sorted by hashed bytes)
		b.Run(fmt.Sprintf("PreHash_%dk", n/1000), func(b *testing.B) {
			// For pre-hash builds, we need to sort by the HASHED key prefix
			hashedKeys := make([]hashedKey, len(keys))
			for i, k := range keys {
				hashedKeys[i] = hashedKey{hashed: PreHash(k)}
			}
			sortByHashedPrefix(hashedKeys)

			tempDir := b.TempDir()
			indexPath := filepath.Join(tempDir, "bench.idx")
			ctx := context.Background()

			b.ResetTimer()
			b.ReportAllocs()
			for range b.N {
				builder, err := NewBuilder(ctx, indexPath, uint64(n), WithWorkers(1))
				if err != nil {
					b.Fatal(err)
				}
				for _, hk := range hashedKeys {
					if err := builder.AddKey(hk.hashed, 0); err != nil {
						b.Fatal(err)
					}
				}
				if err := builder.Finish(); err != nil {
					b.Fatal(err)
				}
			}
			b.ReportMetric(float64(n)*float64(b.N)/b.Elapsed().Seconds(), "keys/sec")
		})
	}
}

// sortByPrefix sorts keys by their prefix (used for block routing)
func sortByPrefix(keys [][]byte) {
	slices.SortFunc(keys, func(a, b []byte) int {
		return cmp.Compare(extractPrefix(a), extractPrefix(b))
	})
}

type hashedKey struct {
	hashed []byte
}

func sortByHashedPrefix(keys []hashedKey) {
	slices.SortFunc(keys, func(a, b hashedKey) int {
		return cmp.Compare(extractPrefix(a.hashed), extractPrefix(b.hashed))
	})
}

// BenchmarkBuilder benchmarks the Builder AddKey API performance
func BenchmarkBuilder(b *testing.B) {
	numKeys := 1_000_000
	payloadSize := 4

	// Generate sorted keys
	rng := newTestRNG(b)
	keys := make([][32]byte, numKeys)
	payloads := make([]uint64, numKeys)
	for i := range keys {
		fillFromRNG(rng, keys[i][:])
		payloads[i] = uint64(i)
	}

	numBlocks, _ := numBlocksForAlgo(AlgoBijection, uint64(numKeys), payloadSize, 1)
	slices.SortFunc(keys, func(a, b [32]byte) int {
		return cmp.Compare(blockIndexFromPrefix(extractPrefix(a[:]), numBlocks),
			blockIndexFromPrefix(extractPrefix(b[:]), numBlocks))
	})

	tempDir := b.TempDir()
	indexPath := filepath.Join(tempDir, "sorted.idx")
	ctx := context.Background()

	b.ResetTimer()
	b.ReportAllocs()
	for range b.N {
		builder, err := NewBuilder(ctx, indexPath, uint64(numKeys),
			WithPayload(payloadSize), WithFingerprint(1))
		if err != nil {
			b.Fatal(err)
		}
		for j := range keys {
			if err := builder.AddKey(keys[j][:], payloads[j]); err != nil {
				b.Fatal(err)
			}
		}
		if err := builder.Finish(); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportMetric(float64(numKeys)*float64(b.N)/b.Elapsed().Seconds(), "keys/sec")
}

// BenchmarkParallelBuilder benchmarks the parallel Builder AddKey API performance
func BenchmarkParallelBuilder(b *testing.B) {
	numKeys := 1_000_000
	payloadSize := 4

	// Generate keys
	rng := newTestRNG(b)
	keys := make([][32]byte, numKeys)
	payloads := make([]uint64, numKeys)
	for i := range keys {
		fillFromRNG(rng, keys[i][:])
		payloads[i] = uint64(i)
	}

	// Pre-sort keys by block index for Builder
	type keyPayload struct {
		key     [32]byte
		payload uint64
	}
	entries := make([]keyPayload, numKeys)
	for i := range keys {
		entries[i] = keyPayload{key: keys[i], payload: payloads[i]}
	}
	numBlocks, _ := numBlocksForAlgo(AlgoBijection, uint64(numKeys), payloadSize, 1)
	slices.SortFunc(entries, func(a, b keyPayload) int {
		return cmp.Compare(blockIndexFromPrefix(extractPrefix(a.key[:]), numBlocks),
			blockIndexFromPrefix(extractPrefix(b.key[:]), numBlocks))
	})

	tempDir := b.TempDir()
	indexPath := filepath.Join(tempDir, "parallel.idx")
	ctx := context.Background()

	b.ResetTimer()
	b.ReportAllocs()
	for range b.N {
		builder, err := NewBuilder(ctx, indexPath, uint64(numKeys),
			WithPayload(payloadSize), WithFingerprint(1), WithWorkers(4))
		if err != nil {
			b.Fatal(err)
		}
		for j := range entries {
			if err := builder.AddKey(entries[j].key[:], entries[j].payload); err != nil {
				b.Fatal(err)
			}
		}
		if err := builder.Finish(); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportMetric(float64(numKeys)*float64(b.N)/b.Elapsed().Seconds(), "keys/sec")
}

// ============================================================================
// Micro benchmarks
// ============================================================================

func BenchmarkAddKey(b *testing.B) {
	const numKeys = 100000
	keys := make([][32]byte, numKeys)
	rng := newTestRNG(b)
	for i := range keys {
		fillFromRNG(rng, keys[i][:])
	}

	numBlocks, _ := numBlocksForAlgo(AlgoBijection, uint64(numKeys), 4, 1)
	slices.SortFunc(keys, func(a, b [32]byte) int {
		return cmp.Compare(blockIndexFromPrefix(extractPrefix(a[:]), numBlocks),
			blockIndexFromPrefix(extractPrefix(b[:]), numBlocks))
	})

	tmpDir := b.TempDir()

	b.ResetTimer()
	b.ReportAllocs()
	for b.Loop() {
		indexPath := filepath.Join(tmpDir, "test.idx")
		builder, err := NewBuilder(context.Background(), indexPath, uint64(numKeys),
			WithPayload(4),
			WithFingerprint(1))
		if err != nil {
			b.Fatal(err)
		}
		for j := range numKeys {
			if err := builder.AddKey(keys[j][:], uint64(j)); err != nil {
				b.Fatal(err)
			}
		}
		if err := builder.Finish(); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportMetric(float64(numKeys), "keys/op")
}

// ============================================================================
// PTRHash benchmarks
// ============================================================================

func BenchmarkPTRHashParallelBuild1M(b *testing.B) {
	const numKeys = 1_000_000

	rng := newTestRNG(b)
	keys := make([][32]byte, numKeys)
	for i := range keys {
		fillFromRNG(rng, keys[i][:])
	}

	numBlocks, _ := numBlocksForAlgo(AlgoPTRHash, uint64(numKeys), 0, 0)
	slices.SortFunc(keys, func(a, b [32]byte) int {
		if c := cmp.Compare(blockIndexFromPrefix(extractPrefix(a[:]), numBlocks),
			blockIndexFromPrefix(extractPrefix(b[:]), numBlocks)); c != 0 {
			return c
		}
		return bytes.Compare(a[:], b[:])
	})

	tmpDir := b.TempDir()
	indexPath := filepath.Join(tmpDir, "ptrhash.idx")
	ctx := context.Background()
	numCPU := runtime.NumCPU()

	b.ResetTimer()
	b.ReportAllocs()

	for i := range b.N {
		builder, err := NewBuilder(ctx, indexPath, uint64(numKeys),
			WithAlgorithm(AlgoPTRHash),
			WithWorkers(numCPU),
			WithGlobalSeed(uint64(i)))
		if err != nil {
			b.Fatalf("NewBuilder failed: %v", err)
		}
		for j := range keys {
			if err := builder.AddKey(keys[j][:], 0); err != nil {
				b.Fatalf("AddKey failed: %v", err)
			}
		}
		if err := builder.Finish(); err != nil {
			b.Fatalf("Finish failed: %v", err)
		}
	}
}

func BenchmarkPTRHashSingleThread1M(b *testing.B) {
	const numKeys = 1_000_000

	rng := newTestRNG(b)
	keys := make([][32]byte, numKeys)
	for i := range keys {
		fillFromRNG(rng, keys[i][:])
	}

	numBlocks, _ := numBlocksForAlgo(AlgoPTRHash, uint64(numKeys), 0, 0)
	slices.SortFunc(keys, func(a, b [32]byte) int {
		if c := cmp.Compare(blockIndexFromPrefix(extractPrefix(a[:]), numBlocks),
			blockIndexFromPrefix(extractPrefix(b[:]), numBlocks)); c != 0 {
			return c
		}
		return bytes.Compare(a[:], b[:])
	})

	tmpDir := b.TempDir()
	indexPath := filepath.Join(tmpDir, "ptrhash.idx")
	ctx := context.Background()

	b.ResetTimer()
	b.ReportAllocs()

	for i := range b.N {
		builder, err := NewBuilder(ctx, indexPath, uint64(numKeys),
			WithAlgorithm(AlgoPTRHash),
			WithWorkers(1),
			WithGlobalSeed(uint64(i)))
		if err != nil {
			b.Fatalf("NewBuilder failed: %v", err)
		}
		for j := range keys {
			if err := builder.AddKey(keys[j][:], 0); err != nil {
				b.Fatalf("AddKey failed: %v", err)
			}
		}
		if err := builder.Finish(); err != nil {
			b.Fatalf("Finish failed: %v", err)
		}
	}
}

func benchmarkPTRHashQueryN(b *testing.B, n int) {
	rng := newTestRNG(b)
	keys := generateRandomKeys(rng, n, 24)

	tempDir := b.TempDir()

	indexPath := filepath.Join(tempDir, "bench.idx")
	ctx := context.Background()
	if err := quickBuild(ctx, indexPath, keys, WithAlgorithm(AlgoPTRHash)); err != nil {
		b.Fatal(err)
	}

	idx, err := Open(indexPath)
	if err != nil {
		b.Fatal(err)
	}
	defer idx.Close()

	b.ResetTimer()
	b.ReportAllocs()
	for i := range b.N {
		_, _ = idx.Query(keys[i%n])
	}
}

func BenchmarkPTRHashQuery1K(b *testing.B)   { benchmarkPTRHashQueryN(b, 1000) }
func BenchmarkPTRHashQuery10K(b *testing.B)  { benchmarkPTRHashQueryN(b, 10000) }
func BenchmarkPTRHashQuery100K(b *testing.B) { benchmarkPTRHashQueryN(b, 100000) }

// ============================================================================
// Unsorted builder benchmarks
// ============================================================================

func BenchmarkUnsortedBuilder(b *testing.B) {
	ctx := context.Background()
	tmpDir := b.TempDir()

	rng := newTestRNG(b)
	keys := generateRandomKeys(rng, 10000, 16)

	b.ResetTimer()
	b.ReportAllocs()
	for b.Loop() {
		output := filepath.Join(tmpDir, "bench.idx")

		builder, err := NewBuilder(ctx, output, uint64(len(keys)),
			WithUnsortedInput(),
			WithPayload(4),
			WithTempDir(tmpDir),
		)
		if err != nil {
			b.Fatalf("NewBuilder failed: %v", err)
		}

		for j, key := range keys {
			if err := builder.AddKey(key, uint64(j)); err != nil {
				builder.Close()
				b.Fatalf("AddKey failed: %v", err)
			}
		}

		if err := builder.Finish(); err != nil {
			b.Fatalf("Finish failed: %v", err)
		}
	}

	b.ReportMetric(float64(len(keys)), "keys/op")
}

func BenchmarkUnsortedBuilderParallel(b *testing.B) {
	ctx := context.Background()
	tmpDir := b.TempDir()

	rng := newTestRNG(b)
	keys := generateRandomKeys(rng, 10000, 16)

	b.ResetTimer()
	b.ReportAllocs()
	for b.Loop() {
		output := filepath.Join(tmpDir, "bench_parallel.idx")

		builder, err := NewBuilder(ctx, output, uint64(len(keys)),
			WithUnsortedInput(),
			WithPayload(4),
			WithWorkers(4),
			WithTempDir(tmpDir),
		)
		if err != nil {
			b.Fatalf("NewBuilder failed: %v", err)
		}

		for j, key := range keys {
			if err := builder.AddKey(key, uint64(j)); err != nil {
				builder.Close()
				b.Fatalf("AddKey failed: %v", err)
			}
		}

		if err := builder.Finish(); err != nil {
			b.Fatalf("Finish failed: %v", err)
		}
	}

	b.ReportMetric(float64(len(keys)), "keys/op")
}

func BenchmarkPreHash(b *testing.B) {
	rng := newTestRNG(b)
	key := make([]byte, 32)
	fillFromRNG(rng, key)

	b.ResetTimer()
	b.ReportAllocs()
	for b.Loop() {
		PreHash(key)
	}
}

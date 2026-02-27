package streamhash

import (
	"context"
	"encoding/binary"
	"path/filepath"
	"runtime"
	"runtime/metrics"
	"sync/atomic"
	"testing"
	"time"
)

// TestMemoryBudgetAccuracy verifies that unsorted builds respect the memory
// budget by sampling peak heap across both write (AddKey/flush) and read-back
// (Finish) phases. Uses runtime/metrics with a 10ms ticker to avoid the
// stop-the-world pause of runtime.ReadMemStats. Catches regressions if a
// future change adds allocations that break the budget.
func TestMemoryBudgetAccuracy(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping memory budget test in short mode")
	}

	configs := []struct {
		name      string
		numKeys   int
		payload   int
		fp        int
		keySize   int
		budgetMB  int64
		maxHeapMB int64 // budget × 2.5 (accounts for GC doubling + overhead)
	}{
		// numKeys must exceed bufferCap (= budget/4/32) to force partition
		// flush + readback (non-fast path). Otherwise the test only exercises
		// the in-memory fast path and misses readPartition memory usage.
		//
		// 64MB budget → bufferCap = 524,288 → need > 524K keys
		// 256MB budget → bufferCap = 2,097,152 → need > 2.1M keys
		{"64MB_p4_fp1", 1_000_000, 4, 1, 20, 64, 160},
		{"256MB_p0_fp0", 5_000_000, 0, 0, 16, 256, 640},
		{"64MB_p8_fp4", 1_000_000, 8, 4, 24, 64, 160},
	}

	for _, tc := range configs {
		t.Run(tc.name, func(t *testing.T) {
			tmpDir := t.TempDir()
			ctx := context.Background()

			rng := newTestRNG(t)
			keys := generateRandomKeys(rng, tc.numKeys, tc.keySize)

			var payloadMask uint64
			if tc.payload >= 8 {
				payloadMask = ^uint64(0)
			} else if tc.payload > 0 {
				payloadMask = (1 << (tc.payload * 8)) - 1
			}

			opts := []BuildOption{
				WithUnsortedInput(TempDir(tmpDir)),
				WithWorkers(1),
			}
			if tc.payload > 0 {
				opts = append(opts, WithPayload(tc.payload))
			}
			if tc.fp > 0 {
				opts = append(opts, WithFingerprint(tc.fp))
			}

			indexPath := filepath.Join(tmpDir, "budget_test.idx")
			builder, err := NewBuilder(ctx, indexPath, uint64(tc.numKeys), opts...)
			if err != nil {
				t.Fatalf("NewBuilder: %v", err)
			}

			// Establish baseline heap after GC
			runtime.GC()
			time.Sleep(10 * time.Millisecond)
			baselineHeap := readHeapObjectsBytes()

			// Start background heap sampler using runtime/metrics (no STW pause).
			// 10ms ticker matches the pattern in cmd/bench.
			var peakHeap atomic.Uint64
			peakHeap.Store(baselineHeap)
			done := make(chan struct{})
			go func() {
				samples := []metrics.Sample{
					{Name: "/memory/classes/heap/objects:bytes"},
				}
				ticker := time.NewTicker(10 * time.Millisecond)
				defer ticker.Stop()
				for {
					select {
					case <-done:
						return
					case <-ticker.C:
						metrics.Read(samples)
						heap := samples[0].Value.Uint64()
						for {
							old := peakHeap.Load()
							if heap <= old || peakHeap.CompareAndSwap(old, heap) {
								break
							}
						}
					}
				}
			}()

			// Add keys (triggers flushes — write phase)
			for i, key := range keys {
				var payload uint64
				if tc.payload > 0 {
					payload = uint64(i) & payloadMask
				}
				if err := builder.AddKey(key, payload); err != nil {
					close(done)
					builder.Close()
					t.Fatalf("AddKey %d: %v", i, err)
				}
			}

			// Finish (triggers read-back phase)
			if err := builder.Finish(); err != nil {
				close(done)
				t.Fatalf("Finish: %v", err)
			}

			// Stop sampler and take a final sample
			close(done)
			finalHeap := readHeapObjectsBytes()
			for {
				old := peakHeap.Load()
				if finalHeap <= old || peakHeap.CompareAndSwap(old, finalHeap) {
					break
				}
			}

			// Check peak heap against budget × 2.5
			peakAboveBaseline := int64(peakHeap.Load()) - int64(baselineHeap)
			peakMB := peakAboveBaseline >> 20
			if peakMB > tc.maxHeapMB {
				t.Errorf("peak heap %dMB exceeds limit %dMB (budget %dMB × 2.5)",
					peakMB, tc.maxHeapMB, tc.budgetMB)
			}
			t.Logf("peak heap above baseline: %dMB (limit %dMB)", peakMB, tc.maxHeapMB)

			// Verify the index is valid
			idx, err := Open(indexPath)
			if err != nil {
				t.Fatalf("Open: %v", err)
			}
			defer idx.Close()

			// Spot-check a few keys
			for i := 0; i < min(100, tc.numKeys); i++ {
				k0 := binary.LittleEndian.Uint64(keys[i][0:8])
				_ = k0
				_, err := idx.Query(keys[i])
				if err != nil {
					t.Fatalf("Query key %d: %v", i, err)
				}
			}
		})
	}
}

// readHeapObjectsBytes returns the current heap objects size in bytes
// using runtime/metrics (no stop-the-world pause).
func readHeapObjectsBytes() uint64 {
	samples := []metrics.Sample{
		{Name: "/memory/classes/heap/objects:bytes"},
	}
	metrics.Read(samples)
	return samples[0].Value.Uint64()
}

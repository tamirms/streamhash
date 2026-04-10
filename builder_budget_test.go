package streamhash

import (
	"context"
	"path/filepath"
	"runtime"
	"runtime/metrics"
	"sync/atomic"
	"testing"
	"time"
)

// TestUnsortedHeapBound verifies that unsorted builds stay within expected
// heap limits by sampling peak heap across both write (AddKey/flush) and
// read-back (Finish) phases. Uses runtime/metrics with a 10ms ticker to
// avoid the stop-the-world pause of runtime.ReadMemStats.
func TestUnsortedHeapBound(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping heap bound test in short mode")
	}

	configs := []struct {
		name      string
		numKeys   int
		payload   int
		fp        int
		keySize   int
		maxHeapMB int64 // absolute cap on heap above baseline
	}{
		// numKeys must exceed bufferCap (≈ 524K) to force partition flush
		// to force partition flush + readback (non-fast path).
		// Heap budget covers both the fast path (all entries in memory, no flush)
		// and the normal path (partition buffers + readback). The fast path has
		// higher peak heap because partition regions + collected entries overlap
		// before GC can reclaim the regions.
		{"1M_p4_fp1", 1_000_000, 4, 1, 20, 320},
		{"5M_p0_fp0", 5_000_000, 0, 0, 16, 320},
		{"1M_p8_fp4", 1_000_000, 8, 4, 24, 320},
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
				WithWorkers(1),
			}
			if tc.payload > 0 {
				opts = append(opts, WithPayload(tc.payload))
			}
			if tc.fp > 0 {
				opts = append(opts, WithFingerprint(tc.fp))
			}

			indexPath := filepath.Join(tmpDir, "budget_test.idx")
			builder, err := NewUnsortedBuilder(ctx, indexPath, uint64(tc.numKeys), tmpDir, opts...)
			if err != nil {
				t.Fatalf("newBuilder: %v", err)
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

			// Check peak heap against limit
			peakAboveBaseline := int64(peakHeap.Load()) - int64(baselineHeap)
			peakMB := peakAboveBaseline >> 20
			if peakMB > tc.maxHeapMB {
				t.Errorf("peak heap %dMB exceeds limit %dMB",
					peakMB, tc.maxHeapMB)
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
				_, err := idx.QueryRank(keys[i])
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

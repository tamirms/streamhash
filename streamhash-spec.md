# StreamHash

**An Algorithm-Agnostic Framework for Streaming Minimal Perfect Hash Construction**

**Technical Specification**

---

## Table of Contents

1. [Overview](#1-overview)
2. [Architecture](#2-architecture)
3. [Comparison with Alternatives](#3-comparison-with-alternatives)
4. [Framework / Algorithm Contract](#4-framework--algorithm-contract)
5. [File Format](#5-file-format)
6. [Algorithm: Bijection](#6-algorithm-bijection)
7. [Algorithm: PTRHash Adaptation](#7-algorithm-ptrhash-adaptation)
8. [Implementation Notes](#8-implementation-notes)
9. [Appendices](#appendices)

**Document Guide:**
- **Quick understanding:** Read §1 (Overview) and §2 (Architecture) — §1.1 explains what an MPHF is, §1.2 describes the framework, and §2 walks through the end-to-end query flow
- **Should I use this?:** Read §3 (Comparison) for trade-offs vs alternatives, §1.7 for algorithm selection guidance
- **Framework extensibility:** Read §1.2 and §4 (Algorithm Contract) for how to plug in new algorithms
- **Implementation:** See §7.0 for a recommended implementation roadmap. The Glossary (Appendix D) defines all technical terms.
- **Reference:** §5 (File Format), §6 (Bijection), and §7 (PTRHash) are the normative specification

---

## 1. Overview

### 1.1. What is an MPHF

A **Minimal Perfect Hash Function (MPHF)** maps N keys to N consecutive integers [0, N) with no collisions and no wasted slots. This lets you build compact, read-only lookup tables where every key has a unique position — enabling O(1) lookups without storing the keys themselves.

### 1.2. What is StreamHash

StreamHash is an **algorithm-agnostic framework** for streaming MPHF construction. It can take almost any perfect hashing algorithm and, with some adaptation, give it:

- **Bounded-RAM streaming construction** — build indexes over billions of keys using only ~1–75 MB of RAM, regardless of dataset size
- **Locality-optimized queries** — all metadata for a group of keys is stored contiguously on disk, so computing the MPHF rank requires reading one contiguous region
- **Parallel builds** — the compute-intensive MPHF solving step parallelizes naturally across workers (each block is solved independently), while a coordinator sequences the output. Monolithic algorithms must coordinate across the entire key space in a single thread; StreamHash's block model turns construction into a pipeline where CPU-bound solving scales with worker count

The framework partitions keys into fixed-size groups, routes each key to its group by prefix, and delegates the actual hash function construction to a pluggable algorithm. The algorithm only needs to solve a bounded subset of keys — mapping them to unique slot positions — while the framework handles partitioning, file layout, parallel coordination, and payload/fingerprint storage.

Two algorithms are integrated today:

- **Bijection** — compact metadata encoding, ~2.46 bits/key, lowest RAM usage
- **PTRHash** — direct pilot (per-bucket parameter that steers keys to unique slots) lookup, ~2.70 bits/key, fastest queries (~0.05 µs vs ~1.0 µs)

### 1.3. When to use this

When configured with a payload size (1–8 bytes, fixed), the structure functions as a **static perfect hash map**, returning small fixed-size payloads directly instead of ranks. This is suitable for storing offsets, compact identifiers, or small fixed-width values. For larger or variable-length values, use MPHF mode and store the rank as an offset into an external data file.

**Example use cases:**
- **Content-addressed storage:** Map unique content hashes to storage locations with O(1) lookup
- **Database indexing:** Map primary keys to row offsets in a data file
- **Search engines:** Map document IDs to posting list offsets
- **Caching metadata:** Map object keys to fixed-size metadata (timestamps, sizes, flags)

**Warning:** By default, this structure does not support membership queries. It returns *some* rank (or payload) for any input, including keys not in the indexed set. Applications requiring membership verification can either: (1) store keys separately, (2) use a companion Bloom filter, or (3) enable fingerprint verification via the `FingerprintSize` header field, which detects non-members with configurable false-positive probability (similar to a Bloom filter).

```
Input:  N uniformly random keys (≥16 bytes; typically 32-byte hashes)
Output: Compact index that answers:
        "what is the rank of key K?" in one disk read (MPHF mode)
        — or —
        "what is the payload for key K?" in two disk reads (payload mode)

Note: Keys must be at least 16 bytes. 32-byte keys (e.g., SHA-256) are recommended.
      See §6.2.1 for collision probability analysis.
```

**When StreamHash is not a good fit:**
- Fewer than ~100K keys — the block overhead dominates; use a simple hash table or a monolithic MPHF
- Dynamic dataset — StreamHash is static; use a hash table or cuckoo filter instead

### 1.4. Comparison with Alternatives

The table below compares StreamHash against alternative MPHF constructions. "O(N) build RAM" means memory proportional to dataset size — for a 10-billion-key dataset, this could mean tens to hundreds of GB. StreamHash's fixed ~1–75 MB is independent of dataset size.

| System | Bits/Key | Build RAM | Query I/O | Membership | Notes |
|--------|----------|-----------|-----------|------------|-------|
| **StreamHash (Bijection)** | ~2.46 | ~1-75 MB | 1 read (MPHF) or 2 reads (payload) | Optional (probabilistic, via fingerprint) | Fixed RAM regardless of dataset size |
| **StreamHash (PTRHash)** | ~2.70 | ~1-75 MB | 1 read (MPHF) or 2 reads (payload) | Optional (probabilistic, via fingerprint) | Fixed RAM, fastest queries |
| RecSplit | ~1.56 | O(N) | Multiple reads | No | Best compression, slow construction, higher RAM |
| PTRHash (Rust) | ~2.1–3.0 | O(N) | 1 disk read | No | Multiple modes: fast (~3.0), balanced (~2.4), compact (~2.1) |
| CHD / CMPH | ~2.07 | O(N) | Multiple reads | No | Classic approach |
| Cuckoo Filter | ~8-12 | O(N) | 1-2 reads | Yes (probabilistic) | Different purpose |
| Bloom Filter | ~10 | O(N) | k reads | Yes (probabilistic) | Different purpose |

StreamHash's Bijection algorithm trades ~0.8–0.9 extra bits/key vs a streaming RecSplit for faster build times. The framework itself is algorithm-agnostic — a streaming RecSplit implementation could be plugged in to achieve similar bits/key with the same bounded-RAM construction (see §3.5).

### 1.5. Design Goals

1. **Extensibility** — The framework is algorithm-agnostic; new MPHF algorithms (including streaming RecSplit) can be added by providing a build-time solver and a query-time decoder (see §3)
2. **Single-region queries** — Every lookup reads metadata from one contiguous region. MPHF mode requires one disk read; payload mode requires two reads (metadata region + payload region at different file offsets)
3. **Streaming construction** — Build with minimal userspace RAM:
   - **~1–75 MB** regardless of dataset size (routing buffers + hash array + block queue)
   - Unsorted input uses a single mmap'd temp file (~1.04–1.13× input size, depending on algorithm)
4. **Build parallelism** — Block independence enables multi-worker construction pipelines. MPHF solving (the CPU-bound step) runs independently per block across workers; a coordinator sequences output. Throughput scales near-linearly with worker count
5. **Fast construction** — Linear time, I/O-bound at NVMe speeds
6. **Simplicity** — Straightforward implementation with standard building blocks

### 1.6. Constraints

- **Input must be uniformly random** — Keys must be pre-hashed if not already random. The implementation does not perform internal pre-hashing; callers must hash non-uniform input before indexing.
- **Static only** — No insertions or deletions after construction
- **No membership test by default** — Returns *some* rank for non-member keys unless `FingerprintSize > 0` is configured
- **Temp disk for construction (unsorted input only)** — When building from unsorted input, temporary disk space approximately equal to input size is required. When building from pre-sorted input, no temp disk is required.
- **Peak disk usage (unsorted input only)** — During construction from unsorted input, peak disk usage is approximately 3× input size: source data + temp files + final output file.
- **Write amplification (unsorted input only)** — Construction from unsorted input involves writing keys to a mmap'd temp file (organized by block), then reading back during the build phase. This results in ~1× read + ~1.04–1.13× write of the input data (the margin is 7/√avgKeysPerBlock, which is smaller for PTRHash's larger blocks). Pre-sorted input has minimal I/O.
- **Maximum key length** — Keys must be ≤ 65,535 bytes. This limit comes from the uint16 key-length field in the unsorted-mode temp file format. In practice, keys are typically 16–64 bytes (hash outputs); this limit exists only as a safety check.
- **Fixed-size payloads only** — All payloads must be exactly `PayloadSize` bytes. Variable-length values require padding or external storage with offsets.

### 1.7. Performance Summary

Reference measurements on Apple M1 Max (100M keys, MPHF mode, pre-sorted input, single-file output):

| Metric              | Bijection               | PTRHash                        |
|---------------------|-------------------------|--------------------------------|
| Index size (MPHF)   | ~2.46 bits/key          | ~2.70 bits/key                 |
| Query latency (CPU) | ~1.0 µs                 | ~0.05 µs                       |
| Build throughput (1w)| ~16 M keys/sec         | ~16 M keys/sec                 |
| Build throughput (4w)| ~65 M keys/sec         | ~61 M keys/sec                 |
| Peak heap RAM (1w)  | ~1 MB                   | ~9 MB                          |
| Peak heap RAM (4w)  | ~9 MB                   | ~74 MB                         |

| Metric              | Pre-sorted Input        | Unsorted Input                 |
|---------------------|-------------------------|--------------------------------|
| Build throughput (1w)| ~16 M keys/sec         | ~12–13 M keys/sec (~20-25% slower) |
| Scale               | 1M – ~10¹² keys         | 1M – ~10¹² keys                |
| Build I/O           | 1× read                 | 1× read + temp file I/O        |
| Temp disk           | None                    | ~7σ Poisson margin             |
| Page cache          | minimal                 | minimal (mmap temp file)       |

Query latency excludes disk I/O. MPHF mode requires one metadata read per query; payload mode requires a second read from the payload region. On NVMe, each read is typically ~100 µs.

*Scale is limited by 40-bit index fields (~1.1 trillion keys max). See §6.2.3 for detailed limits.*

### 1.8. Choosing an Algorithm

| | Bijection | PTRHash |
|---|---|---|
| **Best for** | Compactness, low RAM | Query speed |
| Index size | ~2.46 bits/key | ~2.70 bits/key |
| Query latency (CPU) | ~1.0 µs | ~0.05 µs |
| Build throughput (4w) | ~65 M keys/sec | ~61 M keys/sec |
| Peak RAM (4w) | ~9 MB | ~74 MB |

**Use Bijection** when index size or RAM usage matters most. Bijection achieves better compression (~2.46 vs ~2.70 bits/key) and uses ~8× less RAM during construction (~9 vs ~74 MB at 4 workers). Build throughput is similar between the two algorithms. The trade-off is higher query latency (~1.0 µs vs ~0.05 µs).

**Use PTRHash** when query speed is critical. PTRHash achieves O(1) queries by reading a single pilot byte — roughly 20× faster per query than Bijection's checkpoint-based O(128) decode.

---

## 2. Architecture

**Note:** §2.7 traces a complete example through every step described below — you may want to read it alongside these sections.

**Terminology:** Key terms are defined when first introduced. A complete glossary is in Appendix D.

### 2.1. Key Terminology

| Term | Definition |
|------|------------|
| `prefix` | First 8 bytes of a key interpreted as **big-endian** uint64. Used only by the **framework** for monotonic block routing. Big-endian preserves the sort order of raw key bytes, which is what makes block routing monotonic. |
| `k0` | The same first 8 bytes interpreted as **little-endian** uint64 (= `ReverseBytes64(prefix)`). Used by **algorithms** for slot/bucket computation. Little-endian is the native integer format on most CPUs, avoiding byte-swap overhead in hash arithmetic. |
| `k1` | Bytes 8-15 of a key as **little-endian** uint64. Used by **algorithms** for slot/bucket computation. Completely independent of `prefix` — different bytes of the key. |
| Block | A self-contained group of keys on disk; exactly one block's metadata is read per MPHF query. |
| Bucket | A partitioning mechanism used by MPHF algorithms to divide keys into small groups (averaging ~3 keys) that can each be solved independently. Constructing a perfect hash for all keys at once would be computationally infeasible; buckets make the problem tractable by breaking it into many trivial subproblems. Keys are assigned to buckets based on their hash values, and the algorithm searches for a seed/pilot for each bucket that produces collision-free slot assignments. |
| Slot | Position within a block (0 to keysInBlock-1). Output of the algorithm for a specific key. |
| Rank | Final MPHF output (0 to N-1). Computed as `keysBefore + localSlot`. |
| `numBlocks` | Total blocks in the index. Determined by the algorithm based on its own parameters. |

Note: `prefix` and `k0` are two different integer interpretations of the **same 8 bytes** (bytes 0-7 of the key). The framework needs big-endian (`prefix`) so that sorted key bytes produce sorted integers for monotonic block routing. The algorithm needs little-endian (`k0`) because it's the native CPU format for arithmetic. They contain the same information — `prefix = ReverseBytes64(k0)` — but serve different purposes at different layers.

### 2.2. Core Concepts & Two-Level Dispatch

Before diving into implementation details, it's helpful to understand why StreamHash is designed this way and what the key abstractions mean.

**What an MPHF does:** A Minimal Perfect Hash Function maps N keys to exactly N consecutive slots [0, N) with no collisions. Given a key, it returns a unique rank that can be used to index into an array or file.

**Why monolithic algorithms are problematic at scale:** Traditional MPHF algorithms keep all state in RAM during construction. For a billion keys, this means gigabytes of RAM. Some algorithms (like RecSplit) also access multiple non-contiguous regions during queries, causing random I/O.

**How StreamHash solves this:** StreamHash partitions keys into **blocks** — fixed-size groups of a few thousand keys each. Each block is:
- Solved independently (enabling bounded RAM and parallelism)
- Stored contiguously on disk (enabling single-read queries)
- Small enough to fit metadata in one page (~1 KB for Bijection)

**Two-level dispatch model:** StreamHash uses a **two-level dispatch** architecture that separates framework concerns from algorithm-specific logic:

```
Query(key) → Framework routes key to block → Algorithm computes slot within block
```

**Level 1: Framework (block routing)** — The framework handles:
- Extracts `prefix = BigEndian.Uint64(key[0:8])` for routing
- Computes `blockIdx = fastRange32(prefix, numBlocks)` to select a block
- Looks up block metadata via an in-memory index (see §4.4)
- Manages file layout, payloads, fingerprints, and parallelism

**How fastRange32 works:** `fastRange32` maps a hash uniformly to [0, n) without modulo bias. The naive `hash % n` introduces non-uniform distribution when n is not a power of two; `fastRange32` avoids this using 128-bit multiplication:

```
function fastRange32(hash: uint64, n: uint32) → uint32:
    hi, lo = Mul128(hash, uint64(n))
    return uint32(hi)
```

This is monotonic: sorted prefixes map to non-decreasing block indices. This property enables streaming construction — keys can be processed in prefix-sorted order, completing one block at a time.

**How numBlocks is determined:** The framework asks the algorithm for `numBlocks` at construction time. Each algorithm computes this from its own parameters (lambda — the target average number of keys per bucket (3.0 for Bijection, 3.16 for PTRHash), and bucketsPerBlock — the number of buckets per block; see §6 and §7 for algorithm-specific values):

```
totalBuckets = ceil(N / lambda)
numBlocks    = max(2, ceil(totalBuckets / bucketsPerBlock))
```

For example, with Bijection (lambda=3, bucketsPerBlock=1024): 10M keys → ~3.33M buckets → 3,256 blocks of ~3,072 keys each.

Note: Keys in the same block share similar prefix values, but k1 (bytes 8-15) is independent of prefix, and slot computation depends on both k0 and k1 for full 128-bit collision resistance.

**Level 2: Algorithm (slot computation)** — Within the selected block, the algorithm:
- Receives `k0 = LittleEndian.Uint64(key[0:8])` and `k1 = LittleEndian.Uint64(key[8:16])`
- Assigns keys to local positions using **buckets** — small groups averaging ~3 keys
- Searches for a **seed** (also called a **pilot**) for each bucket that makes the hash collision-free
- Computes the local **slot** index (position 0 to keysInBlock-1) for the key
- Each algorithm has its own bucket organization, seed search, and metadata encoding

**Computing the final rank:** The framework tracks how many keys came before each block. The final MPHF output for a key is:
```
rank = keysBefore + localSlot
```

This gives each key a globally unique rank in [0, N).

**Why this separation?** The two-level architecture means:
- New algorithms can be plugged in without changing the framework
- Algorithms only see bounded subsets of keys (a single block at a time)
- Query locality is guaranteed by the framework's file layout

### 2.3. Data Flow Overview

```
                    +----------------------------------------+
                    |           Framework                    |
                    |                                        |
    key ----------->|  prefix = BE.Uint64(key[0:8])          |
                    |  blockIdx = fastRange32(               |
                    |      prefix, numBlocks)                |
                    |                                        |
                    |  k0 = LE.Uint64(key[0:8])              |
                    |  k1 = LE.Uint64(key[8:16])             |
                    |                                        |
                    +--------------------+-------------------+
                                         |
                                         v
                    +----------------------------------------+
                    |       Algorithm Decoder                |
                    |                                        |
                    |  localSlot = QuerySlot(                |
                    |      k0, k1, metadata,                 |
                    |      keysInBlock)                      |
                    |                                        |
                    +--------------------+-------------------+
                                         |
                                         v
                    globalRank = keysBefore + localSlot
```

### 2.4. Query Pseudocode (Framework Level)

This shows how the two-level dispatch works. The core query path computes the rank; optional fingerprint verification (when configured) can detect non-member keys.

**Core query path:**
```
function Query(key) → (globalRank, error):
    // Framework: parse key
    k0     = LittleEndian.Uint64(key[0:8])
    k1     = LittleEndian.Uint64(key[8:16])
    prefix = BigEndian.Uint64(key[0:8])

    // Framework: route to block
    blockIdx = fastRange32(prefix, numBlocks)

    // Framework: block index lookup (§4.4)
    entry     = ramIndex[blockIdx]
    nextEntry = ramIndex[blockIdx + 1]
    keysBefore  = entry.KeysBefore
    keysInBlock = nextEntry.KeysBefore - entry.KeysBefore

    if keysInBlock == 0:
        return error(NotFound)

    // Framework: get metadata slice
    metadataData = metadataRegion[entry.MetadataOffset : nextEntry.MetadataOffset]

    // Algorithm: compute local slot (dispatched to Bijection or PTRHash decoder)
    localSlot = decoder.QuerySlot(k0, k1, metadataData, keysInBlock)

    // Framework: compute global rank
    globalRank = keysBefore + localSlot

    return globalRank
```

**Optional fingerprint verification (when FingerprintSize > 0):**
```
    // After computing globalRank, verify fingerprint to detect non-members
    storedFP  = readFingerprint(payloadRegion, globalRank)
    expectedFP = extractFingerprintHybrid(key, k0, k1)
    if storedFP != expectedFP:
        return error(FingerprintMismatch)
```

**Fingerprint hybrid extraction:**
```
function extractFingerprintHybrid(key, k0, k1) → uint32:
    if len(key) - 16 >= fingerprintSize:
        return packBytes(key[len(key) - fingerprintSize:])
    else:
        // Unified mixer: depends on both hash halves
        h = k0 XOR (k1 × 0x517cc1b727220a95)
        return uint32(h >> 32) & mask(fingerprintSize)
```

`packBytes` interprets the given byte slice as a little-endian unsigned integer, zero-extended to uint32. For example, `packBytes([0xAB, 0xCD])` returns `0x0000CDAB`.

### 2.5. Query Locality

**Framework locality advantage:** StreamHash stores all metadata for a group of keys contiguously on disk. For example, the Bijection algorithm's metadata per block is typically ~1 KB, fitting within a single 4 KB page. This means computing the MPHF rank requires reading one contiguous region — a single page fault in the common case.

**Contrast with monolithic approaches:** A monolithic (non-StreamHash) RecSplit stores its hash function as two separate data structures — an Elias-Fano index for bucket boundaries plus a bitvector for splitting trees. A query accesses both at different offsets. For large indexes, these are on different pages. If RecSplit were adapted to StreamHash's framework, each block's metadata would be stored contiguously, inheriting the framework's locality advantage.

**PTRHash query locality:** PTRHash achieves O(1) queries by design — each query reads a single pilot byte. This is inherent to PTRHash's algorithm, not a StreamHash contribution.

**Payload mode:** When payloads or fingerprints are configured, queries always require two reads (metadata region + payload region) since these are at different file offsets.

### 2.6. Build Parallelism

Monolithic MPHF algorithms must solve the entire key space as a single unit — the hash function for key N depends on decisions made for keys 1 through N-1, preventing meaningful parallelization of construction.

StreamHash's block model eliminates this dependency. Each block's MPHF is solved independently: the algorithm sees only the keys routed to that block and produces a self-contained metadata blob. This enables a pipeline architecture where multiple workers solve blocks in parallel while a single coordinator writes output in block order.

The pipeline is not embarrassingly parallel — the coordinator serializes file writes — but the serialized output step (writing metadata bytes and folding hashes) is fast relative to MPHF solving (the CPU-bound step). As a result, throughput scales near-linearly with worker count: 4 workers achieve ~3–4× single-threaded throughput (see §1.6).

Queries are also naturally parallel and lock-free: the index is immutable after construction, and each query reads an independent block. No synchronization is needed between concurrent queries.

### 2.7. Worked Example

Tracing a single key through the entire system (Bijection algorithm, 10M keys). This example references Bijection-specific concepts (Elias-Fano, Golomb-Rice, buckets with lambda=3) defined in §6 — skip ahead to §6 first if the details are unfamiliar, or read this at a high level to see how the framework and algorithm layers interact:

```
Key (32 bytes, hex): 7A 3F B8 01 CC 55 D2 E9  4B 11 8A F7 63 20 DE A4  ...

Step 1: Parse key
  prefix = BigEndian.Uint64(key[0:8])  = 0x7A3FB801CC55D2E9
  k0     = LittleEndian.Uint64(key[0:8])  = 0xE9D255CC01B83F7A
  k1     = LittleEndian.Uint64(key[8:16]) = 0xA4DE2063F78A114B

Step 2: Block routing (numBlocks = 3,256 for 10M keys with Bijection)
  blockIdx = fastRange32(0x7A3FB801CC55D2E9, 3256)
           = uint32(Hi64(0x7A3FB801CC55D2E9 × 3256))
           = 1554

Step 3: Block index lookup (§4.4)
  entry     = ramIndex[1554]  →  KeysBefore=4773000, MetadataOffset=1545912
  nextEntry = ramIndex[1555]  →  KeysBefore=4776003, MetadataOffset=...
  keysInBlock = 4776003 - 4773000 = 3003

Step 4: Read metadata (one contiguous region, ~920 bytes for this block)
  metadataData = metadataRegion[1545912 : ...]

Step 5: Bijection slot computation
  localBucket = fastRange32(k0, 1024) = fastRange32(0xE9D255CC01B83F7A, 1024)
              = 935

  Decode Elias-Fano → cumulative[934] = 2827, cumulative[935] = 2830
  bucketStart = 2827, bucketSize = 3

  Decode Golomb-Rice seed for bucket 935 → seed = 5

  slot = Mix(k0, k1, seed=5, bucketSize=3, globalSeed)
       = fastRange32(wymix(k0 ^ globalSeed ^ 5, k1 ^ globalSeed), 3)
       = 1

  localSlot = bucketStart + slot = 2827 + 1 = 2828

Step 6: Global rank
  globalRank = keysBefore + localSlot = 4773000 + 2828 = 4775828

Step 7: Payload lookup (if configured)
  payloadOffset = payloadRegionOffset + 4775828 × entrySize
  → read payload from this offset (second disk read)
```

---

## 3. Framework / Algorithm Contract

StreamHash is designed as an extensible framework. The MPHF algorithm is pluggable: the framework handles key routing, file layout, parallelism, and payload management, while each algorithm provides a solver (build-time) and a decoder (query-time). Currently two algorithms are implemented:

- **Bijection** (AlgoBijection = 0): EF/GR encoding, O(128) checkpoint-based queries, ~2.46 bits/key
- **PTRHash** (AlgoPTRHash = 1): 8-bit pilots with cuckoo hashing, O(1) metadata decode, ~2.70 bits/key

### 3.1. Build-Time Solver

At build time, the framework feeds keys to the algorithm one block at a time. The algorithm must:

1. **Declare how many blocks it needs** — given the total key count, compute `numBlocks` based on its own parameters (lambda, bucketsPerBlock, etc.)
2. **Accept keys incrementally** — accumulate keys for the current block as the framework routes them
3. **Solve the MPHF for one block** — find parameters (seeds, pilots) that map the block's keys to unique slots in [0, keysInBlock)
4. **Serialize the solution** — encode all query-time state into a flat metadata blob
5. **Reset for the next block** — clear state while retaining allocated buffers for reuse

The solver is **not thread-safe**. In parallel builds, each worker thread creates its own solver instance.

### 3.2. Query-Time Decoder

At query time, the framework reads a block's metadata from disk and passes it to the algorithm's decoder. The decoder must:

1. **Compute the local slot** — given `(k0, k1, metadata, keysInBlock)`, return the slot index in [0, keysInBlock)

Fingerprint extraction is handled at the framework level using a unified mixer (see §2.6), not by the decoder.

The decoder is **thread-safe** and created once at index open time.

### 3.3. What the Framework Provides

| Responsibility | Owner |
|----------------|-------|
| Key length validation (≥ 16 bytes) | Framework |
| Block routing (`fastRange32(prefix, numBlocks)`) | Framework |
| Block index management (§4.4) | Framework |
| File layout (header, regions, footer) | Framework |
| Payload/fingerprint storage and retrieval | Framework |
| Parallel construction coordination | Framework |
| Unsorted input temp file management | Framework |
| Streaming integrity hashing (hash-of-hashes) | Framework |

### 3.4. What Algorithms Must Provide

| Responsibility | Owner |
|----------------|-------|
| Number of blocks needed | Algorithm |
| Bucket organization within blocks | Algorithm |
| MPHF solving (seed/pilot search) | Algorithm |
| Metadata encoding and decoding | Algorithm |
| Slot computation at query time | Algorithm |
| Fingerprint extraction strategy (for 16-byte keys) | Algorithm |
| Duplicate key detection during solving | Algorithm |

### 3.5. Algorithm Requirements

Any MPHF algorithm can be plugged into the framework, provided it satisfies these requirements:

1. **Must produce a minimal perfect hash** — the decoder must return slots covering exactly `[0, keysInBlock)` with no gaps. The framework indexes payloads directly at `globalRank × entrySize`, so any gap corrupts payload reads. (Non-minimal algorithms can participate by including their own remap table, as PTRHash does.)

2. **Must work with only k0 and k1** (128 bits per key) — The algorithm receives `k0 = LE.Uint64(key[0:8])` and `k1 = LE.Uint64(key[8:16])`, not the full key. Algorithms needing more entropy would require framework changes.

3. **Must encode all query state into a flat metadata blob** — The algorithm writes metadata during build, and must reconstruct the hash function from that metadata at query time using only `(k0, k1, metadata, keysInBlock)`.

4. **Must handle variable and empty blocks** — Block sizes vary due to Poisson routing (keys are independently and uniformly assigned to blocks, so keys-per-block follows a Poisson distribution). The algorithm must work for any `keysInBlock` from 0 to the Poisson tail. For empty blocks (`keysInBlock = 0`), the framework short-circuits before calling the decoder, but the solver's reset must handle the transition cleanly.

**Within-block key correlations:** Because the framework routes keys by `prefix = BigEndian.Uint64(key[0:8])`, keys in the same block share similar prefix values, which means byte 0 of the key is approximately fixed within a block. In little-endian, this constrains only the *least* significant byte of k0 — the high bytes remain effectively random. Meanwhile, k1 (bytes 8-15) is completely independent of the routing prefix. In practice, neither current algorithm is affected: Bijection uses `fastRange32(k0, ...)` which is dominated by high bits, and PTRHash uses k1 for bucket assignment. However, algorithm implementors should be aware of this property — if needed, the algorithm can internally re-hash (k0, k1) to produce fully independent values.

**Prefix routing requires uniform input.** Block assignment depends on the key prefix distribution. If keys are highly non-uniform (e.g., all keys share a common prefix), blocks will be severely imbalanced — in the worst case collapsing to a single block. This is why §1.5 requires pre-hashing for non-random input: pre-hashing with a 128-bit hash (see Appendix A) ensures the prefix is uniformly distributed, which in turn ensures balanced block assignment.

**Framework extensibility:** Other algorithms can be added beyond Bijection and PTRHash. For example, RecSplit could be adapted to achieve close to ~1.56 bits/key with the same bounded-RAM streaming construction — the algorithm only needs a build-time solver and a query-time decoder (see §3.1 and §3.2). (The standalone RecSplit figure is ~1.56 bits/key; framework overhead from the per-block index and fixed costs would add a small amount, likely ~1.6–1.7 bits/key in practice.)

### 3.6. Adding a New Algorithm

To add a new algorithm, you need to provide:

1. A **solver** that accepts keys for a block and produces a metadata blob encoding the MPHF solution
2. A **decoder** that reads the metadata blob and computes slot indices at query time
3. A new `BlockAlgorithmID` constant registered in the header's algorithm field

The framework handles everything else: routing, file layout, parallelism, temp files.

The `AlgoConfig` section in the file layout (variable-length section after header) supports per-algorithm configuration. Currently empty for both algorithms, but designed for future algorithms with tunable parameters.

**Concrete example — Streaming RecSplit:**
- RecSplit recursively splits key sets using bijection-like seed search
- A streaming RecSplit solver would accept keys for a block, build RecSplit's recursive splitting tree, and serialize it as the block's metadata blob
- The decoder would reconstruct the splitting tree from the metadata and compute slot indices
- `numBlocks` would be computed based on RecSplit's natural partition sizes
- The framework handles routing, file layout, parallelism — unchanged
- This would achieve close to ~1.56 bits/key with the same bounded-RAM streaming construction (likely ~1.6–1.7 bits/key after framework overhead from the block index and per-block fixed costs) — the ~0.8–0.9 bit/key difference between Bijection and a streaming RecSplit is a build-speed tradeoff, not a framework limitation
- Note: RecSplit would need to handle the within-block key correlation (shared prefix bits) — if its internal hash functions are sensitive to this, it can re-hash (k0, k1) to produce fully independent values

---

## 4. File Format

The file format is language-independent. The byte layouts, encoding formats, and hash functions specified below are sufficient to produce interoperable files from any implementation language. Go-specific terminology in this document refers to the reference implementation.

### 4.1. File Layout

```
+--------------------------------------------------+
|  Header (64 B)                                   |
|  Magic, Version, TotalKeys, NumBlocks, Seed, ... |
+--------------------------------------------------+
|  UserMetadataLen (4 B) + UserMetadata (variable) |
+--------------------------------------------------+
|  AlgoConfigLen (4 B) + AlgoConfig (variable)     |
+--------------------------------------------------+
|  RAM Index  ((NumBlocks+1) x 10 B)               |
|  [KeysBefore: 5B | MetadataOffset: 5B] per block |
+--------------------------------------------------+
|  Payload Region  (N x entrySize B)               |
|  [FP | Payload] per key, at globalRank position  |
+--------------------------------------------------+
|  Metadata Region  (variable)                     |
|  Per-block metadata, variable-length, in order   |
+--------------------------------------------------+
|  Footer (32 B)                                   |
|  PayloadRegionHash, MetadataRegionHash           |
+--------------------------------------------------+
```

**Detailed offset table:**

```
Offset                              Size              Content
---------------------------------------------------------------------
0                                   64 B              Header
64                                  4 B               UserMetadataLen (uint32_le)
68                                  variable          UserMetadata
68+UML                              4 B               AlgoConfigLen (uint32_le)
68+UML+4                            variable          AlgoConfig
68+UML+4+ACL                        (NumBlocks+1)x10 B RAM Index
ramIndexEnd                         N x entrySize B   Payload Region
payloadEnd                          variable          Metadata Region
metadataEnd                         32 B              Footer
```

where:
- `UML` = UserMetadataLen
- `ACL` = AlgoConfigLen
- `N` = TotalKeys
- `entrySize` = PayloadSize + FingerprintSize

### 4.2. Header (64 bytes)

| Offset | Size | Field | Type | Description |
|--------|------|-------|------|-------------|
| 0 | 4 | Magic | uint32_le | `0x53544D48` ("STMH") |
| 4 | 2 | Version | uint16_le | `0x0001` |
| 6 | 8 | TotalKeys | uint64_le | Total number of keys (N) |
| 14 | 4 | NumBlocks | uint32_le | Number of blocks |
| 18 | 4 | RAMBits | uint32_le | R = ceil(log2(NumBlocks)) |
| 22 | 4 | PayloadSize | uint32_le | Payload bytes per key (0 = MPHF only) |
| 26 | 1 | FingerprintSize | uint8 | Fingerprint bytes (0-4) |
| 27 | 8 | Seed | uint64_le | Global seed for hash functions |
| 35 | 2 | BlockAlgorithm | uint16_le | 0=Bijection, 1=PTRHash |
| 37 | 27 | Reserved | bytes | Zero-filled |

**Note:** `TotalBuckets`, `BucketsPerBlock`, and `PrefixBits` are **not in the header**. These are algorithm-internal constants. Each algorithm derives bucket counts from `NumBlocks` using its own `lambda` and `bucketsPerBlock`.

### 4.3. Variable-Length Sections

Immediately after the 64-byte header:

**UserMetadata:**
```
[UserMetadataLen: uint32_le][UserMetadata: UserMetadataLen bytes]
```
Application-defined data. Not interpreted by the index. Can be zero-length.

**AlgoConfig:**
```
[AlgoConfigLen: uint32_le][AlgoConfig: AlgoConfigLen bytes]
```
Algorithm-specific configuration. Currently zero-length for both Bijection and PTRHash (they use compile-time constants). Future algorithms may store tunable parameters here.

**Hex dump example:** Here's what a minimal index looks like for 5 keys in 2 blocks using Bijection (MPHF mode, no fingerprints):

```
Offset  Hex bytes                                       Description
──────────────────────────────────────────────────────────────────────
0000    48 4D 54 53 01 00 05 00 00 00 00 00 00 00      Header: Magic "STMH", Version 1
0010    02 00 00 00 01 00 00 00 00 00 12 34 56 78      NumBlocks=2, RAMBits=1, PayloadSize=0
0020    9A BC DE F0 00 00 00 00 00 00 00 00 00 00      FingerprintSize=0, Seed=0x..., BlockAlgorithm=0
0030    00 00 00 00 00 00 00 00 00 00 00 00 00 00      Reserved (zeros)
0040    00 00 00 00                                    UserMetadataLen = 0 (no user metadata)
0044    00 00 00 00                                    AlgoConfigLen = 0 (no algo config)
0048    00 00 00 00 00 00 00 00 00 00                 RAM Index entry 0: KeysBefore=0, MetadataOffset=0
0052    03 00 00 00 00 A5 00 00 00 00                 RAM Index entry 1: KeysBefore=3, MetadataOffset=0xA5
005C    05 00 00 00 00 42 01 00 00 00                 RAM Index entry 2 (sentinel): KeysBefore=5, MetadataOffset=0x142
0066    (empty payload region, PayloadSize=0)
0066    1C 00 1C 00 ... (Bijection metadata block 0)   Block 0 metadata (~165 bytes)
010B    1C 00 1C 00 ... (Bijection metadata block 1)   Block 1 metadata (~221 bytes)
01E6    [32 bytes footer with hashes]                  Footer
```

This example shows:
- Header at offset 0 (64 bytes)
- UserMetadataLen at 0x40 (4 bytes, value = 0)
- AlgoConfigLen at 0x44 (4 bytes, value = 0)
- RAM Index at 0x48 (3 entries × 10 bytes = 30 bytes)
- Empty payload region (no payloads in MPHF mode)
- Metadata region starts immediately with block 0's metadata
- Footer at the end

### 4.4. RAM Index

The RAM index contains `NumBlocks + 1` entries (the extra entry is a sentinel entry — an extra entry beyond the last real block, used to compute the last block's size by subtraction).

**Entry format (10 bytes):**

| Offset | Size | Field | Type |
|--------|------|-------|------|
| 0 | 5 | KeysBefore | uint40_le |
| 5 | 5 | MetadataOffset | uint40_le |

- `KeysBefore`: Cumulative count of keys in all blocks before this one
- `MetadataOffset`: Byte offset into the metadata region where this block's metadata starts

**Deriving block information at query time:**
```
entry     = ramIndex[blockIdx]
nextEntry = ramIndex[blockIdx + 1]

keysInBlock  = nextEntry.KeysBefore - entry.KeysBefore
metadataSlice = metadataRegion[entry.MetadataOffset : nextEntry.MetadataOffset]
```

**Payload offset:** Payloads are at fixed offsets in the payload region:
```
payloadOffset = globalRank × entrySize
```

This avoids storing payload offsets in the RAM index.

**RAM index is an optimization, not a requirement.** The file format supports queries without loading the RAM index into memory. The RAM index is at a fixed, computable file offset. A query can read the two needed entries (20 bytes, contiguous) directly from disk. This adds one disk read: MPHF mode goes from 1 to 2 reads, payload mode from 2 to 3. The RAM index is small (10 bytes/block, e.g., ~32 KB for 10M keys with Bijection) so keeping it in memory is cheap, but not mandatory.

### 4.5. Payload Region

Payloads are stored contiguously in a separated region. Each entry is `PayloadSize + FingerprintSize` bytes, stored at the key's global rank position:

```
entryOffset = payloadRegionOffset + globalRank × (PayloadSize + FingerprintSize)
```

Entry layout: `[Fingerprint: FingerprintSize bytes][Payload: PayloadSize bytes]`

Both fingerprints and payloads are stored in little-endian byte order within each entry. For example, a 4-byte payload with value 0x12345678 is stored as [0x78, 0x56, 0x34, 0x12].

Fingerprints are stored first within each entry for efficient access during verification.

### 4.6. Metadata Region

Contains per-block metadata in block order. Each block's metadata is variable-length and algorithm-specific:

- **Bijection:** Checkpoints (28B, see §5.7) + Elias-Fano data (a succinct representation for monotonically increasing integer sequences — see §5.5) + Golomb-Rice seed stream (a variable-length encoding efficient for geometrically distributed values — see §5.6) + fallback list
- **PTRHash:** Pilot bytes (bucketsPerBlock bytes) + remap table

**Empty blocks** (keysInBlock = 0) still have metadata entries:

- **PTRHash**: bucketsPerBlock zero bytes (all-zero pilots) + uint16_le(0) (empty remap table) = 10,002 bytes.
- **Bijection**: 28-byte zero checkpoints + Elias-Fano encoding for 1024 zero-valued cumulatives (128 bytes) + 1 zero byte (empty seed stream) = 157 bytes.

### 4.7. Footer (32 bytes)

| Offset | Size | Field | Type |
|--------|------|-------|------|
| 0 | 8 | PayloadRegionHash | uint64_le |
| 8 | 8 | MetadataRegionHash | uint64_le |
| 16 | 16 | Reserved | bytes |

Both hashes use canonical (unseeded) xxHash64:

- **MetadataRegionHash**: `xxHash64(metadataRegion)` — single pass over the raw metadata bytes from metadata region start to footer start.

- **PayloadRegionHash**: Hash-of-hashes computed as follows:
  ```
  hasher = xxHash64_streaming()
  for blockID in 0..NumBlocks-1:
    startKey = ramIndex[blockID].KeysBefore
    endKey   = ramIndex[blockID+1].KeysBefore
    if endKey > startKey AND entrySize > 0:
      blockPayloads = payloadRegion[startKey×entrySize : endKey×entrySize]
      blockHash = xxHash64(blockPayloads)
    else:
      blockHash = xxHash64(empty)     // deterministic: 0xEF46DB3751D8E999
    hasher.Write(LittleEndian.Bytes8(blockHash))
  PayloadRegionHash = hasher.Sum64()
  ```
  Each block's payload bytes are hashed independently, then the 8-byte little-endian encoding of each block hash is fed sequentially into a streaming xxHash64 hasher.

---

## 5. Algorithm: Bijection

### 5.1. Overview

The Bijection algorithm combines several well-known MPHF techniques:

- **Per-bucket bijection solving** (brute-force seed search) — a technique used in CHD ([Belazzougui, Botelho & Dietzfelbinger, 2009](https://cmph.sourceforge.net/papers/esa09.pdf)) and RecSplit ([Esposito, Graf & Vigna, 2020](https://epubs.siam.org/doi/pdf/10.1137/1.9781611976007.14))
- **Elias-Fano encoding** for cumulative bucket sizes and **Golomb-Rice coding** for seeds — standard succinct/compressed encoding techniques, applied to MPHF seed storage by RecSplit
- **Hierarchical splitting** for large buckets (see §6.4) — inspired by RecSplit's recursive splitting

Where Bijection diverges from RecSplit: it replaces RecSplit's tree-based recursive structure with a flat per-bucket layout plus 128-bucket checkpoints for O(128) query decode. A key design goal of Bijection was **faster build times** — the flat structure with small buckets (lambda=3) enables rapid seed search without RecSplit's expensive recursive splitting, trading space efficiency (~2.46 vs ~1.6–1.7 bits/key for a streaming RecSplit) for significantly faster construction.

**Parameters:**
- `lambda = 3.0` (average keys per bucket)
- `bucketsPerBlock = 1024` — chosen to balance metadata size, query decode cost, and block count. At lambda=3, each block holds ~3,072 keys. With 128-bucket checkpoints, query decode scans at most 128 buckets. Increasing `bucketsPerBlock` reduces the number of blocks (and RAM index size) but increases per-block metadata and decode cost. 1024 keeps metadata per block to ~1 KB typical, fitting within a single 4 KB page.
- `splitThreshold = 8` (buckets ≥ 8 keys use splitting)
- `checkpointInterval = 128`

### 5.2. Bucket Assignment

Bijection uses **uniform bucket assignment** via `fastRange32`:

```
localBucket = fastRange32(k0, bucketsPerBlock)

where k0 = LittleEndian.Uint64(key[0:8])
```

### 5.3. Mix Function

The Mix function computes a slot index within a bucket. The goal is: given a seed, map each key to a slot in [0, bucketSize) such that different seeds produce different slot assignments. The solver tries seeds sequentially until it finds one where all keys in the bucket land on distinct slots (a bijection).

```
function Mix(k0, k1, seed, bucketSize, globalSeed) → slot:
    mixed = wymix(k0 ^ globalSeed ^ seed, k1 ^ globalSeed)
    return fastRange32(mixed, bucketSize)
```

The XOR structure `(k0 ^ globalSeed ^ seed, k1 ^ globalSeed)` ensures that each seed value produces a different pair of inputs to `wymix`. The `globalSeed` decorrelates builds from each other (so a different globalSeed produces a completely different index). The per-bucket `seed` is the search variable — the solver increments it until no collisions occur.

`wymix(a, b)` is the WyHash v4 mixing primitive: a 128-bit multiply with XOR fold. It computes `hi, lo = Mul128(a, b); return hi ^ lo`. The 128-bit multiply provides strong avalanche — small changes in either input (e.g., incrementing the seed by 1) produce uncorrelated outputs, which is essential for the brute-force seed search to converge quickly.

### 5.4. Bijection Solving

For each bucket, find a seed that makes the Mix function a bijection (no slot collisions):

**Direct buckets (size 2-7):** Try seeds sequentially until `Mix(k0, k1, seed, bucketSize, globalSeed)` produces distinct slots for all keys in the bucket.

**Split buckets (size ≥ 8):**
1. Find `seed0` such that `Mix(k0, k1, seed0, bucketSize, globalSeed)` produces exactly `splitPoint = bucketSize / 2` keys with slot < splitPoint (forming a bijection on the first half)
2. Find `seed1` for the second half using standard bijection solving

**Fallback:** Seeds that exceed Golomb-Rice encoding limits are stored in a fallback list at the end of the metadata block.

### 5.5. Elias-Fano Encoding

Cumulative bucket sizes are encoded using Elias-Fano. This is a succinct representation of a monotonically increasing sequence:

All bit-packed data (Elias-Fano and Golomb-Rice) uses **LSB-first** packing within 64-bit little-endian words. Bit 0 is the least significant bit of the first byte.

```
n = bucketsPerBlock (1024)
U = keysInBlock

lowBits = floor(log2(floor(U / n)))    // when U > n, else 0  (integer division)

Lower bits: n × lowBits bits (packed, LSB first)
Upper bits: n + (U >> lowBits) bits (unary gaps between values)
```

**Worked example:** Encoding cumulative bucket sizes for a small block with 8 buckets and 12 keys:

```
Bucket sizes: [2, 1, 3, 0, 2, 1, 1, 2]
Cumulative:   [0, 2, 3, 6, 6, 8, 9, 10, 12]

U = 12, n = 8
lowBits = floor(log2(12/8)) = floor(log2(1)) = 0

Since lowBits = 0, all bits go to the upper bits (unary encoding):
Upper bits represent gaps between cumulative values:
  Gap from 0→2: 2 zeros, then 1
  Gap from 2→3: 1 zero, then 1
  Gap from 3→6: 3 zeros, then 1
  ... and so on

Upper bits: 0010110001010011  (n + U/2^lowBits bits = 8 + 12 = 20 bits)
Lower bits: (empty, since lowBits = 0)
```

For larger blocks with more keys, lowBits > 0, and the lower bits store the low-order bits of each cumulative value while the upper bits store the high-order bits in unary encoding.

### 5.6. Golomb-Rice Seed Encoding

Seeds are encoded using Golomb-Rice coding with parameter `k` derived from bucket size:

```
k = golombParameter(bucketSize)

quotient  = seed >> k
remainder = seed & ((1 << k) - 1)

Encode: q ones + 0 terminator + k remainder bits

Fallback marker: 16 consecutive ones (indicates seed is in fallback list)
```

**Golomb-Rice parameter table** (indexed by bucket size):

| Bucket Size | 0 | 1 | 2 | 3 | 4 | 5 | 6 | 7 | ≥8 |
|-------------|---|---|---|---|---|---|---|---|-----|
| k           | 0 | 0 | 1 | 2 | 3 | 4 | 5 | 7 | 8   |

Size-0 and size-1 buckets emit nothing in the seed stream (always seed=0). The maximum quotient before fallback is 15 (since 16 ones = fallback marker), so the maximum encodable seed is `(16 << k) - 1`. Seeds exceeding this are stored in the fallback list.

**Worked example:** Encoding seed value 13 for a bucket of size 3:

```
bucketSize = 3
k = golombParameter(3) = 2
seed = 13

quotient  = 13 >> 2 = 3
remainder = 13 & ((1 << 2) - 1) = 13 & 3 = 1

Encoding: 3 ones + 0 terminator + 2 remainder bits
Binary: 1110  (3 ones + 0) followed by 01 (remainder = 1 in 2 bits)
Result: 111001  (6 bits total)
```

Another example with a larger seed requiring fallback:

```
bucketSize = 3, k = 2
Maximum encodable: (16 << 2) - 1 = 63
seed = 70  (exceeds maximum)

Encoding: 16 consecutive ones (fallback marker)
Binary: 1111111111111111  (16 bits)
The actual seed value (70) is stored in the fallback list at the end of the metadata block.
```

**Split buckets (size ≥ 8):** Emit two consecutive Golomb-Rice codes in the seed stream. The first code encodes `seed0` using `golombParameter(splitPoint)` where `splitPoint = bucketSize / 2`. The second encodes `seed1` using `golombParameter(bucketSize - splitPoint)`. Either or both may be fallback markers (16 consecutive ones) if the seed exceeds the encodable range.

**Note:** Golomb-Rice encoding only covers bucket sizes 2–8. For split buckets (size ≥ 8), sub-buckets larger than 8 keys force a fallback marker.

### 5.7. Metadata Format (Bijection)

```
Offset  Size          Content
───────────────────────────────────────────
0       28 B          Checkpoints (7 × uint16 EF positions + 7 × uint16 seed positions)
28      variable      Elias-Fano data (cumulative bucket sizes)
28+EF   variable      Golomb-Rice seed stream
end-FL  variable      Fallback list (see below)
```

**Checkpoints** are at 128-bucket intervals, enabling O(128) decode: skip directly to the segment containing the target bucket.

**Fallback list encoding:**

```
Format: [count: uint8][entries: count × 4 bytes][validation: uint8]

validation = count XOR 0x55  (enables reliable detection when searching backwards)
```

Each 4-byte entry packs three fields into a uint32_le using `blockBits = ceil(log2(bucketsPerBlock))` (= 10 for bucketsPerBlock=1024):

```
seedBits = 31 - blockBits

packed = (bucketIndex << (1 + seedBits)) | (subBucket << seedBits) | seed

Fields:
  bucketIndex  [blockBits bits]   Which bucket this fallback is for
  subBucket    [1 bit]            0 = first half, 1 = second half (for split buckets)
  seed         [seedBits bits]    The actual seed value
```

At decode time, the fallback list is located by searching backwards from the end of metadata for a valid `count`/`validation` pair.

### 5.8. Query (Bijection)

```
function QuerySlotBijection(k0, k1, metadata, keysInBlock) → localSlot:
    // Step 1: Compute bucket
    bucketIdx = fastRange32(k0, 1024)

    // Step 2: Decode checkpoints
    cp = decodeCheckpoints(metadata[0:28])
    segment = bucketIdx / 128

    // Step 3: Decode Elias-Fano to get bucket start/size
    bucketStart = cumulative[bucketIdx - 1]   // via EF + checkpoint
    bucketEnd   = cumulative[bucketIdx]
    bucketSize  = bucketEnd - bucketStart

    // Step 4: Decode seed(s) from Golomb-Rice stream (skip to segment via checkpoint)
    if bucketSize >= 8:
        seed0, seed1 = decodeSplitSeeds(...)
        h = Mix(k0, k1, seed0, bucketSize, globalSeed)
        splitPoint = bucketSize / 2
        // seed0 was chosen at build time so that exactly splitPoint keys produce h < splitPoint
        if h < splitPoint:
            localSlot = bucketStart + h
        else:
            localSlot = bucketStart + splitPoint + Mix(k0, k1, seed1, bucketSize - splitPoint, globalSeed)
    else if bucketSize >= 2:
        seed = decodeSeed(...)
        localSlot = bucketStart + Mix(k0, k1, seed, bucketSize, globalSeed)
    else:
        localSlot = bucketStart

    return localSlot
```

### 5.9. Fingerprint Extraction

Fingerprint extraction is handled at the framework level, not per-algorithm. See §2.6 for the
unified hybrid extraction strategy using a mixer over both k0 and k1.

---

## 6. Algorithm: PTRHash Adaptation

### 6.1. Overview

StreamHash's PTRHash adaptation is based on PtrHash by [Groot Koerkamp](https://github.com/RagnarGrootKoerkamp/PtrHash) ([paper](https://arxiv.org/abs/2502.15539)), with several modifications for streaming construction (see §6.9–§6.13).

PTRHash is normally built as a monolithic O(N)-RAM structure over millions of keys per part. StreamHash adapts it for streaming construction with small, fixed-size blocks (~31,600 keys each), enabling bounded-RAM builds while preserving PTRHash's O(1) query performance.

**Understanding pilots:** In PTRHash, a **pilot** is a small integer (0-255) assigned to each bucket that acts as a "steering parameter." When the algorithm processes a key, it (1) identifies which bucket the key belongs to, (2) retrieves that bucket's pilot value, and (3) combines the key and pilot to compute the final slot position. The pilot value is chosen during construction by trying different values until one is found that maps all keys in the bucket to collision-free slots. You can think of the pilot as guiding its bucket's keys into available positions in the output array, avoiding collisions with keys from other buckets.

The adaptation uses CubicEps bucket distribution (a skewed assignment that creates a few large buckets and many small ones — see §6.2), 8-bit pilots (0-255) with cuckoo hashing (a hash table technique where items can be displaced to alternative positions to resolve collisions) for collision resolution, and a remap table for overflow slots. Queries decode pilots directly (no EF/GR).

**Parameters:**
- `lambda = 3.16` (average keys per bucket)
- `alpha = 0.99` (the slot overflow factor; numSlots = ceil(keysInBlock / alpha), so α = 0.99 means 1% extra slots)
- `bucketsPerBlock = 10,000`
- `numPilotValues = 256` (pilots 0-255)

**Example:** For a block with 31,600 keys:
- **Buckets:** 10,000 buckets → average 3.16 keys/bucket (31,600 ÷ 10,000)
- **Slots:** ceil(31,600 / 0.99) = 31,920 slots (1% overflow for collision resolution)
- **Metadata size:** 10,000 pilot bytes (one per bucket) + remap table (~320 bytes for ~320 overflow slots) ≈ 10.3 KB per block
- **Largest bucket:** Due to CubicEps skew, bucket 0 gets ~1.08% of keys = ~341 keys
- **Pilot search:** Each bucket tries up to 256 pilots to find collision-free slot assignments

**Why 10,000 buckets per block?** See §6.12 for the full rationale. In brief: streaming construction requires small, fixed-size blocks. The CubicEps distribution creates a heavy-tailed largest bucket (~1.08% of keys). At 10K buckets per block (~31,600 keys), the largest bucket is ~341 keys, giving a per-block failure probability of ~4.4×10⁻¹². This figure is derived from the cuckoo placement analysis in the PtrHash paper applied to bucket 0: each of K_eff ≈ 256 independent pilots is an independent trial, so P(block fails) ≈ (1 − p)^256 where p is the single-pilot success probability for the ~341-key bucket. At 1T keys (~31.6M blocks), the expected build failure rate is 31.6M × 4.4×10⁻¹² ≈ 1/7,200.

**Why ~341 keys in bucket 0?** Due to the cubic CDF shape, bucket 0 captures a disproportionate share of keys. Empirically measured: ~1.08% of keys → ~341 keys for a 31,600-key block.

**Build algorithm:** The PTRHash build algorithm uses two-phase pilot search with cuckoo-style eviction. For the full algorithm, see the [PtrHash paper](https://arxiv.org/abs/2502.15539).

### 6.2. Bucket Assignment

PTRHash uses **CubicEps bucket assignment** for skewed bucket sizes:

```
localBucket = CubicEpsBucket(k1, bucketsPerBlock)

where k1 = LittleEndian.Uint64(key[8:16])
```

**Why skewed buckets?** PTRHash's pilot search works by finding a pilot value that maps all keys in a bucket to distinct slots. It processes buckets in decreasing size order. Cuckoo hashing — the eviction mechanism used when a pilot assignment collides with an already-placed bucket — works best when large buckets are placed first (when the slot pool is mostly empty) and small buckets fill the remaining gaps. A uniform distribution would give every bucket roughly the same size, losing this advantage.

The CubicEps distribution (`x²(1+x)/2 × 255/256 + x/256`) achieves this by creating many small buckets and a few large ones. The cubic shape concentrates keys into the lowest-numbered buckets: bucket 0 gets ~1.08% of all keys (~341 keys in a 31,600-key block), while most buckets get only 1-3 keys. The `epsilon` term (x/256) ensures every bucket receives at least some keys, preventing empty-bucket pathologies.

**Fixed-point pseudocode:**

```
function CubicEpsBucket(x: uint64, numBuckets: uint32) → uint32:
    if numBuckets <= 1:
        return 0

    // x² (high 64 bits of 128-bit product)
    x2 = Hi64(Mul128(x, x))

    // (1+x)/2 in fixed-point
    xHalf = (x >> 1) | (1 << 63)

    // x² × (1+x)/2 (high 64 bits)
    cubic = Hi64(Mul128(x2, xHalf))

    // Scale: × 255/256 + x/256
    scaled = (cubic / 256) × 255 + x / 256

    return fastRange32(scaled, numBuckets)
```

### 6.3. Pilot Hash

```
function PilotHash(pilot, globalSeed) → hp:
    x = 0x517cc1b727220a95 × (pilot ^ globalSeed)
    // SplitMix64 finalizer (a well-known PRNG finalizer by Vigna, 2015)
    x ^= x >> 30
    x *= 0xbf58476d1ce4e5b9
    x ^= x >> 27
    x *= 0x94d049bb133111eb
    x ^= x >> 31
    return x | 1   // ensure odd (bijective multiplication) and non-zero
```

**Design note:** The SplitMix64 finalizer is not present in canonical PTRHash. StreamHash adds it to ensure pilot independence at small block sizes (~31,600 keys). Without this finalizer, correlated pilots waste attempts and increase build failure probability. With it, effectively all 256 pilots produce independent hash patterns, reducing the failure rate at 1T keys from ~1 in 30 builds to ~1 in 7,200 builds. See §6.11 for detailed analysis.

### 6.4. Slot Computation

```
function PilotSlotFromHashes(k0, k1, pilot, numSlots, globalSeed) → slot:
    hp = PilotHash(pilot, globalSeed)
    slotInput = k0 ^ k1
    slot = fastRange32((slotInput ^ (slotInput >> 32)) × hp, numSlots)
    return slot
```

**Design note:** This diverges from canonical PTRHash in two ways: (1) multiplication mixing (`× hp`) replaces XOR mixing (`^ hp`) for stronger avalanche properties, and (2) `k0 ^ k1` replaces a single hash half for 128-bit collision resistance. The XOR of both key halves ensures the slot input is independent of the k1-only bucket assignment. See §6.9 and §6.10 for analysis.

Key points:
- **Slot input is `k0 ^ k1`**, not just `k1`. This ensures 128-bit collision resistance. See §6.10.
- **Multiplication mixing** (`(h ^ (h >> 32)) × hp`), not XOR (`h ^ hp`). See §6.9.
- `numSlots = ceil(keysInBlock / alpha)`

### 6.5. Remap Table

Because `alpha < 1.0`, `numSlots > keysInBlock`. Slots in `[keysInBlock, numSlots)` are "overflow" slots that must be remapped to "holes" in `[0, keysInBlock)`.

**Binary format:**
```
[Count: uint16_le][Entries: Count × uint16_le]
```

Each entry at index `i` maps overflow slot `keysInBlock + i` to a valid slot in `[0, keysInBlock)`.

At query time:
```
if slot >= keysInBlock:
    slot = remapTable[slot - keysInBlock]
```

### 6.6. Metadata Format (PTRHash)

```
Offset  Size                      Content
───────────────────────────────────────────────────
0       bucketsPerBlock bytes     Pilots (1 byte per bucket, direct storage)
BPB     2 bytes                   RemapCount (uint16_le)
BPB+2   RemapCount × 2 bytes     RemapEntries (uint16_le each)
```

where `BPB = bucketsPerBlock = 10,000`.

### 6.7. Query (PTRHash)

```
function QuerySlotPTRHash(k0, k1, metadata, keysInBlock) → localSlot:
    // Step 1: Compute local bucket
    localBucket = CubicEpsBucket(k1, 10000)

    // Step 2: Read pilot (direct byte access, O(1))
    pilot = metadata[localBucket]

    // Step 3: Compute slot
    numSlots = ceil(keysInBlock / 0.99)
    slot = PilotSlotFromHashes(k0, k1, pilot, numSlots, globalSeed)

    // Step 4: Remap if overflow
    if slot >= keysInBlock:
        remapData = metadata[10000:]
        slot = lookupRemap(remapData, slot, keysInBlock)

    return slot
```

### 6.8. Fingerprint Extraction (PTRHash)

Fingerprint extraction is handled at the framework level, not per-algorithm. See §2.6 for the
unified hybrid extraction strategy using a mixer over both k0 and k1.

### PTRHash Divergence Summary (§6.9–§6.13)

The following subsections provide detailed reference and comparison for where StreamHash's PTRHash adaptation diverges from the Rust PtrHash implementation by Groot Koerkamp. Each divergence is cross-referenced from the relevant algorithm section above and is motivated by StreamHash's small, fixed-size blocks (~31,600 keys) — substantially smaller than the Rust implementation's dynamic part sizes (millions of keys). At this smaller scale, several aspects of the original design (XOR mixing, 64-bit slot input, simple pilot hashing) become unreliable, requiring the modifications described below.

### 6.9. Divergence: Slot Mixing — Multiplication instead of XOR

| | Rust PTRHash | StreamHash |
|---|---|---|
| Formula | `FastReduce(hx.low() ^ hp)` | `fastRange32(((k0^k1) ^ ((k0^k1) >> 32)) × hp)` |
| Mixing | XOR only | Multiplication + XOR fold |

**Why:** CubicEps creates skewed bucket sizes — bucket 0 gets ~1.08% of keys (~341 in a 31,600-key block). XOR mixing cannot break correlations in these large buckets. Multiplication provides an avalanche effect:
- **MUL + k0^k1:** 100% success rate, ~8 avg pilots
- **XOR + k0^k1:** ~78% success rate, ~38 avg pilots

**Why XOR mixing fails:**

With XOR mixing (`slot = fastRange32(h ^ hp, n)`), collision *pairs* are pilot-invariant: if `fastRange32(h₁ ^ hp, n) == fastRange32(h₂ ^ hp, n)` for one pilot, the relationship `h₁ ^ hp` vs `h₂ ^ hp` is structurally similar across pilots. However, the slot *assignments* do change per pilot — which is why K_eff is 3-5 (not 1). The correlation is partial but severe enough to make large buckets (~341 keys) unsolvable with only 256 pilots.

With MUL mixing (`slot = fastRange32((h ^ (h >> 32)) × hp, n)`), the collision condition depends on hp non-linearly. Different pilots see different collision structures, making them approximately independent trials (K_eff ≈ 256).

### 6.10. Divergence: Slot Input — k0^k1 instead of hx.low()

| | Rust PTRHash | StreamHash |
|---|---|---|
| Slot input | `hx.low()` (independent half of 128-bit hash) | `k0 ^ k1` |
| Bucket input | `hx.high()` | `k1` |

**Why:** StreamHash uses the first 16 key bytes directly as k0 and k1 (no upfront hash). Originally, k1 was used for both bucket assignment AND slot computation, creating a 64-bit collision vulnerability (~0.83% failure at 100B keys). Using `k0 ^ k1` for slots ensures the slot input is independent of the k1-only bucket assignment while using bits from both halves for 128-bit collision resistance.

### 6.11. Divergence: PilotHash — SplitMix64 Finalizer

| | Rust PTRHash | StreamHash |
|---|---|---|
| Pilot hash | `C × (pilot ^ seed)` | SplitMix64 finalizer on `C × (pilot ^ seed)`, then `| 1` |
| Effectively independent pilots (measured) | ~3–5 out of 256 | ~253–259 out of 256 |

**Why:** When pilots are correlated, many pilot values attempt essentially the same hash assignments, wasting attempts and increasing build failure probability. With the SplitMix64 finalizer, effectively all 256 pilots produce independent hash patterns. Over 10,000 random key sets, ~253–259 pilots behave independently with SplitMix64, compared to 180–225 without. At 1T keys (~31.6M blocks):
- **Without SplitMix64:** ~1 in 30 builds fails
- **With SplitMix64:** ~1 in 7,200 builds fails

*Note on estimation:* The effectively independent pilot count can be estimated by fitting observed per-bucket failure rates to the model P(all 256 pilots fail) = (1 − p)^K, where p is the single-pilot success probability for the largest bucket and K is the effective count.

The `| 1` in PilotHash (§6.3) ensures the result is odd (bijective multiplication mod 2^64) and prevents `hp = 0`.

**How Rust compensates:** The Rust implementation uses the Vigna formula to compute very large part sizes (~millions of keys per part). At that scale, even with only a few effectively independent pilots, individual bucket failure probabilities are extremely small relative to the slot pool. StreamHash's small blocks (31,600 keys) require high pilot independence.

### 6.12. Divergence: Block Sizing — Fixed 10K Buckets instead of Dynamic Vigna Formula

| | Rust PTRHash | StreamHash |
|---|---|---|
| Part size | Dynamic (Vigna formula, ~millions of keys) | Fixed: 10,000 buckets (~31,600 keys) |
| Partitioning | Random (hash-based) | Deterministic (CubicEpsBucket per block) |

**Why:** Streaming construction requires small, fixed-size blocks that fit in bounded RAM. Vigna's dynamic sizing produces parts too large to fit. Small blocks enable:
- O(block) RAM per worker during construction
- Single metadata read per query
- Parallel builds (each block's MPHF is solved independently; output is sequenced by a coordinator)

The tradeoff: small blocks have higher per-block failure probability, requiring better pilot independence (solved by SplitMix64, §6.11).

### 6.13. Divergence: No Full-Key Hash (Suffix-Based Hashing)

| | Rust PTRHash | StreamHash |
|---|---|---|
| Key processing | Hash full key upfront | Use bytes 0-15 directly (k0, k1 as little-endian uint64s) |

**Why:** The caller is required to provide uniformly random input (or pre-hash). Given that guarantee, the first 16 bytes provide sufficient entropy for both routing and slot computation. This eliminates a full-key hash in the build/query hot path.

---

## 7. Implementation Notes

### 7.0. Implementation Roadmap

For implementers, this is the recommended order for building a StreamHash library:

1. **Mental model:** Read §2 (Architecture) and §4 (Contract)
2. **Query path first:** Implement §4.4 (RAM Index), §5.5 (Elias-Fano decoding), §5.8 (Query)
3. **Single-threaded build:** Implement §7.1.1 (Sorted Mode), §4.4 (Bijection solving), §5.3 (Mix function)
4. **Seed encoding:** Add §5.6 (Golomb-Rice encoding/decoding)
5. **Split buckets:** Add deterministic splitting — buckets with ≥8 keys use splitPoint = size/2
6. **Unsorted input:** Add §7.1.2 (Unsorted Input Mode with temp file routing)
7. **Parallelism:** Add multi-threaded builds with worker pools and ordered block delivery

Starting with the query path lets you verify correctness incrementally: build a small index, then test queries before adding complexity.

### 7.1. Construction

#### 8.1.1. Sorted Mode (Default)

Keys arrive in non-decreasing prefix order. The framework routes each key to its block, accumulates keys for the current block, and when the block boundary changes, invokes the algorithm's solver, writes the metadata, and resets for the next block. This enables true streaming construction with ~1 MB RAM (single-threaded, Bijection).

#### 8.1.2. Unsorted Mode

For unsorted input, the framework uses a **two-pass construction** process:

**Pass 1: Routing to temp file**
- Each key is routed to its block using `blockIdx = fastRange32(prefix, numBlocks)`
- Keys are written to block-specific regions in a memory-mapped temp file
- The temp file contains `numBlocks` contiguous regions, each sized for `regionCapacity` entries

**Temp file layout:**
```
[Region 0: regionCapacity entries]
[Region 1: regionCapacity entries]
...
[Region numBlocks-1: regionCapacity entries]
```

**Entry format:**
```
[keyLen: uint16_le][key: keyLen bytes][payload: PayloadSize bytes]
```

The `uint16_le` key-length field limits keys to ≤ 65,535 bytes (see §1.5). In practice, keys are typically 16–64 bytes (hash outputs); this limit exists only as a safety check.

**Per-block write cursors:**
- An array of `numBlocks` counters tracks the write position within each block's region
- This small RAM cost (e.g., ~126 KB for 31.6M blocks at 1T keys) enables concurrent writes without coordination

**Pass 2: Read-back and solving**
- Blocks are read back sequentially in order (block 0, block 1, ...)
- For each block, the framework reads entries from `[region_start, region_start + cursor_value)` where cursor_value is the final write position for that block
- The solver receives all keys for the block and produces metadata
- This ensures the normative requirement: the solver receives all keys for block N before block N+1

**Overflow handling:**
- If a block's region fills up (cursor reaches `regionCapacity`), the builder returns `ErrRegionOverflow`
- The 7σ Poisson margin (see below) makes this extremely unlikely (~10⁻¹² per block)

**Temp file sizing:** Uses a dynamic 7σ Poisson margin instead of a fixed percentage:
```
avgKeysPerBlock = N / numBlocks
σ = sqrt(avgKeysPerBlock)
regionCapacity = ceil(avgKeysPerBlock × (1 + 7 / sqrt(avgKeysPerBlock)))
regionSize = regionCapacity × entrySize
tempFileSize = numBlocks × regionSize
```

The 7σ margin provides ~10⁻¹² overflow probability per block (Gaussian tail Q(7) ≈ 1.28×10⁻¹²). The temp file is deleted after construction.

#### 8.1.3. Parallel Mode

Both sorted and unsorted modes support parallel block building with configurable worker counts. Workers solve blocks independently; a coordinator thread sequences output in block order. See §2.8 for the architectural rationale. See §1.6 for performance measurements.

#### 8.1.4. Thread Safety

**Queries:** Thread-safe and lock-free. The RAM index is immutable after load, and block reads are independent.

**Initialization requirement:** The RAM index must be fully loaded and visible to all threads before concurrent queries begin.

### 7.2. Operational Concerns

#### 8.2.1. Key Length and Collision Analysis

Keys must be at least 16 bytes. StreamHash uses the first 16 bytes as k0 and k1.

**Build failure modes:**

1. **ErrDuplicateKey**: Two keys share identical k0 AND k1.
   For uniformly random 128-bit keys, probability is negligible (~10⁻²¹ at 1B keys).
   Pre-hash non-uniform input with xxHash3-128.

2. **ErrIndistinguishableHashes** (PTRHash only): Different keys compute same slot for ALL 256 pilots.
   Addressed by block sizing and pilot independence (see §6.11, §6.12).
   At 1T keys: ~1 in 7,200 builds fails.

**Build failure frequency by scale:**

| Dataset Size | Approximate Failure Rate |
|---|---|
| 10M keys (~317 blocks) | ~1 in 7×10⁸ builds (effectively never) |
| 1B keys (~31,600 blocks) | ~1 in 7×10⁶ builds (rare) |
| 100B keys (~3.16M blocks) | ~1 in 72,000 builds |
| 1T keys (~31.6M blocks) | ~1 in 7,200 builds |

**Retry guidance:** When a build fails with `ErrIndistinguishableHashes`, retry with a different `globalSeed`. The new seed changes all pilot hashes (§6.3), producing independent slot assignments. Each retry has the same independent failure probability, so a handful of retries is sufficient. For datasets above ~100B keys, callers should wrap the build in a retry loop. The framework cannot retry internally because it does not own the key source — the caller controls the data pipeline and must re-feed keys with a new seed. Bijection does not have this failure mode — its brute-force seed search over an unbounded seed range always succeeds (in practice, seeds are found within the Golomb-Rice encodable range or stored in the fallback list).

| Keys | P(128-bit collision) |
|------|---------------------|
| 1 billion | ~1.5×10⁻²¹ (≈0) |
| 100 billion | ~1.5×10⁻¹⁷ (≈0) |

For structured (non-random) input, keys **must** be pre-hashed with a 128-bit hash (see Appendix A). See §2.1 for the definitions of k0, k1, and prefix.

#### 8.2.2. Payload Modes

| PayloadSize | FingerprintSize | Mode | Query Returns |
|-------------|-----------------|------|---------------|
| 0 | 0 | Pure MPHF | Rank (no verification) |
| 0 | >0 | MPHF + verification | Rank or ErrNotFound |
| >0 | 0 | Payload only | Payload (no verification) |
| >0 | >0 | Payload + verification | Payload or ErrNotFound |

#### 8.2.3. Dataset Size Limits

StreamHash is optimized for large datasets (N > 100,000 keys). Block counts depend on the algorithm — Bijection uses smaller blocks (~3,072 keys) than PTRHash (~31,600 keys), so Bijection produces more blocks for the same N. For smaller datasets, framework fixed costs (header, footer, RAM index) become significant relative to useful MPHF data:

| N | Bijection Blocks | PTRHash Blocks | Recommendation |
|---|-----------------|----------------|----------------|
| < 1,000 | 2 | 2 | Use simpler data structure |
| 1,000 - 10,000 | 2-4 | 2 | High relative overhead |
| 10,000 - 100,000 | 4-33 | 2-4 | Reasonable for Bijection |
| > 100,000 | 33+ | 4+ | Optimal use case |

**Maximum dataset size:** ~2^40 keys due to 40-bit RAM index fields. At this scale, the RAM index is ~3.6 GB (Bijection, ~358M blocks) or ~348 MB (PTRHash, ~34.8M blocks).

#### 8.2.4. RAM Index Safety

**Bounds checking:** Before reading block metadata, implementations SHOULD verify that `MetadataOffset` points within the metadata region:

```
metadataRegionEnd = metadataRegionOffset + metadataRegionSize
metadataStart = metadataRegionOffset + entry.MetadataOffset
if metadataStart >= metadataRegionEnd:
    return ErrCorruptedIndex
```

**Fail-fast validation:** Implementations SHOULD verify a checksum over the header + RAM index at load time:

```
headerAndIndexBytes = file[0 : 64 + variableSections + 10 × (NumBlocks + 1)]
if xxHash64(headerAndIndexBytes) != expectedChecksum:
    return ErrCorruptHeader
```

The expected checksum can be stored in `UserMetadata` or a separate manifest file.

**Large file support:** The 40-bit MetadataOffset field supports metadata regions up to ~1.1 TB.

#### 8.2.5. Error Handling

Error names below are from the reference implementation; other implementations may use different naming.

**Construction Errors:**

| Error | Condition | Resolution |
|-------|-----------|------------|
| ErrEmptyIndex | N = 0 | Reject |
| ErrKeyTooShort | Key < 16 bytes | Reject |
| ErrDuplicateKey | Duplicate keys detected (identical k0 and k1) | Reject |
| ErrPayloadTooLarge | PayloadSize > 8 | Reject |
| ErrFingerprintTooLarge | FingerprintSize > 4 | Reject |
| ErrKeyCountMismatch | Declared totalKeys ≠ actual | Reject |
| ErrKeyTooLong | Key exceeds 65535 bytes | Reject |
| ErrTooManyKeys | Key count exceeds 2^40 | Reject |
| ErrUnsortedInput | Input keys not sorted (sorted mode) | Reject |
| ErrPayloadOverflow | Payload value exceeds PayloadSize capacity | Reject |
| ErrRegionOverflow | Block metadata/payload region capacity exceeded | Reject |
| ErrSplitBucketSeedSearchFailed | Bijection split-bucket seed search exhausted | Retry with different globalSeed |
| ErrIndistinguishableHashes | Indistinguishable hash pairs in a bucket | Retry with different globalSeed |

**Query Errors:**

| Error | Condition | Resolution |
|-------|-----------|------------|
| ErrInvalidMagic | Magic ≠ 0x53544D48 | Reject file |
| ErrInvalidVersion | Version ≠ 0x0001 | Reject file |
| ErrChecksumFailed | Footer hash mismatch | Reject file |
| ErrFingerprintMismatch | Fingerprint doesn't match (non-member query) | Return "not found" |
| ErrCorruptedIndex | Invalid metadata or out-of-bounds | Reject |
| ErrTruncatedFile | File shorter than expected | Reject |

#### 8.2.6. Security Considerations

StreamHash assumes uniformly random input. Non-random input causes failure modes:

**Non-uniform key distribution:**
- Keys clustering in blocks cause temp file overflow (unsorted mode)
- The 7σ Poisson margin (§7.1.2) is calibrated for uniform random keys

**Correlated keys (e.g., sequential IDs):**
- Pre-hashing with xxHash3-128 is REQUIRED for non-random keys

**Adversarial key selection:**
- An attacker knowing globalSeed could craft keys mapping to same block
- Mitigation: Use random globalSeed unknown to untrusted input sources

**Fingerprint bypassing:**
- Non-member keys pass fingerprint check with probability 2^(-8×fpSize)
- This is the expected false positive rate, not a vulnerability

---

## Appendices

### A. Pre-hash Transformation

For non-random input, keys MUST be hashed before indexing. Callers are responsible for applying the same transformation during both construction and queries.

**Recommended Algorithm: xxHash3-128**

```
function PreHash(input):
    return xxHash3_128(input)    // 16 bytes (128 bits), uniformly random
```

The output is a 128-bit value stored as 16 bytes in little-endian order (low 64 bits first, then high 64 bits).

**Why 128 bits?** Due to the birthday paradox, 64-bit hashes have significant collision probability at scale:

| Keys | 64-bit collision probability | 128-bit collision probability |
|------|------------------------------|-------------------------------|
| 100 million | ~0.03% | ≈0 |
| 1 billion | ~2.7% | ≈0 |
| 4 billion | ~35% | ≈0 |

Collisions cause build failure (duplicate key error), not silent corruption.

### B. Load Factor Selection

**Bijection:** The default lambda = 3.0 with hierarchical bucket splitting (threshold 8) balances build speed and storage:

| lambda | Bits/Key | Fallback Rate | Split Rate | Notes |
|--------|----------|---------------|------------|-------|
| 2.0 | ~2.8     | <0.01%        | ~1%        | Conservative |
| 3.0 | ~2.45    | ~0.01%        | ~1.2%      | **Recommended** |
| 3.6 | ~2.40    | ~0.02%        | ~3%        | Higher split rate |
| 5.0 | ~2.30    | ~0.3%         | ~7%        | Aggressive |

**PTRHash:** lambda = 3.16 with alpha = 0.99 provides ~1% slot slack. The remap table handles overflow slots efficiently.

### C. Storage Breakdown

**Bijection MPHF Mode (PayloadSize = 0, bucketsPerBlock = 1024, lambda = 3.0):**

| Component | Contribution |
|-----------|--------------|
| Elias-Fano | ~1.17 bits/key |
| Seed stream (direct solve, sizes 2-7) | ~1.05 bits/key |
| Seed stream (split buckets, sizes 8+) | ~0.12 bits/key |
| Checkpoints (28 bytes/block) | ~0.07 bits/key |
| Fallback list | ~0.01 bits/key |
| RAM index (10 bytes/block) | ~0.03 bits/key |
| Header + Footer | negligible |
| **Total** | **~2.45 bits/key** |

**PTRHash MPHF Mode (PayloadSize = 0, bucketsPerBlock = 10,000, lambda = 3.16, alpha = 0.99):**

| Component | Contribution |
|-----------|--------------|
| Pilots (1 byte × 10,000 per block) | ~2.53 bits/key |
| Remap table (~1% overflow) | ~0.16 bits/key |
| RAM index (10 bytes/block) | ~0.003 bits/key |
| Header + Footer | negligible |
| **Total** | **~2.70 bits/key** |

**Supported Entry Sizes:**

| Entry Size | Configuration | Total bits/key |
|------------|---------------|----------------|
| 0 (MPHF) | PayloadSize=0, FingerprintSize=0 | ~2.45 (Bij) / ~2.70 (PTR) |
| 5 bytes | PayloadSize=4, FingerprintSize=1 | ~42.5 |
| 8 bytes | PayloadSize=4, FingerprintSize=4 | ~66.5 |
| 12 bytes | PayloadSize=8, FingerprintSize=4 | ~98.5 |

Non-MPHF rows use Bijection overhead (~2.45 bits/key). PTRHash adds ~0.25 bits/key more.

**General Formula:**
```
Total bits/key = routing_overhead + 8 × (PayloadSize + FingerprintSize)
```

### D. Glossary

| Term | Definition |
|------|------------|
| MPHF | Minimal Perfect Hash Function — bijection from N keys to [0, N) |
| Bijection | One-to-one mapping with no collisions |
| prefix | First 8 bytes of a key, interpreted as **big-endian** uint64, used for block routing (see §2.1) |
| k0 | First 8 bytes of a key, interpreted as **little-endian** uint64, used for algorithm operations (see §2.1) |
| k1 | Bytes 8-15 of a key, interpreted as **little-endian** uint64, used for algorithm operations (see §2.1) |
| Block | A self-contained group of keys on disk; exactly one block's metadata is read per MPHF query |
| Bucket | A partitioning mechanism that divides keys into small groups (averaging ~3 keys) for independent solving. Buckets make MPHF construction tractable by breaking an infeasible problem into many trivial subproblems. |
| Slot | Position within a block (0 to keysInBlock-1); output of the algorithm for a key |
| Bucket Seed / Pilot | A small integer value assigned to each bucket that determines where its keys map to in the output slots. The algorithm searches through possible pilot values until it finds one that steers all keys in the bucket to collision-free positions. |
| Global Seed | 64-bit header value that randomizes slot assignment across the entire index |
| Rank | Final MPHF output (0 to N-1); computed as keysBefore + localSlot |
| Cumulative Count | Running total of keys; cumulative[i] = number of keys in buckets 0 through i |
| Elias-Fano | Succinct encoding for monotonically increasing integer sequences |
| Golomb-Rice | Variable-length encoding optimal for geometrically distributed values |
| Sentinel | Special value indicating "look in fallback list" |
| Fallback List | Explicit storage for seeds that exceed normal encoding range |
| Hierarchical Splitting | Buckets with ≥8 keys split into two sub-buckets using hash-based classification |
| Deterministic Splitting | splitPoint = bucketSize / 2; sub-bucket sizes are exactly splitPoint and bucketSize - splitPoint |
| Payload | Fixed-size data (1-8 bytes) associated with a key |
| Fingerprint | Bytes stored per entry to detect non-member queries (1-4 bytes) |
| UserMetadata | Variable-length application-defined data stored after header |
| AlgoConfig | Variable-length algorithm-specific configuration stored after UserMetadata |
| fastRange32 | Block/bucket routing: `bucket = hi64(hash × n)`. Monotonic for sorted prefixes. See §2.4. |
| lambda | Target average keys per bucket (3.0 for Bijection, 3.16 for PTRHash) |
| alpha | PTRHash slot overflow factor (0.99); numSlots = ceil(N/alpha) |
| Effectively independent pilots | Measure of pilot value independence in PTRHash; the number of pilots (out of 256) that produce truly independent hash patterns. Higher values reduce build failure probability. See §6.11. |
| CubicEps | Skewed bucket distribution: x²(1+x)/2 × 255/256 + x/256 |
| Checkpoints | 28-byte block metadata enabling O(128) decode instead of O(1024) |
| Separated Layout | File layout where payloads are in a contiguous region separate from block metadata |
| Streaming Construction | Building the index with bounded RAM regardless of dataset size |
| Remap Table | PTRHash overflow table mapping slots ≥ numKeys to holes in [0, numKeys) |
| wymix | WyHash v4 mixing primitive: 128-bit multiply with XOR fold (hi ^ lo). See §6.3. |
| SplitMix64 | PRNG finalizer (Vigna, 2015) providing strong avalanche properties. See §6.3. |
| Cuckoo Hashing | Hash table technique where items can be displaced to alternative positions to resolve collisions |

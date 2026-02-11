# StreamHash

A Go library for building and querying **Minimal Perfect Hash Function (MPHF)** indexes over billions of keys, using bounded RAM and streaming construction.

An MPHF maps N keys to N consecutive integers [0, N) with no collisions. This enables compact, read-only lookup tables where every key has a unique position — O(1) lookups without storing the keys themselves.

## Features

- **Streaming construction** — build indexes over 1B+ keys with ~1–75 MB of RAM, regardless of dataset size
- **Sorted and unsorted input** — pre-sorted keys build with no temp disk; unsorted keys use a single temp file
- **Parallel builds** — block-independent construction scales near-linearly with worker count
- **Two algorithms** — Bijection (compact, low RAM) and PTRHash (fast queries)
- **Optional payloads** — store 1–8 byte fixed-size values alongside keys
- **Optional fingerprints** — detect non-member keys with configurable false-positive rates
- **Single-file output** — one mmap'd file for queries, no external dependencies

## Performance

Reference measurements on Apple M1 Max, 100M keys, MPHF mode, pre-sorted input:

| Metric | Bijection | PTRHash |
|---|---|---|
| Index size (MPHF) | ~2.46 bits/key | ~2.70 bits/key |
| Query latency (CPU) | ~1.0 µs | ~0.05 µs |
| Build throughput (1 worker) | ~16 M keys/sec | ~16 M keys/sec |
| Build throughput (4 workers) | ~65 M keys/sec | ~61 M keys/sec |
| Peak heap RAM (1 worker) | ~1 MB | ~9 MB |
| Peak heap RAM (4 workers) | ~9 MB | ~74 MB |

Unsorted input builds are ~20-25% slower (single-threaded) due to temp file I/O:

| Metric | Bijection | PTRHash |
|---|---|---|
| Build throughput (1 worker, unsorted) | ~12 M keys/sec | ~13 M keys/sec |

Query latency is the same regardless of input mode. Query latency excludes disk I/O. Each query requires one metadata read (~100 µs on NVMe); payload mode adds a second read.

## Installation

```
go get github.com/tamirms/streamhash
```

Requires Go 1.25+.

## Usage

### Building an index (sorted input)

Keys must be at least 16 bytes and uniformly distributed. Use `PreHash` for non-uniform keys (strings, integers, etc.).

```go
builder, err := streamhash.NewBuilder(ctx, "index.idx", totalKeys,
    streamhash.WithPayload(4),
    streamhash.WithWorkers(4),
)
if err != nil {
    log.Fatal(err)
}
defer builder.Close()

for _, key := range sortedKeys {
    if err := builder.AddKey(key, payload); err != nil {
        log.Fatal(err)
    }
}
if err := builder.Finish(); err != nil {
    log.Fatal(err)
}
```

### Building an index (unsorted input)

```go
builder, err := streamhash.NewBuilder(ctx, "index.idx", totalKeys,
    streamhash.WithUnsortedInput(),
    streamhash.WithWorkers(4),
)
```

### Querying

```go
idx, err := streamhash.Open("index.idx")
if err != nil {
    log.Fatal(err)
}
defer idx.Close()

// MPHF mode: get the rank (0-based index) for a key
rank, err := idx.Query(key)

// Payload mode: get the stored payload for a key
payload, err := idx.QueryPayload(key)
```

### Pre-hashing non-uniform keys

If your keys are not already uniformly random (e.g., strings, sequential integers, UUIDs), pre-hash them before building and querying:

```go
// Pre-hash and sort
hashedKeys := make([][]byte, len(keys))
for i, key := range keys {
    hashedKeys[i] = streamhash.PreHash(key)
}
sort.Slice(hashedKeys, func(i, j int) bool {
    return bytes.Compare(hashedKeys[i], hashedKeys[j]) < 0
})

// Build
builder, err := streamhash.NewBuilder(ctx, "index.idx", uint64(len(hashedKeys)))
if err != nil {
    log.Fatal(err)
}
defer builder.Close()
for _, hk := range hashedKeys {
    if err := builder.AddKey(hk, 0); err != nil {
        log.Fatal(err)
    }
}
if err := builder.Finish(); err != nil {
    log.Fatal(err)
}

// Query: pre-hash the lookup key
rank, err := idx.Query(streamhash.PreHash(originalKey))
```

## Build Options

| Option | Description | Default |
|---|---|---|
| `WithWorkers(n)` | Parallel build workers | 1 |
| `WithPayload(size)` | Payload size in bytes (0–8) | 0 (MPHF only) |
| `WithFingerprint(size)` | Fingerprint size in bytes (0–4) | 0 (disabled) |
| `WithAlgorithm(algo)` | `AlgoBijection` or `AlgoPTRHash` | `AlgoBijection` |
| `WithUnsortedInput()` | Accept keys in any order | sorted required |
| `WithTempDir(dir)` | Temp file location for unsorted builds | system default |
| `WithGlobalSeed(seed)` | Hash seed (change on build failure) | fixed default |
| `WithUserMetadata(data)` | Arbitrary metadata stored in the file | none |

## Choosing an Algorithm

**Bijection** (default) — best for most use cases. Smallest indexes (~2.46 bits/key) and lowest RAM usage. Query decoding is O(128) via checkpoint-based Elias-Fano/Golomb-Rice decoding.

**PTRHash** — best when query speed is critical. O(1) queries via direct pilot byte lookup (~0.05 µs vs ~1.0 µs), at the cost of slightly larger indexes (~2.70 bits/key) and more RAM during construction.

## Design

StreamHash partitions keys into fixed-size blocks by prefix, routes each key to its block, and delegates MPHF construction to a pluggable algorithm. This block-partitioning architecture enables:

- **Bounded RAM**: each block is solved independently, using only O(block) memory
- **Parallelism**: workers solve blocks concurrently while a coordinator sequences output
- **Locality**: all metadata for a block is contiguous on disk — one read per query

The framework handles key routing, file layout, parallel coordination, and payload/fingerprint storage. Algorithms only need to implement a build-time solver and a query-time decoder.

See [streamhash-spec.md](streamhash-spec.md) for the full technical specification.

## Benchmarking

```bash
# MPHF-only mode (core hash function performance)
go run ./cmd/bench -keys 10000000 -payload 0 -fp 0 -algo bijection

# With payload and fingerprint
go run ./cmd/bench -keys 10000000 -payload 4 -fp 1 -algo ptrhash -workers 4
```

## License

TODO

// Package streamhash implements a Minimal Perfect Hash Function (MPHF) library
// with streaming build support and bounded RAM usage.
//
// StreamHash is designed for building large-scale indexes (1B+ keys) efficiently.
// See the README for performance characteristics.
//
// # Basic Usage
//
// Building an index:
//
//	builder, err := streamhash.NewBuilder(ctx, "index.idx", totalKeys)
//	if err != nil {
//	    log.Fatal(err)
//	}
//	for key, payload := range sortedKeys {
//	    if err := builder.AddKey(key, payload); err != nil {
//	        log.Fatal(err)
//	    }
//	}
//	if err := builder.Finish(); err != nil {
//	    log.Fatal(err)
//	}
//
// Querying an index:
//
//	idx, err := streamhash.Open("index.idx")
//	if err != nil {
//	    log.Fatal(err)
//	}
//	defer idx.Close()
//
//	rank, err := idx.Query(streamhash.PreHash([]byte("mykey")))
//	if err != nil {
//	    log.Fatal(err)
//	}
//	fmt.Printf("Key rank: %d\n", rank)
//
// # Package Structure
//
// The implementation is organized as follows:
//
//   - Public API: builder.go (NewBuilder, AddKey, Finish), index.go (Open, Query)
//   - Configuration: builder_options.go (BuildOption, With* functions)
//   - Serialization: header.go (header, footer, ramIndexEntry), index_writer.go
//   - Key routing: key.go (prefix extraction, fastRange), prehash.go (PreHash)
//   - Algorithm dispatch: algorithm.go (blockBuilder/blockDecoder interfaces, factory functions)
//   - Block algorithms: internal/bijection/ (EF/GR), internal/ptrhash/ (Cuckoo)
//   - Platform: fallocate_*.go, prefault_*.go (OS-specific optimizations)
package streamhash

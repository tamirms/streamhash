package main

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
)

// --- File reader ---

type fileReader struct {
	f      *os.File
	buf    []byte
	cursor int
	valid  int
}

func newFileReader(path string, bufsize int) (*fileReader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	if _, err := f.Seek(8, io.SeekStart); err != nil {
		f.Close()
		return nil, err
	}
	bufsize = (bufsize / benchEntrySize) * benchEntrySize
	if bufsize < benchEntrySize {
		f.Close()
		return nil, fmt.Errorf("bufsize too small (min %d bytes)", benchEntrySize)
	}
	r := &fileReader{f: f, buf: make([]byte, bufsize)}
	n, _ := io.ReadFull(f, r.buf)
	r.valid = n
	return r, nil
}

func (r *fileReader) advance() bool {
	r.cursor += benchEntrySize
	if r.cursor+benchEntrySize <= r.valid {
		return true
	}
	n, _ := io.ReadFull(r.f, r.buf)
	r.cursor = 0
	r.valid = n
	return n >= benchEntrySize
}

func (r *fileReader) prepareFirst() bool { return r.cursor+benchEntrySize <= r.valid }
func (r *fileReader) entry() []byte      { return r.buf[r.cursor : r.cursor+benchEntrySize] }
func (r *fileReader) prefix() uint64     { return binary.BigEndian.Uint64(r.buf[r.cursor:]) }
func (r *fileReader) close()             { r.f.Close() }

// --- Merge primitives ---

type mergeEntry struct {
	prefix uint64
	idx    int
}

const mergeBatchSize = 4096

type mergeBatch struct {
	data  [mergeBatchSize * benchEntrySize]byte
	count int
}

type streamReader struct {
	ch     <-chan *mergeBatch
	pool   chan *mergeBatch
	batch  *mergeBatch
	cursor int
	prefix uint64
	done   bool
}

func (s *streamReader) advance() bool {
	s.cursor++
	if s.cursor < s.batch.count {
		off := s.cursor * benchEntrySize
		s.prefix = binary.BigEndian.Uint64(s.batch.data[off:])
		return true
	}
	s.pool <- s.batch
	b, ok := <-s.ch
	if !ok {
		s.done = true
		return false
	}
	s.batch = b
	s.cursor = 0
	s.prefix = binary.BigEndian.Uint64(b.data[0:])
	return true
}

func (s *streamReader) entry() []byte {
	off := s.cursor * benchEntrySize
	return s.batch.data[off : off+benchEntrySize]
}

func mergeStream(files []string, bufsize int, out chan<- *mergeBatch, pool chan *mergeBatch) {
	defer close(out)

	readers := make([]*fileReader, len(files))
	defer func() {
		for _, r := range readers {
			if r != nil {
				r.close()
			}
		}
	}()
	for i, path := range files {
		r, err := newFileReader(path, bufsize)
		if err != nil {
			fmt.Fprintf(os.Stderr, "mergeStream: open %s: %v\n", path, err)
			return
		}
		readers[i] = r
	}

	heap := make([]mergeEntry, 0, len(files))
	for i, r := range readers {
		if !r.prepareFirst() {
			continue
		}
		heap = append(heap, mergeEntry{prefix: r.prefix(), idx: i})
	}
	heapSize := len(heap)
	for i := heapSize/2 - 1; i >= 0; i-- {
		siftDown(heap, i, heapSize)
	}

	batch := <-pool
	pos := 0

	for heapSize > 0 {
		ri := heap[0].idx
		r := readers[ri]
		off := pos * benchEntrySize
		copy(batch.data[off:off+benchEntrySize], r.entry())
		pos++

		if pos == mergeBatchSize {
			batch.count = pos
			out <- batch
			batch = <-pool
			pos = 0
		}

		if r.advance() {
			heap[0].prefix = r.prefix()
			siftDown(heap, 0, heapSize)
		} else {
			heapSize--
			if heapSize > 0 {
				heap[0] = heap[heapSize]
				siftDown(heap, 0, heapSize)
			}
		}
	}

	if pos > 0 {
		batch.count = pos
		out <- batch
	}
}

func finalMerge(streams []*streamReader, out chan<- *mergeBatch, pool chan *mergeBatch) {
	defer close(out)

	G := len(streams)
	heap := make([]mergeEntry, 0, G)
	for i, s := range streams {
		if !s.done {
			heap = append(heap, mergeEntry{prefix: s.prefix, idx: i})
		}
	}
	heapSize := len(heap)
	for i := heapSize/2 - 1; i >= 0; i-- {
		siftDown(heap, i, heapSize)
	}

	batch := <-pool
	pos := 0

	for heapSize > 0 {
		si := heap[0].idx
		s := streams[si]
		off := pos * benchEntrySize
		copy(batch.data[off:off+benchEntrySize], s.entry())
		pos++

		if pos == mergeBatchSize {
			batch.count = pos
			out <- batch
			batch = <-pool
			pos = 0
		}

		if s.advance() {
			heap[0].prefix = s.prefix
			siftDown(heap, 0, heapSize)
		} else {
			heapSize--
			if heapSize > 0 {
				heap[0] = heap[heapSize]
				siftDown(heap, 0, heapSize)
			}
		}
	}

	if pos > 0 {
		batch.count = pos
		out <- batch
	}
}

func siftDown(h []mergeEntry, i, n int) {
	for {
		left := 2*i + 1
		if left >= n {
			break
		}
		j := left
		if right := left + 1; right < n && h[right].prefix < h[j].prefix {
			j = right
		}
		if h[i].prefix <= h[j].prefix {
			break
		}
		h[i], h[j] = h[j], h[i]
		i = j
	}
}

// --- Merge tree helpers ---

func launchMergeStream(files []string, bufsize int) *streamReader {
	ch := make(chan *mergeBatch, 2)
	pool := make(chan *mergeBatch, 3)
	for range 3 {
		pool <- &mergeBatch{}
	}
	go mergeStream(files, bufsize, ch, pool)

	b, ok := <-ch
	if !ok {
		return &streamReader{done: true}
	}
	return &streamReader{
		ch:     ch,
		pool:   pool,
		batch:  b,
		prefix: binary.BigEndian.Uint64(b.data[0:]),
	}
}

func launchFinalMerge(streams []*streamReader) *streamReader {
	ch := make(chan *mergeBatch, 2)
	pool := make(chan *mergeBatch, 3)
	for range 3 {
		pool <- &mergeBatch{}
	}
	go finalMerge(streams, ch, pool)

	b, ok := <-ch
	if !ok {
		return &streamReader{done: true}
	}
	return &streamReader{
		ch:     ch,
		pool:   pool,
		batch:  b,
		prefix: binary.BigEndian.Uint64(b.data[0:]),
	}
}

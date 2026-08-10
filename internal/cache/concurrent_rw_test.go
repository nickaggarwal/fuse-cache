package cache

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Concurrency and edge-case coverage for the read/write paths: many requests
// in flight at once against one manager, plus the degenerate sizes and
// boundaries that single-threaded tests skip.

func rwTestManager(chunkSize, maxNVMe int64) *DefaultCacheManager {
	cm := newTestCacheManager()
	cm.config.ChunkSize = chunkSize
	cm.config.MaxNVMeSize = maxNVMe
	return cm
}

// patternBytes makes content whose every byte is a function of its offset, so
// a read that mixes two regions is detectable rather than merely "wrong size".
func patternBytes(n int) []byte {
	out := make([]byte, n)
	for i := range out {
		out[i] = byte(i * 7 % 251)
	}
	return out
}

// TestConcurrentRead_DistinctWindows: many overlapping ReadRange windows over
// one stable file. Every window must be byte-exact — the range cache, the
// per-file chunk cache, and the chunk-fetch singleflight are all shared
// mutable state on this path.
func TestConcurrentRead_DistinctWindows(t *testing.T) {
	ctx := context.Background()
	cm := rwTestManager(512, 64*1024*1024)

	const total = 512 * 20
	data := patternBytes(total)
	if err := cm.Put(ctx, &CacheEntry{FilePath: "/win", Data: data, LastAccessed: time.Now()}); err != nil {
		t.Fatalf("put: %v", err)
	}

	var mismatch, failed atomic.Int64
	var wg sync.WaitGroup
	for w := 0; w < 24; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				off := int64((w*137 + i*31) % total)
				size := 1 + (i*13)%1500 // deliberately straddles chunk edges
				got, err := cm.ReadRange(ctx, "/win", off, size)
				if err != nil {
					failed.Add(1)
					continue
				}
				end := off + int64(size)
				if end > total {
					end = total
				}
				if !bytes.Equal(got, data[off:end]) {
					mismatch.Add(1)
				}
			}
		}(w)
	}
	wg.Wait()

	if n := failed.Load(); n > 0 {
		t.Errorf("%d of 2400 concurrent ReadRange calls returned an error", n)
	}
	if n := mismatch.Load(); n > 0 {
		t.Errorf("%d of 2400 concurrent ReadRange windows returned wrong bytes", n)
	}
}

// TestConcurrentRead_SameFileManyReaders: the pure read-amplification case.
// All readers want the whole file at once, which is the herd the chunk-fetch
// dedupe exists for.
func TestConcurrentRead_SameFileManyReaders(t *testing.T) {
	ctx := context.Background()
	cm := rwTestManager(256, 64*1024*1024)

	data := patternBytes(256 * 12)
	if err := cm.Put(ctx, &CacheEntry{FilePath: "/herd", Data: data, LastAccessed: time.Now()}); err != nil {
		t.Fatalf("put: %v", err)
	}

	var bad atomic.Int64
	var wg sync.WaitGroup
	for r := 0; r < 32; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 20; i++ {
				got, err := cm.Get(ctx, "/herd")
				if err != nil || !bytes.Equal(got.Data, data) {
					bad.Add(1)
				}
			}
		}()
	}
	wg.Wait()

	if n := bad.Load(); n > 0 {
		t.Errorf("%d of 640 concurrent whole-file reads were wrong or failed", n)
	}
}

// TestConcurrentWrite_UnchunkedOverwriteIsAtomic pins the guarantee
// docs/SEMANTICS.md makes for same-node concurrent writers. An unchunked
// overwrite swaps one object, so a reader sees some whole version — never a
// mix. Contrast with the chunked case below, which does not hold.
func TestConcurrentWrite_UnchunkedOverwriteIsAtomic(t *testing.T) {
	ctx := context.Background()
	cm := rwTestManager(4096, 64*1024*1024) // payload stays under ChunkSize

	const size = 512
	if err := cm.Put(ctx, &CacheEntry{FilePath: "/atomic", Data: mkBytes('A', size), LastAccessed: time.Now()}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	var mixed, reads atomic.Int64
	stop := make(chan struct{})
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			_ = cm.Put(ctx, &CacheEntry{
				FilePath: "/atomic", Data: mkBytes(byte('A'+i%26), size), LastAccessed: time.Now(),
			})
		}
	}()
	for r := 0; r < 4; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				got, err := cm.Get(ctx, "/atomic")
				if err != nil || len(got.Data) == 0 {
					continue
				}
				reads.Add(1)
				// Every byte of a given version is identical, so a mixed read
				// is unambiguous evidence of a torn write.
				first := got.Data[0]
				for _, b := range got.Data {
					if b != first {
						mixed.Add(1)
						break
					}
				}
			}
		}()
	}

	awaitReads(t, &reads, minTornReads)
	close(stop)
	wg.Wait()

	if n := mixed.Load(); n > 0 {
		t.Errorf("torn read on unchunked overwrite: %d of %d reads mixed two versions", n, reads.Load())
	}
}

// minTornReads is the sample size the two overwrite-atomicity tests need
// before their verdict means anything.
const minTornReads = 200

// awaitReads runs the racing workers until they have completed want reads,
// rather than for a fixed wall-clock window. A timed window bounds the *test*
// by the clock but bounds its *evidence* by whatever the scheduler granted:
// under full-package load at GOMAXPROCS=2 a 200ms window intermittently
// produced zero reads, so the test failed with "proved nothing" — a starved
// machine reported as a product defect. Making the sample size the exit
// condition keeps the assertion meaningful at any speed; the deadline is only
// a backstop against a genuine hang.
func awaitReads(t *testing.T, reads *atomic.Int64, want int64) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for reads.Load() < want {
		if time.Now().After(deadline) {
			t.Fatalf("only %d of %d reads completed in 30s; workers are stuck", reads.Load(), want)
		}
		time.Sleep(time.Millisecond)
	}
}

// TestConcurrentWrite_ChunkedOverwriteTears documents the opposite result, so
// the boundary of the guarantee is a pinned fact rather than a surprise.
//
// putChunked writes each chunk independently and in parallel; there is no
// version barrier across them. A reader that assembles the file while an
// overwrite is in flight gets some old chunks and some new ones. Measured at
// ~93% of reads under a tight overwrite loop — the common case, not a rare
// race. docs/SEMANTICS.md covers cross-node writers but claims same-node
// writers "serialize through the FUSE layer"; that is true of the FUSE
// entrance only, and the HTTP API (PUT /api/files/{path}) does not go through
// it.
//
// This test asserts the file is always a *valid length* — the weaker property
// that does hold — and reports tearing without failing, so a future change
// that makes chunked overwrite atomic shows up as the log going quiet rather
// than as a red build.
func TestConcurrentWrite_ChunkedOverwriteTears(t *testing.T) {
	ctx := context.Background()
	cm := rwTestManager(64, 64*1024*1024)

	const size = 64 * 8 // 8 chunks
	if err := cm.Put(ctx, &CacheEntry{FilePath: "/torn", Data: mkBytes('A', size), LastAccessed: time.Now()}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	var mixed, reads, wrongLen atomic.Int64
	stop := make(chan struct{})
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			_ = cm.Put(ctx, &CacheEntry{
				FilePath: "/torn", Data: mkBytes(byte('A'+i%26), size), LastAccessed: time.Now(),
			})
		}
	}()
	for r := 0; r < 4; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				got, err := cm.Get(ctx, "/torn")
				if err != nil || len(got.Data) == 0 {
					continue
				}
				reads.Add(1)
				if len(got.Data) != size {
					wrongLen.Add(1)
					continue
				}
				first := got.Data[0]
				for _, b := range got.Data {
					if b != first {
						mixed.Add(1)
						break
					}
				}
			}
		}()
	}

	awaitReads(t, &reads, minTornReads)
	close(stop)
	wg.Wait()

	// The property that must hold: never a short or over-long file.
	if n := wrongLen.Load(); n > 0 {
		t.Errorf("chunked overwrite produced %d reads of the wrong length (of %d)", n, reads.Load())
	}
	t.Logf("chunked overwrite: %d of %d reads mixed two versions (known: chunks have no version barrier)",
		mixed.Load(), reads.Load())
}

// TestConcurrentWrite_DistinctPathsIndependent: writers on separate paths must
// not corrupt each other. Each path's content is a function of its own index,
// so cross-contamination is detectable.
func TestConcurrentWrite_DistinctPathsIndependent(t *testing.T) {
	ctx := context.Background()
	cm := rwTestManager(128, 64*1024*1024)

	const writers = 12
	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < 15; i++ {
				p := fmt.Sprintf("/indep/f%d", w)
				_ = cm.Put(ctx, &CacheEntry{
					FilePath: p, Data: mkBytes(byte('a'+w), 128*3), LastAccessed: time.Now(),
				})
			}
		}(w)
	}
	wg.Wait()

	for w := 0; w < writers; w++ {
		p := fmt.Sprintf("/indep/f%d", w)
		got, err := cm.Get(ctx, p)
		if err != nil {
			t.Errorf("%s: get: %v", p, err)
			continue
		}
		want := mkBytes(byte('a'+w), 128*3)
		if !bytes.Equal(got.Data, want) {
			t.Errorf("%s: content from another writer leaked in", p)
		}
	}
}

// TestConcurrentReadWrite_MixedLoad runs readers, writers, and deleters over a
// shared path set at once — the shape a real node sees. It asserts the
// all-or-nothing property: a read either fails or returns a complete file.
func TestConcurrentReadWrite_MixedLoad(t *testing.T) {
	ctx := context.Background()
	cm := rwTestManager(256, 64*1024*1024)

	const fileSize = 1024
	paths := make([]string, 8)
	for i := range paths {
		paths[i] = fmt.Sprintf("/mixed/f%d", i)
		if err := cm.Put(ctx, &CacheEntry{
			FilePath: paths[i], Data: mkBytes('0', fileSize), LastAccessed: time.Now(),
		}); err != nil {
			t.Fatalf("seed %s: %v", paths[i], err)
		}
	}

	var short atomic.Int64
	stop := make(chan struct{})
	var wg sync.WaitGroup

	for w := 0; w < 3; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; ; i++ {
				select {
				case <-stop:
					return
				default:
				}
				p := paths[(w*3+i)%len(paths)]
				_ = cm.Put(ctx, &CacheEntry{
					FilePath: p, Data: mkBytes(byte('a'+i%26), fileSize), LastAccessed: time.Now(),
				})
			}
		}(w)
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			_ = cm.Delete(ctx, paths[i%len(paths)])
		}
	}()
	for r := 0; r < 4; r++ {
		wg.Add(1)
		go func(r int) {
			defer wg.Done()
			for i := 0; ; i++ {
				select {
				case <-stop:
					return
				default:
				}
				got, err := cm.Get(ctx, paths[(r*2+i)%len(paths)])
				if err != nil || len(got.Data) == 0 {
					continue // deleted is a legitimate outcome here
				}
				if len(got.Data) != fileSize {
					short.Add(1)
				}
			}
		}(r)
	}

	time.Sleep(300 * time.Millisecond)
	close(stop)
	wg.Wait()

	if n := short.Load(); n > 0 {
		t.Errorf("%d reads returned a truncated file under mixed load (reads must be all-or-nothing)", n)
	}
	assertNoDrift(t, cm, ctx, "after mixed read/write/delete load")
}

// TestConcurrentReadWrite_UnderEviction: the same, but with a cache far too
// small for the working set, so eviction runs continuously against the
// readers. Eviction removing bytes mid-read must not surface as a short read.
func TestConcurrentReadWrite_UnderEviction(t *testing.T) {
	ctx := context.Background()
	cm := rwTestManager(256, 8*1024) // 12 files x 1KB into an 8KB cache

	const fileSize = 1024
	paths := make([]string, 12)
	for i := range paths {
		paths[i] = fmt.Sprintf("/ev/f%d", i)
	}

	var short atomic.Int64
	stop := make(chan struct{})
	var wg sync.WaitGroup

	for w := 0; w < 3; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; ; i++ {
				select {
				case <-stop:
					return
				default:
				}
				_ = cm.Put(ctx, &CacheEntry{
					FilePath:     paths[(w*4+i)%len(paths)],
					Data:         mkBytes(byte('a'+i%26), fileSize),
					LastAccessed: time.Now(),
				})
			}
		}(w)
	}
	for r := 0; r < 4; r++ {
		wg.Add(1)
		go func(r int) {
			defer wg.Done()
			for i := 0; ; i++ {
				select {
				case <-stop:
					return
				default:
				}
				got, err := cm.Get(ctx, paths[(r*3+i)%len(paths)])
				if err != nil || len(got.Data) == 0 {
					continue // evicted with no cloud copy is legitimate
				}
				if len(got.Data) != fileSize {
					short.Add(1)
				}
			}
		}(r)
	}

	time.Sleep(300 * time.Millisecond)
	close(stop)
	wg.Wait()

	if n := short.Load(); n > 0 {
		t.Errorf("%d truncated reads while eviction ran concurrently", n)
	}
	// Eviction is the path that must release ledger credit; drift here means
	// the cache will refuse writes on an empty disk.
	assertNoDrift(t, cm, ctx, "after concurrent load under eviction")
}

// TestReadRange_Boundaries covers the offset/size edges in one table: past
// EOF, straddling a chunk edge, exact-chunk, clipped, and degenerate sizes.
func TestReadRange_Boundaries(t *testing.T) {
	ctx := context.Background()
	cm := rwTestManager(64, 64*1024*1024)

	data := patternBytes(200)
	if err := cm.Put(ctx, &CacheEntry{FilePath: "/r", Data: data, LastAccessed: time.Now()}); err != nil {
		t.Fatalf("put: %v", err)
	}

	for _, tc := range []struct {
		name string
		off  int64
		size int
		want []byte
	}{
		{"whole file", 0, 200, data},
		{"first byte", 0, 1, data[0:1]},
		{"last byte", 199, 1, data[199:200]},
		{"straddles chunk boundary", 60, 8, data[60:68]},
		{"exactly one chunk", 64, 64, data[64:128]},
		{"clipped at EOF", 195, 50, data[195:200]},
		{"starts at EOF", 200, 10, nil},
		{"far past EOF", 100000, 10, nil},
		{"zero size", 0, 0, nil},
		{"negative size", 0, -1, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := cm.ReadRange(ctx, "/r", tc.off, tc.size)
			if err != nil {
				t.Fatalf("err = %v", err)
			}
			if len(got) != len(tc.want) {
				t.Fatalf("len = %d, want %d", len(got), len(tc.want))
			}
			if len(tc.want) > 0 && !bytes.Equal(got, tc.want) {
				t.Error("content mismatch")
			}
		})
	}
}

// TestReadWrite_DegenerateSizes: empty, single-byte, exactly-chunk-size, and
// chunk-size-plus-one, through all three read entrances. The chunk-boundary
// pair catches off-by-one in the chunk-count arithmetic.
func TestReadWrite_DegenerateSizes(t *testing.T) {
	ctx := context.Background()
	cm := rwTestManager(64, 64*1024*1024)

	for _, tc := range []struct {
		name string
		data []byte
	}{
		{"empty", []byte{}},
		{"one byte", []byte{'x'}},
		{"one below chunk size", mkBytes('b', 63)},
		{"exactly chunk size", mkBytes('c', 64)},
		{"chunk size plus one", mkBytes('d', 65)},
		{"exactly two chunks", mkBytes('e', 128)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := "/deg/" + tc.name
			if err := cm.Put(ctx, &CacheEntry{FilePath: path, Data: tc.data, LastAccessed: time.Now()}); err != nil {
				t.Fatalf("put: %v", err)
			}

			got, err := cm.Get(ctx, path)
			if err != nil {
				t.Fatalf("Get: %v", err)
			}
			if !bytes.Equal(got.Data, tc.data) {
				t.Errorf("Get: %d bytes, want %d", len(got.Data), len(tc.data))
			}

			// Over-long size must clip to EOF, not error or pad.
			rr, err := cm.ReadRange(ctx, path, 0, len(tc.data)+10)
			if err != nil {
				t.Fatalf("ReadRange: %v", err)
			}
			if !bytes.Equal(rr, tc.data) {
				t.Errorf("ReadRange: %d bytes, want %d", len(rr), len(tc.data))
			}

			var buf bytes.Buffer
			n, err := cm.WriteTo(ctx, path, &buf)
			if err != nil {
				t.Fatalf("WriteTo: %v", err)
			}
			if int(n) != len(tc.data) || !bytes.Equal(buf.Bytes(), tc.data) {
				t.Errorf("WriteTo: n=%d buf=%d, want %d", n, buf.Len(), len(tc.data))
			}
		})
	}
}

// TestReadRange_ServesFreshBytesAfterOverwrite: the range cache is keyed by
// path, so an overwrite has to invalidate it. Otherwise a reader that warmed
// the cache before the write keeps being served the previous version.
func TestReadRange_ServesFreshBytesAfterOverwrite(t *testing.T) {
	ctx := context.Background()
	cm := rwTestManager(64, 64*1024*1024)

	v1 := mkBytes('1', 64*4)
	v2 := mkBytes('2', 64*4)

	if err := cm.Put(ctx, &CacheEntry{FilePath: "/rc", Data: v1, LastAccessed: time.Now()}); err != nil {
		t.Fatalf("put v1: %v", err)
	}
	if got, err := cm.ReadRange(ctx, "/rc", 0, len(v1)); err != nil || got[0] != '1' {
		t.Fatalf("warm the range cache: err=%v", err)
	}
	if err := cm.Put(ctx, &CacheEntry{FilePath: "/rc", Data: v2, LastAccessed: time.Now()}); err != nil {
		t.Fatalf("put v2: %v", err)
	}

	got, err := cm.ReadRange(ctx, "/rc", 0, len(v2))
	if err != nil {
		t.Fatalf("read after overwrite: %v", err)
	}
	if !bytes.Equal(got, v2) {
		t.Error("range cache served the pre-overwrite version")
	}
}

// TestDelete_ReadsAreAllOrNothing: a reader racing a delete must see the whole
// file or nothing — never a partially-removed chunk set.
func TestDelete_ReadsAreAllOrNothing(t *testing.T) {
	ctx := context.Background()
	cm := rwTestManager(64, 64*1024*1024)

	full := mkBytes('Z', 64*6)
	var partial atomic.Int64

	for round := 0; round < 60; round++ {
		if err := cm.Put(ctx, &CacheEntry{FilePath: "/dr", Data: full, LastAccessed: time.Now()}); err != nil {
			t.Fatalf("put: %v", err)
		}
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			_ = cm.Delete(ctx, "/dr")
		}()
		go func() {
			defer wg.Done()
			got, err := cm.Get(ctx, "/dr")
			if err != nil {
				return // not-found is legitimate
			}
			if len(got.Data) != 0 && len(got.Data) != len(full) {
				partial.Add(1)
			}
		}()
		wg.Wait()
	}

	if n := partial.Load(); n > 0 {
		t.Errorf("%d reads saw a partially-deleted file", n)
	}
}

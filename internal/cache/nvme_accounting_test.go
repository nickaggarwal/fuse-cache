package cache

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

// nvmeUsed is a hand-maintained counter, not a derived quantity, and eviction
// can only reclaim bytes that the ledger credits to a key. So any path that
// adds without a matching release leaks capacity permanently: the node drifts
// toward "full" while the disk is nearly empty, and then refuses every write
// with "NVMe storage full after eviction". These tests pin the invariant
// nvmeUsed == sum of on-disk entry sizes across the paths that mutate it.

func mkBytes(b byte, n int) []byte {
	out := make([]byte, n)
	for i := range out {
		out[i] = b
	}
	return out
}

// nvmeSnapshot reads the counter and the ground truth it claims to track under
// a single lock acquisition. Taking them separately tears: a write landing
// between the two reads shows up as drift of exactly one chunk, in whichever
// direction the interleaving fell, even though the ledger was consistent at
// every instant. Ground truth is the summed size of every NVMe entry whose
// bytes are actually present.
func nvmeSnapshot(cm *DefaultCacheManager, ctx context.Context) (used, truth int64) {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	used = cm.nvmeUsed
	for _, e := range cm.entries.snapshot() {
		if e != nil && e.Tier == TierNVMe && cm.nvmeStorage.Exists(ctx, e.FilePath) {
			truth += e.Size
		}
	}
	return used, truth
}

// onDiskBytes is the ground truth alone, for callers that only need it.
func onDiskBytes(t *testing.T, cm *DefaultCacheManager, ctx context.Context) int64 {
	t.Helper()
	_, truth := nvmeSnapshot(cm, ctx)
	return truth
}

func assertNoDrift(t *testing.T, cm *DefaultCacheManager, ctx context.Context, when string) {
	t.Helper()
	used, truth := nvmeSnapshot(cm, ctx)
	if used != truth {
		t.Errorf("%s: nvmeUsed = %d, want %d (drift %+d)", when, used, truth, used-truth)
	}
}

func accountingTestManager(chunkSize, maxNVMe int64) *DefaultCacheManager {
	cm := newTestCacheManager()
	cm.config.ChunkSize = chunkSize
	cm.config.MaxNVMeSize = maxNVMe
	return cm
}

// TestNVMeAccounting_ConcurrentSamePathWrites is the regression test for the
// drift bug. Callers install the entry-table row *after* putToNVMeWithEviction
// returns, so an implementation that sizes the delta from the table sees "no
// existing entry" in every racing writer: each adds a full size, one row
// lands, and the surplus is unreclaimable because eviction only frees what an
// entry claims. Before the fix this reported nvmeUsed=2112 against 320 real
// bytes.
func TestNVMeAccounting_ConcurrentSamePathWrites(t *testing.T) {
	ctx := context.Background()
	cm := accountingTestManager(64, 64*1024*1024)

	data := mkBytes('S', 64*5) // chunked: worst case, one account per chunk
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = cm.Put(ctx, &CacheEntry{
				FilePath:     "/same",
				Data:         append([]byte(nil), data...),
				LastAccessed: time.Now(),
			})
		}()
	}
	wg.Wait()

	assertNoDrift(t, cm, ctx, "after 16 concurrent same-path writes")
}

// TestNVMeAccounting_SamePathVsDistinctPaths separates the two shapes: distinct
// paths never collided even before the fix, so a test that only writes distinct
// paths passes against the bug.
func TestNVMeAccounting_SamePathVsDistinctPaths(t *testing.T) {
	ctx := context.Background()

	for _, tc := range []struct {
		name    string
		pathFor func(i int) string
		size    int
	}{
		{"same path unchunked", func(int) string { return "/one" }, 32},
		{"same path chunked", func(int) string { return "/one" }, 64 * 5},
		{"distinct paths unchunked", func(i int) string { return fmt.Sprintf("/d%d", i) }, 32},
		{"distinct paths chunked", func(i int) string { return fmt.Sprintf("/d%d", i) }, 64 * 5},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cm := accountingTestManager(64, 64*1024*1024)
			var wg sync.WaitGroup
			for i := 0; i < 16; i++ {
				wg.Add(1)
				go func(i int) {
					defer wg.Done()
					_ = cm.Put(ctx, &CacheEntry{
						FilePath:     tc.pathFor(i),
						Data:         mkBytes('x', tc.size),
						LastAccessed: time.Now(),
					})
				}(i)
			}
			wg.Wait()
			assertNoDrift(t, cm, ctx, tc.name)
		})
	}
}

// TestNVMeAccounting_ReleasedByRewriteAndDelete: drift that survives a delete
// is permanent for the process lifetime, since nothing else ever revisits the
// key. Both exits from a path must return the counter to truth.
func TestNVMeAccounting_ReleasedByRewriteAndDelete(t *testing.T) {
	ctx := context.Background()
	cm := accountingTestManager(64, 64*1024*1024)

	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = cm.Put(ctx, &CacheEntry{FilePath: "/p", Data: mkBytes('y', 64*5), LastAccessed: time.Now()})
		}()
	}
	wg.Wait()
	assertNoDrift(t, cm, ctx, "after concurrent burst")

	if err := cm.Put(ctx, &CacheEntry{FilePath: "/p", Data: mkBytes('z', 64*5), LastAccessed: time.Now()}); err != nil {
		t.Fatalf("serial rewrite: %v", err)
	}
	assertNoDrift(t, cm, ctx, "after serial rewrite")

	if err := cm.Delete(ctx, "/p"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if used, _ := cm.Stats(); used != 0 {
		t.Errorf("after deleting the only file, nvmeUsed = %d, want 0", used)
	}
}

// TestNVMeAccounting_ShrinkingOverwrite: replacing a chunked file with a
// smaller one must release the retired chunks' bytes. Dropping the chunk rows
// without releasing their credit leaked on every overwrite.
func TestNVMeAccounting_ShrinkingOverwrite(t *testing.T) {
	ctx := context.Background()
	cm := accountingTestManager(64, 64*1024*1024)

	if err := cm.Put(ctx, &CacheEntry{FilePath: "/s", Data: mkBytes('A', 64*8), LastAccessed: time.Now()}); err != nil {
		t.Fatalf("put big: %v", err)
	}
	small := mkBytes('B', 64*2)
	if err := cm.Put(ctx, &CacheEntry{FilePath: "/s", Data: small, LastAccessed: time.Now()}); err != nil {
		t.Fatalf("put small: %v", err)
	}

	assertNoDrift(t, cm, ctx, "after shrinking overwrite")

	got, err := cm.Get(ctx, "/s")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if len(got.Data) != len(small) {
		t.Errorf("read back %d bytes, want %d (orphan chunks from the larger version)", len(got.Data), len(small))
	}
}

// TestNVMeAccounting_ConcurrentPutAndDelete exercises the two mutation
// directions against each other on many paths at once.
func TestNVMeAccounting_ConcurrentPutAndDelete(t *testing.T) {
	ctx := context.Background()
	cm := accountingTestManager(1024, 64*1024*1024)

	var wg sync.WaitGroup
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < 60; i++ {
				p := fmt.Sprintf("/acct/w%d-f%d", w, i%5)
				_ = cm.Put(ctx, &CacheEntry{FilePath: p, Data: mkBytes(byte(i), 300), LastAccessed: time.Now()})
				if i%3 == 0 {
					_ = cm.Delete(ctx, p)
				}
			}
		}(w)
	}
	wg.Wait()

	assertNoDrift(t, cm, ctx, "after concurrent put/delete")
}

// TestNVMeAccounting_DriftDoesNotStarveTheCache is the consequence test: the
// counter is only interesting because writes are refused against it. A
// long-lived daemon rewriting hot paths concurrently — a warm racing a demand
// promote — must not accumulate apparent fullness.
func TestNVMeAccounting_DriftDoesNotStarveTheCache(t *testing.T) {
	ctx := context.Background()
	cm := accountingTestManager(64, 16*1024)

	for round := 0; round < 12; round++ {
		var wg sync.WaitGroup
		for i := 0; i < 8; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				_ = cm.Put(ctx, &CacheEntry{FilePath: "/hot", Data: mkBytes('h', 64*4), LastAccessed: time.Now()})
			}()
		}
		wg.Wait()
	}

	_, capacity := cm.Stats()
	used, truth := nvmeSnapshot(cm, ctx)
	if used != truth {
		t.Errorf("after 12 concurrent rounds: nvmeUsed = %d, want %d (%d of %d capacity is phantom)",
			used, truth, used-truth, capacity)
	}

	// The counter only matters because writes are refused against it.
	if err := cm.Put(ctx, &CacheEntry{FilePath: "/newfile", Data: mkBytes('n', 256), LastAccessed: time.Now()}); err != nil {
		t.Errorf("writing 256 bytes into a %d-byte cache holding %d real bytes failed: %v",
			capacity, truth, err)
	}
}

// TestNVMeAccounting_RenameIsNeutral: a rename moves bytes between keys rather
// than freeing them, so the credit has to move too — releasing without
// re-crediting undercounts and lets the cache overfill.
func TestNVMeAccounting_RenameIsNeutral(t *testing.T) {
	ctx := context.Background()
	// Rename moves bytes with os.Rename, so it needs a real NVMe directory.
	cm, _ := renameTestManager(t)

	if err := cm.Put(ctx, &CacheEntry{FilePath: "/old.bin", Data: mkBytes('r', 200), LastAccessed: time.Now()}); err != nil {
		t.Fatalf("put: %v", err)
	}
	before, _ := cm.Stats()

	if err := cm.Rename(ctx, "/old.bin", "/new.bin"); err != nil {
		t.Fatalf("rename: %v", err)
	}

	after, _ := cm.Stats()
	if after != before {
		t.Errorf("rename changed nvmeUsed: %d -> %d (a rename moves bytes, it does not free them)", before, after)
	}
	assertNoDrift(t, cm, ctx, "after rename")
}

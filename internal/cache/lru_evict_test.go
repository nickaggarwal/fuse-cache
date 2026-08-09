package cache

import (
	"context"
	"fmt"
	"testing"
	"time"
)

// The recency list is the eviction order, so these tests exercise it through
// the table directly (ordering, touch, delete) and through evictToTarget
// (candidates come off the cold end, blocked ones do not wedge the walk).

func tableOrder(t *entryTable) []string {
	var out []string
	t.eachColdest(func(path string, _ *CacheEntry) bool {
		out = append(out, path)
		return true
	})
	return out
}

func putPath(t *entryTable, path string) {
	t.put(&CacheEntry{FilePath: path, StoragePath: path, Tier: TierNVMe, Size: 1})
}

func TestEntryTableRecencyOrder(t *testing.T) {
	tbl := newEntryTable()
	for _, p := range []string{"/a", "/b", "/c"} {
		putPath(tbl, p)
	}
	if got := tableOrder(tbl); !equalStrings(got, []string{"/a", "/b", "/c"}) {
		t.Fatalf("insertion order = %v", got)
	}

	// Touching the coldest entry sends it to the hot end.
	tbl.touch("/a")
	if got := tableOrder(tbl); !equalStrings(got, []string{"/b", "/c", "/a"}) {
		t.Fatalf("after touching /a: %v", got)
	}
	// Touching what is already hottest is a no-op, not a corruption.
	tbl.touch("/a")
	if got := tableOrder(tbl); !equalStrings(got, []string{"/b", "/c", "/a"}) {
		t.Fatalf("re-touching the hottest entry reordered: %v", got)
	}
	// An unknown path must not disturb the list.
	tbl.touch("/nope")
	if got := tableOrder(tbl); !equalStrings(got, []string{"/b", "/c", "/a"}) {
		t.Fatalf("touching an absent path reordered: %v", got)
	}

	// A rewrite counts as a use.
	putPath(tbl, "/b")
	if got := tableOrder(tbl); !equalStrings(got, []string{"/c", "/a", "/b"}) {
		t.Fatalf("after rewriting /b: %v", got)
	}
}

// Deleting from the head, middle, and tail must all leave a walkable list.
func TestEntryTableRecencyDelete(t *testing.T) {
	cases := []struct {
		del  string
		want []string
	}{
		{"/a", []string{"/b", "/c"}},
		{"/b", []string{"/a", "/c"}},
		{"/c", []string{"/a", "/b"}},
	}
	for _, tc := range cases {
		t.Run("delete"+tc.del, func(t *testing.T) {
			tbl := newEntryTable()
			for _, p := range []string{"/a", "/b", "/c"} {
				putPath(tbl, p)
			}
			tbl.del(tc.del)
			if got := tableOrder(tbl); !equalStrings(got, tc.want) {
				t.Fatalf("got %v, want %v", got, tc.want)
			}
			// Re-adding the deleted path lands it at the hot end, not back in
			// its old slot (a stale prev/next would show up here).
			putPath(tbl, tc.del)
			if got := tableOrder(tbl); !equalStrings(got, append(append([]string{}, tc.want...), tc.del)) {
				t.Fatalf("after re-adding %s: %v", tc.del, got)
			}
		})
	}

	// Emptying the list entirely must reset both ends.
	tbl := newEntryTable()
	putPath(tbl, "/only")
	tbl.del("/only")
	if tbl.lruHead != nil || tbl.lruTail != nil {
		t.Fatal("emptied list kept a head or tail pointer")
	}
	putPath(tbl, "/again")
	if got := tableOrder(tbl); !equalStrings(got, []string{"/again"}) {
		t.Fatalf("list unusable after being emptied: %v", got)
	}
}

// touchEntries is the one place LastAccessed is bumped; it must move the
// recency list too, or eviction picks by an order nothing maintains.
func TestTouchEntriesMovesRecency(t *testing.T) {
	cm := evictTestManager(t, 10_000)
	seedNVMe(t, cm, "/cold.bin", 1000, true, 2*time.Hour)
	seedNVMe(t, cm, "/warm.bin", 1000, true, time.Hour)

	cm.touchEntries("/cold.bin")

	cm.mu.RLock()
	order := tableOrder(cm.entries)
	cm.mu.RUnlock()
	if !equalStrings(order, []string{"/warm.bin", "/cold.bin"}) {
		t.Fatalf("touch did not reorder the list: %v", order)
	}
}

// A read served off NVMe by the ranged path is a use, so the file it touched
// must survive an eviction that its untouched neighbour does not.
func TestReadRangePromotesOutOfEvictionOrder(t *testing.T) {
	cm := evictTestManager(t, 10_000)
	seedNVMe(t, cm, "/first.bin", 4000, true, 2*time.Hour)
	seedNVMe(t, cm, "/second.bin", 4000, true, time.Hour)

	if _, err := cm.ReadRange(context.Background(), "/first.bin", 0, 16); err != nil {
		t.Fatalf("ReadRange: %v", err)
	}
	cm.evictToTarget(context.Background(), 4000)

	cm.mu.RLock()
	_, firstAlive := cm.entries.get("/first.bin")
	_, secondAlive := cm.entries.get("/second.bin")
	cm.mu.RUnlock()
	if !firstAlive {
		t.Fatal("evicted the file that was just read")
	}
	if secondAlive {
		t.Fatal("kept the colder file instead")
	}
}

// Candidates that cannot be evicted — pinned or not yet durable — must not
// stall the walk: they stay at the cold end of the list, so a pass that
// re-examined them would loop on the same entries and free nothing.
func TestEvictToTargetSkipsPastBlockedEntries(t *testing.T) {
	cm := evictTestManager(t, 100_000)
	cm.config.PinnedPrefixesFn = func() []string { return []string{"/pinned"} }

	// Enough blocked entries at the cold end to exceed one batch, so the pass
	// has to advance through more than one batch to reach anything evictable.
	blocked := evictBatchSize + 16
	for i := 0; i < blocked; i++ {
		seedNVMe(t, cm, fmt.Sprintf("/pinned/p%03d.bin", i), 100, true, time.Duration(blocked+10-i)*time.Minute)
	}
	seedNVMe(t, cm, "/unpersisted.bin", 100, false, 9*time.Minute)
	seedNVMe(t, cm, "/evictable.bin", 5000, true, 8*time.Minute)

	before, _ := cm.Stats()
	cm.evictToTarget(context.Background(), before-5000)

	cm.mu.RLock()
	_, evictableAlive := cm.entries.get("/evictable.bin")
	_, unpersistedAlive := cm.entries.get("/unpersisted.bin")
	_, pinnedAlive := cm.entries.get("/pinned/p000.bin")
	cm.mu.RUnlock()
	if evictableAlive {
		t.Fatal("blocked entries at the cold end stopped the pass from reaching an evictable file")
	}
	if !unpersistedAlive {
		t.Fatal("evicted a file with no durable copy")
	}
	if !pinnedAlive {
		t.Fatal("evicted a pinned file")
	}
}

// With nothing evictable the pass must return rather than spin on the same
// blocked candidates.
func TestEvictToTargetTerminatesWhenNothingEvictable(t *testing.T) {
	cm := evictTestManager(t, 10_000)
	for i := 0; i < 4; i++ {
		seedNVMe(t, cm, fmt.Sprintf("/u%d.bin", i), 1000, false, time.Duration(10-i)*time.Hour)
	}

	done := make(chan struct{})
	go func() {
		cm.evictToTarget(context.Background(), 0)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("evictToTarget did not terminate with no evictable candidates")
	}

	if used, _ := cm.Stats(); used != 4000 {
		t.Fatalf("used %d, want the 4000 bytes it could not evict", used)
	}
}

// A write that pushes usage into the watermark band wakes the background
// evictor instead of leaving it to the next 15s sweep.
func TestWriteSignalsEvictionPressure(t *testing.T) {
	cm := evictTestManager(t, 10_000)
	cm.evictSignal = make(chan struct{}, 1)

	seedNVMe(t, cm, "/quiet.bin", 1000, true, time.Hour)
	cm.maybeSignalEvictionPressure()
	select {
	case <-cm.evictSignal:
		t.Fatal("signalled at 10% usage, well below the high watermark")
	default:
	}

	seedNVMe(t, cm, "/loud.bin", 8000, true, time.Minute)
	cm.maybeSignalEvictionPressure()
	select {
	case <-cm.evictSignal:
	default:
		t.Fatal("no signal at 90% usage")
	}

	// A burst coalesces onto the pending wake rather than blocking.
	cm.maybeSignalEvictionPressure()
	cm.maybeSignalEvictionPressure()
	cm.maybeSignalEvictionPressure()
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// BenchmarkEvictToTarget measures the cost of reclaiming one file from a table
// holding many entries — the write-path case. The pre-LRU shape collected and
// sorted every entry regardless of how little it needed to free.
func BenchmarkEvictToTarget(b *testing.B) {
	const entries = 20000
	ctx := context.Background()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		cm := benchEvictManager(b, entries)
		used, _ := cm.Stats()
		b.StartTimer()
		cm.evictToTarget(ctx, used-1)
	}
}

// benchEvictManager seeds n durable NVMe entries, oldest first, without any
// real files behind them: the benchmark measures candidate selection, and the
// delete of the single file it reclaims is noise either way.
func benchEvictManager(b *testing.B, n int) *DefaultCacheManager {
	b.Helper()
	cm, dir := newRangeReadManager(b)
	cm.config.MaxNVMeSize = int64(n) * 200
	ctx := context.Background()
	cm.mu.Lock()
	for i := 0; i < n; i++ {
		path := fmt.Sprintf("/bench/f%06d.bin", i)
		cm.entries.put(&CacheEntry{
			FilePath: path, StoragePath: path, Size: 100,
			Tier: TierNVMe, PersistedToCloud: true,
			LastAccessed: time.Unix(int64(i), 0),
		})
		cm.accountNVMeLocked(path, 100)
	}
	cm.mu.Unlock()
	// Only the coldest entry is actually reclaimed, so only it needs to exist.
	if err := cm.nvmeStorage.Write(ctx, "/bench/f000000.bin", make([]byte, 100)); err != nil {
		b.Fatalf("seed: %v", err)
	}
	_ = dir
	return cm
}

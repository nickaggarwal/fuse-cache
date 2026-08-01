package cache

import (
	"io"
	"log"
	"testing"
	"time"
)

func newRangeBudgetManager(t testing.TB, perFileMax, globalMax int64) *DefaultCacheManager {
	t.Helper()
	return &DefaultCacheManager{
		config: &CacheConfig{
			ChunkSize:                4,
			RangeChunkCacheSize:      1024,
			RangeChunkCacheMaxBytes:  perFileMax,
			RangeCacheGlobalMaxBytes: globalMax,
			RangeCacheIdleExpiry:     time.Minute,
		},
		entries:     make(map[string]*CacheEntry),
		logger:      log.New(io.Discard, "", 0),
		metrics:     NewCacheMetrics(),
		rangeChunks: make(map[string]*chunkFileCache),
		hybridHints: make(map[string]hybridReadHint),
		tierPerf:    newTierPerfTracker(),
	}
}

// fillFile inserts n 4-byte chunks for path, stamping lastAccess to at.
func fillFile(cm *DefaultCacheManager, path string, n int, at time.Time) {
	for i := 0; i < n; i++ {
		cm.setChunkInRangeCache(path, int64(i), []byte("abcd"), TierPeer)
	}
	cm.rangeMu.Lock()
	if fc := cm.rangeChunks[path]; fc != nil {
		fc.lastAccess = at
	}
	cm.rangeMu.Unlock()
}

// TestGlobalRangeBudget_EvictsLRUFile verifies whole-file LRU eviction kicks
// in when the all-files budget is exceeded, and spares the active file.
func TestGlobalRangeBudget_EvictsLRUFile(t *testing.T) {
	// Per-file budget 40 bytes (10 chunks), global 100 bytes.
	cm := newRangeBudgetManager(t, 40, 100)
	now := time.Now()

	fillFile(cm, "/old.bin", 10, now.Add(-time.Hour)) // 40 bytes, oldest
	fillFile(cm, "/mid.bin", 10, now.Add(-time.Minute))
	// Third file pushes total to 120 > 100: /old.bin must go, active file stays.
	fillFile(cm, "/hot.bin", 10, now)

	cm.rangeMu.RLock()
	_, oldAlive := cm.rangeChunks["/old.bin"]
	_, midAlive := cm.rangeChunks["/mid.bin"]
	_, hotAlive := cm.rangeChunks["/hot.bin"]
	cm.rangeMu.RUnlock()

	if oldAlive {
		t.Fatal("LRU file /old.bin should have been evicted by global budget")
	}
	if !midAlive || !hotAlive {
		t.Fatalf("mid=%v hot=%v: newer files should survive", midAlive, hotAlive)
	}
	if got := cm.metrics.RangeCacheFileEvictions.Load(); got != 1 {
		t.Fatalf("RangeCacheFileEvictions = %d, want 1", got)
	}
}

// TestGlobalRangeBudget_ActiveFileAloneOverBudget: when a single active file
// exceeds the global budget by itself, per-file limits govern — the active
// cache must not be evicted (that would make every read a re-fetch).
func TestGlobalRangeBudget_ActiveFileAloneOverBudget(t *testing.T) {
	// Per-file budget 400 (no per-file eviction for 10 chunks), global 20.
	cm := newRangeBudgetManager(t, 400, 20)
	fillFile(cm, "/big.bin", 10, time.Now()) // 40 bytes > global 20

	cm.rangeMu.RLock()
	fc := cm.rangeChunks["/big.bin"]
	cm.rangeMu.RUnlock()
	if fc == nil || len(fc.chunks) == 0 {
		t.Fatal("active file's cache must survive when it is the only one")
	}
}

// TestGlobalRangeBudget_Disabled verifies negative budget disables enforcement.
func TestGlobalRangeBudget_Disabled(t *testing.T) {
	cm := newRangeBudgetManager(t, 40, -1)
	now := time.Now()
	fillFile(cm, "/a.bin", 10, now.Add(-time.Hour))
	fillFile(cm, "/b.bin", 10, now.Add(-time.Hour))
	fillFile(cm, "/c.bin", 10, now)

	cm.rangeMu.RLock()
	files := len(cm.rangeChunks)
	cm.rangeMu.RUnlock()
	if files != 3 {
		t.Fatalf("files = %d, want 3 (no eviction when disabled)", files)
	}
}

// TestSweepIdleRangeCaches verifies idle caches are dropped, recently-touched
// and prefetch-in-flight caches are kept.
func TestSweepIdleRangeCaches(t *testing.T) {
	cm := newRangeBudgetManager(t, 400, -1)
	now := time.Now()

	fillFile(cm, "/idle.bin", 4, now.Add(-2*time.Minute))
	fillFile(cm, "/fresh.bin", 4, now.Add(-2*time.Second))
	fillFile(cm, "/inflight.bin", 4, now.Add(-2*time.Minute))
	cm.rangeMu.Lock()
	cm.rangeChunks["/inflight.bin"].prefetchInFlight[7] = struct{}{}
	cm.rangeMu.Unlock()

	cm.sweepIdleRangeCaches(now, time.Minute)

	cm.rangeMu.RLock()
	_, idleAlive := cm.rangeChunks["/idle.bin"]
	_, freshAlive := cm.rangeChunks["/fresh.bin"]
	_, inflightAlive := cm.rangeChunks["/inflight.bin"]
	cm.rangeMu.RUnlock()

	if idleAlive {
		t.Fatal("/idle.bin should have been expired")
	}
	if !freshAlive {
		t.Fatal("/fresh.bin should have been kept (recent access)")
	}
	if !inflightAlive {
		t.Fatal("/inflight.bin should have been kept (prefetch in flight)")
	}
	if got := cm.metrics.RangeCacheIdleExpiries.Load(); got != 1 {
		t.Fatalf("RangeCacheIdleExpiries = %d, want 1", got)
	}
}

// TestSweepIdle_CountsUnconsumedPrefetchAsWaste ensures dropping an idle cache
// still feeds the prefetch waste counter for unconsumed readahead.
func TestSweepIdle_CountsUnconsumedPrefetchAsWaste(t *testing.T) {
	cm := newRangeBudgetManager(t, 400, -1)
	now := time.Now()
	fillFile(cm, "/w.bin", 3, now.Add(-2*time.Minute))
	cm.markChunkPrefetched("/w.bin", 0)
	cm.markChunkPrefetched("/w.bin", 1)

	cm.sweepIdleRangeCaches(now, time.Minute)

	if got := cm.metrics.PrefetchWasted.Load(); got != 2 {
		t.Fatalf("PrefetchWasted = %d, want 2", got)
	}
}

// TestDeriveRangeCacheGlobalMaxBytes checks the derivation bounds.
func TestDeriveRangeCacheGlobalMaxBytes(t *testing.T) {
	perFile := int64(256 * 1024 * 1024)
	got := deriveRangeCacheGlobalMaxBytes(perFile)
	if got < perFile {
		t.Fatalf("global budget %d < per-file %d", got, perFile)
	}
	if got > 4*perFile {
		t.Fatalf("global budget %d > 4x per-file %d", got, 4*perFile)
	}
	if d := deriveRangeCacheGlobalMaxBytes(0); d <= 0 {
		t.Fatalf("derivation with zero per-file budget = %d, want positive", d)
	}
}

// TestObserveReadPattern_TouchesLastAccess verifies reads refresh the LRU
// stamp so an actively-streamed file never idles out.
func TestObserveReadPattern_TouchesLastAccess(t *testing.T) {
	cm := newRangeBudgetManager(t, 400, -1)
	cm.config.RangePrefetchChunks = 2
	cm.config.RangePrefetchMaxChunks = 8

	fillFile(cm, "/s.bin", 2, time.Now().Add(-time.Hour))
	before := time.Now()
	cm.observeReadPattern("/s.bin", 0, 8, 1, 4)

	cm.rangeMu.RLock()
	la := cm.rangeChunks["/s.bin"].lastAccess
	cm.rangeMu.RUnlock()
	if la.Before(before) {
		t.Fatalf("lastAccess %v not refreshed by observeReadPattern", la)
	}
}

package cache

// Tests for the mountpoint-s3-inspired changes: size revalidation on the
// eviction probe, long-streak window widening, throughput ceiling, and the
// multipart part-size guard.

import (
	"context"
	"testing"
	"time"
)

// TestEvictionSafe_SizeMismatchBlocksEviction: an object in cloud with the
// same key but different size is a different version — the local copy must
// not be evicted against it.
func TestEvictionSafe_SizeMismatchBlocksEviction(t *testing.T) {
	cm := evictTestManager(t, 10_000)
	seedNVMe(t, cm, "/v2.bin", 4000, false, 2*time.Hour)
	// Cloud holds an OLD version with a different size.
	cm.cloudStorage.Write(context.Background(), "/v2.bin", make([]byte, 1000))
	seedNVMe(t, cm, "/other.bin", 4000, true, time.Hour)
	seedNVMe(t, cm, "/other2.bin", 4000, true, time.Minute)

	cm.Evict(context.Background(), TierNVMe)

	cm.mu.RLock()
	_, alive := cm.entries["/v2.bin"]
	cm.mu.RUnlock()
	if !alive {
		t.Fatal("evicted local copy against a different-size cloud version")
	}
}

// TestEvictionSafe_ChunkedLastChunkSizeCheck: chunked parents revalidate the
// last chunk's exact size, catching truncated persists.
func TestEvictionSafe_ChunkedLastChunkSizeCheck(t *testing.T) {
	cm := evictTestManager(t, 1<<30)
	cm.config.ChunkSize = 1000
	ctx := context.Background()

	entry := &CacheEntry{FilePath: "/c.bin", Size: 2500, IsChunked: true, NumChunks: 3, Tier: TierNVMe}
	// Correct persist: chunks 0,1 = 1000 bytes, chunk 2 = 500.
	cm.cloudStorage.Write(ctx, chunkPathFor("/c.bin", 0), make([]byte, 1000))
	cm.cloudStorage.Write(ctx, chunkPathFor("/c.bin", 2), make([]byte, 500))
	if !cm.evictionSafe(ctx, "/c.bin", entry) {
		t.Fatal("correctly-persisted chunked entry should be evictable")
	}
	// Truncated last chunk: NOT safe.
	cm.cloudStorage.Write(ctx, chunkPathFor("/c.bin", 2), make([]byte, 200))
	if cm.evictionSafe(ctx, "/c.bin", entry) {
		t.Fatal("truncated last chunk must block eviction")
	}
}

// TestLongStreakWidensWindow: past the streak threshold the window may
// exceed the throughput-derived cap (up to the byte budget).
func TestLongStreakWidensWindow(t *testing.T) {
	cm := profileTestManager()
	cm.rangeChunks = make(map[string]*chunkFileCache)
	cm.config.ChunkSize = 4
	cm.config.RangePrefetchChunks = 2
	cm.config.RangePrefetchMaxChunks = 8
	cm.config.RangePrefetchMaxBytes = 1 << 20

	path := "/stream.bin"
	fileSize := int64(1 << 20)
	var window int
	// Drive a long sequential streak.
	for i := 0; i < longStreakThreshold+4; i++ {
		off := int64(i * 4)
		window, _ = cm.observeReadPattern(path, fileSize, off, off+4, off/4, 4)
	}
	if window <= 8 {
		t.Fatalf("window after long streak = %d, want > static cap 8", window)
	}
	if max := longStreakMultiplier * 8; window > max {
		t.Fatalf("window %d exceeds widened cap %d", window, max)
	}

	// A seek resets the streak and the window.
	window, seq := cm.observeReadPattern(path, fileSize, 1<<19, 1<<19+4, (1<<19)/4, 4)
	if seq || window > 8 {
		t.Fatalf("after seek: window=%d sequential=%v, want reset", window, seq)
	}
}

// TestThroughputCeiling_CapsDerivation: MaxThroughputBytesPerSec bounds the
// derived per-stream rate and therefore the window.
func TestThroughputCeiling_CapsDerivation(t *testing.T) {
	cm := profileTestManager()
	const chunkSize = 4 * 1024 * 1024
	seedPeerThroughput(cm, 2_000_000_000) // 2GB/s measured

	uncapped := cm.adaptivePrefetchMaxChunks(chunkSize)

	// Cap total throughput at 200MB/s: per-stream 25MB/s across 8 workers.
	cm.config.MaxThroughputBytesPerSec = 200 * 1024 * 1024
	capped := cm.adaptivePrefetchMaxChunks(chunkSize)

	if capped >= uncapped {
		t.Fatalf("ceiling had no effect: capped=%d uncapped=%d", capped, uncapped)
	}
	if capped < cm.config.RangePrefetchMaxChunks {
		t.Fatalf("capped window %d fell below static floor", capped)
	}
}

// TestMultipartPartSizeFor: configured size below the 10,000-part ceiling is
// kept; oversized objects get the minimal part size that fits.
func TestMultipartPartSizeFor(t *testing.T) {
	const mib = int64(1024 * 1024)
	cases := []struct {
		object, configured, want int64
	}{
		{100 * mib, 8 * mib, 8 * mib},                            // small: unchanged
		{78 * 1024 * mib, 8 * mib, 8 * mib},                      // 78GiB: exactly at cap, fits
		{80 * 1024 * mib, 8 * mib, (80*1024*mib + 9999) / 10000}, // past cap: raised
		{0, 8 * mib, 8 * mib},                                    // empty object
	}
	for _, c := range cases {
		if got := multipartPartSizeFor(c.object, c.configured); got != c.want {
			t.Fatalf("multipartPartSizeFor(%d, %d) = %d, want %d", c.object, c.configured, got, c.want)
		}
		// Invariant: result always fits in 10,000 parts.
		if c.object > 0 {
			got := multipartPartSizeFor(c.object, c.configured)
			if parts := (c.object + got - 1) / got; parts > 10000 {
				t.Fatalf("object %d at part size %d needs %d parts", c.object, got, parts)
			}
		}
	}
}

// TestRehydrateNVMeAccounting: a fresh manager over a pre-populated cache dir
// adopts files with sizes, mtime-based LRU age, and no persisted bit; skips
// sidecars and staging temps; leaves pre-existing entries alone.
func TestRehydrateNVMeAccounting(t *testing.T) {
	cm := evictTestManager(t, 1<<30)
	ctx := context.Background()

	// Simulate the PREVIOUS process: land files directly on disk.
	cm.nvmeStorage.Write(ctx, "/old-whole.bin", make([]byte, 3000))
	cm.nvmeStorage.Write(ctx, "/old.bin_chunk_0", make([]byte, 1024))
	cm.nvmeStorage.Write(ctx, "/sub/dir/nested.bin", make([]byte, 500))
	cm.nvmeStorage.Write(ctx, "/old-whole.bin.sha256", []byte("cafe"))
	// Pre-existing entry must not be double-counted.
	seedNVMe(t, cm, "/known.bin", 2000, true, time.Hour)

	adopted, bytes := cm.rehydrateNVMeAccounting()

	if adopted != 3 {
		t.Fatalf("adopted = %d, want 3 (whole, chunk, nested; not sidecar/known)", adopted)
	}
	if bytes != 3000+1024+500 {
		t.Fatalf("bytes = %d, want %d", bytes, 3000+1024+500)
	}
	cm.mu.RLock()
	used := cm.nvmeUsed
	e := cm.entries["/old-whole.bin"]
	nested := cm.entries["/sub/dir/nested.bin"]
	cm.mu.RUnlock()
	if used != 2000+3000+1024+500 {
		t.Fatalf("nvmeUsed = %d, want %d", used, 2000+3000+1024+500)
	}
	if e == nil || e.Tier != TierNVMe || e.PersistedToCloud {
		t.Fatalf("rehydrated entry wrong: %+v (persisted bit must be false)", e)
	}
	if nested == nil || nested.Size != 500 {
		t.Fatalf("nested entry not adopted: %+v", nested)
	}

	// Idempotent: second pass adopts nothing.
	adopted2, _ := cm.rehydrateNVMeAccounting()
	if adopted2 != 0 {
		t.Fatalf("second rehydration adopted %d, want 0", adopted2)
	}
}

// TestRehydratedEntriesEvictOnlyWhenCloudConfirms: the restart flow end to
// end — rehydrated (unpersisted-bit) entries evict only after the cloud
// size-probe confirms durability.
func TestRehydratedEntriesEvictOnlyWhenCloudConfirms(t *testing.T) {
	cm := evictTestManager(t, 5_000)
	ctx := context.Background()

	// Previous process landed two files; only one made it to cloud.
	cm.nvmeStorage.Write(ctx, "/durable.bin", make([]byte, 3000))
	cm.cloudStorage.Write(ctx, "/durable.bin", make([]byte, 3000))
	cm.nvmeStorage.Write(ctx, "/local-only.bin", make([]byte, 3000))

	cm.rehydrateNVMeAccounting()
	// Make /durable.bin the older (preferred) eviction candidate.
	cm.mu.Lock()
	cm.entries["/durable.bin"].LastAccessed = time.Now().Add(-2 * time.Hour)
	cm.entries["/local-only.bin"].LastAccessed = time.Now().Add(-1 * time.Hour)
	used := cm.nvmeUsed
	cm.mu.Unlock()
	if used != 6000 {
		t.Fatalf("nvmeUsed = %d, want 6000", used)
	}

	// 6000 > 5000 budget: evict down to 4500.
	cm.Evict(ctx, TierNVMe)

	cm.mu.RLock()
	_, durableAlive := cm.entries["/durable.bin"]
	_, localAlive := cm.entries["/local-only.bin"]
	cm.mu.RUnlock()
	if durableAlive {
		t.Fatal("cloud-confirmed rehydrated entry should have been evicted")
	}
	if !localAlive {
		t.Fatal("local-only rehydrated entry must survive (only copy)")
	}
}

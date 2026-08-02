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

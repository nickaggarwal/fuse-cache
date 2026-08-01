package cache

import (
	"testing"
	"time"
)

func profileTestManager() *DefaultCacheManager {
	return &DefaultCacheManager{
		config: &CacheConfig{
			ChunkSize:              8 * 1024 * 1024,
			ParallelRangeReads:     8,
			RangePrefetchChunks:    4,
			RangePrefetchMaxChunks: 16,
			RangePrefetchMaxBytes:  512 * 1024 * 1024,
		},
		metrics: NewCacheMetrics(),
	}
}

// seedPeerThroughput simulates measured per-stream peer reads at bps.
func seedPeerThroughput(cm *DefaultCacheManager, bytesPerSec int64) {
	cm.metrics.PeerReadOps.Store(100)
	cm.metrics.PeerReadBytes.Store(bytesPerSec)
	cm.metrics.PeerReadNanos.Store(int64(time.Second))
}

// TestReadProfile_SmallFileSkipsRangeMachinery: <= 1 chunk means one worker
// and zero readahead regardless of link speed.
func TestReadProfile_SmallFileSkipsRangeMachinery(t *testing.T) {
	cm := profileTestManager()
	seedPeerThroughput(cm, 2_000_000_000) // 2GB/s — must not matter

	p := cm.readProfileFor(4*1024*1024, cm.config.ChunkSize)
	if p.workers != 1 || p.prefetchMaxChunks != 0 {
		t.Fatalf("small profile = %+v, want workers=1 prefetch=0", p)
	}
}

// TestReadProfile_MediumUsesConfigCappedToFile: configured workers, window
// never exceeding the file's own chunk count.
func TestReadProfile_MediumUsesConfigCappedToFile(t *testing.T) {
	cm := profileTestManager()
	seedPeerThroughput(cm, 2_000_000_000)

	// 64MB file = 8 chunks; derived window would be far larger.
	p := cm.readProfileFor(64*1024*1024, cm.config.ChunkSize)
	if p.workers != cm.config.ParallelRangeReads {
		t.Fatalf("medium workers = %d, want configured %d", p.workers, cm.config.ParallelRangeReads)
	}
	if p.prefetchMaxChunks > 8 {
		t.Fatalf("medium window %d exceeds file's 8 chunks", p.prefetchMaxChunks)
	}
}

// TestReadProfile_LargeScalesWorkersWithThroughput: fast measured link widens
// the pool above config, capped at largeReadWorkersMax; slow link keeps the
// configured floor.
func TestReadProfile_LargeScalesWorkersWithThroughput(t *testing.T) {
	cm := profileTestManager()
	big := int64(5) * 1024 * 1024 * 1024

	// No samples: configured floor.
	p := cm.readProfileFor(big, cm.config.ChunkSize)
	if p.workers != cm.config.ParallelRangeReads {
		t.Fatalf("no-samples workers = %d, want floor %d", p.workers, cm.config.ParallelRangeReads)
	}

	// Fast link (1GB/s per stream): widened, but bounded.
	seedPeerThroughput(cm, 1_000_000_000)
	p = cm.readProfileFor(big, cm.config.ChunkSize)
	if p.workers <= cm.config.ParallelRangeReads {
		t.Fatalf("fast-link workers = %d, want > configured %d", p.workers, cm.config.ParallelRangeReads)
	}
	if p.workers > largeReadWorkersMax {
		t.Fatalf("workers %d exceed cap %d", p.workers, largeReadWorkersMax)
	}

	// Slow link (20MB/s per stream): derivation would shrink below config —
	// configured value must win (never downgrade explicit tuning).
	seedPeerThroughput(cm, 20_000_000)
	p = cm.readProfileFor(big, cm.config.ChunkSize)
	if p.workers != cm.config.ParallelRangeReads {
		t.Fatalf("slow-link workers = %d, want configured floor %d", p.workers, cm.config.ParallelRangeReads)
	}
}

// TestPersistWorkers_ScaleWithBacklog: floor for small files, max for
// checkpoint-scale chunk counts, never more workers than chunks.
func TestPersistWorkers_ScaleWithBacklog(t *testing.T) {
	cm := profileTestManager()
	cases := []struct {
		chunks int64
		want   int
	}{
		{1, 1},                      // fewer chunks than floor -> chunk count
		{3, 3},                      // still under floor
		{8, persistWorkersMin},      // floor
		{64, 8},                     // 64/8
		{640, persistWorkersMax},    // checkpoint-scale hits the max
		{100000, persistWorkersMax}, // stays bounded
	}
	for _, c := range cases {
		if got := cm.persistWorkersFor(c.chunks); got != c.want {
			t.Fatalf("persistWorkersFor(%d) = %d, want %d", c.chunks, got, c.want)
		}
	}
}

// TestObserveReadPattern_SmallFileNoPrefetch: profile integration — a file
// of one chunk gets no readahead window even on a sequential pass.
func TestObserveReadPattern_SmallFileNoPrefetch(t *testing.T) {
	cm := profileTestManager()
	cm.rangeChunks = make(map[string]*chunkFileCache)
	cm.config.ChunkSize = 4
	cm.config.RangePrefetchChunks = 2

	window, _ := cm.observeReadPattern("/tiny.bin", 4, 0, 4, 0, 4)
	if window != 0 {
		t.Fatalf("single-chunk file window = %d, want 0", window)
	}
	// A larger file on the same manager still gets a window.
	window, _ = cm.observeReadPattern("/big.bin", 1<<20, 0, 4, 0, 4)
	if window <= 0 {
		t.Fatalf("large file window = %d, want > 0", window)
	}
}

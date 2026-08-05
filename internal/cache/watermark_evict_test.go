package cache

import (
	"context"
	"fmt"
	"io"
	"log"
	"testing"
	"time"
)

func evictTestManager(t *testing.T, maxNVMe int64) *DefaultCacheManager {
	t.Helper()
	dir := t.TempDir()
	nvme, err := NewNVMeStorage(dir)
	if err != nil {
		t.Fatalf("NewNVMeStorage: %v", err)
	}
	shutdownCtx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	return &DefaultCacheManager{
		config: &CacheConfig{
			NVMePath: dir, MaxNVMeSize: maxNVMe, ChunkSize: 1024,
			CloudRetryCount: 1, CloudRetryBaseWait: time.Millisecond,
		},
		nvmeStorage:  nvme,
		peerStorage:  newMockStorage(),
		cloudStorage: newMockStorage(),
		entries:      make(map[string]*CacheEntry),
		logger:       log.New(io.Discard, "", 0),
		metrics:      NewCacheMetrics(),
		rangeChunks:  make(map[string]*chunkFileCache),
		hybridHints:  make(map[string]hybridReadHint),
		tierPerf:     newTierPerfTracker(),
		shutdownCtx:  shutdownCtx,
	}
}

// seedNVMe writes a file to NVMe with the given persist state and age.
func seedNVMe(t *testing.T, cm *DefaultCacheManager, path string, size int, persisted bool, age time.Duration) {
	t.Helper()
	data := make([]byte, size)
	if err := cm.nvmeStorage.Write(context.Background(), path, data); err != nil {
		t.Fatalf("seed %s: %v", path, err)
	}
	if persisted {
		// Mirror into mock cloud so the Exists probe also passes.
		cm.cloudStorage.Write(context.Background(), path, data)
	}
	cm.mu.Lock()
	cm.entries[path] = &CacheEntry{
		FilePath: path, StoragePath: path, Size: int64(size),
		LastAccessed: time.Now().Add(-age), Tier: TierNVMe,
		PersistedToCloud: persisted,
	}
	cm.nvmeUsed += int64(size)
	cm.mu.Unlock()
}

// TestEvict_SkipsUnpersistedFiles: the durability guard must leave the only
// copy alone even when it is the LRU candidate.
func TestEvict_SkipsUnpersistedFiles(t *testing.T) {
	cm := evictTestManager(t, 10_000)
	// Oldest file NOT persisted, newer ones persisted.
	seedNVMe(t, cm, "/unpersisted-old.bin", 4000, false, 3*time.Hour)
	seedNVMe(t, cm, "/persisted-mid.bin", 4000, true, 2*time.Hour)
	seedNVMe(t, cm, "/persisted-new.bin", 4000, true, 1*time.Hour)
	// Usage 12000 > 10000 budget; target = 9000.

	if err := cm.Evict(context.Background(), TierNVMe); err != nil {
		t.Fatalf("Evict: %v", err)
	}

	cm.mu.RLock()
	_, unpersistedAlive := cm.entries["/unpersisted-old.bin"]
	_, midAlive := cm.entries["/persisted-mid.bin"]
	used := cm.nvmeUsed
	cm.mu.RUnlock()

	if !unpersistedAlive {
		t.Fatal("evicted the only copy of an unpersisted file")
	}
	if midAlive {
		t.Fatal("persisted LRU file should have been evicted instead")
	}
	if used > 9000 {
		t.Fatalf("used %d > target 9000", used)
	}
	if cm.metrics.EvictionSkippedUnpersisted.Load() == 0 {
		t.Fatal("skip counter not incremented")
	}
}

// TestEvict_CloudProbeRescuesRestartedEntries: after a restart the in-memory
// bit is false, but a cloud HEAD finding the object makes it evictable.
func TestEvict_CloudProbeRescuesRestartedEntries(t *testing.T) {
	cm := evictTestManager(t, 5_000)
	// Bit false but object present in (mock) cloud — simulates restart.
	seedNVMe(t, cm, "/restarted.bin", 4000, false, 2*time.Hour)
	cm.cloudStorage.Write(context.Background(), "/restarted.bin", make([]byte, 4000))
	seedNVMe(t, cm, "/hot.bin", 3000, true, time.Minute)
	// 7000 > 5000; target 4500.

	cm.Evict(context.Background(), TierNVMe)

	cm.mu.RLock()
	_, restartedAlive := cm.entries["/restarted.bin"]
	cm.mu.RUnlock()
	if restartedAlive {
		t.Fatal("cloud-confirmed entry should have been evicted after probe")
	}
}

// TestMarkPersisted_ChunkedParentAggregation: parent flips only when every
// chunk is durable.
func TestMarkPersisted_ChunkedParentAggregation(t *testing.T) {
	cm := evictTestManager(t, 1<<30)
	parent := "/p.bin"
	cm.mu.Lock()
	cm.entries[parent] = &CacheEntry{FilePath: parent, Size: 3072, IsChunked: true, NumChunks: 3, Tier: TierNVMe}
	for i := int64(0); i < 3; i++ {
		cp := chunkPathFor(parent, i)
		cm.entries[cp] = &CacheEntry{FilePath: cp, Size: 1024, Tier: TierNVMe}
	}
	cm.mu.Unlock()

	cm.markPersistedToCloud(chunkPathFor(parent, 0))
	cm.markPersistedToCloud(chunkPathFor(parent, 2))
	cm.mu.RLock()
	parentFlagged := cm.entries[parent].PersistedToCloud
	cm.mu.RUnlock()
	if parentFlagged {
		t.Fatal("parent flagged with a chunk still unpersisted")
	}

	cm.markPersistedToCloud(chunkPathFor(parent, 1))
	cm.mu.RLock()
	parentFlagged = cm.entries[parent].PersistedToCloud
	cm.mu.RUnlock()
	if !parentFlagged {
		t.Fatal("parent not flagged after all chunks persisted")
	}
}

// TestWatermarkEviction_TriggersAboveHighWatermark: internal pressure over
// 85% evicts down to 75% via the background path.
func TestWatermarkEviction_TriggersAboveHighWatermark(t *testing.T) {
	cm := evictTestManager(t, 10_000)
	for i := 0; i < 9; i++ {
		seedNVMe(t, cm, fmt.Sprintf("/f%d.bin", i), 1000, true, time.Duration(10-i)*time.Hour)
	}
	// used 9000 = 90% of 10000 > high watermark 85.

	cm.runWatermarkEviction(context.Background())

	used, _ := cm.Stats()
	if used > 7500 {
		t.Fatalf("used %d after watermark pass, want <= 7500 (75%%)", used)
	}
	if cm.metrics.WatermarkEvictorRuns.Load() != 1 {
		t.Fatal("run counter not incremented")
	}
	if cm.metrics.WatermarkEvictedBytes.Load() == 0 {
		t.Fatal("evicted bytes not counted")
	}
	// Oldest files went first.
	cm.mu.RLock()
	_, oldestAlive := cm.entries["/f0.bin"]
	_, newestAlive := cm.entries["/f8.bin"]
	cm.mu.RUnlock()
	if oldestAlive || !newestAlive {
		t.Fatalf("LRU order violated: oldest=%v newest=%v", oldestAlive, newestAlive)
	}
}

// TestWatermarkEviction_IdleBelowWatermark: no work under the band.
func TestWatermarkEviction_IdleBelowWatermark(t *testing.T) {
	cm := evictTestManager(t, 100_000)
	// nvmePressurePct takes the max of budget usage and real device usage
	// (statfs on NVMePath), so a developer machine whose disk is over the
	// high watermark makes the premise of this test unreachable — the
	// evictor correctly runs, on device pressure it cannot control. Skip
	// rather than fail: the assertion below is about budget pressure.
	if p := cm.nvmePressurePct(); p >= watermarkHighPct {
		t.Skipf("host filesystem at %d%% is already over the %d%% watermark", p, watermarkHighPct)
	}
	seedNVMe(t, cm, "/small.bin", 1000, true, time.Hour)

	cm.runWatermarkEviction(context.Background())

	if cm.metrics.WatermarkEvictorRuns.Load() != 0 {
		t.Fatal("evictor ran below the high watermark")
	}
	cm.mu.RLock()
	_, alive := cm.entries["/small.bin"]
	cm.mu.RUnlock()
	if !alive {
		t.Fatal("file evicted below watermark")
	}
}

// TestStatfsFreeTotal_Works sanity-checks the syscall wrapper.
func TestStatfsFreeTotal_Works(t *testing.T) {
	free, total, ok := statfsFreeTotal(t.TempDir())
	if !ok {
		t.Fatal("statfs failed on temp dir")
	}
	if total <= 0 || free < 0 || free > total {
		t.Fatalf("implausible statfs: free=%d total=%d", free, total)
	}
}

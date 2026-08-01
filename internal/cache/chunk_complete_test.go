package cache

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// completionTestManager builds a manager with a real NVMe dir, a parent
// chunked entry, some chunks present locally, and a mock peer storage
// serving the missing ones.
func completionTestManager(t *testing.T, numChunks int64, chunkSize int64, missing map[int64]bool) (*DefaultCacheManager, string, []byte) {
	t.Helper()
	dir := t.TempDir()
	nvme, err := NewNVMeStorage(dir)
	if err != nil {
		t.Fatalf("NewNVMeStorage: %v", err)
	}

	parent := "/cc.bin"
	full := make([]byte, numChunks*chunkSize)
	for i := range full {
		full[i] = byte(i % 251)
	}

	peer := newMockStorage()
	ctx := context.Background()
	for i := int64(0); i < numChunks; i++ {
		chunk := full[i*chunkSize : (i+1)*chunkSize]
		cp := fmt.Sprintf("%s_chunk_%d", parent, i)
		// All chunks available on the peer; only non-missing ones local.
		peer.Write(ctx, cp, chunk)
		if !missing[i] {
			if err := nvme.Write(ctx, cp, chunk); err != nil {
				t.Fatalf("seed chunk %d: %v", i, err)
			}
		}
	}

	shutdownCtx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	cm := &DefaultCacheManager{
		config: &CacheConfig{
			NVMePath:    dir,
			ChunkSize:   chunkSize,
			MaxNVMeSize: 1 << 30,
		},
		nvmeStorage:  nvme,
		peerStorage:  peer,
		cloudStorage: newMockStorage(),
		entries:      make(map[string]*CacheEntry),
		logger:       log.New(io.Discard, "", 0),
		metrics:      NewCacheMetrics(),
		rangeChunks:  make(map[string]*chunkFileCache),
		hybridHints:  make(map[string]hybridReadHint),
		tierPerf:     newTierPerfTracker(),
		shutdownCtx:  shutdownCtx,
	}
	cm.entries[parent] = &CacheEntry{
		FilePath: parent, Size: int64(len(full)), IsChunked: true, NumChunks: numChunks,
	}
	// Local chunk entries for the seeded ones.
	for i := int64(0); i < numChunks; i++ {
		if !missing[i] {
			cp := fmt.Sprintf("%s_chunk_%d", parent, i)
			cm.entries[cp] = &CacheEntry{FilePath: cp, Size: chunkSize, Tier: TierNVMe}
			cm.nvmeUsed += chunkSize
		}
	}
	return cm, parent, full
}

// TestChunkCompletion_AssemblesWholeFile: holes are fetched from the peer
// tier, the whole parent lands on NVMe byte-exact, chunk files are retired,
// and accounting is consistent.
func TestChunkCompletion_AssemblesWholeFile(t *testing.T) {
	const chunkSize = 1024
	cm, parent, full := completionTestManager(t, 8, chunkSize, map[int64]bool{2: true, 5: true, 7: true})

	if err := cm.runChunkCompletion(context.Background(), parent, 8, int64(len(full))); err != nil {
		t.Fatalf("runChunkCompletion: %v", err)
	}

	localPath, ok := cm.LocalFilePath(context.Background(), parent)
	if !ok {
		t.Fatal("whole file not present after completion")
	}
	got, err := os.ReadFile(localPath)
	if err != nil {
		t.Fatalf("read assembled: %v", err)
	}
	if string(got) != string(full) {
		t.Fatalf("assembled bytes differ: len %d vs %d", len(got), len(full))
	}

	// Chunk files retired from disk and entries.
	for i := int64(0); i < 8; i++ {
		cp := fmt.Sprintf("%s_chunk_%d", parent, i)
		if cm.nvmeStorage.Exists(context.Background(), cp) {
			t.Fatalf("chunk file %d not retired", i)
		}
		cm.mu.RLock()
		_, hasEntry := cm.entries[cp]
		cm.mu.RUnlock()
		if hasEntry {
			t.Fatalf("chunk entry %d not removed", i)
		}
	}
	// nvmeUsed should equal the whole file size (chunks retired).
	cm.mu.RLock()
	used := cm.nvmeUsed
	cm.mu.RUnlock()
	if used != int64(len(full)) {
		t.Fatalf("nvmeUsed = %d, want %d", used, len(full))
	}
	if cm.metrics.ChunkCompletionAssembled.Load() != 1 {
		t.Fatal("ChunkCompletionAssembled not counted")
	}
	if cm.metrics.ChunkCompletionFetched.Load() != 3 {
		t.Fatalf("ChunkCompletionFetched = %d, want 3", cm.metrics.ChunkCompletionFetched.Load())
	}
}

// TestChunkCompletion_SkipsWithoutHeadroom: no eviction for background work.
func TestChunkCompletion_SkipsWithoutHeadroom(t *testing.T) {
	const chunkSize = 1024
	cm, parent, full := completionTestManager(t, 4, chunkSize, nil)
	cm.config.MaxNVMeSize = cm.nvmeUsed + 100 // whole file cannot fit

	err := cm.runChunkCompletion(context.Background(), parent, 4, int64(len(full)))
	if err == nil {
		t.Fatal("expected headroom error")
	}
	if cm.metrics.ChunkCompletionSkipped.Load() != 1 {
		t.Fatal("ChunkCompletionSkipped not counted")
	}
	if _, ok := cm.LocalFilePath(context.Background(), parent); ok {
		t.Fatal("whole file must not exist after skip")
	}
}

// TestChunkCompletion_FetchFailureLeavesChunks: a hole that no tier can serve
// aborts assembly but keeps already-fetched chunks for the next attempt.
func TestChunkCompletion_FetchFailureLeavesChunks(t *testing.T) {
	const chunkSize = 1024
	cm, parent, full := completionTestManager(t, 4, chunkSize, map[int64]bool{1: true})
	// Remove the missing chunk from the peer too: unfetchable.
	cm.peerStorage.Delete(context.Background(), fmt.Sprintf("%s_chunk_%d", parent, 1))

	if err := cm.runChunkCompletion(context.Background(), parent, 4, int64(len(full))); err == nil {
		t.Fatal("expected fetch failure")
	}
	if _, ok := cm.LocalFilePath(context.Background(), parent); ok {
		t.Fatal("no whole file on failed completion")
	}
	// Seeded chunks must still be there.
	if !cm.nvmeStorage.Exists(context.Background(), fmt.Sprintf("%s_chunk_%d", parent, 0)) {
		t.Fatal("existing chunks must survive a failed pass")
	}
	// No temp leftovers.
	leftover, _ := filepath.Glob(filepath.Join(cm.config.NVMePath, ".nvme-stream-*"))
	if len(leftover) != 0 {
		t.Fatalf("leftover staging files: %v", leftover)
	}
}

// TestMaybeScheduleChunkCompletion_Dedupes: repeated triggers while a pass is
// in flight (or cooling down) schedule only one run.
func TestMaybeScheduleChunkCompletion_Dedupes(t *testing.T) {
	const chunkSize = 1024
	cm, parent, _ := completionTestManager(t, 4, chunkSize, map[int64]bool{3: true})

	for i := 0; i < 5; i++ {
		cm.maybeScheduleChunkCompletion(parent)
	}
	cm.bgWg.Wait()

	if got := cm.metrics.ChunkCompletionAssembled.Load(); got != 1 {
		t.Fatalf("assembled %d times, want exactly 1", got)
	}
	// Already whole: further triggers are no-ops.
	cm.maybeScheduleChunkCompletion(parent)
	cm.bgWg.Wait()
	if got := cm.metrics.ChunkCompletionAssembled.Load(); got != 1 {
		t.Fatalf("assembled again on whole file: %d", got)
	}
}

// busyThenOKStorage fails the first Read per path with busy, then serves.
type busyThenOKStorage struct {
	*mockStorage
	failed map[string]bool
}

func (b *busyThenOKStorage) Read(ctx context.Context, path string) ([]byte, error) {
	if !b.failed[path] {
		b.failed[path] = true
		return nil, &peerBusyError{addr: "test"}
	}
	return b.mockStorage.Read(ctx, path)
}

// TestBusyRetry_RecoversWithoutCloud: one busy failure on the peer tier is
// absorbed by the jittered retry; cloud is never touched.
func TestBusyRetry_RecoversWithoutCloud(t *testing.T) {
	dir := t.TempDir()
	nvme, _ := NewNVMeStorage(dir)
	peer := &busyThenOKStorage{mockStorage: newMockStorage(), failed: map[string]bool{}}
	peer.Write(context.Background(), "/f_chunk_0", []byte("payload"))

	shutdownCtx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	cloud := newMockStorage()
	cm := &DefaultCacheManager{
		config:       &CacheConfig{NVMePath: dir, ChunkSize: 4, MaxNVMeSize: 1 << 20},
		nvmeStorage:  nvme,
		peerStorage:  peer,
		cloudStorage: cloud,
		entries:      make(map[string]*CacheEntry),
		logger:       log.New(io.Discard, "", 0),
		metrics:      NewCacheMetrics(),
		rangeChunks:  make(map[string]*chunkFileCache),
		hybridHints:  make(map[string]hybridReadHint),
		tierPerf:     newTierPerfTracker(),
		shutdownCtx:  shutdownCtx,
	}

	entry, servedTier, err := cm.getFromRemoteTierWithBusyRetry(context.Background(), "/f_chunk_0", TierPeer)
	if err != nil {
		t.Fatalf("busy retry did not recover: %v", err)
	}
	if servedTier != TierPeer || string(entry.Data) != "payload" {
		t.Fatalf("tier=%v data=%q", servedTier, entry.Data)
	}
	if cm.herdStats.busyChunkRetries.Load() != 1 || cm.herdStats.busyChunkRetryHits.Load() != 1 {
		t.Fatalf("retry counters = %d/%d, want 1/1",
			cm.herdStats.busyChunkRetries.Load(), cm.herdStats.busyChunkRetryHits.Load())
	}
}

// alwaysBusyStorage always rejects with busy.
type alwaysBusyStorage struct{ *mockStorage }

func (alwaysBusyStorage) Read(ctx context.Context, path string) ([]byte, error) {
	return nil, &peerBusyError{addr: "test"}
}

// TestBusyRetry_StillBusySurfacesError: a persistently busy peer fails after
// exactly one retry so the caller can proceed to the next tier.
func TestBusyRetry_StillBusySurfacesError(t *testing.T) {
	dir := t.TempDir()
	nvme, _ := NewNVMeStorage(dir)
	shutdownCtx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	cm := &DefaultCacheManager{
		config:       &CacheConfig{NVMePath: dir, ChunkSize: 4, MaxNVMeSize: 1 << 20},
		nvmeStorage:  nvme,
		peerStorage:  alwaysBusyStorage{newMockStorage()},
		cloudStorage: newMockStorage(),
		entries:      make(map[string]*CacheEntry),
		logger:       log.New(io.Discard, "", 0),
		metrics:      NewCacheMetrics(),
		rangeChunks:  make(map[string]*chunkFileCache),
		hybridHints:  make(map[string]hybridReadHint),
		tierPerf:     newTierPerfTracker(),
		shutdownCtx:  shutdownCtx,
	}

	start := time.Now()
	_, _, err := cm.getFromRemoteTierWithBusyRetry(context.Background(), "/x", TierPeer)
	if err == nil || !isPeerBusy(err) {
		t.Fatalf("want busy error, got %v", err)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("retry took %v, should be one short jittered wait", elapsed)
	}
	if cm.herdStats.busyChunkRetryHits.Load() != 0 {
		t.Fatal("no retry hit expected")
	}
}

// TestAdaptivePrefetchMaxChunks covers the derivation gates and bounds.
func TestAdaptivePrefetchMaxChunks(t *testing.T) {
	cm := &DefaultCacheManager{
		config: &CacheConfig{
			RangePrefetchMaxChunks: 16,
			ParallelRangeReads:     8,
			RangePrefetchMaxBytes:  512 * 1024 * 1024,
		},
		metrics: NewCacheMetrics(),
	}
	const chunkSize = 4 * 1024 * 1024

	// Below sample gate: static cap.
	if got := cm.adaptivePrefetchMaxChunks(chunkSize); got != 16 {
		t.Fatalf("pre-samples window = %d, want static 16", got)
	}

	// Simulate measured peer reads: 100MB/s per stream x 8 workers x 250ms
	// = 200MB target = 50 chunks.
	cm.metrics.PeerReadOps.Store(100)
	cm.metrics.PeerReadBytes.Store(100 * 1024 * 1024)
	cm.metrics.PeerReadNanos.Store(int64(time.Second))
	got := cm.adaptivePrefetchMaxChunks(chunkSize)
	if got != 50 {
		t.Fatalf("derived window = %d chunks, want 50", got)
	}

	// Budget ceiling: shrink prefetch budget below the derived target.
	cm.config.RangePrefetchMaxBytes = 64 * 1024 * 1024
	if got := cm.adaptivePrefetchMaxChunks(chunkSize); got != 16 {
		t.Fatalf("budget-capped window = %d, want static floor 16", got)
	}

	// Slow link: derived below static floor -> static wins.
	cm.config.RangePrefetchMaxBytes = 512 * 1024 * 1024
	cm.metrics.PeerReadBytes.Store(1024 * 1024) // 1MB/s
	if got := cm.adaptivePrefetchMaxChunks(chunkSize); got != 16 {
		t.Fatalf("slow-link window = %d, want static 16", got)
	}
}

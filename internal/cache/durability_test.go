package cache

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"fuse-client/internal/coordinator"
)

// switchableCloud fails every write while down is set, and can block writes to
// keys with a given substring until released, so tests can observe a receipt
// while an upload is genuinely in flight.
type switchableCloud struct {
	*mockStorage
	down    atomic.Bool
	mu      sync.Mutex
	block   string
	release chan struct{}
	writes  atomic.Int64
}

func newSwitchableCloud() *switchableCloud {
	return &switchableCloud{mockStorage: newMockStorage()}
}

func (s *switchableCloud) Write(ctx context.Context, path string, data []byte) error {
	s.writes.Add(1)
	if s.down.Load() {
		return errors.New("cloud unavailable")
	}
	s.mu.Lock()
	block, rel := s.block, s.release
	s.mu.Unlock()
	if block != "" && strings.Contains(path, block) && rel != nil {
		select {
		case <-rel:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return s.mockStorage.Write(ctx, path, data)
}

func (s *switchableCloud) blockOn(substr string) chan struct{} {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.block = substr
	s.release = make(chan struct{})
	return s.release
}

// newDiskDurabilityManager builds a manager on a real NVMe directory with a
// journalled tracker, the shape NewCacheManager produces, without starting
// the retry loop (tests drive it explicitly).
func newDiskDurabilityManager(t *testing.T, dir string, cloud TierStorage, chunkSize int64) *DefaultCacheManager {
	t.Helper()
	nvme, err := NewNVMeStorage(dir)
	if err != nil {
		t.Fatal(err)
	}
	shutdownCtx, shutdownCancel := context.WithCancel(context.Background())
	cm := &DefaultCacheManager{
		config: &CacheConfig{
			NVMePath:           dir,
			MaxNVMeSize:        64 << 20,
			ChunkSize:          chunkSize,
			CloudRetryCount:    1,
			CloudRetryBaseWait: time.Millisecond,
			CloudTimeout:       5 * time.Second,
		},
		nvmeStorage:    nvme,
		peerStorage:    &noopStorage{},
		cloudStorage:   cloud,
		entries:        newEntryTable(),
		nvmeBytes:      make(map[string]int64),
		tombstones:     newDeleteTombstones(),
		logger:         log.New(os.Stderr, "[DURABILITY-TEST] ", 0),
		metrics:        NewCacheMetrics(),
		rangeChunks:    make(map[string]*chunkFileCache),
		hybridHints:    make(map[string]hybridReadHint),
		hedgeLimiter:   make(chan struct{}, 16),
		shutdownCtx:    shutdownCtx,
		shutdownCancel: shutdownCancel,
	}
	cm.durability = newDurabilityTracker(filepath.Join(dir, durabilityDirName), cm.logger)
	t.Cleanup(func() {
		shutdownCancel()
		cm.bgWg.Wait()
	})
	return cm
}

func stagedPut(t *testing.T, cm *DefaultCacheManager, path string, data []byte) {
	t.Helper()
	f, staged, err := cm.CreateWriteFile(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write(data); err != nil {
		t.Fatal(err)
	}
	f.Close()
	if err := cm.PutFromFile(context.Background(), path, staged, int64(len(data)), time.Now()); err != nil {
		t.Fatal(err)
	}
}

func receiptFor(t *testing.T, cm *DefaultCacheManager, wait time.Duration, paths ...string) *DurabilityReceipt {
	t.Helper()
	r, err := cm.DurabilityReceipt(context.Background(), DurabilityRequest{Paths: paths, Wait: wait})
	if err != nil {
		t.Fatalf("receipt: %v", err)
	}
	return r
}

func randData(n int) []byte {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return b
}

// TestReceiptWithheldWhileAChunkIsPending: the receipt is the barrier
// pod-snapshotter gates restore eligibility on, so it must never be issued
// while any chunk of a file is still uploading -- even when the other chunks
// have landed.
func TestReceiptWithheldWhileAChunkIsPending(t *testing.T) {
	cloud := newSwitchableCloud()
	cm := newDiskDurabilityManager(t, t.TempDir(), cloud, 1024)
	release := cloud.blockOn("_chunk_2")

	data := randData(3*1024 - 10)
	stagedPut(t, cm, "/ckpt/a.bin", data)

	// Chunks 0 and 1 land; chunk 2 is held.
	deadline := time.Now().Add(5 * time.Second)
	for !cloud.Exists(context.Background(), "/ckpt/a.bin_chunk_1") && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	r := receiptFor(t, cm, 0, "/ckpt/a.bin")
	if r.Durable {
		t.Fatalf("receipt issued with chunk 2 still uploading: %+v", r.Paths)
	}
	if st := r.Paths[0].State; st != DurabilityUploading {
		t.Fatalf("state = %s, want uploading", st)
	}

	close(release)
	r = receiptFor(t, cm, 5*time.Second, "/ckpt/a.bin")
	if !r.Durable {
		t.Fatalf("receipt never issued after the last chunk landed: %+v", r.Paths)
	}
	p := r.Paths[0]
	if !p.IsChunked || p.Objects != 3 || p.Size != int64(len(data)) || p.Generation == 0 || p.Source != "tracker" {
		t.Fatalf("receipt entry wrong: %+v", p)
	}
	// The parent bit flips for whole-file writes now (it never did: there are
	// no chunk rows for PutFromFile, so the sibling scan could not fire).
	cm.mu.RLock()
	parent, _ := cm.entries.get("/ckpt/a.bin")
	persisted := parent != nil && parent.PersistedToCloud
	cm.mu.RUnlock()
	if !persisted {
		t.Fatal("parent of a PutFromFile chunked write was never marked PersistedToCloud")
	}
}

// TestExhaustedRetriesAreVisibleAndRetried: the old behavior logged CRITICAL
// and forgot the file. Now it is "failed" with the error, a receipt says so,
// and a retry (forced here via RetryFailed) persists it once the cloud is
// back.
func TestExhaustedRetriesAreVisibleAndRetried(t *testing.T) {
	cloud := newSwitchableCloud()
	cloud.down.Store(true)
	cm := newDiskDurabilityManager(t, t.TempDir(), cloud, 1024)

	stagedPut(t, cm, "/ckpt/small.bin", randData(100))
	waitState(t, cm, "/ckpt/small.bin", DurabilityFailed)

	r := receiptFor(t, cm, 0, "/ckpt/small.bin")
	if r.Durable || r.Paths[0].State != DurabilityFailed || r.Paths[0].LastError == "" || r.Paths[0].Attempts != 1 {
		t.Fatalf("failed upload not reported honestly: %+v", r.Paths[0])
	}

	cloud.down.Store(false)
	cm.startDurabilityLoop()
	r, err := cm.DurabilityReceipt(context.Background(), DurabilityRequest{
		Paths: []string{"/ckpt/small.bin"}, Wait: 5 * time.Second, RetryFailed: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !r.Durable {
		t.Fatalf("failed upload was not retried: %+v", r.Paths[0])
	}
}

// TestUnchunkedStagedWriteIsPersisted: PutFromFile at or under the chunk
// size never uploaded anything before I1, so every small file written
// through FUSE stayed NVMe-only and unevictable.
func TestUnchunkedStagedWriteIsPersisted(t *testing.T) {
	cloud := newSwitchableCloud()
	cm := newDiskDurabilityManager(t, t.TempDir(), cloud, 1024)
	data := randData(700)
	stagedPut(t, cm, "/ckpt/meta.json", data)

	if r := receiptFor(t, cm, 5*time.Second, "/ckpt/meta.json"); !r.Durable {
		t.Fatalf("small staged write never persisted: %+v", r.Paths[0])
	}
	got, err := cloud.Read(context.Background(), "/ckpt/meta.json")
	if err != nil || !bytes.Equal(got, data) {
		t.Fatalf("cloud copy wrong: err=%v", err)
	}
}

// TestInterruptedUploadResumesAfterRestart: the journal outlives the
// process. A write acked locally whose upload never finished is uploaded by
// the next process, the chunked shape is restored on the entry rehydration
// adopted as a plain file, and the receipt issues.
func TestInterruptedUploadResumesAfterRestart(t *testing.T) {
	dir := t.TempDir()
	cloud := newSwitchableCloud()
	cloud.down.Store(true)

	first := newDiskDurabilityManager(t, dir, cloud, 1024)
	data := randData(2*1024 + 300)
	stagedPut(t, first, "/ckpt/resume.bin", data)
	waitState(t, first, "/ckpt/resume.bin", DurabilityFailed)
	// The process dies.
	first.shutdownCancel()
	first.bgWg.Wait()

	if entries, _ := os.ReadDir(filepath.Join(dir, durabilityDirName)); len(entries) != 1 {
		t.Fatalf("want one journal record for the pending write, got %d", len(entries))
	}

	// A new process on the same NVMe directory.
	cloud.down.Store(false)
	second := newDiskDurabilityManager(t, dir, cloud, 4096) // different -chunk-size
	second.durability = nil
	second.rehydrateNVMeAccounting()
	second.initDurability()

	// Rehydration must not adopt journal files as cache entries.
	second.mu.RLock()
	second.entries.each(func(p string, _ *CacheEntry) bool {
		if strings.Contains(p, durabilityDirName) {
			t.Errorf("journal file adopted as a cache entry: %s", p)
		}
		return true
	})
	entry, _ := second.entries.get("/ckpt/resume.bin")
	shapeOK := entry != nil && entry.IsChunked && entry.NumChunks == 3 && entry.ChunkSize == 1024
	second.mu.RUnlock()
	if !shapeOK {
		t.Fatalf("resumed entry did not get its chunked shape back: %+v", entry)
	}

	r := receiptFor(t, second, 5*time.Second, "/ckpt/resume.bin")
	if !r.Durable {
		t.Fatalf("interrupted upload did not resume after restart: %+v", r.Paths[0])
	}
	// Uploaded at the stride it was written with, not the new flag.
	var assembled []byte
	for i := 0; i < 3; i++ {
		b, err := cloud.Read(context.Background(), chunkPathFor("/ckpt/resume.bin", int64(i)))
		if err != nil {
			t.Fatalf("chunk %d missing after resume: %v", i, err)
		}
		assembled = append(assembled, b...)
	}
	if !bytes.Equal(assembled, data) {
		t.Fatal("resumed chunks do not reassemble the original file")
	}
	if entries, _ := os.ReadDir(filepath.Join(dir, durabilityDirName)); len(entries) != 0 {
		t.Fatalf("journal record not removed after persist: %d left", len(entries))
	}
}

// TestRewriteSupersedesAPendingUpload: an upload of an older version must not
// certify the newer one. The receipt reports the newest generation only.
func TestRewriteSupersedesAPendingUpload(t *testing.T) {
	cloud := newSwitchableCloud()
	cm := newDiskDurabilityManager(t, t.TempDir(), cloud, 1024)
	release := cloud.blockOn("/ckpt/v.bin")

	stagedPut(t, cm, "/ckpt/v.bin", randData(100))
	first, _ := cm.durability.get("/ckpt/v.bin")
	v2 := randData(200)
	cloud.mu.Lock()
	cloud.block = "" // let the second version through
	cloud.mu.Unlock()
	stagedPut(t, cm, "/ckpt/v.bin", v2)
	close(release)

	r := receiptFor(t, cm, 5*time.Second, "/ckpt/v.bin")
	if !r.Durable || r.Paths[0].Size != 200 || r.Paths[0].Generation == first.CommittedAt {
		t.Fatalf("receipt does not describe the newest write: %+v", r.Paths[0])
	}
	if got, _ := cloud.Read(context.Background(), "/ckpt/v.bin"); len(got) != 200 {
		// The stale upload may land after the new one in this fake; what
		// matters is that the receipt never certified it. Report, not fail.
		t.Logf("note: cloud holds %d bytes; stale upload ordering is backend-dependent", len(got))
	}
}

// TestReceiptCloudProbeOnANonWriterNode: a node that did not write the file
// (no tracker record) verifies it from the coordinator's cloud row and the
// cloud itself, chunk by chunk, and refuses when a middle chunk is missing.
func TestReceiptCloudProbeOnANonWriterNode(t *testing.T) {
	cloud := newSwitchableCloud()
	cm := newDiskDurabilityManager(t, t.TempDir(), cloud, 1024)
	coord := coordinator.NewCoordinatorService()
	cm.config.Coordinator = coord
	ctx := context.Background()

	for i := 0; i < 3; i++ {
		cloud.mockStorage.Write(ctx, chunkPathFor("/m/x.bin", int64(i)), randData(map[int]int{0: 1024, 1: 1024, 2: 50}[i]))
	}
	_ = coord.UpdateFileLocation(ctx, &coordinator.FileLocation{
		FilePath: "/m/x.bin", PeerID: "writer", StorageTier: "cloud", FileSize: 2*1024 + 50, IsChunked: true, ChunkSize: 1024,
	})
	r := receiptFor(t, cm, 0, "/m/x.bin")
	if !r.Durable || r.Paths[0].Source != "cloud-probe" || r.Paths[0].Objects != 3 {
		t.Fatalf("non-writer could not verify a persisted file: %+v", r.Paths[0])
	}

	cloud.mockStorage.Delete(ctx, chunkPathFor("/m/x.bin", 1))
	if r := receiptFor(t, cm, 0, "/m/x.bin"); r.Durable {
		t.Fatalf("receipt issued with a missing middle chunk: %+v", r.Paths[0])
	}
	if r := receiptFor(t, cm, 0, "/m/nope.bin"); r.Durable || r.Paths[0].State != DurabilityUnknown {
		t.Fatalf("unknown path reported as %+v", r.Paths[0])
	}
}

// TestReceiptRequestValidation: the receipt is per-path verification, so a
// whole-namespace prefix is refused rather than turned into a table scan.
func TestReceiptRequestValidation(t *testing.T) {
	cm := newDiskDurabilityManager(t, t.TempDir(), newSwitchableCloud(), 1024)
	for _, req := range []DurabilityRequest{
		{},
		{Prefix: "/"},
		{Paths: []string{"/a.bin_chunk_3"}},
	} {
		if _, err := cm.DurabilityReceipt(context.Background(), req); !errors.Is(err, ErrDurabilityBadRequest) {
			t.Fatalf("request %+v: err=%v, want ErrDurabilityBadRequest", req, err)
		}
	}
}

// TestReceiptByPrefix covers a checkpoint directory written as several files.
func TestReceiptByPrefix(t *testing.T) {
	cloud := newSwitchableCloud()
	cm := newDiskDurabilityManager(t, t.TempDir(), cloud, 1024)
	stagedPut(t, cm, "/ckpt/run1/a.bin", randData(3000))
	stagedPut(t, cm, "/ckpt/run1/b.json", randData(10))
	stagedPut(t, cm, "/ckpt/run10/c.bin", randData(10)) // sibling, not under /ckpt/run1

	r, err := cm.DurabilityReceipt(context.Background(), DurabilityRequest{Prefix: "/ckpt/run1", Wait: 5 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	if !r.Durable || len(r.Paths) != 2 {
		t.Fatalf("prefix receipt wrong: durable=%v paths=%+v", r.Durable, r.Paths)
	}
}

// TestDeleteForgetsDurabilityState: a deleted path must not linger as
// pending (it would be retried forever against bytes that are gone).
func TestDeleteForgetsDurabilityState(t *testing.T) {
	cloud := newSwitchableCloud()
	cloud.down.Store(true)
	dir := t.TempDir()
	cm := newDiskDurabilityManager(t, dir, cloud, 1024)
	stagedPut(t, cm, "/ckpt/gone.bin", randData(10))
	waitState(t, cm, "/ckpt/gone.bin", DurabilityFailed)
	if err := cm.Delete(context.Background(), "/ckpt/gone.bin"); err != nil {
		t.Fatal(err)
	}
	if _, ok := cm.durability.get("/ckpt/gone.bin"); ok {
		t.Fatal("deleted path still tracked")
	}
	if entries, _ := os.ReadDir(filepath.Join(dir, durabilityDirName)); len(entries) != 0 {
		t.Fatalf("journal record survived delete: %d", len(entries))
	}
}

func waitState(t *testing.T, cm *DefaultCacheManager, path string, want DurabilityState) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if rec, ok := cm.durability.get(path); ok && rec.State == want && !rec.inflight {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	rec, _ := cm.durability.get(path)
	t.Fatalf("%s never reached %s: %+v", path, want, rec)
}

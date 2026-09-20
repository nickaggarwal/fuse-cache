package cache

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// fakeCloud is an in-memory TierStorage with enumeration, standing in for
// Azure Blob. It records writes so a test can assert how many cloud round
// trips a workload actually caused — the number this store exists to lower.
type fakeCloud struct {
	mu       sync.Mutex
	objects  map[string][]byte
	writes   int
	failNext bool
}

func newFakeCloud() *fakeCloud {
	return &fakeCloud{objects: make(map[string][]byte)}
}

func (f *fakeCloud) Read(ctx context.Context, path string) ([]byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	d, ok := f.objects[path]
	if !ok {
		return nil, fmt.Errorf("not found: %s", path)
	}
	out := make([]byte, len(d))
	copy(out, d)
	return out, nil
}

func (f *fakeCloud) Write(ctx context.Context, path string, data []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failNext {
		f.failNext = false
		return fmt.Errorf("injected cloud failure")
	}
	cp := make([]byte, len(data))
	copy(cp, data)
	f.objects[path] = cp
	f.writes++
	return nil
}

func (f *fakeCloud) Delete(ctx context.Context, path string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.objects, path)
	return nil
}

func (f *fakeCloud) Exists(ctx context.Context, path string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	_, ok := f.objects[path]
	return ok
}

func (f *fakeCloud) Size(ctx context.Context, path string) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	d, ok := f.objects[path]
	if !ok {
		return 0, fmt.Errorf("not found")
	}
	return int64(len(d)), nil
}

func (f *fakeCloud) ListUnder(ctx context.Context, prefix string) ([]string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []string
	for k := range f.objects {
		if strings.HasPrefix(k, prefix) {
			out = append(out, k)
		}
	}
	return out, nil
}

func (f *fakeCloud) writeCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.writes
}

// newTestStore builds a store over a real NVMeStorage in a temp dir, so the
// fsync/rename path under test is the production one.
func newTestStore(t *testing.T, cfg ModelKVConfig) (*ModelKVStore, *NVMeStorage, *fakeCloud, string) {
	t.Helper()
	dir := t.TempDir()
	nvme, err := NewNVMeStorage(dir)
	if err != nil {
		t.Fatalf("NewNVMeStorage: %v", err)
	}
	cloud := newFakeCloud()
	s := NewModelKVStore(cfg, nvme, cloud, nil)
	return s, nvme, cloud, dir
}

func testCfg() ModelKVConfig {
	c := DefaultModelKVConfig()
	// Keep batches small so a test can flush without generating 64MiB.
	c.BatchBytes = 1 << 20
	c.BatchInterval = 0
	c.ReapInterval = 0
	return c
}

// TestPutDoesNotTouchCloud pins the central policy claim: a write is local
// only. If this fails the store has regressed into the weights write path,
// which is the entire thing it was built to avoid.
func TestPutDoesNotTouchCloud(t *testing.T) {
	s, _, cloud, _ := newTestStore(t, testCfg())
	ctx := context.Background()

	for i := 0; i < 20; i++ {
		if err := s.Put(ctx, fmt.Sprintf("k%d", i), "conv-a", []byte("kvblockdata")); err != nil {
			t.Fatalf("Put: %v", err)
		}
	}
	if got := cloud.writeCount(); got != 0 {
		t.Fatalf("20 puts caused %d cloud writes, want 0", got)
	}
	if got := s.Stats().Puts; got != 20 {
		t.Fatalf("Puts = %d, want 20", got)
	}
}

// TestPromotionRequiresReuse checks that durability is earned. A block read
// once is not durable; the threshold-th read promotes it.
func TestPromotionRequiresReuse(t *testing.T) {
	cfg := testCfg()
	cfg.PromoteAfterReads = 2
	s, _, cloud, _ := newTestStore(t, cfg)
	ctx := context.Background()

	if err := s.Put(ctx, "hot", "conv-a", []byte("payload")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if _, err := s.Get(ctx, "hot"); err != nil {
		t.Fatalf("Get 1: %v", err)
	}
	if cloud.writeCount() != 0 {
		t.Fatalf("promoted after 1 read, want promotion only at 2")
	}
	if _, err := s.Get(ctx, "hot"); err != nil {
		t.Fatalf("Get 2: %v", err)
	}
	// Second read enqueues; batch is under BatchBytes so force the flush.
	if err := s.FlushPrefix(ctx, "conv-a"); err != nil {
		t.Fatalf("FlushPrefix: %v", err)
	}
	if cloud.writeCount() == 0 {
		t.Fatalf("no cloud write after reuse threshold reached")
	}
	st := s.Stats()
	if st.DurableBlocks != 1 {
		t.Fatalf("DurableBlocks = %d, want 1", st.DurableBlocks)
	}
}

// TestBatchCoalescing asserts many promoted blocks become few cloud objects.
// The ratio is the write-amplification reduction the design is selling.
func TestBatchCoalescing(t *testing.T) {
	cfg := testCfg()
	cfg.PromoteAfterReads = 1
	cfg.BatchBytes = 1 << 30 // never auto-flush; one forced flush at the end
	s, _, cloud, _ := newTestStore(t, cfg)
	ctx := context.Background()

	const n = 50
	for i := 0; i < n; i++ {
		k := fmt.Sprintf("blk%03d", i)
		if err := s.Put(ctx, k, "conv-a", []byte(strings.Repeat("x", 1024))); err != nil {
			t.Fatalf("Put: %v", err)
		}
		if _, err := s.Get(ctx, k); err != nil {
			t.Fatalf("Get: %v", err)
		}
	}
	if err := s.FlushPrefix(ctx, "conv-a"); err != nil {
		t.Fatalf("FlushPrefix: %v", err)
	}
	// One batch object + one catalog entry, regardless of block count.
	if got := cloud.writeCount(); got != 2 {
		t.Fatalf("%d blocks produced %d cloud writes, want 2 (batch + catalog)", n, got)
	}
	if got := s.Stats().DurableBlocks; got != n {
		t.Fatalf("DurableBlocks = %d, want %d", got, n)
	}
}

// TestRecoverIndexAfterRestart simulates a daemon restart: same disk, new
// store instance, empty in-memory index. This is the gap LMCache's disk
// backend leaves open — its __init__ never walks the directory, so every file
// on disk becomes unreachable.
func TestRecoverIndexAfterRestart(t *testing.T) {
	cfg := testCfg()
	dir := t.TempDir()
	nvme, err := NewNVMeStorage(dir)
	if err != nil {
		t.Fatalf("NewNVMeStorage: %v", err)
	}
	cloud := newFakeCloud()
	ctx := context.Background()

	s1 := NewModelKVStore(cfg, nvme, cloud, nil)
	for i := 0; i < 5; i++ {
		if err := s1.Put(ctx, fmt.Sprintf("k%d", i), "conv-a", []byte("data")); err != nil {
			t.Fatalf("Put: %v", err)
		}
	}

	// Restart: fresh store, same disk.
	s2 := NewModelKVStore(cfg, nvme, cloud, nil)
	if got := s2.Stats().Blocks; got != 0 {
		t.Fatalf("fresh store already has %d blocks", got)
	}
	if err := s2.RecoverIndex(ctx); err != nil {
		t.Fatalf("RecoverIndex: %v", err)
	}
	if got := s2.Stats().Blocks; got != 5 {
		t.Fatalf("recovered %d blocks, want 5", got)
	}
	// And the bytes are actually readable through the recovered index.
	if _, err := s2.Get(ctx, "k3"); err != nil {
		t.Fatalf("Get after recovery: %v", err)
	}
}

// TestRecoverFromCloudAfterNodeLoss is the real durability test: the node's
// entire disk is gone, and a replacement node must serve blocks it never
// wrote. A restart test cannot catch a regression here.
func TestRecoverFromCloudAfterNodeLoss(t *testing.T) {
	cfg := testCfg()
	cfg.PromoteAfterReads = 1
	cloud := newFakeCloud()
	ctx := context.Background()

	// Node A writes and promotes.
	dirA := t.TempDir()
	nvmeA, err := NewNVMeStorage(dirA)
	if err != nil {
		t.Fatalf("NewNVMeStorage A: %v", err)
	}
	sA := NewModelKVStore(cfg, nvmeA, cloud, nil)
	payload := []byte(strings.Repeat("kv", 512))
	for i := 0; i < 4; i++ {
		k := fmt.Sprintf("blk%d", i)
		if err := sA.Put(ctx, k, "conv-a", payload); err != nil {
			t.Fatalf("Put: %v", err)
		}
		if _, err := sA.Get(ctx, k); err != nil {
			t.Fatalf("Get: %v", err)
		}
	}
	if err := sA.FlushPrefix(ctx, "conv-a"); err != nil {
		t.Fatalf("FlushPrefix: %v", err)
	}

	// Node A is destroyed. Node B has a fresh, empty disk and no journal.
	os.RemoveAll(dirA)
	dirB := t.TempDir()
	nvmeB, err := NewNVMeStorage(dirB)
	if err != nil {
		t.Fatalf("NewNVMeStorage B: %v", err)
	}
	sB := NewModelKVStore(cfg, nvmeB, cloud, nil)

	n, err := sB.RecoverFromCloud(ctx, "conv-a")
	if err != nil {
		t.Fatalf("RecoverFromCloud: %v", err)
	}
	if n != 4 {
		t.Fatalf("recovered %d blocks, want 4", n)
	}
	// The decisive assertion: node B serves the bytes.
	got, err := sB.Get(ctx, "blk2")
	if err != nil {
		t.Fatalf("Get on replacement node: %v", err)
	}
	if string(got) != string(payload) {
		t.Fatalf("recovered payload mismatch: got %d bytes want %d", len(got), len(payload))
	}
	if sB.Stats().RecoveredFromCloud == 0 {
		t.Fatalf("RecoveredFromCloud not counted")
	}
}

// TestNonDurableBlockIsNotRecovered is the honesty check on the other side:
// a block that never earned promotion must NOT appear recoverable. A store
// that claimed it would be lying about durability.
func TestNonDurableBlockIsNotRecovered(t *testing.T) {
	cfg := testCfg()
	cfg.PromoteAfterReads = 5 // nothing will reach this
	cloud := newFakeCloud()
	ctx := context.Background()

	dirA := t.TempDir()
	nvmeA, _ := NewNVMeStorage(dirA)
	sA := NewModelKVStore(cfg, nvmeA, cloud, nil)
	if err := sA.Put(ctx, "cold", "conv-a", []byte("data")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if _, err := sA.Get(ctx, "cold"); err != nil {
		t.Fatalf("Get: %v", err)
	}

	os.RemoveAll(dirA)
	dirB := t.TempDir()
	nvmeB, _ := NewNVMeStorage(dirB)
	sB := NewModelKVStore(cfg, nvmeB, cloud, nil)
	n, _ := sB.RecoverFromCloud(ctx, "conv-a")
	if n != 0 {
		t.Fatalf("recovered %d never-promoted blocks, want 0", n)
	}
	if _, err := sB.Get(ctx, "cold"); err == nil {
		t.Fatalf("Get succeeded for a block that was never durable")
	}
}

// TestRecoverIndexDropsPhantomRows: a journal row whose block is gone locally
// and was never made durable describes nothing. Keeping it would make the
// index over-claim residency, the same failure the residency work found in
// coordinator location metadata.
func TestRecoverIndexDropsPhantomRows(t *testing.T) {
	cfg := testCfg()
	dir := t.TempDir()
	nvme, _ := NewNVMeStorage(dir)
	cloud := newFakeCloud()
	ctx := context.Background()

	s1 := NewModelKVStore(cfg, nvme, cloud, nil)
	if err := s1.Put(ctx, "gone", "conv-a", []byte("data")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	// Evict the block but leave its journal row, exactly as LRU eviction does.
	if err := nvme.Delete(ctx, s1.blockPath("gone")); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	s2 := NewModelKVStore(cfg, nvme, cloud, nil)
	if err := s2.RecoverIndex(ctx); err != nil {
		t.Fatalf("RecoverIndex: %v", err)
	}
	if got := s2.Stats().Blocks; got != 0 {
		t.Fatalf("index kept %d phantom rows, want 0", got)
	}
}

// TestTTLReaping checks blocks expire on last access, not creation.
func TestTTLReaping(t *testing.T) {
	cfg := testCfg()
	cfg.TTL = time.Hour
	s, _, _, _ := newTestStore(t, cfg)
	ctx := context.Background()

	base := time.Now()
	s.now = func() time.Time { return base }

	if err := s.Put(ctx, "old", "conv-a", []byte("d")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := s.Put(ctx, "fresh", "conv-a", []byte("d")); err != nil {
		t.Fatalf("Put: %v", err)
	}

	// Two hours later, touch only "fresh".
	s.now = func() time.Time { return base.Add(2 * time.Hour) }
	if _, err := s.Get(ctx, "fresh"); err != nil {
		t.Fatalf("Get: %v", err)
	}

	n := s.ReapExpired(ctx)
	if n != 1 {
		t.Fatalf("reaped %d, want 1", n)
	}
	if s.nvme.Exists(ctx, s.blockPath("old")) {
		t.Fatalf("expired block still on disk")
	}
	if !s.nvme.Exists(ctx, s.blockPath("fresh")) {
		t.Fatalf("recently-read block was reaped")
	}
}

// TestFlushFailureRetainsBatch: a failed cloud write must not silently drop
// durability for blocks that earned it.
func TestFlushFailureRetainsBatch(t *testing.T) {
	cfg := testCfg()
	cfg.PromoteAfterReads = 1
	cfg.BatchBytes = 1 << 30
	s, _, cloud, _ := newTestStore(t, cfg)
	ctx := context.Background()

	if err := s.Put(ctx, "k1", "conv-a", []byte("data")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if _, err := s.Get(ctx, "k1"); err != nil {
		t.Fatalf("Get: %v", err)
	}

	cloud.mu.Lock()
	cloud.failNext = true
	cloud.mu.Unlock()

	if err := s.FlushPrefix(ctx, "conv-a"); err == nil {
		t.Fatalf("FlushPrefix succeeded despite injected failure")
	}
	if s.Stats().DurableBlocks != 0 {
		t.Fatalf("block marked durable after a failed flush")
	}
	// The retry must succeed: the batch was put back, not dropped.
	if err := s.FlushPrefix(ctx, "conv-a"); err != nil {
		t.Fatalf("retry flush: %v", err)
	}
	if s.Stats().DurableBlocks != 1 {
		t.Fatalf("retry did not restore durability")
	}
}

// TestWriteSyncDurability checks the fsync path produces a correct file. It
// cannot prove power-loss durability in a unit test, but it can prove the
// barrier path is wired and writes the right bytes.
func TestWriteSyncDurability(t *testing.T) {
	dir := t.TempDir()
	nvme, err := NewNVMeStorage(dir)
	if err != nil {
		t.Fatalf("NewNVMeStorage: %v", err)
	}
	ctx := context.Background()
	want := []byte("durable bytes")
	if err := nvme.WriteSync(ctx, "a/b/c.bin", want); err != nil {
		t.Fatalf("WriteSync: %v", err)
	}
	got, err := os.ReadFile(filepath.Join(dir, "a/b/c.bin"))
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if string(got) != string(want) {
		t.Fatalf("content mismatch: %q", got)
	}
	// No temp files left behind.
	entries, _ := os.ReadDir(filepath.Join(dir, "a/b"))
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), ".nvme-") {
			t.Fatalf("temp file left behind: %s", e.Name())
		}
	}
}

// TestStoreUsesDurableWriter pins that the store actually picked up the fsync
// path rather than silently falling back to plain Write.
func TestStoreUsesDurableWriter(t *testing.T) {
	s, _, _, _ := newTestStore(t, testCfg())
	if s.durable == nil {
		t.Fatalf("store did not detect NVMeStorage.WriteSync")
	}
}

// TestConcurrentPutGet runs the store under -race to catch index/pending
// locking mistakes.
func TestConcurrentPutGet(t *testing.T) {
	cfg := testCfg()
	cfg.PromoteAfterReads = 2
	cfg.BatchBytes = 4096
	s, _, _, _ := newTestStore(t, cfg)
	ctx := context.Background()

	var wg sync.WaitGroup
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < 25; i++ {
				k := fmt.Sprintf("w%d-k%d", w, i)
				if err := s.Put(ctx, k, fmt.Sprintf("conv-%d", w%3), []byte("payloadpayload")); err != nil {
					t.Errorf("Put: %v", err)
					return
				}
				for r := 0; r < 3; r++ {
					if _, err := s.Get(ctx, k); err != nil {
						t.Errorf("Get: %v", err)
						return
					}
				}
			}
		}(w)
	}
	wg.Wait()
	if got := s.Stats().Puts; got != 200 {
		t.Fatalf("Puts = %d, want 200", got)
	}
}

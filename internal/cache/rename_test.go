package cache

import (
	"context"
	"errors"
	"io"
	"log"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"fuse-client/internal/coordinator"
)

// renameTestManager builds a manager over a real NVMe directory, because the
// point of the rename path is which syscall moves the bytes — a mock tier
// cannot tell os.Rename apart from read-write-delete.
func renameTestManager(t *testing.T) (*DefaultCacheManager, string) {
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
			NVMePath: dir, MaxNVMeSize: 1 << 30, ChunkSize: 1024,
			CloudRetryCount: 1, CloudRetryBaseWait: time.Millisecond,
			CloudTimeout: time.Second,
		},
		nvmeStorage:  nvme,
		peerStorage:  newMockStorage(),
		cloudStorage: newMockStorage(),
		entries:      newEntryTable(),
		logger:       log.New(io.Discard, "", 0),
		metrics:      NewCacheMetrics(),
		rangeChunks:  make(map[string]*chunkFileCache),
		hybridHints:  make(map[string]hybridReadHint),
		tierPerf:     newTierPerfTracker(),
		shutdownCtx:  shutdownCtx,
	}, dir
}

// seedRenameFile writes one NVMe object plus its checksum sidecar and records
// the matching NVMe-tier entry.
func seedRenameFile(t *testing.T, cm *DefaultCacheManager, path string, size int) []byte {
	t.Helper()
	data := make([]byte, size)
	for i := range data {
		data[i] = byte(i % 253)
	}
	ctx := context.Background()
	if err := cm.nvmeStorage.Write(ctx, path, data); err != nil {
		t.Fatalf("seed %s: %v", path, err)
	}
	if err := cm.nvmeStorage.Write(ctx, path+".sha256", []byte("deadbeef")); err != nil {
		t.Fatalf("seed sidecar %s: %v", path, err)
	}
	cm.mu.Lock()
	cm.entries.put(&CacheEntry{
		FilePath: path, StoragePath: path, Size: int64(size),
		Tier: TierNVMe, LastAccessed: time.Now(),
	})
	cm.accountNVMeLocked(path, int64(size))
	cm.mu.Unlock()
	return data
}

// seedRenameChunked seeds a chunked parent with the chunk indices in present.
func seedRenameChunked(t *testing.T, cm *DefaultCacheManager, parent string, numChunks int64, chunkSize int, present []int64) {
	t.Helper()
	cm.mu.Lock()
	cm.entries.put(&CacheEntry{
		FilePath: parent, StoragePath: parent, Size: numChunks * int64(chunkSize),
		IsChunked: true, NumChunks: numChunks, Tier: TierNVMe, LastAccessed: time.Now(),
	})
	cm.mu.Unlock()
	for _, idx := range present {
		seedRenameFile(t, cm, chunkPathFor(parent, idx), chunkSize)
	}
}

func nvmeExists(t *testing.T, dir, key string) bool {
	t.Helper()
	_, err := os.Stat(filepath.Join(dir, key))
	return err == nil
}

// TestRename_ChunkedMovesEveryKeyWithoutRewritingBytes is the whole point of
// the change: parent and chunk entries are re-keyed, the bytes stay on the
// same inodes, and nothing is left filed under the old parent.
func TestRename_ChunkedMovesEveryKeyWithoutRewritingBytes(t *testing.T) {
	cm, dir := renameTestManager(t)
	const chunkSize = 512
	seedRenameChunked(t, cm, "/old.bin", 3, chunkSize, []int64{0, 1, 2})

	// Identity of the bytes before the move; os.Rename keeps the inode, a
	// read-write-delete does not.
	before := make([]os.FileInfo, 3)
	for i := int64(0); i < 3; i++ {
		info, err := os.Stat(filepath.Join(dir, chunkPathFor("/old.bin", i)))
		if err != nil {
			t.Fatalf("stat chunk %d: %v", i, err)
		}
		before[i] = info
	}

	if err := cm.Rename(context.Background(), "/old.bin", "/new.bin"); err != nil {
		t.Fatalf("Rename: %v", err)
	}
	cm.bgWg.Wait()

	cm.mu.RLock()
	parent, parentOK := cm.entries.get("/new.bin")
	_, oldParentOK := cm.entries.get("/old.bin")
	oldChunks := cm.entries.chunkKeys("/old.bin")
	newChunks := cm.entries.chunkKeys("/new.bin")
	cm.mu.RUnlock()

	if !parentOK {
		t.Fatal("destination parent entry missing")
	}
	if oldParentOK {
		t.Fatal("source parent entry still present")
	}
	if !parent.IsChunked || parent.NumChunks != 3 {
		t.Fatalf("chunk metadata lost: IsChunked=%t NumChunks=%d", parent.IsChunked, parent.NumChunks)
	}
	if parent.StoragePath != "/new.bin" {
		t.Fatalf("StoragePath = %q, want /new.bin", parent.StoragePath)
	}
	if len(oldChunks) != 0 {
		t.Fatalf("chunk entries orphaned under old parent: %v", oldChunks)
	}
	if len(newChunks) != 3 {
		t.Fatalf("new parent has %d chunk entries, want 3", len(newChunks))
	}

	for i := int64(0); i < 3; i++ {
		oldKey, newKey := chunkPathFor("/old.bin", i), chunkPathFor("/new.bin", i)
		if nvmeExists(t, dir, oldKey) {
			t.Fatalf("%s still on disk", oldKey)
		}
		after, err := os.Stat(filepath.Join(dir, newKey))
		if err != nil {
			t.Fatalf("stat %s: %v", newKey, err)
		}
		if !os.SameFile(before[i], after) {
			t.Fatalf("%s was rewritten rather than renamed", newKey)
		}
		if !nvmeExists(t, dir, newKey+".sha256") {
			t.Fatalf("checksum sidecar for %s not moved", newKey)
		}
		if nvmeExists(t, dir, oldKey+".sha256") {
			t.Fatalf("checksum sidecar for %s left behind", oldKey)
		}
	}
}

// TestRename_PartiallyFetchedChunksMoveToo: chunk keys come off the index, not
// from NumChunks, so a file whose chunks are only partly resident cannot
// strand entries under a parent that no longer exists.
func TestRename_PartiallyFetchedChunksMoveToo(t *testing.T) {
	cm, dir := renameTestManager(t)
	// Parent claims 4 chunks; only 0 and 2 have been fetched.
	seedRenameChunked(t, cm, "/partial.bin", 4, 256, []int64{0, 2})

	if err := cm.Rename(context.Background(), "/partial.bin", "/moved.bin"); err != nil {
		t.Fatalf("Rename: %v", err)
	}
	cm.bgWg.Wait()

	cm.mu.RLock()
	oldChunks := cm.entries.chunkKeys("/partial.bin")
	newChunks := cm.entries.chunkKeys("/moved.bin")
	cm.mu.RUnlock()

	if len(oldChunks) != 0 {
		t.Fatalf("stranded chunk entries: %v", oldChunks)
	}
	if len(newChunks) != 2 {
		t.Fatalf("moved %d chunk entries, want 2", len(newChunks))
	}
	for _, idx := range []int64{0, 2} {
		if !nvmeExists(t, dir, chunkPathFor("/moved.bin", idx)) {
			t.Fatalf("chunk %d bytes not at destination", idx)
		}
	}
}

// TestRename_ReplacesDestination: POSIX rename overwrites. The old occupant's
// bytes and its share of the NVMe budget both have to go.
func TestRename_ReplacesDestination(t *testing.T) {
	cm, dir := renameTestManager(t)
	src := seedRenameFile(t, cm, "/src.bin", 50)
	seedRenameFile(t, cm, "/dst.bin", 100)

	cm.mu.RLock()
	usedBefore := cm.nvmeUsed
	cm.mu.RUnlock()
	if usedBefore != 150 {
		t.Fatalf("seeded nvmeUsed = %d, want 150", usedBefore)
	}

	if err := cm.Rename(context.Background(), "/src.bin", "/dst.bin"); err != nil {
		t.Fatalf("Rename: %v", err)
	}
	cm.bgWg.Wait()

	cm.mu.RLock()
	dst, ok := cm.entries.get("/dst.bin")
	_, srcOK := cm.entries.get("/src.bin")
	used := cm.nvmeUsed
	cm.mu.RUnlock()

	if !ok {
		t.Fatal("destination entry missing after replace")
	}
	if srcOK {
		t.Fatal("source entry survived")
	}
	if dst.Size != 50 {
		t.Fatalf("destination size = %d, want the source's 50", dst.Size)
	}
	if used != 50 {
		t.Fatalf("nvmeUsed = %d, want 50 (replaced file's 100 bytes released)", used)
	}
	if nvmeExists(t, dir, "/src.bin") {
		t.Fatal("source bytes still on disk")
	}
	got, err := cm.nvmeStorage.Read(context.Background(), "/dst.bin")
	if err != nil {
		t.Fatalf("read destination: %v", err)
	}
	if string(got) != string(src) {
		t.Fatal("destination holds the replaced file's bytes, not the source's")
	}
}

func TestRename_MissingSourceIsErrNotFound(t *testing.T) {
	cm, _ := renameTestManager(t)
	err := cm.Rename(context.Background(), "/nope.bin", "/somewhere.bin")
	if err == nil {
		t.Fatal("renaming a path the cache does not know returned nil")
	}
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("err = %v, want ErrNotFound", err)
	}
}

// TestRename_SamePathIsNoop guards the early return: the local move would
// otherwise rename a file onto itself and the cloud step would copy-then-delete
// its own destination.
func TestRename_SamePathIsNoop(t *testing.T) {
	cm, dir := renameTestManager(t)
	seedRenameFile(t, cm, "/same.bin", 32)

	if err := cm.Rename(context.Background(), "/same.bin", "/same.bin"); err != nil {
		t.Fatalf("Rename: %v", err)
	}
	cm.bgWg.Wait()

	cm.mu.RLock()
	_, ok := cm.entries.get("/same.bin")
	cm.mu.RUnlock()
	if !ok {
		t.Fatal("entry deleted by a self-rename")
	}
	if !nvmeExists(t, dir, "/same.bin") {
		t.Fatal("bytes deleted by a self-rename")
	}
}

// TestRename_WithoutMoveCapabilityStillMoves: a TierStorage that predates
// Move (or a test double) falls back to copy-then-delete. Slow, still correct.
func TestRename_WithoutMoveCapabilityStillMoves(t *testing.T) {
	cm, _ := renameTestManager(t)
	mock := newMockStorage()
	cm.nvmeStorage = mock
	if _, isMover := cm.nvmeStorage.(nvmeMover); isMover {
		t.Fatal("mockStorage must not implement nvmeMover for this test to mean anything")
	}
	data := seedRenameFile(t, cm, "/fallback.bin", 40)

	if err := cm.Rename(context.Background(), "/fallback.bin", "/landed.bin"); err != nil {
		t.Fatalf("Rename: %v", err)
	}
	cm.bgWg.Wait()

	got, err := mock.Read(context.Background(), "/landed.bin")
	if err != nil {
		t.Fatalf("destination not written: %v", err)
	}
	if string(got) != string(data) {
		t.Fatal("destination bytes differ from source")
	}
	if mock.Exists(context.Background(), "/fallback.bin") {
		t.Fatal("source bytes not removed")
	}
}

// copyStorage is a cloud tier that supports server-side copy, and can be told
// to fail it.
type copyStorage struct {
	*mockStorage
	mu      sync.Mutex
	copies  [][2]string
	failAll bool
}

func newCopyStorage() *copyStorage {
	return &copyStorage{mockStorage: newMockStorage()}
}

func (c *copyStorage) CopyObject(ctx context.Context, srcPath, dstPath string) error {
	c.mu.Lock()
	c.copies = append(c.copies, [2]string{srcPath, dstPath})
	fail := c.failAll
	c.mu.Unlock()
	if fail {
		return context.DeadlineExceeded
	}
	data, err := c.mockStorage.Read(ctx, srcPath)
	if err != nil {
		return err
	}
	return c.mockStorage.Write(ctx, dstPath, data)
}

func (c *copyStorage) copyCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.copies)
}

// TestRename_CloudCopiesChunksServerSide: the durable copy is re-pointed with
// per-chunk server-side copies, the sources go away only after every
// destination landed, and the entries end up marked persisted.
func TestRename_CloudCopiesChunksServerSide(t *testing.T) {
	cm, _ := renameTestManager(t)
	cloud := newCopyStorage()
	cm.cloudStorage = cloud
	const chunkSize = 128
	seedRenameChunked(t, cm, "/ckpt.bin", 3, chunkSize, []int64{0, 1, 2})

	ctx := context.Background()
	for i := int64(0); i < 3; i++ {
		cloud.Write(ctx, chunkPathFor("/ckpt.bin", i), make([]byte, chunkSize))
	}

	if err := cm.Rename(ctx, "/ckpt.bin", "/ckpt-final.bin"); err != nil {
		t.Fatalf("Rename: %v", err)
	}
	cm.bgWg.Wait()

	if got := cloud.copyCount(); got != 3 {
		t.Fatalf("%d server-side copies, want one per chunk (3)", got)
	}
	for i := int64(0); i < 3; i++ {
		if !cloud.Exists(ctx, chunkPathFor("/ckpt-final.bin", i)) {
			t.Fatalf("cloud chunk %d missing at destination", i)
		}
		if cloud.Exists(ctx, chunkPathFor("/ckpt.bin", i)) {
			t.Fatalf("cloud chunk %d left at source", i)
		}
	}

	cm.mu.RLock()
	parent, _ := cm.entries.get("/ckpt-final.bin")
	cm.mu.RUnlock()
	if parent == nil || !parent.PersistedToCloud {
		t.Fatal("renamed parent not marked persisted after every chunk copied")
	}
}

// TestRename_CloudCopyFailureKeepsSources: an unconfirmed upload plus a
// deleted original is data loss, so the fallback re-persists from the local
// copy and leaves the source objects alone.
func TestRename_CloudCopyFailureKeepsSources(t *testing.T) {
	cm, _ := renameTestManager(t)
	cloud := newCopyStorage()
	cloud.failAll = true
	cm.cloudStorage = cloud
	seedRenameFile(t, cm, "/flat.bin", 64)

	ctx := context.Background()
	cloud.Write(ctx, "/flat.bin", make([]byte, 64))

	if err := cm.Rename(ctx, "/flat.bin", "/flat-new.bin"); err != nil {
		t.Fatalf("Rename: %v", err)
	}
	cm.bgWg.Wait()

	if !cloud.Exists(ctx, "/flat.bin") {
		t.Fatal("source cloud object deleted despite the copy failing")
	}
	if !cloud.Exists(ctx, "/flat-new.bin") {
		t.Fatal("destination not re-persisted from the local copy")
	}
}

// TestRename_DropsSourceCoordinatorRows: etcd file rows carry no TTL, so a
// source row left behind is permanent — the merged metadata view keeps serving
// the old name as a file that still resolves and still reads. That ghost is
// what the rename path could not fix before the coordinator had a delete.
func TestRename_DropsSourceCoordinatorRows(t *testing.T) {
	cm, _ := renameTestManager(t)
	cm.tombstones = newDeleteTombstones()
	coord := newDeleteCoordinator()
	cm.config.Coordinator = coord
	cm.config.LocalPeerID = "peer-a"
	cloud := newCopyStorage()
	cm.cloudStorage = cloud
	seedRenameFile(t, cm, "/before.bin", 64)

	ctx := context.Background()
	cloud.Write(ctx, "/before.bin", make([]byte, 64))
	coord.seed(&coordinator.FileLocation{
		FilePath: "/before.bin", PeerID: "peer-a", StorageTier: "nvme", FileSize: 64,
	})

	if err := cm.Rename(ctx, "/before.bin", "/after.bin"); err != nil {
		t.Fatalf("Rename: %v", err)
	}
	cm.bgWg.Wait()

	calls := coord.deleteCalls()
	if len(calls) != 1 {
		t.Fatalf("coordinator retractions = %v, want exactly one for the source", calls)
	}
	if calls[0].path != "/before.bin" || !calls[0].wholePath {
		t.Fatalf("retraction = %+v, want whole-path on /before.bin", calls[0])
	}

	// The destination must still be published — retracting the source is not
	// allowed to take the renamed file's own metadata with it.
	published := false
	for _, path := range coord.updateCalls() {
		if path == "/after.bin" {
			published = true
		}
		if path == "/before.bin" {
			t.Fatal("source republished after the rename")
		}
	}
	if !published {
		t.Fatal("destination never published: the renamed file is invisible cluster-wide")
	}
}

// TestRename_CloudCopyFailureKeepsSourceRows: the fallback deliberately leaves
// the source objects in place, so their rows are still true and retracting
// them would point readers at nothing.
func TestRename_CloudCopyFailureKeepsSourceRows(t *testing.T) {
	cm, _ := renameTestManager(t)
	cm.tombstones = newDeleteTombstones()
	coord := newDeleteCoordinator()
	cm.config.Coordinator = coord
	cm.config.LocalPeerID = "peer-a"
	cloud := newCopyStorage()
	cloud.failAll = true
	cm.cloudStorage = cloud
	seedRenameFile(t, cm, "/keep.bin", 64)

	ctx := context.Background()
	cloud.Write(ctx, "/keep.bin", make([]byte, 64))

	if err := cm.Rename(ctx, "/keep.bin", "/keep-new.bin"); err != nil {
		t.Fatalf("Rename: %v", err)
	}
	cm.bgWg.Wait()

	if calls := coord.deleteCalls(); len(calls) != 0 {
		t.Fatalf("source rows retracted despite the copy failing: %v", calls)
	}
}

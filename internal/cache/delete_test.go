package cache

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"fuse-client/internal/coordinator"
	pb "fuse-client/internal/pb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// deleteCall records one retraction so tests can assert the shape of it —
// per-peer vs whole-path is the difference between "this node dropped its
// copy" and "the file is gone", and getting it wrong is invisible at runtime.
type deleteCall struct {
	path      string
	peerID    string
	wholePath bool
}

// deleteCoordinator is a Coordinator that remembers location rows and every
// retraction made against them.
type deleteCoordinator struct {
	herdCoordinator
	mu        sync.Mutex
	locations map[string][]*coordinator.FileLocation
	updates   []string
	deletes   []deleteCall
	failWith  error
}

func newDeleteCoordinator() *deleteCoordinator {
	return &deleteCoordinator{locations: make(map[string][]*coordinator.FileLocation)}
}

func (c *deleteCoordinator) seed(loc *coordinator.FileLocation) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.locations[loc.FilePath] = append(c.locations[loc.FilePath], loc)
}

func (c *deleteCoordinator) GetFileLocation(_ context.Context, filePath string) ([]*coordinator.FileLocation, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]*coordinator.FileLocation(nil), c.locations[filePath]...), nil
}

func (c *deleteCoordinator) ListFileLocations(_ context.Context, prefix string) ([]*coordinator.FileLocation, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	var out []*coordinator.FileLocation
	for path, locs := range c.locations {
		if prefix != "" && !strings.HasPrefix(path, prefix) {
			continue
		}
		if len(locs) > 0 {
			out = append(out, locs[0])
		}
	}
	return out, nil
}

func (c *deleteCoordinator) UpdateFileLocation(_ context.Context, loc *coordinator.FileLocation) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.updates = append(c.updates, loc.FilePath)
	c.locations[loc.FilePath] = append(c.locations[loc.FilePath], loc)
	return nil
}

func (c *deleteCoordinator) DeleteFileLocation(_ context.Context, filePath, peerID string, wholePath bool) error {
	c.mu.Lock()
	c.deletes = append(c.deletes, deleteCall{path: filePath, peerID: peerID, wholePath: wholePath})
	fail := c.failWith
	if fail == nil {
		delete(c.locations, filePath)
	}
	c.mu.Unlock()
	return fail
}

func (c *deleteCoordinator) deleteCalls() []deleteCall {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]deleteCall(nil), c.deletes...)
}

func (c *deleteCoordinator) updateCalls() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]string(nil), c.updates...)
}

// deleteFailStorage is a tier whose Delete always fails with a non-404 error,
// standing in for a peer that is down or a cloud endpoint that is refusing.
type deleteFailStorage struct {
	*mockStorage
	err error
}

func newDeleteFailStorage() *deleteFailStorage {
	return &deleteFailStorage{mockStorage: newMockStorage(), err: errors.New("tier unavailable")}
}

func (f *deleteFailStorage) Delete(context.Context, string) error { return f.err }

// seedDeletable puts one NVMe-tier object in place with matching accounting.
func seedDeletable(t *testing.T, cm *DefaultCacheManager, path string, data []byte, persisted bool) {
	t.Helper()
	ctx := context.Background()
	if err := cm.nvmeStorage.Write(ctx, path, data); err != nil {
		t.Fatalf("seed nvme %s: %v", path, err)
	}
	cm.mu.Lock()
	cm.entries.put(&CacheEntry{
		FilePath:         path,
		StoragePath:      path,
		Size:             int64(len(data)),
		Tier:             TierNVMe,
		PersistedToCloud: persisted,
		LastAccessed:     time.Now(),
	})
	cm.accountNVMeLocked(path, int64(len(data)))
	cm.mu.Unlock()
}

// TestDelete_RemovesCloudObjects is the regression test for the primary bug:
// a locally-written, cloud-persisted file keeps Tier == TierNVMe forever
// (markPersistedToCloud only flips PersistedToCloud), so the old delete
// resolved a single tier off entry.Tier and never touched the durable copy.
func TestDelete_RemovesCloudObjects(t *testing.T) {
	cm := newTestCacheManager()
	cloud := newRecordingDeleteStorage()
	cm.cloudStorage = cloud
	ctx := context.Background()

	const path = "/durable.bin"
	if err := cloud.Write(ctx, path, []byte("durable")); err != nil {
		t.Fatalf("seed cloud: %v", err)
	}
	seedDeletable(t, cm, path, []byte("durable"), true)

	if err := cm.Delete(ctx, path); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	if !cloud.DeletedPath(path) {
		t.Fatalf("cloud deletes = %v, want to include %q", cloud.DeletedPaths(), path)
	}
	if cloud.Exists(ctx, path) {
		t.Fatal("cloud object survived delete: rm left the durable copy behind")
	}
	if got := cm.DeleteSnapshot().CloudFailuresTotal; got != 0 {
		t.Fatalf("cloud failures = %d, want 0", got)
	}
}

// TestDelete_RemovesPeerReplicas covers the second half of the leak: nothing
// used to ask peers to drop their copies, so every replica stayed on disk and
// kept serving the deleted name.
func TestDelete_RemovesPeerReplicas(t *testing.T) {
	cm := newTestCacheManager()
	peer := newRecordingDeleteStorage()
	cm.peerStorage = peer
	ctx := context.Background()

	const path = "/replicated.bin"
	seedDeletable(t, cm, path, []byte("payload"), false)

	if err := cm.Delete(ctx, path); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	if !peer.DeletedPath(path) {
		t.Fatalf("peer deletes = %v, want to include %q", peer.DeletedPaths(), path)
	}
}

// TestDelete_ChunkedFansOutOncePerFile: the peer handler removes the parent
// and every chunk it holds, so fanning out per chunk would be N times the RPCs
// for strictly worse coverage (a peer can hold chunks this node never saw).
func TestDelete_ChunkedFansOutOncePerFile(t *testing.T) {
	cm := newTestCacheManager()
	peer := newRecordingDeleteStorage()
	cm.peerStorage = peer
	ctx := context.Background()

	const parent = "/big.bin"
	for i := int64(0); i < 4; i++ {
		seedDeletable(t, cm, chunkPathFor(parent, i), []byte("chunk"), true)
	}
	cm.mu.Lock()
	cm.entries.put(&CacheEntry{
		FilePath: parent, StoragePath: parent, Size: 20, Tier: TierNVMe,
		IsChunked: true, NumChunks: 4, LastAccessed: time.Now(),
	})
	cm.mu.Unlock()

	if err := cm.Delete(ctx, parent); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	got := peer.DeletedPaths()
	if len(got) != 1 || got[0] != parent {
		t.Fatalf("peer deletes = %v, want exactly one call for %q", got, parent)
	}
}

// TestDelete_NotifiesCoordinator: the rows must go with whole_path, because a
// node publishes one row per tier it holds the object in (nvme at write, cloud
// once persisted) and a per-peer retraction would leave the sibling row alive.
func TestDelete_NotifiesCoordinator(t *testing.T) {
	cm := newTestCacheManager()
	coord := newDeleteCoordinator()
	cm.config.Coordinator = coord
	cm.config.LocalPeerID = "peer-a"
	ctx := context.Background()

	const path = "/tracked.bin"
	coord.seed(&coordinator.FileLocation{FilePath: path, PeerID: "peer-a", StorageTier: "nvme", FileSize: 7})
	seedDeletable(t, cm, path, []byte("payload"), false)

	if err := cm.Delete(ctx, path); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	calls := coord.deleteCalls()
	if len(calls) != 1 {
		t.Fatalf("coordinator deletes = %v, want exactly 1", calls)
	}
	if calls[0].path != path || !calls[0].wholePath {
		t.Fatalf("retraction = %+v, want path=%q wholePath=true", calls[0], path)
	}
}

// TestDelete_BestEffortOnTierFailure: an unreachable peer, cloud, or
// coordinator must not turn rm into an error — coordination is advisory — but
// the partial failure has to be visible, since silent partial failure is the
// exact bug this change exists to fix.
func TestDelete_BestEffortOnTierFailure(t *testing.T) {
	cm := newTestCacheManager()
	cm.peerStorage = newDeleteFailStorage()
	cm.cloudStorage = newDeleteFailStorage()
	coord := newDeleteCoordinator()
	coord.failWith = errors.New("coordinator unreachable")
	cm.config.Coordinator = coord
	cm.config.LocalPeerID = "peer-a"
	ctx := context.Background()

	const path = "/unlucky.bin"
	seedDeletable(t, cm, path, []byte("payload"), true)

	if err := cm.Delete(ctx, path); err != nil {
		t.Fatalf("Delete returned %v, want success despite unreachable tiers", err)
	}

	snap := cm.DeleteSnapshot()
	if snap.PeerFailuresTotal != 1 {
		t.Errorf("peer failures = %d, want 1", snap.PeerFailuresTotal)
	}
	if snap.CloudFailuresTotal != 1 {
		t.Errorf("cloud failures = %d, want 1", snap.CloudFailuresTotal)
	}
	if snap.CoordinatorFailuresTotal != 1 {
		t.Errorf("coordinator failures = %d, want 1", snap.CoordinatorFailuresTotal)
	}

	// The local half still has to have happened: the caller was told the file
	// is gone.
	cm.mu.RLock()
	_, exists := cm.entries.get(path)
	cm.mu.RUnlock()
	if exists {
		t.Fatal("entry survived a best-effort delete")
	}
}

// TestDelete_MissingPathIsNotFound keeps rm of a nonexistent file reporting
// ENOENT rather than success.
func TestDelete_MissingPathIsNotFound(t *testing.T) {
	cm := newTestCacheManager()
	err := cm.Delete(context.Background(), "/never-existed.bin")
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("Delete of missing path = %v, want ErrNotFound", err)
	}
}

// TestDelete_NonLocalPathStillFansOut: the FUSE layer resolves names through
// the coordinator view, so a path is visible on every node while the bytes
// live on only some. Keying existence off the local table would make rm
// succeed on the holder and return ENOENT everywhere else — while the file
// went on resolving.
func TestDelete_NonLocalPathStillFansOut(t *testing.T) {
	cm := newTestCacheManager()
	peer := newRecordingDeleteStorage()
	cloud := newRecordingDeleteStorage()
	cm.peerStorage = peer
	cm.cloudStorage = cloud
	coord := newDeleteCoordinator()
	cm.config.Coordinator = coord
	cm.config.LocalPeerID = "peer-a"
	ctx := context.Background()

	const path = "/elsewhere.bin"
	coord.seed(&coordinator.FileLocation{FilePath: path, PeerID: "peer-b", StorageTier: "nvme", FileSize: 9})

	if err := cm.Delete(ctx, path); err != nil {
		t.Fatalf("Delete of a remotely-held path: %v", err)
	}

	if !peer.DeletedPath(path) {
		t.Fatalf("peer deletes = %v, want to include %q", peer.DeletedPaths(), path)
	}
	if !cloud.DeletedPath(path) {
		t.Fatalf("cloud deletes = %v, want to include %q", cloud.DeletedPaths(), path)
	}
	if calls := coord.deleteCalls(); len(calls) != 1 || !calls[0].wholePath {
		t.Fatalf("coordinator deletes = %v, want one whole-path retraction", calls)
	}
}

// TestDelete_RehydratedEntryStillDeletesChunks: after a restart,
// rehydrateNVMeAccounting adopts local files with neither IsChunked nor
// NumChunks set, so a chunked file looks flat and cloudObjectKeys returns the
// bare path — a key that does not exist. Every chunk blob would be left behind
// while the delete reported success.
func TestDelete_RehydratedEntryStillDeletesChunks(t *testing.T) {
	cm := newTestCacheManager()
	cm.config.ChunkSize = 1000
	cloud := newRecordingDeleteStorage()
	cm.cloudStorage = cloud
	coord := newDeleteCoordinator()
	cm.config.Coordinator = coord
	cm.config.LocalPeerID = "peer-a"
	ctx := context.Background()

	const parent = "/rehydrated.bin"
	for i := int64(0); i < 3; i++ {
		if err := cloud.Write(ctx, chunkPathFor(parent, i), make([]byte, 1000)); err != nil {
			t.Fatalf("seed cloud chunk %d: %v", i, err)
		}
	}
	// The coordinator knows the real shape; the local entry does not.
	coord.seed(&coordinator.FileLocation{
		FilePath: parent, PeerID: "peer-a", StorageTier: "cloud",
		FileSize: 2500, IsChunked: true, ChunkSize: 1000,
	})
	seedDeletable(t, cm, parent, []byte("adopted"), true)

	if err := cm.Delete(ctx, parent); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	for i := int64(0); i < 3; i++ {
		key := chunkPathFor(parent, i)
		if cloud.Exists(ctx, key) {
			t.Fatalf("cloud chunk %s leaked: rehydrated metadata hid the chunked shape", key)
		}
	}
}

// TestDelete_NonNVMeTierEntryRemovedFromTable: an entry parked at TierPeer or
// TierCloud by the parallel-write fallback has no local bytes, so the NVMe
// footprint pass collects nothing — but its table row still keeps the name
// alive locally and must go.
func TestDelete_NonNVMeTierEntryRemovedFromTable(t *testing.T) {
	cm := newTestCacheManager()
	ctx := context.Background()

	const path = "/peer-parked.bin"
	cm.mu.Lock()
	cm.entries.put(&CacheEntry{
		FilePath: path, StoragePath: path, Size: 12,
		Tier: TierPeer, LastAccessed: time.Now(),
	})
	cm.mu.Unlock()

	if err := cm.Delete(ctx, path); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	cm.mu.RLock()
	_, exists := cm.entries.get(path)
	cm.mu.RUnlock()
	if exists {
		t.Fatal("non-NVMe entry survived delete and keeps resolving locally")
	}
}

// TestDelete_PrunesMetadataView: resolveChunkedEntry rebuilds a live
// CacheEntry out of the cached coordinator view and puts it back in the table,
// so a stale view is a resurrection path inside the read path itself.
func TestDelete_PrunesMetadataView(t *testing.T) {
	cm := newTestCacheManager()
	ctx := context.Background()

	const path = "/viewed.bin"
	seedDeletable(t, cm, path, []byte("payload"), false)
	cm.mu.Lock()
	cm.metadataView = []*CacheEntry{
		{FilePath: path, Size: 7, Tier: TierCloud},
		{FilePath: chunkPathFor(path, 0), Size: 7, Tier: TierCloud},
		{FilePath: "/kept.bin", Size: 3, Tier: TierCloud},
	}
	cm.metadataAt = time.Now()
	cm.mu.Unlock()

	if err := cm.Delete(ctx, path); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	cm.mu.RLock()
	view := cm.metadataView
	cm.mu.RUnlock()
	if len(view) != 1 || view[0].FilePath != "/kept.bin" {
		paths := make([]string, 0, len(view))
		for _, e := range view {
			paths = append(paths, e.FilePath)
		}
		t.Fatalf("metadata view = %v, want only /kept.bin", paths)
	}
}

// TestDelete_TombstoneSuppressesLatePublish: a background cloud persist or
// chunk completion already in flight when the delete lands would otherwise
// re-publish the row the delete just retracted, and the ghost is back.
func TestDelete_TombstoneSuppressesLatePublish(t *testing.T) {
	cm := newTestCacheManager()
	coord := newDeleteCoordinator()
	cm.config.Coordinator = coord
	cm.config.LocalPeerID = "peer-a"
	ctx := context.Background()

	const path = "/racing.bin"
	seedDeletable(t, cm, path, []byte("payload"), false)
	if err := cm.Delete(ctx, path); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	// The publish a straggling goroutine would have made.
	cm.publishFileLocation(ctx, &CacheEntry{FilePath: path, Size: 7}, TierNVMe)
	// And the one its per-chunk cloud persist would have made.
	cm.publishFileLocation(ctx, &CacheEntry{FilePath: chunkPathFor(path, 0), Size: 7}, TierCloud)

	if got := coord.updateCalls(); len(got) != 0 {
		t.Fatalf("post-delete publishes = %v, want none", got)
	}
	if got := cm.DeleteSnapshot().ResurrectionSuppressedTotal; got != 2 {
		t.Fatalf("suppressed publishes = %d, want 2 (parent + chunk)", got)
	}
}

// TestDelete_RewriteClearsTombstone: re-creating a deleted file is legitimate,
// and its metadata has to publish again or the new file is invisible.
func TestDelete_RewriteClearsTombstone(t *testing.T) {
	cm := newTestCacheManager()
	coord := newDeleteCoordinator()
	cm.config.Coordinator = coord
	cm.config.LocalPeerID = "peer-a"
	ctx := context.Background()

	const path = "/recreated.bin"
	seedDeletable(t, cm, path, []byte("v1"), false)
	if err := cm.Delete(ctx, path); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	entry := &CacheEntry{FilePath: path, Size: 2, LastAccessed: time.Now(), Data: []byte("v2")}
	if err := cm.Put(ctx, entry); err != nil {
		t.Fatalf("Put after delete: %v", err)
	}
	cm.bgWg.Wait()

	cm.publishFileLocation(ctx, entry, TierNVMe)
	if got := coord.updateCalls(); len(got) == 0 {
		t.Fatal("re-created file never published: the tombstone outlived the rewrite")
	}
}

// TestPeerGRPC_DeleteFile_DoesNotFanOut is the storm guard. The peer handler
// must delete locally only: if it re-entered the cluster-wide path, A→B would
// become B→{A,C} without bound, and since delete now removes cloud objects
// that storm would be unrecoverable.
func TestPeerGRPC_DeleteFile_DoesNotFanOut(t *testing.T) {
	addr, cm, stop := startHerdPeer(t, 4, map[string][]byte{"/served.bin": []byte("payload")})
	defer stop()

	peer := newRecordingDeleteStorage()
	cloud := newRecordingDeleteStorage()
	cm.peerStorage = peer
	cm.cloudStorage = cloud
	ctx := context.Background()
	if err := cloud.Write(ctx, "/served.bin", []byte("payload")); err != nil {
		t.Fatalf("seed cloud: %v", err)
	}

	conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	if _, err := pb.NewPeerServiceClient(conn).DeleteFile(ctx, &pb.DeleteFileRequest{Path: "/served.bin"}); err != nil {
		t.Fatalf("DeleteFile: %v", err)
	}

	if got := peer.DeletedPaths(); len(got) != 0 {
		t.Fatalf("peer-served delete fanned out to %v: this is the broadcast storm", got)
	}
	if !cloud.Exists(ctx, "/served.bin") {
		t.Fatal("peer-served delete removed the durable copy: one node's eviction is not an rm")
	}
	if cm.nvmeStorage.Exists(ctx, "/served.bin") {
		t.Fatal("peer-served delete left the local bytes behind")
	}
}

// TestDeleteLocal_RetractsOnlyThisPeersRows: this node stopped holding the
// object, which is stale routing worth retracting — but the file may well live
// on elsewhere, so the other holders' rows must survive.
func TestDeleteLocal_RetractsOnlyThisPeersRows(t *testing.T) {
	cm := newTestCacheManager()
	coord := newDeleteCoordinator()
	cm.config.Coordinator = coord
	cm.config.LocalPeerID = "peer-a"
	ctx := context.Background()

	const path = "/shared.bin"
	seedDeletable(t, cm, path, []byte("payload"), false)

	if err := cm.DeleteLocal(ctx, path); err != nil {
		t.Fatalf("DeleteLocal: %v", err)
	}

	calls := coord.deleteCalls()
	if len(calls) != 1 {
		t.Fatalf("coordinator deletes = %v, want exactly 1", calls)
	}
	if calls[0].wholePath {
		t.Fatalf("retraction = %+v, want per-peer (wholePath=false)", calls[0])
	}
	if calls[0].peerID != "peer-a" {
		t.Fatalf("retraction peer = %q, want peer-a", calls[0].peerID)
	}
}

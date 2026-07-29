package cache

import (
	"bytes"
	"context"
	"sync"
	"testing"
	"time"

	"fuse-client/internal/coordinator"
)

// leaseCoordinator extends herdCoordinator with FetchLeaser support and
// records lease/location traffic for assertions.
type leaseCoordinator struct {
	herdCoordinator
	svc *coordinator.CoordinatorService

	mu        sync.Mutex
	acquires  []string
	releases  []string
	published []*coordinator.FileLocation
	locations map[string][]*coordinator.FileLocation
	denyAll   bool
	holder    string
	failLease bool
}

func newLeaseCoordinator() *leaseCoordinator {
	return &leaseCoordinator{
		svc:       coordinator.NewCoordinatorService(),
		locations: make(map[string][]*coordinator.FileLocation),
	}
}

func (c *leaseCoordinator) AcquireFetchLease(ctx context.Context, key, peerID string, ttl time.Duration) (string, bool, error) {
	c.mu.Lock()
	c.acquires = append(c.acquires, key)
	denyAll, holder, fail := c.denyAll, c.holder, c.failLease
	c.mu.Unlock()
	if fail {
		return "", false, context.DeadlineExceeded
	}
	if denyAll {
		return holder, false, nil
	}
	return c.svc.AcquireFetchLease(ctx, key, peerID, ttl)
}

func (c *leaseCoordinator) ReleaseFetchLease(ctx context.Context, key, peerID string) error {
	c.mu.Lock()
	c.releases = append(c.releases, key)
	c.mu.Unlock()
	return c.svc.ReleaseFetchLease(ctx, key, peerID)
}

func (c *leaseCoordinator) UpdateFileLocation(_ context.Context, loc *coordinator.FileLocation) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	lc := *loc
	c.published = append(c.published, &lc)
	c.locations[loc.FilePath] = append(c.locations[loc.FilePath], &lc)
	return nil
}

func (c *leaseCoordinator) GetFileLocation(_ context.Context, path string) ([]*coordinator.FileLocation, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	locs := c.locations[path]
	out := make([]*coordinator.FileLocation, len(locs))
	copy(out, locs)
	return out, nil
}

func (c *leaseCoordinator) publishedCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.published)
}

func newLeasedTestManager(coord coordinator.Coordinator) *DefaultCacheManager {
	cm := newTestCacheManager()
	cm.config.Coordinator = coord
	cm.config.LocalPeerID = "local-node"
	cm.config.FetchLeaseEnabled = true
	cm.chunkAds = newChunkAdvertiser()
	return cm
}

// TestGetFromCloudLeased_LeaderPullsAndReleases: the lease winner reads cloud
// and releases the lease afterward.
func TestGetFromCloudLeased_LeaderPullsAndReleases(t *testing.T) {
	coord := newLeaseCoordinator()
	cm := newLeasedTestManager(coord)
	payload := []byte("cloud payload")
	cm.cloudStorage.Write(context.Background(), "/lease.bin", payload)

	entry, tier, err := cm.getFromCloudLeased(context.Background(), "/lease.bin")
	if err != nil {
		t.Fatalf("leader read: %v", err)
	}
	if tier != TierCloud || !bytes.Equal(entry.Data, payload) {
		t.Fatalf("tier=%v len=%d, want cloud/%d bytes", tier, len(entry.Data), len(payload))
	}
	if len(coord.acquires) != 1 || len(coord.releases) != 1 {
		t.Fatalf("acquires=%d releases=%d, want 1/1", len(coord.acquires), len(coord.releases))
	}
	snap := cm.HerdControlSnapshot()
	if snap.LeaseGrantedTotal != 1 || snap.LeaseDeniedTotal != 0 {
		t.Fatalf("granted/denied = %d/%d, want 1/0", snap.LeaseGrantedTotal, snap.LeaseDeniedTotal)
	}
}

// TestGetFromCloudLeased_FollowerReadsFromPeer: a denied requester waits, then
// gets the data from the peer tier (the leader has it by then) — origin is
// never touched by the follower.
func TestGetFromCloudLeased_FollowerReadsFromPeer(t *testing.T) {
	coord := newLeaseCoordinator()
	coord.denyAll = true
	coord.holder = "peer-leader"
	cm := newLeasedTestManager(coord)

	payload := []byte("swarm payload")
	// Leader's copy is reachable via the peer tier; cloud is empty, so a
	// follower that (incorrectly) fell through to cloud would fail.
	cm.peerStorage.Write(context.Background(), "/follow.bin", payload)

	entry, tier, err := cm.getFromCloudLeased(context.Background(), "/follow.bin")
	if err != nil {
		t.Fatalf("follower read: %v", err)
	}
	if tier != TierPeer || !bytes.Equal(entry.Data, payload) {
		t.Fatalf("tier=%v len=%d, want peer/%d bytes", tier, len(entry.Data), len(payload))
	}
	snap := cm.HerdControlSnapshot()
	if snap.LeaseDeniedTotal != 1 || snap.FollowerPeerHitsTotal != 1 || snap.FollowerCloudFallbackTotal != 0 {
		t.Fatalf("denied/peerHits/cloudFallback = %d/%d/%d, want 1/1/0",
			snap.LeaseDeniedTotal, snap.FollowerPeerHitsTotal, snap.FollowerCloudFallbackTotal)
	}
}

// TestGetFromCloudLeased_FollowerFallsBackToCloud: when the leader is slow or
// dead (peer tier misses), the advisory lease must not strand the read.
func TestGetFromCloudLeased_FollowerFallsBackToCloud(t *testing.T) {
	coord := newLeaseCoordinator()
	coord.denyAll = true
	coord.holder = "peer-leader"
	cm := newLeasedTestManager(coord)

	payload := []byte("fallback payload")
	cm.cloudStorage.Write(context.Background(), "/fb.bin", payload) // only cloud has it

	entry, tier, err := cm.getFromCloudLeased(context.Background(), "/fb.bin")
	if err != nil {
		t.Fatalf("fallback read: %v", err)
	}
	if tier != TierCloud || !bytes.Equal(entry.Data, payload) {
		t.Fatalf("tier=%v, want cloud", tier)
	}
	snap := cm.HerdControlSnapshot()
	if snap.FollowerCloudFallbackTotal != 1 {
		t.Fatalf("cloudFallback = %d, want 1", snap.FollowerCloudFallbackTotal)
	}
}

// TestGetFromCloudLeased_CoordinatorErrorDegrades: lease RPC failure = plain
// cloud read, never an error surfaced to the reader.
func TestGetFromCloudLeased_CoordinatorErrorDegrades(t *testing.T) {
	coord := newLeaseCoordinator()
	coord.failLease = true
	cm := newLeasedTestManager(coord)

	payload := []byte("degraded payload")
	cm.cloudStorage.Write(context.Background(), "/deg.bin", payload)

	entry, tier, err := cm.getFromCloudLeased(context.Background(), "/deg.bin")
	if err != nil || tier != TierCloud || !bytes.Equal(entry.Data, payload) {
		t.Fatalf("degraded read = (%v, %v), want clean cloud read", tier, err)
	}
	if snap := cm.HerdControlSnapshot(); snap.LeaseErrorsTotal != 1 {
		t.Fatalf("leaseErrors = %d, want 1", snap.LeaseErrorsTotal)
	}
}

// TestGetFromCloudLeased_DisabledBypassesLease: flag off = zero coordinator
// lease traffic.
func TestGetFromCloudLeased_DisabledBypassesLease(t *testing.T) {
	coord := newLeaseCoordinator()
	cm := newLeasedTestManager(coord)
	cm.config.FetchLeaseEnabled = false

	cm.cloudStorage.Write(context.Background(), "/off.bin", []byte("x"))
	if _, _, err := cm.getFromCloudLeased(context.Background(), "/off.bin"); err != nil {
		t.Fatalf("read: %v", err)
	}
	if len(coord.acquires) != 0 {
		t.Fatalf("acquires = %d, want 0 when disabled", len(coord.acquires))
	}
}

func TestChunkAdvertiser_CoalescesPerParent(t *testing.T) {
	a := newChunkAdvertiser()
	now := time.Now()

	if !a.shouldPublish("/f.bin", now) {
		t.Fatal("first publish should pass")
	}
	// Burst of chunk arrivals within the window: coalesced.
	for i := 0; i < 10; i++ {
		if a.shouldPublish("/f.bin", now.Add(time.Duration(i)*time.Millisecond)) {
			t.Fatal("publish within coalesce window should be suppressed")
		}
	}
	// A different parent is independent.
	if !a.shouldPublish("/g.bin", now) {
		t.Fatal("different parent should publish")
	}
	// After the window, the same parent may publish again.
	if !a.shouldPublish("/f.bin", now.Add(chunkAdvertiseCoalesceWindow+time.Millisecond)) {
		t.Fatal("publish after window should pass")
	}
}

// TestPromoteChunkAndAdvertise_MakesChunkServable: a remotely-fetched chunk
// becomes locally servable (GetLocal) and the node advertises the parent.
func TestPromoteChunkAndAdvertise_MakesChunkServable(t *testing.T) {
	coord := newLeaseCoordinator()
	cm := newLeasedTestManager(coord)
	cm.config.FastChunkAdvertise = true
	cm.config.NVMePath = t.TempDir()

	parent := "/adv.bin"
	cm.mu.Lock()
	cm.entries[parent] = &CacheEntry{FilePath: parent, Size: 12, IsChunked: true, NumChunks: 3}
	cm.mu.Unlock()

	data := []byte("chunk-1-data")
	cm.maybeAdvertiseFetchedChunk(parent, parent+"_chunk_1", 1, TierCloud, data)
	cm.bgWg.Wait()

	// Chunk is now servable to peers.
	got, err := cm.GetLocal(context.Background(), parent+"_chunk_1")
	if err != nil {
		t.Fatalf("GetLocal after promote: %v", err)
	}
	if !bytes.Equal(got.Data, data) {
		t.Fatalf("served chunk = %q, want %q", got.Data, data)
	}

	// Holder advertised at parent granularity.
	if coord.publishedCount() != 1 {
		t.Fatalf("published = %d, want 1", coord.publishedCount())
	}
	coord.mu.Lock()
	pub := coord.published[0]
	coord.mu.Unlock()
	if pub.FilePath != parent || pub.PeerID != "local-node" || !pub.IsChunked {
		t.Fatalf("published location = %+v", pub)
	}
	if snap := cm.HerdControlSnapshot(); snap.AdvertisePublishedTotal != 1 {
		t.Fatalf("advertise counter = %d, want 1", snap.AdvertisePublishedTotal)
	}

	// A second chunk arriving within the coalesce window must not re-publish.
	cm.maybeAdvertiseFetchedChunk(parent, parent+"_chunk_2", 2, TierCloud, []byte("chunk-2-data"))
	cm.bgWg.Wait()
	if coord.publishedCount() != 1 {
		t.Fatalf("published after coalesced chunk = %d, want still 1", coord.publishedCount())
	}
	// But the chunk itself is still promoted and servable.
	if _, err := cm.GetLocal(context.Background(), parent+"_chunk_2"); err != nil {
		t.Fatalf("GetLocal chunk_2: %v", err)
	}
}

func TestMaybeAdvertiseFetchedChunk_SkipsLocalAndDisabled(t *testing.T) {
	coord := newLeaseCoordinator()
	cm := newLeasedTestManager(coord)
	cm.config.NVMePath = t.TempDir()

	// Disabled: nothing happens.
	cm.config.FastChunkAdvertise = false
	cm.maybeAdvertiseFetchedChunk("/x.bin", "/x.bin_chunk_0", 0, TierCloud, []byte("d"))
	cm.bgWg.Wait()
	if coord.publishedCount() != 0 {
		t.Fatal("disabled advertise must not publish")
	}

	// NVMe-tier chunks are already local: no advertisement.
	cm.config.FastChunkAdvertise = true
	cm.maybeAdvertiseFetchedChunk("/x.bin", "/x.bin_chunk_0", 0, TierNVMe, []byte("d"))
	cm.bgWg.Wait()
	if coord.publishedCount() != 0 {
		t.Fatal("NVMe-tier chunk must not be re-advertised")
	}
}

// TestReconcile_TopsUpUnderReplicatedHotObject: a hot local object with too
// few holders gets replicated to real peer servers, excluding existing
// holders, within the per-run budget.
func TestReconcile_TopsUpUnderReplicatedHotObject(t *testing.T) {
	okAddr1, okCM1, stop1 := startHerdPeer(t, 8, nil)
	defer stop1()
	okAddr2, okCM2, stop2 := startHerdPeer(t, 8, nil)
	defer stop2()

	coord := newLeaseCoordinator()
	coord.herdCoordinator.peers = []*coordinator.PeerInfo{
		{ID: "ok1", Status: "active", GRPCAddress: okAddr1},
		{ID: "ok2", Status: "active", GRPCAddress: okAddr2},
	}

	cm := newLeasedTestManager(coord)
	cm.config.NVMePath = t.TempDir()
	cm.config.ReplicaReconcileTarget = 3
	cm.config.ReplicaReconcileMaxPerRun = 8

	ps := newHerdPeerStorage(t, coord)
	defer ps.Close()
	cm.peerStorage = ps

	// Hot local object: entry + NVMe bytes; coordinator knows only this node.
	payload := []byte("reconcile me please")
	path := "/hot-object.bin"
	if err := cm.nvmeStorage.Write(context.Background(), path, payload); err != nil {
		t.Fatalf("seed nvme: %v", err)
	}
	cm.mu.Lock()
	cm.entries[path] = &CacheEntry{FilePath: path, Size: int64(len(payload)), LastAccessed: time.Now(), Tier: TierNVMe}
	cm.mu.Unlock()
	coord.locations[path] = []*coordinator.FileLocation{
		{FilePath: path, PeerID: "local-node", StorageTier: "nvme"},
	}

	cm.reconcileReplicasOnce(context.Background())

	// Target 3, holders {local} => deficit 2 => both peers get a copy.
	replicated := 0
	for _, peerCM := range []*DefaultCacheManager{okCM1, okCM2} {
		peerCM.mu.RLock()
		if _, ok := peerCM.entries[path]; ok {
			replicated++
		}
		peerCM.mu.RUnlock()
	}
	if replicated != 2 {
		t.Fatalf("replicated to %d peers, want 2", replicated)
	}
	snap := cm.HerdControlSnapshot()
	if snap.ReconcileRunsTotal != 1 || snap.ReconcileReplicationsTotal != 2 {
		t.Fatalf("runs/replications = %d/%d, want 1/2", snap.ReconcileRunsTotal, snap.ReconcileReplicationsTotal)
	}
}

// TestReconcile_SkipsSatisfiedAndColdObjects: objects at target R or without
// recent demand are left alone.
func TestReconcile_SkipsSatisfiedAndColdObjects(t *testing.T) {
	okAddr, okCM, stop := startHerdPeer(t, 8, nil)
	defer stop()

	coord := newLeaseCoordinator()
	coord.herdCoordinator.peers = []*coordinator.PeerInfo{
		{ID: "ok1", Status: "active", GRPCAddress: okAddr},
	}

	cm := newLeasedTestManager(coord)
	cm.config.NVMePath = t.TempDir()
	cm.config.ReplicaReconcileTarget = 2

	ps := newHerdPeerStorage(t, coord)
	defer ps.Close()
	cm.peerStorage = ps

	seed := func(path string, accessed time.Time) {
		payload := []byte("data for " + path)
		if err := cm.nvmeStorage.Write(context.Background(), path, payload); err != nil {
			t.Fatalf("seed: %v", err)
		}
		cm.mu.Lock()
		cm.entries[path] = &CacheEntry{FilePath: path, Size: int64(len(payload)), LastAccessed: accessed, Tier: TierNVMe}
		cm.mu.Unlock()
	}

	// Satisfied: already at target (local + peer-x).
	seed("/satisfied.bin", time.Now())
	coord.locations["/satisfied.bin"] = []*coordinator.FileLocation{
		{FilePath: "/satisfied.bin", PeerID: "local-node", StorageTier: "nvme"},
		{FilePath: "/satisfied.bin", PeerID: "peer-x", StorageTier: "nvme"},
	}
	// Cold: under-replicated but no recent demand.
	seed("/cold.bin", time.Now().Add(-time.Hour))
	coord.locations["/cold.bin"] = []*coordinator.FileLocation{
		{FilePath: "/cold.bin", PeerID: "local-node", StorageTier: "nvme"},
	}

	cm.reconcileReplicasOnce(context.Background())

	okCM.mu.RLock()
	_, hasSatisfied := okCM.entries["/satisfied.bin"]
	_, hasCold := okCM.entries["/cold.bin"]
	okCM.mu.RUnlock()
	if hasSatisfied {
		t.Fatal("satisfied object must not be re-replicated")
	}
	if hasCold {
		t.Fatal("cold object must not be replicated")
	}
	if snap := cm.HerdControlSnapshot(); snap.ReconcileReplicationsTotal != 0 {
		t.Fatalf("replications = %d, want 0", snap.ReconcileReplicationsTotal)
	}
}

// TestReconcile_BudgetCapsWork: per-run budget bounds total replication ops
// across objects.
func TestReconcile_BudgetCapsWork(t *testing.T) {
	okAddr1, okCM1, stop1 := startHerdPeer(t, 8, nil)
	defer stop1()
	okAddr2, okCM2, stop2 := startHerdPeer(t, 8, nil)
	defer stop2()

	coord := newLeaseCoordinator()
	coord.herdCoordinator.peers = []*coordinator.PeerInfo{
		{ID: "ok1", Status: "active", GRPCAddress: okAddr1},
		{ID: "ok2", Status: "active", GRPCAddress: okAddr2},
	}

	cm := newLeasedTestManager(coord)
	cm.config.NVMePath = t.TempDir()
	cm.config.ReplicaReconcileTarget = 3
	cm.config.ReplicaReconcileMaxPerRun = 1 // hard throttle

	ps := newHerdPeerStorage(t, coord)
	defer ps.Close()
	cm.peerStorage = ps

	for _, path := range []string{"/hot-a.bin", "/hot-b.bin"} {
		payload := []byte("data " + path)
		if err := cm.nvmeStorage.Write(context.Background(), path, payload); err != nil {
			t.Fatalf("seed: %v", err)
		}
		cm.mu.Lock()
		cm.entries[path] = &CacheEntry{FilePath: path, Size: int64(len(payload)), LastAccessed: time.Now(), Tier: TierNVMe}
		cm.mu.Unlock()
		coord.locations[path] = []*coordinator.FileLocation{
			{FilePath: path, PeerID: "local-node", StorageTier: "nvme"},
		}
	}

	cm.reconcileReplicasOnce(context.Background())

	total := 0
	for _, peerCM := range []*DefaultCacheManager{okCM1, okCM2} {
		peerCM.mu.RLock()
		for path := range peerCM.entries {
			if path == "/hot-a.bin" || path == "/hot-b.bin" {
				total++
			}
		}
		peerCM.mu.RUnlock()
	}
	if total != 1 {
		t.Fatalf("total replications = %d, want exactly 1 (budget)", total)
	}
}

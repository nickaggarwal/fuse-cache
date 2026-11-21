package cache

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"fuse-client/internal/coordinator"
)

// holderLeaseCoordinator is the tier-retract double plus generation-guarded
// retraction, recording what was published and retracted.
type holderLeaseCoordinator struct {
	*tierRetractCoordinator
	gmu       sync.Mutex
	genCalls  []uint64
	failFirst atomic.Int64 // fail this many retractions before succeeding
}

func (c *holderLeaseCoordinator) DeletePeerTierLocationGen(_ context.Context, filePath, peerID, tier string, gen uint64) error {
	if c.failFirst.Load() > 0 {
		c.failFirst.Add(-1)
		return errors.New("coordinator unavailable")
	}
	c.gmu.Lock()
	c.genCalls = append(c.genCalls, gen)
	c.gmu.Unlock()
	c.deleteCoordinator.mu.Lock()
	defer c.deleteCoordinator.mu.Unlock()
	kept := c.locations[filePath][:0]
	for _, l := range c.locations[filePath] {
		if !(l.PeerID == peerID && l.StorageTier == tier && (gen == 0 || l.Generation <= gen)) {
			kept = append(kept, l)
		}
	}
	c.locations[filePath] = kept
	return nil
}

// UpdateFileLocation upserts per peer and tier, like the real stores, and
// ignores an older admission than the stored one.
func (c *holderLeaseCoordinator) UpdateFileLocation(_ context.Context, loc *coordinator.FileLocation) error {
	c.deleteCoordinator.mu.Lock()
	defer c.deleteCoordinator.mu.Unlock()
	cp := *loc
	for i, l := range c.locations[loc.FilePath] {
		if l.PeerID == loc.PeerID && l.StorageTier == loc.StorageTier {
			if l.Generation != 0 && loc.Generation != 0 && loc.Generation < l.Generation {
				return nil
			}
			c.locations[loc.FilePath][i] = &cp
			return nil
		}
	}
	c.locations[loc.FilePath] = append(c.locations[loc.FilePath], &cp)
	return nil
}

func (c *holderLeaseCoordinator) gens() []uint64 {
	c.gmu.Lock()
	defer c.gmu.Unlock()
	return append([]uint64(nil), c.genCalls...)
}

func (c *holderLeaseCoordinator) row(path string) *coordinator.FileLocation {
	c.deleteCoordinator.mu.Lock()
	defer c.deleteCoordinator.mu.Unlock()
	for _, l := range c.locations[path] {
		if l.StorageTier == "nvme" {
			cp := *l
			return &cp
		}
	}
	return nil
}

func leaseTestManager(t *testing.T) (*DefaultCacheManager, *holderLeaseCoordinator) {
	t.Helper()
	cm := evictTestManager(t, 10_000)
	coord := &holderLeaseCoordinator{tierRetractCoordinator: &tierRetractCoordinator{deleteCoordinator: newDeleteCoordinator()}}
	cm.config.Coordinator = coord
	cm.config.LocalPeerID = "peer-a"
	cm.SetCoordinatorCapabilities([]string{"tier-retract", "holder-lease"})
	return cm, coord
}

// Admission publishes a generation and a lease; the whole local file is not
// partial.
func TestHolderRowCarriesGenerationAndLease(t *testing.T) {
	cm, coord := leaseTestManager(t)
	seedNVMe(t, cm, "/m/a.bin", 100, true, time.Minute)
	cm.publishFileLocation(context.Background(), &CacheEntry{FilePath: "/m/a.bin", Size: 100}, TierNVMe)
	r := coord.row("/m/a.bin")
	if r == nil || r.Generation == 0 || r.ExpiresAt.Before(time.Now().Add(10*time.Minute)) || r.Partial {
		t.Fatalf("row = %+v, want generation, ~15 min lease, not partial", r)
	}
}

// Against a coordinator without holder-lease nothing changes on the wire.
func TestHolderRowLegacyCoordinator(t *testing.T) {
	cm, coord := leaseTestManager(t)
	cm.SetCoordinatorCapabilities([]string{"tier-retract"})
	seedNVMe(t, cm, "/m/a.bin", 100, true, time.Minute)
	cm.publishFileLocation(context.Background(), &CacheEntry{FilePath: "/m/a.bin", Size: 100}, TierNVMe)
	if r := coord.row("/m/a.bin"); r == nil || r.Generation != 0 || !r.ExpiresAt.IsZero() {
		t.Fatalf("row = %+v, want no lifecycle fields", r)
	}
}

// A node holding one chunk of a chunked file advertises it as partial, not
// as a whole copy.
func TestHolderRowPartialChunks(t *testing.T) {
	cm, coord := leaseTestManager(t)
	cm.mu.Lock()
	cm.entries.put(&CacheEntry{FilePath: "/m/w.bin", Size: 300, IsChunked: true, ChunkSize: 100, NumChunks: 3, Tier: TierPeer})
	cm.mu.Unlock()
	seedNVMe(t, cm, "/m/w.bin_chunk_1", 100, true, time.Minute)
	if cm.chunkAds == nil {
		cm.chunkAds = newChunkAdvertiser()
	}
	cm.advertiseHolder(context.Background(), "/m/w.bin", true)
	if r := coord.row("/m/w.bin"); r == nil || !r.Partial {
		t.Fatalf("row = %+v, want partial", r)
	}
}

// Eviction retracts the generation it evicted; a re-admission gets a newer
// generation that the late retraction cannot remove.
func TestEvictionRetractsItsGeneration(t *testing.T) {
	cm, coord := leaseTestManager(t)
	ctx := context.Background()
	seedNVMe(t, cm, "/m/a.bin", 100, true, time.Minute)
	cm.publishFileLocation(ctx, &CacheEntry{FilePath: "/m/a.bin", Size: 100}, TierNVMe)
	g1 := coord.row("/m/a.bin").Generation

	// Evicted: the admission ends; then the file lands again before the
	// retraction is sent.
	cm.mu.Lock()
	cm.entries.del("/m/a.bin")
	cm.mu.Unlock()
	gEvicted := cm.holders.evicted("/m/a.bin")
	seedNVMe(t, cm, "/m/a.bin", 100, true, time.Second)
	cm.publishFileLocation(ctx, &CacheEntry{FilePath: "/m/a.bin", Size: 100}, TierNVMe)
	g2 := coord.row("/m/a.bin").Generation
	if gEvicted != g1 || g2 <= g1 {
		t.Fatalf("generations: first %d, evicted %d, re-admitted %d", g1, gEvicted, g2)
	}
	// The late retraction for g1 skips (file held) or, if sent, removes only <= g1.
	_ = coord.DeletePeerTierLocationGen(ctx, "/m/a.bin", "peer-a", "nvme", g1)
	if r := coord.row("/m/a.bin"); r == nil || r.Generation != g2 {
		t.Fatalf("row after late retraction = %+v, want generation %d kept", r, g2)
	}
}

// A failed retraction is retried with backoff until it lands.
func TestRetractionRetries(t *testing.T) {
	cm, coord := leaseTestManager(t)
	coord.failFirst.Store(2)
	coord.seed(&coordinator.FileLocation{FilePath: "/m/x.bin", PeerID: "peer-a", StorageTier: "nvme", Generation: 3})
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if !cm.retractHolder(ctx, "/m/x.bin", 3) {
		t.Fatal("retraction did not land after retries")
	}
	if n := cm.holders.retractRetried.Load(); n != 2 {
		t.Fatalf("retries = %d, want 2", n)
	}
	if coord.row("/m/x.bin") != nil {
		t.Fatal("row still present")
	}
}

// Renewal republishes what is held with a fresh lease and retracts rows for
// paths no longer held (a missed eviction notice).
func TestRenewHoldersReconciles(t *testing.T) {
	cm, coord := leaseTestManager(t)
	ctx := context.Background()
	seedNVMe(t, cm, "/m/keep.bin", 100, true, 2*time.Minute)
	seedNVMe(t, cm, "/m/lost.bin", 100, true, time.Minute)
	cm.publishFileLocation(ctx, &CacheEntry{FilePath: "/m/keep.bin", Size: 100}, TierNVMe)
	cm.publishFileLocation(ctx, &CacheEntry{FilePath: "/m/lost.bin", Size: 100}, TierNVMe)
	before := coord.row("/m/keep.bin").ExpiresAt

	// lost.bin disappears without an eviction notice (crash, disk wipe).
	cm.mu.Lock()
	cm.entries.del("/m/lost.bin")
	cm.mu.Unlock()

	time.Sleep(5 * time.Millisecond)
	renewed, retracted := cm.RenewHolders(ctx)
	if renewed != 1 || retracted != 1 {
		t.Fatalf("renewed %d retracted %d, want 1 and 1", renewed, retracted)
	}
	if r := coord.row("/m/keep.bin"); r == nil || !r.ExpiresAt.After(before) {
		t.Fatalf("keep.bin lease not renewed: %+v", r)
	}
	if coord.row("/m/lost.bin") != nil {
		t.Fatal("stale row for lost.bin not retracted")
	}
}

// A file deleted from disk behind the client's back is dropped from the
// index on the next renewal and its row retracted, not renewed.
func TestRenewHoldersDropsVanishedFile(t *testing.T) {
	cm, coord := leaseTestManager(t)
	ctx := context.Background()
	seedNVMe(t, cm, "/m/gone.bin", 100, true, time.Minute)
	cm.publishFileLocation(ctx, &CacheEntry{FilePath: "/m/gone.bin", Size: 100}, TierNVMe)
	if err := os.Remove(filepath.Join(cm.config.NVMePath, "m/gone.bin")); err != nil {
		t.Fatal(err)
	}
	renewed, retracted := cm.RenewHolders(ctx)
	if renewed != 0 || retracted != 1 {
		t.Fatalf("renewed %d retracted %d, want 0 and 1", renewed, retracted)
	}
	if coord.row("/m/gone.bin") != nil {
		t.Fatal("row for a vanished file still published")
	}
	if cm.holders.vanished.Load() != 1 {
		t.Fatalf("vanished = %d", cm.holders.vanished.Load())
	}
	cm.mu.RLock()
	_, still := cm.entries.get("/m/gone.bin")
	cm.mu.RUnlock()
	if still {
		t.Fatal("vanished entry kept in the index")
	}
}

// A retraction that fails every attempt is kept and lands on a later
// renewal pass once the coordinator is back, not dropped.
func TestFailedRetractionRetriedNextPass(t *testing.T) {
	cm, coord := leaseTestManager(t)
	coord.seed(&coordinator.FileLocation{FilePath: "/m/p.bin", PeerID: "peer-a", StorageTier: "nvme", Generation: 9})
	coord.failFirst.Store(1 << 20) // coordinator down
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // no backoff waits: one attempt, then give up
	cm.retractHolder(ctx, "/m/p.bin", 9)
	if n := cm.HolderLeaseStats().PendingRetractions; n != 1 {
		t.Fatalf("pending = %d, want 1 after a failed retraction", n)
	}
	coord.failFirst.Store(0) // coordinator back
	cm.RenewHolders(context.Background())
	if coord.row("/m/p.bin") != nil {
		t.Fatal("pending retraction not sent on the next pass")
	}
	if s := cm.HolderLeaseStats(); s.PendingRetractions != 0 || s.PendingRetried != 1 {
		t.Fatalf("stats = %+v", s)
	}
}

// A pending retraction for a path held again is dropped, not sent.
func TestPendingRetractionSkipsRelanded(t *testing.T) {
	cm, coord := leaseTestManager(t)
	cm.holders.keepPending("/m/back.bin", 3)
	seedNVMe(t, cm, "/m/back.bin", 100, true, time.Minute)
	coord.seed(&coordinator.FileLocation{FilePath: "/m/back.bin", PeerID: "peer-a", StorageTier: "nvme", Generation: 4})
	cm.retryPendingRetractions(context.Background())
	if coord.row("/m/back.bin") == nil {
		t.Fatal("retracted the row of a file held again")
	}
	if cm.HolderLeaseStats().PendingRetractions != 0 {
		t.Fatal("pending entry kept for a re-landed file")
	}
}

// A renewal pass cancelled before its retraction step keeps the retraction
// pending (it used to skip the path and forget it).
func TestCancelledPassKeepsRetraction(t *testing.T) {
	cm, coord := leaseTestManager(t)
	ctx := context.Background()
	seedNVMe(t, cm, "/m/c.bin", 100, true, time.Minute)
	cm.publishFileLocation(ctx, &CacheEntry{FilePath: "/m/c.bin", Size: 100}, TierNVMe)
	cm.mu.Lock()
	cm.entries.del("/m/c.bin")
	cm.mu.Unlock()
	cctx, cancel := context.WithCancel(ctx)
	cancel()
	cm.RenewHolders(cctx)
	if n := cm.HolderLeaseStats().PendingRetractions; n != 1 {
		t.Fatalf("pending = %d after a cancelled pass, want 1", n)
	}
	cm.RenewHolders(ctx)
	if coord.row("/m/c.bin") != nil {
		t.Fatal("retraction not sent on the next pass")
	}
}

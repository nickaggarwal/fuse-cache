package cache

import (
	"context"
	"sync"
	"testing"
	"time"

	"fuse-client/internal/coordinator"
)

// tierRetractCoordinator records tier-filtered retractions on top of the
// delete double.
type tierRetractCoordinator struct {
	*deleteCoordinator
	tmu      sync.Mutex
	retracts []string
	fail     error
}

func (c *tierRetractCoordinator) DeletePeerTierLocation(_ context.Context, filePath, peerID, tier string) error {
	c.tmu.Lock()
	defer c.tmu.Unlock()
	if c.fail != nil {
		return c.fail
	}
	c.retracts = append(c.retracts, filePath+"|"+peerID+"|"+tier)
	return nil
}

func (c *tierRetractCoordinator) calls() []string {
	c.tmu.Lock()
	defer c.tmu.Unlock()
	return append([]string(nil), c.retracts...)
}

func retractTestManager(t *testing.T) (*DefaultCacheManager, *tierRetractCoordinator) {
	t.Helper()
	cm := evictTestManager(t, 10_000)
	coord := &tierRetractCoordinator{deleteCoordinator: newDeleteCoordinator()}
	cm.config.Coordinator = coord
	cm.config.LocalPeerID = "peer-a"
	cm.SetCoordinatorCapabilities([]string{"tier-retract"})
	return cm, coord
}

// Before the coordinator advertises tier-retract nothing is queued: an older
// coordinator would read the request as a per-peer delete and drop the cloud
// row too.
func TestEvictionRetractNeedsCapability(t *testing.T) {
	cm, coord := retractTestManager(t)
	cm.SetCoordinatorCapabilities(nil)
	cm.queueEvictRetraction("/old-coordinator.bin")
	cm.flushEvictRetractions(context.Background())
	if got := coord.calls(); len(got) != 0 {
		t.Fatalf("retracted against a coordinator without tier-retract: %v", got)
	}
}

// An evicted file's "nvme" row is retracted for this peer only, and a chunk
// eviction retracts the parent (holders are advertised per parent file).
func TestEvictionRetractsNVMeRow(t *testing.T) {
	cm, coord := retractTestManager(t)
	seedNVMe(t, cm, "/models/m/a.bin", 4000, true, 3*time.Minute)
	seedNVMe(t, cm, "/models/m/keep.bin", 100, true, time.Minute)

	cm.evictToTarget(context.Background(), 1000)
	cm.queueEvictRetraction("/models/m/big.bin_chunk_7")
	cm.flushEvictRetractions(context.Background())

	got := coord.calls()
	want := map[string]bool{
		"/models/m/a.bin|peer-a|nvme":   true,
		"/models/m/big.bin|peer-a|nvme": true,
	}
	if len(got) != len(want) {
		t.Fatalf("retractions = %v, want %v", got, want)
	}
	for _, c := range got {
		if !want[c] {
			t.Fatalf("unexpected retraction %q (all: %v)", c, got)
		}
	}
	if m := cm.metrics.EvictRetracted.Load(); m != 2 {
		t.Fatalf("EvictRetracted = %d, want 2", m)
	}
}

// A file landed again between eviction and the flush is held now: its row is
// true and must not be retracted.
func TestEvictionRetractSkipsRelandedFile(t *testing.T) {
	cm, coord := retractTestManager(t)
	cm.queueEvictRetraction("/models/m/back.bin")
	seedNVMe(t, cm, "/models/m/back.bin", 100, true, time.Minute)

	cm.flushEvictRetractions(context.Background())

	if got := coord.calls(); len(got) != 0 {
		t.Fatalf("retracted a re-landed file: %v", got)
	}
	if m := cm.metrics.EvictRetractSkippedRelanded.Load(); m != 1 {
		t.Fatalf("EvictRetractSkippedRelanded = %d, want 1", m)
	}
}

// A coordinator that cannot retract by tier is left alone (never a whole-path
// or per-peer delete, which would also drop the cloud row), and failures are
// counted and dropped, not retried forever.
func TestEvictionRetractAdvisory(t *testing.T) {
	cm := evictTestManager(t, 10_000)
	plain := newDeleteCoordinator()
	cm.config.Coordinator = plain
	cm.config.LocalPeerID = "peer-a"
	cm.queueEvictRetraction("/x.bin")
	cm.flushEvictRetractions(context.Background())
	if len(plain.deleteCalls()) != 0 {
		t.Fatalf("fell back to DeleteFileLocation: %v", plain.deleteCalls())
	}

	cm2, coord := retractTestManager(t)
	coord.fail = context.DeadlineExceeded
	cm2.queueEvictRetraction("/y.bin")
	cm2.flushEvictRetractions(context.Background())
	cm2.flushEvictRetractions(context.Background())
	if m := cm2.metrics.EvictRetractFailed.Load(); m != 1 {
		t.Fatalf("EvictRetractFailed = %d, want 1 (one attempt, then dropped)", m)
	}
}

// The coordinator store drops only the peer's row in the named tier.
func TestStoreDeletePeerTierLocation(t *testing.T) {
	s := coordinator.NewInMemoryStore()
	ctx := context.Background()
	for _, loc := range []*coordinator.FileLocation{
		{FilePath: "/f", PeerID: "peer-a", StorageTier: "nvme", FileSize: 10},
		{FilePath: "/f", PeerID: "peer-a", StorageTier: "cloud", FileSize: 10},
		{FilePath: "/f", PeerID: "peer-b", StorageTier: "nvme", FileSize: 10},
	} {
		if err := s.PutFileLocation(ctx, loc); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.DeletePeerTierLocation(ctx, "/f", "peer-a", "nvme"); err != nil {
		t.Fatal(err)
	}
	locs, err := s.GetFileLocations(ctx, "/f")
	if err != nil {
		t.Fatal(err)
	}
	seen := map[string]bool{}
	for _, l := range locs {
		seen[l.PeerID+"|"+l.StorageTier] = true
	}
	if seen["peer-a|nvme"] || !seen["peer-a|cloud"] || !seen["peer-b|nvme"] || len(locs) != 2 {
		t.Fatalf("rows after tier delete = %v", seen)
	}
}

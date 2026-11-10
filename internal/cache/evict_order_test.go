package cache

import (
	"context"
	"testing"
	"time"

	"fuse-client/internal/coordinator"
)

func orderTestManager(t *testing.T, budget int64) (*DefaultCacheManager, *deleteCoordinator) {
	t.Helper()
	cm := evictTestManager(t, budget)
	coord := newDeleteCoordinator()
	cm.config.Coordinator = coord
	cm.config.LocalPeerID = "peer-a"
	cm.config.EvictPreferReplicated = true
	return cm, coord
}

func evicted(cm *DefaultCacheManager, p string) bool {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	_, ok := cm.entries.get(p)
	return !ok
}

// The oldest file is this node's only local copy; a slightly newer one is
// also on another node. Only one has to go: the shared one does.
func TestEvictPrefersFilesOtherNodesHold(t *testing.T) {
	cm, coord := orderTestManager(t, 10_000)
	seedNVMe(t, cm, "/only-here.bin", 3000, true, 3*time.Hour)
	seedNVMe(t, cm, "/shared.bin", 3000, true, 2*time.Hour)
	seedNVMe(t, cm, "/recent.bin", 3000, true, time.Minute)
	coord.seed(&coordinator.FileLocation{FilePath: "/shared.bin", PeerID: "peer-b", StorageTier: "nvme"})
	// A cloud row and our own row do not count as another holder.
	coord.seed(&coordinator.FileLocation{FilePath: "/only-here.bin", PeerID: "peer-b", StorageTier: "cloud"})
	coord.seed(&coordinator.FileLocation{FilePath: "/only-here.bin", PeerID: "peer-a", StorageTier: "nvme"})

	cm.evictToTarget(context.Background(), 6500)

	if !evicted(cm, "/shared.bin") {
		t.Fatal("shared file kept while the only local copy went")
	}
	if evicted(cm, "/only-here.bin") {
		t.Fatal("evicted the only local copy when a shared one freed enough")
	}
	if cm.metrics.EvictOrderShared.Load() == 0 {
		t.Fatal("EvictOrderShared not counted")
	}
}

// Without holder information, idle x size orders the batch: one big cold
// file frees in one delete what would take several small ones.
func TestEvictWeighsSizeAndIdle(t *testing.T) {
	cm, _ := orderTestManager(t, 1<<30)
	cm.config.Coordinator = nil
	mb := 1 << 20
	seedNVMe(t, cm, "/small-old-1.bin", mb, true, 3*time.Hour)
	seedNVMe(t, cm, "/small-old-2.bin", mb, true, 3*time.Hour)
	seedNVMe(t, cm, "/big.bin", 20*mb, true, 2*time.Hour)
	seedNVMe(t, cm, "/recent.bin", mb, true, time.Second)

	used, _ := cm.Stats()
	cm.evictToTarget(context.Background(), used-10*int64(mb))

	if !evicted(cm, "/big.bin") {
		t.Fatal("big cold file kept")
	}
	if evicted(cm, "/small-old-1.bin") || evicted(cm, "/small-old-2.bin") {
		t.Fatal("small files evicted although the big one alone freed enough")
	}
}

// Disabled: plain LRU, oldest first.
func TestEvictOrderDisabledIsLRU(t *testing.T) {
	cm, coord := orderTestManager(t, 10_000)
	cm.config.EvictPreferReplicated = false
	seedNVMe(t, cm, "/oldest.bin", 3000, true, 3*time.Hour)
	seedNVMe(t, cm, "/shared.bin", 3000, true, 2*time.Hour)
	seedNVMe(t, cm, "/recent.bin", 3000, true, time.Minute)
	coord.seed(&coordinator.FileLocation{FilePath: "/shared.bin", PeerID: "peer-b", StorageTier: "nvme"})

	cm.evictToTarget(context.Background(), 6500)
	if !evicted(cm, "/oldest.bin") || evicted(cm, "/shared.bin") {
		t.Fatal("disabled ordering did not evict oldest first")
	}
}

// Pins and the durability guard still apply after the reorder.
func TestEvictOrderKeepsGuards(t *testing.T) {
	cm, coord := orderTestManager(t, 10_000)
	seedNVMe(t, cm, "/pinned/shared.bin", 3000, true, 3*time.Hour)
	seedNVMe(t, cm, "/unpersisted-shared.bin", 3000, false, 2*time.Hour)
	seedNVMe(t, cm, "/plain.bin", 3000, true, time.Hour)
	for _, p := range []string{"/pinned/shared.bin", "/unpersisted-shared.bin"} {
		coord.seed(&coordinator.FileLocation{FilePath: p, PeerID: "peer-b", StorageTier: "nvme"})
	}
	cm.config.PinnedPrefixesFn = func() []string { return []string{"/pinned"} }

	cm.evictToTarget(context.Background(), 6500)
	if evicted(cm, "/pinned/shared.bin") {
		t.Fatal("pinned file evicted")
	}
	if !evicted(cm, "/plain.bin") {
		t.Fatal("nothing evictable was evicted")
	}
}

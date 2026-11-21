package cache

import (
	"context"
	"sync"
	"testing"
	"time"

	"fuse-client/internal/coordinator"
)

// slotCoordinator grants fetch leases like the coordinator: one holder per
// key until released or expired.
type slotCoordinator struct {
	*deleteCoordinator
	mu     sync.Mutex
	leases map[string]string
}

func (c *slotCoordinator) AcquireFetchLease(_ context.Context, key, holder string, _ time.Duration) (string, bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.leases == nil {
		c.leases = map[string]string{}
	}
	if h, ok := c.leases[key]; ok && h != holder {
		return h, false, nil
	}
	c.leases[key] = holder
	return holder, true, nil
}

func (c *slotCoordinator) ReleaseFetchLease(_ context.Context, key, holder string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.leases[key] == holder {
		delete(c.leases, key)
	}
	return nil
}

var _ coordinator.FetchLeaser = (*slotCoordinator)(nil)

// At most ReplicationSlots replications hold a slot at once across nodes;
// a released slot can be taken again.
func TestReplicationSlotsCapClusterWide(t *testing.T) {
	coord := &slotCoordinator{deleteCoordinator: newDeleteCoordinator()}
	mk := func(id string) *DefaultCacheManager {
		cm := evictTestManager(t, 10_000)
		cm.config.Coordinator = coord
		cm.config.LocalPeerID = id
		cm.config.ReplicationSlots = 3
		return cm
	}
	a, b := mk("a"), mk("b")
	ctx := context.Background()
	var rels []func()
	for i, cm := range []*DefaultCacheManager{a, b, a} {
		rel, ok := cm.acquireReplicationSlot(ctx)
		if !ok {
			t.Fatalf("slot %d refused with free slots", i)
		}
		rels = append(rels, rel)
	}
	if _, ok := b.acquireReplicationSlot(ctx); ok {
		t.Fatal("a 4th replication got a slot with ReplicationSlots=3")
	}
	rels[0]()
	if rel, ok := b.acquireReplicationSlot(ctx); !ok {
		t.Fatal("released slot not reusable")
	} else {
		rel()
	}
}

// Negative disables the cap; no coordinator lease support means no cap.
func TestReplicationSlotsDisabled(t *testing.T) {
	cm := evictTestManager(t, 10_000)
	cm.config.ReplicationSlots = -1
	if _, ok := cm.acquireReplicationSlot(context.Background()); !ok {
		t.Fatal("disabled slots refused")
	}
	cm.config.ReplicationSlots = 0
	cm.config.Coordinator = newDeleteCoordinator()
	cm.config.LocalPeerID = "x"
	for i := 0; i < 50; i++ {
		if _, ok := cm.acquireReplicationSlot(context.Background()); !ok {
			t.Fatal("coordinator without leases must not block replication")
		}
	}
}

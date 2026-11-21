package cache

import (
	"context"
	"crypto/rand"
	"fmt"
	"math/big"
	"sync"
	"time"

	"fuse-client/internal/coordinator"
)

// Cluster-wide replication slots.
//
// The replica reconciler's budget is per node per pass (8 by default). When
// many holders disappear together (a network partition, a zone outage, a
// mass expiry of holder leases), every node finds deficits in the same pass
// and the fleet copies at N x budget: a 20-node cluster would start up to
// 160 copies at once against the same few surviving sources, the
// replication storm Ceph's osd_max_backfills exists to prevent.
//
// Each replication now holds one of ReplicationSlots cluster-wide slots, taken
// as a coordinator fetch lease on "replication-slot/<i>" (the same leases
// that single-flight origin fetches). A node that finds every slot taken
// ends its pass; the deficit is picked up on a later pass. Advisory like all
// coordination: with the coordinator unreachable, replication proceeds under
// the per-node budget alone.

const (
	// defaultReplicationSlots caps simultaneous replications cluster-wide.
	defaultReplicationSlots = 16
	// replicationSlotTTL bounds a slot held by a node that dies mid-copy.
	// The coordinator caps fetch leases at 60 s, so a longer copy renews
	// its slot every replicationSlotRenew (re-acquiring one's own lease
	// extends it).
	replicationSlotTTL   = 60 * time.Second
	replicationSlotRenew = 20 * time.Second
)

func (cm *DefaultCacheManager) replicationSlots() int {
	if cm.config.ReplicationSlots > 0 {
		return cm.config.ReplicationSlots
	}
	return defaultReplicationSlots
}

// acquireReplicationSlot takes one free cluster-wide slot. ok is false when
// every slot is held by some node.
//
// Leases are per node, and re-acquiring one's own lease renews it, so a slot
// this node already holds (an earlier replication still running) is skipped
// locally: each in-flight replication on this node holds a distinct slot.
func (cm *DefaultCacheManager) acquireReplicationSlot(ctx context.Context) (release func(), ok bool) {
	n := cm.replicationSlots()
	if n <= 0 || cm.config.ReplicationSlots < 0 {
		return func() {}, true // disabled
	}
	// No lease support (no coordinator, or an older one): the per-node
	// budget is the only cap, as before.
	if _, ok := cm.config.Coordinator.(coordinator.FetchLeaser); !ok || cm.config.LocalPeerID == "" {
		return func() {}, true
	}
	// Start at a random slot so nodes do not all contend for slot 0.
	start := 0
	if r, err := rand.Int(rand.Reader, big.NewInt(int64(n))); err == nil {
		start = int(r.Int64())
	}
	for i := 0; i < n; i++ {
		slot := (start + i) % n
		if !cm.slotsHeld.claim(slot) {
			continue
		}
		key := fmt.Sprintf("replication-slot/%d", slot)
		granted, rel := cm.TryOriginLease(ctx, key, replicationSlotTTL)
		if !granted {
			cm.slotsHeld.drop(slot)
			continue
		}
		stop := make(chan struct{})
		go func() {
			t := time.NewTicker(replicationSlotRenew)
			defer t.Stop()
			for {
				select {
				case <-stop:
					return
				case <-t.C:
					cm.TryOriginLease(ctx, key, replicationSlotTTL) // own lease: renews
				}
			}
		}()
		var once sync.Once
		return func() {
			once.Do(func() {
				close(stop)
				rel()
				cm.slotsHeld.drop(slot)
			})
		}, true
	}
	return nil, false
}

// heldSlots is the set of slots this node holds.
type heldSlots struct {
	mu sync.Mutex
	m  map[int]bool
}

func (h *heldSlots) claim(i int) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.m == nil {
		h.m = map[int]bool{}
	}
	if h.m[i] {
		return false
	}
	h.m[i] = true
	return true
}

func (h *heldSlots) drop(i int) {
	h.mu.Lock()
	delete(h.m, i)
	h.mu.Unlock()
}

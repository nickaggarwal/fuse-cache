package cache

import (
	"sync"
	"sync/atomic"
	"time"
)

// "Down" vs "out" for re-replication, after Ceph's mon_osd_down_out_interval
// and its noout/norebalance flags.
//
// A peer that disappears is routed around at once: the coordinator drops it
// from GetPeers, so no read goes to it. The replica reconciler, though,
// counted holders from location rows, and the coordinator never removes a
// dead peer's rows. So a node that was gone for good still counted as a
// holder forever, and its files were never re-replicated: the replica target
// silently degraded by one for every node that ever left.
//
// The reconciler now applies Ceph's two states. A holder missing from the
// live peer list for less than DownOutInterval is "down": still counted, its
// disk is intact and it is most likely restarting (a DaemonSet rollout
// restarts every node within minutes, and none of them should trigger
// copies). Past the interval it is "out": no longer counted, and the
// deficit is re-replicated to live peers.
//
// The cluster flag "norebalance" (coordinator /api/flags), set before planned
// maintenance that may outlast the interval, pauses re-replication entirely.

// defaultDownOutInterval is Ceph's mon_osd_down_out_interval.
const defaultDownOutInterval = 10 * time.Minute

type peerDownTracker struct {
	mu sync.Mutex
	// lastLive is when each peer was last seen in GetPeers (or this node).
	lastLive map[string]time.Time
	// noRebalance mirrors the coordinator's "norebalance" flag.
	noRebalance atomic.Bool
	// pausedPasses counts reconcile passes skipped by norebalance;
	// downHeld counts holders kept in the count while down.
	pausedPasses atomic.Int64
	downHeld     atomic.Int64
}

// observeLive records the current live peer IDs.
func (t *peerDownTracker) observeLive(ids []string, now time.Time) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.lastLive == nil {
		t.lastLive = make(map[string]time.Time)
	}
	for _, id := range ids {
		t.lastLive[id] = now
	}
}

// counts reports whether holder id still counts toward the replica target:
// live now, or down for less than interval. A holder never seen live by this
// process (rows from before it started) counts until interval after start,
// the same grace a just-seen peer gets.
func (t *peerDownTracker) counts(id string, live map[string]struct{}, started, now time.Time, interval time.Duration) bool {
	if _, ok := live[id]; ok {
		return true
	}
	t.mu.Lock()
	last, seen := t.lastLive[id]
	t.mu.Unlock()
	if !seen {
		if started.IsZero() {
			return true // no start time to measure from: keep the old rule
		}
		last = started
	}
	return now.Sub(last) < interval
}

// SetClusterFlags records the coordinator's active flags (from /api/health).
func (cm *DefaultCacheManager) SetClusterFlags(flags []string) {
	on := false
	for _, f := range flags {
		if f == "norebalance" {
			on = true
		}
	}
	if cm.downOut.noRebalance.Swap(on) != on {
		cm.logger.Printf("Re-replication %s by cluster flag norebalance", map[bool]string{true: "paused", false: "resumed"}[on])
	}
}

func (cm *DefaultCacheManager) downOutInterval() time.Duration {
	if cm.config.DownOutInterval > 0 {
		return cm.config.DownOutInterval
	}
	return defaultDownOutInterval
}

func (cm *DefaultCacheManager) rebalanceHealthCheck(time.Time) *HealthCheck {
	if !cm.downOut.noRebalance.Load() {
		return nil
	}
	return &HealthCheck{
		Code: HealthNoRebalance, Severity: "degraded",
		Message: "re-replication paused by the norebalance cluster flag; clear it when the rollout is done",
	}
}

// RebalanceStats reports passes paused by norebalance and holders kept while
// down.
func (cm *DefaultCacheManager) RebalanceStats() (paused, downHeld int64, noRebalance bool) {
	return cm.downOut.pausedPasses.Load(), cm.downOut.downHeld.Load(), cm.downOut.noRebalance.Load()
}

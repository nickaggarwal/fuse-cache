package cache

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"fuse-client/internal/coordinator"
)

// Retracting location rows on eviction.
//
// A node publishes itself as a holder ("nvme" row) on write, warm and fetch,
// but eviction used to delete the bytes and leave the row behind. The row then
// said "held at some point", not "held now": peers kept asking this node for
// files it no longer had (a typed miss, so reads stayed correct, but each one
// cost a round trip), and anything routing on location metadata over-counted
// residency.
//
// Eviction now queues the evicted path, and a background loop retracts this
// node's "nvme" row for it in small batches. Only the nvme tier is touched:
// the same peer's "cloud" row records that the object is durable in object
// storage, which eviction does not change.
//
// Advisory, like every coordinator interaction: a failed retraction is
// counted and dropped, never retried forever, and reads never wait on it.
// A file re-landed after it was queued (re-fetched, re-warmed) is skipped at
// send time, so a slow batch cannot retract a row that is true again.

const (
	// retractFlushInterval batches retractions: an eviction pass frees many
	// files at once and one RPC each is fine, but not inline in the pass.
	retractFlushInterval = 2 * time.Second
	// retractMaxPending bounds the queue; past it the oldest pending
	// retractions are dropped (the rows stay stale, which is today's state).
	retractMaxPending  = 4096
	retractCallTimeout = 2 * time.Second
)

type evictRetractor struct {
	mu      sync.Mutex
	pending map[string]struct{}
	order   []string
	// enabled is set once the coordinator advertises "tier-retract": before
	// that, a tier-filtered delete would reach an older coordinator as a
	// per-peer delete and drop this node's cloud row as well.
	enabled atomic.Bool
}

func (r *evictRetractor) add(path string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.pending == nil {
		r.pending = map[string]struct{}{}
	}
	if _, ok := r.pending[path]; ok {
		return true
	}
	dropped := false
	if len(r.order) >= retractMaxPending {
		oldest := r.order[0]
		r.order = r.order[1:]
		delete(r.pending, oldest)
		dropped = true
	}
	r.pending[path] = struct{}{}
	r.order = append(r.order, path)
	return !dropped
}

func (r *evictRetractor) take() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := r.order
	r.order = nil
	r.pending = nil
	return out
}

// queueEvictRetraction records that filePath's local copy was evicted. Chunk
// paths are folded onto their parent: holders are advertised per parent file.
func (cm *DefaultCacheManager) queueEvictRetraction(filePath string) {
	if cm.config.Coordinator == nil || cm.config.LocalPeerID == "" {
		return
	}
	if _, ok := cm.config.Coordinator.(coordinator.TierLocationDeleter); !ok || !cm.evictRetract.enabled.Load() {
		return
	}
	if parent, isChunk := parentFilePathFromChunkPath(filePath); isChunk {
		filePath = parent
	}
	if !cm.evictRetract.add(filePath) {
		cm.metrics.EvictRetractDropped.Add(1)
	}
}

// SetCoordinatorCapabilities records the features the coordinator advertises
// in GET /api/health; the client calls it on every coordinator instance.
func (cm *DefaultCacheManager) SetCoordinatorCapabilities(caps []string) {
	has, reports := false, false
	for _, c := range caps {
		switch c {
		case "tier-retract":
			has = true
		case "failure-report":
			reports = true
		}
	}
	cm.evictRetract.enabled.Store(has)
	if ps, ok := cm.peerStorage.(*PeerStorage); ok {
		ps.EnableFailureReports(reports)
	}
}

// startEvictRetractor runs the batched retraction loop.
func (cm *DefaultCacheManager) startEvictRetractor() {
	if cm.config.Coordinator == nil {
		return
	}
	cm.goLoop(func() {
		for {
			select {
			case <-cm.shutdownCtx.Done():
				return
			case <-time.After(jitteredDuration(retractFlushInterval)):
			}
			cm.flushEvictRetractions(cm.shutdownCtx)
		}
	})
}

// flushEvictRetractions sends one retraction per queued path that is still
// not held locally.
func (cm *DefaultCacheManager) flushEvictRetractions(ctx context.Context) {
	del, ok := cm.config.Coordinator.(coordinator.TierLocationDeleter)
	if !ok {
		return
	}
	for _, path := range cm.evictRetract.take() {
		if ctx.Err() != nil {
			return
		}
		if cm.holdsLocally(path) {
			cm.metrics.EvictRetractSkippedRelanded.Add(1)
			continue
		}
		callCtx, cancel := context.WithTimeout(ctx, retractCallTimeout)
		err := del.DeletePeerTierLocation(callCtx, path, cm.config.LocalPeerID, cacheTierToStorageTier(TierNVMe))
		cancel()
		if err != nil {
			cm.metrics.EvictRetractFailed.Add(1)
			continue
		}
		cm.metrics.EvictRetracted.Add(1)
	}
}

// holdsLocally reports whether any local NVMe entry still covers path: the
// whole file, or one of its chunks (a node holding chunks still serves them).
// chunkKeys is the table's per-parent chunk index, so this is not a scan.
func (cm *DefaultCacheManager) holdsLocally(path string) bool {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	if e, ok := cm.entries.get(path); ok && e != nil && e.Tier == TierNVMe {
		return true
	}
	for _, k := range cm.entries.chunkKeys(path) {
		if e, ok := cm.entries.get(k); ok && e != nil && e.Tier == TierNVMe {
			return true
		}
	}
	return false
}

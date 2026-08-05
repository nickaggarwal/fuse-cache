package cache

import (
	"context"
	"sort"
	"time"
)

// Demand-driven replica reconciler — Phase 3 of
// docs/peer-coordination-thundering-herd.md.
//
// A background loop compares the desired replica count against the holder set
// the coordinator knows about, for objects this node holds and that are in
// demand (recently accessed). Under-replicated hot objects are topped up with
// throttled, staggered, busy-aware replication (PeerStorage.ReplicateTo).
//
// Decay of cold objects is intentionally delegated to the existing LRU
// eviction: every replica is write-through backed by cloud, so reclaiming
// capacity is exactly what eviction already does, on actual space pressure
// rather than on a timer. The reconciler only ever adds replicas, and only
// for objects with observed demand, so it cannot fight eviction.
const (
	// reconcileHotWindow: an object counts as "in demand" if accessed within
	// this window. Hysteresis against replicating one-off reads.
	reconcileHotWindow = 10 * time.Minute
	// reconcileMaxObjectBytes bounds how large an object the reconciler will
	// proactively copy; bigger objects propagate via read-promotion and fast
	// chunk advertisement instead.
	reconcileMaxObjectBytes = 64 * 1024 * 1024
	// defaultReconcileMaxPerRun caps replication ops per pass so the
	// reconciler cannot become its own stampede.
	defaultReconcileMaxPerRun = 8
)

// startReplicaReconciler launches the reconcile loop. Called from
// NewCacheManager when ReplicaReconcileInterval > 0.
func (cm *DefaultCacheManager) startReplicaReconciler(ctx context.Context) {
	interval := cm.config.ReplicaReconcileInterval
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				cm.reconcileReplicasOnce(ctx)
			}
		}
	}()
}

// reconcileTarget returns R_desired.
func (cm *DefaultCacheManager) reconcileTarget() int {
	if cm.config.ReplicaReconcileTarget > 0 {
		return cm.config.ReplicaReconcileTarget
	}
	if cm.config.MinPeerReplicas > 0 {
		return cm.config.MinPeerReplicas
	}
	return 3
}

// reconcileCandidate is a hot local object the reconciler may top up.
type reconcileCandidate struct {
	path         string
	size         int64
	lastAccessed time.Time
}

// hotLocalObjects snapshots local entries with recent demand, oldest-access
// last so the hottest get budget first.
func (cm *DefaultCacheManager) hotLocalObjects(now time.Time) []reconcileCandidate {
	cutoff := now.Add(-reconcileHotWindow)
	var out []reconcileCandidate
	cm.mu.RLock()
	for path, entry := range cm.entries {
		if entry == nil || entry.LastAccessed.Before(cutoff) {
			continue
		}
		// Chunk entries propagate with their parent; skip to keep coordinator
		// lookups at parent granularity (metadata is published per parent).
		if _, isChunk := parentFilePathFromChunkPath(path); isChunk {
			continue
		}
		if entry.Size <= 0 || entry.Size > reconcileMaxObjectBytes {
			continue
		}
		out = append(out, reconcileCandidate{path: path, size: entry.Size, lastAccessed: entry.LastAccessed})
	}
	cm.mu.RUnlock()
	// Most-recently-accessed first so the hottest objects get the pass budget
	// (map iteration order is random).
	sort.Slice(out, func(i, j int) bool {
		return out[i].lastAccessed.After(out[j].lastAccessed)
	})
	return out
}

// reconcileReplicasOnce runs one reconcile pass.
func (cm *DefaultCacheManager) reconcileReplicasOnce(ctx context.Context) {
	if cm.config.Coordinator == nil {
		return
	}
	ps, ok := cm.peerStorage.(*PeerStorage)
	if !ok {
		return
	}
	cm.herdStats.reconcileRuns.Add(1)

	budget := cm.config.ReplicaReconcileMaxPerRun
	if budget <= 0 {
		budget = defaultReconcileMaxPerRun
	}

	now := time.Now()
	cm.readerHeat.sweep(now.Add(-reconcileHotWindow))
	for _, cand := range cm.hotLocalObjects(now) {
		if budget <= 0 || ctx.Err() != nil {
			return
		}

		// Heat-proportional target: burst demand raises R per file.
		target := cm.reconcileTargetFor(cand.path, now)
		holders, err := cm.replicaHolders(ctx, cand.path)
		if err != nil {
			continue // coordinator hiccup: try again next pass
		}
		// This node serves reads too even if its own publish hasn't landed.
		holders[cm.config.LocalPeerID] = struct{}{}
		deficit := target - len(holders)
		if deficit <= 0 {
			continue
		}
		if deficit > budget {
			deficit = budget
		}

		entry, err := cm.GetLocal(ctx, cand.path)
		if err != nil || len(entry.Data) == 0 {
			continue
		}

		written, err := ps.ReplicateTo(ctx, cand.path, entry.Data, holders, deficit)
		if written > 0 {
			budget -= written
			cm.herdStats.reconcileReplications.Add(int64(written))
			cm.logger.Printf("Reconciler replicated %s to %d peer(s) (holders %d -> %d, target %d)",
				cand.path, written, len(holders), len(holders)+written, target)
		} else if err != nil && isPeerBusy(err) {
			// Cluster under pressure: source-pressure gate says stop entirely
			// rather than probe more objects this pass.
			cm.herdStats.reconcileSkippedBusy.Add(1)
			return
		}
	}
}

// replicaHolders returns the set of active peers the coordinator believes
// hold path on a local tier (nvme/peer); cloud copies don't count toward R.
func (cm *DefaultCacheManager) replicaHolders(ctx context.Context, path string) (map[string]struct{}, error) {
	callCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	locations, err := cm.config.Coordinator.GetFileLocation(callCtx, path)
	if err != nil {
		return nil, err
	}
	holders := make(map[string]struct{})
	for _, loc := range locations {
		if loc == nil || loc.PeerID == "" {
			continue
		}
		if loc.StorageTier == "cloud" {
			continue
		}
		holders[loc.PeerID] = struct{}{}
	}
	return holders, nil
}

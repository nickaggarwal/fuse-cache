package cache

import (
	"net"
	"sync"
	"time"
)

// Heat-proportional replication.
//
// A fixed replica target starves swarms under burst (Kraken's design lesson:
// peers enforce per-blob connection limits, so a burst of readers against a
// fixed holder set queues on those holders). The peer-serve paths (gRPC
// ReadFile and HTTP /api/peer/read) record each distinct remote reader per
// parent file; the reconciler raises its per-file replica target as the
// distinct-reader count grows and lets the boost decay as readers age out of
// the hot window. Decay of the extra replicas themselves stays delegated to
// LRU eviction, same as base replicas.
const (
	// heatReadersPerReplica: one extra replica per this many distinct remote
	// readers inside the hot window.
	heatReadersPerReplica = 2
	// defaultReconcileMaxTarget bounds the heat-boosted target when
	// ReplicaReconcileMaxTarget is unset.
	defaultReconcileMaxTarget = 8
)

// RemoteReadObserver is implemented by cache managers that track per-file
// remote-reader demand. Serve paths outside this package (the HTTP peer-read
// handler) assert against it.
type RemoteReadObserver interface {
	NoteRemoteReader(path, reader string)
}

// readerHeat tracks distinct remote readers per parent file inside a sliding
// window. All methods are safe for concurrent use.
type readerHeat struct {
	mu     sync.Mutex
	byPath map[string]map[string]time.Time
}

func (h *readerHeat) note(path, reader string, now time.Time) {
	if path == "" || reader == "" {
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.byPath == nil {
		h.byPath = make(map[string]map[string]time.Time)
	}
	readers := h.byPath[path]
	if readers == nil {
		readers = make(map[string]time.Time)
		h.byPath[path] = readers
	}
	readers[reader] = now
}

// distinct returns the number of distinct readers seen since cutoff, pruning
// this path's stale samples as a side effect.
func (h *readerHeat) distinct(path string, cutoff time.Time) int {
	h.mu.Lock()
	defer h.mu.Unlock()
	readers := h.byPath[path]
	for reader, at := range readers {
		if at.Before(cutoff) {
			delete(readers, reader)
		}
	}
	if len(readers) == 0 && readers != nil {
		delete(h.byPath, path)
	}
	return len(readers)
}

// sweep drops every sample older than cutoff. Called once per reconcile pass
// so paths that stopped being read don't accumulate forever.
func (h *readerHeat) sweep(cutoff time.Time) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for path, readers := range h.byPath {
		for reader, at := range readers {
			if at.Before(cutoff) {
				delete(readers, reader)
			}
		}
		if len(readers) == 0 {
			delete(h.byPath, path)
		}
	}
}

// NoteRemoteReader records that reader (a peer address; the host part is
// identity enough) read path from this node. Chunk reads accrue to the parent
// file so heat matches the reconciler's parent-level granularity.
func (cm *DefaultCacheManager) NoteRemoteReader(path, reader string) {
	if parent, isChunk := parentFilePathFromChunkPath(path); isChunk {
		path = parent
	}
	if host, _, err := net.SplitHostPort(reader); err == nil && host != "" {
		reader = host
	}
	cm.readerHeat.note(path, reader, time.Now())
}

// reconcileTargetFor returns the replica target for one file: the configured
// base, plus one replica per heatReadersPerReplica distinct remote readers in
// the hot window, capped at ReplicaReconcileMaxTarget (default 8). A base
// configured above the cap is always respected.
func (cm *DefaultCacheManager) reconcileTargetFor(path string, now time.Time) int {
	base := cm.reconcileTarget()
	extra := cm.readerHeat.distinct(path, now.Add(-reconcileHotWindow)) / heatReadersPerReplica
	if extra == 0 {
		return base
	}
	maxTarget := cm.config.ReplicaReconcileMaxTarget
	if maxTarget <= 0 {
		maxTarget = defaultReconcileMaxTarget
	}
	if base >= maxTarget {
		return base
	}
	target := base + extra
	if target > maxTarget {
		target = maxTarget
	}
	cm.herdStats.reconcileHeatBoosts.Add(1)
	return target
}

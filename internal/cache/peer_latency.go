package cache

import (
	"sort"
	"sync"
	"time"

	"fuse-client/internal/coordinator"
)

// Pairwise peer latency tracking. The coordinator's netprobe gives each peer
// one speed/latency figure measured against a single probe target; the latency
// that actually matters for traversal is per *pair* — this node to that peer.
// PeerStorage records an EWMA of observed read latency/success per peer from
// real transfers and uses it to order candidates, so traversal converges on
// the closest holders and routes around peers whose latency regresses.
// The EWMA weight and the latency floor are shared with the other adaptive
// scorer; see ewma.go.
const (
	// peerPairMinSamples is how many observations a pair needs before its
	// measured latency is trusted over the coordinator's probe estimate.
	peerPairMinSamples = 3
	// peerPairAssumedLatencyMs is the neutral prior for peers with no probe
	// data and no samples.
	peerPairAssumedLatencyMs = 5.0
)

// PeerPairLatency is the exported per-peer view for metrics/metadata.
type PeerPairLatency struct {
	PeerID    string  `json:"peer_id"`
	LatencyMs float64 `json:"latency_ms"`
	Success   float64 `json:"success_ratio"`
	Samples   int64   `json:"samples"`
}

// peerLatencyTracker holds this node's observed latency to each peer.
type peerLatencyTracker struct {
	mu    sync.RWMutex
	stats map[string]*ewmaStat
}

func newPeerLatencyTracker() *peerLatencyTracker {
	return &peerLatencyTracker{stats: make(map[string]*ewmaStat)}
}

// record folds one observed transfer into the pair's EWMA. Busy rejections
// must not be recorded — they are admission control, not network signal.
func (t *peerLatencyTracker) record(peerID string, latency time.Duration, ok bool) {
	if t == nil || peerID == "" {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	s := t.stats[peerID]
	if s == nil {
		s = &ewmaStat{}
		t.stats[peerID] = s
	}
	s.observe(latency, ok)
}

// measured returns the pair's EWMA latency/success and whether enough samples
// exist to trust it.
func (t *peerLatencyTracker) measured(peerID string) (latencyMs, success float64, ok bool) {
	if t == nil {
		return 0, 0, false
	}
	t.mu.RLock()
	defer t.mu.RUnlock()
	s := t.stats[peerID]
	if s == nil || s.samples < peerPairMinSamples {
		return 0, 0, false
	}
	return s.ewmaLatencyMs, s.ewmaSuccess, true
}

// snapshot returns all pairs for metrics exposure, sorted by peer ID for
// stable output.
func (t *peerLatencyTracker) snapshot() []PeerPairLatency {
	if t == nil {
		return nil
	}
	t.mu.RLock()
	out := make([]PeerPairLatency, 0, len(t.stats))
	for id, s := range t.stats {
		out = append(out, PeerPairLatency{
			PeerID:    id,
			LatencyMs: s.ewmaLatencyMs,
			Success:   s.ewmaSuccess,
			Samples:   s.samples,
		})
	}
	t.mu.RUnlock()
	sort.Slice(out, func(i, j int) bool { return out[i].PeerID < out[j].PeerID })
	return out
}

// pairScore returns a higher-is-better traversal score for a candidate:
// observed success over observed latency when the pair has enough samples,
// falling back to the coordinator's probe latency (success prior 1.0), then a
// neutral assumed latency. Scores stay on one scale (1/ms) so measured and
// estimated peers sort together.
func (ps *PeerStorage) pairScore(peer *coordinator.PeerInfo) float64 {
	if peer == nil {
		return -1
	}
	latencyMs, success, ok := ps.pairLatency.measured(peer.ID)
	if !ok {
		latencyMs = peer.NetworkLatencyMs
		if latencyMs <= 0 {
			latencyMs = peerPairAssumedLatencyMs
		}
		success = 1.0
	}
	// Estimated peers get scored through the same fold as measured ones, so
	// the two stay on one 1/ms scale and sort together.
	est := ewmaStat{ewmaLatencyMs: latencyMs}
	return est.scoreWith(success)
}

// hasMeasuredPair reports whether any candidate has trusted pair samples.
func (ps *PeerStorage) hasMeasuredPair(peers []*coordinator.PeerInfo) bool {
	for _, p := range peers {
		if p == nil {
			continue
		}
		if _, _, ok := ps.pairLatency.measured(p.ID); ok {
			return true
		}
	}
	return false
}

// sortByObservedLatency reorders candidates by measured pair latency, best
// first. It is a no-op until at least one candidate has enough samples, so
// cold-start traversal keeps the probe/hash ordering (and its deterministic
// chunk spread) untouched.
func (ps *PeerStorage) sortByObservedLatency(peers []*coordinator.PeerInfo, key string) {
	if len(peers) <= 1 || !ps.hasMeasuredPair(peers) {
		return
	}
	sort.SliceStable(peers, func(i, j int) bool {
		si := ps.pairScore(peers[i])
		sj := ps.pairScore(peers[j])
		if si == sj {
			// Deterministic tie break keeps chunk distribution stable.
			hi := peerStartIndexForKey(key+"#"+peers[i].ID, 1<<30)
			hj := peerStartIndexForKey(key+"#"+peers[j].ID, 1<<30)
			return hi < hj
		}
		return si > sj
	})
}

// PeerPairLatencies exposes the observed per-peer latency table (this node →
// each peer) for metrics and debugging. Empty when peer storage is not
// configured.
func (cm *DefaultCacheManager) PeerPairLatencies() []PeerPairLatency {
	if ps, ok := cm.peerStorage.(*PeerStorage); ok {
		return ps.pairLatency.snapshot()
	}
	return nil
}

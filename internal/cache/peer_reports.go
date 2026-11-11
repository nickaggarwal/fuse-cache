package cache

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"fuse-client/internal/coordinator"
)

// Client side of peer failure reports (coordinator/peer_health.go).
//
// A transfer failure (not a miss, not "busy") against a peer is counted here.
// Once a peer has failed failureReportThreshold times within
// failureReportWindow, this node reports it to the coordinator, at most once
// per report interval per target. The coordinator marks a peer suspect only
// when enough distinct nodes report it, and GetPeers carries the flag back;
// readers then try suspect peers after every healthy holder.
//
// Advisory both ways: a failed report is dropped, and a coordinator without
// the "failure-report" capability is never sent one.

const (
	failureReportThreshold = 3
	failureReportWindow    = 30 * time.Second
	// failureReportEvery spaces repeat reports about one target; well under
	// the coordinator's 60 s report TTL so a still-failing peer stays
	// reported.
	failureReportEvery = 20 * time.Second
)

type peerFailureReporter struct {
	mu       sync.Mutex
	fails    map[string][]time.Time // target -> recent failure times
	lastSent map[string]time.Time
	enabled  atomic.Bool
	sent     atomic.Int64
}

// noteFailure records a transfer failure against peerID and returns true when
// a report should be sent now.
func (r *peerFailureReporter) noteFailure(peerID string, now time.Time) bool {
	if peerID == "" || !r.enabled.Load() {
		return false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.fails == nil {
		r.fails = map[string][]time.Time{}
		r.lastSent = map[string]time.Time{}
	}
	cut := now.Add(-failureReportWindow)
	kept := r.fails[peerID][:0]
	for _, t := range r.fails[peerID] {
		if t.After(cut) {
			kept = append(kept, t)
		}
	}
	kept = append(kept, now)
	r.fails[peerID] = kept
	if len(kept) < failureReportThreshold || now.Sub(r.lastSent[peerID]) < failureReportEvery {
		return false
	}
	r.lastSent[peerID] = now
	return true
}

// noteSuccess clears peerID's failure history.
func (r *peerFailureReporter) noteSuccess(peerID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.fails, peerID)
}

// recordPeerOutcome feeds a transfer outcome against peer into the reporter.
func (ps *PeerStorage) recordPeerOutcome(peer *coordinator.PeerInfo, err error) {
	if peer == nil {
		return
	}
	if err == nil {
		ps.failReports.noteSuccess(peer.ID)
		return
	}
	if !ps.failReports.noteFailure(peer.ID, time.Now()) {
		return
	}
	fr, ok := ps.coordinator.(coordinator.FailureReporter)
	if !ok || ps.localPeerID == "" {
		return
	}
	target := peer.ID
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if fr.ReportPeerFailure(ctx, target, ps.localPeerID) == nil {
			ps.failReports.sent.Add(1)
		}
	}()
}

// EnableFailureReports turns reporting on once the coordinator advertises
// "failure-report".
func (ps *PeerStorage) EnableFailureReports(on bool) { ps.failReports.enabled.Store(on) }

// FailureReportsSent counts reports this node sent.
func (ps *PeerStorage) FailureReportsSent() int64 { return ps.failReports.sent.Load() }

// suspectLast moves suspect peers behind every non-suspect one, keeping each
// group's order.
func suspectLast(peers []*coordinator.PeerInfo) []*coordinator.PeerInfo {
	n := 0
	for _, p := range peers {
		if p != nil && p.Suspect {
			n++
		}
	}
	if n == 0 || n == len(peers) {
		return peers
	}
	out := make([]*coordinator.PeerInfo, 0, len(peers))
	for _, p := range peers {
		if p == nil || !p.Suspect {
			out = append(out, p)
		}
	}
	for _, p := range peers {
		if p != nil && p.Suspect {
			out = append(out, p)
		}
	}
	return out
}

// PeerFailureReportsSent counts failure reports this node sent; -1 without
// peer storage.
func (cm *DefaultCacheManager) PeerFailureReportsSent() int64 {
	if ps, ok := cm.peerStorage.(*PeerStorage); ok {
		return ps.FailureReportsSent()
	}
	return -1
}

package cache

import (
	"context"
	"crypto/rand"
	"math/big"
	"sync/atomic"
	"time"
)

// Serve-side admission control and requester backoff tuning
// (docs/peer-coordination-thundering-herd.md, Phase 1).
const (
	// defaultPeerServeMaxInflight bounds concurrent peer serves (gRPC ReadFile/
	// WriteFile and the raw HTTP bulk-read endpoint) per node. Beyond the cap the
	// server rejects with RESOURCE_EXHAUSTED / 503 instead of queueing
	// unboundedly, so an origin under stampede degrades by shedding load rather
	// than collapsing.
	defaultPeerServeMaxInflight = 64
	// peerBusyRetryMinWait / peerBusyRetryMaxWait bound the randomized jitter a
	// requester sleeps before re-trying a peer that signalled "busy". The spread
	// de-synchronizes retries across the herd.
	peerBusyRetryMinWait = 10 * time.Millisecond
	peerBusyRetryMaxWait = 150 * time.Millisecond
	// defaultPeerReplicationStagger is the base delay inserted between successive
	// replica writes for one object, jittered to [0.5x, 1.5x]. Staggering keeps
	// write-time replication itself from stampeding the network.
	defaultPeerReplicationStagger = 25 * time.Millisecond
)

// peerServeGate is a semaphore bounding concurrent peer serves on this node.
// It is the serve-side flow control from the thundering-herd plan: a full gate
// makes the server answer "busy" immediately so requesters fail over to another
// holder (or retry with jitter) instead of piling up on a saturated source.
//
// A nil gate admits everything, so callers never need a nil check.
type peerServeGate struct {
	slots    chan struct{}
	inflight atomic.Int64
	accepted atomic.Int64
	rejected atomic.Int64
}

func newPeerServeGate(capacity int) *peerServeGate {
	if capacity <= 0 {
		capacity = defaultPeerServeMaxInflight
	}
	return &peerServeGate{slots: make(chan struct{}, capacity)}
}

// tryAcquire attempts to take a serve slot without blocking. On success it
// returns a release func that must be called exactly once when the serve ends.
func (g *peerServeGate) tryAcquire() (release func(), ok bool) {
	if g == nil {
		return func() {}, true
	}
	select {
	case g.slots <- struct{}{}:
		g.inflight.Add(1)
		g.accepted.Add(1)
		var released atomic.Bool
		return func() {
			if released.CompareAndSwap(false, true) {
				g.inflight.Add(-1)
				<-g.slots
			}
		}, true
	default:
		g.rejected.Add(1)
		return nil, false
	}
}

func (g *peerServeGate) capacity() int64 {
	if g == nil {
		return 0
	}
	return int64(cap(g.slots))
}

// PeerLoadSnapshot reports serve-side admission and requester backoff counters
// for metrics exposure.
type PeerLoadSnapshot struct {
	ServeInflight        int64
	ServeCapacity        int64
	ServeAcceptedTotal   int64
	ServeRejectedTotal   int64
	FetchBusySkips       int64
	FetchMissSkips       int64
	FetchJitterRetries   int64
	ReplicationBusySkips int64
	ReplicationStaggers  int64
}

// PeerServeAdmitter is implemented by cache managers that bound concurrent
// peer serves. Transport layers (gRPC peer server, raw HTTP peer-read handler)
// consult it before doing any work for a remote peer.
type PeerServeAdmitter interface {
	TryAcquirePeerServe() (release func(), ok bool)
}

// TryAcquirePeerServe implements PeerServeAdmitter.
func (cm *DefaultCacheManager) TryAcquirePeerServe() (release func(), ok bool) {
	return cm.peerServeGate.tryAcquire()
}

// PeerLoadSnapshot returns the current admission/backoff counters. Safe on a
// manager without a gate or peer storage (all-zero snapshot).
func (cm *DefaultCacheManager) PeerLoadSnapshot() PeerLoadSnapshot {
	var snap PeerLoadSnapshot
	if g := cm.peerServeGate; g != nil {
		snap.ServeInflight = g.inflight.Load()
		snap.ServeCapacity = g.capacity()
		snap.ServeAcceptedTotal = g.accepted.Load()
		snap.ServeRejectedTotal = g.rejected.Load()
	}
	if ps, ok := cm.peerStorage.(*PeerStorage); ok {
		snap.FetchBusySkips = ps.busySkipsTotal.Load()
		snap.FetchMissSkips = ps.missSkipsTotal.Load()
		snap.FetchJitterRetries = ps.jitterRetriesTotal.Load()
		snap.ReplicationBusySkips = ps.replBusySkipsTotal.Load()
		snap.ReplicationStaggers = ps.replStaggersTotal.Load()
	}
	return snap
}

// jitterDuration returns a uniformly random duration in [min, max], using
// crypto/rand per project convention. Falls back to min on rand failure.
func jitterDuration(min, max time.Duration) time.Duration {
	if max <= min {
		return min
	}
	n, err := rand.Int(rand.Reader, big.NewInt(int64(max-min)))
	if err != nil {
		return min
	}
	return min + time.Duration(n.Int64())
}

// sleepWithJitter sleeps a random duration in [min, max] or returns early with
// the context's error when it is cancelled.
func sleepWithJitter(ctx context.Context, min, max time.Duration) error {
	d := jitterDuration(min, max)
	if d <= 0 {
		return ctx.Err()
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

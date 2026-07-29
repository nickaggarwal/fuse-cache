package cache

import (
	"context"
	"sync"
	"testing"
	"time"

	"fuse-client/internal/coordinator"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestPeerServeGate_AdmitsUpToCapacity(t *testing.T) {
	g := newPeerServeGate(2)

	rel1, ok := g.tryAcquire()
	if !ok {
		t.Fatal("first acquire should succeed")
	}
	rel2, ok := g.tryAcquire()
	if !ok {
		t.Fatal("second acquire should succeed")
	}
	if _, ok := g.tryAcquire(); ok {
		t.Fatal("third acquire should be rejected at capacity 2")
	}
	if got := g.inflight.Load(); got != 2 {
		t.Fatalf("inflight = %d, want 2", got)
	}
	if got := g.rejected.Load(); got != 1 {
		t.Fatalf("rejected = %d, want 1", got)
	}

	rel1()
	if rel3, ok := g.tryAcquire(); !ok {
		t.Fatal("acquire after release should succeed")
	} else {
		rel3()
	}
	rel2()
	if got := g.inflight.Load(); got != 0 {
		t.Fatalf("inflight after all releases = %d, want 0", got)
	}
}

func TestPeerServeGate_ReleaseIsIdempotent(t *testing.T) {
	g := newPeerServeGate(1)
	rel, ok := g.tryAcquire()
	if !ok {
		t.Fatal("acquire should succeed")
	}
	rel()
	rel() // double release must not free a second slot or underflow

	if got := g.inflight.Load(); got != 0 {
		t.Fatalf("inflight = %d, want 0", got)
	}
	rel2, ok := g.tryAcquire()
	if !ok {
		t.Fatal("acquire after release should succeed")
	}
	defer rel2()
	if _, ok := g.tryAcquire(); ok {
		t.Fatal("gate must still enforce capacity 1 after a double release")
	}
}

func TestPeerServeGate_NilAdmitsEverything(t *testing.T) {
	var g *peerServeGate
	for i := 0; i < 100; i++ {
		rel, ok := g.tryAcquire()
		if !ok {
			t.Fatal("nil gate must admit everything")
		}
		rel()
	}
}

func TestPeerServeGate_DefaultCapacity(t *testing.T) {
	g := newPeerServeGate(0)
	if got := g.capacity(); got != defaultPeerServeMaxInflight {
		t.Fatalf("capacity = %d, want default %d", got, defaultPeerServeMaxInflight)
	}
}

func TestPeerServeGate_ConcurrentAcquireRelease(t *testing.T) {
	g := newPeerServeGate(8)
	var wg sync.WaitGroup
	for i := 0; i < 64; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				if rel, ok := g.tryAcquire(); ok {
					rel()
				}
			}
		}()
	}
	wg.Wait()
	if got := g.inflight.Load(); got != 0 {
		t.Fatalf("inflight after churn = %d, want 0", got)
	}
}

func TestIsPeerBusy(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"raw busy", &peerBusyError{addr: "10.0.0.1:8081"}, true},
		{"grpc resource exhausted", status.Error(codes.ResourceExhausted, "full"), true},
		{"grpc unavailable", status.Error(codes.Unavailable, "down"), false},
		{"plain error", context.DeadlineExceeded, false},
	}
	for _, tc := range cases {
		if got := isPeerBusy(tc.err); got != tc.want {
			t.Errorf("isPeerBusy(%s) = %v, want %v", tc.name, got, tc.want)
		}
	}
}

func TestJitterDuration_WithinBounds(t *testing.T) {
	min, max := 10*time.Millisecond, 150*time.Millisecond
	for i := 0; i < 200; i++ {
		d := jitterDuration(min, max)
		if d < min || d > max {
			t.Fatalf("jitter %v out of [%v, %v]", d, min, max)
		}
	}
	if got := jitterDuration(max, min); got != max {
		t.Fatalf("inverted bounds should return min arg, got %v", got)
	}
}

func TestSleepWithJitter_CancelledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	start := time.Now()
	if err := sleepWithJitter(ctx, time.Second, 2*time.Second); err == nil {
		t.Fatal("expected context error")
	}
	if elapsed := time.Since(start); elapsed > 500*time.Millisecond {
		t.Fatalf("cancelled sleep took %v, should return immediately", elapsed)
	}
}

func TestPeerLoadSnapshot_ReportsGateAndStorageCounters(t *testing.T) {
	cm := newTestCacheManager()
	cm.peerServeGate = newPeerServeGate(3)
	ps := &PeerStorage{}
	ps.busySkipsTotal.Store(4)
	ps.jitterRetriesTotal.Store(2)
	ps.replBusySkipsTotal.Store(1)
	ps.replStaggersTotal.Store(7)
	cm.peerStorage = ps

	rel, ok := cm.TryAcquirePeerServe()
	if !ok {
		t.Fatal("acquire via manager should succeed")
	}
	defer rel()

	snap := cm.PeerLoadSnapshot()
	if snap.ServeInflight != 1 || snap.ServeCapacity != 3 {
		t.Fatalf("inflight/capacity = %d/%d, want 1/3", snap.ServeInflight, snap.ServeCapacity)
	}
	if snap.ServeAcceptedTotal != 1 || snap.ServeRejectedTotal != 0 {
		t.Fatalf("accepted/rejected = %d/%d, want 1/0", snap.ServeAcceptedTotal, snap.ServeRejectedTotal)
	}
	if snap.FetchBusySkips != 4 || snap.FetchJitterRetries != 2 ||
		snap.ReplicationBusySkips != 1 || snap.ReplicationStaggers != 7 {
		t.Fatalf("storage counters = %+v", snap)
	}
}

func TestPeerReplicationScore_PrefersHeadroom(t *testing.T) {
	roomy := &coordinator.PeerInfo{ID: "roomy", AvailableSpace: 100 << 30, NetworkSpeedMBps: 200, NetworkLatencyMs: 2}
	tight := &coordinator.PeerInfo{ID: "tight", AvailableSpace: 1 << 30, NetworkSpeedMBps: 200, NetworkLatencyMs: 2}
	fastButFull := &coordinator.PeerInfo{ID: "fast-full", AvailableSpace: 1 << 30, NetworkSpeedMBps: 1000, NetworkLatencyMs: 1}

	peers := []*coordinator.PeerInfo{tight, fastButFull, roomy}
	sortPeersByReplicationScore(peers)
	if peers[0].ID != "roomy" {
		t.Fatalf("first replication target = %s, want roomy", peers[0].ID)
	}
	if peers[len(peers)-1].ID != "tight" {
		t.Fatalf("last replication target = %s, want tight", peers[len(peers)-1].ID)
	}
}

func TestSortPeersByReplicationScore_NeutralWithoutSignals(t *testing.T) {
	// With no probe/space data every peer scores the same, so the stable sort
	// must preserve the (shuffled) input order.
	peers := []*coordinator.PeerInfo{{ID: "a"}, {ID: "b"}, {ID: "c"}}
	sortPeersByReplicationScore(peers)
	for i, want := range []string{"a", "b", "c"} {
		if peers[i].ID != want {
			t.Fatalf("order changed at %d: got %s, want %s", i, peers[i].ID, want)
		}
	}
}

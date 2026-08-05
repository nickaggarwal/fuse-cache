package cache

import (
	"context"
	"testing"
	"time"

	"fuse-client/internal/coordinator"
)

func newLatencyTestPeerStorage(t *testing.T) *PeerStorage {
	t.Helper()
	ps, err := NewPeerStorage(nil, 5*time.Second, "local", false, false, false, "")
	if err != nil {
		t.Fatalf("NewPeerStorage: %v", err)
	}
	return ps
}

// samplePair feeds n identical samples so the pair crosses the trust threshold.
func samplePair(ps *PeerStorage, peerID string, latency time.Duration, ok bool, n int) {
	for i := 0; i < n; i++ {
		ps.pairLatency.record(peerID, latency, ok)
	}
}

func TestPeerLatencyTracker_EWMAAndTrustThreshold(t *testing.T) {
	tr := newPeerLatencyTracker()

	// Below the sample threshold the measurement is not trusted.
	tr.record("p1", 10*time.Millisecond, true)
	tr.record("p1", 10*time.Millisecond, true)
	if _, _, ok := tr.measured("p1"); ok {
		t.Fatal("pair should not be trusted below min samples")
	}

	tr.record("p1", 10*time.Millisecond, true)
	lat, success, ok := tr.measured("p1")
	if !ok {
		t.Fatal("pair should be trusted at min samples")
	}
	if lat < 9 || lat > 11 {
		t.Fatalf("latency = %.2fms, want ~10ms", lat)
	}
	if success != 1.0 {
		t.Fatalf("success = %.2f, want 1.0", success)
	}

	// Unknown peers are never trusted.
	if _, _, ok := tr.measured("unknown"); ok {
		t.Fatal("unknown pair must not be trusted")
	}
}

func TestSortByObservedLatency_PrefersMeasuredCloserPeer(t *testing.T) {
	ps := newLatencyTestPeerStorage(t)
	defer ps.Close()

	// Coordinator probe data says "far" is best, but observed transfers say
	// "near" is 10x closer.
	near := &coordinator.PeerInfo{ID: "near", NetworkLatencyMs: 20}
	far := &coordinator.PeerInfo{ID: "far", NetworkLatencyMs: 1}
	samplePair(ps, "near", 2*time.Millisecond, true, 5)
	samplePair(ps, "far", 20*time.Millisecond, true, 5)

	peers := []*coordinator.PeerInfo{far, near}
	ps.sortByObservedLatency(peers, "/data.bin_chunk_0")
	if peers[0].ID != "near" {
		t.Fatalf("first candidate = %s, want near (measured latency must beat probe estimate)", peers[0].ID)
	}
}

// TestSortByObservedLatency_LatencyRegression is the latency-regression test:
// a peer that was fastest degrades (e.g. saturated NIC, cross-zone reroute).
// As new, slower samples fold into its EWMA, traversal must reorder away from
// it — the regressed peer may not stay the preferred candidate.
func TestSortByObservedLatency_LatencyRegression(t *testing.T) {
	ps := newLatencyTestPeerStorage(t)
	defer ps.Close()

	fast := &coordinator.PeerInfo{ID: "was-fast"}
	steady := &coordinator.PeerInfo{ID: "steady"}
	samplePair(ps, "was-fast", 1*time.Millisecond, true, 5)
	samplePair(ps, "steady", 4*time.Millisecond, true, 5)

	peers := []*coordinator.PeerInfo{steady, fast}
	ps.sortByObservedLatency(peers, "/data.bin_chunk_0")
	if peers[0].ID != "was-fast" {
		t.Fatalf("baseline: first candidate = %s, want was-fast", peers[0].ID)
	}

	// Regression: the fast peer's latency degrades to 50ms. The EWMA (alpha
	// 0.3) needs a handful of samples to cross steady's 4ms.
	samplePair(ps, "was-fast", 50*time.Millisecond, true, 10)

	peers = []*coordinator.PeerInfo{steady, fast}
	ps.sortByObservedLatency(peers, "/data.bin_chunk_0")
	if peers[0].ID != "steady" {
		lat, _, _ := ps.pairLatency.measured("was-fast")
		t.Fatalf("after regression: first candidate = %s (was-fast EWMA %.1fms), want steady", peers[0].ID, lat)
	}

	// Recovery: latency comes back down and the peer earns its spot back.
	samplePair(ps, "was-fast", 1*time.Millisecond, true, 20)
	peers = []*coordinator.PeerInfo{steady, fast}
	ps.sortByObservedLatency(peers, "/data.bin_chunk_0")
	if peers[0].ID != "was-fast" {
		t.Fatalf("after recovery: first candidate = %s, want was-fast", peers[0].ID)
	}
}

// TestSortByObservedLatency_FailuresDemotePeer verifies success ratio weighs
// into traversal: a low-latency peer that keeps failing loses its lead.
func TestSortByObservedLatency_FailuresDemotePeer(t *testing.T) {
	ps := newLatencyTestPeerStorage(t)
	defer ps.Close()

	flaky := &coordinator.PeerInfo{ID: "flaky"}
	reliable := &coordinator.PeerInfo{ID: "reliable"}
	samplePair(ps, "flaky", 2*time.Millisecond, false, 8) // fast but failing
	samplePair(ps, "reliable", 6*time.Millisecond, true, 8)

	peers := []*coordinator.PeerInfo{flaky, reliable}
	ps.sortByObservedLatency(peers, "/x_chunk_1")
	if peers[0].ID != "reliable" {
		t.Fatalf("first candidate = %s, want reliable", peers[0].ID)
	}
}

func TestSortByObservedLatency_NoSamplesKeepsOrder(t *testing.T) {
	ps := newLatencyTestPeerStorage(t)
	defer ps.Close()

	peers := []*coordinator.PeerInfo{{ID: "a"}, {ID: "b"}, {ID: "c"}}
	ps.sortByObservedLatency(peers, "/data.bin_chunk_0")
	for i, want := range []string{"a", "b", "c"} {
		if peers[i].ID != want {
			t.Fatalf("cold-start order changed at %d: got %s, want %s", i, peers[i].ID, want)
		}
	}
}

func TestPeerPairLatencies_SnapshotSortedAndPopulated(t *testing.T) {
	cm := newTestCacheManager()
	ps := newLatencyTestPeerStorage(t)
	defer ps.Close()
	cm.peerStorage = ps

	samplePair(ps, "zed", 3*time.Millisecond, true, 4)
	samplePair(ps, "alpha", 9*time.Millisecond, true, 4)

	pairs := cm.PeerPairLatencies()
	if len(pairs) != 2 {
		t.Fatalf("pairs = %d, want 2", len(pairs))
	}
	if pairs[0].PeerID != "alpha" || pairs[1].PeerID != "zed" {
		t.Fatalf("snapshot not sorted by peer ID: %+v", pairs)
	}
	if pairs[0].Samples != 4 || pairs[0].LatencyMs <= 0 {
		t.Fatalf("alpha pair not populated: %+v", pairs[0])
	}
}

// TestReadPeerData_MissDoesNotPoisonSuccessEWMA is the regression test for the
// stargz-test finding: every pair reported success_ratio ~= 0 with 47-140
// samples, so pairScore (success/latencyMs) was ~0 for all peers and the
// latency ordering never took effect. The cause was that traversal probes
// holders in order — most attempts miss by construction — and misses were
// folded into the success EWMA.
func TestReadPeerData_MissDoesNotPoisonSuccessEWMA(t *testing.T) {
	holderAddr, _, stopHolder := startHerdPeer(t, 8, map[string][]byte{"/held.bin": []byte("payload")})
	defer stopHolder()
	// An empty peer: reachable and healthy, just doesn't have the object.
	emptyAddr, _, stopEmpty := startHerdPeer(t, 8, nil)
	defer stopEmpty()

	coord := &herdCoordinator{}
	ps := newHerdPeerStorage(t, coord)
	defer ps.Close()

	holder := &coordinator.PeerInfo{ID: "holder", GRPCAddress: holderAddr, Status: "active"}
	empty := &coordinator.PeerInfo{ID: "empty", GRPCAddress: emptyAddr, Status: "active"}
	ctx := context.Background()

	for i := 0; i < 5; i++ {
		if _, err := ps.readPeerData(ctx, empty, "/held.bin"); err == nil {
			t.Fatal("read from non-holder should fail")
		} else if !isPeerMiss(err) {
			t.Fatalf("non-holder error not classified as a miss: %v", err)
		}
		if _, err := ps.readPeerData(ctx, holder, "/held.bin"); err != nil {
			t.Fatalf("read from holder: %v", err)
		}
	}

	// The empty peer answered every request correctly, so it has no network
	// signal at all — not a run of recorded failures.
	if _, _, ok := ps.pairLatency.measured("empty"); ok {
		t.Fatal("misses must not accumulate samples for the peer")
	}
	if got := ps.missSkipsTotal.Load(); got != 5 {
		t.Fatalf("missSkipsTotal = %d, want 5", got)
	}

	_, success, ok := ps.pairLatency.measured("holder")
	if !ok {
		t.Fatal("holder should have trusted samples after 5 reads")
	}
	if success < 0.99 {
		t.Fatalf("holder success = %.4f, want ~1.0 (the live bug drove this to ~0)", success)
	}

	// With the holder scoring on real signal and the empty peer falling back
	// to the neutral prior, traversal must still order sensibly.
	if ps.pairScore(holder) <= 0 {
		t.Fatalf("holder pairScore = %.6f, want > 0", ps.pairScore(holder))
	}
}

// TestReadPeerData_TransferFailureStillRecorded guards the other side of the
// classification: a genuine transport failure must still lower the pair's
// success ratio, or the EWMA stops routing around bad links.
func TestReadPeerData_TransferFailureStillRecorded(t *testing.T) {
	coord := &herdCoordinator{}
	ps := newHerdPeerStorage(t, coord)
	defer ps.Close()

	// Port 1 on loopback: dial fails, which is a real failure, not a miss.
	dead := &coordinator.PeerInfo{ID: "dead", GRPCAddress: "127.0.0.1:1", Status: "active"}
	ctx := context.Background()
	for i := 0; i < 4; i++ {
		if _, err := ps.readPeerData(ctx, dead, "/x.bin"); err == nil {
			t.Fatal("read from dead peer should fail")
		}
	}
	_, success, ok := ps.pairLatency.measured("dead")
	if !ok {
		t.Fatal("failures should still accumulate samples")
	}
	if success > 0.01 {
		t.Fatalf("dead peer success = %.4f, want ~0", success)
	}
}

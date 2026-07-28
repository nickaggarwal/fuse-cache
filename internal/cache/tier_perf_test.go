package cache

import (
	"testing"
	"time"
)

func recordN(t *tierPerfTracker, tier CacheTier, n int, latency time.Duration, ok bool) {
	for i := 0; i < n; i++ {
		t.record(tier, latency, ok)
	}
}

func TestTierPerf_ColdStartIsPeerFirst(t *testing.T) {
	tr := newTierPerfTracker()
	// Not enough samples yet.
	tr.record(TierPeer, 2*time.Millisecond, true)
	tr.record(TierCloud, 50*time.Millisecond, true)
	order := tr.order()
	if len(order) != 2 || order[0] != TierPeer || order[1] != TierCloud {
		t.Fatalf("cold-start order = %v, want peer-first", order)
	}
	if _, ok := tr.shouldHedge(); ok {
		t.Fatal("shouldHedge should report ok=false before min samples")
	}
}

func TestTierPerf_FastReliablePeerWins(t *testing.T) {
	tr := newTierPerfTracker()
	recordN(tr, TierPeer, 8, 2*time.Millisecond, true)
	recordN(tr, TierCloud, 8, 60*time.Millisecond, true)
	order := tr.order()
	if order[0] != TierPeer {
		t.Fatalf("order[0] = %v, want peer (fast+reliable)", order[0])
	}
	// Clear reliable winner, far apart → no hedge.
	hedge, ok := tr.shouldHedge()
	if !ok {
		t.Fatal("shouldHedge ok=false, want true with enough samples")
	}
	if hedge {
		t.Fatal("expected no hedge for a clear reliable winner")
	}
}

func TestTierPerf_FailingPeerDemotedAndSurfaced(t *testing.T) {
	tr := newTierPerfTracker()
	// Peer keeps failing (the peer=0% scenario); cloud succeeds slowly.
	recordN(tr, TierPeer, 8, 5*time.Millisecond, false)
	recordN(tr, TierCloud, 8, 60*time.Millisecond, true)

	order := tr.order()
	if order[0] != TierCloud {
		t.Fatalf("order[0] = %v, want cloud (peer failing)", order[0])
	}
	// Cloud is now the reliable primary; racing the broken peer would be pure
	// waste, so the efficient choice is cloud-alone (no hedge). The failure is
	// still surfaced through the per-tier success metric below.
	hedge, ok := tr.shouldHedge()
	if !ok {
		t.Fatal("shouldHedge ok=false, want true with enough samples")
	}
	if hedge {
		t.Fatal("expected no hedge: cloud is the reliable primary, peer is broken")
	}
	_, peerSucc, _, _, cloudSucc, _ := tr.snapshot()
	if peerSucc > 0.2 {
		t.Fatalf("peer success = %.2f, want near 0 (failing)", peerSucc)
	}
	if cloudSucc < 0.8 {
		t.Fatalf("cloud success = %.2f, want near 1", cloudSucc)
	}
}

func TestTierPerf_CloseRaceHedges(t *testing.T) {
	tr := newTierPerfTracker()
	recordN(tr, TierPeer, 8, 10*time.Millisecond, true)
	recordN(tr, TierCloud, 8, 11*time.Millisecond, true)
	hedge, ok := tr.shouldHedge()
	if !ok || !hedge {
		t.Fatalf("shouldHedge = (%t,%t), want hedge=true for close race", hedge, ok)
	}
}

func TestTierPerf_StaleTierReprobed(t *testing.T) {
	tr := newTierPerfTracker()
	// Peer failed a while ago; cloud is current and reliable.
	recordN(tr, TierPeer, 5, 5*time.Millisecond, false)
	recordN(tr, TierCloud, 5, 40*time.Millisecond, true)
	// Force peer's last sample into the past beyond the recovery window.
	tr.mu.Lock()
	tr.peer.lastSampleAt = time.Now().Add(-2 * tierPerfRecoveryWindow)
	tr.mu.Unlock()

	// With optimism-under-uncertainty, stale failing peer's score should be
	// lifted enough to be re-probed ahead of a slow cloud.
	order := tr.order()
	if order[0] != TierPeer {
		t.Fatalf("order[0] = %v, want peer re-promoted for re-probe after staleness", order[0])
	}
}

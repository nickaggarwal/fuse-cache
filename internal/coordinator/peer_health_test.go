package coordinator

import (
	"context"
	"testing"
	"time"
)

// One reporter is not enough to mark a peer suspect; two distinct ones are.
// Self-reports and repeats from the same reporter do not count.
func TestSuspectNeedsDistinctReporters(t *testing.T) {
	ctx := context.Background()
	cs := NewCoordinatorServiceWithStore(NewInMemoryStore())
	for _, id := range []string{"a", "b", "c"} {
		if err := cs.RegisterPeer(ctx, &PeerInfo{ID: id, Address: id + ":1"}); err != nil {
			t.Fatal(err)
		}
	}
	suspect := func() bool {
		peers, _ := cs.GetPeers(ctx, "")
		for _, p := range peers {
			if p.ID == "c" {
				return p.Suspect
			}
		}
		t.Fatal("c not listed")
		return false
	}
	_ = cs.ReportFailure(ctx, "c", "a")
	_ = cs.ReportFailure(ctx, "c", "a")
	_ = cs.ReportFailure(ctx, "c", "c")
	if suspect() {
		t.Fatal("one reporter marked c suspect")
	}
	_ = cs.ReportFailure(ctx, "c", "b")
	if !suspect() {
		t.Fatal("two distinct reporters did not mark c suspect")
	}
	if h := cs.PeerHealth(ctx); h.Suspect["c"] != 2 || h.MinReporters != DefaultMinFailureReporters {
		t.Fatalf("peer health = %+v", h)
	}
	_ = cs.ReportFailure(ctx, "ghost", "a")
	if h := cs.PeerHealth(ctx); len(h.Reporters["ghost"]) != 0 {
		t.Fatal("report about an unknown peer stored")
	}
}

func TestClusterFlagsExpire(t *testing.T) {
	ctx := context.Background()
	cs := NewCoordinatorServiceWithStore(NewInMemoryStore())
	if err := cs.SetFlag(ctx, FlagNoRebalance, time.Hour); err != nil {
		t.Fatal(err)
	}
	if f := cs.Flags(ctx); len(f) != 1 || f[0] != FlagNoRebalance {
		t.Fatalf("flags = %v", f)
	}
	_ = cs.SetFlag(ctx, FlagNoRebalance, 0)
	if f := cs.Flags(ctx); len(f) != 0 {
		t.Fatalf("flag not cleared: %v", f)
	}
	s := cs.store.(ClusterFlagStore)
	_ = s.SetClusterFlag(ctx, "old", time.Now().Add(-time.Second))
	if f := cs.Flags(ctx); len(f) != 0 {
		t.Fatalf("expired flag reported: %v", f)
	}
}

// Suspect survives the gRPC round trip.
func TestSuspectInProto(t *testing.T) {
	if !peerInfoFromProto(peerInfoToProto(&PeerInfo{ID: "x", Suspect: true})).Suspect {
		t.Fatal("suspect lost in proto conversion")
	}
}

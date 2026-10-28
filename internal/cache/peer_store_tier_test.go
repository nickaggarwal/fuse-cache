package cache

import (
	"testing"
	"time"

	"fuse-client/internal/coordinator"
)

func storePeer(id string) *coordinator.PeerInfo {
	return &coordinator.PeerInfo{ID: id, Status: "active", GRPCAddress: id + ":9081",
		Labels: map[string]string{RoleLabel: RoleStore}}
}

func workPeer(id string) *coordinator.PeerInfo {
	return &coordinator.PeerInfo{ID: id, Status: "active", GRPCAddress: id + ":9081"}
}

// Store peers lead the holder list; order within each group is kept, so the
// per-path rotation still spreads chunks over several stores.
func TestStoreFirstKeepsGroupOrder(t *testing.T) {
	in := []*coordinator.PeerInfo{workPeer("w1"), storePeer("s1"), workPeer("w2"), storePeer("s2")}
	got := storeFirst(in)
	want := []string{"s1", "s2", "w1", "w2"}
	for i, p := range got {
		if p.ID != want[i] {
			t.Fatalf("order = %v, want %v", ids(got), want)
		}
	}
	// No stores, or only stores: unchanged (same slice).
	only := []*coordinator.PeerInfo{workPeer("a"), workPeer("b")}
	if got := storeFirst(only); &got[0] != &only[0] {
		t.Error("no-store input was copied")
	}
}

// The latency sort keeps a healthy store ahead of a faster-measured workload
// peer, but a store measured as failing loses its precedence.
func TestLatencySortKeepsHealthyStoreFirst(t *testing.T) {
	ps := &PeerStorage{pairLatency: newPeerLatencyTracker()}
	s, w := storePeer("s1"), workPeer("w1")
	for i := 0; i < peerPairMinSamples+2; i++ {
		ps.pairLatency.record("w1", 1*time.Millisecond, true)  // fast workload peer
		ps.pairLatency.record("s1", 20*time.Millisecond, true) // slower but healthy store
	}
	peers := []*coordinator.PeerInfo{w, s}
	ps.sortByObservedLatency(peers, "/m/model.safetensors")
	if peers[0].ID != "s1" {
		t.Fatalf("healthy store not first: %v", ids(peers))
	}
	ps2 := &PeerStorage{pairLatency: newPeerLatencyTracker()}
	for i := 0; i < peerPairMinSamples+4; i++ {
		ps2.pairLatency.record("w1", 1*time.Millisecond, true)
		ps2.pairLatency.record("s1", 20*time.Millisecond, false) // failing store
	}
	peers = []*coordinator.PeerInfo{s, w}
	ps2.sortByObservedLatency(peers, "/m/model.safetensors")
	if peers[0].ID != "w1" {
		t.Fatalf("failing store kept precedence: %v", ids(peers))
	}
}

func TestWarmupPlanClusterSources(t *testing.T) {
	order, _, _, err := warmupPlan(WarmupOptions{Source: "cluster"})
	if err != nil || order != nil {
		t.Fatalf("cluster: order=%v err=%v (want adaptive order)", order, err)
	}
	order, _, _, err = warmupPlan(WarmupOptions{Source: "cluster-only"})
	if err != nil || len(order) != 1 || order[0] != TierPeer {
		t.Fatalf("cluster-only: order=%v err=%v (want peers only)", order, err)
	}
}

func ids(ps []*coordinator.PeerInfo) []string {
	out := make([]string, len(ps))
	for i, p := range ps {
		out[i] = p.ID
	}
	return out
}

// The network sort runs after storeFirst in Read; a workload peer reporting a
// faster link must not move ahead of a store.
func TestNetworkSortKeepsStoreFirst(t *testing.T) {
	fast := workPeer("fast")
	fast.NetworkSpeedMBps = 10000
	store := storePeer("store")
	store.NetworkSpeedMBps = 100
	peers := []*coordinator.PeerInfo{store, fast, workPeer("slow")}
	sortPeersByNetwork(peers, "/p")
	if peers[0].ID != "store" {
		t.Fatalf("order after network sort = %s, %s, %s; store must stay first", peers[0].ID, peers[1].ID, peers[2].ID)
	}
	if peers[1].ID != "fast" {
		t.Fatalf("workload peers lost their score order: %s before fast", peers[1].ID)
	}
}

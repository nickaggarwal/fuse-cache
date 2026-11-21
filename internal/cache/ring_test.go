package cache

import (
	"fmt"
	"math"
	"testing"

	"fuse-client/internal/coordinator"
)

func ringPeers(n int, gb int64) []*coordinator.PeerInfo {
	var ps []*coordinator.PeerInfo
	for i := 0; i < n; i++ {
		ps = append(ps, &coordinator.PeerInfo{ID: fmt.Sprintf("node-%02d", i), Status: "active", UsedSpace: 0, AvailableSpace: gb << 30})
	}
	return ps
}

// Every node computes the same owners regardless of list order.
func TestRingOwnersDeterministic(t *testing.T) {
	ps := ringPeers(16, 100)
	rev := make([]*coordinator.PeerInfo, len(ps))
	for i := range ps {
		rev[len(ps)-1-i] = ps[i]
	}
	for c := 0; c < 50; c++ {
		key := fmt.Sprintf("/oci/sha256/abc_chunk_%d", c)
		a, b := ringOwners(ps, key), ringOwners(rev, key)
		for i := 0; i < 3; i++ {
			if a[i].ID != b[i].ID {
				t.Fatalf("owners differ by list order for %s: %s vs %s", key, a[i].ID, b[i].ID)
			}
		}
	}
}

// Chunks of one blob spread over all nodes roughly evenly.
func TestRingSpreadsChunks(t *testing.T) {
	ps := ringPeers(16, 100)
	count := map[string]int{}
	const chunks = 3200
	for c := 0; c < chunks; c++ {
		count[ringOwners(ps, fmt.Sprintf("/oci/sha256/abc_chunk_%d", c))[0].ID]++
	}
	if len(count) != 16 {
		t.Fatalf("only %d of 16 nodes own chunks", len(count))
	}
	want := float64(chunks) / 16
	for id, n := range count {
		if math.Abs(float64(n)-want) > 0.35*want {
			t.Fatalf("%s owns %d chunks, want about %.0f", id, n, want)
		}
	}
}

// Removing a node moves only the chunks it owned (about 1/N).
func TestRingMinimalMovement(t *testing.T) {
	ps := ringPeers(16, 100)
	gone := ps[5]
	rest := append(append([]*coordinator.PeerInfo{}, ps[:5]...), ps[6:]...)
	moved, owned := 0, 0
	for c := 0; c < 3200; c++ {
		key := fmt.Sprintf("/b_chunk_%d", c)
		before := ringOwners(ps, key)[0]
		after := ringOwners(rest, key)[0]
		if before.ID == gone.ID {
			owned++
			continue
		}
		if before.ID != after.ID {
			moved++
		}
	}
	if moved != 0 {
		t.Fatalf("%d chunks not owned by the removed node changed owner", moved)
	}
	if owned == 0 {
		t.Fatal("removed node owned nothing")
	}
}

// A node with 4x the cache owns about 4x the chunks.
func TestRingWeightedByCapacity(t *testing.T) {
	ps := ringPeers(4, 100)
	ps = append(ps, &coordinator.PeerInfo{ID: "big", Status: "active", AvailableSpace: 400 << 30})
	n := 0
	const chunks = 8000
	for c := 0; c < chunks; c++ {
		if ringOwners(ps, fmt.Sprintf("/w_chunk_%d", c))[0].ID == "big" {
			n++
		}
	}
	share := float64(n) / chunks // expect 400/800 = 0.5
	if share < 0.42 || share > 0.58 {
		t.Fatalf("4x node owns %.2f of chunks, want about 0.50", share)
	}
}

// ringOrder puts owners first and keeps holders ahead of non-holders.
func TestRingOrder(t *testing.T) {
	ps := ringPeers(6, 100)
	key := "/x_chunk_7"
	owners := ringOwners(ps, key)[:2]
	isOwner := map[string]bool{owners[0].ID: true, owners[1].ID: true}
	var holders, rest []*coordinator.PeerInfo
	for _, p := range ps {
		if !isOwner[p.ID] && len(holders) < 1 {
			holders = append(holders, p)
		} else {
			rest = append(rest, p)
		}
	}
	pref, r := ringOrder(holders, rest, key, 2)
	if pref[0].ID != owners[0].ID || pref[1].ID != owners[1].ID || pref[2].ID != holders[0].ID {
		t.Fatalf("order = %v %v %v, want owners then holder", pref[0].ID, pref[1].ID, pref[2].ID)
	}
	if len(pref)+len(r) != len(ps) {
		t.Fatalf("lost peers: %d + %d != %d", len(pref), len(r), len(ps))
	}
	if p2, r2 := ringOrder(holders, rest, key, 0); len(p2) != len(holders) || len(r2) != len(rest) {
		t.Fatal("r=0 must leave the order unchanged")
	}
}

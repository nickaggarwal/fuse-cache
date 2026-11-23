package cache

import (
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

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

// ringOrder: owners that hold first, then other holders, then owners that
// do not hold yet (returned for nudging), then the rest.
func TestRingOrder(t *testing.T) {
	ps := ringPeers(6, 100)
	key := "/x_chunk_7"
	owners := ringOwners(ps, key)[:2]
	isOwner := map[string]bool{owners[0].ID: true, owners[1].ID: true}
	// holders: owner[0] and one non-owner; owner[1] does not hold
	var nonOwner *coordinator.PeerInfo
	for _, p := range ps {
		if !isOwner[p.ID] {
			nonOwner = p
			break
		}
	}
	holders := []*coordinator.PeerInfo{nonOwner, owners[0]}
	var rest []*coordinator.PeerInfo
	for _, p := range ps {
		if p.ID != nonOwner.ID && p.ID != owners[0].ID {
			rest = append(rest, p)
		}
	}
	first, second, unheld := ringOrder(holders, rest, key, 2)
	if first[0].ID != owners[0].ID || first[1].ID != nonOwner.ID {
		t.Fatalf("first = %s %s, want holding owner then holder", first[0].ID, first[1].ID)
	}
	if second[0].ID != owners[1].ID || len(unheld) != 1 || unheld[0].ID != owners[1].ID {
		t.Fatalf("non-holding owner must lead the second group and be returned for nudging")
	}
	if len(first)+len(second) != len(ps) {
		t.Fatalf("lost peers: %d + %d != %d", len(first), len(second), len(ps))
	}
	if p2, r2, u2 := ringOrder(holders, rest, key, 0); len(p2) != len(holders) || len(r2) != len(rest) || u2 != nil {
		t.Fatal("r=0 must leave the order unchanged")
	}
}

// nudgeOwners sends one background 1-byte read per owner, once per key.
func TestNudgeOwnersOncePerKey(t *testing.T) {
	var mu sync.Mutex
	hits := map[string]int{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		hits[r.URL.Query().Get("path")+"|"+r.URL.Query().Get("len")]++
		mu.Unlock()
		http.Error(w, "not found", http.StatusNotFound)
	}))
	defer srv.Close()
	ps, err := NewPeerStorage(newDeleteCoordinator(), time.Second, "me", false, false, true, "")
	if err != nil {
		t.Fatal(err)
	}
	owner := &coordinator.PeerInfo{ID: "o", Address: strings.TrimPrefix(srv.URL, "http://"), Status: "active"}
	for i := 0; i < 3; i++ {
		ps.nudgeOwners("/m/w.bin_chunk_4", []*coordinator.PeerInfo{owner})
	}
	ps.nudgeOwners("/m/w.bin", []*coordinator.PeerInfo{owner}) // not a chunk: ignored
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		n := hits["/m/w.bin_chunk_4|1"]
		mu.Unlock()
		if n > 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	time.Sleep(50 * time.Millisecond)
	mu.Lock()
	defer mu.Unlock()
	if hits["/m/w.bin_chunk_4|1"] != 1 || len(hits) != 1 {
		t.Fatalf("nudges = %v, want exactly one 1-byte read of the chunk", hits)
	}
}

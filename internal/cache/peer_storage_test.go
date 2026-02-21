package cache

import (
	"testing"

	"fuse-client/internal/coordinator"
)

func TestOrderedPeersForPath_UsesChunkIndex(t *testing.T) {
	peers := []*coordinator.PeerInfo{
		{ID: "p0"},
		{ID: "p1"},
		{ID: "p2"},
	}

	got0 := orderedPeersForPath(peers, "/data.bin_chunk_0")
	if got0[0].ID != "p0" {
		t.Fatalf("chunk_0 first peer = %s, want p0", got0[0].ID)
	}

	got1 := orderedPeersForPath(peers, "/data.bin_chunk_1")
	if got1[0].ID != "p1" {
		t.Fatalf("chunk_1 first peer = %s, want p1", got1[0].ID)
	}

	got2 := orderedPeersForPath(peers, "/data.bin_chunk_2")
	if got2[0].ID != "p2" {
		t.Fatalf("chunk_2 first peer = %s, want p2", got2[0].ID)
	}
}

func TestOrderedPeersForPath_NonChunkIsStable(t *testing.T) {
	peers := []*coordinator.PeerInfo{
		{ID: "a"},
		{ID: "b"},
		{ID: "c"},
	}
	got1 := orderedPeersForPath(peers, "/regular-object")
	got2 := orderedPeersForPath(peers, "/regular-object")

	if len(got1) != len(peers) || len(got2) != len(peers) {
		t.Fatalf("unexpected peer ordering length: %d %d", len(got1), len(got2))
	}
	for i := range got1 {
		if got1[i].ID != got2[i].ID {
			t.Fatalf("ordering is not stable at %d: %s vs %s", i, got1[i].ID, got2[i].ID)
		}
	}
}

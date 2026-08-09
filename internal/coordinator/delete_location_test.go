package coordinator

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"
)

// File location rows, unlike peer rows, carry no TTL in etcd — nothing removes
// them but an explicit retraction. These tests pin the semantics the cache
// layer relies on, against InMemoryStore; EtcdStore implements the same
// contract (prefix delete for whole-path, CAS for per-peer).

func TestDeleteFileLocation_WholePathDropsEveryReplica(t *testing.T) {
	cs := NewCoordinatorService()
	ctx := context.Background()

	for _, peer := range []string{"peer-1", "peer-2", "peer-3"} {
		cs.UpdateFileLocation(ctx, &FileLocation{
			FilePath: "/gone.bin", PeerID: peer, StorageTier: "nvme", FileSize: 100,
		})
	}

	if err := cs.DeleteFileLocation(ctx, "/gone.bin", "peer-1", true); err != nil {
		t.Fatalf("DeleteFileLocation: %v", err)
	}

	locs, err := cs.GetFileLocation(ctx, "/gone.bin")
	if err != nil {
		t.Fatalf("GetFileLocation: %v", err)
	}
	if len(locs) != 0 {
		t.Fatalf("whole-path delete left %d rows, want 0", len(locs))
	}
}

// A node publishes one row per tier it holds the object in — nvme at write,
// cloud once persisted — so retracting "this peer's copy" has to drop every
// row it owns, not just the one that matches some tier.
func TestDeleteFileLocation_PerPeerDropsAllOfThatPeersTiers(t *testing.T) {
	cs := NewCoordinatorService()
	ctx := context.Background()

	cs.UpdateFileLocation(ctx, &FileLocation{
		FilePath: "/shared.bin", PeerID: "peer-1", StorageTier: "nvme", FileSize: 100,
	})
	cs.UpdateFileLocation(ctx, &FileLocation{
		FilePath: "/shared.bin", PeerID: "peer-1", StorageTier: "cloud", FileSize: 100,
	})
	cs.UpdateFileLocation(ctx, &FileLocation{
		FilePath: "/shared.bin", PeerID: "peer-2", StorageTier: "nvme", FileSize: 100,
	})

	if err := cs.DeleteFileLocation(ctx, "/shared.bin", "peer-1", false); err != nil {
		t.Fatalf("DeleteFileLocation: %v", err)
	}

	locs, _ := cs.GetFileLocation(ctx, "/shared.bin")
	if len(locs) != 1 {
		t.Fatalf("rows after per-peer retraction = %d, want 1", len(locs))
	}
	if locs[0].PeerID != "peer-2" {
		t.Fatalf("surviving row is peer %q, want peer-2", locs[0].PeerID)
	}
}

// Retracting the last replica must remove the key, not leave an empty list
// behind: an empty row still answers a lookup and keeps the name resolving.
func TestDeleteFileLocation_LastReplicaDropsTheKey(t *testing.T) {
	cs := NewCoordinatorService()
	ctx := context.Background()

	cs.UpdateFileLocation(ctx, &FileLocation{
		FilePath: "/only.bin", PeerID: "peer-1", StorageTier: "nvme", FileSize: 10,
	})
	if err := cs.DeleteFileLocation(ctx, "/only.bin", "peer-1", false); err != nil {
		t.Fatalf("DeleteFileLocation: %v", err)
	}

	listed, err := cs.ListFileLocations(ctx, "")
	if err != nil {
		t.Fatalf("ListFileLocations: %v", err)
	}
	for _, loc := range listed {
		if loc.FilePath == "/only.bin" {
			t.Fatal("path still listed after its last replica was retracted")
		}
	}
}

// Chunked files carry a row per chunk (the cloud persist publishes one row per
// cloud key, and those keys are the chunk paths), so the parent's retraction
// has to take them with it.
func TestDeleteFileLocation_WholePathSweepsChunkRows(t *testing.T) {
	cs := NewCoordinatorService()
	ctx := context.Background()

	cs.UpdateFileLocation(ctx, &FileLocation{
		FilePath: "/big.bin", PeerID: "peer-1", StorageTier: "nvme", FileSize: 300, IsChunked: true,
	})
	for i := 0; i < 3; i++ {
		cs.UpdateFileLocation(ctx, &FileLocation{
			FilePath:    "/big.bin_chunk_" + string(rune('0'+i)),
			PeerID:      "peer-1",
			StorageTier: "cloud",
			FileSize:    100,
		})
	}

	if err := cs.DeleteFileLocation(ctx, "/big.bin", "peer-1", true); err != nil {
		t.Fatalf("DeleteFileLocation: %v", err)
	}

	listed, _ := cs.ListFileLocations(ctx, "/big.bin")
	if len(listed) != 0 {
		paths := make([]string, 0, len(listed))
		for _, loc := range listed {
			paths = append(paths, loc.FilePath)
		}
		t.Fatalf("chunk rows survived the parent's retraction: %v", paths)
	}
}

// The "_chunk_" suffix on the prefix sweep is load-bearing: a bare prefix
// delete on "/a" would also erase "/a/b" and wipe a whole subtree's metadata.
func TestDeleteFileLocation_WholePathDoesNotEatSubtree(t *testing.T) {
	cs := NewCoordinatorService()
	ctx := context.Background()

	cs.UpdateFileLocation(ctx, &FileLocation{
		FilePath: "/dir", PeerID: "peer-1", StorageTier: "nvme", FileSize: 10,
	})
	cs.UpdateFileLocation(ctx, &FileLocation{
		FilePath: "/dir/child.bin", PeerID: "peer-1", StorageTier: "nvme", FileSize: 20,
	})
	cs.UpdateFileLocation(ctx, &FileLocation{
		FilePath: "/dir-sibling.bin", PeerID: "peer-1", StorageTier: "nvme", FileSize: 30,
	})

	if err := cs.DeleteFileLocation(ctx, "/dir", "peer-1", true); err != nil {
		t.Fatalf("DeleteFileLocation: %v", err)
	}

	for _, survivor := range []string{"/dir/child.bin", "/dir-sibling.bin"} {
		locs, _ := cs.GetFileLocation(ctx, survivor)
		if len(locs) == 0 {
			t.Fatalf("%s was wiped by the retraction of /dir", survivor)
		}
	}
}

// Retracting rows that were never there is a success: delete is best-effort
// and gets retried from paths that cannot know what the coordinator holds.
func TestDeleteFileLocation_AbsentPathIsNotAnError(t *testing.T) {
	cs := NewCoordinatorService()
	ctx := context.Background()

	if err := cs.DeleteFileLocation(ctx, "/never.bin", "peer-1", false); err != nil {
		t.Fatalf("per-peer retraction of an absent path: %v", err)
	}
	if err := cs.DeleteFileLocation(ctx, "/never.bin", "peer-1", true); err != nil {
		t.Fatalf("whole-path retraction of an absent path: %v", err)
	}
}

// TestCoordinatorClient_DeleteFileLocation pins the HTTP wire shape the
// client daemon uses when the gRPC transport is unavailable.
func TestCoordinatorClient_DeleteFileLocation(t *testing.T) {
	var gotMethod, gotPath, gotQuery string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod, gotPath, gotQuery = r.Method, r.URL.Path, r.URL.RawQuery
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	u, err := url.Parse(srv.URL)
	if err != nil {
		t.Fatalf("parse server url: %v", err)
	}
	client := NewCoordinatorClient(u.Host, 2*time.Second)

	if err := client.DeleteFileLocation(context.Background(), "/a b.bin", "peer-1", true); err != nil {
		t.Fatalf("DeleteFileLocation: %v", err)
	}

	if gotMethod != http.MethodDelete {
		t.Errorf("method = %s, want DELETE", gotMethod)
	}
	if gotPath != "/api/files/location" {
		t.Errorf("path = %s, want /api/files/location", gotPath)
	}
	values, err := url.ParseQuery(gotQuery)
	if err != nil {
		t.Fatalf("parse query %q: %v", gotQuery, err)
	}
	if got := values.Get("path"); got != "/a b.bin" {
		t.Errorf("path param = %q, want %q (escaping lost)", got, "/a b.bin")
	}
	if got := values.Get("peer_id"); got != "peer-1" {
		t.Errorf("peer_id param = %q, want peer-1", got)
	}
	if got := values.Get("whole_path"); got != "true" {
		t.Errorf("whole_path param = %q, want true", got)
	}
}

package globalsync

import (
	"testing"
	"time"

	"fuse-client/internal/pb"
)

func loc(cluster, path string, size int64, published int64) *pb.GlobalLocation {
	return &pb.GlobalLocation{
		ClusterId:     cluster,
		FilePath:      path,
		FileSize:      size,
		PublishedUnix: published,
		Object:        &pb.ObjectRef{Provider: "s3", Bucket: "shared", KeyPrefix: cluster},
	}
}

func TestMirrorLookupEmptyBeforeSync(t *testing.T) {
	m := NewMirror()
	if got := m.Lookup("/models/a.bin"); got != nil {
		t.Fatalf("unsynced mirror returned %v, want nil", got)
	}
	// Ready must stay false: "I have not synced" and "I synced and found
	// nothing" are different answers, and only the second is safe to act on.
	if m.Status().Ready {
		t.Fatal("mirror reports ready before any snapshot")
	}
}

func TestMirrorSnapshotReplacesRatherThanMerges(t *testing.T) {
	m := NewMirror()
	m.ApplySnapshot([]*pb.GlobalLocation{loc("a", "/models/one.bin", 10, 1)})
	m.ApplySnapshot([]*pb.GlobalLocation{loc("a", "/models/two.bin", 20, 2)})

	if got := m.Lookup("/models/one.bin"); len(got) != 0 {
		t.Fatalf("row from the superseded snapshot survived: %v", got)
	}
	if got := m.Lookup("/models/two.bin"); len(got) != 1 {
		t.Fatalf("want 1 row for two.bin, got %d", len(got))
	}
	if !m.Status().Ready {
		t.Fatal("mirror not ready after a snapshot")
	}
}

func TestMirrorKeepsEveryClustersCopy(t *testing.T) {
	m := NewMirror()
	m.ApplySnapshot([]*pb.GlobalLocation{
		loc("a", "/models/x.bin", 10, 100),
		loc("b", "/models/x.bin", 10, 200),
	})
	got := m.Lookup("/models/x.bin")
	if len(got) != 2 {
		t.Fatalf("want both clusters' copies, got %d", len(got))
	}
	// Newest publication first.
	if got[0].GetClusterId() != "b" {
		t.Fatalf("want newest-first ordering, got %s first", got[0].GetClusterId())
	}
}

func TestMirrorWithdrawRemovesOnlyThatCluster(t *testing.T) {
	m := NewMirror()
	m.ApplySnapshot([]*pb.GlobalLocation{
		loc("a", "/models/x.bin", 10, 100),
		loc("b", "/models/x.bin", 10, 200),
	})
	m.ApplyEvent(&pb.GlobalEvent{
		Kind: pb.GlobalEvent_WITHDRAW, ClusterId: "b", FilePath: "/models/x.bin",
	})
	got := m.Lookup("/models/x.bin")
	if len(got) != 1 || got[0].GetClusterId() != "a" {
		t.Fatalf("want only cluster a left, got %v", got)
	}

	m.ApplyEvent(&pb.GlobalEvent{
		Kind: pb.GlobalEvent_WITHDRAW, ClusterId: "a", FilePath: "/models/x.bin",
	})
	if got := m.Lookup("/models/x.bin"); len(got) != 0 {
		t.Fatalf("want path gone, got %v", got)
	}
	if st := m.Status(); st.Paths != 0 {
		t.Fatalf("empty path map not reclaimed: %d paths remain", st.Paths)
	}
}

// TestMirrorSurvivesDisconnect is the autonomy property in miniature: the
// requirement is that losing the control plane costs only NEW cross-cluster
// discovery, so the mirror must keep answering from what it already knows.
func TestMirrorSurvivesDisconnect(t *testing.T) {
	m := NewMirror()
	m.ApplySnapshot([]*pb.GlobalLocation{loc("a", "/models/x.bin", 10, 1)})
	m.MarkDisconnected()

	if got := m.Lookup("/models/x.bin"); len(got) != 1 {
		t.Fatalf("mirror forgot its contents on disconnect: %v", got)
	}
	if !m.Status().Ready {
		t.Fatal("mirror dropped readiness on disconnect")
	}
}

// TestMirrorLookupReturnsCopies pins that a caller cannot mutate the index
// through the rows it is handed.
func TestMirrorLookupReturnsCopies(t *testing.T) {
	m := NewMirror()
	m.ApplySnapshot([]*pb.GlobalLocation{loc("a", "/models/x.bin", 10, 1)})

	got := m.Lookup("/models/x.bin")
	got[0].FileSize = 999
	got[0].Object.Bucket = "attacker"

	again := m.Lookup("/models/x.bin")
	if again[0].GetFileSize() != 10 || again[0].GetObject().GetBucket() != "shared" {
		t.Fatalf("mirror state mutated through a returned row: %v", again[0])
	}
}

func TestMirrorStatusTracksSyncTime(t *testing.T) {
	m := NewMirror()
	before := time.Now()
	m.ApplySnapshot(nil)
	st := m.Status()
	if st.LastSyncAt.Before(before) {
		t.Fatalf("LastSyncAt not advanced: %v", st.LastSyncAt)
	}
	// An empty snapshot still means "synced, nothing there" -- which the
	// data path is allowed to act on, unlike "never synced".
	if !st.Ready {
		t.Fatal("empty snapshot left the mirror not-ready")
	}
}

// TestResolveCarriesReadyEvenWithNoRows is the claim the data plane acts on.
// An unsynced mirror and a synced one both return no rows for an unknown
// path; only the second is a negative a reader may trust.
func TestResolveCarriesReadyEvenWithNoRows(t *testing.T) {
	m := NewMirror()
	if _, ready := m.Resolve("/models/a.bin"); ready {
		t.Fatal("unsynced mirror resolved as ready")
	}
	m.ApplySnapshot(nil)
	if _, ready := m.Resolve("/models/a.bin"); !ready {
		t.Fatal("an empty snapshot is still a sync; ready must be true")
	}
}

// TestResolveTranslatesTheObjectRef: the mirror is where the wire type
// becomes the type the cache reads keys out of. A field dropped here
// produces a read under the wrong key.
func TestResolveTranslatesTheObjectRef(t *testing.T) {
	m := NewMirror()
	m.ApplySnapshot([]*pb.GlobalLocation{{
		ClusterId: "cluster-a", FilePath: "/models/a.bin",
		FileSize: 99, IsChunked: true, ChunkSize: 1 << 20, PublishedUnix: 7,
		Object: &pb.ObjectRef{
			Provider: "azure", Bucket: "blobs", KeyPrefix: "cluster-a",
			Region: "eastus", Endpoint: "https://x.blob.core.windows.net",
		},
	}})

	got, ready := m.Resolve("/models/a.bin")
	if !ready || len(got) != 1 {
		t.Fatalf("ready=%v rows=%d", ready, len(got))
	}
	g := got[0]
	if g.ClusterID != "cluster-a" || g.FilePath != "/models/a.bin" || g.FileSize != 99 ||
		!g.IsChunked || g.ChunkSize != 1<<20 || g.PublishedAt != 7 ||
		g.Provider != "azure" || g.Bucket != "blobs" || g.KeyPrefix != "cluster-a" ||
		g.Region != "eastus" || g.Endpoint != "https://x.blob.core.windows.net" {
		t.Fatalf("translation lost a field: %+v", *g)
	}
}

// TestResolveDropsRowsWithNoReadableObject. The global data path is object
// storage only, so a row with no bucket names nothing a reader can fetch.
// Returning it would cost a wasted round trip on every miss for that path.
func TestResolveDropsRowsWithNoReadableObject(t *testing.T) {
	m := NewMirror()
	m.ApplySnapshot([]*pb.GlobalLocation{
		{ClusterId: "a", FilePath: "/models/a.bin"},                                        // no ObjectRef at all
		{ClusterId: "b", FilePath: "/models/a.bin", Object: &pb.ObjectRef{Provider: "s3"}}, // no bucket
		loc("c", "/models/a.bin", 10, 3),
	})

	got, ready := m.Resolve("/models/a.bin")
	if !ready {
		t.Fatal("not ready after a snapshot")
	}
	if len(got) != 1 || got[0].ClusterID != "c" {
		t.Fatalf("got %d rows (%+v), want only the one with a bucket", len(got), got)
	}
}

// TestResolveAfterWithdrawStopsAdvertising: the data plane must stop
// resolving a path the publishing cluster withdrew, or every reader keeps
// paying a wasted remote read for a deleted object.
func TestResolveAfterWithdrawStopsAdvertising(t *testing.T) {
	m := NewMirror()
	m.ApplySnapshot([]*pb.GlobalLocation{loc("a", "/models/a.bin", 10, 1)})
	if got, _ := m.Resolve("/models/a.bin"); len(got) != 1 {
		t.Fatalf("setup: got %d rows", len(got))
	}

	m.ApplyEvent(&pb.GlobalEvent{
		Kind:      pb.GlobalEvent_WITHDRAW,
		ClusterId: "a",
		FilePath:  "/models/a.bin",
	})

	got, ready := m.Resolve("/models/a.bin")
	if len(got) != 0 {
		t.Fatalf("withdrawn path still resolves: %+v", got)
	}
	// Ready must stay true: the mirror is still in sync, it just knows the
	// object is gone. Collapsing to not-ready here would be a worse answer
	// than the one it has.
	if !ready {
		t.Fatal("a withdraw must not make a synced mirror report unsynced")
	}
}

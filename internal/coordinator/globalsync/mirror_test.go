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

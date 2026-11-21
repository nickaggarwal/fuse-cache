package coordinator

import (
	"context"
	"io"
	"log"
	"testing"
	"time"
)

func leaseService() *CoordinatorService {
	return &CoordinatorService{store: NewInMemoryStore(), logger: log.New(io.Discard, "", 0)}
}

func nvmeRow(path, peer string, gen uint64, exp time.Time) *FileLocation {
	return &FileLocation{FilePath: path, PeerID: peer, StorageTier: "nvme", FileSize: 10, Generation: gen, ExpiresAt: exp}
}

// A retraction for an older admission arriving after a newer admission must
// not remove the newer row.
func TestLateRetractionKeepsNewerAdmission(t *testing.T) {
	ctx := context.Background()
	cs := leaseService()
	exp := time.Now().Add(time.Hour)
	_ = cs.UpdateFileLocation(ctx, nvmeRow("/m", "a", 5, exp))                      // admission 5
	_ = cs.UpdateFileLocation(ctx, nvmeRow("/m", "a", 7, exp))                      // evicted, re-admitted as 7
	if err := cs.DeletePeerTierLocationGen(ctx, "/m", "a", "nvme", 5); err != nil { // late notice for 5
		t.Fatal(err)
	}
	locs, _ := cs.GetFileLocation(ctx, "/m")
	if len(locs) != 1 || locs[0].Generation != 7 {
		t.Fatalf("rows = %+v, want generation 7 kept", locs)
	}
	_ = cs.DeletePeerTierLocationGen(ctx, "/m", "a", "nvme", 7)
	if locs, _ := cs.GetFileLocation(ctx, "/m"); len(locs) != 0 {
		t.Fatalf("rows = %+v after retracting 7", locs)
	}
}

// A delayed publish of an older admission must not replace a newer row.
func TestStaleAdmissionIgnored(t *testing.T) {
	ctx := context.Background()
	cs := leaseService()
	exp := time.Now().Add(time.Hour)
	_ = cs.UpdateFileLocation(ctx, nvmeRow("/m", "a", 9, exp))
	_ = cs.UpdateFileLocation(ctx, nvmeRow("/m", "a", 8, exp))
	locs, _ := cs.GetFileLocation(ctx, "/m")
	if len(locs) != 1 || locs[0].Generation != 9 {
		t.Fatalf("rows = %+v, want generation 9", locs)
	}
}

// Expired rows are hidden from every read, and swept after the grace period;
// rows without a lease (older clients) never expire.
func TestExpiredRowsHiddenAndSwept(t *testing.T) {
	ctx := context.Background()
	cs := leaseService()
	_ = cs.UpdateFileLocation(ctx, nvmeRow("/m", "dead", 1, time.Now().Add(-time.Hour)))
	_ = cs.UpdateFileLocation(ctx, nvmeRow("/m", "live", 1, time.Now().Add(time.Hour)))
	_ = cs.UpdateFileLocation(ctx, &FileLocation{FilePath: "/m", PeerID: "old", StorageTier: "nvme", FileSize: 10})
	_ = cs.UpdateFileLocation(ctx, nvmeRow("/gone", "dead", 1, time.Now().Add(-time.Hour)))

	locs, _ := cs.GetFileLocation(ctx, "/m")
	peers := map[string]bool{}
	for _, l := range locs {
		peers[l.PeerID] = true
	}
	if peers["dead"] || !peers["live"] || !peers["old"] {
		t.Fatalf("visible holders = %v, want live+old only", peers)
	}
	if list, _ := cs.ListFileLocations(ctx, "/gone"); len(list) != 0 {
		t.Fatalf("a path with only expired rows is listed: %+v", list)
	}
	if m, _ := cs.RangeFileLocations(ctx, ""); len(m["/m"]) != 2 || len(m["/gone"]) != 0 {
		t.Fatalf("range = %v", m)
	}
	if n := cs.SweepExpiredLocations(ctx); n != 2 {
		t.Fatalf("swept %d, want 2", n)
	}
	raw, _ := cs.store.GetFileLocations(ctx, "/m")
	if len(raw) != 2 {
		t.Fatalf("stored rows after sweep = %d, want 2", len(raw))
	}
}

// A row that expired only just now is hidden but not yet swept: a holder
// renewing a little late keeps its row.
func TestSweepGrace(t *testing.T) {
	ctx := context.Background()
	cs := leaseService()
	_ = cs.UpdateFileLocation(ctx, nvmeRow("/m", "a", 1, time.Now().Add(-10*time.Second)))
	if n := cs.SweepExpiredLocations(ctx); n != 0 {
		t.Fatalf("swept %d within grace", n)
	}
	_ = cs.UpdateFileLocation(ctx, nvmeRow("/m", "a", 1, time.Now().Add(time.Hour))) // renewal
	if locs, _ := cs.GetFileLocation(ctx, "/m"); len(locs) != 1 {
		t.Fatalf("renewed row not visible: %+v", locs)
	}
}

// Generation 0 keeps today's behaviour: any retraction removes the row.
func TestUnconditionalRetraction(t *testing.T) {
	ctx := context.Background()
	cs := leaseService()
	_ = cs.UpdateFileLocation(ctx, nvmeRow("/m", "a", 12, time.Time{}))
	_ = cs.DeletePeerTierLocation(ctx, "/m", "a", "nvme")
	if locs, _ := cs.GetFileLocation(ctx, "/m"); len(locs) != 0 {
		t.Fatalf("rows = %+v", locs)
	}
}

// Generation, lease and partial survive the gRPC converters.
func TestLeaseFieldsRoundTripProto(t *testing.T) {
	exp := time.Now().Add(time.Minute).Truncate(time.Microsecond)
	in := &FileLocation{FilePath: "/m", PeerID: "a", StorageTier: "nvme", Generation: 42, ExpiresAt: exp, Partial: true}
	out := fileLocationFromProto(fileLocationToProto(in))
	if out.Generation != 42 || !out.ExpiresAt.Equal(exp) || !out.Partial {
		t.Fatalf("round trip = %+v", out)
	}
	if z := fileLocationFromProto(fileLocationToProto(&FileLocation{FilePath: "/x"})); !z.ExpiresAt.IsZero() {
		t.Fatalf("zero lease became %v", z.ExpiresAt)
	}
}

// The audit removes the node's rows for paths it does not report, keeps the
// rows it does, and never touches other nodes' or cloud rows.
func TestAuditHolderRemovesUnbackedRows(t *testing.T) {
	ctx := context.Background()
	cs := leaseService()
	exp := time.Now().Add(time.Hour)
	_ = cs.UpdateFileLocation(ctx, nvmeRow("/keep", "a", 1, exp))
	_ = cs.UpdateFileLocation(ctx, nvmeRow("/lost", "a", 2, exp))
	_ = cs.UpdateFileLocation(ctx, nvmeRow("/lost", "b", 3, exp))
	_ = cs.UpdateFileLocation(ctx, &FileLocation{FilePath: "/lost", PeerID: "a", StorageTier: "cloud", FileSize: 10})
	res, err := cs.AuditHolder(ctx, HolderAuditRequest{PeerID: "a", Paths: []string{"/keep"}})
	if err != nil || res.Removed != 1 || res.Checked != 2 {
		t.Fatalf("audit = %+v, %v; want 1 removed of 2 checked", res, err)
	}
	locs, _ := cs.GetFileLocation(ctx, "/lost")
	tiers := map[string]bool{}
	for _, l := range locs {
		tiers[l.PeerID+"/"+l.StorageTier] = true
	}
	if tiers["a/nvme"] || !tiers["b/nvme"] || !tiers["a/cloud"] {
		t.Fatalf("rows after audit = %v", tiers)
	}
	if l, _ := cs.GetFileLocation(ctx, "/keep"); len(l) != 1 {
		t.Fatal("a reported row was removed")
	}
	if _, err := cs.AuditHolder(ctx, HolderAuditRequest{}); err == nil {
		t.Fatal("audit without peer_id accepted")
	}
}

package cache

// Tests for manifest-driven warmup (WarmFiles): a path known only through the
// global index is warmed where WarmPrefixOpts reports files=0, every listed
// path gets an outcome, and Complete is true only when every file is local.

import (
	"context"
	"strings"
	"testing"

	"fuse-client/internal/coordinator"

	"github.com/aws/aws-sdk-go/aws/awserr"
)

// globalOnlyCoordinator has no local rows for anything (so prefix
// enumeration finds nothing) but resolves listed paths through the global
// index, like a destination cluster reading another cluster's release.
type globalOnlyCoordinator struct {
	herdCoordinator
	global map[string]*coordinator.GlobalLocation
}

func (c *globalOnlyCoordinator) ResolveGlobal(_ context.Context, p string) (*coordinator.GlobalResolution, error) {
	res := &coordinator.GlobalResolution{Ready: true}
	if loc, ok := c.global[p]; ok {
		res.Locations = []*coordinator.GlobalLocation{loc}
	}
	if parent, ok := parentFilePathFromChunkPath(p); ok {
		if loc, ok := c.global[parent]; ok {
			res.Locations = []*coordinator.GlobalLocation{loc}
		}
	}
	return res, nil
}

// globalBackedCloud is the local cloud tier (misses everything under its own
// scope) plus raw-key reads of the publishing cluster's prefix.
type globalBackedCloud struct {
	TierStorage
	bucket string
	data   map[string][]byte
}

func (g *globalBackedCloud) ReadKey(_ context.Context, key string) ([]byte, error) {
	d, ok := g.data[key]
	if !ok {
		return nil, errNoSuchKeyForTest
	}
	return d, nil
}
func (g *globalBackedCloud) BucketName() string { return g.bucket }

// Read misses with a typed 404, as the real backends do: the global branch
// runs only on isCloudMiss, never on a generic error.
func (g *globalBackedCloud) Read(ctx context.Context, p string) ([]byte, error) {
	d, err := g.TierStorage.Read(ctx, p)
	if err != nil {
		return nil, errNoSuchKeyForTest
	}
	return d, nil
}

func TestWarmFiles_GlobalOnlyReleaseIsWarmedWherePrefixWarmFindsNothing(t *testing.T) {
	cm := evictTestManager(t, 1<<30)
	cm.config.ChunkSize = 1000
	ctx := context.Background()

	cloud := &globalBackedCloud{TierStorage: cm.cloudStorage, bucket: "shared", data: map[string][]byte{
		"cluster-a/models/chat/r1/config.json":     []byte(`{"a":1}`),
		"cluster-a/models/chat/r1/big.bin_chunk_0": make([]byte, 1000),
		"cluster-a/models/chat/r1/big.bin_chunk_1": make([]byte, 200),
	}}
	cm.cloudStorage = cloud
	cm.config.GlobalReadEnabled = true
	cm.config.Coordinator = &globalOnlyCoordinator{global: map[string]*coordinator.GlobalLocation{
		"/models/chat/r1/config.json": {ClusterID: "cluster-a", FilePath: "/models/chat/r1/config.json",
			Bucket: "shared", KeyPrefix: "cluster-a", Provider: "s3", FileSize: 7},
		"/models/chat/r1/big.bin": {ClusterID: "cluster-a", FilePath: "/models/chat/r1/big.bin",
			Bucket: "shared", KeyPrefix: "cluster-a", Provider: "s3", FileSize: 1200, IsChunked: true, ChunkSize: 1000},
	}}

	// The gap: prefix warm enumerates local rows only and "succeeds" empty.
	pre, err := cm.WarmPrefixOpts(ctx, "/models/chat/r1", WarmupOptions{Mode: "full"})
	if err != nil {
		t.Fatal(err)
	}
	if pre.Files != 0 {
		t.Fatalf("prefix warm found %d files; this test needs the global-only shape", pre.Files)
	}

	res, err := cm.WarmFiles(ctx, []WarmFile{
		{Path: "/models/chat/r1/config.json", Size: 7},
		{Path: "/models/chat/r1/big.bin", Size: 1200},
	}, WarmupOptions{Mode: "full", Source: "cloud-only"})
	if err != nil {
		t.Fatal(err)
	}
	cm.bgWg.Wait()
	if !res.Complete || res.Warmed != 2 || res.Files != 2 || res.Bytes != 1207 {
		t.Fatalf("result = %+v", res)
	}
	for _, r := range res.Results {
		if r.Source != "global" {
			t.Errorf("%s source = %q, want global", r.Path, r.Source)
		}
	}
	for p, want := range map[string]int64{"/models/chat/r1/config.json": 7, "/models/chat/r1/big.bin": 1200} {
		if got, err := cm.nvmeStorage.Size(ctx, p); err != nil || got != want {
			t.Errorf("%s local size = %d (%v), want %d", p, got, err, want)
		}
	}

	// Repeat is a no-op.
	again, err := cm.WarmFiles(ctx, []WarmFile{{Path: "/models/chat/r1/config.json"}, {Path: "/models/chat/r1/big.bin"}},
		WarmupOptions{Mode: "full", Source: "cloud-only"})
	if err != nil || !again.Complete || again.AlreadyLocal != 2 || again.Warmed != 0 {
		t.Fatalf("repeat = %+v %v", again, err)
	}
}

func TestWarmFiles_EveryPathGetsAnOutcome(t *testing.T) {
	cm := evictTestManager(t, 1<<30)
	ctx := context.Background()
	cm.cloudStorage.Write(ctx, "/m/ok.bin", make([]byte, 300))
	cm.cloudStorage.Write(ctx, "/m/wrongsize.bin", make([]byte, 300))
	cm.config.Coordinator = &pointCoordinator{rows: map[string]*coordinator.FileLocation{
		"/m/ok.bin":        {FilePath: "/m/ok.bin", PeerID: "p", StorageTier: "cloud", FileSize: 300},
		"/m/wrongsize.bin": {FilePath: "/m/wrongsize.bin", PeerID: "p", StorageTier: "cloud", FileSize: 300},
	}}

	res, err := cm.WarmFiles(ctx, []WarmFile{
		{Path: "/m/ok.bin", Size: 300},
		{Path: "/m/missing.bin", Size: 10},
		{Path: "/m/wrongsize.bin", Size: 999},
	}, WarmupOptions{Mode: "full", Source: "cloud-only"})
	if err != nil {
		t.Fatal(err)
	}
	cm.bgWg.Wait()
	if res.Complete {
		t.Fatal("complete with a missing and a mismatched file")
	}
	want := map[string]string{"/m/ok.bin": WarmFileWarmed, "/m/missing.bin": WarmFileNotFound, "/m/wrongsize.bin": WarmFileSizeMismatch}
	for _, r := range res.Results {
		if r.Status != want[r.Path] {
			t.Errorf("%s = %s (%s), want %s", r.Path, r.Status, r.Error, want[r.Path])
		}
	}
	if res.Warmed != 1 || res.NotFound != 1 || res.Failed != 1 {
		t.Errorf("counts = %+v", res)
	}
	if cm.nvmeStorage.Exists(ctx, "/m/wrongsize.bin") {
		t.Error("a size-mismatched file was fetched")
	}
}

func TestWarmFiles_MetadataModeClaimsNothing(t *testing.T) {
	cm := evictTestManager(t, 1<<30)
	ctx := context.Background()
	cm.cloudStorage.Write(ctx, "/m/a.bin", make([]byte, 50))
	cm.config.Coordinator = &pointCoordinator{rows: map[string]*coordinator.FileLocation{
		"/m/a.bin": {FilePath: "/m/a.bin", PeerID: "p", StorageTier: "cloud", FileSize: 50},
	}}
	res, err := cm.WarmFiles(ctx, []WarmFile{{Path: "/m/a.bin"}}, WarmupOptions{Mode: "metadata"})
	if err != nil {
		t.Fatal(err)
	}
	if res.Complete || res.Skipped != 1 || res.Results[0].Size != 50 {
		t.Fatalf("metadata result = %+v", res)
	}
	if cm.nvmeStorage.Exists(ctx, "/m/a.bin") {
		t.Fatal("metadata mode moved bytes")
	}
}

func TestWarmFiles_HeadroomSkipIsNotComplete(t *testing.T) {
	cm := evictTestManager(t, 100) // 100-byte cache
	ctx := context.Background()
	cm.cloudStorage.Write(ctx, "/m/big.bin", make([]byte, 500))
	cm.config.Coordinator = &pointCoordinator{rows: map[string]*coordinator.FileLocation{
		"/m/big.bin": {FilePath: "/m/big.bin", PeerID: "p", StorageTier: "cloud", FileSize: 500},
	}}
	res, err := cm.WarmFiles(ctx, []WarmFile{{Path: "/m/big.bin"}}, WarmupOptions{Mode: "full"})
	if err != nil {
		t.Fatal(err)
	}
	if res.Complete || res.Skipped != 1 || !strings.Contains(res.Results[0].Error, "headroom") {
		t.Fatalf("result = %+v", res)
	}
}

func TestWarmFiles_RejectsBadRequests(t *testing.T) {
	cm := evictTestManager(t, 1<<30)
	cm.config.Coordinator = &pointCoordinator{}
	ctx := context.Background()
	for name, files := range map[string][]WarmFile{
		"empty":     nil,
		"dup":       {{Path: "/a"}, {Path: "/a"}},
		"chunk":     {{Path: "/a_chunk_0"}},
		"traversal": {{Path: "/a/../b"}},
		"root":      {{Path: "/"}},
	} {
		if _, err := cm.WarmFiles(ctx, files, WarmupOptions{}); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	if _, err := cm.WarmFiles(ctx, []WarmFile{{Path: "/a"}}, WarmupOptions{Mode: "bogus"}); err == nil {
		t.Error("bogus mode accepted")
	}
}

// pointCoordinator answers single-path lookups from a fixed map.
type pointCoordinator struct {
	herdCoordinator
	rows map[string]*coordinator.FileLocation
}

func (c *pointCoordinator) GetFileLocation(_ context.Context, p string) ([]*coordinator.FileLocation, error) {
	if loc, ok := c.rows[p]; ok {
		return []*coordinator.FileLocation{loc}, nil
	}
	return nil, nil
}

var errNoSuchKeyForTest = awserr.NewRequestFailure(awserr.New("NoSuchKey", "not found", nil), 404, "req")

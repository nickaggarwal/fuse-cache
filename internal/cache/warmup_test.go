package cache

// Tests for declarative session warmup (WarmPrefix): full mode pulls whole
// and chunked files under the prefix into local NVMe, metadata mode only
// enumerates, and the headroom guard keeps warmup from forcing eviction.

import (
	"context"
	"strings"
	"testing"

	"fuse-client/internal/coordinator"
)

// warmupCoordinator serves ListFileLocations from a fixed slice with prefix
// filtering; everything else is inherited no-op herdCoordinator behavior.
type warmupCoordinator struct {
	herdCoordinator
	locations []*coordinator.FileLocation
}

func (c *warmupCoordinator) ListFileLocations(_ context.Context, prefix string) ([]*coordinator.FileLocation, error) {
	var out []*coordinator.FileLocation
	for _, loc := range c.locations {
		if prefix == "" || strings.HasPrefix(loc.FilePath, prefix) {
			out = append(out, loc)
		}
	}
	return out, nil
}

func TestWarmPrefix_FullPullsWholeAndChunkedFiles(t *testing.T) {
	cm := evictTestManager(t, 1<<30)
	cm.config.ChunkSize = 1000
	ctx := context.Background()

	// Everything lives only in cloud, as after a fresh node join.
	cm.cloudStorage.Write(ctx, "/models/whole.bin", make([]byte, 3000))
	cm.cloudStorage.Write(ctx, chunkPathFor("/models/big.bin", 0), make([]byte, 1000))
	cm.cloudStorage.Write(ctx, chunkPathFor("/models/big.bin", 1), make([]byte, 500))
	cm.cloudStorage.Write(ctx, "/other/nope.bin", make([]byte, 100))

	coord := &warmupCoordinator{locations: []*coordinator.FileLocation{
		{FilePath: "/models/whole.bin", PeerID: "p1", StorageTier: "cloud", FileSize: 3000},
		{FilePath: "/models/big.bin", PeerID: "p1", StorageTier: "cloud", FileSize: 1500,
			IsChunked: true, Chunks: []coordinator.ChunkInfo{{}, {}}},
		// Chunk rows must be folded into the parent, not warmed separately.
		{FilePath: "/models/big.bin_chunk_0", PeerID: "p1", StorageTier: "cloud", FileSize: 1000},
		{FilePath: "/other/nope.bin", PeerID: "p1", StorageTier: "cloud", FileSize: 100},
	}}
	cm.config.Coordinator = coord

	res, err := cm.WarmPrefix(ctx, "/models", "full")
	if err != nil {
		t.Fatalf("WarmPrefix: %v", err)
	}
	cm.bgWg.Wait()
	if res.Files != 2 || res.Warmed != 2 || res.Failed != 0 {
		t.Fatalf("result = %+v, want 2 files / 2 warmed / 0 failed", res)
	}
	if _, ok := cm.LocalFilePath(ctx, "/models/whole.bin"); !ok {
		t.Fatal("whole.bin not local after warmup")
	}
	if _, ok := cm.LocalFilePath(ctx, "/models/big.bin"); !ok {
		t.Fatal("big.bin not assembled locally after warmup")
	}
	if cm.nvmeStorage.Exists(ctx, "/other/nope.bin") {
		t.Fatal("file outside the prefix was warmed")
	}

	// Second pass is a no-op: everything already local.
	res2, err := cm.WarmPrefix(ctx, "/models", "full")
	if err != nil {
		t.Fatalf("second WarmPrefix: %v", err)
	}
	if res2.AlreadyLocal != 2 || res2.Warmed != 0 {
		t.Fatalf("second pass = %+v, want 2 already-local / 0 warmed", res2)
	}
}

// TestWarmPrefix_ChunkedWithoutChunkList reproduces what the coordinator
// actually stores: publishFileLocation sets IsChunked but never enumerates
// Chunks, so numChunks must be derived from the size. Before that fallback a
// chunked file took the whole-file path and failed with BlobNotFound, since
// the parent object exists only as chunk_N blobs.
func TestWarmPrefix_ChunkedWithoutChunkList(t *testing.T) {
	cm := evictTestManager(t, 1<<30)
	cm.config.ChunkSize = 1000
	ctx := context.Background()

	// Only the chunks exist remotely — no parent object, as in production.
	cm.cloudStorage.Write(ctx, chunkPathFor("/models/big.bin", 0), make([]byte, 1000))
	cm.cloudStorage.Write(ctx, chunkPathFor("/models/big.bin", 1), make([]byte, 1000))
	cm.cloudStorage.Write(ctx, chunkPathFor("/models/big.bin", 2), make([]byte, 500))

	cm.config.Coordinator = &warmupCoordinator{locations: []*coordinator.FileLocation{
		// IsChunked true, Chunks empty: exactly what publishFileLocation writes.
		{FilePath: "/models/big.bin", PeerID: "p1", StorageTier: "nvme",
			FileSize: 2500, IsChunked: true},
	}}

	res, err := cm.WarmPrefixOpts(ctx, "/models", WarmupOptions{Mode: "full", Source: "cloud-only"})
	if err != nil {
		t.Fatalf("WarmPrefixOpts: %v", err)
	}
	if res.Warmed != 1 || res.Failed != 0 {
		t.Fatalf("result = %+v, want 1 warmed / 0 failed", res)
	}
	if _, ok := cm.LocalFilePath(ctx, "/models/big.bin"); !ok {
		t.Fatal("big.bin not assembled locally from derived chunk count")
	}
}

func TestWarmPrefix_MetadataModeAndHeadroomGuard(t *testing.T) {
	cm := evictTestManager(t, 2000)
	ctx := context.Background()

	cm.cloudStorage.Write(ctx, "/m/a.bin", make([]byte, 3000))
	coord := &warmupCoordinator{locations: []*coordinator.FileLocation{
		{FilePath: "/m/a.bin", PeerID: "p1", StorageTier: "cloud", FileSize: 3000},
	}}
	cm.config.Coordinator = coord

	// Metadata mode enumerates without moving bytes.
	res, err := cm.WarmPrefix(ctx, "/m", "metadata")
	if err != nil {
		t.Fatalf("metadata WarmPrefix: %v", err)
	}
	if res.Files != 1 || res.Warmed != 0 {
		t.Fatalf("metadata result = %+v, want 1 file / 0 warmed", res)
	}
	if cm.nvmeStorage.Exists(ctx, "/m/a.bin") {
		t.Fatal("metadata mode moved bytes")
	}

	// Full mode: 3000 bytes over a 2000-byte budget must be skipped, never
	// forced in via eviction.
	res, err = cm.WarmPrefix(ctx, "/m", "full")
	if err != nil {
		t.Fatalf("full WarmPrefix: %v", err)
	}
	if res.Skipped != 1 || res.Warmed != 0 {
		t.Fatalf("headroom result = %+v, want 1 skipped / 0 warmed", res)
	}

	// Unknown mode errors; none/empty are no-ops.
	if _, err := cm.WarmPrefix(ctx, "/m", "everything"); err == nil {
		t.Fatal("unknown warmup mode must error")
	}
	if res, err := cm.WarmPrefix(ctx, "/m", "none"); err != nil || res.Files != 0 {
		t.Fatalf("none mode = %+v err=%v, want empty no-op", res, err)
	}
}

func TestWarmupPlan_StrategyResolution(t *testing.T) {
	cases := []struct {
		source, bandwidth string
		wantOrder         []CacheTier // nil = adaptive
		wantFiles, wantWk int
		wantErr           bool
	}{
		{"", "", nil, 2, chunkCompletionFetchWorkers, false},
		{"peer-first", "background", nil, 2, chunkCompletionFetchWorkers, false},
		{"hybrid", "", nil, 2, chunkCompletionFetchWorkers, false},
		{"cloud-first", "max", []CacheTier{TierCloud, TierPeer}, 4, 16, false},
		{"cloud-only", "", []CacheTier{TierCloud}, 2, chunkCompletionFetchWorkers, false},
		{"warp-speed", "", nil, 0, 0, true},
		{"", "ludicrous", nil, 0, 0, true},
	}
	for _, tc := range cases {
		order, files, wk, err := warmupPlan(WarmupOptions{Source: tc.source, Bandwidth: tc.bandwidth})
		if tc.wantErr {
			if err == nil {
				t.Fatalf("(%q,%q): want error", tc.source, tc.bandwidth)
			}
			continue
		}
		if err != nil {
			t.Fatalf("(%q,%q): %v", tc.source, tc.bandwidth, err)
		}
		if len(order) != len(tc.wantOrder) || files != tc.wantFiles || wk != tc.wantWk {
			t.Fatalf("(%q,%q): order=%v files=%d workers=%d, want %v/%d/%d",
				tc.source, tc.bandwidth, order, files, wk, tc.wantOrder, tc.wantFiles, tc.wantWk)
		}
		for i := range order {
			if order[i] != tc.wantOrder[i] {
				t.Fatalf("(%q,%q): order=%v, want %v", tc.source, tc.bandwidth, order, tc.wantOrder)
			}
		}
	}
}

// TestWarmPrefixOpts_CloudOnlyNeverTouchesPeers: with source=cloud-only a
// file that exists only on the peer tier must fail rather than fall back.
func TestWarmPrefixOpts_CloudOnlyNeverTouchesPeers(t *testing.T) {
	cm := evictTestManager(t, 1<<30)
	ctx := context.Background()

	// File exists ONLY on the peer tier.
	cm.peerStorage.Write(ctx, "/m/peer-only.bin", make([]byte, 500))
	coord := &warmupCoordinator{locations: []*coordinator.FileLocation{
		{FilePath: "/m/peer-only.bin", PeerID: "p1", StorageTier: "nvme", FileSize: 500},
	}}
	cm.config.Coordinator = coord

	res, err := cm.WarmPrefixOpts(ctx, "/m", WarmupOptions{Mode: "full", Source: "cloud-only"})
	if err != nil {
		t.Fatalf("WarmPrefixOpts: %v", err)
	}
	if res.Failed != 1 || res.Warmed != 0 {
		t.Fatalf("cloud-only result = %+v, want 1 failed / 0 warmed", res)
	}

	// Default (peer-first) succeeds against the same layout.
	res, err = cm.WarmPrefixOpts(ctx, "/m", WarmupOptions{Mode: "full", Bandwidth: "max"})
	if err != nil {
		t.Fatalf("default WarmPrefixOpts: %v", err)
	}
	cm.bgWg.Wait()
	if res.Warmed != 1 || res.Failed != 0 {
		t.Fatalf("peer-first result = %+v, want 1 warmed", res)
	}
}

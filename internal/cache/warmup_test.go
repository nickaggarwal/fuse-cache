package cache

// Tests for declarative session warmup (WarmPrefix): full mode pulls whole
// and chunked files under the prefix into local NVMe, metadata mode only
// enumerates, and the headroom guard keeps warmup from forcing eviction.

import (
	"context"
	"strings"
	"sync"
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

// TestWarmPrefixOpts_ChunkLevelProgress covers the case per-file progress
// cannot report: a prefix holding one very large chunked file. With only
// file-boundary callbacks the job sits at Done=0 for the entire transfer and
// then jumps to 1, so an operator cannot tell a slow warm from a hung one.
func TestWarmPrefixOpts_ChunkLevelProgress(t *testing.T) {
	cm := evictTestManager(t, 1<<30)
	cm.config.ChunkSize = 1000
	ctx := context.Background()

	const numChunks = 8
	for i := int64(0); i < numChunks; i++ {
		cm.cloudStorage.Write(ctx, chunkPathFor("/models/huge.bin", i), make([]byte, 1000))
	}
	coord := &warmupCoordinator{locations: []*coordinator.FileLocation{
		{FilePath: "/models/huge.bin", PeerID: "p1", StorageTier: "cloud",
			FileSize: numChunks * 1000, IsChunked: true, ChunkSize: 1000},
	}}
	cm.config.Coordinator = coord

	var (
		mu    sync.Mutex
		snaps []WarmupProgress
	)
	res, err := cm.WarmPrefixOpts(ctx, "/models", WarmupOptions{
		Mode: "full",
		OnProgress: func(p WarmupProgress) {
			mu.Lock()
			snaps = append(snaps, p)
			mu.Unlock()
		},
	})
	if err != nil {
		t.Fatalf("WarmPrefixOpts: %v", err)
	}
	cm.bgWg.Wait()
	if res.Warmed != 1 {
		t.Fatalf("result = %+v, want 1 warmed", res)
	}

	mu.Lock()
	defer mu.Unlock()
	// Plan + one per chunk + the file-completion callback.
	if len(snaps) < numChunks+1 {
		t.Fatalf("got %d progress callbacks, want > %d (one per chunk)", len(snaps), numChunks)
	}

	// The whole point: movement while the single file is still incomplete.
	var midFlight int
	for _, s := range snaps {
		if s.Done == 0 && s.ChunksDone > 0 {
			midFlight++
		}
	}
	if midFlight == 0 {
		t.Fatal("no progress reported before the file completed — chunk granularity not working")
	}

	// Counters must be monotonic and land exactly on the planned total.
	var prev int64
	for i, s := range snaps {
		if s.ChunksDone < prev {
			t.Fatalf("snapshot %d went backwards: %d after %d", i, s.ChunksDone, prev)
		}
		prev = s.ChunksDone
	}
	final := snaps[len(snaps)-1]
	if final.Chunks != numChunks || final.ChunksDone != numChunks {
		t.Fatalf("final chunks = %d/%d, want %d/%d",
			final.ChunksDone, final.Chunks, numChunks, numChunks)
	}
	// In-flight bytes are backed out once the file's bytes move into Bytes,
	// so the two must never double-count at the end.
	if final.InFlightBytes != 0 {
		t.Fatalf("final in_flight_bytes = %d, want 0", final.InFlightBytes)
	}
	if final.Bytes != numChunks*1000 {
		t.Fatalf("final bytes = %d, want %d", final.Bytes, numChunks*1000)
	}
}

// TestWarmPrefixOpts_AlreadyLocalChunksNotCounted keeps the denominator
// honest on a resumed warm: chunks already on NVMe are not fetched, so
// counting them would leave ChunksDone permanently short of Chunks.
func TestWarmPrefixOpts_AlreadyLocalChunksNotCounted(t *testing.T) {
	cm := evictTestManager(t, 1<<30)
	cm.config.ChunkSize = 1000
	ctx := context.Background()

	const numChunks = 4
	for i := int64(0); i < numChunks; i++ {
		cm.cloudStorage.Write(ctx, chunkPathFor("/models/part.bin", i), make([]byte, 1000))
	}
	// Half the file already landed in an earlier, interrupted pass.
	for i := int64(0); i < 2; i++ {
		cm.nvmeStorage.Write(ctx, chunkPathFor("/models/part.bin", i), make([]byte, 1000))
	}
	cm.config.Coordinator = &warmupCoordinator{locations: []*coordinator.FileLocation{
		{FilePath: "/models/part.bin", PeerID: "p1", StorageTier: "cloud",
			FileSize: numChunks * 1000, IsChunked: true, ChunkSize: 1000},
	}}

	var last WarmupProgress
	var mu sync.Mutex
	if _, err := cm.WarmPrefixOpts(ctx, "/models", WarmupOptions{
		Mode: "full",
		OnProgress: func(p WarmupProgress) {
			mu.Lock()
			last = p
			mu.Unlock()
		},
	}); err != nil {
		t.Fatalf("WarmPrefixOpts: %v", err)
	}
	cm.bgWg.Wait()

	mu.Lock()
	defer mu.Unlock()
	if last.Chunks != 2 || last.ChunksDone != 2 {
		t.Fatalf("chunks = %d/%d, want 2/2 (only the missing half is fetched)",
			last.ChunksDone, last.Chunks)
	}
}

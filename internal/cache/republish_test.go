package cache

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"fuse-client/internal/coordinator"
)

// After a coordinator restart (a fresh in-memory store), RepublishLocal puts
// back a row for every whole local object with its size, a holder row for a
// parent held only as chunk files, and skips an unverified blob.
func TestRepublishLocalAfterCoordinatorRestart(t *testing.T) {
	cm, dir := newVerifyCM(t)
	ctx := context.Background()
	cm.config.LocalPeerID = "node-0"
	cm.config.Coordinator = coordinator.NewCoordinatorService()

	// A whole weights file.
	w := []byte(strings.Repeat("w", 5000))
	wf := filepath.Join(dir, "models", "m", "w.bin")
	_ = os.MkdirAll(filepath.Dir(wf), 0o755)
	if err := os.WriteFile(wf+".stage-t", w, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := cm.PutFromFile(ctx, "/models/m/w.bin", wf+".stage-t", int64(len(w)), time.Now()); err != nil {
		t.Fatal(err)
	}
	// A verified whole blob and an unverified (corrupted) one.
	good, gd := blobOf(2<<20, 21)
	putWhole(t, cm, dir, good, gd)
	cm.MarkBlobVerified(ociBlobPrefix + gd)
	bad, bd := blobOf(2<<20, 22)
	bfull := putWhole(t, cm, dir, bad, bd)
	corrupt(t, bfull, 0, 64)
	// A parent held only as chunk files.
	cp := "/models/m/big.bin_chunk_3"
	if err := cm.nvmeStorage.Write(ctx, cp, []byte("chunk")); err != nil {
		t.Fatal(err)
	}
	cm.mu.Lock()
	cm.entries.put(&CacheEntry{FilePath: cp, Size: 5, Tier: TierNVMe})
	cm.mu.Unlock()

	// Background publishes from the writes above read the coordinator; let
	// them finish before swapping it (the swap stands in for a restart).
	cm.bgWg.Wait()
	// The coordinator restarts empty.
	fresh := coordinator.NewCoordinatorService()
	cm.config.Coordinator = fresh
	cm.chunkAds = newChunkAdvertiser()
	n := cm.RepublishLocal(ctx)
	if n < 3 {
		t.Fatalf("republished %d rows", n)
	}
	get := func(p string) []*coordinator.FileLocation {
		l, _ := fresh.GetFileLocation(ctx, p)
		return l
	}
	if l := get("/models/m/w.bin"); len(l) != 1 || l[0].FileSize != int64(len(w)) {
		t.Fatalf("weights row: %+v", l)
	}
	if l := get(ociBlobPrefix + gd); len(l) != 1 || l[0].FileSize != int64(len(good)) {
		t.Fatalf("verified blob row: %+v", l)
	}
	if l := get(ociBlobPrefix + bd); len(l) != 0 {
		t.Fatalf("unverified blob was republished: %+v", l)
	}
	if l := get("/models/m/big.bin"); len(l) != 1 || !l[0].IsChunked {
		t.Fatalf("chunk-only parent not advertised: %+v", l)
	}
}

// A large whole file adopted by rehydration (no chunk shape) is republished
// chunked at the node's stride; a small one stays unchunked.
func TestRepublishRestoresChunkShape(t *testing.T) {
	cm, _ := newVerifyCM(t)
	big := cm.republishShape(&CacheEntry{FilePath: "/m/w.bin", Size: 3<<20 + 5, Tier: TierNVMe})
	if !big.IsChunked || big.ChunkSize != 1<<20 || big.NumChunks != 4 {
		t.Fatalf("big file shape %+v", big)
	}
	small := cm.republishShape(&CacheEntry{FilePath: "/m/c.json", Size: 600, Tier: TierNVMe})
	if small.IsChunked {
		t.Fatalf("small file marked chunked: %+v", small)
	}
	kept := cm.republishShape(&CacheEntry{FilePath: "/m/x.bin", Size: 9 << 20, IsChunked: true, ChunkSize: 4 << 20})
	if kept.ChunkSize != 4<<20 {
		t.Fatalf("published stride not kept: %+v", kept)
	}
}

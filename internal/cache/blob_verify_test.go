package cache

import (
	"bytes"

	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"fuse-client/internal/coordinator"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func newVerifyCM(t *testing.T) (*DefaultCacheManager, string) {
	t.Helper()
	dir := t.TempDir()
	cmI, err := NewCacheManager(&CacheConfig{NVMePath: dir, MaxNVMeSize: 256 << 20, ChunkSize: 1 << 20, CloudTimeout: time.Second, BlobScrubInterval: -1})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cmI.Shutdown)
	return cmI, dir
}

func blobOf(n int, seed byte) ([]byte, string) {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(i*31+int(seed)) ^ seed
	}
	s := sha256.Sum256(b)
	return b, hex.EncodeToString(s[:])
}

// putWhole lands a whole blob file the way a store holds it, indexed.
func putWhole(t *testing.T, cm *DefaultCacheManager, dir string, data []byte, d string) string {
	t.Helper()
	fp := ociBlobPrefix + d
	full := filepath.Join(dir, strings.TrimPrefix(fp, "/"))
	if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
		t.Fatal(err)
	}
	stage := full + ".stage-t"
	if err := os.WriteFile(stage, data, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := cm.PutFromFile(context.Background(), fp, stage, int64(len(data)), time.Now()); err != nil {
		t.Fatal(err)
	}
	return full
}

func corrupt(t *testing.T, full string, off int64, n int) {
	t.Helper()
	f, err := os.OpenFile(full, os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteAt(bytes.Repeat([]byte{0xA5}, n), off); err != nil {
		t.Fatal(err)
	}
	f.Close()
}

func corruptKeepMtime(t *testing.T, full string, off int64, n int) {
	t.Helper()
	info, _ := os.Stat(full)
	corrupt(t, full, off, n)
	if err := os.Chtimes(full, info.ModTime(), info.ModTime()); err != nil {
		t.Fatal(err)
	}
}

// A good whole blob verifies and is served; a same-size corrupted one is
// quarantined: moved aside, dropped from the index, not served to peers.
func TestBlobVerifyWholeGoodAndBad(t *testing.T) {
	cm, dir := newVerifyCM(t)
	ctx := context.Background()
	good, gd := blobOf(3<<20+5, 1)
	full := putWhole(t, cm, dir, good, gd)
	if v := cm.blobLocalVerdict(ctx, ociBlobPrefix+gd, full, true); v != blobGood {
		t.Fatalf("good blob verdict %v", v)
	}
	if _, ok := cm.LocalFilePath(ctx, ociBlobPrefix+gd); !ok {
		t.Fatal("verified blob not offered to peers")
	}
	if _, err := os.Stat(full + ".verified"); err != nil {
		t.Fatal("no marker for a verified blob")
	}

	bad, bd := blobOf(2<<20, 2)
	bfull := putWhole(t, cm, dir, bad, bd)
	corrupt(t, bfull, 100<<10, 64<<10)
	if v := cm.blobLocalVerdict(ctx, ociBlobPrefix+bd, bfull, true); v != blobBadMoved {
		t.Fatalf("corrupt blob verdict %v", v)
	}
	if _, err := os.Stat(bfull); !os.IsNotExist(err) {
		t.Fatal("corrupt blob still in place")
	}
	if m, _ := filepath.Glob(bfull + ".corrupt-*"); len(m) != 1 {
		t.Fatalf("corrupt copy not kept for inspection: %v", m)
	}
	if _, ok := cm.LookupEntry(ctx, ociBlobPrefix+bd, false); ok {
		t.Fatal("corrupt blob still indexed")
	}
	if _, ok := cm.LocalFilePath(ctx, ociBlobPrefix+bd); ok {
		t.Fatal("corrupt blob offered to peers")
	}
	if got := cm.metrics.BlobQuarantined.Load(); got != 1 {
		t.Fatalf("quarantined = %d", got)
	}
}

// The serve path never blocks on hashing: an unverified blob is refused
// this time and verified in the background, then served.
func TestBlobVerifyNonBlockingServe(t *testing.T) {
	cm, dir := newVerifyCM(t)
	ctx := context.Background()
	data, d := blobOf(4<<20, 3)
	full := putWhole(t, cm, dir, data, d)
	cm.blobVerify.forget(full)
	_ = os.Remove(full + ".verified")
	if _, ok := cm.LocalFilePath(ctx, ociBlobPrefix+d); ok {
		t.Fatal("unverified blob served before its check")
	}
	deadline := time.Now().Add(10 * time.Second)
	for {
		if _, ok := cm.LocalFilePath(ctx, ociBlobPrefix+d); ok {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("background check never made the blob servable")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// Rewriting a verified blob in place (new mtime) voids the verdict; the
// next check catches it.
func TestBlobVerifyRewriteVoidsVerdict(t *testing.T) {
	cm, dir := newVerifyCM(t)
	ctx := context.Background()
	data, d := blobOf(2<<20, 4)
	full := putWhole(t, cm, dir, data, d)
	if v := cm.blobLocalVerdict(ctx, ociBlobPrefix+d, full, true); v != blobGood {
		t.Fatal("setup")
	}
	time.Sleep(10 * time.Millisecond)
	corrupt(t, full, 0, 4096)
	future := time.Now().Add(time.Second)
	_ = os.Chtimes(full, future, future)
	if v := cm.blobLocalVerdict(ctx, ociBlobPrefix+d, full, true); v != blobBadMoved {
		t.Fatalf("rewritten blob verdict %v", v)
	}
}

// Corruption that keeps the mtime passes the per-version cache and marker;
// the forced scrub catches it.
func TestBlobScrubCatchesSameMtimeRot(t *testing.T) {
	cm, dir := newVerifyCM(t)
	ctx := context.Background()
	data, d := blobOf(2<<20, 5)
	full := putWhole(t, cm, dir, data, d)
	if v := cm.blobLocalVerdict(ctx, ociBlobPrefix+d, full, true); v != blobGood {
		t.Fatal("setup")
	}
	corruptKeepMtime(t, full, 1<<20, 512)
	if v := cm.blobLocalVerdict(ctx, ociBlobPrefix+d, full, true); v != blobGood {
		t.Fatal("expected the cached verdict to hold until a scrub")
	}
	checked, bad := cm.ScrubBlobs(ctx, true)
	if checked != 1 || bad != 1 {
		t.Fatalf("scrub checked=%d bad=%d", checked, bad)
	}
	if _, err := os.Stat(full); !os.IsNotExist(err) {
		t.Fatal("scrub left the rotten blob in place")
	}
}

// After a restart the marker spares a re-hash of a good blob, and a blob
// changed while the process was down is caught by the startup pass.
func TestBlobVerifyAcrossRestart(t *testing.T) {
	dir := t.TempDir()
	cfg := &CacheConfig{NVMePath: dir, MaxNVMeSize: 256 << 20, ChunkSize: 1 << 20, CloudTimeout: time.Second, BlobScrubInterval: -1}
	cmI, err := NewCacheManager(cfg)
	if err != nil {
		t.Fatal(err)
	}
	cm := cmI
	good, gd := blobOf(2<<20, 6)
	bad, bd := blobOf(2<<20, 7)
	putWhole(t, cm, dir, good, gd)
	bfull := putWhole(t, cm, dir, bad, bd)
	cm.MarkBlobVerified(ociBlobPrefix + gd)
	cm.MarkBlobVerified(ociBlobPrefix + bd)
	cm.Shutdown()
	time.Sleep(10 * time.Millisecond)
	corrupt(t, bfull, 0, 1024) // changed while down: new mtime voids its marker

	cmI2, err := NewCacheManager(cfg)
	if err != nil {
		t.Fatal(err)
	}
	cm2 := cmI2
	defer cm2.Shutdown()
	deadline := time.Now().Add(10 * time.Second)
	for cm2.metrics.BlobQuarantined.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if cm2.metrics.BlobQuarantined.Load() != 1 {
		t.Fatal("startup pass did not catch the blob changed while down")
	}
	if cm2.metrics.BlobVerified.Load() != 1 {
		t.Fatalf("startup re-hashed %d blobs; the good one had a valid marker", cm2.metrics.BlobVerified.Load())
	}
	if _, ok := cm2.LocalFilePath(context.Background(), ociBlobPrefix+gd); !ok {
		t.Fatal("good blob not servable after restart")
	}
}

// A blob a node assembles from chunk files is verified when the last chunk
// lands; a bad chunk quarantines the whole set.
func TestBlobVerifyChunkedSet(t *testing.T) {
	cm, dir := newVerifyCM(t)
	ctx := context.Background()
	for _, tc := range []struct {
		name    string
		corrupt bool
	}{{"good", false}, {"bad", true}} {
		data, d := blobOf(3<<20+100, map[bool]byte{false: 8, true: 9}[tc.corrupt])
		fp := ociBlobPrefix + d
		cs := int64(1 << 20)
		n := (int64(len(data)) + cs - 1) / cs
		var paths []string
		for i := int64(0); i < n; i++ {
			end := min((i+1)*cs, int64(len(data)))
			chunk := append([]byte(nil), data[i*cs:end]...)
			if tc.corrupt && i == 1 {
				chunk[10] ^= 0xFF
			}
			p := filepath.Join(dir, strings.TrimPrefix(fmt.Sprintf("%s_chunk_%d", fp, i), "/"))
			_ = os.MkdirAll(filepath.Dir(p), 0o755)
			if err := os.WriteFile(p, chunk, 0o644); err != nil {
				t.Fatal(err)
			}
			paths = append(paths, p)
		}
		v := cm.verifyChunkedBlob(ctx, fp, int64(len(data)), cs)
		want := map[bool]blobVerdict{false: blobGood, true: blobBadMoved}[tc.corrupt]
		if v != want {
			t.Fatalf("%s: verdict %v, want %v", tc.name, v, want)
		}
		for _, p := range paths {
			_, err := os.Stat(p)
			if tc.corrupt && !os.IsNotExist(err) {
				t.Fatalf("%s: chunk %s left in place", tc.name, p)
			}
			if !tc.corrupt && err != nil {
				t.Fatalf("%s: good chunk removed", tc.name)
			}
		}
	}
	// Incomplete sets are not judged.
	data, d := blobOf(2<<20+1, 10)
	p := filepath.Join(dir, strings.TrimPrefix(fmt.Sprintf("%s%s_chunk_0", ociBlobPrefix, d), "/"))
	_ = os.WriteFile(p, data[:1<<20], 0o644)
	if v := cm.verifyChunkedBlob(ctx, ociBlobPrefix+d, int64(len(data)), 1<<20); v != blobUnknown {
		t.Fatalf("incomplete set verdict %v", v)
	}
}

// Non-blob paths are never hashed or refused.
func TestBlobVerifyIgnoresOtherPaths(t *testing.T) {
	cm, dir := newVerifyCM(t)
	full := filepath.Join(dir, "models", "w.bin")
	_ = os.MkdirAll(filepath.Dir(full), 0o755)
	_ = os.WriteFile(full, []byte("weights"), 0o644)
	if _, ok := cm.LocalFilePath(context.Background(), "/models/w.bin"); !ok {
		t.Fatal("plain file refused")
	}
	if cm.metrics.BlobVerified.Load() != 0 {
		t.Fatal("plain file hashed")
	}
	for _, p := range []string{"/oci/sha256/abc", "/oci/sha256/" + strings.Repeat("a", 64) + "_chunk_0", "/models/x"} {
		if blobDigestFromPath(p) != "" {
			t.Errorf("blobDigestFromPath(%q) should be empty", p)
		}
	}
}

// A fresh process whose first verdict comes from a marker must not panic
// (the verifier's maps are created lazily).
func TestBlobVerifierZeroValueEnd(t *testing.T) {
	var v blobVerifier
	v.end("/x", blobVersion{size: 1, mtime: 2}, true)
	if !v.known("/x", blobVersion{size: 1, mtime: 2}) {
		t.Fatal("verdict not recorded")
	}
}

// A path with a sized row and a size-0 holder row (a node that advertised
// itself after landing one chunk) must resolve to the sized row, whatever
// the order: a size-0 shape made every read of the blob fail ("empty
// blob") once the store's sized row was retracted.
func TestFindInViewPrefersSizedRow(t *testing.T) {
	zero := &CacheEntry{FilePath: "/oci/sha256/x", Size: 0, IsChunked: true}
	sized := &CacheEntry{FilePath: "/oci/sha256/x", Size: 698568384, IsChunked: true, ChunkSize: 16 << 20}
	for _, view := range [][]*CacheEntry{{zero, sized}, {sized, zero}} {
		e, ok := findInView(view, "/oci/sha256/x")
		if !ok || e.Size != 698568384 {
			t.Fatalf("got %+v", e)
		}
	}
	if e, ok := findInView([]*CacheEntry{zero}, "/oci/sha256/x"); !ok || e.Size != 0 {
		t.Fatalf("a size-0 row alone must still prove the path exists: %+v %v", e, ok)
	}
}

// A blob placed on disk outside a PUT (a restore, or a copy a cluster only
// knows through size-0 holder rows) publishes a sized row once it verifies,
// so the cluster can read it again; lookups then see the real size.
func TestVerifiedBlobRepublishesSizedRow(t *testing.T) {
	cm, dir := newVerifyCM(t)
	ctx := context.Background()
	coord := coordinator.NewCoordinatorService()
	cm.config.Coordinator = coord
	cm.config.LocalPeerID = "store-0"
	data, d := blobOf(2<<20+3, 12)
	fp := ociBlobPrefix + d
	// Only a size-0 holder row is known, as on cpu-w3.
	_ = coord.UpdateFileLocation(ctx, &coordinator.FileLocation{FilePath: fp, PeerID: "node-a", StorageTier: "nvme", StoragePath: fp, FileSize: 0, IsChunked: true, ChunkSize: 1 << 20})
	full := filepath.Join(dir, strings.TrimPrefix(fp, "/"))
	_ = os.MkdirAll(filepath.Dir(full), 0o755)
	if err := os.WriteFile(full, data, 0o644); err != nil {
		t.Fatal(err)
	}
	if v := cm.blobLocalVerdict(ctx, fp, full, true); v != blobGood {
		t.Fatalf("verdict %v", v)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		e, ok, _ := cm.lookupRemoteEntry(ctx, fp)
		if ok && e.Size == int64(len(data)) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("no sized row after verification: %+v", e)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// Chunk completion of an OCI blob hashes while it assembles: a good set
// becomes a verified whole blob (servable at once), a set with a bad chunk
// is never committed and its chunks are quarantined.
func TestChunkCompletionVerifiesBlob(t *testing.T) {
	for _, bad := range []bool{false, true} {
		cm, dir := newVerifyCM(t)
		ctx := context.Background()
		data, d := blobOf(3<<20+77, map[bool]byte{false: 13, true: 14}[bad])
		fp := ociBlobPrefix + d
		cs := int64(1 << 20)
		n := (int64(len(data)) + cs - 1) / cs
		cm.mu.Lock()
		cm.entries.put(&CacheEntry{FilePath: fp, Size: int64(len(data)), IsChunked: true, NumChunks: n, ChunkSize: cs})
		cm.mu.Unlock()
		for i := int64(0); i < n; i++ {
			chunk := append([]byte(nil), data[i*cs:min((i+1)*cs, int64(len(data)))]...)
			if bad && i == 2 {
				chunk[5] ^= 0x55
			}
			cp := fmt.Sprintf("%s_chunk_%d", fp, i)
			if err := cm.nvmeStorage.Write(ctx, cp, chunk); err != nil {
				t.Fatal(err)
			}
			cm.mu.Lock()
			cm.entries.put(&CacheEntry{FilePath: cp, Size: int64(len(chunk)), Tier: TierNVMe})
			cm.mu.Unlock()
		}
		err := cm.runChunkCompletion(ctx, fp, n, int64(len(data)))
		full := filepath.Join(dir, strings.TrimPrefix(fp, "/"))
		if !bad {
			if err != nil {
				t.Fatalf("good set: %v", err)
			}
			if _, ok := cm.LocalFilePath(ctx, fp); !ok {
				t.Fatal("assembled good blob not servable at once")
			}
			continue
		}
		if err == nil {
			t.Fatal("bad set assembled without error")
		}
		if _, err := os.Stat(full); !os.IsNotExist(err) {
			t.Fatal("bad set committed as a whole blob")
		}
		if m, _ := filepath.Glob(filepath.Join(dir, "oci", "sha256", d+"_chunk_*")); len(m) != int(n) {
			t.Fatalf("expected %d chunk files kept aside, found %v", n, m)
		}
		for _, p := range func() []string { m, _ := filepath.Glob(filepath.Join(dir, "oci", "sha256", d+"_chunk_*")); return m }() {
			if !strings.Contains(p, ".corrupt-") {
				t.Fatalf("chunk %s still servable", p)
			}
		}
	}
}

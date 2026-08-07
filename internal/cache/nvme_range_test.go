package cache

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"log"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func newRangeReadManager(t testing.TB) (*DefaultCacheManager, string) {
	t.Helper()
	dir := t.TempDir()
	nvme, err := NewNVMeStorage(dir)
	if err != nil {
		t.Fatalf("NewNVMeStorage: %v", err)
	}
	shutdownCtx, cancel := context.WithCancel(context.Background())
	cm := &DefaultCacheManager{
		config: &CacheConfig{
			NVMePath:    dir,
			MaxNVMeSize: 1 << 30,
			ChunkSize:   8 * 1024 * 1024,
		},
		nvmeStorage:    nvme,
		peerStorage:    newMockStorage(),
		cloudStorage:   newMockStorage(),
		entries:        newEntryTable(),
		logger:         log.New(io.Discard, "", 0),
		metrics:        NewCacheMetrics(),
		rangeChunks:    make(map[string]*chunkFileCache),
		hybridHints:    make(map[string]hybridReadHint),
		tierPerf:       newTierPerfTracker(),
		shutdownCtx:    shutdownCtx,
		shutdownCancel: cancel,
	}
	// A remote-tier read promotes to NVMe in the background; drain it before
	// TempDir cleanup so the two do not race over the directory.
	t.Cleanup(cm.Shutdown)
	return cm, dir
}

// seedWholeFile lands payload on NVMe with a checksum sidecar and the matching
// cache entry, i.e. the state Put leaves behind for a file under the chunk size.
func seedWholeFile(t testing.TB, cm *DefaultCacheManager, dir, path string, payload []byte) {
	t.Helper()
	full := filepath.Join(dir, path[1:])
	if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(full, payload, 0o644); err != nil {
		t.Fatalf("write payload: %v", err)
	}
	sum := sha256.Sum256(payload)
	if err := os.WriteFile(full+".sha256", []byte(hex.EncodeToString(sum[:])), 0o644); err != nil {
		t.Fatalf("write sidecar: %v", err)
	}
	cm.entries.put(&CacheEntry{
		FilePath: path, StoragePath: path, Size: int64(len(payload)),
		Tier: TierNVMe, LastAccessed: time.Now(), Checksum: hex.EncodeToString(sum[:]),
	})
}

func seqPayload(n int) []byte {
	out := make([]byte, n)
	for i := range out {
		out[i] = byte(i * 7 % 251)
	}
	return out
}

// A sequential read through many small windows must move the file's own size
// off NVMe, not size-per-window. The old path answered each window with a
// whole-file read, so a 1MB file read in 128KiB windows moved 8MB.
func TestReadRange_WholeFileDoesNotAmplify(t *testing.T) {
	cm, dir := newRangeReadManager(t)
	const size = 1 << 20
	const window = 128 << 10
	payload := seqPayload(size)
	seedWholeFile(t, cm, dir, "/small.bin", payload)

	var got []byte
	for off := int64(0); off < size; off += window {
		data, err := cm.ReadRange(context.Background(), "/small.bin", off, window)
		if err != nil {
			t.Fatalf("ReadRange at %d: %v", off, err)
		}
		got = append(got, data...)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("reassembled %d bytes, want the original %d", len(got), len(payload))
	}
	if read := cm.metrics.NVMeReadBytes.Load(); read != size {
		t.Fatalf("moved %d bytes off NVMe to serve %d bytes of reads (%.1fx amplification)",
			read, size, float64(read)/float64(size))
	}
}

// The sidecar is checked once per version of the file, not once per read.
func TestReadRange_VerifiesChecksumOncePerVersion(t *testing.T) {
	cm, dir := newRangeReadManager(t)
	payload := seqPayload(64 << 10)
	seedWholeFile(t, cm, dir, "/v.bin", payload)
	ctx := context.Background()

	if _, err := cm.ReadRange(ctx, "/v.bin", 0, 4096); err != nil {
		t.Fatalf("first read: %v", err)
	}
	info, err := os.Stat(filepath.Join(dir, "v.bin"))
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	id := nvmeFileID{size: info.Size(), mtime: info.ModTime().UnixNano()}
	if !cm.nvmeVerify.verified("/v.bin", id) {
		t.Fatal("first read did not record the verification")
	}

	// Replacing the bytes invalidates the record, so the next read re-verifies
	// rather than trusting the previous approval.
	cm.invalidateRangeCache("/v.bin")
	if cm.nvmeVerify.verified("/v.bin", id) {
		t.Fatal("verification survived invalidation")
	}
}

// Corruption must still be caught. The ranged path reports the local copy as
// unusable so the read falls through to a remote tier rather than serving
// bytes that do not match the sidecar.
func TestReadRange_ChecksumMismatchRejectsLocalCopy(t *testing.T) {
	cm, dir := newRangeReadManager(t)
	payload := seqPayload(4096)
	seedWholeFile(t, cm, dir, "/bad.bin", payload)

	corrupt := append([]byte(nil), payload...)
	corrupt[0] ^= 0xff
	if err := os.WriteFile(filepath.Join(dir, "bad.bin"), corrupt, 0o644); err != nil {
		t.Fatalf("corrupt: %v", err)
	}

	if data, ok := cm.readWholeFileRange(context.Background(), "/bad.bin", 0, 4096); ok {
		t.Fatalf("served %d bytes from a copy that fails its checksum", len(data))
	}
	// ReadRange falls through to the whole-file path, which finds no good copy
	// on any tier.
	if _, err := cm.ReadRange(context.Background(), "/bad.bin", 0, 4096); err == nil {
		t.Fatal("ReadRange served a corrupt file")
	}
}

// A file with no sidecar is unverifiable, not corrupt: the whole-file path
// serves it, so the ranged path must too.
func TestReadRange_NoSidecarStillServes(t *testing.T) {
	cm, dir := newRangeReadManager(t)
	payload := seqPayload(2048)
	seedWholeFile(t, cm, dir, "/nosum.bin", payload)
	if err := os.Remove(filepath.Join(dir, "nosum.bin.sha256")); err != nil {
		t.Fatalf("remove sidecar: %v", err)
	}

	data, err := cm.ReadRange(context.Background(), "/nosum.bin", 512, 256)
	if err != nil {
		t.Fatalf("ReadRange: %v", err)
	}
	if !bytes.Equal(data, payload[512:768]) {
		t.Fatal("wrong bytes returned")
	}
}

func TestReadRange_WholeFileBoundaries(t *testing.T) {
	cm, dir := newRangeReadManager(t)
	payload := seqPayload(1000)
	seedWholeFile(t, cm, dir, "/b.bin", payload)
	ctx := context.Background()

	cases := []struct {
		name   string
		off    int64
		size   int
		expect []byte
	}{
		{"head", 0, 10, payload[:10]},
		{"middle", 400, 100, payload[400:500]},
		{"past EOF clamps", 900, 500, payload[900:]},
		{"at EOF", 1000, 100, []byte{}},
		{"beyond EOF", 5000, 100, []byte{}},
		{"zero size", 100, 0, []byte{}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := cm.ReadRange(ctx, "/b.bin", tc.off, tc.size)
			if err != nil {
				t.Fatalf("ReadRange: %v", err)
			}
			if !bytes.Equal(got, tc.expect) {
				t.Fatalf("got %d bytes, want %d", len(got), len(tc.expect))
			}
		})
	}
}

// A path this node does not hold locally must fall through to the tier path
// rather than being reported as a miss by the ranged read.
func TestReadRange_FallsThroughWhenNotLocal(t *testing.T) {
	cm, _ := newRangeReadManager(t)
	ctx := context.Background()
	payload := []byte("remote bytes")
	if err := cm.cloudStorage.Write(ctx, "/remote.bin", payload); err != nil {
		t.Fatalf("seed cloud: %v", err)
	}

	if _, ok := cm.readWholeFileRange(ctx, "/remote.bin", 0, 4); ok {
		t.Fatal("ranged path claimed a file it does not hold")
	}
	got, err := cm.ReadRange(ctx, "/remote.bin", 0, 6)
	if err != nil {
		t.Fatalf("ReadRange: %v", err)
	}
	if !bytes.Equal(got, payload[:6]) {
		t.Fatalf("got %q, want %q", got, payload[:6])
	}
}

func TestNVMeVerifier_BoundsItsSize(t *testing.T) {
	var v nvmeVerifier
	id := nvmeFileID{size: 1, mtime: 1}
	for i := 0; i < nvmeVerifyLimit+64; i++ {
		v.markVerified(string(rune('a'+i%26))+string(rune(i)), id)
	}
	v.mu.Lock()
	n := len(v.seen)
	v.mu.Unlock()
	if n > nvmeVerifyLimit {
		t.Fatalf("verifier holds %d records, limit is %d", n, nvmeVerifyLimit)
	}
}

func BenchmarkReadRangeWholeFile(b *testing.B) {
	cm, dir := newRangeReadManager(b)
	const size = 8 << 20
	const window = 128 << 10
	seedWholeFile(b, cm, dir, "/big.bin", seqPayload(size))
	ctx := context.Background()

	b.SetBytes(size)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for off := int64(0); off < size; off += window {
			if _, err := cm.ReadRange(ctx, "/big.bin", off, window); err != nil {
				b.Fatal(err)
			}
		}
	}
}

// The pre-ReadAt shape: every window re-reads and re-hashes the whole file.
func BenchmarkReadRangeWholeFileViaGet(b *testing.B) {
	cm, dir := newRangeReadManager(b)
	const size = 8 << 20
	const window = 128 << 10
	seedWholeFile(b, cm, dir, "/big.bin", seqPayload(size))
	ctx := context.Background()

	b.SetBytes(size)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for off := int64(0); off < size; off += window {
			got, err := cm.Get(ctx, "/big.bin")
			if err != nil {
				b.Fatal(err)
			}
			end := off + window
			if end > int64(len(got.Data)) {
				end = int64(len(got.Data))
			}
			_ = got.Data[off:end]
		}
	}
}

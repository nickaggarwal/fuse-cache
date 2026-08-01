package cache

import (
	"context"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

func newStreamPromoteManager(t testing.TB, maxNVMe int64) (*DefaultCacheManager, string) {
	t.Helper()
	dir := t.TempDir()
	nvme, err := NewNVMeStorage(dir)
	if err != nil {
		t.Fatalf("NewNVMeStorage: %v", err)
	}
	cm := &DefaultCacheManager{
		config:           &CacheConfig{NVMePath: dir, MaxNVMeSize: maxNVMe, ChunkSize: 8 * 1024 * 1024},
		nvmeStorage:      nvme,
		peerStorage:      newMockStorage(),
		cloudStorage:     newMockStorage(),
		entries:          make(map[string]*CacheEntry),
		logger:           log.New(io.Discard, "", 0),
		metrics:          NewCacheMetrics(),
		rangeChunks:      make(map[string]*chunkFileCache),
		hybridHints:      make(map[string]hybridReadHint),
		chunkPromoteGate: make(chan struct{}, chunkPromoteMaxConcurrent),
		tierPerf:         newTierPerfTracker(),
	}
	return cm, dir
}

// TestStreamPromotion_CommitLandsFileAndAccounting verifies a committed
// streaming promotion produces the file on disk, an NVMe entry, and correct
// usage accounting.
func TestStreamPromotion_CommitLandsFileAndAccounting(t *testing.T) {
	cm, dir := newStreamPromoteManager(t, 1<<30)
	payload := []byte(strings.Repeat("data", 1024)) // 4KiB

	promo, ok := cm.BeginPromotion("/f.bin", int64(len(payload)))
	if !ok {
		t.Fatal("BeginPromotion declined")
	}
	if _, err := promo.Write(payload); err != nil {
		t.Fatalf("Write: %v", err)
	}
	promo.Commit()

	got, err := os.ReadFile(filepath.Join(dir, "f.bin"))
	if err != nil {
		t.Fatalf("read landed file: %v", err)
	}
	if string(got) != string(payload) {
		t.Fatalf("landed bytes mismatch: %d vs %d", len(got), len(payload))
	}
	cm.mu.RLock()
	entry := cm.entries["/f.bin"]
	used := cm.nvmeUsed
	cm.mu.RUnlock()
	if entry == nil || entry.Tier != TierNVMe || entry.Size != int64(len(payload)) {
		t.Fatalf("entry = %+v, want NVMe entry of size %d", entry, len(payload))
	}
	if used != int64(len(payload)) {
		t.Fatalf("nvmeUsed = %d, want %d", used, len(payload))
	}
}

// TestStreamPromotion_ShortBodyNeverLands ensures a truncated stream commits
// nothing: no final file, no entry, no accounting.
func TestStreamPromotion_ShortBodyNeverLands(t *testing.T) {
	cm, dir := newStreamPromoteManager(t, 1<<30)

	promo, ok := cm.BeginPromotion("/short.bin", 100)
	if !ok {
		t.Fatal("BeginPromotion declined")
	}
	promo.Write([]byte("only-fifteen-b")) // 14 bytes < declared 100
	promo.Commit()

	if _, err := os.Stat(filepath.Join(dir, "short.bin")); !os.IsNotExist(err) {
		t.Fatalf("short.bin should not exist, stat err=%v", err)
	}
	cm.mu.RLock()
	_, exists := cm.entries["/short.bin"]
	used := cm.nvmeUsed
	cm.mu.RUnlock()
	if exists || used != 0 {
		t.Fatalf("no entry/accounting expected, exists=%v used=%d", exists, used)
	}
	// Temp staging file must be cleaned up.
	leftover, _ := filepath.Glob(filepath.Join(dir, ".nvme-stream-*"))
	if len(leftover) != 0 {
		t.Fatalf("leftover temp files: %v", leftover)
	}
}

// TestStreamPromotion_AbortCleansUp verifies Abort removes staging state.
func TestStreamPromotion_AbortCleansUp(t *testing.T) {
	cm, dir := newStreamPromoteManager(t, 1<<30)
	promo, ok := cm.BeginPromotion("/a.bin", 10)
	if !ok {
		t.Fatal("BeginPromotion declined")
	}
	promo.Write([]byte("12345"))
	promo.Abort()

	leftover, _ := filepath.Glob(filepath.Join(dir, ".nvme-stream-*"))
	if len(leftover) != 0 {
		t.Fatalf("leftover temp files after abort: %v", leftover)
	}
	// Gate slot must be released: a follow-up promotion should be admitted.
	if _, ok := cm.BeginPromotion("/b.bin", 10); !ok {
		t.Fatal("gate slot not released after Abort")
	}
}

// TestStreamPromotion_DeclinesUnderPressure verifies the 80% watermark and
// no-evict capacity rule.
func TestStreamPromotion_DeclinesUnderPressure(t *testing.T) {
	cm, _ := newStreamPromoteManager(t, 1000)
	cm.mu.Lock()
	cm.nvmeUsed = 900 // > 80% of 1000
	cm.mu.Unlock()
	if _, ok := cm.BeginPromotion("/hot.bin", 10); ok {
		t.Fatal("expected decline above pressure watermark")
	}

	cm.mu.Lock()
	cm.nvmeUsed = 500
	cm.mu.Unlock()
	if _, ok := cm.BeginPromotion("/big.bin", 600); ok {
		t.Fatal("expected decline when size would exceed capacity")
	}
	if _, ok := cm.BeginPromotion("/fits.bin", 100); !ok {
		t.Fatal("expected admit when object fits below watermark")
	}
}

// TestStreamPromotion_SkipsAlreadyLocal verifies an NVMe-resident object of
// the same size is not re-promoted.
func TestStreamPromotion_SkipsAlreadyLocal(t *testing.T) {
	cm, _ := newStreamPromoteManager(t, 1<<30)
	payload := []byte("hello world")
	if err := cm.nvmeStorage.Write(context.Background(), "/dup.bin", payload); err != nil {
		t.Fatalf("seed write: %v", err)
	}
	cm.mu.Lock()
	cm.entries["/dup.bin"] = &CacheEntry{FilePath: "/dup.bin", Size: int64(len(payload)), Tier: TierNVMe}
	cm.mu.Unlock()

	if _, ok := cm.BeginPromotion("/dup.bin", int64(len(payload))); ok {
		t.Fatal("expected decline for already-local object")
	}
}

// TestReadFromPeerRaw_StreamsToNVMe is the end-to-end tee test: a raw HTTP
// peer read returns the payload AND lands it on local NVMe during the same
// transfer.
func TestReadFromPeerRaw_StreamsToNVMe(t *testing.T) {
	cm, dir := newStreamPromoteManager(t, 1<<30)
	payload := []byte(strings.Repeat("chunkdata!", 100_000)) // ~1MB
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", strconv.Itoa(len(payload)))
		w.WriteHeader(http.StatusOK)
		w.Write(payload)
	}))
	defer srv.Close()

	ps := newRawPeerStorage()
	ps.promoteSink = cm
	addr := strings.TrimPrefix(srv.URL, "http://")
	data, err := ps.readFromPeerRaw(context.Background(), addr, "/parent.bin_chunk_0")
	if err != nil {
		t.Fatalf("readFromPeerRaw: %v", err)
	}
	if string(data) != string(payload) {
		t.Fatalf("returned bytes mismatch: %d vs %d", len(data), len(payload))
	}

	landed, err := os.ReadFile(filepath.Join(dir, "parent.bin_chunk_0"))
	if err != nil {
		t.Fatalf("chunk not landed on NVMe: %v", err)
	}
	if string(landed) != string(payload) {
		t.Fatalf("landed bytes mismatch: %d vs %d", len(landed), len(payload))
	}
	// And the background promotion path must now be a no-op.
	if !cm.alreadyLocalNVMe(context.Background(), "/parent.bin_chunk_0", int64(len(payload))) {
		t.Fatal("alreadyLocalNVMe = false after streamed promotion")
	}
}

// TestReadFromPeerRaw_PromotionFailureDoesNotFailRead simulates a sink whose
// writes fail mid-stream; the read must still return the full payload.
func TestReadFromPeerRaw_PromotionFailureDoesNotFailRead(t *testing.T) {
	payload := []byte(strings.Repeat("z", 64*1024))
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", strconv.Itoa(len(payload)))
		w.WriteHeader(http.StatusOK)
		w.Write(payload)
	}))
	defer srv.Close()

	ps := newRawPeerStorage()
	ps.promoteSink = failingSink{}
	addr := strings.TrimPrefix(srv.URL, "http://")
	data, err := ps.readFromPeerRaw(context.Background(), addr, "/f.bin")
	if err != nil {
		t.Fatalf("read must survive promotion failure: %v", err)
	}
	if string(data) != string(payload) {
		t.Fatalf("payload mismatch: %d vs %d", len(data), len(payload))
	}
}

type failingSink struct{}

func (failingSink) BeginPromotion(path string, size int64) (StreamPromotion, bool) {
	return &failingPromotion{}, true
}

type failingPromotion struct{ aborted bool }

func (p *failingPromotion) Write(b []byte) (int, error) { return 0, io.ErrClosedPipe }
func (p *failingPromotion) Commit()                     {}
func (p *failingPromotion) Abort()                      { p.aborted = true }

// TestPromoteToNVMe_SkipsWhenStreamed verifies the background promotion
// goroutine short-circuits after a tee already landed the object.
func TestPromoteToNVMe_SkipsWhenStreamed(t *testing.T) {
	cm, dir := newStreamPromoteManager(t, 1<<30)
	payload := []byte("streamed-bytes")

	promo, ok := cm.BeginPromotion("/s.bin", int64(len(payload)))
	if !ok {
		t.Fatal("BeginPromotion declined")
	}
	promo.Write(payload)
	promo.Commit()

	// Corrupt-detector: make the landed file's mtime observable, then run the
	// legacy promotion; if it rewrote the file the inode would change.
	before, err := os.Stat(filepath.Join(dir, "s.bin"))
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	time.Sleep(10 * time.Millisecond)
	cm.promoteToNVMe(context.Background(), &CacheEntry{
		FilePath: "/s.bin", StoragePath: "/s.bin",
		Size: int64(len(payload)), Data: payload, Tier: TierPeer,
	})
	after, err := os.Stat(filepath.Join(dir, "s.bin"))
	if err != nil {
		t.Fatalf("stat after: %v", err)
	}
	if !after.ModTime().Equal(before.ModTime()) {
		t.Fatal("promoteToNVMe rewrote a file the stream already landed")
	}
	cm.mu.RLock()
	used := cm.nvmeUsed
	cm.mu.RUnlock()
	if used != int64(len(payload)) {
		t.Fatalf("nvmeUsed double-counted: %d, want %d", used, len(payload))
	}
}

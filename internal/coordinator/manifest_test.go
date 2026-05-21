package coordinator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"sync"
	"testing"
)

// fakeCloud is a CloudReader backed by an in-memory map.
type fakeCloud struct {
	mu     sync.Mutex
	blobs  map[string][]byte
	errors map[string]error
}

func newFakeCloud() *fakeCloud {
	return &fakeCloud{
		blobs:  make(map[string][]byte),
		errors: make(map[string]error),
	}
}

func (f *fakeCloud) put(path string, data []byte) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.blobs[path] = data
}

func (f *fakeCloud) putError(path string, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.errors[path] = err
}

func (f *fakeCloud) Read(_ context.Context, path string) ([]byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err, ok := f.errors[path]; ok {
		return nil, err
	}
	b, ok := f.blobs[path]
	if !ok {
		return nil, fmt.Errorf("fakeCloud: not found: %s", path)
	}
	return b, nil
}

// fakePeer is an httptest server that accepts PUTs to /api/files/... and
// records the paths and bodies received.
type fakePeer struct {
	server *httptest.Server
	mu     sync.Mutex
	puts   map[string][]byte
}

func newFakePeer() *fakePeer {
	p := &fakePeer{puts: make(map[string][]byte)}
	p.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		filePath := strings.TrimPrefix(r.URL.Path, "/api/files/")
		body, _ := io.ReadAll(r.Body)
		p.mu.Lock()
		p.puts[filePath] = body
		p.mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	return p
}

func (p *fakePeer) addr() string  { return strings.TrimPrefix(p.server.URL, "http://") }
func (p *fakePeer) close()        { p.server.Close() }
func (p *fakePeer) putCount() int { p.mu.Lock(); defer p.mu.Unlock(); return len(p.puts) }

func (p *fakePeer) gotPut(path string) ([]byte, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	b, ok := p.puts[path]
	return b, ok
}

func (p *fakePeer) paths() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]string, 0, len(p.puts))
	for k := range p.puts {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func TestBuildManifest_Empty(t *testing.T) {
	cs := NewCoordinatorService()
	m, err := cs.BuildManifest(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(m.Files) != 0 {
		t.Errorf("expected empty manifest, got %d files", len(m.Files))
	}
}

func TestBuildManifest_DedupByPathMaxSize(t *testing.T) {
	cs := NewCoordinatorService()
	ctx := context.Background()

	// Two replicas of the same file with different reported sizes.
	if err := cs.UpdateFileLocation(ctx, &FileLocation{FilePath: "a.txt", PeerID: "p1", StorageTier: "nvme", FileSize: 100}); err != nil {
		t.Fatal(err)
	}
	if err := cs.UpdateFileLocation(ctx, &FileLocation{FilePath: "a.txt", PeerID: "p2", StorageTier: "nvme", FileSize: 200}); err != nil {
		t.Fatal(err)
	}
	if err := cs.UpdateFileLocation(ctx, &FileLocation{FilePath: "b.txt", PeerID: "p1", StorageTier: "nvme", FileSize: 50}); err != nil {
		t.Fatal(err)
	}

	m, err := cs.BuildManifest(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(m.Files) != 2 {
		t.Fatalf("expected 2 unique files, got %d", len(m.Files))
	}
	// Sorted ascending: a.txt then b.txt.
	if m.Files[0].Path != "a.txt" || m.Files[0].Size != 200 {
		t.Errorf("a.txt: want size 200, got %+v", m.Files[0])
	}
	if m.Files[1].Path != "b.txt" || m.Files[1].Size != 50 {
		t.Errorf("b.txt: want size 50, got %+v", m.Files[1])
	}
}

func TestBuildManifest_Chunked(t *testing.T) {
	cs := NewCoordinatorService()
	ctx := context.Background()
	if err := cs.UpdateFileLocation(ctx, &FileLocation{
		FilePath:    "big.parquet",
		PeerID:      "p1",
		StorageTier: "nvme",
		FileSize:    12 * 1024 * 1024,
		IsChunked:   true,
		Chunks:      []ChunkInfo{{Index: 0}, {Index: 1}, {Index: 2}},
	}); err != nil {
		t.Fatal(err)
	}
	m, err := cs.BuildManifest(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(m.Files) != 1 {
		t.Fatalf("expected 1 file, got %d", len(m.Files))
	}
	if !m.Files[0].IsChunked {
		t.Error("expected IsChunked=true")
	}
	if m.Files[0].ChunkCount != 3 {
		t.Errorf("expected ChunkCount=3, got %d", m.Files[0].ChunkCount)
	}
}

func TestRestoreFromManifest_NilArgs(t *testing.T) {
	cs := NewCoordinatorService()
	if _, err := cs.RestoreFromManifest(context.Background(), nil, newFakeCloud(), RestoreOptions{}); err == nil {
		t.Error("expected error for nil manifest")
	}
	if _, err := cs.RestoreFromManifest(context.Background(), &FileManifest{}, nil, RestoreOptions{}); err == nil {
		t.Error("expected error for nil cloud")
	}
}

func TestRestoreFromManifest_NoActivePeers(t *testing.T) {
	cs := NewCoordinatorService()
	cloud := newFakeCloud()
	cloud.put("a.txt", []byte("hello"))

	manifest := &FileManifest{Files: []ManifestEntry{{Path: "a.txt", Size: 5}}}
	result, err := cs.RestoreFromManifest(context.Background(), manifest, cloud, RestoreOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if result.SeededFiles != 0 || len(result.FailedFiles) != 1 {
		t.Errorf("expected 0 seeded + 1 failed, got seeded=%d failed=%v", result.SeededFiles, result.FailedFiles)
	}
}

func TestRestoreFromManifest_SingleFile(t *testing.T) {
	cs := NewCoordinatorService()
	ctx := context.Background()

	peer := newFakePeer()
	defer peer.close()
	if err := cs.RegisterPeer(ctx, &PeerInfo{ID: "peer-1", Address: peer.addr()}); err != nil {
		t.Fatal(err)
	}

	cloud := newFakeCloud()
	cloud.put("a.txt", []byte("hello"))

	manifest := &FileManifest{Files: []ManifestEntry{{Path: "a.txt", Size: 5}}}
	result, err := cs.RestoreFromManifest(ctx, manifest, cloud, RestoreOptions{SeedPercentage: 100})
	if err != nil {
		t.Fatal(err)
	}
	if result.SeededFiles != 1 {
		t.Fatalf("expected 1 seeded, got %d (failed=%v)", result.SeededFiles, result.FailedFiles)
	}
	body, ok := peer.gotPut("a.txt")
	if !ok || string(body) != "hello" {
		t.Errorf("peer didn't receive a.txt: ok=%v body=%q", ok, string(body))
	}
}

func TestRestoreFromManifest_Chunked(t *testing.T) {
	cs := NewCoordinatorService()
	ctx := context.Background()

	peer := newFakePeer()
	defer peer.close()
	if err := cs.RegisterPeer(ctx, &PeerInfo{ID: "peer-1", Address: peer.addr()}); err != nil {
		t.Fatal(err)
	}

	cloud := newFakeCloud()
	cloud.put("big.parquet_chunk_0", []byte("AAA"))
	cloud.put("big.parquet_chunk_1", []byte("BBB"))

	manifest := &FileManifest{Files: []ManifestEntry{{
		Path: "big.parquet", Size: 6, IsChunked: true, ChunkCount: 2,
	}}}
	result, err := cs.RestoreFromManifest(ctx, manifest, cloud, RestoreOptions{SeedPercentage: 100})
	if err != nil {
		t.Fatal(err)
	}
	if result.SeededFiles != 1 {
		t.Errorf("expected 1 seeded, got %d (failed=%v)", result.SeededFiles, result.FailedFiles)
	}
	if body, ok := peer.gotPut("big.parquet_chunk_0"); !ok || string(body) != "AAA" {
		t.Errorf("chunk_0: ok=%v body=%q", ok, string(body))
	}
	if body, ok := peer.gotPut("big.parquet_chunk_1"); !ok || string(body) != "BBB" {
		t.Errorf("chunk_1: ok=%v body=%q", ok, string(body))
	}
}

func TestRestoreFromManifest_SeedPercentageHalf(t *testing.T) {
	cs := NewCoordinatorService()
	ctx := context.Background()

	peers := make([]*fakePeer, 4)
	for i := range peers {
		peers[i] = newFakePeer()
		defer peers[i].close()
		if err := cs.RegisterPeer(ctx, &PeerInfo{ID: fmt.Sprintf("peer-%d", i), Address: peers[i].addr()}); err != nil {
			t.Fatal(err)
		}
	}

	cloud := newFakeCloud()
	cloud.put("a.txt", []byte("X"))

	// 50% of 4 active peers = ceil(2) = 2 PUTs.
	manifest := &FileManifest{Files: []ManifestEntry{{Path: "a.txt", Size: 1}}}
	result, err := cs.RestoreFromManifest(ctx, manifest, cloud, RestoreOptions{SeedPercentage: 50})
	if err != nil {
		t.Fatal(err)
	}
	if result.SeededFiles != 1 {
		t.Errorf("expected 1 seeded file, got %d", result.SeededFiles)
	}
	totalPuts := 0
	for _, p := range peers {
		totalPuts += p.putCount()
	}
	if totalPuts != 2 {
		t.Errorf("expected 2 PUTs total (50%% of 4 peers), got %d", totalPuts)
	}
}

func TestRestoreFromManifest_CloudReadError(t *testing.T) {
	cs := NewCoordinatorService()
	ctx := context.Background()

	peer := newFakePeer()
	defer peer.close()
	if err := cs.RegisterPeer(ctx, &PeerInfo{ID: "peer-1", Address: peer.addr()}); err != nil {
		t.Fatal(err)
	}

	cloud := newFakeCloud()
	cloud.putError("missing.txt", errors.New("blob not found"))

	manifest := &FileManifest{Files: []ManifestEntry{{Path: "missing.txt", Size: 1}}}
	result, err := cs.RestoreFromManifest(ctx, manifest, cloud, RestoreOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if result.SeededFiles != 0 || len(result.FailedFiles) != 1 {
		t.Errorf("expected 0 seeded + 1 failed, got %+v", result)
	}
	if peer.putCount() != 0 {
		t.Errorf("peer should not have received any PUTs, got %d", peer.putCount())
	}
}

// Smoke test: build manifest, marshal to JSON, unmarshal, restore. Round-trips
// through the wire format to catch encoding bugs and verify chunked +
// non-chunked entries seed correctly end-to-end.
func TestSnapshotRestoreRoundTrip(t *testing.T) {
	src := NewCoordinatorService()
	ctx := context.Background()

	if err := src.UpdateFileLocation(ctx, &FileLocation{
		FilePath: "a.txt", PeerID: "p1", StorageTier: "nvme", FileSize: 5,
	}); err != nil {
		t.Fatal(err)
	}
	if err := src.UpdateFileLocation(ctx, &FileLocation{
		FilePath: "big.bin", PeerID: "p1", StorageTier: "nvme",
		FileSize: 10, IsChunked: true,
		Chunks:   []ChunkInfo{{Index: 0}, {Index: 1}},
	}); err != nil {
		t.Fatal(err)
	}

	manifest, err := src.BuildManifest(ctx)
	if err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}

	// Restore side: fresh coordinator, one active peer, cloud with all bytes.
	dst := NewCoordinatorService()
	peer := newFakePeer()
	defer peer.close()
	if err := dst.RegisterPeer(ctx, &PeerInfo{ID: "peer-1", Address: peer.addr()}); err != nil {
		t.Fatal(err)
	}

	cloud := newFakeCloud()
	cloud.put("a.txt", []byte("hello"))
	cloud.put("big.bin_chunk_0", []byte("AAAAA"))
	cloud.put("big.bin_chunk_1", []byte("BBBBB"))

	var restored FileManifest
	if err := json.Unmarshal(data, &restored); err != nil {
		t.Fatalf("unmarshal manifest: %v", err)
	}

	result, err := dst.RestoreFromManifest(ctx, &restored, cloud, RestoreOptions{SeedPercentage: 100})
	if err != nil {
		t.Fatal(err)
	}
	if result.SeededFiles != 2 {
		t.Fatalf("expected 2 seeded files, got %+v", result)
	}

	gotPaths := peer.paths()
	wantPaths := []string{"a.txt", "big.bin_chunk_0", "big.bin_chunk_1"}
	if strings.Join(gotPaths, ",") != strings.Join(wantPaths, ",") {
		t.Errorf("peer received %v, want %v", gotPaths, wantPaths)
	}
	if body, _ := peer.gotPut("a.txt"); string(body) != "hello" {
		t.Errorf("a.txt body: %q", string(body))
	}
	if body, _ := peer.gotPut("big.bin_chunk_0"); string(body) != "AAAAA" {
		t.Errorf("chunk_0 body: %q", string(body))
	}
	if body, _ := peer.gotPut("big.bin_chunk_1"); string(body) != "BBBBB" {
		t.Errorf("chunk_1 body: %q", string(body))
	}
}

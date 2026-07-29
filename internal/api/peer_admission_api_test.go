package api

import (
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"fuse-client/internal/cache"
)

// gatedPeerCache is a fakePeerCache that also implements cache.PeerServeAdmitter
// with a fixed capacity, mimicking the DefaultCacheManager serve gate.
type gatedPeerCache struct {
	fakePeerCache
	mu       sync.Mutex
	capacity int
	inUse    int
	rejected int
}

func (g *gatedPeerCache) TryAcquirePeerServe() (func(), bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.inUse >= g.capacity {
		g.rejected++
		return nil, false
	}
	g.inUse++
	var once sync.Once
	return func() {
		once.Do(func() {
			g.mu.Lock()
			g.inUse--
			g.mu.Unlock()
		})
	}, true
}

func TestHandlePeerRead_BusyReturns503(t *testing.T) {
	dir := t.TempDir()
	local := filepath.Join(dir, "f.bin")
	if err := os.WriteFile(local, []byte("data"), 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}

	fc := &gatedPeerCache{
		fakePeerCache: fakePeerCache{
			files:  map[string]string{"/f.bin": local},
			chunks: map[string]chunkLoc{},
		},
		capacity: 0, // gate always full
	}
	h := NewHandler(fc, nil, "peer-test", "")
	srv := httptest.NewServer(h.SetupRoutes())
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/api/peer/read?path=/f.bin")
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()

	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", resp.StatusCode)
	}
	if resp.Header.Get("Retry-After") == "" {
		t.Fatal("503 should carry Retry-After")
	}
	if fc.rejected != 1 {
		t.Fatalf("rejected = %d, want 1", fc.rejected)
	}
}

func TestHandlePeerRead_AdmittedServesAndReleases(t *testing.T) {
	dir := t.TempDir()
	local := filepath.Join(dir, "f.bin")
	content := []byte("served under admission")
	if err := os.WriteFile(local, content, 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}

	fc := &gatedPeerCache{
		fakePeerCache: fakePeerCache{
			files:  map[string]string{"/f.bin": local},
			chunks: map[string]chunkLoc{},
		},
		capacity: 1,
	}
	h := NewHandler(fc, nil, "peer-test", "")
	srv := httptest.NewServer(h.SetupRoutes())
	defer srv.Close()

	// Two sequential reads must both succeed — proving the slot is released
	// after each request rather than leaking.
	for i := 0; i < 2; i++ {
		resp, err := http.Get(srv.URL + "/api/peer/read?path=/f.bin")
		if err != nil {
			t.Fatalf("GET %d: %v", i, err)
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("GET %d status = %d, want 200", i, resp.StatusCode)
		}
		if string(body) != string(content) {
			t.Fatalf("GET %d body = %q", i, body)
		}
	}

	fc.mu.Lock()
	defer fc.mu.Unlock()
	if fc.inUse != 0 {
		t.Fatalf("inUse after requests = %d, want 0", fc.inUse)
	}
}

var _ cache.PeerServeAdmitter = (*gatedPeerCache)(nil)

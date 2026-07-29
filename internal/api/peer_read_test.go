package api

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"fuse-client/internal/cache"
)

type chunkLoc struct {
	local  string
	off    int64
	length int64
}

// fakePeerCache satisfies cache.CacheManager (embedded, unused methods) plus the
// localChunkServer interface the peer-read handler needs.
type fakePeerCache struct {
	cache.CacheManager
	files  map[string]string
	chunks map[string]chunkLoc
}

func (f *fakePeerCache) LocalFilePath(_ context.Context, p string) (string, bool) {
	lp, ok := f.files[p]
	return lp, ok
}

func (f *fakePeerCache) LocalChunkFile(_ context.Context, p string) (string, int64, int64, bool) {
	c, ok := f.chunks[p]
	if !ok {
		return "", 0, 0, false
	}
	return c.local, c.off, c.length, true
}

func newPeerReadServer(t *testing.T, fc *fakePeerCache, apiKey string) *httptest.Server {
	t.Helper()
	h := NewHandler(fc, nil, "peer-test", apiKey)
	return httptest.NewServer(h.SetupRoutes())
}

func TestHandlePeerRead_WholeAndRange(t *testing.T) {
	dir := t.TempDir()
	whole := filepath.Join(dir, "whole.bin")
	content := []byte("ABCDEFGHIJKLMNOP") // 16 bytes
	if err := os.WriteFile(whole, content, 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}
	fc := &fakePeerCache{
		files:  map[string]string{"/whole.bin": whole},
		chunks: map[string]chunkLoc{"/big.bin_chunk_2": {local: whole, off: 8, length: 4}}, // "IJKL"
	}
	srv := newPeerReadServer(t, fc, "")
	defer srv.Close()

	// Whole file.
	body, cl := getRaw(t, srv.URL+"/api/peer/read?path=/whole.bin", "")
	if string(body) != string(content) {
		t.Fatalf("whole = %q, want %q", body, content)
	}
	if cl != int64(len(content)) {
		t.Fatalf("whole Content-Length = %d, want %d", cl, len(content))
	}

	// Chunk range -> bytes [8,12) = "IJKL".
	body, cl = getRaw(t, srv.URL+"/api/peer/read?path=/big.bin_chunk_2", "")
	if string(body) != "IJKL" {
		t.Fatalf("range = %q, want IJKL", body)
	}
	if cl != 4 {
		t.Fatalf("range Content-Length = %d, want 4", cl)
	}
}

func TestHandlePeerRead_NotFoundAndTraversal(t *testing.T) {
	fc := &fakePeerCache{files: map[string]string{}, chunks: map[string]chunkLoc{}}
	srv := newPeerReadServer(t, fc, "")
	defer srv.Close()

	for _, p := range []string{"/does-not-exist", "/../../etc/passwd", ""} {
		url := srv.URL + "/api/peer/read?path=" + p
		resp, err := http.Get(url)
		if err != nil {
			t.Fatalf("GET %q: %v", p, err)
		}
		resp.Body.Close()
		if resp.StatusCode == http.StatusOK {
			t.Fatalf("path %q returned 200, want non-200", p)
		}
	}
}

func TestHandlePeerRead_AuthRequired(t *testing.T) {
	dir := t.TempDir()
	whole := filepath.Join(dir, "w.bin")
	if err := os.WriteFile(whole, []byte("data"), 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}
	fc := &fakePeerCache{files: map[string]string{"/w.bin": whole}, chunks: map[string]chunkLoc{}}
	srv := newPeerReadServer(t, fc, "secret-key")
	defer srv.Close()

	// No key -> 401.
	resp, err := http.Get(srv.URL + "/api/peer/read?path=/w.bin")
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("no-key status = %d, want 401", resp.StatusCode)
	}

	// Correct key -> 200 with data.
	body, _ := getRaw(t, srv.URL+"/api/peer/read?path=/w.bin", "secret-key")
	if string(body) != "data" {
		t.Fatalf("with-key body = %q, want data", body)
	}
}

func getRaw(t *testing.T, url, apiKey string) ([]byte, int64) {
	t.Helper()
	req, _ := http.NewRequest(http.MethodGet, url, nil)
	if apiKey != "" {
		req.Header.Set("X-API-Key", apiKey)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET %s status %d", url, resp.StatusCode)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	return body, resp.ContentLength
}

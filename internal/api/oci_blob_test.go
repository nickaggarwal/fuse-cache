package api

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"fuse-client/internal/cache"
)

// rangeCache is a CacheManager that also answers LookupEntry and ReadRange,
// like DefaultCacheManager.
type rangeCache struct {
	cache.CacheManager
	files map[string][]byte
}

func (c *rangeCache) LookupEntry(_ context.Context, p string, _ bool) (*cache.CacheEntry, bool) {
	b, ok := c.files[p]
	if !ok {
		return nil, false
	}
	return &cache.CacheEntry{FilePath: p, Size: int64(len(b))}, true
}

func (c *rangeCache) ReadRange(_ context.Context, p string, off int64, n int) ([]byte, error) {
	b := c.files[p]
	end := off + int64(n)
	if end > int64(len(b)) {
		end = int64(len(b))
	}
	return b[off:end], nil
}

func TestOCIBlob(t *testing.T) {
	digest := strings.Repeat("ab", 32)
	blob := bytes.Repeat([]byte("0123456789"), 10)
	h := NewHandler(&rangeCache{files: map[string][]byte{OCIBlobPrefix + digest: blob}}, nil, "p", "")
	srv := httptest.NewServer(h.SetupRoutes())
	defer srv.Close()

	do := func(method, id, rng string) *http.Response {
		req, _ := http.NewRequest(method, srv.URL+"/oci/blobs/"+id, nil)
		if rng != "" {
			req.Header.Set("Range", rng)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		return resp
	}

	resp := do(http.MethodHead, digest, "bytes=0-")
	if resp.StatusCode != 200 || resp.ContentLength != 100 {
		t.Fatalf("HEAD: status %d length %d, want 200 100", resp.StatusCode, resp.ContentLength)
	}

	resp = do(http.MethodGet, "sha256:"+digest, "bytes=10-19")
	var got bytes.Buffer
	got.ReadFrom(resp.Body)
	if resp.StatusCode != 206 || got.String() != "0123456789" || resp.Header.Get("Content-Range") != "bytes 10-19/100" {
		t.Fatalf("GET: status %d body %q range %q", resp.StatusCode, got.String(), resp.Header.Get("Content-Range"))
	}

	resp = do(http.MethodGet, digest, "bytes=95-200")
	got.Reset()
	got.ReadFrom(resp.Body)
	if resp.StatusCode != 206 || got.Len() != 5 {
		t.Fatalf("GET past end: status %d len %d, want 206 5", resp.StatusCode, got.Len())
	}

	if resp := do(http.MethodGet, digest, "bytes=100-"); resp.StatusCode != http.StatusRequestedRangeNotSatisfiable {
		t.Fatalf("GET at size: status %d, want 416", resp.StatusCode)
	}
	if resp := do(http.MethodGet, strings.Repeat("cd", 32), ""); resp.StatusCode != 404 {
		t.Fatalf("unknown blob: status %d, want 404", resp.StatusCode)
	}
	if resp := do(http.MethodGet, "not-a-digest", ""); resp.StatusCode != 400 {
		t.Fatalf("bad id: status %d, want 400", resp.StatusCode)
	}

	if resp := do(http.MethodGet, "not-a-digest", ""); resp.StatusCode != 400 {
		t.Fatalf("bad id: status %d, want 400", resp.StatusCode)
	}
}

// nydusd sends registry requests through its proxy in absolute form.
// A blob uploaded through the file API must be the blob nydusd reads.
func TestOCIBlobPathMatchesFileAPI(t *testing.T) {
	digest := strings.Repeat("ef", 32)
	stored, _ := sanitizePath("oci/sha256/" + digest)
	if stored != OCIBlobPrefix+digest {
		t.Fatalf("PUT /api/files/oci/sha256/<hex> stores %q, blob endpoint reads %q", stored, OCIBlobPrefix+digest)
	}
}

func TestRegistryBlobViaProxy(t *testing.T) {
	digest := strings.Repeat("ab", 32)
	blob := bytes.Repeat([]byte("x"), 64)
	h := NewHandler(&rangeCache{files: map[string][]byte{OCIBlobPrefix + digest: blob}}, nil, "p", "")
	srv := httptest.NewServer(h.SetupRoutes())
	defer srv.Close()
	proxy, _ := url.Parse(srv.URL)
	client := &http.Client{Transport: &http.Transport{Proxy: http.ProxyURL(proxy)}}

	get := func(path, rng string) *http.Response {
		req, _ := http.NewRequest(http.MethodGet, "http://registry.example"+path, nil)
		if rng != "" {
			req.Header.Set("Range", rng)
		}
		resp, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		return resp
	}
	if resp := get("/v2/", ""); resp.StatusCode != 200 {
		t.Fatalf("ping: %d", resp.StatusCode)
	}
	resp := get("/v2/models/qwen7b-cpu/blobs/sha256:"+digest, "bytes=0-15")
	if resp.StatusCode != 206 || resp.ContentLength != 16 {
		t.Fatalf("blob: status %d length %d", resp.StatusCode, resp.ContentLength)
	}
	if resp := get("/v2/models/qwen7b-cpu/blobs/sha256:"+strings.Repeat("cd", 32), "bytes=0-15"); resp.StatusCode != 503 {
		t.Fatalf("missing blob: %d, want 503 so nydusd falls back", resp.StatusCode)
	}
}

// End to end on a real cache manager: a multi-chunk blob uploaded through the
// file API is served by the planned path (local chunk files, sendfile) with
// exact bytes for ranges inside a chunk, across chunks and at the tail.
func TestRegistryBlobPlannedPath(t *testing.T) {
	cm, err := cache.NewCacheManager(&cache.CacheConfig{
		NVMePath:     t.TempDir(),
		MaxNVMeSize:  64 << 20,
		ChunkSize:    1 << 20,
		CloudTimeout: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer cm.Shutdown()
	srv := httptest.NewServer(NewHandler(cm, nil, "p", "").SetupRoutes())
	defer srv.Close()

	blob := make([]byte, 3<<20+12345)
	for i := range blob {
		blob[i] = byte(i * 31 % 253)
	}
	sum := sha256.Sum256(blob)
	digest := hex.EncodeToString(sum[:])
	req, _ := http.NewRequest(http.MethodPut, srv.URL+"/api/files/oci/sha256/"+digest, bytes.NewReader(blob))
	if resp, err := http.DefaultClient.Do(req); err != nil || resp.StatusCode != 200 {
		t.Fatalf("upload: %v %v", err, resp)
	}

	for _, c := range []struct{ start, end int }{
		{10, 4095},
		{1<<20 - 100, 1<<20 + 99},
		{100, 2<<20 + 500},
		{3 << 20, len(blob) - 1},
	} {
		req, _ := http.NewRequest(http.MethodGet, srv.URL+"/v2/r/blobs/sha256:"+digest, nil)
		req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", c.start, c.end))
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		got, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != 206 || !bytes.Equal(got, blob[c.start:c.end+1]) {
			t.Fatalf("range %d-%d: status %d, %d bytes, match=%t", c.start, c.end, resp.StatusCode, len(got), bytes.Equal(got, blob[c.start:c.end+1]))
		}
	}
}

// The peer read endpoint serves an off/len window of a chunk of a whole
// parent, and of a whole file.
func TestPeerReadSubRange(t *testing.T) {
	cm, err := cache.NewCacheManager(&cache.CacheConfig{NVMePath: t.TempDir(), MaxNVMeSize: 64 << 20, ChunkSize: 1 << 20, CloudTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer cm.Shutdown()
	srv := httptest.NewServer(NewHandler(cm, nil, "p", "").SetupRoutes())
	defer srv.Close()
	blob := make([]byte, 3<<20)
	for i := range blob {
		blob[i] = byte(i * 13 % 251)
	}
	req, _ := http.NewRequest(http.MethodPut, srv.URL+"/api/files/w.bin", bytes.NewReader(blob))
	if resp, err := http.DefaultClient.Do(req); err != nil || resp.StatusCode != 200 {
		t.Fatalf("upload: %v", err)
	}
	get := func(q string) []byte {
		resp, err := http.Get(srv.URL + "/api/peer/read?" + q)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		b, _ := io.ReadAll(resp.Body)
		if resp.StatusCode != 200 {
			t.Fatalf("%s: status %d %s", q, resp.StatusCode, b)
		}
		return b
	}
	// Chunked upload: the chunk files exist; whole-parent and chunk paths both work.
	if got := get("path=/w.bin_chunk_1&off=100&len=50"); !bytes.Equal(got, blob[1<<20+100:1<<20+150]) {
		t.Fatalf("chunk window mismatch: %d bytes", len(got))
	}
	if got := get("path=/w.bin_chunk_2&off=1048000&len=1000"); !bytes.Equal(got, blob[2<<20+1048000:3<<20]) {
		t.Fatalf("clamped chunk window mismatch: %d bytes", len(got))
	}
}

// A PUT of an OCI blob whose body does not hash to its digest is refused and
// nothing is stored; a matching body is stored.
func TestOCIBlobPutVerifiesDigest(t *testing.T) {
	cm, err := cache.NewCacheManager(&cache.CacheConfig{NVMePath: t.TempDir(), MaxNVMeSize: 64 << 20, ChunkSize: 1 << 20, CloudTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer cm.Shutdown()
	srv := httptest.NewServer(NewHandler(cm, nil, "p", "").SetupRoutes())
	defer srv.Close()
	body := []byte("real layer bytes")
	sum := sha256.Sum256(body)
	good := hex.EncodeToString(sum[:])
	put := func(digest string, b []byte) int {
		req, _ := http.NewRequest(http.MethodPut, srv.URL+"/api/files/oci/sha256/"+digest, bytes.NewReader(b))
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		return resp.StatusCode
	}
	if code := put(good, []byte("tampered layer!!")); code != http.StatusBadRequest {
		t.Fatalf("mismatched blob: status %d, want 400", code)
	}
	if resp, _ := http.Head(srv.URL + "/v2/r/blobs/sha256:" + good); resp.StatusCode == 200 {
		t.Fatal("a rejected blob is being served")
	}
	if code := put(good, body); code != 200 {
		t.Fatalf("matching blob: status %d, want 200", code)
	}
	if resp, _ := http.Head(srv.URL + "/v2/r/blobs/sha256:" + good); resp.StatusCode != 200 || resp.ContentLength != int64(len(body)) {
		t.Fatalf("stored blob not served: %d %d", resp.StatusCode, resp.ContentLength)
	}
}

// A plain GET returns the whole blob with 200; a node with an API key still
// serves the blob routes to nydusd (which sends no key) but not the file API.
func TestOCIBlobWholeGetAndAPIKey(t *testing.T) {
	cm, err := cache.NewCacheManager(&cache.CacheConfig{NVMePath: t.TempDir(), MaxNVMeSize: 64 << 20, ChunkSize: 1 << 20, CloudTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer cm.Shutdown()
	srv := httptest.NewServer(NewHandler(cm, nil, "p", "secret").SetupRoutes())
	defer srv.Close()
	blob := make([]byte, 2<<20+777)
	for i := range blob {
		blob[i] = byte(i * 7 % 251)
	}
	sum := sha256.Sum256(blob)
	d := hex.EncodeToString(sum[:])
	req, _ := http.NewRequest(http.MethodPut, srv.URL+"/api/files/oci/sha256/"+d, bytes.NewReader(blob))
	req.Header.Set("X-API-Key", "secret")
	if resp, err := http.DefaultClient.Do(req); err != nil || resp.StatusCode != 200 {
		t.Fatalf("upload: %v %v", err, resp)
	}
	resp, err := http.Get(srv.URL + "/v2/r/blobs/sha256:" + d) // no key, no Range
	if err != nil {
		t.Fatal(err)
	}
	got, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 || !bytes.Equal(got, blob) {
		t.Fatalf("whole GET: status %d, %d bytes, match=%t", resp.StatusCode, len(got), bytes.Equal(got, blob))
	}
	if resp, _ := http.Get(srv.URL + "/v2/"); resp.StatusCode != 200 {
		t.Fatalf("ping without key: %d", resp.StatusCode)
	}
	if resp, _ := http.Get(srv.URL + "/api/files/oci/sha256/" + d); resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("file API without key: %d, want 401", resp.StatusCode)
	}
}

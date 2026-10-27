package api

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

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

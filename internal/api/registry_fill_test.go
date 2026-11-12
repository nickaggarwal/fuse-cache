package api

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"fuse-client/internal/cache"
)

// fillCache is a cache double for fill tests: it answers LookupEntry from
// what was committed, stages through real files, and can deny the lease.
type fillCache struct {
	cache.CacheManager
	mu        sync.Mutex
	files     map[string][]byte
	verified  map[string]bool
	denyLease bool
}

func newFillCache() *fillCache {
	return &fillCache{CacheManager: newMockCacheManager(), files: map[string][]byte{}, verified: map[string]bool{}}
}

func (c *fillCache) LookupEntry(_ context.Context, p string, _ bool) (*cache.CacheEntry, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	b, ok := c.files[p]
	if !ok {
		return nil, false
	}
	return &cache.CacheEntry{FilePath: p, Size: int64(len(b))}, true
}

func (c *fillCache) ReadRange(_ context.Context, p string, off int64, n int) ([]byte, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	b := c.files[p]
	end := off + int64(n)
	if end > int64(len(b)) {
		end = int64(len(b))
	}
	return b[off:end], nil
}

func (c *fillCache) CreateWriteFile(context.Context, string) (*os.File, string, error) {
	f, err := os.CreateTemp("", "fill-*")
	if err != nil {
		return nil, "", err
	}
	return f, f.Name(), nil
}

func (c *fillCache) PutFromFile(_ context.Context, p, staged string, _ int64, _ time.Time) error {
	b, err := os.ReadFile(staged)
	if err != nil {
		return err
	}
	c.mu.Lock()
	c.files[p] = b
	c.mu.Unlock()
	return nil
}

func (c *fillCache) MarkBlobVerified(p string) {
	c.mu.Lock()
	c.verified[p] = true
	c.mu.Unlock()
}

func (c *fillCache) TryOriginLease(context.Context, string, time.Duration) (bool, func()) {
	return !c.denyLease, func() {}
}

func (c *fillCache) has(p string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	_, ok := c.files[p]
	return ok
}

// fakeRegistry serves one blob over TLS, counting GETs and the Authorization
// header it saw. body overrides what is served (to test a bad digest).
type fakeRegistry struct {
	srv    *httptest.Server
	gets   atomic.Int64
	ranges atomic.Int64
	auth   atomic.Value
	body   []byte
	delay  time.Duration
}

func newFakeRegistry(t *testing.T, repo, hexDigest string, body []byte) *fakeRegistry {
	fr := &fakeRegistry{body: body}
	fr.srv = httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v2/"+repo+"/blobs/sha256:"+hexDigest {
			http.NotFound(w, r)
			return
		}
		fr.gets.Add(1)
		fr.auth.Store(r.Header.Get("Authorization"))
		if fr.delay > 0 {
			time.Sleep(fr.delay)
		}
		// Like ACR: honour a single "bytes=a-b" range with 206.
		if rg := r.Header.Get("Range"); rg != "" {
			var a, b int64
			if _, err := fmt.Sscanf(rg, "bytes=%d-%d", &a, &b); err == nil && a < int64(len(fr.body)) {
				if b >= int64(len(fr.body)) {
					b = int64(len(fr.body)) - 1
				}
				fr.ranges.Add(1)
				w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", a, b, len(fr.body)))
				w.Header().Set("Content-Length", fmt.Sprint(b-a+1))
				w.WriteHeader(http.StatusPartialContent)
				_, _ = w.Write(fr.body[a : b+1])
				return
			}
		}
		w.Header().Set("Content-Length", fmt.Sprint(len(fr.body)))
		_, _ = w.Write(fr.body)
	}))
	t.Cleanup(fr.srv.Close)
	return fr
}

func (fr *fakeRegistry) host() string { u, _ := url.Parse(fr.srv.URL); return u.Host }

// proxyGet sends what nydusd sends: an HTTP proxy request with an absolute
// http:// URI for the registry blob, a Range and a Bearer token.
func proxyGet(t *testing.T, proxyAddr, registryHost, repo, hexDigest string) int {
	t.Helper()
	conn, err := net.Dial("tcp", proxyAddr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	fmt.Fprintf(conn, "GET http://%s/v2/%s/blobs/sha256:%s HTTP/1.1\r\nHost: %s\r\nRange: bytes=0-9\r\nAuthorization: Bearer test-token\r\nUser-Agent: nydusd/1.0.0\r\nConnection: close\r\n\r\n",
		registryHost, repo, hexDigest, registryHost)
	resp, err := http.ReadResponse(bufio.NewReader(conn), nil)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	return resp.StatusCode
}

func fillHandler(t *testing.T, fc *fillCache, fr *fakeRegistry, hosts ...string) (*Handler, string) {
	t.Helper()
	h := NewHandler(fc, nil, "p", "")
	h.EnableRegistryFill(RegistryFillConfig{Enabled: true, AllowedHosts: hosts, HTTPClient: fr.srv.Client()})
	srv := httptest.NewServer(h.SetupRoutes())
	t.Cleanup(srv.Close)
	u, _ := url.Parse(srv.URL)
	return h, u.Host
}

func waitFor(t *testing.T, cond func() bool, what string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func blobOf(content string) ([]byte, string) {
	b := []byte(strings.Repeat(content, 1000))
	s := sha256.Sum256(b)
	return b, hex.EncodeToString(s[:])
}

// The first miss answers 503 (nydusd reads it directly), starts one fill with
// nydusd's own token, and once the blob is verified and committed the next
// read is served from the cache.
func TestRegistryFillOnFirstMiss(t *testing.T) {
	body, dg := blobOf("model-weights-")
	fr := newFakeRegistry(t, "vllm/cpu", dg, body)
	fc := newFillCache()
	h, addr := fillHandler(t, fc, fr)

	if code := proxyGet(t, addr, fr.host(), "vllm/cpu", dg); code != http.StatusPartialContent {
		t.Fatalf("first miss answered %d, want 206 served from the fill in progress", code)
	}
	waitFor(t, func() bool { return fc.has(OCIBlobPrefix + dg) }, "fill to commit")
	if got := fr.auth.Load(); got != "Bearer test-token" {
		t.Fatalf("registry saw Authorization %q, want nydusd's token", got)
	}
	if !fc.verified[OCIBlobPrefix+dg] {
		t.Fatal("filled blob not marked verified")
	}
	h.ociMisses.forget(OCIBlobPrefix + dg)
	if code := proxyGet(t, addr, fr.host(), "vllm/cpu", dg); code != http.StatusPartialContent {
		t.Fatalf("read after fill answered %d, want 206 from the cache", code)
	}
	if s := h.registryFillStats(); s.Completed != 1 || s.Bytes != int64(len(body)) {
		t.Fatalf("stats = %+v", s)
	}
}

// Many simultaneous misses of one blob start exactly one download.
func TestRegistryFillSingleFlight(t *testing.T) {
	body, dg := blobOf("layer-")
	fr := newFakeRegistry(t, "r", dg, body)
	fr.delay = 200 * time.Millisecond
	fc := newFillCache()
	h, addr := fillHandler(t, fc, fr)
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); proxyGet(t, addr, fr.host(), "r", dg) }()
	}
	wg.Wait()
	waitFor(t, func() bool { return fc.has(OCIBlobPrefix + dg) }, "fill")
	if n := h.registryFillStats().Started; n != 1 {
		t.Fatalf("fills started = %d, want 1", n)
	}
}

// A body that does not hash to the digest is never committed.
func TestRegistryFillRejectsWrongDigest(t *testing.T) {
	_, dg := blobOf("real-")
	fr := newFakeRegistry(t, "r", dg, []byte("tampered or truncated"))
	fc := newFillCache()
	h, addr := fillHandler(t, fc, fr)
	proxyGet(t, addr, fr.host(), "r", dg)
	waitFor(t, func() bool { return h.registryFillStats().Rejected+h.registryFillStats().Failed >= 1 }, "fill to finish")
	if fc.has(OCIBlobPrefix + dg) {
		t.Fatal("blob with the wrong digest was committed")
	}
	if h.registryFillStats().Rejected != 1 {
		t.Fatalf("stats = %+v, want Rejected 1", h.registryFillStats())
	}
}

// Another node holding the cluster lease means this node does not download.
func TestRegistryFillLeaseLost(t *testing.T) {
	body, dg := blobOf("x")
	fr := newFakeRegistry(t, "r", dg, body)
	fc := newFillCache()
	fc.denyLease = true
	h, addr := fillHandler(t, fc, fr)
	proxyGet(t, addr, fr.host(), "r", dg)
	waitFor(t, func() bool { return h.registryFillStats().LeaseLost == 1 }, "lease outcome")
	if fr.gets.Load() != 0 {
		t.Fatal("downloaded without the lease")
	}
}

// Disabled, a host outside the allowlist, or a request with no registry host
// (the plain /v2/<repo>/blobs form) starts nothing.
func TestRegistryFillGuards(t *testing.T) {
	body, dg := blobOf("g")
	fr := newFakeRegistry(t, "r", dg, body)

	fc := newFillCache()
	_, addr := fillHandler(t, fc, fr, "only.example.io")
	proxyGet(t, addr, fr.host(), "r", dg)
	time.Sleep(100 * time.Millisecond)
	if fr.gets.Load() != 0 {
		t.Fatal("filled from a host outside the allowlist")
	}

	fc2 := newFillCache()
	h2 := NewHandler(fc2, nil, "p", "")
	h2.EnableRegistryFill(RegistryFillConfig{Enabled: false, HTTPClient: fr.srv.Client()})
	srv := httptest.NewServer(h2.SetupRoutes())
	defer srv.Close()
	u, _ := url.Parse(srv.URL)
	proxyGet(t, u.Host, fr.host(), "r", dg)
	resp, err := http.Get(srv.URL + "/v2/r/blobs/sha256:" + dg)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	time.Sleep(100 * time.Millisecond)
	if fr.gets.Load() != 0 {
		t.Fatal("filled while disabled or from a host-less request")
	}
}

// A read for a blob being filled is answered from the units that landed,
// with the right bytes, before the whole blob is in; the unit it needs is
// fetched first.
func TestRegistryFillServesWhileFilling(t *testing.T) {
	SetFillUnitBytes(256 << 10)
	defer SetFillUnitBytes(defaultFillUnit)
	b := make([]byte, 8<<20) // 32 units of 256 KiB
	for i := range b {
		b[i] = byte(i * 7 % 251)
	}
	sum := sha256.Sum256(b)
	dg := hex.EncodeToString(sum[:])
	fr := newFakeRegistry(t, "r", dg, b)
	fr.delay = 30 * time.Millisecond // each range GET is slow
	fc := newFillCache()
	h, addr := fillHandler(t, fc, fr)

	// Ask for a range near the end: it must not wait for the whole blob.
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	start, end := int64(7<<20), int64(7<<20+4095)
	t0 := time.Now()
	fmt.Fprintf(conn, "GET http://%s/v2/r/blobs/sha256:%s HTTP/1.1\r\nHost: x\r\nRange: bytes=%d-%d\r\nAuthorization: Bearer t\r\nConnection: close\r\n\r\n", fr.host(), dg, start, end)
	resp, err := http.ReadResponse(bufio.NewReader(conn), nil)
	if err != nil {
		t.Fatal(err)
	}
	got, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	took := time.Since(t0)
	if resp.StatusCode != http.StatusPartialContent || !bytes.Equal(got, b[start:end+1]) {
		t.Fatalf("status %d, %d bytes, match=%t", resp.StatusCode, len(got), bytes.Equal(got, b[start:end+1]))
	}
	// 32 units x 30 ms / 8 workers ~= 120 ms for everything; the prioritized
	// unit should be back well before a sequential fill reached it.
	if fc.has(OCIBlobPrefix+dg) && took > 400*time.Millisecond {
		t.Fatalf("read waited for the whole blob (%v)", took)
	}
	if h.registryFillStats().ServedReads != 1 {
		t.Fatalf("served reads = %d", h.registryFillStats().ServedReads)
	}
	waitFor(t, func() bool { return fc.has(OCIBlobPrefix + dg) }, "commit")
	if fr.gets.Load() > 33+1 { // 1 size probe + 32 units, no duplicates
		t.Fatalf("registry GETs = %d, want <= 33 (each unit once)", fr.gets.Load())
	}
}

// When the registry body for a unit is wrong the blob is never committed, and
// a later read falls back (503) instead of being served the bad bytes.
func TestRegistryFillBadBlobNotCommitted(t *testing.T) {
	_, dg := blobOf("expected")
	fr := newFakeRegistry(t, "r", dg, []byte(strings.Repeat("WRONG", 2000)))
	fc := newFillCache()
	h, addr := fillHandler(t, fc, fr)
	proxyGet(t, addr, fr.host(), "r", dg) // may be served: it is what the registry returned
	waitFor(t, func() bool { return h.registryFillStats().Rejected >= 1 }, "rejection")
	if fc.has(OCIBlobPrefix + dg) {
		t.Fatal("blob that failed its digest was committed")
	}
}

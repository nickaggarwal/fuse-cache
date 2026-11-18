package api

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"

	"sync"
	"sync/atomic"
	"testing"
	"time"

	"fuse-client/internal/cache"
)

// peerFillCache is a cache double for a node that does not hold a blob some
// other node holds: LookupEntry finds it (cluster metadata), the bytes come
// from ReadBlobRangeFromPeers, and a committed copy lands in files.
type peerFillCache struct {
	*fillCache
	remote     map[string][]byte
	peerErr    atomic.Bool
	peerDelay  time.Duration
	peerReads  atomic.Int64
	inflight   atomic.Int64
	maxInfl    atomic.Int64
	localPuts  atomic.Int64
	cloudPuts  atomic.Int64
	corruptOne atomic.Bool
}

func newPeerFillCache(path string, body []byte) *peerFillCache {
	return &peerFillCache{fillCache: newFillCache(), remote: map[string][]byte{path: body}}
}

func (c *peerFillCache) LookupEntry(ctx context.Context, p string, force bool) (*cache.CacheEntry, bool) {
	if e, ok := c.fillCache.LookupEntry(ctx, p, force); ok {
		return e, true
	}
	if b, ok := c.remote[p]; ok {
		return &cache.CacheEntry{FilePath: p, Size: int64(len(b)), IsChunked: true, ChunkSize: 16 << 20}, true
	}
	return nil, false
}

func (c *peerFillCache) ReadBlobRangeFromPeers(ctx context.Context, e *cache.CacheEntry, off, n int64) ([]byte, error) {
	c.peerReads.Add(1)
	cur := c.inflight.Add(1)
	defer c.inflight.Add(-1)
	for {
		m := c.maxInfl.Load()
		if cur <= m || c.maxInfl.CompareAndSwap(m, cur) {
			break
		}
	}
	if c.peerDelay > 0 {
		select {
		case <-time.After(c.peerDelay):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	if c.peerErr.Load() {
		return nil, errors.New("holder busy")
	}
	b := c.remote[e.FilePath]
	out := append([]byte(nil), b[off:off+n]...)
	if c.corruptOne.Load() && off == 0 {
		out[0] ^= 0xff
	}
	return out, nil
}

func (c *peerFillCache) LocalBlobPresent(p string, size int64) bool {
	return c.has(p)
}

func (c *peerFillCache) PutFromFileLocal(ctx context.Context, p, staged string, size int64) error {
	c.localPuts.Add(1)
	return c.fillCache.PutFromFile(ctx, p, staged, size, time.Now())
}

func (c *peerFillCache) PutFromFile(ctx context.Context, p, staged string, size int64, t time.Time) error {
	c.cloudPuts.Add(1)
	return c.fillCache.PutFromFile(ctx, p, staged, size, t)
}

func patterned(n int) ([]byte, string) {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(i*13 + i/4096)
	}
	s := sha256.Sum256(b)
	return b, hex.EncodeToString(s[:])
}

func peerFillHandler(t *testing.T, c *peerFillCache, fr *fakeRegistry) (*Handler, string) {
	t.Helper()
	h := NewHandler(c, nil, "p", "")
	if fr != nil {
		h.EnableRegistryFill(RegistryFillConfig{Enabled: true, HTTPClient: fr.srv.Client()})
	}
	h.EnablePeerFill(true)
	srv := httptest.NewServer(h.SetupRoutes())
	t.Cleanup(srv.Close)
	u, _ := url.Parse(srv.URL)
	return h, u.Host
}

// rangeGet sends a nydusd proxy-form range read and returns status and body.
func rangeGet(t *testing.T, addr, regHost, dg string, start, end int64) (int, []byte) {
	t.Helper()
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	fmt.Fprintf(conn, "GET http://%s/v2/r/blobs/sha256:%s HTTP/1.1\r\nHost: x\r\nRange: bytes=%d-%d\r\nAuthorization: Bearer t\r\nConnection: close\r\n\r\n", regHost, dg, start, end)
	resp, err := http.ReadResponse(bufio.NewReader(conn), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, b
}

// A read of a blob another node holds starts one whole-blob fill from the
// holders in parallel ranges, is answered from it with the right bytes, and
// the blob is committed locally without a second cloud upload.
func TestPeerFillServesAndCommits(t *testing.T) {
	SetFillUnitBytes(1 << 20)
	defer SetFillUnitBytes(defaultFillUnit)
	body, dg := patterned(24 << 20) // 24 units
	c := newPeerFillCache(OCIBlobPrefix+dg, body)
	c.peerDelay = 20 * time.Millisecond
	h, addr := peerFillHandler(t, c, nil)

	start, end := int64(20<<20), int64(20<<20+65535)
	code, got := rangeGet(t, addr, "reg.example", dg, start, end)
	if code != http.StatusPartialContent || !bytes.Equal(got, body[start:end+1]) {
		t.Fatalf("status %d, match=%t", code, bytes.Equal(got, body[start:end+1]))
	}
	waitFor(t, func() bool { return h.peerFillStats().Completed == 1 }, "peer fill to commit")
	if c.localPuts.Load() != 1 || c.cloudPuts.Load() != 0 {
		t.Fatalf("commits: local=%d cloud=%d, want 1 local and no re-upload", c.localPuts.Load(), c.cloudPuts.Load())
	}
	if !c.verified[OCIBlobPrefix+dg] {
		t.Fatal("peer-filled blob not marked verified")
	}
	if n := c.peerReads.Load(); n != 24 { // no unit fetched twice
		t.Fatalf("peer range reads = %d, want 24 (one per unit)", n)
	}
	if m := c.maxInfl.Load(); m < 4 {
		t.Fatalf("max parallel peer reads = %d, want parallel ranges", m)
	}
	if s := h.peerFillStats(); s.Completed != 1 || s.UnitsPeer != 24 || s.UnitsHedged != 0 {
		t.Fatalf("stats = %+v", s)
	}
}

// Many concurrent reads of the blob start one fill, each answered correctly.
func TestPeerFillSingleFlight(t *testing.T) {
	SetFillUnitBytes(1 << 20)
	defer SetFillUnitBytes(defaultFillUnit)
	body, dg := patterned(8 << 20)
	c := newPeerFillCache(OCIBlobPrefix+dg, body)
	c.peerDelay = 10 * time.Millisecond
	h, addr := peerFillHandler(t, c, nil)
	var wg sync.WaitGroup
	var bad atomic.Int64
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			s := int64(i) * (512 << 10)
			code, got := rangeGet(t, addr, "reg.example", dg, s, s+4095)
			if code != http.StatusPartialContent || !bytes.Equal(got, body[s:s+4096]) {
				bad.Add(1)
			}
		}(i)
	}
	wg.Wait()
	if bad.Load() != 0 {
		t.Fatalf("%d reads answered wrong", bad.Load())
	}
	waitFor(t, func() bool { return h.peerFillStats().Completed == 1 }, "commit")
	if s := h.peerFillStats(); s.Started != 1 {
		t.Fatalf("fills started = %d, want 1", s.Started)
	}
}

// When the holders cannot deliver, the units come from the registry the
// request named, so the read is still answered and the blob still lands.
func TestPeerFillHedgesToRegistry(t *testing.T) {
	SetFillUnitBytes(1 << 20)
	defer SetFillUnitBytes(defaultFillUnit)
	body, dg := patterned(4 << 20)
	fr := newFakeRegistry(t, "r", dg, body)
	c := newPeerFillCache(OCIBlobPrefix+dg, body)
	c.peerErr.Store(true)
	h, addr := peerFillHandler(t, c, fr)

	code, got := rangeGet(t, addr, fr.host(), dg, 100, 4195)
	if code != http.StatusPartialContent || !bytes.Equal(got, body[100:4196]) {
		t.Fatalf("status %d, match=%t", code, bytes.Equal(got, body[100:4196]))
	}
	waitFor(t, func() bool { return h.peerFillStats().Completed == 1 }, "commit")
	s := h.peerFillStats()
	if s.UnitsHedged != 4 || s.UnitsPeer != 0 || s.Completed != 1 { // each unit once
		t.Fatalf("stats = %+v, want 4 units from the registry", s)
	}
	if fr.auth.Load() != "Bearer t" {
		t.Fatalf("registry saw %q, want nydusd's token", fr.auth.Load())
	}
}

// Bytes from a holder that do not hash to the digest are never committed.
func TestPeerFillRejectsBadHolder(t *testing.T) {
	SetFillUnitBytes(1 << 20)
	defer SetFillUnitBytes(defaultFillUnit)
	body, dg := patterned(5 << 20)
	c := newPeerFillCache(OCIBlobPrefix+dg, body)
	c.corruptOne.Store(true)
	h, addr := peerFillHandler(t, c, nil)
	rangeGet(t, addr, "reg.example", dg, 2<<20, 2<<20+99)
	waitFor(t, func() bool { return h.peerFillStats().Rejected == 1 }, "fill to be rejected")
	if c.has(OCIBlobPrefix + dg) {
		t.Fatal("blob from a bad holder was committed")
	}
}

// A node that already holds the whole blob does not peer-fill it.
func TestPeerFillSkipsLocalBlob(t *testing.T) {
	body, dg := patterned(6 << 20)
	c := newPeerFillCache(OCIBlobPrefix+dg, body)
	c.files[OCIBlobPrefix+dg] = body
	h, addr := peerFillHandler(t, c, nil)
	code, got := rangeGet(t, addr, "reg.example", dg, 0, 99)
	if code != http.StatusPartialContent || !bytes.Equal(got, body[:100]) {
		t.Fatalf("status %d", code)
	}
	if s := h.peerFillStats(); s.Started != 0 {
		t.Fatalf("fills started = %d on a node holding the blob", s.Started)
	}
}

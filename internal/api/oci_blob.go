package api

import (
	"context"
	"fmt"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/mux"

	"fuse-client/internal/cache"
)

// OCIBlobPrefix is where OCI blobs live in the cache namespace, by digest:
// /oci/sha256/<hex>, the path a PUT to /api/files/oci/sha256/<hex> stores
// (sanitizePath roots every path). A blob two images share is one cache
// file, whatever repository either image came from.
const OCIBlobPrefix = "/oci/sha256/"

// ociBlobMaxRange bounds one GET. nydusd asks for a chunk or a merged
// prefetch window (1 MiB by default, a few MiB merged); the cap keeps a
// "bytes=0-" request from loading a multi-GB blob into memory.
const ociBlobMaxRange = 64 << 20

var ociDigestHex = regexp.MustCompile(`^(sha256:)?([a-f0-9]{64})$`)

// entryLookup is the point metadata lookup DefaultCacheManager provides.
// Get() would read the whole blob just to learn its size.
type entryLookup interface {
	LookupEntry(ctx context.Context, filePath string, forceRefresh bool) (*cache.CacheEntry, bool)
}

// rangePlanner is DefaultCacheManager's read path for callers that do their
// own read-ahead (cache/range_plan.go).
type rangePlanner interface {
	PlanRange(ctx context.Context, entry *cache.CacheEntry, offset, length int64) ([]cache.RangePiece, func(), error)
}

type rangeReader interface {
	ReadRange(ctx context.Context, filePath string, offset int64, size int) ([]byte, error)
}

// handleOCIBlob serves nydusd's http-proxy backend: HEAD for the blob size,
// GET with a Range header for the bytes. A miss is filled from the store, a
// peer or the cloud.
func (h *Handler) handleOCIBlob(w http.ResponseWriter, r *http.Request) {
	h.serveOCIBlob(w, r, http.StatusNotFound)
}

// handleRegistryPing answers the proxy health check nydusd sends to ping_url.
func (h *Handler) handleRegistryPing(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Docker-Distribution-API-Version", "registry/2.0")
	w.WriteHeader(http.StatusOK)
}

// handleRegistryBlob serves a blob nydusd asked its proxy for. A blob this
// cluster has never held answers 503, not 404: nydusd falls back to the
// origin registry only on a 5xx, and a 404 fails the read.
func (h *Handler) handleRegistryBlob(w http.ResponseWriter, r *http.Request) {
	h.serveOCIBlob(w, r, http.StatusServiceUnavailable)
}

func (h *Handler) serveOCIBlob(w http.ResponseWriter, r *http.Request, missing int) {
	m := ociDigestHex.FindStringSubmatch(mux.Vars(r)["digest"])
	if m == nil {
		http.Error(w, "blob id must be a sha256 hex digest", http.StatusBadRequest)
		return
	}
	filePath := OCIBlobPrefix + m[2]
	ctx := r.Context()

	entry, ok := h.ociBlobEntry(ctx, filePath)
	if !ok {
		http.Error(w, "blob not found", missing)
		return
	}
	size := entry.Size
	w.Header().Set("Accept-Ranges", "bytes")
	w.Header().Set("Content-Type", "application/octet-stream")

	if r.Method == http.MethodHead {
		w.Header().Set("Content-Length", strconv.FormatInt(size, 10))
		w.WriteHeader(http.StatusOK)
		return
	}

	if r.Header.Get("Range") == "" {
		// A plain GET is a whole-blob fetch (containerd, skopeo): answer 200
		// with every byte, not a capped 206 the client would not check.
		rp, ok := h.cacheManager.(rangePlanner)
		if !ok {
			http.Error(w, "whole-blob reads not supported", http.StatusNotImplemented)
			return
		}
		h.writeWholeBlob(w, ctx, rp, entry, m[2][:12])
		return
	}
	start, end, err := parseByteRange(r.Header.Get("Range"), size)
	if err != nil {
		w.Header().Set("Content-Range", fmt.Sprintf("bytes */%d", size))
		http.Error(w, err.Error(), http.StatusRequestedRangeNotSatisfiable)
		return
	}
	if end-start+1 > ociBlobMaxRange {
		end = start + ociBlobMaxRange - 1 // Content-Range tells the client
	}

	if rp, ok := h.cacheManager.(rangePlanner); ok {
		h.writePlannedRange(w, r.Context(), rp, entry, start, end, m[2][:12])
		return
	}
	rr, ok := h.cacheManager.(rangeReader)
	if !ok {
		http.Error(w, "range reads not supported", http.StatusNotImplemented)
		return
	}
	data, err := rr.ReadRange(ctx, filePath, start, int(end-start+1))
	if err != nil {
		h.logger.Printf("oci blob %s range %d-%d: %v", m[2][:12], start, end, err)
		http.Error(w, "read failed", http.StatusBadGateway) // 5xx: nydusd falls back
		return
	}
	w.Header().Set("Content-Length", strconv.Itoa(len(data)))
	w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, start+int64(len(data))-1, size))
	w.WriteHeader(http.StatusPartialContent)
	_, _ = w.Write(data)
}

// writePlannedRange serves a range through PlanRange: local chunk files go to
// the socket with sendfile, missing chunks are fetched once (store/peer
// first, no read-ahead). Every piece is resolved before the status line, so
// a failed fetch still answers 502 and nydusd falls back.
func (h *Handler) writePlannedRange(w http.ResponseWriter, ctx context.Context, rp rangePlanner, entry *cache.CacheEntry, start, end int64, id string) {
	pieces, release, err := rp.PlanRange(ctx, entry, start, end-start+1)
	defer release()
	if err != nil {
		h.logger.Printf("oci blob %s range %d-%d: %v", id, start, end, err)
		http.Error(w, "read failed", http.StatusBadGateway) // 5xx: nydusd falls back
		return
	}
	var n int64
	for _, p := range pieces {
		n += p.Len()
	}
	w.Header().Set("Content-Length", strconv.FormatInt(n, 10))
	w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, start+n-1, entry.Size))
	w.WriteHeader(http.StatusPartialContent)
	for _, p := range pieces {
		if _, err := p.WriteTo(w); err != nil {
			return // client went away; the status is already sent
		}
	}
}

// writeWholeBlob streams the whole blob with 200, ociBlobMaxRange at a time
// so memory stays bounded. The first window is resolved before the status
// line (a failure is a 502 nydusd/containerd fall back on); a failure after
// that cuts the response short, which the client sees against
// Content-Length.
func (h *Handler) writeWholeBlob(w http.ResponseWriter, ctx context.Context, rp rangePlanner, entry *cache.CacheEntry, id string) {
	var off int64
	for off < entry.Size {
		n := min(int64(ociBlobMaxRange), entry.Size-off)
		pieces, release, err := rp.PlanRange(ctx, entry, off, n)
		if err != nil {
			release()
			h.logger.Printf("oci blob %s whole read at %d: %v", id, off, err)
			if off == 0 {
				http.Error(w, "read failed", http.StatusBadGateway)
			}
			return
		}
		if off == 0 {
			w.Header().Set("Content-Length", strconv.FormatInt(entry.Size, 10))
			w.WriteHeader(http.StatusOK)
		}
		for _, p := range pieces {
			if _, err := p.WriteTo(w); err != nil {
				release()
				return
			}
		}
		release()
		off += n
	}
}

// ociMissTTL is how long a blob the cluster does not hold is answered 503
// without asking the coordinator again. A cold image means thousands of
// chunk requests from every node's nydusd; each miss would otherwise be a
// forced coordinator lookup (the burst that OOM-killed the coordinators on
// 2026-10-10). Short, so a blob filled into the store is seen within seconds.
const ociMissTTL = 5 * time.Second

func (h *Handler) ociBlobEntry(ctx context.Context, filePath string) (*cache.CacheEntry, bool) {
	if l, ok := h.cacheManager.(entryLookup); ok {
		if e, found := l.LookupEntry(ctx, filePath, false); found && e != nil {
			return e, true
		}
		if h.ociMisses.recent(filePath) {
			return nil, false
		}
		if e, found := l.LookupEntry(ctx, filePath, true); found && e != nil {
			return e, true
		}
		h.ociMisses.note(filePath)
		return nil, false
	}
	e, err := h.cacheManager.Get(ctx, filePath)
	if err != nil {
		return nil, false
	}
	return &cache.CacheEntry{FilePath: filePath, Size: e.Size}, true
}

// parseByteRange parses a single "bytes=a-b" or "bytes=a-" range. No header
// means the whole blob, which the caller then caps.
func parseByteRange(header string, size int64) (int64, int64, error) {
	if size <= 0 {
		return 0, 0, fmt.Errorf("empty blob")
	}
	if header == "" {
		return 0, size - 1, nil
	}
	spec, ok := strings.CutPrefix(header, "bytes=")
	if !ok || strings.Contains(spec, ",") {
		return 0, 0, fmt.Errorf("unsupported range %q", header)
	}
	a, b, ok := strings.Cut(spec, "-")
	if !ok || a == "" {
		return 0, 0, fmt.Errorf("unsupported range %q", header)
	}
	start, err := strconv.ParseInt(a, 10, 64)
	if err != nil || start < 0 || start >= size {
		return 0, 0, fmt.Errorf("range %q outside blob of %d bytes", header, size)
	}
	end := size - 1
	if b != "" {
		end, err = strconv.ParseInt(b, 10, 64)
		if err != nil || end < start {
			return 0, 0, fmt.Errorf("bad range %q", header)
		}
		if end > size-1 {
			end = size - 1
		}
	}
	return start, end, nil
}

// ociBlobDigestFromPath returns the hex digest when filePath is an OCI blob
// path (/oci/sha256/<64 hex>), else "".
func ociBlobDigestFromPath(filePath string) string {
	hex, ok := strings.CutPrefix(filePath, OCIBlobPrefix)
	if !ok || len(hex) != 64 || strings.ContainsAny(hex, "/_") {
		return ""
	}
	for _, c := range hex {
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return ""
		}
	}
	return hex
}

// missCache remembers recent lookups that found nothing. The zero value is
// usable.
type missCache struct {
	mu sync.Mutex
	at map[string]time.Time
}

func (c *missCache) recent(k string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	t, ok := c.at[k]
	if ok && time.Since(t) >= ociMissTTL {
		delete(c.at, k)
		return false
	}
	return ok
}

func (c *missCache) note(k string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.at == nil {
		c.at = make(map[string]time.Time)
	}
	if len(c.at) > 65536 { // bound memory: drop everything, it is a cache
		c.at = make(map[string]time.Time)
	}
	c.at[k] = time.Now()
}

// isOCIBlobRoute matches the read-only registry-proxy routes.
func isOCIBlobRoute(r *http.Request) bool {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		return false
	}
	p := r.URL.Path
	return p == "/v2/" || strings.HasPrefix(p, "/oci/blobs/") || (strings.HasPrefix(p, "/v2/") && strings.Contains(p, "/blobs/"))
}

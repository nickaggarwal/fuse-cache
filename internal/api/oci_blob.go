package api

import (
	"context"
	"fmt"
	"net/http"
	"regexp"
	"strconv"
	"strings"

	"github.com/gorilla/mux"

	"fuse-client/internal/cache"
)

// OCIBlobPrefix is where OCI blobs live in the cache namespace, by digest:
// oci/sha256/<hex>. A blob two images share is one cache file, whatever
// repository either image came from.
const OCIBlobPrefix = "oci/sha256/"

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

type rangeReader interface {
	ReadRange(ctx context.Context, filePath string, offset int64, size int) ([]byte, error)
}

// handleOCIBlob serves nydusd's http-proxy backend: HEAD for the blob size,
// GET with a Range header for the bytes. The bytes come through the normal
// read path, so a miss is filled from a peer, the store or the cloud.
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

	size, ok := h.ociBlobSize(ctx, filePath)
	if !ok {
		http.Error(w, "blob not found", missing)
		return
	}
	w.Header().Set("Accept-Ranges", "bytes")
	w.Header().Set("Content-Type", "application/octet-stream")

	if r.Method == http.MethodHead {
		w.Header().Set("Content-Length", strconv.FormatInt(size, 10))
		w.WriteHeader(http.StatusOK)
		return
	}

	start, end, err := parseByteRange(r.Header.Get("Range"), size)
	if err != nil {
		w.Header().Set("Content-Range", fmt.Sprintf("bytes */%d", size))
		http.Error(w, err.Error(), http.StatusRequestedRangeNotSatisfiable)
		return
	}
	if end-start+1 > ociBlobMaxRange {
		end = start + ociBlobMaxRange - 1
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

func (h *Handler) ociBlobSize(ctx context.Context, filePath string) (int64, bool) {
	if l, ok := h.cacheManager.(entryLookup); ok {
		if e, found := l.LookupEntry(ctx, filePath, false); found && e != nil {
			return e.Size, true
		}
		if e, found := l.LookupEntry(ctx, filePath, true); found && e != nil {
			return e.Size, true
		}
		return 0, false
	}
	e, err := h.cacheManager.Get(ctx, filePath)
	if err != nil {
		return 0, false
	}
	return e.Size, true
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

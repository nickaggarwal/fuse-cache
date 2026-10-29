package api

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"syscall"

	"fuse-client/internal/cache"
)

// localChunkServer is implemented by the cache manager to resolve a request
// path to a local file (whole) or a byte range within a whole parent file.
type localChunkServer interface {
	LocalFilePath(ctx context.Context, filePath string) (string, bool)
	LocalChunkFile(ctx context.Context, chunkPath string) (localPath string, offset, length int64, ok bool)
}

// handlePeerRead serves peer bulk chunk reads over plain HTTP instead of gRPC.
// This bypasses the gRPC/protobuf framing that caps a single peer-read stream
// at ~94 MiB/s (vs ~773 MiB/s raw on the same nodes). Serving a whole file or a
// byte range straight from an *os.File with Content-Length set lets the Go HTTP
// server use sendfile(2) (zero-copy file->socket) on the plain in-cluster
// connection. It runs under the normal auth middleware (X-API-Key when set).
func (h *Handler) handlePeerRead(w http.ResponseWriter, r *http.Request) {
	// Serve-side admission control: shed load with 503 when the peer-serve
	// gate is full so requesters fail over to another holder (or retry with
	// jitter) instead of queueing on a saturated source.
	if admitter, ok := h.cacheManager.(cache.PeerServeAdmitter); ok {
		release, admitted := admitter.TryAcquirePeerServe()
		if !admitted {
			w.Header().Set("Retry-After", "1")
			http.Error(w, "peer serve capacity exhausted", http.StatusServiceUnavailable)
			return
		}
		defer release()
	}

	rawPath := r.URL.Query().Get("path")
	if rawPath == "" {
		http.Error(w, "path query parameter required", http.StatusBadRequest)
		return
	}
	// filepath.Clean resolves any ".." traversal to a rooted path.
	clean, err := sanitizePath(rawPath)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Heat tracking mirrors the gRPC serve path: distinct remote readers
	// raise the file's replica target in the reconciler.
	if observer, ok := h.cacheManager.(cache.RemoteReadObserver); ok {
		observer.NoteRemoteReader(clean, r.RemoteAddr)
	}

	lcs, ok := h.cacheManager.(localChunkServer)
	if !ok {
		http.Error(w, "peer read not supported", http.StatusNotImplemented)
		return
	}
	ctx := r.Context()

	// Optional sub-range of the object: off/len (bytes). A reader that needs
	// a 1 MiB window of a 16 MiB chunk gets 1 MiB, not the chunk.
	subOff, subLen, sub, err := peerReadSubRange(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Whole file held locally: stream it directly (sendfile).
	if localPath, ok := lcs.LocalFilePath(ctx, clean); ok {
		if sub {
			h.serveSubRange(w, localPath, 0, -1, subOff, subLen)
			return
		}
		h.serveFileRange(w, localPath, 0, -1)
		return
	}
	// Chunk of a whole parent file: stream the byte range (sendfile).
	if localPath, offset, length, ok := lcs.LocalChunkFile(ctx, clean); ok {
		if sub {
			h.serveSubRange(w, localPath, offset, length, subOff, subLen)
			return
		}
		h.serveFileRange(w, localPath, offset, length)
		return
	}
	http.Error(w, "not found", http.StatusNotFound)
}

// serveFileRange streams [offset, offset+length) of localPath to w. length < 0
// means "to end of file". Content-Length is set so the HTTP server avoids
// chunked encoding and can take the sendfile fast path.
func (h *Handler) serveFileRange(w http.ResponseWriter, localPath string, offset, length int64) {
	f, err := os.Open(localPath)
	if err != nil {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	defer f.Close()

	if length < 0 {
		info, err := f.Stat()
		if err != nil {
			http.Error(w, "stat failed", http.StatusInternalServerError)
			return
		}
		length = info.Size() - offset
		if length < 0 {
			length = 0
		}
	}
	if offset > 0 {
		if _, err := f.Seek(offset, io.SeekStart); err != nil {
			http.Error(w, "seek failed", http.StatusInternalServerError)
			return
		}
	}

	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Length", strconv.FormatInt(length, 10))
	w.WriteHeader(http.StatusOK)

	// io.CopyN wraps f in an *io.LimitedReader; net/http's response.ReadFrom
	// forwards *os.File / *io.LimitedReader to the TCP connection's sendfile
	// path, so this is zero-copy on the plain in-cluster connection.
	if _, err := io.CopyN(w, f, length); err != nil {
		// Header/Content-Length already written; nothing to do but stop.
		// A requester that raced several holders hangs up on the losers: a
		// broken pipe or reset is that, not a serve failure, and logging
		// each one buried real errors (39 lines per cold start on cpu-w3).
		if errors.Is(err, syscall.EPIPE) || errors.Is(err, syscall.ECONNRESET) {
			h.peerReadAborted.Add(1)
			return
		}
		h.logger.Printf("peer read stream error path=%s off=%d len=%d: %v", localPath, offset, length, err)
	}
}

// peerReadSubRange parses the optional off/len query parameters.
func peerReadSubRange(r *http.Request) (off, n int64, ok bool, err error) {
	q := r.URL.Query()
	if q.Get("off") == "" && q.Get("len") == "" {
		return 0, 0, false, nil
	}
	off, err1 := strconv.ParseInt(q.Get("off"), 10, 64)
	n, err2 := strconv.ParseInt(q.Get("len"), 10, 64)
	if err1 != nil || err2 != nil || off < 0 || n <= 0 {
		return 0, 0, false, fmt.Errorf("off and len must be a byte offset and a positive length")
	}
	return off, n, true, nil
}

// serveSubRange serves [subOff, subOff+subLen) of the object held at
// [offset, offset+length) of localPath (length < 0: to end of file), clamped
// to the object.
func (h *Handler) serveSubRange(w http.ResponseWriter, localPath string, offset, length, subOff, subLen int64) {
	if length < 0 {
		info, err := os.Stat(localPath)
		if err != nil {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		length = info.Size() - offset
	}
	if subOff >= length {
		http.Error(w, "range outside object", http.StatusRequestedRangeNotSatisfiable)
		return
	}
	if subLen > length-subOff { // no subOff+subLen: it can overflow int64
		subLen = length - subOff
	}
	h.serveFileRange(w, localPath, offset+subOff, subLen)
}

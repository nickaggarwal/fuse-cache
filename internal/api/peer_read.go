package api

import (
	"context"
	"io"
	"net/http"
	"os"
	"strconv"
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

	lcs, ok := h.cacheManager.(localChunkServer)
	if !ok {
		http.Error(w, "peer read not supported", http.StatusNotImplemented)
		return
	}
	ctx := r.Context()

	// Whole file held locally: stream it directly (sendfile).
	if localPath, ok := lcs.LocalFilePath(ctx, clean); ok {
		h.serveFileRange(w, localPath, 0, -1)
		return
	}
	// Chunk of a whole parent file: stream the byte range (sendfile).
	if localPath, offset, length, ok := lcs.LocalChunkFile(ctx, clean); ok {
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
		h.logger.Printf("peer read stream error path=%s off=%d len=%d: %v", localPath, offset, length, err)
	}
}

package api

import (
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// commitRequest is the body of POST /api/internal/commit: a front server
// (rust/store store-front) received a PUT, staged the body next to its final
// path and verified it; Go adopts the staged file as if its own PUT handler
// had written it. Go stays the only writer of the index, the coordinator
// rows and the cloud copy.
type commitRequest struct {
	// Path is the file path, as in PUT /api/files/<path>.
	Path string `json:"path"`
	// Staged is the staged file's path relative to the cache dir; it must be
	// a "<base>.stage-<suffix>" sibling of Path, the name CreateWriteFile
	// uses (startup rehydration skips those).
	Staged string `json:"staged"`
	Size   int64  `json:"size"`
	// SHA256 is the body's hex digest, as the front computed it. For an OCI
	// blob path it must equal the path's digest.
	SHA256 string `json:"sha256"`
}

// stagedCommitter is the cache manager surface commit needs: the same two
// calls the streaming PUT path uses, plus the cache dir to resolve paths.
type stagedCommitter interface {
	streamingWriter
	NVMeDir() string
}

// handleInternalCommit adopts a file a front server staged. Loopback only:
// the front runs in the same pod, and nothing outside it may name files on
// this node's disk. Every check Go's own PUT makes is made again here, so a
// front bug can refuse a write but never store a wrong one.
func (h *Handler) handleInternalCommit(w http.ResponseWriter, r *http.Request) {
	if !isLoopback(r.RemoteAddr) {
		http.Error(w, "commit is loopback only", http.StatusForbidden)
		return
	}
	sc, ok := h.cacheManager.(stagedCommitter)
	if !ok {
		http.Error(w, "commit needs the streaming store", http.StatusNotImplemented)
		return
	}
	var req commitRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 64<<10)).Decode(&req); err != nil {
		http.Error(w, "bad commit request", http.StatusBadRequest)
		return
	}
	filePath, staged, err := commitPaths(sc.NVMeDir(), req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	// The staged file is removed on every refusal, so a refused commit
	// leaves nothing behind; after a successful rename this is a no-op.
	defer os.Remove(staged)

	if req.Size < 0 || req.Size > h.maxUpload() {
		http.Error(w, "File too large", http.StatusRequestEntityTooLarge)
		return
	}
	info, err := os.Lstat(staged)
	if err != nil || !info.Mode().IsRegular() {
		http.Error(w, "staged file missing", http.StatusBadRequest)
		return
	}
	if info.Size() != req.Size {
		http.Error(w, "staged file size does not match", http.StatusBadRequest)
		return
	}
	if want := ociBlobDigestFromPath(filePath); want != "" && !strings.EqualFold(req.SHA256, want) {
		h.logger.Printf("Rejected OCI blob commit %s: front reported %s", filePath, req.SHA256)
		http.Error(w, "content does not match the blob digest", http.StatusBadRequest)
		return
	}
	if err := sc.PutFromFile(r.Context(), filePath, staged, req.Size, time.Now()); err != nil {
		h.logger.Printf("Failed to commit staged file %s: %v", filePath, err)
		http.Error(w, "Failed to store file", http.StatusInternalServerError)
		return
	}
	if ociBlobDigestFromPath(filePath) != "" {
		if bv, ok := h.cacheManager.(blobVerifyMarker); ok {
			bv.MarkBlobVerified(filePath)
		}
	}
	w.WriteHeader(http.StatusOK)
	h.logger.Printf("Stored file: %s (%d bytes, via front)", filePath, req.Size)
}

// commitPaths validates a commit request's two paths and returns the file
// path (as PUT /api/files would sanitize it) and the staged file's absolute
// path, which must sit beside the final file under the cache dir.
func commitPaths(cacheDir string, req commitRequest) (string, string, error) {
	if cacheDir == "" {
		return "", "", errors.New("no cache dir")
	}
	if strings.TrimSpace(req.Path) == "" || strings.TrimSpace(req.Staged) == "" {
		return "", "", errors.New("path and staged are required")
	}
	filePath, err := sanitizePath(req.Path)
	if err != nil || filePath == "/" {
		return "", "", errors.New("bad path")
	}
	stagedRel, err := sanitizePath(req.Staged)
	if err != nil || stagedRel != "/"+strings.TrimPrefix(req.Staged, "/") {
		return "", "", errors.New("bad staged path")
	}
	if filepath.Dir(stagedRel) != filepath.Dir(filePath) || !strings.HasPrefix(filepath.Base(stagedRel), filepath.Base(filePath)+".stage-") {
		return "", "", errors.New("staged file must be a .stage- sibling of path")
	}
	return filePath, filepath.Join(cacheDir, strings.TrimPrefix(stagedRel, "/")), nil
}

func isLoopback(remoteAddr string) bool {
	host, _, err := net.SplitHostPort(remoteAddr)
	if err != nil {
		host = remoteAddr
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

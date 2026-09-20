package api

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"fuse-client/internal/cache"

	"github.com/gorilla/mux"
)

// HTTP surface for the Model KV store.
//
// This exists primarily so the durability claim can be tested against a real
// deployment: recovery is only meaningful if it can be driven from outside the
// process that is about to be killed. The endpoints are deliberately thin —
// all policy lives in cache.ModelKVStore.

// SetModelKVStore attaches a store to the handler. Nil leaves the endpoints
// returning 503, so a daemon built without KV support answers honestly rather
// than 404-ing as if the route did not exist.
func (h *Handler) SetModelKVStore(s *cache.ModelKVStore) {
	h.modelKV.Store(s)
}

func (h *Handler) kvStore(w http.ResponseWriter) (*cache.ModelKVStore, bool) {
	s := h.modelKV.Load()
	if s == nil {
		http.Error(w, "model KV store not enabled on this node", http.StatusServiceUnavailable)
		return nil, false
	}
	return s, true
}

func (h *Handler) registerModelKVRoutes(router *mux.Router) {
	router.HandleFunc("/api/kv/stats", h.handleKVStats).Methods("GET")
	router.HandleFunc("/api/kv/flush", h.handleKVFlush).Methods("POST")
	router.HandleFunc("/api/kv/recover", h.handleKVRecover).Methods("POST")
	router.HandleFunc("/api/kv/reap", h.handleKVReap).Methods("POST")
	// Block CRUD is last so /api/kv/{key} cannot shadow the verbs above.
	router.HandleFunc("/api/kv/blocks/{key}", h.handleKVGet).Methods("GET")
	router.HandleFunc("/api/kv/blocks/{key}", h.handleKVPut).Methods("PUT")
}

func (h *Handler) handleKVStats(w http.ResponseWriter, r *http.Request) {
	s, ok := h.kvStore(w)
	if !ok {
		return
	}
	writeJSON(w, http.StatusOK, s.Stats())
}

func (h *Handler) handleKVPut(w http.ResponseWriter, r *http.Request) {
	s, ok := h.kvStore(w)
	if !ok {
		return
	}
	key := mux.Vars(r)["key"]
	prefix := r.URL.Query().Get("prefix")
	if prefix == "" {
		prefix = "default"
	}
	data, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxKVBlockBytes))
	if err != nil {
		http.Error(w, fmt.Sprintf("read body: %v", err), http.StatusBadRequest)
		return
	}
	if err := s.Put(r.Context(), key, prefix, data); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"key":    key,
		"prefix": prefix,
		"bytes":  len(data),
	})
}

func (h *Handler) handleKVGet(w http.ResponseWriter, r *http.Request) {
	s, ok := h.kvStore(w)
	if !ok {
		return
	}
	key := mux.Vars(r)["key"]
	data, err := s.Get(r.Context(), key)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(data)
}

// handleKVFlush forces a batch flush. The e2e test uses it so the sequence is
// mutate, force, assert — causal — rather than sleeping past BatchInterval and
// testing the clock.
func (h *Handler) handleKVFlush(w http.ResponseWriter, r *http.Request) {
	s, ok := h.kvStore(w)
	if !ok {
		return
	}
	prefix := r.URL.Query().Get("prefix")
	if prefix == "" {
		http.Error(w, "prefix query parameter is required", http.StatusBadRequest)
		return
	}
	if err := s.FlushPrefix(r.Context(), prefix); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{"flushed": prefix, "stats": s.Stats()})
}

// handleKVRecover runs a recovery pass. mode=local rebuilds from the on-disk
// journal (daemon restart); mode=cloud rebuilds from the cloud catalog (node
// loss). They are separate because they prove different things, and a test
// that conflated them could pass while cross-node recovery was broken.
func (h *Handler) handleKVRecover(w http.ResponseWriter, r *http.Request) {
	s, ok := h.kvStore(w)
	if !ok {
		return
	}
	mode := r.URL.Query().Get("mode")
	if mode == "" {
		mode = "local"
	}
	switch mode {
	case "local":
		if err := s.RecoverIndex(r.Context()); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		writeJSON(w, http.StatusOK, map[string]interface{}{"mode": "local", "stats": s.Stats()})
	case "cloud":
		prefix := r.URL.Query().Get("prefix")
		if prefix == "" {
			http.Error(w, "prefix query parameter is required for mode=cloud", http.StatusBadRequest)
			return
		}
		n, err := s.RecoverFromCloud(r.Context(), prefix)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		writeJSON(w, http.StatusOK, map[string]interface{}{
			"mode":      "cloud",
			"prefix":    prefix,
			"recovered": n,
			"stats":     s.Stats(),
		})
	default:
		http.Error(w, "mode must be 'local' or 'cloud'", http.StatusBadRequest)
	}
}

func (h *Handler) handleKVReap(w http.ResponseWriter, r *http.Request) {
	s, ok := h.kvStore(w)
	if !ok {
		return
	}
	n := s.ReapExpired(r.Context())
	writeJSON(w, http.StatusOK, map[string]interface{}{"reaped": n, "stats": s.Stats()})
}

// maxKVBlockBytes bounds a single uploaded block. KV chunks are megabytes, not
// gigabytes; the ceiling is here so a malformed client cannot drive the daemon
// out of memory.
const maxKVBlockBytes = 512 << 20

func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

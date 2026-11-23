package api

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"
	"time"
)

// holderRenewer is the cache side of POST /api/holders/renew.
type holderRenewer interface {
	RenewHolders(ctx context.Context) (renewed, retracted int)
}

// handleHolderRenew runs one holder renewal pass now (holder_lease.go):
// renew rows for what this node holds on disk, retract rows for what it no
// longer holds, retry pending retractions. Operators and tests use it to
// make a change visible without waiting for the jittered timer.
func (h *Handler) handleHolderRenew(w http.ResponseWriter, r *http.Request) {
	hr, ok := h.cacheManager.(holderRenewer)
	if !ok {
		http.Error(w, "holder renewal not supported", http.StatusNotImplemented)
		return
	}
	// Run detached from the request (bounded): a client that hangs up, or
	// a coordinator slow enough to outlast the request, must not cut the
	// pass short.
	ctx, cancel := context.WithTimeout(context.WithoutCancel(r.Context()), 2*time.Minute)
	defer cancel()
	renewed, retracted := hr.RenewHolders(ctx)
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]int{"renewed": renewed, "retracted": retracted})
}

// pathEvictor is the cache side of POST /api/cache/evict.
type pathEvictor interface {
	EvictPath(ctx context.Context, filePath string) (int64, error)
}

// handleCacheEvict evicts ?path= (a file or a prefix's files are not
// expanded: one path) from this node as LRU eviction would.
func (h *Handler) handleCacheEvict(w http.ResponseWriter, r *http.Request) {
	ev, ok := h.cacheManager.(pathEvictor)
	if !ok {
		http.Error(w, "eviction not supported", http.StatusNotImplemented)
		return
	}
	p, err := sanitizePath(r.URL.Query().Get("path"))
	if err != nil || p == "/" {
		http.Error(w, "path required", http.StatusBadRequest)
		return
	}
	freed, err := ev.EvictPath(r.Context(), p)
	if err != nil {
		http.Error(w, err.Error(), http.StatusConflict)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{"path": p, "freed": freed})
}

// ringSetter is the cache side of POST /api/ring.
type ringSetter interface {
	SetRingReplicas(r int)
}

// handleRingSet sets ring placement at runtime: POST /api/ring?replicas=R
// (0 disables). Lets an operator or an A/B switch placement fleet-wide
// without restarting clients (which would also reset their counters).
func (h *Handler) handleRingSet(w http.ResponseWriter, r *http.Request) {
	rs, ok := h.cacheManager.(ringSetter)
	if !ok {
		http.Error(w, "ring placement not supported", http.StatusNotImplemented)
		return
	}
	n, err := strconv.Atoi(r.URL.Query().Get("replicas"))
	if err != nil || n < 0 || n > 8 {
		http.Error(w, "replicas must be 0..8", http.StatusBadRequest)
		return
	}
	rs.SetRingReplicas(n)
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]int{"replicas": n})
}

package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"

	"fuse-client/internal/cache"
)

// durabilityReporter is the receipt capability (implemented by
// *cache.DefaultCacheManager). Optional so test doubles need not implement it.
type durabilityReporter interface {
	DurabilityReceipt(ctx context.Context, req cache.DurabilityRequest) (*cache.DurabilityReceipt, error)
}

// durableRequest is the wire form of POST /api/cache/durable.
type durableRequest struct {
	Paths  []string `json:"paths,omitempty"`
	Prefix string   `json:"prefix,omitempty"`
	// TimeoutSeconds is how long to wait for every path to become
	// persisted. 0 answers immediately. Capped server-side.
	TimeoutSeconds float64 `json:"timeout_seconds,omitempty"`
	RetryFailed    bool    `json:"retry_failed,omitempty"`
}

// handleCacheDurable answers "do these artifacts survive node loss".
//
//	POST /api/cache/durable {"paths":[...]} | {"prefix":"/ckpt/x"}, "timeout_seconds":N, "retry_failed":bool
//	GET  /api/cache/durable?path=/a&path=/b | ?prefix=/ckpt/x  (no wait, no side effects)
//
// 200 with "durable": true is the receipt: every listed path, including a
// chunked file's parent, is in cloud storage. 202 with "durable": false means
// not yet (the body says which paths and why); retry or pass a timeout. 400 is
// a request the caller must fix (no paths, root prefix, chunk path, too many
// paths).
func (h *Handler) handleCacheDurable(w http.ResponseWriter, r *http.Request) {
	reporter, ok := h.cacheManager.(durabilityReporter)
	if !ok {
		http.Error(w, "durability receipts not supported by this cache", http.StatusNotImplemented)
		return
	}
	var req durableRequest
	if r.Method == http.MethodGet {
		req.Paths = r.URL.Query()["path"]
		req.Prefix = r.URL.Query().Get("prefix")
	} else if r.ContentLength != 0 {
		dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
		dec.DisallowUnknownFields()
		if err := dec.Decode(&req); err != nil {
			http.Error(w, "invalid request body: "+err.Error(), http.StatusBadRequest)
			return
		}
	}
	if req.TimeoutSeconds < 0 {
		http.Error(w, "timeout_seconds must not be negative", http.StatusBadRequest)
		return
	}
	wait := time.Duration(req.TimeoutSeconds * float64(time.Second))
	if wait > cache.DurabilityMaxWait {
		wait = cache.DurabilityMaxWait
	}
	receipt, err := reporter.DurabilityReceipt(r.Context(), cache.DurabilityRequest{
		Paths:       req.Paths,
		Prefix:      strings.TrimSpace(req.Prefix),
		Wait:        wait,
		RetryFailed: req.RetryFailed && r.Method == http.MethodPost,
	})
	if err != nil {
		status := http.StatusInternalServerError
		if errors.Is(err, cache.ErrDurabilityBadRequest) {
			status = http.StatusBadRequest
		}
		http.Error(w, err.Error(), status)
		return
	}
	if receipt.PeerID == "" {
		receipt.PeerID = h.peerID
	}
	w.Header().Set("Content-Type", "application/json")
	if receipt.Durable {
		w.WriteHeader(http.StatusOK)
	} else {
		w.WriteHeader(http.StatusAccepted)
	}
	_ = json.NewEncoder(w).Encode(receipt)
}

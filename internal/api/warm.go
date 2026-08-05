package api

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"fuse-client/internal/cache"
)

// POST /api/cache/warm — trigger a declarative warmup of a prefix on THIS
// node, same engine as CSI warmup=full sessions. The coordinator's /api/warm
// fan-out calls this per selected node with async=true.

// warmRequest is the JSON body for /api/cache/warm.
type warmRequest struct {
	Prefix    string `json:"prefix"`
	Mode      string `json:"mode,omitempty"`      // default "full"
	Source    string `json:"source,omitempty"`    // peer-first | cloud-first | cloud-only
	Bandwidth string `json:"bandwidth,omitempty"` // background | max
	// Async returns 202 immediately and warms in the background (results in
	// logs + cache stats). Synchronous callers get the full WarmupResult and
	// control the wait via TimeoutSeconds (default 600).
	Async          bool `json:"async,omitempty"`
	TimeoutSeconds int  `json:"timeout_seconds,omitempty"`
}

func (h *Handler) handleCacheWarm(w http.ResponseWriter, r *http.Request) {
	warmer, ok := h.cacheManager.(cache.PrefixWarmer)
	if !ok {
		http.Error(w, "warmup not supported by this cache manager", http.StatusNotImplemented)
		return
	}

	var req warmRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid JSON body", http.StatusBadRequest)
		return
	}
	if req.Prefix == "" {
		http.Error(w, "prefix is required", http.StatusBadRequest)
		return
	}
	if req.Mode == "" {
		req.Mode = "full"
	}
	opts := cache.WarmupOptions{Mode: req.Mode, Source: req.Source, Bandwidth: req.Bandwidth}
	timeout := time.Duration(req.TimeoutSeconds) * time.Second
	if timeout <= 0 {
		timeout = 10 * time.Minute
	}

	if req.Async {
		go func() {
			// Detached from the request: warm outlives the HTTP call.
			ctx, cancel := context.WithTimeout(context.Background(), timeout)
			defer cancel()
			if res, err := warmer.WarmPrefixOpts(ctx, req.Prefix, opts); err != nil {
				h.logger.Printf("Async warm of %s failed: %v", req.Prefix, err)
			} else {
				h.logger.Printf("Async warm of %s done: files=%d warmed=%d already_local=%d skipped=%d failed=%d bytes=%d",
					req.Prefix, res.Files, res.Warmed, res.AlreadyLocal, res.Skipped, res.Failed, res.Bytes)
			}
		}()
		w.WriteHeader(http.StatusAccepted)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"started": true, "prefix": req.Prefix, "mode": req.Mode,
			"source": req.Source, "bandwidth": req.Bandwidth,
		})
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), timeout)
	defer cancel()
	res, err := warmer.WarmPrefixOpts(ctx, req.Prefix, opts)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	json.NewEncoder(w).Encode(res)
}

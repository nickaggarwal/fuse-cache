package api

import (
	"context"
	"encoding/json"
	"net/http"
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

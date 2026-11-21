package api

import (
	"context"
	"encoding/json"
	"net/http"
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
	renewed, retracted := hr.RenewHolders(r.Context())
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]int{"renewed": renewed, "retracted": retracted})
}

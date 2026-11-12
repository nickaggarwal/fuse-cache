package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

// POST /api/cache/warm-image: pre-warm a whole image from its registry.
//
//	{"registry": "myacr.azurecr.io", "repo": "models/qwen7b-cpu",
//	 "digests": ["sha256:...", ...], "token": "<registry bearer token>"}
//
// Every blob is filled exactly as a nydusd miss would fill it
// (registry_fill.go): range GETs in fetch units, hashed against the digest,
// committed and published only when they match, one node per blob through
// the coordinator lease. All of an image's blobs start at once, so the
// node-wide range-GET pool (-registry-fill-parallel, default 16) stays full:
// on cpu-w3 that measured 237 MB/s from ACR against 80 MB/s for one stream.
//
// The caller passes the layer digests (it already has the manifest; this
// proxy never sees one) and a pull token, used only for these downloads and
// never stored. The response waits until every blob is in, failed, or being
// filled by another node, and reports per-blob outcomes.

type warmImageRequest struct {
	Registry string   `json:"registry"`
	Repo     string   `json:"repo"`
	Digests  []string `json:"digests"`
	Token    string   `json:"token"`
}

type warmImageBlob struct {
	Digest string `json:"digest"`
	State  string `json:"state"` // filled | already_local | other_node | failed
	Error  string `json:"error,omitempty"`
}

type warmImageResponse struct {
	Blobs   []warmImageBlob `json:"blobs"`
	Seconds float64         `json:"seconds"`
	Failed  int             `json:"failed"`
}

func (h *Handler) handleWarmImage(w http.ResponseWriter, r *http.Request) {
	var req warmImageRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&req); err != nil {
		http.Error(w, "bad request: "+err.Error(), http.StatusBadRequest)
		return
	}
	reg := strings.TrimSpace(req.Registry)
	repo := strings.Trim(strings.TrimSpace(req.Repo), "/")
	if reg == "" || repo == "" || len(req.Digests) == 0 {
		http.Error(w, "registry, repo and digests are required", http.StatusBadRequest)
		return
	}
	if !h.regFill.hostAllowed(reg) {
		http.Error(w, "registry not allowed by -registry-fill-hosts", http.StatusForbidden)
		return
	}
	if h.regFill.sem == nil {
		http.Error(w, "registry fill not configured", http.StatusServiceUnavailable)
		return
	}
	auth := ""
	if req.Token != "" {
		auth = "Bearer " + req.Token
	}

	start := time.Now()
	out := make([]warmImageBlob, len(req.Digests))
	var wg sync.WaitGroup
	for i, d := range req.Digests {
		m := ociDigestHex.FindStringSubmatch(strings.TrimSpace(d))
		if m == nil {
			out[i] = warmImageBlob{Digest: d, State: "failed", Error: "not a sha256 digest"}
			continue
		}
		hex := m[2]
		out[i].Digest = "sha256:" + hex
		if _, ok := h.ociBlobEntry(r.Context(), OCIBlobPrefix+hex); ok {
			out[i].State = "already_local"
			continue
		}
		src := &url.URL{Scheme: "https", Host: reg, Path: fmt.Sprintf("/v2/%s/blobs/sha256:%s", repo, hex)}
		wg.Add(1)
		i := i
		h.startRegistryFill(src, auth, hex, func(err error) {
			defer wg.Done()
			switch {
			case err == nil:
				out[i].State = "filled"
			case errors.Is(err, errFillLeaseLost):
				out[i].State = "other_node"
			default:
				out[i].State = "failed"
				out[i].Error = err.Error()
			}
		})
	}
	wg.Wait()
	// A blob that was already being filled (started == false) reports nil
	// at once; wait for its fill to land so "filled" means committed.
	for i := range out {
		if out[i].State != "filled" {
			continue
		}
		hex := strings.TrimPrefix(out[i].Digest, "sha256:")
		for h.regFill.running(hex) {
			select {
			case <-r.Context().Done():
				return
			case <-time.After(200 * time.Millisecond):
			}
		}
		if _, ok := h.ociBlobEntry(r.Context(), OCIBlobPrefix+hex); !ok {
			out[i].State = "failed"
			out[i].Error = "fill did not commit"
		}
	}
	resp := warmImageResponse{Blobs: out, Seconds: time.Since(start).Seconds()}
	for _, b := range out {
		if b.State == "failed" {
			resp.Failed++
		}
	}
	w.Header().Set("Content-Type", "application/json")
	if resp.Failed > 0 {
		w.WriteHeader(http.StatusMultiStatus)
	}
	_ = json.NewEncoder(w).Encode(resp)
}

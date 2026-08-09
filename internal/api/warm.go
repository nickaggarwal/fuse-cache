package api

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"sort"
	"sync"
	"time"

	"fuse-client/internal/cache"

	"github.com/gorilla/mux"
)

// POST /api/cache/warm — trigger a declarative warmup of a prefix on THIS
// node, same engine as CSI warmup=full sessions. The coordinator's /api/warm
// fan-out calls this per selected node with async=true.
//
// Async warms return a job ID. GET /api/cache/warm/{id} reports live progress
// and GET /api/cache/warm lists recent jobs — without them an async warm is
// fire-and-forget and a multi-hour model distribution is unobservable except
// through logs.

// warmRequest is the JSON body for /api/cache/warm.
type warmRequest struct {
	Prefix    string `json:"prefix"`
	Mode      string `json:"mode,omitempty"`      // default "full"
	Source    string `json:"source,omitempty"`    // peer-first | cloud-first | cloud-only
	Bandwidth string `json:"bandwidth,omitempty"` // background | max
	// Async returns 202 immediately and warms in the background (poll
	// /api/cache/warm/{id}). Synchronous callers get the full WarmupResult and
	// control the wait via TimeoutSeconds (default 600).
	Async          bool `json:"async,omitempty"`
	TimeoutSeconds int  `json:"timeout_seconds,omitempty"`
}

// warmJobStatus values.
const (
	warmJobRunning = "running"
	warmJobDone    = "done"
	warmJobFailed  = "failed"
)

// warmJobRetention bounds the completed-job history: enough to inspect the
// last few warms, not a leak on a node that warms continuously.
const warmJobRetention = 32

// warmJob is one tracked warmup pass on this node.
type warmJob struct {
	ID        string               `json:"id"`
	Prefix    string               `json:"prefix"`
	Mode      string               `json:"mode"`
	Source    string               `json:"source,omitempty"`
	Bandwidth string               `json:"bandwidth,omitempty"`
	Status    string               `json:"status"`
	StartedAt time.Time            `json:"started_at"`
	UpdatedAt time.Time            `json:"updated_at"`
	EndedAt   *time.Time           `json:"ended_at,omitempty"`
	Progress  cache.WarmupProgress `json:"progress"`
	Result    *cache.WarmupResult  `json:"result,omitempty"`
	Error     string               `json:"error,omitempty"`
}

// warmJobs is the per-node registry of async warm passes.
type warmJobs struct {
	mu   sync.Mutex
	jobs map[string]*warmJob
}

func newWarmJobID() string {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		// Only reachable if the system CSPRNG is broken; a time-based ID is
		// still unique enough to correlate one node's logs.
		return "warm-" + time.Now().UTC().Format("20060102T150405.000000000")
	}
	return "warm-" + hex.EncodeToString(b[:])
}

func (r *warmJobs) start(req warmRequest) *warmJob {
	now := time.Now()
	job := &warmJob{
		ID: newWarmJobID(), Prefix: req.Prefix, Mode: req.Mode,
		Source: req.Source, Bandwidth: req.Bandwidth,
		Status: warmJobRunning, StartedAt: now, UpdatedAt: now,
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.jobs == nil {
		r.jobs = make(map[string]*warmJob)
	}
	r.jobs[job.ID] = job
	r.pruneLocked()
	return job
}

func (r *warmJobs) progress(id string, p cache.WarmupProgress) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if job := r.jobs[id]; job != nil {
		job.Progress = p
		job.UpdatedAt = time.Now()
	}
}

func (r *warmJobs) finish(id string, res *cache.WarmupResult, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	job := r.jobs[id]
	if job == nil {
		return
	}
	now := time.Now()
	job.UpdatedAt = now
	job.EndedAt = &now
	if err != nil {
		job.Status = warmJobFailed
		job.Error = err.Error()
		return
	}
	job.Status = warmJobDone
	job.Result = res
	if res != nil {
		job.Progress = cache.WarmupProgress{
			Files: res.Files, Done: res.Warmed + res.AlreadyLocal + res.Skipped + res.Failed,
			Warmed: res.Warmed, AlreadyLocal: res.AlreadyLocal,
			Skipped: res.Skipped, Failed: res.Failed, Bytes: res.Bytes,
		}
	}
}

func (r *warmJobs) get(id string) (warmJob, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	job, ok := r.jobs[id]
	if !ok {
		return warmJob{}, false
	}
	return *job, true
}

// list returns a snapshot of all tracked jobs, newest first.
func (r *warmJobs) list() []warmJob {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]warmJob, 0, len(r.jobs))
	for _, job := range r.jobs {
		out = append(out, *job)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].StartedAt.After(out[j].StartedAt) })
	return out
}

// pruneLocked drops the oldest finished jobs past warmJobRetention. Running
// jobs are never dropped — a long warm must stay pollable however many short
// ones run alongside it.
func (r *warmJobs) pruneLocked() {
	if len(r.jobs) <= warmJobRetention {
		return
	}
	finished := make([]*warmJob, 0, len(r.jobs))
	for _, job := range r.jobs {
		if job.Status != warmJobRunning {
			finished = append(finished, job)
		}
	}
	sort.Slice(finished, func(i, j int) bool { return finished[i].StartedAt.Before(finished[j].StartedAt) })
	for _, job := range finished {
		if len(r.jobs) <= warmJobRetention {
			return
		}
		delete(r.jobs, job.ID)
	}
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
		job := h.warmJobs.start(req)
		jobOpts := opts
		jobOpts.OnProgress = func(p cache.WarmupProgress) { h.warmJobs.progress(job.ID, p) }
		go func() {
			// Detached from the request: warm outlives the HTTP call.
			ctx, cancel := context.WithTimeout(context.Background(), timeout)
			defer cancel()
			res, err := warmer.WarmPrefixOpts(ctx, req.Prefix, jobOpts)
			h.warmJobs.finish(job.ID, res, err)
			if err != nil {
				h.logger.Printf("Async warm %s of %s failed: %v", job.ID, req.Prefix, err)
				return
			}
			h.logger.Printf("Async warm %s of %s done: files=%d warmed=%d already_local=%d skipped=%d failed=%d bytes=%d",
				job.ID, req.Prefix, res.Files, res.Warmed, res.AlreadyLocal, res.Skipped, res.Failed, res.Bytes)
		}()
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"started": true, "job_id": job.ID, "prefix": req.Prefix, "mode": req.Mode,
			"source": req.Source, "bandwidth": req.Bandwidth,
			"status_url": "/api/cache/warm/" + job.ID,
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
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(res)
}

// Declarative warm intents over HTTP.
//
// An intent is normally declared by a CSI session (volumeAttributes
// warmup=full), which is the production path and the only one that carries a
// real volume lifecycle. These endpoints are the second entrance to the same
// registry, and they exist for a specific reason: the CSI path requires a
// scheduled pod on a node running the CSI node plugin, so on a cluster with
// the client DaemonSet but no CSI DaemonSet the reconciler is unreachable and
// therefore untestable. Both entrances register the same warmIntent and are
// re-asserted by the same loop, so exercising this one is evidence about the
// CSI one.
//
// The volume ID is caller-supplied here rather than minted, so a test can
// declare, force a pass, and forget deterministically. Intents declared this
// way are not tied to any mount and so are never dropped by a teardown hook —
// the caller owns the DELETE.

// warmIntentRequest is the JSON body for POST /api/cache/intents.
type warmIntentRequest struct {
	VolumeID  string `json:"volume_id"`
	Prefix    string `json:"prefix"`
	Mode      string `json:"mode,omitempty"` // default "full"
	Source    string `json:"source,omitempty"`
	Bandwidth string `json:"bandwidth,omitempty"`
}

// warmIntentController resolves the cache manager to the intent surface.
func (h *Handler) warmIntentController(w http.ResponseWriter) (cache.WarmIntentController, bool) {
	ctrl, ok := h.cacheManager.(cache.WarmIntentController)
	if !ok {
		http.Error(w, "warm intents not supported by this cache manager", http.StatusNotImplemented)
		return nil, false
	}
	return ctrl, true
}

// handleWarmIntentList serves GET /api/cache/intents.
func (h *Handler) handleWarmIntentList(w http.ResponseWriter, r *http.Request) {
	ctrl, ok := h.warmIntentController(w)
	if !ok {
		return
	}
	intents := ctrl.WarmIntents()
	if intents == nil {
		// Encode [] rather than null: a client that distinguishes "no intents"
		// from "field absent" should not have to special-case Go's nil slice.
		intents = []cache.WarmIntentStatus{}
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"peer_id": h.peerID,
		"intents": intents,
	})
}

// handleWarmIntentCreate serves POST /api/cache/intents. It returns once the
// intent is registered, not once it is satisfied — the first pass runs in the
// background exactly as it does for a CSI mount. Poll GET /api/cache/intents
// for last_verified, or /api/cache/residency for measured bytes.
func (h *Handler) handleWarmIntentCreate(w http.ResponseWriter, r *http.Request) {
	ctrl, ok := h.warmIntentController(w)
	if !ok {
		return
	}
	var req warmIntentRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid JSON body", http.StatusBadRequest)
		return
	}
	if req.Prefix == "" {
		http.Error(w, "prefix is required", http.StatusBadRequest)
		return
	}
	if req.VolumeID == "" {
		http.Error(w, "volume_id is required", http.StatusBadRequest)
		return
	}
	if req.Mode == "" {
		req.Mode = "full"
	}
	ctrl.RegisterWarmIntent(req.VolumeID, req.Prefix, cache.WarmupOptions{
		Mode: req.Mode, Source: req.Source, Bandwidth: req.Bandwidth,
	})
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"registered": true, "volume_id": req.VolumeID, "prefix": req.Prefix,
		"mode": req.Mode, "source": req.Source, "bandwidth": req.Bandwidth,
	})
}

// handleWarmIntentDelete serves DELETE /api/cache/intents/{volumeID}.
// Idempotent, and like a CSI teardown it leaves the warmed bytes in place —
// only the obligation to keep re-asserting them goes away.
func (h *Handler) handleWarmIntentDelete(w http.ResponseWriter, r *http.Request) {
	ctrl, ok := h.warmIntentController(w)
	if !ok {
		return
	}
	volumeID := mux.Vars(r)["volumeID"]
	if volumeID == "" {
		http.Error(w, "volume_id is required", http.StatusBadRequest)
		return
	}
	ctrl.ForgetWarmIntent(volumeID)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{"forgotten": true, "volume_id": volumeID})
}

// handleWarmIntentReconcile serves POST /api/cache/intents/reconcile: run one
// re-assert pass now and block until it finishes.
//
// Synchronous on purpose. The whole point is to replace "wait out a jittered
// multi-minute tick and hope" with a causal check — mutate residency, force a
// pass, assert. An async trigger would put the race straight back.
func (h *Handler) handleWarmIntentReconcile(w http.ResponseWriter, r *http.Request) {
	ctrl, ok := h.warmIntentController(w)
	if !ok {
		return
	}
	before := ctrl.WarmIntents()
	ctrl.ReconcileWarmNow(r.Context())
	after := ctrl.WarmIntents()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"peer_id": h.peerID, "intents_reconciled": len(before), "intents": after,
	})
}

// handleCacheWarmStatus serves GET /api/cache/warm/{id}.
func (h *Handler) handleCacheWarmStatus(w http.ResponseWriter, r *http.Request) {
	job, ok := h.warmJobs.get(mux.Vars(r)["id"])
	if !ok {
		http.Error(w, "warm job not found", http.StatusNotFound)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(job)
}

// handleCacheWarmList serves GET /api/cache/warm.
func (h *Handler) handleCacheWarmList(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"peer_id": h.peerID,
		"jobs":    h.warmJobs.list(),
	})
}

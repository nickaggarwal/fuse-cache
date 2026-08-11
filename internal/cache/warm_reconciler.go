package cache

import (
	"context"
	"sort"
	"sync"
	"time"
)

// Reconciled declarative warmup.
//
// A CSI session that declares warmup=full is stating an intent about steady
// state: "this subtree should be resident on this node for as long as the
// volume is mounted". Applying that intent once at mount time does not make it
// true — the coordinator can be unreachable during the first pass, a peer can
// be busy, and LRU eviction can reclaim warmed bytes minutes later under
// pressure from another workload. Any of those leaves declared intent and
// actual residency silently divergent, and the divergence is expensive: a
// cold read pays peer or cloud latency per chunk, which is the exact cost the
// declaration existed to avoid.
//
// So intents are registered rather than executed. A background loop re-asserts
// each one periodically, and a pass that finds the subtree already resident is
// cheap: warmOneFile stats the local path first and returns AlreadyLocal
// without touching a remote tier, so steady state costs one coordinator
// enumeration plus one stat per file.
//
// Reconciliation inherits the same rules as the replica reconciler it is
// modeled on (replica_reconciler.go): advisory, never evicts to make room
// (WarmPrefixOpts is headroom-guarded per file), bounded per pass, and it
// yields the moment peers signal busy. A warm that cannot complete is a warm
// that retries next tick, not an error anyone has to handle.
const (
	// defaultWarmReconcileInterval is the re-assert cadence when enabled
	// without an explicit interval. Long relative to a read (this is drift
	// correction, not a read path) but short relative to a training job.
	defaultWarmReconcileInterval = 2 * time.Minute
	// warmReconcileJitterPct spreads passes across a fleet: every node in a
	// DaemonSet registers its intents at roughly the same moment (pods start
	// together), so an unjittered ticker would have the whole cluster
	// enumerate and re-fetch in lockstep.
	warmReconcileJitterPct = 50
)

// warmIntent is one declared warmup, keyed by the CSI volume that declared it.
type warmIntent struct {
	volumeID string
	prefix   string
	opts     WarmupOptions
	// lastResult/lastVerified describe the most recent completed pass. They
	// are what a residency query reports: an intent whose last pass failed is
	// a claim nobody should route on.
	lastResult   *WarmupResult
	lastVerified time.Time
	lastErr      error
	passes       int64
}

// warmIntentRegistry holds the declared warmup intents for this node.
//
// Deliberately in-memory, matching session.Manager, which also keeps no
// on-disk state. That means a daemon restart drops every intent — and because
// the CSI driver short-circuits NodePublishVolume when the target path is
// already a mountpoint (internal/csidriver/driver.go), a surviving bind mount
// will not recreate the session, so nothing re-registers the intent either.
// The same restart already drops pinning for the same reason. Fixing that
// belongs with session persistence, not here; until then a restarted daemon
// serves the subtree correctly through just-in-time pulls, just without the
// residency guarantee.
type warmIntentRegistry struct {
	mu      sync.Mutex
	intents map[string]*warmIntent
}

func newWarmIntentRegistry() *warmIntentRegistry {
	return &warmIntentRegistry{intents: make(map[string]*warmIntent)}
}

// register records an intent, replacing any prior one for the same volume.
// Returns false when the intent asks for no work.
func (r *warmIntentRegistry) register(volumeID, prefix string, opts WarmupOptions) bool {
	if volumeID == "" || prefix == "" {
		return false
	}
	switch opts.Mode {
	case "", "none":
		return false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.intents[volumeID] = &warmIntent{volumeID: volumeID, prefix: prefix, opts: opts}
	return true
}

// forget drops an intent (the volume's last mount went away).
func (r *warmIntentRegistry) forget(volumeID string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	_, ok := r.intents[volumeID]
	delete(r.intents, volumeID)
	return ok
}

// snapshot returns the registered intents, volume-ordered so passes are
// deterministic rather than following Go's randomized map iteration.
func (r *warmIntentRegistry) snapshot() []warmIntent {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]warmIntent, 0, len(r.intents))
	for _, in := range r.intents {
		out = append(out, *in)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].volumeID < out[j].volumeID })
	return out
}

// recordPass folds a completed pass back into the intent. A no-op if the
// intent was forgotten mid-pass (session torn down while warming).
func (r *warmIntentRegistry) recordPass(volumeID string, res *WarmupResult, err error, at time.Time) {
	r.mu.Lock()
	defer r.mu.Unlock()
	in, ok := r.intents[volumeID]
	if !ok {
		return
	}
	in.passes++
	in.lastResult = res
	in.lastErr = err
	// lastVerified advances only on a clean pass. A failed pass must not
	// refresh the timestamp: a stale-but-honest "last known good at T" is
	// routable, a fresh timestamp over unknown residency is not.
	if err == nil {
		in.lastVerified = at
	}
}

// WarmIntentStatus is the exported view of one declared warmup intent.
type WarmIntentStatus struct {
	VolumeID     string        `json:"volume_id"`
	Prefix       string        `json:"prefix"`
	Mode         string        `json:"mode"`
	Source       string        `json:"source,omitempty"`
	Bandwidth    string        `json:"bandwidth,omitempty"`
	Passes       int64         `json:"passes"`
	LastVerified time.Time     `json:"last_verified,omitempty"`
	LastError    string        `json:"last_error,omitempty"`
	LastResult   *WarmupResult `json:"last_result,omitempty"`
}

// WarmIntentController is the declarative-warmup control surface: declare an
// intent, drop it, read what is declared, force a re-assert.
//
// Two callers share this registry. The CSI session hook is the production one
// — a volume with warmup=full declares an intent on first mount. The HTTP API
// is the other, and it exists because the CSI path is only reachable from a
// scheduled pod on a node running the CSI node plugin: without a second
// entrance, the reconciler cannot be exercised on a cluster that has the
// client DaemonSet but no CSI DaemonSet, which is exactly the configuration
// most easily stood up for testing. Both paths land in the same registry and
// run the same loop, so verifying one is evidence about the other.
type WarmIntentController interface {
	RegisterWarmIntent(volumeID, prefix string, opts WarmupOptions)
	ForgetWarmIntent(volumeID string)
	WarmIntents() []WarmIntentStatus
	ReconcileWarmNow(ctx context.Context)
}

// WarmIntents returns the declared warmup intents and their last-pass state.
// This is the node-local half of the residency signal an external router
// consumes; the coordinator aggregates it across peers.
func (cm *DefaultCacheManager) WarmIntents() []WarmIntentStatus {
	if cm.warmIntents == nil {
		return nil
	}
	snap := cm.warmIntents.snapshot()
	out := make([]WarmIntentStatus, 0, len(snap))
	for _, in := range snap {
		st := WarmIntentStatus{
			VolumeID: in.volumeID, Prefix: in.prefix,
			Mode: in.opts.Mode, Source: in.opts.Source, Bandwidth: in.opts.Bandwidth,
			Passes: in.passes, LastVerified: in.lastVerified, LastResult: in.lastResult,
		}
		if in.lastErr != nil {
			st.LastError = in.lastErr.Error()
		}
		out = append(out, st)
	}
	return out
}

// RegisterWarmIntent records a declarative warmup and immediately runs the
// first pass in the background. Called from the session warmup hook, which
// fires once per volume on first mount.
func (cm *DefaultCacheManager) RegisterWarmIntent(volumeID, prefix string, opts WarmupOptions) {
	if cm.warmIntents == nil {
		return
	}
	if !cm.warmIntents.register(volumeID, prefix, opts) {
		return
	}
	// First pass now rather than at the next tick: a pod is blocked on its
	// data, and up to a full interval of latency before the first byte moves
	// would be a regression against the old fire-and-forget hook.
	cm.goBackground(func() {
		cm.reconcileOneIntent(cm.shutdownCtx, warmIntent{volumeID: volumeID, prefix: prefix, opts: opts}, true)
	})
}

// ForgetWarmIntent drops a declarative warmup when its volume is fully
// unmounted. Warmed bytes are left on NVMe deliberately — they are valid
// cache, and eviction reclaims them on real pressure. Only the obligation to
// keep re-asserting them goes away.
func (cm *DefaultCacheManager) ForgetWarmIntent(volumeID string) {
	if cm.warmIntents == nil {
		return
	}
	if cm.warmIntents.forget(volumeID) {
		cm.logger.Printf("Warm intent dropped for volume %s", volumeID)
	}
}

// startWarmReconciler launches the re-assert loop. Called from
// NewCacheManager when WarmReconcileInterval > 0.
func (cm *DefaultCacheManager) startWarmReconciler(ctx context.Context) {
	interval := cm.config.WarmReconcileInterval
	if interval <= 0 {
		interval = defaultWarmReconcileInterval
	}
	cm.goLoop(func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-time.After(jitteredDuration(interval)):
			}
			cm.reconcileWarmOnce(ctx)
		}
	})
}

// ReconcileWarmNow runs one re-assert pass synchronously, out of band with the
// ticker. The loop's cadence is minutes with ±50% jitter — deliberately, since
// this is drift correction — but that makes "evict a warmed file, watch it come
// back" a multi-minute wait keyed off a timer nobody can observe. Forcing a
// pass turns that into a causal check: mutate, force, assert. The ticker keeps
// running; a forced pass is just an extra one.
func (cm *DefaultCacheManager) ReconcileWarmNow(ctx context.Context) {
	cm.reconcileWarmOnce(ctx)
}

// reconcileWarmOnce re-asserts every declared intent once.
func (cm *DefaultCacheManager) reconcileWarmOnce(ctx context.Context) {
	if cm.warmIntents == nil {
		return
	}
	intents := cm.warmIntents.snapshot()
	if len(intents) == 0 {
		return
	}
	cm.herdStats.warmReconcileRuns.Add(1)
	for _, in := range intents {
		if ctx.Err() != nil {
			return
		}
		// Serial across intents: each WarmPrefixOpts already fans out
		// internally (fileConcurrency x chunkWorkers), so running intents
		// concurrently would multiply the configured bandwidth ceiling by the
		// number of mounted volumes.
		if busy := cm.reconcileOneIntent(ctx, in, false); busy {
			// Cluster under pressure — stop the whole pass rather than move to
			// the next intent and add more load. Same source-pressure rule the
			// replica reconciler applies.
			cm.herdStats.warmReconcileSkippedBusy.Add(1)
			return
		}
	}
}

// reconcileOneIntent runs one warm pass for one intent. Returns true when
// peers signalled busy during the pass. first marks the pass triggered by the
// mount itself, which always logs its outcome — that line is the operator's
// confirmation that a declared warmup was acted on at all.
func (cm *DefaultCacheManager) reconcileOneIntent(ctx context.Context, in warmIntent, first bool) (busy bool) {
	// Busy has to be read off the peer-storage counter, not the returned
	// error: WarmPrefixOpts folds every per-file failure into res.Failed and
	// only returns enumeration/plan errors, so isPeerBusy on its error would
	// be dead code. The delta over the pass is the honest signal.
	busyBefore := cm.PeerLoadSnapshot().FetchBusySkips

	res, err := cm.WarmPrefixOpts(ctx, in.prefix, in.opts)
	cm.warmIntents.recordPass(in.volumeID, res, err, time.Now())

	busy = cm.PeerLoadSnapshot().FetchBusySkips > busyBefore

	if err != nil {
		cm.logger.Printf("Warm reconcile for volume %s (%s) failed: %v", in.volumeID, in.prefix, err)
		return busy || isPeerBusy(err)
	}
	if res == nil {
		return busy
	}
	moved := res.Warmed > 0 || res.Failed > 0 || res.Skipped > 0
	if moved {
		cm.herdStats.warmReconcileRewarms.Add(1)
		cm.herdStats.warmReconcileFilesWarmed.Add(int64(res.Warmed))
	}
	// Otherwise only report movement: a satisfied intent is the steady state,
	// and logging it every couple of minutes per volume would bury the
	// interesting lines.
	if moved || first {
		cm.logger.Printf("Warm reconcile volume=%s prefix=%s mode=%s files=%d warmed=%d already_local=%d skipped=%d failed=%d bytes=%d",
			in.volumeID, in.prefix, res.Mode, res.Files, res.Warmed, res.AlreadyLocal, res.Skipped, res.Failed, res.Bytes)
	}
	return busy
}

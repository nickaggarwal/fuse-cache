package cache

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"fuse-client/internal/coordinator"
)

// Holder lifecycle, node side (coordinator/holder_lease.go is the other half).
//
// Before this, a node told the coordinator it held a file on admission and,
// on eviction, sent one retraction that was dropped if it failed. Nothing
// re-checked: a node that crashed, lost the network or dropped a retraction
// stayed listed as a holder forever, and a node that landed one chunk of a
// model was listed as holding the whole model.
//
// Now, against a coordinator that advertises "holder-lease":
//
//  1. Admission. Each time a path becomes held locally it gets a new
//     generation (a node-wide counter seeded from the clock, so it keeps
//     increasing across restarts). Every row this node publishes for it
//     carries that generation, a lease (now + HolderLeaseTTL) and whether
//     the node holds the whole file or only some of its chunks.
//  2. Eviction. The retraction names the generation that was evicted, so the
//     coordinator removes only rows at or below it: a late retraction cannot
//     remove the row of a copy admitted again after it. A failed retraction
//     is retried with backoff, never blocking the eviction itself.
//  3. Renewal and reconcile. Every HolderRenewInterval the node republishes
//     what it holds now (whole or partial, current generation, fresh lease)
//     and retracts what it no longer holds but was still published. This
//     repairs missed notices and stale rows in both directions.
//  4. Expiry. A node that stops renewing (crash, partition) has its rows
//     hidden by the coordinator once the lease lapses, and swept later. The
//     replica reconciler sees fewer holders; its down/out interval and
//     per-pass budget keep a mass expiry from becoming a replication storm.

const (
	// defaultHolderLeaseTTL is how long a published holder row counts
	// without renewal: 3 renewals, so one or two missed passes do not hide
	// a live holder.
	defaultHolderLeaseTTL = 15 * time.Minute
	// defaultHolderRenewInterval spaces renewal passes (jittered).
	defaultHolderRenewInterval = 5 * time.Minute
	// retractMaxAttempts bounds retries of one retraction.
	retractMaxAttempts = 6
	// holderRenewPause spaces publishes inside a pass.
	holderRenewBatch = 256
)

// holderLeases tracks, per parent path this node has published, the
// generation of its current admission and whether that was published.
type holderLeases struct {
	mu      sync.Mutex
	next    uint64
	gen     map[string]uint64 // path -> generation of the current admission
	pub     map[string]bool   // path -> a row is published for it
	enabled atomic.Bool       // coordinator advertises "holder-lease"

	renewed, retractRetried, reconcileRetracted, renewPasses atomic.Int64
	vanished                                                 atomic.Int64 // index entries whose file was gone
}

// admit returns the generation of path's current admission, starting a new
// one when the path was not held (first admission, or re-admission after an
// eviction).
func (h *holderLeases) admit(path string) uint64 {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.gen == nil {
		h.gen = map[string]uint64{}
		h.pub = map[string]bool{}
		h.next = uint64(time.Now().UnixNano())
	}
	if g, ok := h.gen[path]; ok {
		return g
	}
	h.next++
	h.gen[path] = h.next
	return h.next
}

// published records that a row for path's current admission is out.
func (h *holderLeases) published(path string) {
	h.mu.Lock()
	if h.pub != nil {
		h.pub[path] = true
	}
	h.mu.Unlock()
}

// evicted ends path's current admission and returns its generation (0 when
// it had none: an unconditional retraction).
func (h *holderLeases) evicted(path string) uint64 {
	h.mu.Lock()
	defer h.mu.Unlock()
	g := h.gen[path]
	delete(h.gen, path)
	delete(h.pub, path)
	return g
}

// snapshot returns the paths with a published row.
func (h *holderLeases) snapshot() map[string]uint64 {
	h.mu.Lock()
	defer h.mu.Unlock()
	out := make(map[string]uint64, len(h.pub))
	for p := range h.pub {
		out[p] = h.gen[p]
	}
	return out
}

func (cm *DefaultCacheManager) holderLeaseTTL() time.Duration {
	if cm.config.HolderLeaseTTL > 0 {
		return cm.config.HolderLeaseTTL
	}
	return defaultHolderLeaseTTL
}

// stampHolderRow fills a node-local row's lifecycle fields: generation of the
// current admission, lease, and partial when this node holds only chunks.
// No-op against a coordinator without "holder-lease" and for cloud rows.
func (cm *DefaultCacheManager) stampHolderRow(loc *coordinator.FileLocation) {
	if !cm.holders.enabled.Load() || loc == nil || loc.StorageTier != "nvme" {
		return
	}
	if _, isChunk := parentFilePathFromChunkPath(loc.FilePath); isChunk {
		return
	}
	loc.Generation = cm.holders.admit(loc.FilePath)
	loc.ExpiresAt = time.Now().Add(cm.holderLeaseTTL())
	loc.Partial = !cm.holdsWhole(loc.FilePath)
	cm.holders.published(loc.FilePath)
}

// holdsWhole reports whether this node has the whole file locally: a whole
// local copy, or every chunk of it.
func (cm *DefaultCacheManager) holdsWhole(path string) bool {
	cm.mu.RLock()
	e, ok := cm.entries.get(path)
	keys := cm.entries.chunkKeys(path)
	cm.mu.RUnlock()
	if !ok || e == nil || e.Size <= 0 {
		return false
	}
	if cm.LocalBlobPresent(path, e.Size) {
		return true
	}
	if !e.IsChunked {
		return e.Tier == TierNVMe
	}
	cs := cm.effectiveChunkSize(e)
	if cs <= 0 {
		return false
	}
	return int64(len(keys)) >= chunkCountFor(e.Size, cs) && cm.chunksAllLocal(path, keys)
}

func (cm *DefaultCacheManager) chunksAllLocal(path string, keys []string) bool {
	for _, k := range keys {
		if _, ok := cm.openLocalExactCheck(k); !ok {
			return false
		}
	}
	return len(keys) > 0
}

// retractHolder retracts this node's nvme row for path at generation, with
// backoff on failure. Runs in the background; eviction never waits on it.
func (cm *DefaultCacheManager) retractHolder(ctx context.Context, path string, gen uint64) bool {
	gd, okGen := cm.config.Coordinator.(coordinator.GenLocationDeleter)
	td, okTier := cm.config.Coordinator.(coordinator.TierLocationDeleter)
	if !okGen && !okTier {
		return false
	}
	backoff := time.Second
	for attempt := 0; attempt < retractMaxAttempts; attempt++ {
		if attempt > 0 {
			cm.holders.retractRetried.Add(1)
			select {
			case <-ctx.Done():
				return false
			case <-time.After(jitteredDuration(backoff)):
			}
			backoff *= 2
		}
		if cm.holdsLocally(path) {
			cm.metrics.EvictRetractSkippedRelanded.Add(1)
			return true
		}
		callCtx, cancel := context.WithTimeout(ctx, retractCallTimeout)
		var err error
		if okGen && cm.holders.enabled.Load() {
			err = gd.DeletePeerTierLocationGen(callCtx, path, cm.config.LocalPeerID, "nvme", gen)
		} else {
			err = td.DeletePeerTierLocation(callCtx, path, cm.config.LocalPeerID, "nvme")
		}
		cancel()
		if err == nil {
			cm.metrics.EvictRetracted.Add(1)
			return true
		}
	}
	cm.metrics.EvictRetractFailed.Add(1)
	return false
}

// startHolderRenewer runs the renewal and reconcile loop.
func (cm *DefaultCacheManager) startHolderRenewer() {
	if cm.config.Coordinator == nil || cm.config.LocalPeerID == "" {
		return
	}
	interval := cm.config.HolderRenewInterval
	if interval == 0 {
		interval = defaultHolderRenewInterval
	}
	if interval < 0 {
		return
	}
	cm.goLoop(func() {
		for {
			select {
			case <-cm.shutdownCtx.Done():
				return
			case <-time.After(jitteredDuration(interval)):
			}
			if cm.holders.enabled.Load() {
				cm.RenewHolders(cm.shutdownCtx)
			}
		}
	})
}

// RenewHolders republishes every parent path this node holds now (fresh
// lease, whole or partial) and retracts rows it published for paths it no
// longer holds. Returns (renewed, retracted).
func (cm *DefaultCacheManager) RenewHolders(ctx context.Context) (int, int) {
	cm.holders.renewPasses.Add(1)
	held := cm.localHolderPaths()
	renewed := 0
	for path, e := range held {
		if ctx.Err() != nil {
			break
		}
		if blobDigestFromPath(path) != "" && !cm.holdsWhole(path) && !cm.holdsAnyChunk(path) {
			continue
		}
		if blobDigestFromPath(path) != "" && cm.holdsWhole(path) {
			full := cm.config.NVMePath + "/" + strings.TrimPrefix(path, "/")
			if cm.blobLocalVerdict(ctx, path, full, false) != blobGood && !cm.holdsAnyChunk(path) {
				continue // published by verification once it passes
			}
		}
		cm.publishFileLocation(ctx, cm.republishShape(e), TierNVMe)
		renewed++
		cm.holders.renewed.Add(1)
		if renewed%holderRenewBatch == 0 {
			select {
			case <-ctx.Done():
				return renewed, 0
			case <-time.After(50 * time.Millisecond):
			}
		}
	}
	retracted := 0
	for path := range cm.holders.snapshot() {
		if _, ok := held[path]; ok || ctx.Err() != nil {
			continue
		}
		gen := cm.holders.evicted(path)
		if cm.retractHolder(ctx, path, gen) {
			retracted++
			cm.holders.reconcileRetracted.Add(1)
		}
	}
	return renewed, retracted
}

// localHolderPaths returns parent paths this node holds on local NVMe, whole
// or as chunks, with an entry describing the file.
//
// The in-memory index is checked against the disk: an entry whose file is
// gone (deleted outside this process, a disk wiped or replaced under a
// running client) is dropped from the index, so its row is retracted below
// instead of being renewed for a copy that no longer exists.
func (cm *DefaultCacheManager) localHolderPaths() map[string]*CacheEntry {
	cm.mu.RLock()
	snap := cm.entries.snapshot()
	cm.mu.RUnlock()
	out := map[string]*CacheEntry{}
	for _, e := range snap {
		if e == nil || e.Tier != TierNVMe {
			continue
		}
		if !cm.onDisk(e.FilePath) {
			cm.forgetVanished(e.FilePath)
			continue
		}
		if p, ok := parentFilePathFromChunkPath(e.FilePath); ok {
			if _, have := out[p]; !have {
				cm.mu.RLock()
				pe, ok := cm.entries.get(p)
				cm.mu.RUnlock()
				if ok && pe != nil {
					out[p] = pe
				} else {
					out[p] = &CacheEntry{FilePath: p, Tier: TierNVMe, IsChunked: true}
				}
			}
			continue
		}
		if e.Size > 0 && !cm.tombstoned(e.FilePath) {
			out[e.FilePath] = e
		}
	}
	return out
}

// onDisk reports whether path's local file exists (size checked by readers).
func (cm *DefaultCacheManager) onDisk(path string) bool {
	_, err := os.Stat(filepath.Join(cm.config.NVMePath, strings.TrimPrefix(path, "/")))
	return err == nil
}

// forgetVanished drops an index entry whose file is gone from disk.
func (cm *DefaultCacheManager) forgetVanished(path string) {
	cm.mu.Lock()
	if e, ok := cm.entries.get(path); ok && e != nil && e.Tier == TierNVMe && !cm.onDisk(path) {
		cm.entries.del(path)
		cm.unaccountNVMeLocked(path)
		cm.holders.vanished.Add(1)
	}
	cm.mu.Unlock()
}

func (cm *DefaultCacheManager) holdsAnyChunk(path string) bool {
	cm.mu.RLock()
	keys := cm.entries.chunkKeys(path)
	cm.mu.RUnlock()
	for _, k := range keys {
		if _, ok := cm.openLocalExactCheck(k); ok {
			return true
		}
	}
	return false
}

// HolderLeaseStats is exported to /metrics.
type HolderLeaseStats struct {
	Enabled                                                  bool
	Published                                                int
	Renewed, RetractRetried, ReconcileRetracted, RenewPasses int64
	Vanished                                                 int64
}

// HolderLeaseStats returns renewal counters.
func (cm *DefaultCacheManager) HolderLeaseStats() HolderLeaseStats {
	cm.holders.mu.Lock()
	n := len(cm.holders.pub)
	cm.holders.mu.Unlock()
	return HolderLeaseStats{
		Enabled: cm.holders.enabled.Load(), Published: n,
		Renewed: cm.holders.renewed.Load(), RetractRetried: cm.holders.retractRetried.Load(),
		ReconcileRetracted: cm.holders.reconcileRetracted.Load(), RenewPasses: cm.holders.renewPasses.Load(),
		Vanished: cm.holders.vanished.Load(),
	}
}

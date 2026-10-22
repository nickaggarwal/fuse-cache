package cache

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Cluster-wide delete.
//
// Delete used to free the local NVMe copy and nothing else. Peer replicas, the
// cloud blob, and the coordinator's location row all survived, so the deleted
// name kept resolving through the merged metadata view and a read re-fetched
// the file from a peer and re-materialized it locally. Delete was not merely
// incomplete, it was self-undoing, and it leaked disk on every peer plus cloud
// spend forever with no way to enumerate the orphans.
//
// "rm means gone" now spans four places: local bytes, peer replicas, the cloud
// object(s), and the coordinator rows. Only the local step decides the return
// value. The rest is best-effort and merely counted, because the repo-wide
// invariant is that coordination is advisory — a delete must not fail because
// a peer is down, and reads must keep working with the coordinator gone.
//
// Two hazards shape the design:
//
//   - Fan-out must be exactly one hop. The peer-side handler deletes locally
//     only (see PeerGRPCServer.DeleteFile); if it re-entered this function,
//     A→B would become B→{A,C} and so on without bound. That is unrecoverable
//     now that cloud objects go too.
//   - Several background paths republish a file after it is gone: the
//     write-through cloud persist (which holds an open fd that outlives the
//     unlink on Linux), chunk completion, warmup, and holder advertisement.
//     A short tombstone closes those windows; see deleteTombstones.

const (
	// deleteTombstoneTTL must comfortably exceed MetadataRefreshTTL so a
	// tombstone outlives the stale metadata view that would otherwise let the
	// deleted name resolve again, and long enough to cover an in-flight cloud
	// persist of an already-written file.
	deleteTombstoneTTL = 60 * time.Second

	// deleteTombstoneMaxEntries bounds the map on a node doing bulk deletes.
	deleteTombstoneMaxEntries = 8192

	// coordinatorCallTimeout bounds each retraction RPC. Coordination is
	// advisory, so a slow coordinator must cost the delete a bounded wait
	// rather than the caller's whole context.
	coordinatorCallTimeout = 5 * time.Second
)

// deleteTombstones records recently deleted paths so background publishers
// stop re-advertising them. Advisory and local: it suppresses this node's own
// writes to the coordinator, it is not a distributed tombstone.
type deleteTombstones struct {
	mu sync.Mutex
	at map[string]time.Time
}

func newDeleteTombstones() *deleteTombstones {
	return &deleteTombstones{at: make(map[string]time.Time)}
}

// mark, clear, and active tolerate a nil receiver: tests construct
// DefaultCacheManager literals directly, and a missing tombstone set degrades
// to the old behaviour rather than panicking on a delete.
func (d *deleteTombstones) mark(path string, now time.Time) {
	if d == nil || strings.TrimSpace(path) == "" {
		return
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	d.at[path] = now
	if len(d.at) > deleteTombstoneMaxEntries {
		cutoff := now.Add(-deleteTombstoneTTL)
		for key, when := range d.at {
			if when.Before(cutoff) {
				delete(d.at, key)
			}
		}
	}
}

// clear lifts the tombstone, which a fresh write of the same path must do:
// re-creating a deleted file is legitimate and its metadata has to publish.
func (d *deleteTombstones) clear(path string) {
	if d == nil || strings.TrimSpace(path) == "" {
		return
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	delete(d.at, path)
	// A chunk write revives its parent: chunks are never written on their own.
	if parent, ok := parentFilePathFromChunkPath(path); ok {
		delete(d.at, parent)
	}
}

// active reports whether path (or the parent file it is a chunk of) was
// deleted recently enough that publishing it would resurrect a ghost.
func (d *deleteTombstones) active(path string, now time.Time) bool {
	if d == nil || strings.TrimSpace(path) == "" {
		return false
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.freshLocked(path, now) {
		return true
	}
	// Chunk keys are published individually (persistToCloud publishes one row
	// per cloud key), so a tombstone on the parent has to cover them.
	if parent, ok := parentFilePathFromChunkPath(path); ok {
		return d.freshLocked(parent, now)
	}
	return false
}

func (d *deleteTombstones) freshLocked(path string, now time.Time) bool {
	when, ok := d.at[path]
	if !ok {
		return false
	}
	if now.Sub(when) >= deleteTombstoneTTL {
		delete(d.at, path)
		return false
	}
	return true
}

// deleteStats counts the best-effort halves of a delete. Silent partial
// failure is precisely the class of bug this whole change exists to fix, so
// every tier that can quietly keep a copy has to be observable.
type deleteStats struct {
	peerFailures        atomic.Int64
	cloudFailures       atomic.Int64
	coordinatorFailures atomic.Int64
	// resurrectionSuppressed counts publishes/persists a tombstone blocked.
	// Sustained growth means something re-advertises deleted files.
	resurrectionSuppressed atomic.Int64
}

// DeleteSnapshot is the exported view of delete-path counters.
type DeleteSnapshot struct {
	PeerFailuresTotal           int64
	CloudFailuresTotal          int64
	CoordinatorFailuresTotal    int64
	ResurrectionSuppressedTotal int64
}

// DeleteSnapshot returns delete fan-out counters.
func (cm *DefaultCacheManager) DeleteSnapshot() DeleteSnapshot {
	return DeleteSnapshot{
		PeerFailuresTotal:           cm.deleteStats.peerFailures.Load(),
		CloudFailuresTotal:          cm.deleteStats.cloudFailures.Load(),
		CoordinatorFailuresTotal:    cm.deleteStats.coordinatorFailures.Load(),
		ResurrectionSuppressedTotal: cm.deleteStats.resurrectionSuppressed.Load(),
	}
}

// tombstoned reports whether path is under a delete tombstone, counting the
// suppression. Publishers call this immediately before writing metadata.
func (cm *DefaultCacheManager) tombstoned(path string) bool {
	if cm.tombstones == nil {
		return false
	}
	if !cm.tombstones.active(path, time.Now()) {
		return false
	}
	cm.deleteStats.resurrectionSuppressed.Add(1)
	return true
}

// Delete removes filePath everywhere: local NVMe, peer replicas, the cloud
// object(s), and the coordinator's location rows.
//
// Only the local removal can fail the call. Peers, cloud, and coordinator are
// best-effort — logged and counted — so an unreachable node or a coordinator
// outage cannot turn rm into an error, per the advisory-coordination
// invariant. The cost is that a replica on a node that was down at delete time
// survives and can later be re-replicated; the counters make that visible.
func (cm *DefaultCacheManager) Delete(ctx context.Context, filePath string) error {
	filePath = strings.TrimSpace(filePath)
	if filePath == "" {
		return fmt.Errorf("delete: empty file path")
	}

	// Resolve through the merged view, not just the local table. The FUSE
	// layer resolves names through the coordinator view, so a path is visible
	// on every node while a local entry exists on only some of them. Keying
	// existence off the local table would make rm succeed on the holder and
	// return ENOENT everywhere else — while the file kept resolving.
	cm.mu.RLock()
	entry, local := cm.entries.get(filePath)
	if local && entry != nil {
		entryCopy := *entry
		entry = &entryCopy
	}
	cm.mu.RUnlock()

	if !local || entry == nil {
		remote, ok := cm.LookupEntry(ctx, filePath, false)
		if !ok {
			return fmt.Errorf("delete %s: %w", filePath, ErrNotFound)
		}
		entry = remote
	}

	// Before anything is removed, so that a publisher racing the removal is
	// already suppressed rather than merely losing the race.
	cm.tombstones.mark(filePath, time.Now())
	// A deleted path has nothing left to persist; an in-flight upload sees
	// its generation retired and stops certifying it.
	cm.durability.forget(filePath)

	// Captured while the metadata is still intact: the local footprint delete
	// drops the chunk entries the key list is derived from.
	cloudKeys := cm.deleteCloudKeys(ctx, filePath, entry)

	localErr := cm.deleteLocalFootprint(ctx, filePath)

	// Coordinator first, deliberately. The replica reconciler tops up any
	// under-replicated local object accessed in the last 10 minutes, and a
	// peer that still holds a replica has exactly such an entry — so between
	// our peer-delete and its own removal, its reconciler can see
	// holders < target and replicate the file straight back. Retracting the
	// rows first means a racing pass reads no holders and has nothing to top
	// up. This narrows the window; it does not close it.
	cm.retractLocationRows(ctx, filePath)
	cm.deleteFromPeers(ctx, filePath)
	cm.deleteCloudObjects(ctx, cloudKeys)

	// Again after the fan-out: a chunk fetch in flight when the first
	// invalidation ran would otherwise have re-populated the range cache, and
	// a later read would serve deleted bytes out of memory.
	cm.invalidateRangeCache(filePath)

	if localErr != nil {
		return localErr
	}
	cm.logger.Printf("Deleted %s (local + %d cloud keys, peers, coordinator)", filePath, len(cloudKeys))
	return nil
}

// DeleteLocal removes filePath from this node only — no peer fan-out, no cloud
// delete, no coordinator retraction beyond this node's own rows. It is what
// the peer gRPC handler serves, and the reason a cluster-wide delete stays one
// hop deep instead of becoming a broadcast storm.
func (cm *DefaultCacheManager) DeleteLocal(ctx context.Context, filePath string) error {
	filePath = strings.TrimSpace(filePath)
	if filePath == "" {
		return fmt.Errorf("delete: empty file path")
	}
	cm.tombstones.mark(filePath, time.Now())
	cm.durability.forget(filePath)

	err := cm.deleteLocalFootprint(ctx, filePath)

	// This node no longer holds the object, so its own rows are stale routing
	// that would cost every reader a failed probe. Other holders' rows stay:
	// the file may well live on elsewhere.
	if cm.config.Coordinator != nil && cm.config.LocalPeerID != "" {
		callCtx, cancel := context.WithTimeout(ctx, coordinatorCallTimeout)
		if rerr := cm.config.Coordinator.DeleteFileLocation(callCtx, filePath, cm.config.LocalPeerID, false); rerr != nil {
			cm.deleteStats.coordinatorFailures.Add(1)
			cm.logger.Printf("Coordinator retraction of local %s failed: %v", filePath, rerr)
		}
		cancel()
	}
	cm.invalidateRangeCache(filePath)
	return err
}

// deleteLocalFootprint removes the local bytes and every table and view record
// naming filePath. Missing bytes are not an error: an entry whose data was
// evicted, or a path known only through the coordinator view, still has
// metadata here that must go.
func (cm *DefaultCacheManager) deleteLocalFootprint(ctx context.Context, filePath string) error {
	cm.invalidateRangeCache(filePath)

	if _, err := cm.deleteLocalNVMeFootprint(ctx, filePath, true, true); err != nil {
		return fmt.Errorf("delete %s: %w", filePath, err)
	}
	// deleteLocalNVMeFootprint only collects TierNVMe targets, so an entry
	// parked at TierPeer or TierCloud by the parallel-write fallback would
	// survive it and keep the name alive locally.
	cm.dropEntries(filePath)
	// The metadata view is the ghost's actual life support: resolveChunkedEntry
	// rebuilds a live CacheEntry out of it and puts it back into the table, so
	// pruning the table alone leaves a resurrection path open inside the read
	// path itself.
	cm.pruneMetadataView(filePath)
	return nil
}

// pruneMetadataView drops filePath and its chunk keys from the cached
// coordinator view, so a delete is not undone by a view snapshot that is up to
// MetadataRefreshTTL stale.
func (cm *DefaultCacheManager) pruneMetadataView(filePath string) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	if len(cm.metadataView) == 0 {
		return
	}
	chunkPrefix := filePath + "_chunk_"
	// Copy-on-write: readers take the slice header under RLock and range over
	// it after unlocking, so filtering in place would mutate a live read.
	kept := make([]*CacheEntry, 0, len(cm.metadataView))
	for _, entry := range cm.metadataView {
		if entry == nil {
			continue
		}
		if entry.FilePath == filePath || strings.HasPrefix(entry.FilePath, chunkPrefix) {
			continue
		}
		kept = append(kept, entry)
	}
	cm.metadataView = kept
}

// deleteCloudKeys lists every cloud object holding filePath's data.
//
// The entry's own IsChunked/NumChunks is not enough. After a restart,
// rehydrateNVMeAccounting adopts local files with neither set, so a chunked
// file looks unchunked and cloudObjectKeys returns the bare path — a key that
// does not exist for a chunked file, leaving every chunk blob behind while
// reporting success. The coordinator row carries the real shape, so consult it
// whenever the local metadata claims the file is unchunked.
func (cm *DefaultCacheManager) deleteCloudKeys(ctx context.Context, filePath string, entry *CacheEntry) []string {
	seen := make(map[string]struct{})
	keys := make([]string, 0, 8)
	add := func(key string) {
		if strings.TrimSpace(key) == "" {
			return
		}
		if _, ok := seen[key]; ok {
			return
		}
		seen[key] = struct{}{}
		keys = append(keys, key)
	}

	for _, key := range cloudObjectKeys(filePath, entry) {
		add(key)
	}

	// Chunks this node actually holds, including ones a partial fetch left
	// outside the parent's NumChunks.
	cm.mu.RLock()
	localChunks := cm.entries.chunkKeys(filePath)
	cm.mu.RUnlock()
	for _, key := range localChunks {
		add(key)
	}

	if entry != nil && entry.IsChunked && entry.NumChunks > 0 {
		return keys
	}
	if cm.config.Coordinator == nil {
		return keys
	}

	callCtx, cancel := context.WithTimeout(ctx, coordinatorCallTimeout)
	locations, err := cm.config.Coordinator.GetFileLocation(callCtx, filePath)
	cancel()
	if err != nil {
		cm.logger.Printf("Cloud key discovery for %s fell back to local metadata: %v", filePath, err)
		return keys
	}
	for _, loc := range locations {
		if loc == nil || !loc.IsChunked {
			continue
		}
		chunkSize := loc.ChunkSize
		if chunkSize <= 0 {
			chunkSize = cm.config.ChunkSize
		}
		if chunkSize <= 0 || loc.FileSize <= 0 {
			continue
		}
		for i := int64(0); i < chunkCountFor(loc.FileSize, chunkSize); i++ {
			add(chunkPathFor(filePath, i))
		}
	}
	return keys
}

// deleteCloudObjects removes the durable copies. This is the step that makes
// rm unrecoverable — there is no versioning here — which is why the one-hop
// guarantee on peer fan-out is not negotiable.
func (cm *DefaultCacheManager) deleteCloudObjects(ctx context.Context, keys []string) {
	if cm.cloudStorage == nil || len(keys) == 0 {
		return
	}
	for _, key := range keys {
		select {
		case <-ctx.Done():
			return
		default:
		}
		// isCloudMiss catches the typed 404s (Azure BlobNotFound, S3
		// NoSuchKey) that the string-matching isDeleteNotFoundError misses;
		// without it every rm of a never-persisted file bumps the counter.
		if err := cm.cloudStorage.Delete(ctx, key); err != nil && !isCloudMiss(err) && !isDeleteNotFoundError(err) {
			cm.deleteStats.cloudFailures.Add(1)
			cm.logger.Printf("Cloud delete of %s failed: %v", key, err)
		}
	}
}

// deleteFromPeers asks every active peer to drop its own copy. One RPC per
// peer, not per chunk: the receiving handler removes the parent and all of its
// chunks locally, which is both cheaper and better coverage than enumerating
// chunk keys here (a peer can hold chunks this node has never heard of).
func (cm *DefaultCacheManager) deleteFromPeers(ctx context.Context, filePath string) {
	if cm.peerStorage == nil {
		return
	}
	if err := cm.peerStorage.Delete(ctx, filePath); err != nil && !isPeerMiss(err) && !isDeleteNotFoundError(err) {
		cm.deleteStats.peerFailures.Add(1)
		cm.logger.Printf("Peer delete fan-out for %s incomplete: %v", filePath, err)
	}
}

// retractLocationRows drops this file's coordinator metadata cluster-wide, so
// the deleted name stops resolving through the merged view on every node.
// etcd file rows, unlike peer rows, carry no TTL — nothing else ever removes
// them.
// A chunked file also has one location row per chunk — the cloud persist
// publishes a row per cloud key, and those keys are the chunk paths. Live etcd
// showed real "..._chunk_N" rows for every chunked file. Retracting them is
// the store's job (wholePath removes the parent key and the chunk rows under
// it in one prefix delete); doing it here would be one RPC per chunk, roughly
// 12,800 of them for a 100 GB file at the 8 MiB stride.
func (cm *DefaultCacheManager) retractLocationRows(ctx context.Context, filePath string) {
	if cm.config.Coordinator == nil {
		return
	}
	callCtx, cancel := context.WithTimeout(ctx, coordinatorCallTimeout)
	defer cancel()

	// wholePath, not per-peer: a node publishes a row per tier it holds the
	// object in (nvme at write, cloud once persisted), so retracting by
	// (path, peer) alone would leave the sibling row behind — and the point of
	// rm is that the file is gone everywhere, not that this node stopped
	// holding it.
	if err := cm.config.Coordinator.DeleteFileLocation(callCtx, filePath, cm.config.LocalPeerID, true); err != nil {
		cm.deleteStats.coordinatorFailures.Add(1)
		cm.logger.Printf("Coordinator retraction of %s failed: %v", filePath, err)
	}
}

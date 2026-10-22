package cache

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

// Persist driver and the durability receipt API (I1).

const (
	durabilityLoopInterval = 15 * time.Second
	durabilityLoopBatch    = 4
	// DurabilityMaxWait caps how long one receipt call may block.
	DurabilityMaxWait = 10 * time.Minute
	// DurabilityMaxPaths bounds a prefix receipt. A receipt is a per-path
	// verification, so an unbounded prefix would turn into the same class of
	// full-table work that OOM-killed the coordinators (metadata_lookup.go).
	DurabilityMaxPaths      = 10000
	durabilityPollInterval  = 250 * time.Millisecond
	durabilityProbeParallel = 8
)

// ErrDurabilityBadRequest marks a receipt request the caller must fix.
var ErrDurabilityBadRequest = errors.New("bad durability request")

// runTrackedPersist runs one upload attempt for generation gen of path and
// records its outcome.
func (cm *DefaultCacheManager) runTrackedPersist(path string, gen uint64, fn func() error) {
	cm.durability.markUploading(path, gen)
	err := fn()
	cm.durability.finish(path, gen, err)
	if err != nil && !errors.Is(err, errPersistAbandoned) && !errors.Is(err, errPersistCancelled) {
		cm.logger.Printf("Durability: %s not persisted (state=failed, will retry): %v", path, err)
	}
}

// startDurabilityLoop resumes journalled uploads and retries failed ones.
func (cm *DefaultCacheManager) startDurabilityLoop() {
	if cm.durability == nil {
		return
	}
	cm.goLoop(func() {
		ticker := time.NewTicker(durabilityLoopInterval)
		defer ticker.Stop()
		for {
			cm.drainDueUploads()
			select {
			case <-cm.shutdownCtx.Done():
				return
			case <-ticker.C:
			case <-cm.durability.wake:
			}
		}
	})
}

// initDurability opens the journal under the NVMe root, re-installs what the
// previous process left pending, and starts the retry loop. Must run after
// rehydrateNVMeAccounting.
func (cm *DefaultCacheManager) initDurability() {
	cm.durability = newDurabilityTracker(filepath.Join(cm.config.NVMePath, durabilityDirName), cm.logger)
	cm.resumeJournalledUploads()
	cm.startDurabilityLoop()
}

// resumeJournalledUploads loads the journal left by the previous process and
// restores what rehydration cannot infer from disk: a chunked file's shape.
// Rehydration adopts a PutFromFile whole file as a plain unchunked entry and a
// putChunked file as loose chunk entries with no parent, so without this a
// resumed file would be read, evicted, and published with the wrong shape.
func (cm *DefaultCacheManager) resumeJournalledUploads() {
	loaded := cm.durability.load()
	if len(loaded) == 0 {
		return
	}
	cm.mu.Lock()
	for _, rec := range loaded {
		if !rec.IsChunked {
			continue
		}
		entry, ok := cm.entries.get(rec.Path)
		switch {
		case ok && entry != nil:
			if entry.Size == rec.Size {
				entry.IsChunked = true
				entry.NumChunks = rec.NumChunks
				entry.ChunkSize = rec.ChunkSize
			}
		case len(cm.entries.chunkKeys(rec.Path)) > 0:
			// Per-chunk layout: the parent row carries no bytes of its own,
			// exactly as putChunked installs it.
			cm.entries.put(&CacheEntry{
				FilePath:     rec.Path,
				StoragePath:  rec.Path,
				Size:         rec.Size,
				LastAccessed: time.Unix(0, rec.CommittedAt),
				Tier:         TierNVMe,
				IsChunked:    true,
				NumChunks:    rec.NumChunks,
				ChunkSize:    rec.ChunkSize,
			})
		}
	}
	cm.mu.Unlock()
	cm.logger.Printf("Durability: resuming %d journalled upload(s) from the previous process", len(loaded))
	cm.durability.signal()
}

// drainDueUploads claims and runs due uploads, a bounded batch at a time.
func (cm *DefaultCacheManager) drainDueUploads() {
	for _, item := range cm.durability.due(time.Now(), durabilityLoopBatch) {
		it := item
		cm.goBackground(func() { cm.resumePersist(it) })
	}
}

// resumePersist re-runs the upload for a claimed record from whatever local
// copy exists.
func (cm *DefaultCacheManager) resumePersist(item dueItem) {
	rec, ok := cm.durability.get(item.path)
	if !ok || rec.gen != item.gen {
		cm.durability.release(item.path, item.gen)
		return
	}
	cm.runTrackedPersist(rec.Path, rec.gen, func() error {
		return cm.persistFromLocal(cm.shutdownCtx, rec)
	})
}

// persistFromLocal uploads rec's path from its NVMe copy, in whichever local
// shape it exists: one object, per-chunk objects, or one whole file served
// as chunks (PutFromFile).
func (cm *DefaultCacheManager) persistFromLocal(ctx context.Context, rec durabilityRecord) error {
	if cm.tombstoned(rec.Path) {
		return errPersistAbandoned
	}
	shape := rec.shape()
	if !shape.IsChunked {
		if cm.nvmeStorage != nil {
			if data, err := cm.nvmeStorage.Read(ctx, rec.Path); err == nil {
				if int64(len(data)) != shape.Size {
					return fmt.Errorf("local copy of %s is %d bytes, journal says %d", rec.Path, len(data), shape.Size)
				}
				return cm.persistToCloudGen(ctx, rec.Path, data, rec.Path, rec.gen)
			}
		}
		return cm.localCopyMissing(ctx, rec.Path, shape)
	}
	if cm.nvmeStorage != nil && cm.nvmeStorage.Exists(ctx, chunkPathFor(rec.Path, 0)) {
		return cm.persistChunkedFileToCloudGen(ctx, rec.Path, shape.NumChunks, rec.gen)
	}
	if localPath, ok := cm.LocalFilePath(ctx, rec.Path); ok {
		return cm.persistWholeFileAsChunksStride(ctx, rec.Path, localPath, shape.NumChunks, shape.ChunkSize, rec.gen)
	}
	return cm.localCopyMissing(ctx, rec.Path, shape)
}

// localCopyMissing settles a record whose local bytes are gone. If the cloud
// already holds the exact shape (an upload that finished just before a crash
// erased the journal row's removal), it is persisted; otherwise the bytes are
// lost and the record stays failed so an operator can see it.
func (cm *DefaultCacheManager) localCopyMissing(ctx context.Context, path string, shape persistShape) error {
	if cm.probeCloudShape(ctx, path, shape) {
		if shape.IsChunked {
			cm.publishCloudParentRow(ctx, path, shape)
		}
		return nil
	}
	return fmt.Errorf("local copy of %s is gone and the cloud does not hold it", path)
}

// probeCloudShape verifies by size that every object of shape exists in the
// cloud tier. Chunked files are probed chunk by chunk: a receipt must not
// certify a file with a missing middle chunk.
func (cm *DefaultCacheManager) probeCloudShape(ctx context.Context, path string, shape persistShape) bool {
	if cm.cloudStorage == nil {
		return false
	}
	if !shape.IsChunked {
		got, err := cm.cloudStorage.Size(ctx, path)
		return err == nil && got == shape.Size
	}
	chunkSize := shape.ChunkSize
	if chunkSize <= 0 {
		chunkSize = cm.config.ChunkSize
	}
	n := shape.NumChunks
	if n <= 0 {
		n = chunkCountFor(shape.Size, chunkSize)
	}
	if n <= 0 || chunkSize <= 0 {
		return false
	}
	var wg sync.WaitGroup
	var mu sync.Mutex
	ok := true
	sem := make(chan struct{}, durabilityProbeParallel)
	for i := int64(0); i < n; i++ {
		want := chunkSize
		if i == n-1 {
			want = shape.Size - i*chunkSize
		}
		if want <= 0 || want > chunkSize {
			return false
		}
		mu.Lock()
		stop := !ok
		mu.Unlock()
		if stop {
			break
		}
		wg.Add(1)
		sem <- struct{}{}
		go func(idx, want int64) {
			defer wg.Done()
			defer func() { <-sem }()
			got, err := cm.cloudStorage.Size(ctx, chunkPathFor(path, idx))
			if err != nil || got != want {
				mu.Lock()
				ok = false
				mu.Unlock()
			}
		}(i, want)
	}
	wg.Wait()
	return ok
}

// DurabilityRequest asks whether a set of artifacts survives node loss.
type DurabilityRequest struct {
	// Paths are exact file paths. Combined with Prefix if both are set.
	Paths []string `json:"paths,omitempty"`
	// Prefix selects every file at or under a directory. Must not be the
	// root: a whole-namespace receipt is a full-table scan.
	Prefix string `json:"prefix,omitempty"`
	// Wait blocks up to this long for every path to become persisted.
	// Zero answers immediately. Capped at DurabilityMaxWait.
	Wait time.Duration `json:"-"`
	// RetryFailed makes failed uploads due immediately instead of waiting
	// out their backoff.
	RetryFailed bool `json:"retry_failed,omitempty"`
}

// PathDurability is one path's entry in a receipt.
type PathDurability struct {
	Path      string          `json:"path"`
	State     DurabilityState `json:"state"`
	Size      int64           `json:"size"`
	IsChunked bool            `json:"is_chunked"`
	ChunkSize int64           `json:"chunk_size,omitempty"`
	// Objects is the number of cloud objects that hold the file: NumChunks
	// for a chunked file, else 1.
	Objects int64 `json:"objects"`
	// Generation identifies the committed write this state describes (its
	// commit time in unix nanoseconds, stable across restarts). Zero when
	// the state was established by a cloud probe rather than the writer's
	// own tracker. A consumer that re-reads the receipt later and sees a
	// different non-zero generation knows the path was rewritten.
	Generation int64  `json:"generation,omitempty"`
	Attempts   int    `json:"attempts,omitempty"`
	LastError  string `json:"last_error,omitempty"`
	// Source says how the state was established: "tracker" (this node
	// wrote it and observed every upload), "cloud-probe" (every object
	// verified by size against the cloud tier), or "local" (this node
	// holds bytes that are not in the cloud).
	Source string `json:"source"`
}

// DurabilityReceipt is the answer. Durable is true only when every listed
// path is persisted, including a chunked file's parent; a receipt is never
// issued while any chunk is pending.
type DurabilityReceipt struct {
	Durable     bool             `json:"durable"`
	PeerID      string           `json:"peer_id,omitempty"`
	GeneratedAt time.Time        `json:"generated_at"`
	Waited      string           `json:"waited"`
	Counts      map[string]int   `json:"counts"`
	Paths       []PathDurability `json:"paths"`
}

// DurabilityReceipt reports, and optionally waits for, cloud durability of
// the requested artifacts.
func (cm *DefaultCacheManager) DurabilityReceipt(ctx context.Context, req DurabilityRequest) (*DurabilityReceipt, error) {
	paths, err := cm.durabilityTargets(ctx, req)
	if err != nil {
		return nil, err
	}
	wait := req.Wait
	if wait < 0 {
		wait = 0
	}
	if wait > DurabilityMaxWait {
		wait = DurabilityMaxWait
	}
	if req.RetryFailed {
		for _, p := range paths {
			cm.durability.kick(p)
		}
	}
	start := time.Now()
	deadline := start.Add(wait)
	for {
		receipt := cm.buildReceipt(ctx, paths)
		if receipt.Durable || !time.Now().Before(deadline) {
			receipt.Waited = time.Since(start).Round(time.Millisecond).String()
			return receipt, nil
		}
		select {
		case <-ctx.Done():
			receipt.Waited = time.Since(start).Round(time.Millisecond).String()
			return receipt, nil
		case <-time.After(durabilityPollInterval):
		}
	}
}

// durabilityTargets normalizes and expands the request into a path list.
func (cm *DefaultCacheManager) durabilityTargets(ctx context.Context, req DurabilityRequest) ([]string, error) {
	seen := make(map[string]struct{})
	var out []string
	add := func(p string) {
		if _, isChunk := parentFilePathFromChunkPath(p); isChunk {
			return
		}
		if _, ok := seen[p]; ok {
			return
		}
		seen[p] = struct{}{}
		out = append(out, p)
	}
	for _, raw := range req.Paths {
		if strings.TrimSpace(raw) == "" {
			continue
		}
		p := filepath.Clean("/" + strings.TrimSpace(raw))
		if _, isChunk := parentFilePathFromChunkPath(p); isChunk {
			return nil, fmt.Errorf("%w: %s names an internal chunk object, not a file", ErrDurabilityBadRequest, raw)
		}
		add(p)
	}
	if strings.TrimSpace(req.Prefix) != "" {
		prefix := filepath.Clean("/" + strings.TrimSpace(req.Prefix))
		if prefix == "/" {
			return nil, fmt.Errorf("%w: prefix must not be the root", ErrDurabilityBadRequest)
		}
		for _, rec := range cm.durability.list(prefix) {
			add(rec.Path)
		}
		cm.mu.RLock()
		cm.entries.each(func(p string, e *CacheEntry) bool {
			if e != nil && pathUnderPrefix(p, prefix) {
				add(p)
			}
			return true
		})
		cm.mu.RUnlock()
		if cm.config.Coordinator != nil {
			// A bounded subtree range, never the whole table.
			callCtx, cancel := context.WithTimeout(ctx, coordinatorCallTimeout)
			locs, err := cm.config.Coordinator.ListFileLocations(callCtx, prefix+"/")
			cancel()
			if err == nil {
				for _, l := range locs {
					if l != nil {
						add(l.FilePath)
					}
				}
			}
		}
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("%w: no paths matched", ErrDurabilityBadRequest)
	}
	if len(out) > DurabilityMaxPaths {
		return nil, fmt.Errorf("%w: %d paths exceed the limit of %d; narrow the prefix", ErrDurabilityBadRequest, len(out), DurabilityMaxPaths)
	}
	sort.Strings(out)
	return out, nil
}

func (cm *DefaultCacheManager) buildReceipt(ctx context.Context, paths []string) *DurabilityReceipt {
	receipt := &DurabilityReceipt{
		Durable:     true,
		PeerID:      cm.config.LocalPeerID,
		GeneratedAt: time.Now().UTC(),
		Counts:      map[string]int{},
		Paths:       make([]PathDurability, 0, len(paths)),
	}
	for _, p := range paths {
		pd := cm.pathDurability(ctx, p)
		receipt.Counts[string(pd.State)]++
		if pd.State != DurabilityPersisted {
			receipt.Durable = false
		}
		receipt.Paths = append(receipt.Paths, pd)
	}
	return receipt
}

// pathDurability answers for one path. The writer's tracker is
// authoritative when it has a record; otherwise the answer comes from a
// cloud probe against the shape the coordinator's cloud row (or the local
// entry) describes, so a node that did not write the file can still verify
// it.
func (cm *DefaultCacheManager) pathDurability(ctx context.Context, path string) PathDurability {
	if rec, ok := cm.durability.get(path); ok {
		return PathDurability{
			Path:       path,
			State:      rec.State,
			Size:       rec.Size,
			IsChunked:  rec.IsChunked,
			ChunkSize:  rec.ChunkSize,
			Objects:    objectCount(rec.shape()),
			Generation: rec.CommittedAt,
			Attempts:   rec.Attempts,
			LastError:  rec.LastError,
			Source:     "tracker",
		}
	}

	var local *CacheEntry
	cm.mu.RLock()
	if e, ok := cm.entries.get(path); ok && e != nil {
		c := *e
		local = &c
	}
	cm.mu.RUnlock()

	// Shapes to verify, most authoritative first: a cloud-tier row is the
	// writer's claim that the whole file was uploaded.
	var shapes []persistShape
	if cm.config.Coordinator != nil {
		callCtx, cancel := context.WithTimeout(ctx, coordinatorCallTimeout)
		locs, err := cm.config.Coordinator.GetFileLocation(callCtx, path)
		cancel()
		if err == nil {
			for _, l := range locs {
				if l == nil || l.StorageTier != "cloud" || l.FileSize <= 0 {
					continue
				}
				s := persistShape{Size: l.FileSize, IsChunked: l.IsChunked, ChunkSize: l.ChunkSize}
				if s.IsChunked {
					if s.ChunkSize <= 0 {
						s.ChunkSize = cm.config.ChunkSize
					}
					s.NumChunks = chunkCountFor(s.Size, s.ChunkSize)
				}
				shapes = append(shapes, s)
			}
		}
	}
	if local != nil && local.Size > 0 && local.Tier != TierCloud {
		s := persistShape{Size: local.Size, IsChunked: local.IsChunked, NumChunks: local.NumChunks}
		if s.IsChunked {
			s.ChunkSize = cm.effectiveChunkSize(local)
		}
		shapes = append(shapes, s)
	}
	for _, s := range shapes {
		if local != nil && local.Tier == TierNVMe && local.Size != s.Size {
			// This node holds a different version than the cloud row
			// describes; the local bytes are not what was persisted.
			continue
		}
		if cm.probeCloudShape(ctx, path, s) {
			return PathDurability{
				Path: path, State: DurabilityPersisted, Size: s.Size, IsChunked: s.IsChunked,
				ChunkSize: s.ChunkSize, Objects: objectCount(s), Source: "cloud-probe",
			}
		}
	}
	if local != nil && local.Tier == TierNVMe {
		return PathDurability{
			Path: path, State: DurabilityLocal, Size: local.Size, IsChunked: local.IsChunked,
			ChunkSize: local.ChunkSize, Objects: objectCount(persistShape{IsChunked: local.IsChunked, NumChunks: local.NumChunks}),
			Source: "local",
		}
	}
	return PathDurability{Path: path, State: DurabilityUnknown, Source: "none"}
}

func objectCount(s persistShape) int64 {
	if s.IsChunked && s.NumChunks > 0 {
		return s.NumChunks
	}
	return 1
}

// DurabilitySnapshot reports how many tracked paths are in each state, for
// /api/cache/stats.
func (cm *DefaultCacheManager) DurabilitySnapshot() map[string]int {
	out := map[string]int{}
	for state, n := range cm.durability.counts() {
		out[string(state)] = n
	}
	return out
}

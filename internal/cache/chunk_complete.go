package cache

import (
	"context"
	"fmt"
	"io"
	"os"
	"sync"
	"time"
)

// Chunk completion: after a chunked cross-node read finishes (detected via
// range-cache idle expiry), fill any chunks the streaming tee didn't land and
// assemble the whole parent file on NVMe. A whole local file makes
// LocalFilePath resolve, which (1) turns warm FUSE reads into gofuse kernel
// passthrough and (2) lets peer serving use whole-file sendfile ranges.
// Without this, every unlanded chunk is a peer round-trip on every warm read
// (observed: ~4.5GB of warm-read peer traffic for a 5GB file at 96% landed).

const (
	// chunkCompletionCooldown prevents rescheduling a parent that recently
	// failed (e.g. NVMe pressure) on every sweep tick.
	chunkCompletionCooldown = 5 * time.Minute
	// chunkCompletionFetchWorkers bounds concurrent hole-fill fetches so a
	// completion pass never competes with foreground reads for peer capacity.
	chunkCompletionFetchWorkers = 4
)

type chunkCompletionState struct {
	mu          sync.Mutex
	inFlight    map[string]struct{}
	lastAttempt map[string]time.Time
}

// maybeScheduleChunkCompletion queues a background completion pass for a
// chunked file that is not yet whole on local NVMe. Called from the range
// cache idle sweep — i.e. after a read session ends, off the hot path.
func (cm *DefaultCacheManager) maybeScheduleChunkCompletion(filePath string) {
	if cm.config.ChunkCompletionDisabled {
		return
	}
	cm.mu.RLock()
	entry := cm.entries[filePath]
	cm.mu.RUnlock()
	if entry == nil || !entry.IsChunked || entry.NumChunks <= 0 || entry.Size <= 0 {
		return
	}
	if _, whole := cm.LocalFilePath(context.Background(), filePath); whole {
		return
	}

	st := &cm.chunkCompletion
	st.mu.Lock()
	if st.inFlight == nil {
		st.inFlight = make(map[string]struct{})
		st.lastAttempt = make(map[string]time.Time)
	}
	if _, running := st.inFlight[filePath]; running {
		st.mu.Unlock()
		return
	}
	if last, ok := st.lastAttempt[filePath]; ok && time.Since(last) < chunkCompletionCooldown {
		st.mu.Unlock()
		return
	}
	st.inFlight[filePath] = struct{}{}
	st.lastAttempt[filePath] = time.Now()
	// Opportunistic GC (same pattern as chunkAdvertiser): entries older than
	// the cooldown are inert, and without pruning this map grows by one entry
	// per distinct chunked file for the life of the node.
	if len(st.lastAttempt) > 4096 {
		cutoff := time.Now().Add(-2 * chunkCompletionCooldown)
		for p, at := range st.lastAttempt {
			if at.Before(cutoff) {
				delete(st.lastAttempt, p)
			}
		}
	}
	st.mu.Unlock()

	numChunks, size := entry.NumChunks, entry.Size
	cm.goBackground(func() {
		defer func() {
			st.mu.Lock()
			delete(st.inFlight, filePath)
			st.mu.Unlock()
		}()
		if err := cm.runChunkCompletion(cm.shutdownCtx, filePath, numChunks, size); err != nil {
			cm.logger.Printf("Chunk completion for %s: %v", filePath, err)
		}
	})
}

// tryStartCompletion claims the in-flight slot for filePath, sharing the same
// guard maybeScheduleChunkCompletion uses so a warmup pass and a scheduled
// completion pass can never assemble the same parent concurrently. Callers
// that get true must call endCompletion. Unlike the scheduler it ignores the
// cooldown: explicit warmup demand should retry a recently-failed parent.
func (cm *DefaultCacheManager) tryStartCompletion(filePath string) bool {
	st := &cm.chunkCompletion
	st.mu.Lock()
	defer st.mu.Unlock()
	if st.inFlight == nil {
		st.inFlight = make(map[string]struct{})
		st.lastAttempt = make(map[string]time.Time)
	}
	if _, running := st.inFlight[filePath]; running {
		return false
	}
	st.inFlight[filePath] = struct{}{}
	st.lastAttempt[filePath] = time.Now()
	return true
}

func (cm *DefaultCacheManager) endCompletion(filePath string) {
	st := &cm.chunkCompletion
	st.mu.Lock()
	delete(st.inFlight, filePath)
	st.mu.Unlock()
}

// runChunkCompletion fetches missing chunks, assembles the whole parent on
// NVMe, swaps accounting from per-chunk files to the whole file, and
// republishes the location. Fails soft everywhere: fetched chunks stay on
// NVMe for the next attempt.
func (cm *DefaultCacheManager) runChunkCompletion(ctx context.Context, filePath string, numChunks, size int64) error {
	return cm.runChunkCompletionOpts(ctx, filePath, numChunks, size, nil, chunkCompletionFetchWorkers)
}

// runChunkCompletionOpts is runChunkCompletion with an explicit tier order
// (nil = adaptive peer-first) and fetch-worker count, used by warmup
// strategies (cloud-only source, max bandwidth).
func (cm *DefaultCacheManager) runChunkCompletionOpts(ctx context.Context, filePath string, numChunks, size int64, order []CacheTier, workers int) error {
	ns, ok := cm.nvmeStorage.(*NVMeStorage)
	if !ok {
		return nil
	}

	// Assembly transiently needs chunks + whole file: require headroom for
	// the whole file on top of current usage, never evict for a background
	// optimization.
	if used, capacity := cm.Stats(); capacity > 0 && used+size > capacity {
		cm.metrics.ChunkCompletionSkipped.Add(1)
		return fmt.Errorf("skipped: needs %d bytes headroom (used %d / cap %d)", size, used, capacity)
	}

	var missing []int64
	for i := int64(0); i < numChunks; i++ {
		if !cm.nvmeStorage.Exists(ctx, chunkPathFor(filePath, i)) {
			missing = append(missing, i)
		}
	}

	if len(missing) > 0 {
		if err := cm.fetchMissingChunks(ctx, filePath, missing, order, workers); err != nil {
			return err
		}
	}

	// Assemble: stream every chunk file into a staged temp, rename when the
	// byte count matches exactly.
	w, err := ns.BeginStream(filePath)
	if err != nil {
		return err
	}
	for i := int64(0); i < numChunks; i++ {
		localPath, ok := cm.LocalFilePath(ctx, chunkPathFor(filePath, i))
		if !ok {
			w.Abort()
			return fmt.Errorf("chunk %d vanished during assembly", i)
		}
		f, err := os.Open(localPath)
		if err != nil {
			w.Abort()
			return err
		}
		_, cerr := io.Copy(w, f)
		f.Close()
		if cerr != nil {
			w.Abort()
			return cerr
		}
	}
	if w.BytesWritten() != size {
		w.Abort()
		return fmt.Errorf("assembled %d bytes, want %d", w.BytesWritten(), size)
	}
	if err := w.Commit(); err != nil {
		return err
	}

	// Whole file is live: account it, then retire the chunk files.
	now := time.Now()
	cm.mu.Lock()
	if parent, ok := cm.entries[filePath]; ok && parent != nil {
		parent.Tier = TierNVMe
		parent.LastAccessed = now
	} else {
		cm.entries[filePath] = &CacheEntry{
			FilePath: filePath, StoragePath: filePath, Size: size,
			LastAccessed: now, Tier: TierNVMe, IsChunked: true, NumChunks: numChunks,
		}
	}
	cm.nvmeUsed += size
	cm.mu.Unlock()

	for i := int64(0); i < numChunks; i++ {
		cp := chunkPathFor(filePath, i)
		chunkBytes, serr := cm.nvmeStorage.Size(ctx, cp)
		if derr := cm.nvmeStorage.Delete(ctx, cp); derr != nil {
			continue
		}
		// Checksum sidecars ride along with chunk files; without this every
		// completed file leaked numChunks orphan .sha256 files on NVMe.
		_ = cm.nvmeStorage.Delete(ctx, cp+".sha256")
		cm.mu.Lock()
		delete(cm.entries, cp)
		if serr == nil {
			cm.nvmeUsed -= chunkBytes
			if cm.nvmeUsed < 0 {
				cm.nvmeUsed = 0
			}
		}
		cm.mu.Unlock()
	}

	cm.metrics.ChunkCompletionAssembled.Add(1)
	cm.goBackground(func() {
		cm.publishFileLocation(cm.shutdownCtx, &CacheEntry{
			FilePath: filePath, StoragePath: filePath, Size: size,
			IsChunked: true, NumChunks: numChunks,
		}, TierNVMe)
	})
	cm.logger.Printf("Chunk completion assembled %s (%d chunks, %d fetched, %d bytes)",
		filePath, numChunks, len(missing), size)
	return nil
}

// fetchMissingChunks pulls the given chunk indices from remote tiers (order
// nil = adaptive peer-first, busy-aware, cloud fallback) and lands them on
// NVMe.
func (cm *DefaultCacheManager) fetchMissingChunks(ctx context.Context, filePath string, missing []int64, order []CacheTier, workers int) error {
	if workers <= 0 {
		workers = chunkCompletionFetchWorkers
	}
	if workers > len(missing) {
		workers = len(missing)
	}
	// Buffered and pre-filled before workers start: no feeder goroutine, no
	// shutdown-time block on an unbuffered send (the other pools select on
	// ctx.Done in their feed loop; prefilling makes that unnecessary here).
	idxCh := make(chan int64, len(missing))
	for _, idx := range missing {
		idxCh <- idx
	}
	close(idxCh)
	errCh := make(chan error, len(missing))
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for idx := range idxCh {
				if ctx.Err() != nil {
					errCh <- ctx.Err()
					return
				}
				errCh <- cm.fetchAndLandChunkOrdered(ctx, chunkPathFor(filePath, idx), order)
			}
		}()
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			return err
		}
	}
	cm.metrics.ChunkCompletionFetched.Add(int64(len(missing)))
	return nil
}

func (cm *DefaultCacheManager) fetchAndLandChunk(ctx context.Context, chunkPath string) error {
	return cm.fetchAndLandChunkOrdered(ctx, chunkPath, nil)
}

// fetchAndLandChunkOrdered fetches any path (chunk or whole file) from the
// given tier order (nil = adaptive) and lands it on NVMe.
func (cm *DefaultCacheManager) fetchAndLandChunkOrdered(ctx context.Context, chunkPath string, order []CacheTier) error {
	if order == nil {
		order = cm.remoteReadOrder(chunkPath)
	}
	var lastErr error
	for _, tier := range order {
		entry, _, err := cm.getFromRemoteTierWithBusyRetry(ctx, chunkPath, tier)
		if err != nil {
			lastErr = err
			continue
		}
		landed := &CacheEntry{
			FilePath: chunkPath, StoragePath: chunkPath,
			Size: int64(len(entry.Data)), LastAccessed: time.Now(),
			Tier: TierNVMe, Data: entry.Data,
		}
		if err := cm.putToNVMeWithEviction(ctx, landed); err != nil {
			return err
		}
		meta := *landed
		meta.Data = nil
		cm.mu.Lock()
		cm.entries[chunkPath] = &meta
		cm.mu.Unlock()
		cm.metrics.RecordWrite(landed.Size)
		return nil
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("no remote tier served %s", chunkPath)
	}
	return lastErr
}

func chunkPathFor(filePath string, idx int64) string {
	return fmt.Sprintf("%s_chunk_%d", filePath, idx)
}

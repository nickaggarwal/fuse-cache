package cache

import (
	"context"
	"io"
	"time"
)

// PromotionSink lets a transport stream remote bytes straight to local NVMe
// while the response is still arriving (tee), instead of materializing the
// object in memory and re-writing it to disk in a background promotion pass.
// BeginPromotion returns ok=false when promotion should be skipped (NVMe
// under pressure, object too large, already local, or the promote gate is
// full) — the caller then just reads normally and the existing background
// promotion path applies.
type PromotionSink interface {
	BeginPromotion(path string, size int64) (StreamPromotion, bool)
}

// StreamPromotion stages one streaming promotion. Exactly one of Commit or
// Abort must be called. Write is called with the bytes in arrival order;
// Commit is only valid after exactly the declared size has been written.
type StreamPromotion interface {
	io.Writer
	Commit()
	Abort()
}

// BeginPromotion implements PromotionSink on the cache manager. It applies
// the same admission rules as maybeAdvertiseFetchedChunk: a bounded gate so
// promotions never compete with foreground reads, and a pressure skip above
// 80% NVMe usage so a streaming write can't trigger a foreground eviction
// scan. Unlike putToNVMeWithEviction it never evicts — under pressure it
// declines and lets existing holders keep serving.
func (cm *DefaultCacheManager) BeginPromotion(path string, size int64) (StreamPromotion, bool) {
	if size <= 0 || path == "" {
		return nil, false
	}
	ns, ok := cm.nvmeStorage.(*NVMeStorage)
	if !ok {
		return nil, false
	}

	if cm.chunkPromoteGate != nil {
		select {
		case cm.chunkPromoteGate <- struct{}{}:
		default:
			return nil, false
		}
	}
	release := func() {
		if cm.chunkPromoteGate != nil {
			<-cm.chunkPromoteGate
		}
	}

	cm.mu.RLock()
	used, capacity := cm.nvmeUsed, cm.config.MaxNVMeSize
	existing := cm.entries[path]
	cm.mu.RUnlock()

	// Pressure skip (same threshold as maybeAdvertiseFetchedChunk) and a
	// no-evict capacity check: streaming promotion is opportunistic.
	if capacity > 0 && (used > capacity*8/10 || used+size > capacity) {
		release()
		return nil, false
	}
	// Already local with the same size: nothing to do.
	if existing != nil && existing.Tier == TierNVMe && existing.Size == size &&
		cm.nvmeStorage.Exists(context.Background(), path) {
		release()
		return nil, false
	}

	w, err := ns.BeginStream(path)
	if err != nil {
		release()
		return nil, false
	}
	return &streamPromotion{cm: cm, path: path, size: size, w: w, release: release}, true
}

type streamPromotion struct {
	cm      *DefaultCacheManager
	path    string
	size    int64
	w       *NVMeStreamWriter
	release func()
	done    bool
}

func (sp *streamPromotion) Write(p []byte) (int, error) {
	return sp.w.Write(p)
}

func (sp *streamPromotion) Commit() {
	if sp.done {
		return
	}
	sp.done = true
	defer sp.release()

	// A partial or over-long body must never land at the final path.
	if sp.w.BytesWritten() != sp.size {
		sp.w.Abort()
		return
	}
	if err := sp.w.Commit(); err != nil {
		sp.cm.logger.Printf("Streaming promotion commit failed for %s: %v", sp.path, err)
		return
	}

	meta := &CacheEntry{
		FilePath:     sp.path,
		StoragePath:  sp.path,
		Size:         sp.size,
		LastAccessed: time.Now(),
		Tier:         TierNVMe,
	}
	sp.cm.mu.Lock()
	if existing, ok := sp.cm.entries[sp.path]; ok && existing != nil && existing.Tier == TierNVMe {
		// Replaced an object we were already accounting for.
		sp.cm.nvmeUsed -= existing.Size
		if sp.cm.nvmeUsed < 0 {
			sp.cm.nvmeUsed = 0
		}
	}
	sp.cm.nvmeUsed += sp.size
	sp.cm.entries[sp.path] = meta
	sp.cm.mu.Unlock()
}

func (sp *streamPromotion) Abort() {
	if sp.done {
		return
	}
	sp.done = true
	sp.w.Abort()
	sp.release()
}

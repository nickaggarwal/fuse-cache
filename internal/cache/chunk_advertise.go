package cache

import (
	"context"
	"sync"
	"time"

	"fuse-client/internal/coordinator"
)

// Fast holder advertisement — Phase 3 of
// docs/peer-coordination-thundering-herd.md.
//
// A chunk fetched from a remote tier used to live only in this node's
// in-memory range cache: no NVMe file, no coordinator record, so no other
// node could pull it from us and every reader went back to the same origin.
// promoteChunkAndAdvertise persists the fetched chunk to local NVMe (making
// it servable via GetLocal / LocalChunkFile) and publishes this node as a
// holder of the parent file the moment the *first* chunk lands — not after
// the whole object — so the holder set grows mid-transfer and later
// requesters pull from the growing swarm (BitTorrent / Dragonfly model).
const (
	// chunkAdvertiseCoalesceWindow bounds coordinator write rate: one
	// advertisement per parent file per window, no matter how many chunks
	// land. Keeps lease/advert churn from turning the coordinator into the
	// new hotspot (plan risk item).
	chunkAdvertiseCoalesceWindow = 5 * time.Second

	// chunkPromoteMaxConcurrent bounds how many remotely-fetched chunks are
	// promoted to NVMe at once. Promotion writes the chunk to disk and takes
	// cm.mu (plus possible eviction), so an unbounded burst — 64 goroutines
	// for a 1 GiB read — contends with the foreground read path and tanks
	// throughput. With a small non-blocking gate, a read's chunk burst
	// promotes only a few chunks and skips the rest (a partial holder is fine:
	// readers try all peers), while an idle node still fills up over time.
	chunkPromoteMaxConcurrent = 2
)

// chunkAdvertiser dedupes per-parent holder advertisements.
type chunkAdvertiser struct {
	mu       sync.Mutex
	lastPub  map[string]time.Time
	lastSeen map[string]time.Time // for GC
}

func newChunkAdvertiser() *chunkAdvertiser {
	return &chunkAdvertiser{
		lastPub:  make(map[string]time.Time),
		lastSeen: make(map[string]time.Time),
	}
}

// shouldPublish reports whether parent may be advertised now, and records the
// publication when yes. Coalesces bursts of chunk arrivals for one file.
func (a *chunkAdvertiser) shouldPublish(parent string, now time.Time) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.lastSeen[parent] = now
	if last, ok := a.lastPub[parent]; ok && now.Sub(last) < chunkAdvertiseCoalesceWindow {
		return false
	}
	a.lastPub[parent] = now

	// Opportunistic GC keeps the maps bounded on long-running nodes.
	if len(a.lastSeen) > 4096 {
		cutoff := now.Add(-10 * chunkAdvertiseCoalesceWindow)
		for k, seen := range a.lastSeen {
			if seen.Before(cutoff) {
				delete(a.lastSeen, k)
				delete(a.lastPub, k)
			}
		}
	}
	return true
}

// promoteChunkAndAdvertise persists a remotely-fetched chunk to local NVMe and
// advertises this node as a holder of the parent. Runs in the background; all
// failures are best-effort (the read already succeeded).
func (cm *DefaultCacheManager) promoteChunkAndAdvertise(ctx context.Context, filePath, chunkPath string, data []byte) {
	if len(data) == 0 {
		return
	}

	// A streaming (tee) promotion may have landed these bytes during the
	// transfer itself; skip the redundant write but still advertise.
	if !cm.alreadyLocalNVMe(ctx, chunkPath, int64(len(data))) {
		chunkEntry := &CacheEntry{
			FilePath:     chunkPath,
			StoragePath:  chunkPath,
			Size:         int64(len(data)),
			LastAccessed: time.Now(),
			Tier:         TierNVMe,
			Data:         data,
		}
		if err := cm.putToNVMeWithEviction(ctx, chunkEntry); err != nil {
			cm.logger.Printf("Chunk promote to NVMe failed for %s: %v", chunkPath, err)
			return
		}
		chunkMeta := *chunkEntry
		chunkMeta.Data = nil
		cm.mu.Lock()
		cm.entries[chunkPath] = &chunkMeta
		cm.mu.Unlock()
	}

	cm.advertiseChunkParent(ctx, filePath)
}

// advertiseChunkParent publishes this node as a holder of filePath (coalesced
// via chunkAds). Split from promotion so a chunk landed by a streaming (tee)
// promotion still grows the swarm without a second payload write.
func (cm *DefaultCacheManager) advertiseChunkParent(ctx context.Context, filePath string) {
	if cm.config.Coordinator == nil || cm.config.LocalPeerID == "" {
		return
	}
	if !cm.chunkAds.shouldPublish(filePath, time.Now()) {
		return
	}

	// Advertise at parent-file granularity: the peer read path resolves
	// chunk hints via the parent (peerIDsForPath), and a partial holder just
	// costs a requester one failed probe (readers already try all peers).
	cm.mu.RLock()
	parentEntry := cm.entries[filePath]
	cm.mu.RUnlock()
	size := int64(0)
	if parentEntry != nil {
		size = parentEntry.Size
	}
	location := &coordinator.FileLocation{
		FilePath:     filePath,
		PeerID:       cm.config.LocalPeerID,
		StorageTier:  "nvme",
		StoragePath:  filePath,
		FileSize:     size,
		LastAccessed: time.Now(),
		IsChunked:    true,
	}
	callCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := cm.config.Coordinator.UpdateFileLocation(callCtx, location); err != nil {
		cm.logger.Printf("Fast chunk advertise failed for %s: %v", filePath, err)
		return
	}
	cm.herdStats.advertisePublished.Add(1)
}

// maybeAdvertiseFetchedChunk schedules background promote+advertise for a
// chunk that arrived from a remote tier. No-op when disabled or the chunk is
// already local.
func (cm *DefaultCacheManager) maybeAdvertiseFetchedChunk(filePath, chunkPath string, tier CacheTier, data []byte) {
	if !cm.config.FastChunkAdvertise || tier == TierNVMe || len(data) == 0 {
		return
	}
	// Non-blocking admission: under a read's parallel chunk burst the gate
	// fills immediately, so most chunks are skipped here — before the copy —
	// keeping promotion from competing with the foreground read. An idle node
	// promotes freely and fills up as a holder over successive reads.
	if cm.chunkPromoteGate != nil {
		select {
		case cm.chunkPromoteGate <- struct{}{}:
		default:
			return
		}
		// Pressure skip: promotion writes to NVMe under cm.mu and, above the
		// 90% eviction watermark, triggers a full-entry eviction scan that
		// blocks the read path's RLocks. Never let a foreground read pay that —
		// skip promotion once NVMe is busy and let existing holders serve.
		if used, capacity := cm.Stats(); capacity > 0 && used > capacity*nvmePressureNum/nvmePressureDen {
			<-cm.chunkPromoteGate
			return
		}
	}
	// A streaming (tee) promotion may have already landed this chunk during
	// the transfer; advertise without copying or rewriting the payload.
	if cm.alreadyLocalNVMe(cm.shutdownCtx, chunkPath, int64(len(data))) {
		cm.goBackground(func() {
			if cm.chunkPromoteGate != nil {
				defer func() { <-cm.chunkPromoteGate }()
			}
			cm.advertiseChunkParent(cm.shutdownCtx, filePath)
		})
		return
	}
	// Copy: the caller's buffer is shared with the range cache.
	dataCopy := make([]byte, len(data))
	copy(dataCopy, data)
	cm.goBackground(func() {
		if cm.chunkPromoteGate != nil {
			defer func() { <-cm.chunkPromoteGate }()
		}
		cm.promoteChunkAndAdvertise(cm.shutdownCtx, filePath, chunkPath, dataCopy)
	})
}

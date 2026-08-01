package cache

import (
	"runtime/metrics"
	"time"
)

const defaultRangeCacheIdleExpiry = 30 * time.Second

// rangeCacheSweepInterval is how often the background sweeper checks for
// idle-expired per-file caches. Kept well under the idle expiry so a dead
// file's cache is reclaimed promptly after its last read.
const rangeCacheSweepInterval = 10 * time.Second

// deriveRangeCacheGlobalMaxBytes picks the all-files range cache budget:
// enough for a few concurrently-hot files, but never a RAM-threatening share
// of the node. min(4 x per-file budget, 25% of total memory), floored at one
// per-file budget so a single hot file always fits.
func deriveRangeCacheGlobalMaxBytes(perFileBudget int64) int64 {
	if perFileBudget <= 0 {
		perFileBudget = defaultRangeChunkCacheMaxBytes
	}
	budget := 4 * perFileBudget
	if total := systemTotalMemoryBytes(); total > 0 && budget > total/4 {
		budget = total / 4
	}
	if budget < perFileBudget {
		budget = perFileBudget
	}
	return budget
}

// systemTotalMemoryBytes returns the cgroup/system memory limit visible to Go,
// or 0 when unavailable. GOMEMLIMIT reflects container limits when set by the
// runtime environment; otherwise fall back to 0 (caller keeps the static cap).
func systemTotalMemoryBytes() int64 {
	samples := []metrics.Sample{{Name: "/gc/gomemlimit:bytes"}}
	metrics.Read(samples)
	if samples[0].Value.Kind() == metrics.KindUint64 {
		v := samples[0].Value.Uint64()
		// Go reports MaxInt64 when no limit is set.
		if v > 0 && v < (1<<62) {
			return int64(v)
		}
	}
	return 0
}

// touchFileCacheLocked stamps access time for global LRU/expiry decisions.
// Callers must hold rangeMu for writing.
func (fc *chunkFileCache) touchLocked(now time.Time) {
	fc.lastAccess = now
}

// enforceGlobalRangeBudgetLocked evicts whole per-file caches, least recently
// accessed first, until the global byte budget is met. The file that was just
// touched (keepPath) is evicted last — evicting the active reader's cache
// would turn every read into a re-fetch. Callers must hold rangeMu for
// writing.
func (cm *DefaultCacheManager) enforceGlobalRangeBudgetLocked(keepPath string) {
	budget := cm.config.RangeCacheGlobalMaxBytes
	if budget <= 0 {
		return
	}
	total := int64(0)
	for _, fc := range cm.rangeChunks {
		if fc != nil {
			total += fc.bytes
		}
	}
	for total > budget && len(cm.rangeChunks) > 0 {
		oldestPath := ""
		var oldest time.Time
		for path, fc := range cm.rangeChunks {
			if fc == nil {
				continue
			}
			if path == keepPath && len(cm.rangeChunks) > 1 {
				continue
			}
			if oldestPath == "" || fc.lastAccess.Before(oldest) {
				oldestPath = path
				oldest = fc.lastAccess
			}
		}
		if oldestPath == "" {
			return
		}
		if fc := cm.rangeChunks[oldestPath]; fc != nil {
			total -= fc.bytes
		}
		cm.dropFileCacheLocked(oldestPath)
		cm.metrics.RangeCacheFileEvictions.Add(1)
		if oldestPath == keepPath {
			// Only the active file remains and it alone exceeds the global
			// budget; per-file limits already bound it. Stop.
			return
		}
	}
}

// startRangeCacheSweeper launches the idle-expiry loop: any per-file cache
// untouched for RangeCacheIdleExpiry is dropped wholesale. This is what
// returns heap after a large sequential read finishes.
func (cm *DefaultCacheManager) startRangeCacheSweeper(interval time.Duration) {
	expiry := cm.config.RangeCacheIdleExpiry
	if expiry <= 0 {
		return
	}
	if interval <= 0 {
		interval = rangeCacheSweepInterval
	}
	cm.goBackground(func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-cm.shutdownCtx.Done():
				return
			case <-ticker.C:
				cm.sweepIdleRangeCaches(time.Now(), expiry)
			}
		}
	})
}

// sweepIdleRangeCaches drops per-file caches whose last access is older than
// expiry. Files with prefetch still in flight are skipped — the fetch is
// about to write into the cache, so dropping it would leak the accounting.
func (cm *DefaultCacheManager) sweepIdleRangeCaches(now time.Time, expiry time.Duration) {
	cutoff := now.Add(-expiry)
	cm.rangeMu.Lock()
	defer cm.rangeMu.Unlock()
	for path, fc := range cm.rangeChunks {
		if fc == nil {
			delete(cm.rangeChunks, path)
			continue
		}
		if len(fc.prefetchInFlight) > 0 {
			continue
		}
		if fc.lastAccess.Before(cutoff) {
			cm.dropFileCacheLocked(path)
			cm.metrics.RangeCacheIdleExpiries.Add(1)
		}
	}
}

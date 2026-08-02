package cache

import (
	"context"
	"time"

	"golang.org/x/sys/unix"
)

// Background watermark eviction: keep NVMe usage inside a low/high band so
// the write path (almost) never pays a foreground eviction scan, and react to
// REAL disk pressure, not just our own accounting. The cache dir lives on a
// shared host disk — kubelet, images, checkpoint tars all land there — so
// internal usage alone can say "fine" while the device is at 95%. Pressure is
// therefore max(ourUsage/budget, deviceUsed/deviceTotal); crossing the high
// watermark evicts down to the low one using the same LRU + pinning +
// durability rules as reactive Evict.
const (
	// watermarkHighPct / LowPct bound the band: evict when max-pressure
	// crosses High, stop once OUR usage is under Low (we can only free our
	// own bytes; foreign disk usage has to be someone else's problem).
	watermarkHighPct = 85
	watermarkLowPct  = 75
	// watermarkSweepInterval is jittered ±50% to de-synchronize a fleet.
	watermarkSweepInterval = 15 * time.Second
)

// statfsFreeTotal returns (free, total) bytes for the filesystem holding dir,
// or ok=false when statfs fails (evictor then runs on internal usage alone).
func statfsFreeTotal(dir string) (free, total int64, ok bool) {
	var st unix.Statfs_t
	if err := unix.Statfs(dir, &st); err != nil {
		return 0, 0, false
	}
	bsize := int64(st.Bsize)
	return int64(st.Bavail) * bsize, int64(st.Blocks) * bsize, true
}

// startWatermarkEvictor launches the background pressure loop.
func (cm *DefaultCacheManager) startWatermarkEvictor() {
	if cm.config.MaxNVMeSize <= 0 || cm.config.WatermarkEvictDisabled {
		return
	}
	cm.goBackground(func() {
		for {
			wait := jitteredDuration(watermarkSweepInterval)
			select {
			case <-cm.shutdownCtx.Done():
				return
			case <-time.After(wait):
			}
			cm.runWatermarkEviction(cm.shutdownCtx)
		}
	})
}

// nvmePressurePct returns the effective pressure percentage: the max of our
// budget usage and the device's real usage. Device pressure only counts when
// statfs works and the device number is higher — a mostly-empty huge disk
// must not mask an over-budget cache, and vice versa.
func (cm *DefaultCacheManager) nvmePressurePct() int {
	used, capacity := cm.Stats()
	internal := 0
	if capacity > 0 {
		internal = int(used * 100 / capacity)
	}
	device := 0
	if free, total, ok := statfsFreeTotal(cm.config.NVMePath); ok && total > 0 {
		device = int((total - free) * 100 / total)
	}
	if device > internal {
		return device
	}
	return internal
}

// runWatermarkEviction performs one pressure check + eviction pass.
func (cm *DefaultCacheManager) runWatermarkEviction(ctx context.Context) {
	pressure := cm.nvmePressurePct()
	if pressure < watermarkHighPct {
		return
	}
	cm.metrics.WatermarkEvictorRuns.Add(1)

	usedBefore, capacity := cm.Stats()
	// Target OUR usage at the low watermark of the budget. When pressure is
	// device-driven our share may already be small — evict proportionally
	// down to the fraction that keeps us blameless, floored at zero work
	// when we hold almost nothing.
	target := capacity * watermarkLowPct / 100
	if usedBefore <= target {
		// Device is full but not because of us; shrink harder only if we
		// hold a meaningful share (>10% of device). Otherwise nothing to do.
		if free, total, ok := statfsFreeTotal(cm.config.NVMePath); ok && total > 0 {
			if usedBefore*10 > total {
				target = usedBefore * int64(watermarkLowPct) / int64(watermarkHighPct)
			} else {
				_ = free
				return
			}
		} else {
			return
		}
	}

	cm.evictToTarget(ctx, target)
	usedAfter, _ := cm.Stats()
	if freed := usedBefore - usedAfter; freed > 0 {
		cm.metrics.WatermarkEvictedBytes.Add(freed)
		cm.logger.Printf("Watermark eviction: pressure=%d%% freed=%dMB used %dMB -> %dMB (target %dMB)",
			pressure, freed/(1024*1024), usedBefore/(1024*1024), usedAfter/(1024*1024), target/(1024*1024))
	}
}

// jitteredDuration returns d ±50% using crypto-rand-free jitter (coarse is
// fine here; sleepWithJitter needs a ctx and this path has its own select).
func jitteredDuration(d time.Duration) time.Duration {
	n := time.Now().UnixNano()
	// Cheap deterministic-ish spread: fold nanos into [50%, 150%).
	frac := 50 + (n/1000)%100
	return d * time.Duration(frac) / 100
}

package cache

import "time"

// Adaptive readahead sizing: the max prefetch window is derived from the
// throughput the peer tier is actually delivering, instead of a static chunk
// count. A static cap tuned for 4MiB chunks on one SKU is wrong everywhere
// else — too shallow on fast links (pipeline drains between round-trips) and
// needlessly deep on slow ones (memory for no gain).
//
// Sizing rule: keep ~windowTargetDuration of measured aggregate throughput in
// flight. bytes = measured_MBps x parallel_workers x target_seconds, floored
// at the static configured cap (never regress below explicit tuning) and
// ceilinged by the prefetch byte budget.
const (
	// windowTargetDuration is how much lead time the readahead window buys.
	// 250ms covers a peer round-trip burst plus scheduling jitter without
	// ballooning memory (at 1GB/s this is a 256MB window).
	windowTargetDuration = 250 * time.Millisecond
	// adaptiveWindowMinSamples gates derivation until the peer-throughput
	// estimate has enough ops behind it to be meaningful.
	adaptiveWindowMinSamples = 8

	// longStreakThreshold/Multiplier: after this many consecutive sequential
	// reads, the window may grow past the bandwidth-delay target up to
	// multiplier x (mountpoint-s3 auto-scales per-handle windows to 2GiB for
	// the same reason: one long sequential stream — checkpoint restore —
	// wants maximum pipeline depth, while short streams shouldn't pay the
	// memory). Budget caps still apply after multiplication.
	longStreakThreshold  = 32
	longStreakMultiplier = 4
)

// peerPerStreamBytesPerSec returns the measured single-stream peer read rate
// (PeerReadNanos sums per-op wall time, so bytes/nanos is per-stream, not
// aggregate). ok=false until enough samples exist — the single gate every
// throughput-derived knob shares.
func (cm *DefaultCacheManager) peerPerStreamBytesPerSec() (float64, bool) {
	if cm.metrics.PeerReadOps.Load() < adaptiveWindowMinSamples {
		return 0, false
	}
	bytes := cm.metrics.PeerReadBytes.Load()
	nanos := cm.metrics.PeerReadNanos.Load()
	if bytes <= 0 || nanos <= 0 {
		return 0, false
	}
	rate := float64(bytes) / (float64(nanos) / 1e9)
	// Operator throughput ceiling: derive pipelines as if the link were no
	// faster than the cap, so aggregate cache traffic stays under it.
	if cap := cm.config.MaxThroughputBytesPerSec; cap > 0 {
		workers := cm.config.ParallelRangeReads
		if workers <= 0 {
			workers = 1
		}
		if perStreamCap := float64(cap) / float64(workers); rate > perStreamCap {
			rate = perStreamCap
		}
	}
	return rate, true
}

// adaptivePrefetchMaxChunks returns the effective max readahead window in
// chunks. Falls back to the configured static cap until enough peer reads
// have been observed.
func (cm *DefaultCacheManager) adaptivePrefetchMaxChunks(chunkSize int64) int {
	staticMax := cm.config.RangePrefetchMaxChunks
	if chunkSize <= 0 {
		return staticMax
	}
	perStreamBytesPerSec, ok := cm.peerPerStreamBytesPerSec()
	if !ok {
		return staticMax
	}
	workers := cm.config.ParallelRangeReads
	if workers <= 0 {
		workers = 1
	}
	targetBytes := perStreamBytesPerSec * float64(workers) * windowTargetDuration.Seconds()

	// Never exceed the in-flight prefetch byte budget: window and budget
	// disagreeing is exactly the waste failure mode the global cache budget
	// work eliminated.
	if maxBytes := cm.config.RangePrefetchMaxBytes; maxBytes > 0 && targetBytes > float64(maxBytes) {
		targetBytes = float64(maxBytes)
	}

	derived := int(targetBytes / float64(chunkSize))
	if derived < staticMax {
		return staticMax
	}
	return derived
}

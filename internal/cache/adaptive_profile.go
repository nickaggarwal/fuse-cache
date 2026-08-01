package cache

// Per-file adaptive I/O profiles.
//
// The static flags (ParallelRangeReads, RangePrefetch*) describe ONE
// workload on ONE hardware profile. Real traffic mixes 4KB metadata files
// with multi-GB checkpoints, on links measured anywhere from 200MB/s to
// 3GB/s in live testing. Instead of asking operators to retune flags per
// SKU, every read/write derives its knobs at call time from two inputs:
//
//   1. File size — a small file must not pay range-pipeline setup, a huge
//      file must not be limited by knobs sized for medium files.
//   2. Measured throughput — the metrics EWMAs the client already collects
//      (per-op peer read rate, same signal adaptivePrefetchMaxChunks uses).
//
// The configured flags remain as the MEDIUM-class baseline and as floors/
// ceilings, so explicit operator tuning is never silently overridden
// downward — derivation only widens the pipeline when the file and the
// measured link justify it.
//
// Size classes (boundaries chosen from live A/B data, 2026-08-01):
//   small  <= 1 chunk        — whole-object path, no range machinery.
//   medium <= mediumFileMax  — configured knobs as-is.
//   large  >  mediumFileMax  — workers scaled up to largeReadWorkersMax,
//                              prefetch window from measured throughput,
//                              persist concurrency scaled with chunk count.

const (
	// mediumFileMax splits medium from large. 256MiB ≈ where the range
	// pipeline (not per-request latency) becomes the bottleneck in testing.
	mediumFileMax = 256 * 1024 * 1024

	// largeReadWorkersMax caps derived read workers. 5GB cold reads measured
	// 1.04GB/s at 24 workers on a 25Gbps link; doubling workers past ~32
	// only adds goroutine churn.
	largeReadWorkersMax = 32

	// persistWorkersMin/Max bound cloud-persist concurrency. The old fixed
	// cap of 8 throttled multi-GB checkpoint uploads; Express/S3 sustain 16
	// parallel chunk PUTs without throttling in live testing.
	persistWorkersMin = 4
	persistWorkersMax = 16
)

// readProfile is the derived per-read tuning set.
type readProfile struct {
	workers int
	// prefetchMaxChunks caps the adaptive readahead window for this file
	// (already throughput-derived; size class only gates it).
	prefetchMaxChunks int
}

// readProfileFor derives the read pipeline shape for one file.
func (cm *DefaultCacheManager) readProfileFor(fileSize, chunkSize int64) readProfile {
	if chunkSize <= 0 || fileSize <= chunkSize {
		// Small: single-object read; no fan-out, no readahead (and no
		// throughput-derivation work — this is the hottest class).
		return readProfile{workers: 1, prefetchMaxChunks: 0}
	}

	workers := cm.config.ParallelRangeReads
	if workers < 1 {
		workers = 1
	}
	prefetchMax := cm.adaptivePrefetchMaxChunks(chunkSize)
	if fileSize <= mediumFileMax {
		// Medium: configured baseline, window capped to the file itself so a
		// 64MB file can't reserve a 512MB readahead budget.
		fileChunks := int((fileSize + chunkSize - 1) / chunkSize)
		if prefetchMax > fileChunks {
			prefetchMax = fileChunks
		}
		return readProfile{workers: workers, prefetchMaxChunks: prefetchMax}
	}

	// Large: widen workers toward the measured link. Approximate the
	// per-stream rate from the same EWMA the window derivation uses; each
	// worker is one stream, so workers ≈ what saturates the link within cap.
	if derived := cm.derivedLargeReadWorkers(chunkSize); derived > workers {
		workers = derived
	}
	if workers > largeReadWorkersMax {
		workers = largeReadWorkersMax
	}
	return readProfile{workers: workers, prefetchMaxChunks: prefetchMax}
}

// derivedLargeReadWorkers sizes the worker pool so that workers x per-stream
// throughput ≈ one chunk per chunk-transfer-time across the window. With no
// samples yet it returns 0 (caller keeps the configured baseline).
func (cm *DefaultCacheManager) derivedLargeReadWorkers(chunkSize int64) int {
	if chunkSize <= 0 {
		return 0
	}
	perStreamBps, ok := cm.peerPerStreamBytesPerSec()
	if !ok {
		return 0
	}
	// Keep windowTargetDuration of data in flight, one chunk per worker.
	chunksInFlight := perStreamBps * windowTargetDuration.Seconds() / float64(chunkSize)
	// Each worker overlaps ~1 chunk; round up and add headroom for stragglers.
	derived := int(chunksInFlight) + 2
	if derived < 1 {
		return 0
	}
	return derived
}

// persistWorkersFor sizes the background cloud-persist pool for one file:
// small files get the floor, checkpoint-scale files get enough workers that
// upload wall time tracks the cloud tier rather than the worker cap, without
// starving foreground traffic.
func (cm *DefaultCacheManager) persistWorkersFor(numChunks int64) int {
	workers := persistWorkersMin
	// One worker per 8 chunks of backlog, within bounds: a 5GB/8MB file
	// (640 chunks) gets the max; a 3-chunk file stays at the floor.
	if byBacklog := int(numChunks / 8); byBacklog > workers {
		workers = byBacklog
	}
	if workers > persistWorkersMax {
		workers = persistWorkersMax
	}
	if int64(workers) > numChunks && numChunks > 0 {
		workers = int(numChunks)
	}
	return workers
}

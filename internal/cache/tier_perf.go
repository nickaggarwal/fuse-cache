package cache

import (
	"sync"
	"time"
)

// Adaptive remote-read tuning constants.
const (
	// tierPerfAlpha is the EWMA weight applied to each new sample. Higher values
	// react faster to change; lower values are steadier.
	tierPerfAlpha = 0.3
	// tierPerfMinSamples is how many samples a tier needs before its measured
	// score is trusted. Below this, the cold-start prior (peer-first) is used.
	tierPerfMinSamples = 3
	// tierPerfRecoveryWindow is how long a tier may go unsampled before its
	// effective success is nudged upward so it gets re-probed (optimism under
	// uncertainty). This lets a transiently-failing tier recover instead of
	// being permanently demoted.
	tierPerfRecoveryWindow = 30 * time.Second
	// tierPerfRecoveryFloor is the optimistic success floor applied to a stale
	// tier when computing its score for re-exploration.
	tierPerfRecoveryFloor = 0.7
	// tierPerfReliableSuccess is the success ratio at or above which a primary
	// tier is considered reliable enough to read without hedging.
	tierPerfReliableSuccess = 0.85
	// tierPerfCloseRatio: if the secondary tier's score is within this fraction
	// of the primary's, the two are a close race and hedging buys latency
	// insurance for little wasted bandwidth.
	tierPerfCloseRatio = 0.75
	// tierPerfLatencyFloorMs avoids divide-by-zero and over-weighting sub-ms
	// reads when computing score = success / latency.
	tierPerfLatencyFloorMs = 0.5
)

// tierStat holds EWMA performance for a single remote tier.
type tierStat struct {
	ewmaLatencyMs float64
	ewmaSuccess   float64
	samples       int64
	lastSampleAt  time.Time
	// misses counts reads this tier declined because it did not hold the
	// object. Kept out of the EWMAs (see recordTierOutcome) but reported, so
	// samples==0 can be told apart from "busy tier that never has anything".
	misses int64
}

// tierPerfTracker tracks measured peer vs cloud read performance so the read
// path can adaptively choose which tier to try first and whether to hedge.
// It is global to a CacheManager: peer-vs-cloud behavior is a property of the
// network/tiers, not of individual files.
type tierPerfTracker struct {
	mu    sync.RWMutex
	peer  tierStat
	cloud tierStat
}

func newTierPerfTracker() *tierPerfTracker {
	return &tierPerfTracker{}
}

func (t *tierPerfTracker) statFor(tier CacheTier) *tierStat {
	switch tier {
	case TierPeer:
		return &t.peer
	case TierCloud:
		return &t.cloud
	default:
		return nil
	}
}

// record folds one remote read outcome into the tier's EWMAs.
func (t *tierPerfTracker) record(tier CacheTier, latency time.Duration, ok bool) {
	if tier != TierPeer && tier != TierCloud {
		return
	}
	latencyMs := float64(latency) / float64(time.Millisecond)
	successVal := 0.0
	if ok {
		successVal = 1.0
	}

	t.mu.Lock()
	defer t.mu.Unlock()
	s := t.statFor(tier)
	if s.samples == 0 {
		s.ewmaLatencyMs = latencyMs
		s.ewmaSuccess = successVal
	} else {
		s.ewmaLatencyMs = tierPerfAlpha*latencyMs + (1-tierPerfAlpha)*s.ewmaLatencyMs
		s.ewmaSuccess = tierPerfAlpha*successVal + (1-tierPerfAlpha)*s.ewmaSuccess
	}
	s.samples++
	s.lastSampleAt = time.Now()
}

// recordMiss counts a "tier does not hold this object" outcome without
// touching the EWMAs or lastSampleAt.
func (t *tierPerfTracker) recordMiss(tier CacheTier) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if s := t.statFor(tier); s != nil {
		s.misses++
	}
}

// score returns a higher-is-better score for a tier: effective success divided
// by latency. A stale tier (unsampled beyond the recovery window) gets an
// optimistic success floor so it is periodically re-probed.
func (s *tierStat) score(now time.Time) float64 {
	effSuccess := s.ewmaSuccess
	if !s.lastSampleAt.IsZero() && now.Sub(s.lastSampleAt) > tierPerfRecoveryWindow && effSuccess < tierPerfRecoveryFloor {
		effSuccess = tierPerfRecoveryFloor
	}
	latency := s.ewmaLatencyMs
	if latency < tierPerfLatencyFloorMs {
		latency = tierPerfLatencyFloorMs
	}
	return effSuccess / latency
}

// order returns the remote tiers ordered best-first. Until both tiers have
// enough samples it returns the cold-start prior (peer-first), preserving the
// system's prior behavior while measurements accumulate.
func (t *tierPerfTracker) order() []CacheTier {
	t.mu.RLock()
	defer t.mu.RUnlock()
	if t.peer.samples < tierPerfMinSamples || t.cloud.samples < tierPerfMinSamples {
		return remoteReadOrderPeerFirst
	}
	now := time.Now()
	if t.cloud.score(now) > t.peer.score(now) {
		return []CacheTier{TierCloud, TierPeer}
	}
	return []CacheTier{TierPeer, TierCloud}
}

// shouldHedge reports whether to race both tiers. It returns ok=false when
// there is not enough data yet, so the caller can fall back to its size-based
// policy. When ok=true, hedge reflects reliability and closeness of the tiers.
func (t *tierPerfTracker) shouldHedge() (hedge bool, ok bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	if t.peer.samples < tierPerfMinSamples || t.cloud.samples < tierPerfMinSamples {
		return false, false
	}
	now := time.Now()
	peerScore := t.peer.score(now)
	cloudScore := t.cloud.score(now)

	primary := &t.peer
	primaryScore, secondaryScore := peerScore, cloudScore
	if cloudScore > peerScore {
		primary = &t.cloud
		primaryScore, secondaryScore = cloudScore, peerScore
	}

	// Unreliable primary: insure with the other tier.
	if primary.ewmaSuccess < tierPerfReliableSuccess {
		return true, true
	}
	// Close race: hedge to shave tail latency for little wasted bandwidth.
	if primaryScore > 0 && secondaryScore >= primaryScore*tierPerfCloseRatio {
		return true, true
	}
	// Clear, reliable winner: read it alone and save the secondary bandwidth.
	return false, true
}

// snapshot returns per-tier stats for metrics exposure.
func (t *tierPerfTracker) snapshot() (peerLatencyMs, peerSuccess float64, peerSamples int64, cloudLatencyMs, cloudSuccess float64, cloudSamples int64) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.peer.ewmaLatencyMs, t.peer.ewmaSuccess, t.peer.samples,
		t.cloud.ewmaLatencyMs, t.cloud.ewmaSuccess, t.cloud.samples
}

// missCounts returns per-tier miss totals.
func (t *tierPerfTracker) missCounts() (peer, cloud int64) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.peer.misses, t.cloud.misses
}

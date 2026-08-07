package cache

import (
	"math"
	"testing"
	"time"
)

func closeTo(a, b float64) bool { return math.Abs(a-b) < 1e-9 }

// The first sample must seed the averages outright. Blending it against a zero
// initial value would report half the measured latency — a link that looks
// twice as fast as it is, on exactly the sample that decides cold-start
// routing.
func TestEWMASeedsOnFirstSample(t *testing.T) {
	var s ewmaStat
	s.observe(20*time.Millisecond, true)
	if !closeTo(s.ewmaLatencyMs, 20) {
		t.Fatalf("first latency sample = %v, want the sample itself (20)", s.ewmaLatencyMs)
	}
	if !closeTo(s.ewmaSuccess, 1) {
		t.Fatalf("first success sample = %v, want 1", s.ewmaSuccess)
	}
	if s.samples != 1 {
		t.Fatalf("samples = %d, want 1", s.samples)
	}

	// The second blends at alpha.
	s.observe(120*time.Millisecond, false)
	wantLatency := ewmaAlpha*120 + (1-ewmaAlpha)*20
	if !closeTo(s.ewmaLatencyMs, wantLatency) {
		t.Fatalf("blended latency = %v, want %v", s.ewmaLatencyMs, wantLatency)
	}
	if !closeTo(s.ewmaSuccess, 1-ewmaAlpha) {
		t.Fatalf("blended success = %v, want %v", s.ewmaSuccess, 1-ewmaAlpha)
	}
}

// A failure-only stat scores zero, and repeated failures never push it below
// zero or produce a NaN — score feeds a comparison, and a NaN silently loses
// every one of them.
func TestEWMAFailuresScoreZero(t *testing.T) {
	var s ewmaStat
	for i := 0; i < 10; i++ {
		s.observe(5*time.Millisecond, false)
	}
	if got := s.score(); got != 0 {
		t.Fatalf("all-failure score = %v, want 0", got)
	}
}

// The latency floor is applied at score time, not at record time: a sub-ms
// link must be reported at its true latency and only floored in the division.
func TestEWMALatencyFloorAppliesAtScoreTime(t *testing.T) {
	var s ewmaStat
	s.observe(100*time.Microsecond, true)
	if !closeTo(s.ewmaLatencyMs, 0.1) {
		t.Fatalf("recorded latency = %v, want the measured 0.1ms", s.ewmaLatencyMs)
	}
	if want := 1.0 / ewmaLatencyFloorMs; !closeTo(s.score(), want) {
		t.Fatalf("score = %v, want %v (floored divisor)", s.score(), want)
	}
}

// scoreWith substitutes the success value without touching the recorded one —
// tier_perf's stale-tier optimism depends on that.
func TestEWMAScoreWithDoesNotMutate(t *testing.T) {
	var s ewmaStat
	s.observe(10*time.Millisecond, false)
	if got := s.scoreWith(0.7); !closeTo(got, 0.07) {
		t.Fatalf("scoreWith(0.7) = %v, want 0.07", got)
	}
	if s.ewmaSuccess != 0 {
		t.Fatalf("scoreWith mutated the recorded success to %v", s.ewmaSuccess)
	}
}

// Both scorers must keep folding through the shared implementation: a tier
// stat and a peer-pair stat given the same samples must agree.
func TestSharedFoldUsedByBothScorers(t *testing.T) {
	samples := []struct {
		d  time.Duration
		ok bool
	}{
		{12 * time.Millisecond, true},
		{40 * time.Millisecond, true},
		{8 * time.Millisecond, false},
		{15 * time.Millisecond, true},
	}

	tiers := newTierPerfTracker()
	pairs := newPeerLatencyTracker()
	for _, s := range samples {
		tiers.record(TierPeer, s.d, s.ok)
		pairs.record("peer-a", s.d, s.ok)
	}

	tierLatency, tierSuccess, tierSamples, _, _, _ := tiers.snapshot()
	pairLatency, pairSuccess, ok := pairs.measured("peer-a")
	if !ok {
		t.Fatal("pair not trusted after 4 samples")
	}
	if !closeTo(tierLatency, pairLatency) || !closeTo(tierSuccess, pairSuccess) {
		t.Fatalf("scorers diverged on identical samples: tier=(%v,%v) pair=(%v,%v)",
			tierLatency, tierSuccess, pairLatency, pairSuccess)
	}
	if tierSamples != int64(len(samples)) {
		t.Fatalf("samples = %d, want %d", tierSamples, len(samples))
	}
}

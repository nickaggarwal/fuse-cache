package cache

import "time"

// The adaptive scorers in this package — per-peer pair latency
// (peer_latency.go) and peer-vs-cloud tier performance (tier_perf.go) —
// measure different things but fold their samples the same way: an
// exponentially weighted moving average of latency and of success, scored as
// success/latency with a floor so a sub-millisecond read cannot dominate.
//
// They are kept as one type because the fold is where the subtle rules live.
// The first sample must seed rather than blend (blending against a zero
// initial value halves the first real measurement, which reads as "this link
// is twice as fast as it is"), and the latency floor has to be applied at
// score time rather than at record time, or a genuinely fast link is recorded
// as slower than it is. Both were correct in both copies; keeping two copies
// meant the next change had to be made correctly twice.
//
// What is deliberately NOT shared: which outcomes are eligible to be recorded
// at all. Busy and miss classification differs per scorer and is the caller's
// job — see the three-class rule in CLAUDE.md.
const (
	// ewmaAlpha is the weight applied to each new sample. Higher reacts faster
	// to change; lower is steadier.
	ewmaAlpha = 0.3
	// ewmaLatencyFloorMs avoids divide-by-zero and over-weighting sub-ms reads
	// when computing score = success / latency.
	ewmaLatencyFloorMs = 0.5
)

// ewmaStat is the shared fold: latency and success EWMAs plus a sample count.
// The zero value is an unsampled stat. Not safe for concurrent use — callers
// hold their own lock, since they also guard the surrounding maps and fields.
type ewmaStat struct {
	ewmaLatencyMs float64
	ewmaSuccess   float64
	samples       int64
}

// observe folds one outcome in. The first sample seeds the averages outright.
func (s *ewmaStat) observe(latency time.Duration, ok bool) {
	latencyMs := float64(latency) / float64(time.Millisecond)
	successVal := 0.0
	if ok {
		successVal = 1.0
	}
	if s.samples == 0 {
		s.ewmaLatencyMs = latencyMs
		s.ewmaSuccess = successVal
	} else {
		s.ewmaLatencyMs = ewmaAlpha*latencyMs + (1-ewmaAlpha)*s.ewmaLatencyMs
		s.ewmaSuccess = ewmaAlpha*successVal + (1-ewmaAlpha)*s.ewmaSuccess
	}
	s.samples++
}

// scoreWith returns higher-is-better success/latency using the supplied
// success value, so callers can substitute an adjusted one (tier_perf's
// optimistic floor for a stale tier) without reimplementing the division.
func (s *ewmaStat) scoreWith(effSuccess float64) float64 {
	latency := s.ewmaLatencyMs
	if latency < ewmaLatencyFloorMs {
		latency = ewmaLatencyFloorMs
	}
	return effSuccess / latency
}

// score is scoreWith the recorded success.
func (s *ewmaStat) score() float64 { return s.scoreWith(s.ewmaSuccess) }

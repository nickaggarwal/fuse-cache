package cache

import (
	"sync"
	"time"
)

// Cache health: a node-level signal a router can act on.
//
// Residency (residency.go) answers "is prefix X here". It does not answer
// "can this node keep it here". A node whose working set is bigger than its
// NVMe re-warms the same intents every reconcile pass: each pass finds files
// evicted since the last one, fetches them back, and evicts something else
// to make room. Every individual report is honest, and the node still
// thrashes: cold starts there pay peer or cloud latency for files that were
// "resident" two minutes earlier. Two signals name that state:
//
//   - Re-warm streak, per intent: consecutive reconcile passes that had to
//     fetch files back. One re-warm is normal drift (a node reboot, a burst
//     from another workload). A streak means the node cannot hold the
//     declared set.
//   - Eviction rate: bytes evicted over the last window. Sustained high
//     eviction with live intents is the same story from the other side.
//
// State is "ok", "degraded" (intents unsatisfied: a failed or skipped pass,
// or a short re-warm streak) or "thrashing" (a re-warm streak at or past
// thrashStreak, or eviction churning through more than thrashEvictFraction
// of the budget within the window). A router should down-weight
// "thrashing" nodes for placement of more models, and an operator alert on
// it means "this node needs more NVMe or fewer models".

const (
	// thrashStreak consecutive re-warming passes mark an intent thrashing
	// (at the 2-minute default interval, ~6 minutes of continuous churn).
	thrashStreak = 3
	// healthEvictWindow is the window for the eviction-rate signal.
	healthEvictWindow = 10 * time.Minute
	// thrashEvictFraction: evicting more than this fraction of the NVMe
	// budget within the window, while intents are declared, is thrashing.
	thrashEvictFraction = 0.5
)

// CacheHealth is the node-level health report.
type CacheHealth struct {
	State string `json:"state"` // ok | degraded | thrashing
	// Reasons explains a non-ok state in a sentence each.
	Reasons []string `json:"reasons,omitempty"`
	// Intents is how many warm intents are declared; Unsatisfied how many had
	// failed or skipped files on their last pass.
	Intents     int `json:"intents"`
	Unsatisfied int `json:"unsatisfied"`
	// MaxRewarmStreak is the longest current re-warm streak across intents.
	MaxRewarmStreak int `json:"max_rewarm_streak"`
	// EvictedBytesWindow is bytes evicted over the last WindowSeconds.
	EvictedBytesWindow int64     `json:"evicted_bytes_window"`
	WindowSeconds      int       `json:"window_seconds"`
	BudgetBytes        int64     `json:"budget_bytes"`
	UsedBytes          int64     `json:"used_bytes"`
	OpenFiles          int       `json:"open_files"`
	MeasuredAt         time.Time `json:"measured_at"`
}

// evictWindow keeps per-minute eviction byte buckets for the rate signal.
type evictWindow struct {
	mu      sync.Mutex
	buckets [10]int64
	minute  [10]int64 // unix minute each bucket holds
}

func (w *evictWindow) add(bytes int64, now time.Time) {
	m := now.Unix() / 60
	i := int(m % int64(len(w.buckets)))
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.minute[i] != m {
		w.minute[i] = m
		w.buckets[i] = 0
	}
	w.buckets[i] += bytes
}

func (w *evictWindow) sum(now time.Time) int64 {
	m := now.Unix() / 60
	w.mu.Lock()
	defer w.mu.Unlock()
	var total int64
	for i := range w.buckets {
		if m-w.minute[i] < int64(len(w.buckets)) {
			total += w.buckets[i]
		}
	}
	return total
}

// Health computes the node's cache health.
func (cm *DefaultCacheManager) Health() CacheHealth {
	now := time.Now()
	used, _ := cm.Stats()
	h := CacheHealth{
		State:              "ok",
		EvictedBytesWindow: cm.evictBytes.sum(now),
		WindowSeconds:      int(healthEvictWindow / time.Second),
		BudgetBytes:        cm.config.MaxNVMeSize,
		UsedBytes:          used,
		OpenFiles:          cm.OpenFileCount(),
		MeasuredAt:         now,
	}
	var intents []warmIntent
	if cm.warmIntents != nil {
		intents = cm.warmIntents.snapshot()
	}
	h.Intents = len(intents)
	for _, in := range intents {
		if in.rewarmStreak > h.MaxRewarmStreak {
			h.MaxRewarmStreak = in.rewarmStreak
		}
		if in.lastErr != nil || (in.lastResult != nil && (in.lastResult.Failed > 0 || in.lastResult.Skipped > 0)) {
			h.Unsatisfied++
		}
	}
	degrade := func(reason string) {
		if h.State == "ok" {
			h.State = "degraded"
		}
		h.Reasons = append(h.Reasons, reason)
	}
	thrash := func(reason string) {
		h.State = "thrashing"
		h.Reasons = append(h.Reasons, reason)
	}
	if h.Unsatisfied > 0 {
		degrade("declared warm intents are not fully resident (failed or skipped files on the last pass)")
	}
	switch {
	case h.MaxRewarmStreak >= thrashStreak:
		thrash("warm intents re-fetched evicted files on consecutive reconcile passes: the declared set does not fit")
	case h.MaxRewarmStreak > 0:
		degrade("a warm intent re-fetched evicted files on its last pass")
	}
	if h.Intents > 0 && h.BudgetBytes > 0 && float64(h.EvictedBytesWindow) > thrashEvictFraction*float64(h.BudgetBytes) {
		thrash("evicted more than half the NVMe budget in the last window while intents are declared")
	}
	return h
}

// healthStateValue maps a state to a gauge value for /metrics.
func healthStateValue(state string) int {
	switch state {
	case "degraded":
		return 1
	case "thrashing":
		return 2
	}
	return 0
}

// HealthStateValue is Health().State as 0 (ok), 1 (degraded), 2 (thrashing).
func (h CacheHealth) HealthStateValue() int { return healthStateValue(h.State) }

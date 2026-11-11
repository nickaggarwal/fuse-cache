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
	// Checks are the active health conditions, each with a stable code.
	// Match on Code; Message is for humans and may change.
	Checks []HealthCheck `json:"checks,omitempty"`
	// Reasons repeats each check's message (kept for existing readers).
	Reasons []string `json:"reasons,omitempty"`
	// Muted lists codes suppressed via MuteHealth: still reported in
	// Checks (with Muted set) but they no longer raise State.
	Muted []string `json:"muted,omitempty"`
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

// HealthCheck is one active condition, after Ceph's health checks: a stable
// code a router or alert matches on, the state it implies, and a message.
type HealthCheck struct {
	Code     string `json:"code"`
	Severity string `json:"severity"` // degraded | thrashing
	Message  string `json:"message"`
	Count    int    `json:"count,omitempty"`
	Muted    bool   `json:"muted,omitempty"`
}

// Stable health codes. Add new ones; never rename or reuse one.
const (
	// HealthIntentUnsatisfied: a declared warm intent had failed or skipped
	// files on its last pass.
	HealthIntentUnsatisfied = "INTENT_UNSATISFIED"
	// HealthRewarm: an intent re-fetched evicted files on its last pass.
	HealthRewarm = "REWARM"
	// HealthRewarmStreak: re-fetches on consecutive passes; the declared
	// set does not fit.
	HealthRewarmStreak = "REWARM_STREAK"
	// HealthEvictChurn: over half the budget evicted within the window while
	// intents are declared.
	HealthEvictChurn = "EVICT_CHURN"
	// HealthSlowOps: remote fetches in flight longer than slowOpThreshold.
	HealthSlowOps = "SLOW_OPS"
	// HealthScrubErrors: the scrubber found corrupt local copies recently.
	HealthScrubErrors = "SCRUB_ERRORS"
	// HealthNoRebalance: re-replication paused by an operator (rollout).
	HealthNoRebalance = "NOREBALANCE"
)

// healthMutes holds codes an operator silenced (Ceph's "health mute"), each
// until an expiry.
type healthMutes struct {
	mu    sync.Mutex
	until map[string]time.Time
}

// MuteHealth silences code for d (d <= 0 unmutes). A muted check is still
// listed, marked muted, but does not raise the node's state.
func (cm *DefaultCacheManager) MuteHealth(code string, d time.Duration) {
	cm.healthMutes.mu.Lock()
	defer cm.healthMutes.mu.Unlock()
	if cm.healthMutes.until == nil {
		cm.healthMutes.until = make(map[string]time.Time)
	}
	if d <= 0 {
		delete(cm.healthMutes.until, code)
		return
	}
	cm.healthMutes.until[code] = time.Now().Add(d)
}

func (cm *DefaultCacheManager) healthMuted(code string, now time.Time) bool {
	cm.healthMutes.mu.Lock()
	defer cm.healthMutes.mu.Unlock()
	t, ok := cm.healthMutes.until[code]
	if ok && now.After(t) {
		delete(cm.healthMutes.until, code)
		return false
	}
	return ok
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
	add := func(code, severity, msg string, count int) {
		c := HealthCheck{Code: code, Severity: severity, Message: msg, Count: count}
		if cm.healthMuted(code, now) {
			c.Muted = true
			h.Muted = append(h.Muted, code)
		} else if healthStateValue(severity) > healthStateValue(h.State) {
			h.State = severity
		}
		h.Checks = append(h.Checks, c)
		h.Reasons = append(h.Reasons, msg)
	}
	if h.Unsatisfied > 0 {
		add(HealthIntentUnsatisfied, "degraded", "declared warm intents are not fully resident (failed or skipped files on the last pass)", h.Unsatisfied)
	}
	switch {
	case h.MaxRewarmStreak >= thrashStreak:
		add(HealthRewarmStreak, "thrashing", "warm intents re-fetched evicted files on consecutive reconcile passes: the declared set does not fit", h.MaxRewarmStreak)
	case h.MaxRewarmStreak > 0:
		add(HealthRewarm, "degraded", "a warm intent re-fetched evicted files on its last pass", h.MaxRewarmStreak)
	}
	if h.Intents > 0 && h.BudgetBytes > 0 && float64(h.EvictedBytesWindow) > thrashEvictFraction*float64(h.BudgetBytes) {
		add(HealthEvictChurn, "thrashing", "evicted more than half the NVMe budget in the last window while intents are declared", 0)
	}
	for _, extra := range cm.extraHealthChecks(now) {
		add(extra.Code, extra.Severity, extra.Message, extra.Count)
	}
	return h
}

// extraHealthChecks collects checks owned by other subsystems (slow ops,
// scrubbing, re-replication state). Each returns nil when healthy.
func (cm *DefaultCacheManager) extraHealthChecks(now time.Time) []HealthCheck {
	var out []HealthCheck
	for _, f := range []func(time.Time) *HealthCheck{cm.slowOpsCheck, cm.scrubHealthCheck, cm.rebalanceHealthCheck} {
		if c := f(now); c != nil {
			out = append(out, *c)
		}
	}
	return out
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

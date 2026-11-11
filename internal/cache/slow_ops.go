package cache

import (
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// In-flight operation tracking, after Ceph's op tracker.
//
// Every remote fetch (peer or cloud tier) is registered while it runs: path,
// tier, start time. GET /api/debug/ops lists what is in flight right now,
// oldest first, plus the slowest recent completions. Any fetch older than
// slowOpThreshold raises SLOW_OPS on /api/cache/health. A hung peer, a stalled
// cloud chunk or a starved worker pool shows up as a named path with an age
// instead of an unexplained slow cold start.
//
// Cost: one map insert and delete per remote fetch (milliseconds of transfer
// each), under a mutex that is never held across I/O.

const (
	// slowOpThreshold marks an in-flight fetch slow. A 16 MiB chunk at a
	// pessimistic 50 MB/s is ~0.3 s; 10 s means something is wrong.
	slowOpThreshold = 10 * time.Second
	// slowOpHistory is how many slow completions are kept for inspection.
	slowOpHistory = 32
)

// OpInfo describes one tracked operation.
type OpInfo struct {
	ID       uint64        `json:"id"`
	Kind     string        `json:"kind"` // fetch
	Path     string        `json:"path"`
	Tier     string        `json:"tier"`
	Started  time.Time     `json:"started"`
	Age      time.Duration `json:"age_ns"`
	AgeSec   float64       `json:"age_sec"`
	Err      string        `json:"error,omitempty"`
	Finished bool          `json:"finished,omitempty"`
}

type opTracker struct {
	mu       sync.Mutex
	next     uint64
	inflight map[uint64]*OpInfo
	slow     []OpInfo // ring of recent slow completions
	slowNext int
	// slowTotal counts completed ops that took longer than the threshold.
	slowTotal atomic.Int64
}

// start registers an operation and returns its completion func.
func (t *opTracker) start(kind, path, tier string) func(err error) {
	t.mu.Lock()
	if t.inflight == nil {
		t.inflight = make(map[uint64]*OpInfo)
	}
	t.next++
	op := &OpInfo{ID: t.next, Kind: kind, Path: path, Tier: tier, Started: time.Now()}
	t.inflight[op.ID] = op
	t.mu.Unlock()
	return func(err error) {
		dur := time.Since(op.Started)
		t.mu.Lock()
		delete(t.inflight, op.ID)
		if dur >= slowOpThreshold {
			done := *op
			done.Age, done.AgeSec, done.Finished = dur, dur.Seconds(), true
			if err != nil {
				done.Err = err.Error()
			}
			if len(t.slow) < slowOpHistory {
				t.slow = append(t.slow, done)
			} else {
				t.slow[t.slowNext] = done
				t.slowNext = (t.slowNext + 1) % slowOpHistory
			}
		}
		t.mu.Unlock()
		if dur >= slowOpThreshold {
			t.slowTotal.Add(1)
		}
	}
}

// snapshot returns in-flight ops (oldest first) and recent slow ones
// (slowest first).
func (t *opTracker) snapshot(now time.Time) (inflight, slow []OpInfo) {
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, op := range t.inflight {
		c := *op
		c.Age = now.Sub(c.Started)
		c.AgeSec = c.Age.Seconds()
		inflight = append(inflight, c)
	}
	slow = append(slow, t.slow...)
	sort.Slice(inflight, func(i, j int) bool { return inflight[i].Started.Before(inflight[j].Started) })
	sort.Slice(slow, func(i, j int) bool { return slow[i].Age > slow[j].Age })
	return inflight, slow
}

// slowInflight counts in-flight ops older than the threshold.
func (t *opTracker) slowInflight(now time.Time) (n int, oldest time.Duration) {
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, op := range t.inflight {
		if age := now.Sub(op.Started); age >= slowOpThreshold {
			n++
			if age > oldest {
				oldest = age
			}
		}
	}
	return n, oldest
}

// OpsReport is the /api/debug/ops payload.
type OpsReport struct {
	Inflight      []OpInfo `json:"inflight"`
	RecentSlow    []OpInfo `json:"recent_slow"`
	SlowThreshold float64  `json:"slow_threshold_sec"`
	SlowTotal     int64    `json:"slow_total"`
}

// Ops reports in-flight and recent slow remote fetches.
func (cm *DefaultCacheManager) Ops() OpsReport {
	in, slow := cm.ops.snapshot(time.Now())
	return OpsReport{Inflight: in, RecentSlow: slow, SlowThreshold: slowOpThreshold.Seconds(), SlowTotal: cm.ops.slowTotal.Load()}
}

// SlowOpsTotal counts completed remote fetches slower than the threshold.
func (cm *DefaultCacheManager) SlowOpsTotal() int64 { return cm.ops.slowTotal.Load() }

func (cm *DefaultCacheManager) slowOpsCheck(now time.Time) *HealthCheck {
	n, oldest := cm.ops.slowInflight(now)
	if n == 0 {
		return nil
	}
	return &HealthCheck{
		Code: HealthSlowOps, Severity: "degraded", Count: n,
		Message: "remote fetches in flight longer than " + slowOpThreshold.String() + " (oldest " + oldest.Round(time.Second).String() + "); see /api/debug/ops",
	}
}

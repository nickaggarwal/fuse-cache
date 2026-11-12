package cache

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

// Background I/O scheduling, after Ceph's mClock.
//
// Foreground reads (a pod loading its model through FUSE), warming, chunk
// assembly, replication and scrubbing all share one NIC and one disk. Before
// this, the only guards were the peer serve gate and per-feature worker caps,
// so a warm of one model could visibly slow the cold start of another on the
// same node, and nothing could be tuned to stop it.
//
// Each background class gets mClock's three knobs, in the form that fits a
// cache with one foreground class:
//
//   - reservation, for the foreground: while foreground reads are active
//     (seen within busyWindow), all background classes together are held to
//     BusyBytesPerSec, so the rest of the link is left to the reads that a
//     pod is waiting on;
//   - weight: that shared allowance is split across the background classes
//     active at the moment by weight (warm > assemble > replicate = scrub);
//   - limit: each class may also have an absolute cap, busy or not.
//
// Charging is after the fact (a fetch is charged when its bytes arrive), so a
// class that runs over its share sleeps before its next fetch: the long-run
// rate holds, a single transfer is never split. The foreground is never
// delayed. Classes travel in the context (withIOClass); untagged work is
// foreground.

type ioClass int

const (
	ioForeground ioClass = iota
	ioWarm
	ioAssemble
	ioReplicate
	ioScrub
	numIOClasses
)

var ioClassNames = [numIOClasses]string{"foreground", "warm", "assemble", "replicate", "scrub"}

// ioClassWeights split the busy allowance among active background classes.
var ioClassWeights = [numIOClasses]float64{0, 4, 2, 1, 1}

const (
	// busyWindow is how long after a foreground read the node counts as busy.
	busyWindow = 5 * time.Second
	// maxThrottleSleep is the step a wait is slept in, so a change (the
	// foreground went idle) is noticed within it.
	maxThrottleSleep = 2 * time.Second
)

// IOSchedConfig sets the scheduler's rates. Zero disables a limit.
type IOSchedConfig struct {
	// BusyBytesPerSec caps all background classes together while foreground
	// reads are active (the foreground reservation).
	BusyBytesPerSec int64
	// ClassBytesPerSec caps one class at all times, by name (warm, assemble,
	// replicate, scrub).
	ClassBytesPerSec map[string]int64
}

type ioIOClassKey struct{}

// withIOClass tags ctx's work with a background class.
func withIOClass(ctx context.Context, c ioClass) context.Context {
	return context.WithValue(ctx, ioIOClassKey{}, c)
}

func ioClassOf(ctx context.Context) ioClass {
	if ctx == nil {
		return ioForeground
	}
	if c, ok := ctx.Value(ioIOClassKey{}).(ioClass); ok {
		return c
	}
	return ioForeground
}

// tokenBucket allows debt: a charge larger than the balance is accepted and
// the caller sleeps it off at the current rate.
type tokenBucket struct {
	tokens float64
	last   time.Time
}

// take charges n bytes at rate bytes/s and returns how long to wait.
func (b *tokenBucket) take(n int64, rate float64, now time.Time) time.Duration {
	if rate <= 0 {
		return 0
	}
	if !b.last.IsZero() {
		b.tokens += now.Sub(b.last).Seconds() * rate
	}
	b.last = now
	// At most one second of burst banked.
	if b.tokens > rate {
		b.tokens = rate
	}
	b.tokens -= float64(n)
	if b.tokens >= 0 {
		return 0
	}
	return time.Duration(-b.tokens / rate * float64(time.Second))
}

type ioScheduler struct {
	mu        sync.Mutex
	cfg       IOSchedConfig
	limits    [numIOClasses]float64
	limitB    [numIOClasses]tokenBucket
	busyB     [numIOClasses]tokenBucket
	lastSeen  [numIOClasses]time.Time
	lastFG    atomic.Int64 // unix nanos of the last foreground activity
	bytes     [numIOClasses]atomic.Int64
	throttled [numIOClasses]atomic.Int64 // nanoseconds slept
	sleepFn   func(ctx context.Context, d time.Duration) error
	nowFn     func() time.Time
}

func (s *ioScheduler) configure(cfg IOSchedConfig) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cfg = cfg
	for c := ioClass(1); c < numIOClasses; c++ {
		s.limits[c] = float64(cfg.ClassBytesPerSec[ioClassNames[c]])
	}
}

func (s *ioScheduler) now() time.Time {
	if s.nowFn != nil {
		return s.nowFn()
	}
	return time.Now()
}

// noteForeground records foreground activity (a FUSE read or open).
func (s *ioScheduler) noteForeground() { s.lastFG.Store(s.now().UnixNano()) }

func (s *ioScheduler) busy(now time.Time) bool {
	last := s.lastFG.Load()
	return last != 0 && now.Sub(time.Unix(0, last)) < busyWindow
}

// charge accounts n bytes of class c's I/O and, for a background class over
// its share, sleeps. Returns ctx's error if it ends during the sleep.
func (s *ioScheduler) charge(ctx context.Context, c ioClass, n int64) error {
	if n <= 0 {
		return nil
	}
	s.bytes[c].Add(n)
	if c == ioForeground {
		s.noteForeground()
		return nil
	}
	now := s.now()
	s.mu.Lock()
	s.lastSeen[c] = now
	wait := s.limitB[c].take(n, s.limits[c], now)
	if busyRate := float64(s.cfg.BusyBytesPerSec); busyRate > 0 && s.busy(now) {
		var sum float64
		for k := ioClass(1); k < numIOClasses; k++ {
			if now.Sub(s.lastSeen[k]) < busyWindow {
				sum += ioClassWeights[k]
			}
		}
		share := busyRate * ioClassWeights[c] / sum
		if w := s.busyB[c].take(n, share, now); w > wait {
			wait = w
		}
	} else {
		// Idle: the busy bucket starts fresh next time the node gets busy,
		// instead of carrying debt from an earlier busy period.
		s.busyB[c] = tokenBucket{}
	}
	limitWait := s.limitB[c].pending(s.limits[c])
	s.mu.Unlock()
	sleep := s.sleepFn
	if sleep == nil {
		sleep = sleepCtx
	}
	// Sleep the debt off in steps of at most maxThrottleSleep. Capping one
	// sleep without looping let concurrent workers pile up debt and skip
	// past it (live: a warm under a 20 MB/s busy cap ran at 60 MB/s). Between
	// steps, a debt owed only to the busy allowance is dropped once the
	// foreground goes idle; a class's own limit is always paid.
	for wait > 0 {
		step := wait
		if step > maxThrottleSleep {
			step = maxThrottleSleep
		}
		s.throttled[c].Add(int64(step))
		if err := sleep(ctx, step); err != nil {
			return err
		}
		wait -= step
		limitWait -= step
		if wait > 0 && limitWait <= 0 && !s.busy(s.now()) {
			return nil
		}
	}
	return nil
}

// pending is how long the bucket's current debt takes to repay at rate.
func (b *tokenBucket) pending(rate float64) time.Duration {
	if rate <= 0 || b.tokens >= 0 {
		return 0
	}
	return time.Duration(-b.tokens / rate * float64(time.Second))
}

func sleepCtx(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// IOClassStat is one class's counters for /metrics.
type IOClassStat struct {
	Class        string
	Bytes        int64
	ThrottledSec float64
}

// IOClassStats reports bytes and throttle time per class.
func (cm *DefaultCacheManager) IOClassStats() []IOClassStat {
	out := make([]IOClassStat, 0, numIOClasses)
	for c := ioClass(0); c < numIOClasses; c++ {
		out = append(out, IOClassStat{
			Class:        ioClassNames[c],
			Bytes:        cm.sched.bytes[c].Load(),
			ThrottledSec: time.Duration(cm.sched.throttled[c].Load()).Seconds(),
		})
	}
	return out
}

// ChargeWarmIO charges n bytes of background fill traffic to the warm class
// (a registry fill of a missed blob, api/registry_fill.go).
func (cm *DefaultCacheManager) ChargeWarmIO(ctx context.Context, n int64) error {
	return cm.sched.charge(ctx, ioWarm, n)
}

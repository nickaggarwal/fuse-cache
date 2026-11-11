package cache

import (
	"errors"
	"testing"
	"time"
)

func TestHealthOKWithoutIntents(t *testing.T) {
	cm := evictTestManager(t, 1<<30)
	cm.warmIntents = newWarmIntentRegistry()
	if h := cm.Health(); h.State != "ok" || len(h.Reasons) != 0 {
		t.Fatalf("health = %+v, want ok", h)
	}
}

// One re-warm is drift (degraded); consecutive re-warms mean the declared set
// does not fit (thrashing); a clean pass resets the streak.
func TestHealthRewarmStreak(t *testing.T) {
	cm := evictTestManager(t, 1<<30)
	cm.warmIntents = newWarmIntentRegistry()
	cm.warmIntents.register("vol-1", "/models/m", WarmupOptions{Mode: "full"})
	pass := func(warmed int) {
		cm.warmIntents.recordPass("vol-1", &WarmupResult{Files: 4, Warmed: warmed, AlreadyLocal: 4 - warmed}, nil, time.Now())
	}

	pass(4) // initial warm: not a re-warm
	if h := cm.Health(); h.State != "ok" {
		t.Fatalf("after initial warm: %+v", h)
	}
	pass(1)
	if h := cm.Health(); h.State != "degraded" || h.MaxRewarmStreak != 1 {
		t.Fatalf("after one re-warm: %+v", h)
	}
	pass(2)
	pass(1)
	if h := cm.Health(); h.State != "thrashing" || h.MaxRewarmStreak != thrashStreak {
		t.Fatalf("after %d re-warms: %+v", thrashStreak, h)
	}
	pass(0)
	if h := cm.Health(); h.State != "ok" || h.MaxRewarmStreak != 0 {
		t.Fatalf("clean pass did not reset: %+v", h)
	}
	if got := cm.WarmIntents()[0].RewarmStreak; got != 0 {
		t.Fatalf("WarmIntents RewarmStreak = %d", got)
	}
}

func TestHealthUnsatisfiedIntent(t *testing.T) {
	cm := evictTestManager(t, 1<<30)
	cm.warmIntents = newWarmIntentRegistry()
	cm.warmIntents.register("vol-1", "/a", WarmupOptions{Mode: "full"})
	cm.warmIntents.register("vol-2", "/b", WarmupOptions{Mode: "full"})
	cm.warmIntents.recordPass("vol-1", &WarmupResult{Files: 2, Skipped: 1, AlreadyLocal: 1}, nil, time.Now())
	cm.warmIntents.recordPass("vol-2", nil, errors.New("coordinator down"), time.Now())
	h := cm.Health()
	if h.State != "degraded" || h.Unsatisfied != 2 {
		t.Fatalf("health = %+v, want degraded with 2 unsatisfied", h)
	}
}

// Churning through over half the budget within the window, with intents
// declared, is thrashing even before a streak builds.
func TestHealthEvictionChurn(t *testing.T) {
	cm := evictTestManager(t, 1000)
	cm.warmIntents = newWarmIntentRegistry()
	cm.warmIntents.register("vol-1", "/a", WarmupOptions{Mode: "full"})
	now := time.Now()
	cm.evictBytes.add(300, now.Add(-15*time.Minute)) // outside the window
	cm.evictBytes.add(300, now.Add(-2*time.Minute))
	if h := cm.Health(); h.State == "thrashing" {
		t.Fatalf("thrashing at %d bytes in window: %+v", h.EvictedBytesWindow, h)
	}
	cm.evictBytes.add(300, now)
	h := cm.Health()
	if h.State != "thrashing" || h.EvictedBytesWindow != 600 {
		t.Fatalf("health = %+v, want thrashing with 600 bytes in window", h)
	}
}

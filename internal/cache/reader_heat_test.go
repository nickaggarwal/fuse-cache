package cache

import (
	"fmt"
	"testing"
	"time"
)

func TestReaderHeat_DistinctPrunesWindow(t *testing.T) {
	var h readerHeat
	now := time.Now()
	h.note("/m.bin", "10.0.0.1", now.Add(-20*time.Minute)) // outside window
	h.note("/m.bin", "10.0.0.2", now)
	h.note("/m.bin", "10.0.0.2", now) // duplicate reader counts once
	h.note("/m.bin", "10.0.0.3", now)

	if got := h.distinct("/m.bin", now.Add(-10*time.Minute)); got != 2 {
		t.Fatalf("distinct = %d, want 2 (stale + duplicate excluded)", got)
	}
	if got := h.distinct("/unknown.bin", now); got != 0 {
		t.Fatalf("distinct for unknown path = %d, want 0", got)
	}

	// sweep drops everything before the cutoff and empties the maps.
	h.sweep(now.Add(time.Minute))
	if got := h.distinct("/m.bin", now.Add(-10*time.Minute)); got != 0 {
		t.Fatalf("distinct after sweep = %d, want 0", got)
	}
	if len(h.byPath) != 0 {
		t.Fatalf("byPath not emptied after sweep: %d paths", len(h.byPath))
	}
}

// TestNoteRemoteReader_ChunkAccruesToParent: chunk reads count against the
// parent file, and the same host on different ports is one reader.
func TestNoteRemoteReader_ChunkAccruesToParent(t *testing.T) {
	cm := profileTestManager()
	cm.NoteRemoteReader("/model.bin_chunk_3", "10.0.0.9:52011")
	cm.NoteRemoteReader("/model.bin_chunk_7", "10.0.0.9:41822")
	cm.NoteRemoteReader("/model.bin", "10.0.0.8:1000")

	if got := cm.readerHeat.distinct("/model.bin", time.Now().Add(-time.Minute)); got != 2 {
		t.Fatalf("distinct readers = %d, want 2 (host-deduped, parent-accrued)", got)
	}
}

func TestReconcileTargetFor_HeatBoost(t *testing.T) {
	cm := profileTestManager()
	cm.config.ReplicaReconcileTarget = 3
	now := time.Now()

	if got := cm.reconcileTargetFor("/a.bin", now); got != 3 {
		t.Fatalf("no-heat target = %d, want base 3", got)
	}

	// 6 distinct readers => +3 replicas => 6.
	for i := 0; i < 6; i++ {
		cm.readerHeat.note("/a.bin", fmt.Sprintf("10.0.0.%d", i), now)
	}
	if got := cm.reconcileTargetFor("/a.bin", now); got != 6 {
		t.Fatalf("heat target = %d, want 6", got)
	}

	// 20 readers: capped at the default ceiling (8).
	for i := 0; i < 20; i++ {
		cm.readerHeat.note("/b.bin", fmt.Sprintf("10.1.0.%d", i), now)
	}
	if got := cm.reconcileTargetFor("/b.bin", now); got != 8 {
		t.Fatalf("capped target = %d, want 8", got)
	}

	// Configured ceiling wins.
	cm.config.ReplicaReconcileMaxTarget = 4
	if got := cm.reconcileTargetFor("/b.bin", now); got != 4 {
		t.Fatalf("config-capped target = %d, want 4", got)
	}

	// A base above the ceiling is always respected.
	cm.config.ReplicaReconcileTarget = 10
	if got := cm.reconcileTargetFor("/b.bin", now); got != 10 {
		t.Fatalf("base-above-cap target = %d, want 10", got)
	}

	if cm.HerdControlSnapshot().ReconcileHeatBoostsTotal == 0 {
		t.Fatal("heat boosts counter never incremented")
	}
}

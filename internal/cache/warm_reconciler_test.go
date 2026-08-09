package cache

// Tests for reconciled declarative warmup (warm_reconciler.go): a satisfied
// intent costs no data movement, an evicted file comes back on the next pass,
// a torn-down session stops being re-asserted, and the loop honors shutdown.
//
// Per the project convention, handoffs key off observed state (counters, the
// registry) rather than sleeping against real I/O — a wall-clock race against
// a fetch passes bare and fails under -race.

import (
	"context"
	"errors"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"fuse-client/internal/coordinator"
)

// flakyWarmCoordinator is warmupCoordinator with an injectable enumeration
// failure, so a reconcile pass can be made to fail the way a real one does
// (coordinator unreachable) without disturbing the registered intent.
type flakyWarmCoordinator struct {
	warmupCoordinator
	listErr atomic.Pointer[error]
}

func (c *flakyWarmCoordinator) ListFileLocations(ctx context.Context, prefix string) ([]*coordinator.FileLocation, error) {
	if p := c.listErr.Load(); p != nil {
		return nil, *p
	}
	return c.warmupCoordinator.ListFileLocations(ctx, prefix)
}

func (c *flakyWarmCoordinator) fail(err error) { c.listErr.Store(&err) }

// warmReconcileManager is evictTestManager plus the pieces the reconciler
// needs: an intent registry, a coordinator that enumerates locations, and a
// cloud tier that counts reads so "did this pass move bytes" is observable
// rather than inferred from timing.
func warmReconcileManager(t *testing.T, maxNVMe int64, locs []*coordinator.FileLocation) (*DefaultCacheManager, *countingStorage, *flakyWarmCoordinator) {
	t.Helper()
	cm := evictTestManager(t, maxNVMe)
	cloud := newCountingStorage()
	cm.cloudStorage = cloud
	cm.warmIntents = newWarmIntentRegistry()
	coord := &flakyWarmCoordinator{warmupCoordinator: warmupCoordinator{locations: locs}}
	cm.config.Coordinator = coord
	return cm, cloud, coord
}

func TestWarmReconcile_SatisfiedIntentMovesNoBytes(t *testing.T) {
	ctx := context.Background()
	cm, cloud, _ := warmReconcileManager(t, 1<<30, []*coordinator.FileLocation{
		{FilePath: "/models/a.bin", PeerID: "p1", StorageTier: "cloud", FileSize: 300},
		{FilePath: "/models/b.bin", PeerID: "p1", StorageTier: "cloud", FileSize: 300},
	})
	cloud.Write(ctx, "/models/a.bin", make([]byte, 300))
	cloud.Write(ctx, "/models/b.bin", make([]byte, 300))

	cm.warmIntents.register("vol-1", "/models", WarmupOptions{Mode: "full", Source: "cloud-only"})

	// First pass warms both files.
	cm.reconcileWarmOnce(ctx)
	cm.bgWg.Wait()
	if got := cm.herdStats.warmReconcileFilesWarmed.Load(); got != 2 {
		t.Fatalf("first pass warmed %d files, want 2", got)
	}

	// Second pass must find them local and read nothing from cloud. Reads are
	// the observable: warmOneFile stats the local path before touching a tier,
	// which is the property that makes steady-state reconciliation affordable.
	readsBefore := cloud.reads()
	cm.reconcileWarmOnce(ctx)
	cm.bgWg.Wait()
	if got := cloud.reads() - readsBefore; got != 0 {
		t.Fatalf("satisfied pass issued %d cloud reads, want 0", got)
	}
	if got := cm.herdStats.warmReconcileFilesWarmed.Load(); got != 2 {
		t.Fatalf("files_warmed = %d after satisfied pass, want it to stay 2", got)
	}
	if got := cm.herdStats.warmReconcileRuns.Load(); got != 2 {
		t.Fatalf("runs = %d, want 2", got)
	}

	st := cm.WarmIntents()
	if len(st) != 1 {
		t.Fatalf("WarmIntents returned %d entries, want 1", len(st))
	}
	if st[0].Passes != 2 || st[0].LastError != "" || st[0].LastVerified.IsZero() {
		t.Fatalf("intent status = %+v, want 2 clean passes with a verified time", st[0])
	}
	if st[0].LastResult == nil || st[0].LastResult.AlreadyLocal != 2 {
		t.Fatalf("last result = %+v, want 2 already-local", st[0].LastResult)
	}
}

func TestWarmReconcile_EvictedFileIsRefetched(t *testing.T) {
	ctx := context.Background()
	cm, cloud, _ := warmReconcileManager(t, 1<<30, []*coordinator.FileLocation{
		{FilePath: "/models/a.bin", PeerID: "p1", StorageTier: "cloud", FileSize: 300},
	})
	cloud.Write(ctx, "/models/a.bin", make([]byte, 300))
	cm.warmIntents.register("vol-1", "/models", WarmupOptions{Mode: "full", Source: "cloud-only"})

	cm.reconcileWarmOnce(ctx)
	cm.bgWg.Wait()
	local, ok := cm.LocalFilePath(ctx, "/models/a.bin")
	if !ok {
		t.Fatal("file not local after first pass")
	}

	// Simulate LRU reclaiming the warmed bytes out from under the intent —
	// the exact divergence a one-shot warmup can never notice.
	if err := os.Remove(local); err != nil {
		t.Fatalf("remove warmed file: %v", err)
	}
	if _, ok := cm.LocalFilePath(ctx, "/models/a.bin"); ok {
		t.Fatal("file still reported local after removal")
	}

	cm.reconcileWarmOnce(ctx)
	cm.bgWg.Wait()
	if _, ok := cm.LocalFilePath(ctx, "/models/a.bin"); !ok {
		t.Fatal("evicted file was not re-fetched by the next reconcile pass")
	}
	if got := cm.herdStats.warmReconcileFilesWarmed.Load(); got != 2 {
		t.Fatalf("files_warmed = %d, want 2 (initial warm + re-warm)", got)
	}
	if got := cm.herdStats.warmReconcileRewarms.Load(); got != 2 {
		t.Fatalf("rewarms = %d, want 2", got)
	}
}

func TestWarmReconcile_FailedPassDoesNotAdvanceLastVerified(t *testing.T) {
	ctx := context.Background()
	cm, cloud, coord := warmReconcileManager(t, 1<<30, []*coordinator.FileLocation{
		{FilePath: "/models/a.bin", PeerID: "p1", StorageTier: "cloud", FileSize: 300},
	})
	cloud.Write(ctx, "/models/a.bin", make([]byte, 300))
	cm.warmIntents.register("vol-1", "/models", WarmupOptions{Mode: "full", Source: "cloud-only"})

	cm.reconcileWarmOnce(ctx)
	cm.bgWg.Wait()
	verified := cm.WarmIntents()[0].LastVerified
	if verified.IsZero() {
		t.Fatal("clean pass did not set last_verified")
	}

	// Coordinator goes unreachable: the pass cannot enumerate, so residency is
	// unknown afterwards and the timestamp must stay where it was. A fresh
	// timestamp over unverified residency is worse than a stale honest one,
	// because a router acts on it.
	coord.fail(errors.New("coordinator unreachable"))
	cm.reconcileWarmOnce(ctx)
	cm.bgWg.Wait()

	st := cm.WarmIntents()[0]
	if st.LastError == "" {
		t.Fatal("failed pass recorded no error")
	}
	if !st.LastVerified.Equal(verified) {
		t.Fatalf("last_verified advanced to %v on a failed pass, want it pinned at %v", st.LastVerified, verified)
	}
	if st.Passes != 2 {
		t.Fatalf("passes = %d, want 2 (a failed pass still counts as attempted)", st.Passes)
	}
}

func TestWarmReconcile_ForgetStopsReassertion(t *testing.T) {
	ctx := context.Background()
	cm, cloud, _ := warmReconcileManager(t, 1<<30, []*coordinator.FileLocation{
		{FilePath: "/models/a.bin", PeerID: "p1", StorageTier: "cloud", FileSize: 300},
	})
	cloud.Write(ctx, "/models/a.bin", make([]byte, 300))

	cm.RegisterWarmIntent("vol-1", "/models", WarmupOptions{Mode: "full", Source: "cloud-only"})
	cm.bgWg.Wait() // RegisterWarmIntent runs the first pass in the background.
	if _, ok := cm.LocalFilePath(ctx, "/models/a.bin"); !ok {
		t.Fatal("RegisterWarmIntent did not run an immediate first pass")
	}
	if len(cm.WarmIntents()) != 1 {
		t.Fatalf("intent not registered")
	}

	cm.ForgetWarmIntent("vol-1")
	if got := len(cm.WarmIntents()); got != 0 {
		t.Fatalf("WarmIntents = %d after forget, want 0", got)
	}

	runsBefore := cm.herdStats.warmReconcileRuns.Load()
	cm.reconcileWarmOnce(ctx)
	if got := cm.herdStats.warmReconcileRuns.Load(); got != runsBefore {
		t.Fatal("reconcile pass ran with no registered intents")
	}
	// Warmed bytes deliberately survive teardown: they are valid cache, and
	// only the obligation to keep re-asserting them goes away.
	if _, ok := cm.LocalFilePath(ctx, "/models/a.bin"); !ok {
		t.Fatal("forget deleted warmed bytes; it should only drop the intent")
	}
}

func TestWarmIntentRegistry_RejectsNoOpIntents(t *testing.T) {
	r := newWarmIntentRegistry()
	for _, tc := range []struct {
		name, volumeID, prefix, mode string
	}{
		{"empty mode", "v", "/p", ""},
		{"none mode", "v", "/p", "none"},
		{"no volume", "", "/p", "full"},
		{"no prefix", "v", "", "full"},
	} {
		if r.register(tc.volumeID, tc.prefix, WarmupOptions{Mode: tc.mode}) {
			t.Errorf("%s: register accepted a no-op intent", tc.name)
		}
	}
	if got := len(r.snapshot()); got != 0 {
		t.Fatalf("registry holds %d intents, want 0", got)
	}
	if r.forget("v") {
		t.Fatal("forget reported dropping an intent that was never registered")
	}
}

func TestWarmReconcile_LoopStopsOnShutdown(t *testing.T) {
	cm, _, _ := warmReconcileManager(t, 1<<30, nil)
	ctx, cancel := context.WithCancel(context.Background())
	cm.config.WarmReconcileInterval = time.Millisecond
	cm.startWarmReconciler(ctx)
	cancel()

	done := make(chan struct{})
	go func() { cm.bgWg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("warm reconciler did not exit after context cancellation")
	}
}

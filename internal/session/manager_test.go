package session

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func tempFuseRoot(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	return dir
}

func TestCreateAndGet(t *testing.T) {
	root := tempFuseRoot(t)
	m := NewManager(root)
	ctx := context.Background()

	sess, err := m.Create(ctx, "vol-1", "/data/models", true, CachePolicy{
		CacheMode: "readonly",
		Pinned:    true,
	})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if sess.VolumeID != "vol-1" {
		t.Errorf("VolumeID = %q, want vol-1", sess.VolumeID)
	}
	if sess.RootPath != "/data/models" {
		t.Errorf("RootPath = %q, want /data/models", sess.RootPath)
	}
	expected := filepath.Join(root, "/data/models")
	if sess.HostPath != expected {
		t.Errorf("HostPath = %q, want %q", sess.HostPath, expected)
	}
	if sess.RefCount != 1 {
		t.Errorf("RefCount = %d, want 1", sess.RefCount)
	}
	if !sess.ReadOnly {
		t.Error("ReadOnly = false, want true")
	}

	got := m.Get(ctx, "vol-1")
	if got == nil {
		t.Fatal("Get returned nil")
	}
	if got.VolumeID != "vol-1" {
		t.Errorf("Get VolumeID = %q, want vol-1", got.VolumeID)
	}
}

func TestCreateIdempotent(t *testing.T) {
	root := tempFuseRoot(t)
	m := NewManager(root)
	ctx := context.Background()

	_, err := m.Create(ctx, "vol-1", "/data", false, CachePolicy{})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	sess, err := m.Create(ctx, "vol-1", "/data", false, CachePolicy{})
	if err != nil {
		t.Fatalf("Create (2nd): %v", err)
	}
	if sess.RefCount != 2 {
		t.Errorf("RefCount = %d, want 2", sess.RefCount)
	}
}

func TestDeleteDecrements(t *testing.T) {
	root := tempFuseRoot(t)
	m := NewManager(root)
	ctx := context.Background()

	m.Create(ctx, "vol-1", "/data", false, CachePolicy{})
	m.Create(ctx, "vol-1", "/data", false, CachePolicy{}) // refcount = 2

	removed, err := m.Delete(ctx, "vol-1")
	if err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if removed {
		t.Error("Delete returned removed=true, but refcount should be 1")
	}

	got := m.Get(ctx, "vol-1")
	if got == nil {
		t.Fatal("Session should still exist")
	}
	if got.RefCount != 1 {
		t.Errorf("RefCount = %d, want 1", got.RefCount)
	}
}

func TestDeleteRemoves(t *testing.T) {
	root := tempFuseRoot(t)
	m := NewManager(root)
	ctx := context.Background()

	m.Create(ctx, "vol-1", "/data", false, CachePolicy{})

	removed, err := m.Delete(ctx, "vol-1")
	if err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if !removed {
		t.Error("Delete returned removed=false, but refcount was 1")
	}

	if m.Get(ctx, "vol-1") != nil {
		t.Error("Session should be gone after delete")
	}
}

func TestDeleteIdempotent(t *testing.T) {
	root := tempFuseRoot(t)
	m := NewManager(root)
	ctx := context.Background()

	removed, err := m.Delete(ctx, "nonexistent")
	if err != nil {
		t.Fatalf("Delete nonexistent: %v", err)
	}
	if !removed {
		t.Error("Delete of nonexistent should return removed=true (idempotent)")
	}
}

func TestList(t *testing.T) {
	root := tempFuseRoot(t)
	m := NewManager(root)
	ctx := context.Background()

	m.Create(ctx, "vol-1", "/a", false, CachePolicy{})
	m.Create(ctx, "vol-2", "/b", true, CachePolicy{Pinned: true})

	sessions := m.List(ctx)
	if len(sessions) != 2 {
		t.Fatalf("List returned %d sessions, want 2", len(sessions))
	}
}

func TestPinnedPrefixes(t *testing.T) {
	root := tempFuseRoot(t)
	m := NewManager(root)
	ctx := context.Background()

	m.Create(ctx, "vol-1", "/a", false, CachePolicy{Pinned: true})
	m.Create(ctx, "vol-2", "/b", false, CachePolicy{Pinned: false})
	m.Create(ctx, "vol-3", "/c", false, CachePolicy{Pinned: true})

	prefixes := m.PinnedPrefixes()
	if len(prefixes) != 2 {
		t.Fatalf("PinnedPrefixes returned %d, want 2", len(prefixes))
	}
}

func TestPathCleaning(t *testing.T) {
	root := tempFuseRoot(t)
	m := NewManager(root)
	ctx := context.Background()

	sess, err := m.Create(ctx, "vol-1", "data/../data/models/", false, CachePolicy{})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if sess.RootPath != "/data/models" {
		t.Errorf("RootPath = %q, want /data/models", sess.RootPath)
	}
}

func TestEmptyVolumeID(t *testing.T) {
	root := tempFuseRoot(t)
	m := NewManager(root)
	ctx := context.Background()

	_, err := m.Create(ctx, "", "/data", false, CachePolicy{})
	if err == nil {
		t.Error("Expected error for empty volume_id")
	}
}

func TestFuseRootNotAccessible(t *testing.T) {
	m := NewManager("/nonexistent/path/that/does/not/exist")
	ctx := context.Background()

	_, err := m.Create(ctx, "vol-1", "/data", false, CachePolicy{})
	if err == nil {
		t.Error("Expected error when fuse root is not accessible")
	}
}

func TestGetReturnsCopy(t *testing.T) {
	root := tempFuseRoot(t)
	m := NewManager(root)
	ctx := context.Background()

	m.Create(ctx, "vol-1", "/data", false, CachePolicy{})

	got1 := m.Get(ctx, "vol-1")
	got2 := m.Get(ctx, "vol-1")

	// Modifying one copy should not affect the other.
	got1.RefCount = 999
	if got2.RefCount == 999 {
		t.Error("Get should return independent copies")
	}
}

func TestGetNonexistent(t *testing.T) {
	root := tempFuseRoot(t)
	m := NewManager(root)
	ctx := context.Background()

	if m.Get(ctx, "nonexistent") != nil {
		t.Error("Get for nonexistent should return nil")
	}
}

// TestCreateSubdir verifies that create works when the root path has nested
// subdirs (the FUSE mount might create them lazily).
func TestCreateSubdir(t *testing.T) {
	root := tempFuseRoot(t)
	// Create a subdirectory to simulate existing FUSE content.
	subdir := filepath.Join(root, "models", "llama")
	if err := os.MkdirAll(subdir, 0755); err != nil {
		t.Fatal(err)
	}

	m := NewManager(root)
	ctx := context.Background()

	sess, err := m.Create(ctx, "vol-1", "/models/llama", true, CachePolicy{
		CacheMode: "readonly",
		Warmup:    "full",
		Pinned:    true,
	})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if sess.HostPath != filepath.Join(root, "models/llama") {
		t.Errorf("HostPath = %q, want %q", sess.HostPath, filepath.Join(root, "models/llama"))
	}
}

// TestManager_WarmupHookFiresOncePerVolume: the hook fires on first creation
// of a warmup-enabled session, not on refcount bumps or warmup=none sessions.
func TestManager_WarmupHookFiresOncePerVolume(t *testing.T) {
	m := NewManager(t.TempDir())
	fired := make(chan WarmupRequest, 4)
	m.SetWarmupHook(func(r WarmupRequest) { fired <- r })

	ctx := context.Background()
	policy := CachePolicy{Warmup: "full", Pinned: true}
	if _, err := m.Create(ctx, "vol-1", "/models/llama", true, policy); err != nil {
		t.Fatalf("create: %v", err)
	}
	select {
	case req := <-fired:
		if req.VolumeID != "vol-1" || req.RootPath != "/models/llama" || req.Mode != "full" {
			t.Fatalf("hook got %+v", req)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("warmup hook never fired")
	}

	// Refcount bump and warmup=none sessions must not fire.
	if _, err := m.Create(ctx, "vol-1", "/models/llama", true, policy); err != nil {
		t.Fatalf("refcount create: %v", err)
	}
	if _, err := m.Create(ctx, "vol-2", "/scratch", false, CachePolicy{Warmup: "none"}); err != nil {
		t.Fatalf("create vol-2: %v", err)
	}
	select {
	case req := <-fired:
		t.Fatalf("unexpected extra hook firing: %+v", req)
	case <-time.After(100 * time.Millisecond):
	}
}

// TestManager_TeardownHookFiresAtRefcountZero: the teardown hook is the signal
// that a declared cache obligation is discharged, so it must fire exactly when
// the session actually goes away — not on an intermediate unmount, and not
// again on a repeat delete of an already-gone volume.
func TestManager_TeardownHookFiresAtRefcountZero(t *testing.T) {
	m := NewManager(t.TempDir())
	var torn []string
	m.SetTeardownHook(func(volumeID string) { torn = append(torn, volumeID) })

	ctx := context.Background()
	for i := 0; i < 2; i++ {
		if _, err := m.Create(ctx, "vol-1", "/models/llama", true, CachePolicy{Warmup: "full"}); err != nil {
			t.Fatalf("create %d: %v", i, err)
		}
	}

	if removed, err := m.Delete(ctx, "vol-1"); err != nil || removed {
		t.Fatalf("first delete: removed=%v err=%v, want removed=false", removed, err)
	}
	if len(torn) != 0 {
		t.Fatalf("teardown hook fired at refcount 1: %v", torn)
	}

	if removed, err := m.Delete(ctx, "vol-1"); err != nil || !removed {
		t.Fatalf("second delete: removed=%v err=%v, want removed=true", removed, err)
	}
	if len(torn) != 1 || torn[0] != "vol-1" {
		t.Fatalf("teardown fired %v, want exactly [vol-1]", torn)
	}

	// Delete is idempotent; a repeat must not re-fire the hook.
	if _, err := m.Delete(ctx, "vol-1"); err != nil {
		t.Fatalf("idempotent delete: %v", err)
	}
	if len(torn) != 1 {
		t.Fatalf("teardown fired %v on a repeat delete, want it to stay [vol-1]", torn)
	}
}

func withBootID(t *testing.T, id string) {
	t.Helper()
	prev := bootID
	bootID = func() string { return id }
	t.Cleanup(func() { bootID = prev })
}

// A restarted daemon reloads its sessions, pins and refcounts included, so
// the unpublish that follows the restart releases them through the normal
// path and fires the teardown hook at zero.
func TestPersistenceSurvivesRestart(t *testing.T) {
	withBootID(t, "boot-1")
	root := tempFuseRoot(t)
	state := filepath.Join(t.TempDir(), "state", "sessions.json")
	ctx := context.Background()

	prev := NewManager(root)
	if _, err := prev.EnablePersistence(state); err != nil {
		t.Fatal(err)
	}
	pol := CachePolicy{Pinned: true, Warmup: "full", SourcePolicy: "peer-first"}
	for i := 0; i < 2; i++ {
		if _, err := prev.Create(ctx, "vol-1", "/models/m", true, pol); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := prev.Create(ctx, "vol-2", "/models/other", true, CachePolicy{}); err != nil {
		t.Fatal(err)
	}
	if _, err := prev.Delete(ctx, "vol-2"); err != nil {
		t.Fatal(err)
	}

	next := NewManager(root)
	var torn []string
	next.SetTeardownHook(func(v string) { torn = append(torn, v) })
	warmed := make(chan WarmupRequest, 4)
	next.SetWarmupHook(func(r WarmupRequest) { warmed <- r })
	n, err := next.EnablePersistence(state)
	if err != nil || n != 1 {
		t.Fatalf("restored %d sessions (err %v), want 1", n, err)
	}
	if got := next.PinnedPrefixes(); len(got) != 1 || got[0] != "/models/m" {
		t.Fatalf("pins after restart = %v, want [/models/m]", got)
	}
	if s := next.Get(ctx, "vol-1"); s == nil || s.RefCount != 2 {
		t.Fatalf("restored session = %+v, want refcount 2", s)
	}

	next.ReplayWarmups()
	select {
	case r := <-warmed:
		if r.VolumeID != "vol-1" || r.Mode != "full" || r.RootPath != "/models/m" {
			t.Fatalf("replayed warmup = %+v", r)
		}
	case <-time.After(time.Second):
		t.Fatal("restored warmup was not replayed")
	}

	for i := 0; i < 2; i++ {
		if _, err := next.Delete(ctx, "vol-1"); err != nil {
			t.Fatal(err)
		}
	}
	if len(torn) != 1 || torn[0] != "vol-1" {
		t.Fatalf("teardown fired %v, want [vol-1]", torn)
	}

	// And the release itself was persisted.
	again := NewManager(root)
	if n, _ := again.EnablePersistence(state); n != 0 {
		t.Fatalf("restored %d sessions after full release, want 0", n)
	}
}

// After a reboot every bind mount is gone and kubelet publishes again, so a
// table from the previous boot must not be restored (it would double-count).
func TestPersistenceDiscardedAcrossReboot(t *testing.T) {
	withBootID(t, "boot-1")
	root := tempFuseRoot(t)
	state := filepath.Join(t.TempDir(), "sessions.json")
	ctx := context.Background()

	prev := NewManager(root)
	if _, err := prev.EnablePersistence(state); err != nil {
		t.Fatal(err)
	}
	if _, err := prev.Create(ctx, "vol-1", "/m", true, CachePolicy{Pinned: true}); err != nil {
		t.Fatal(err)
	}

	withBootID(t, "boot-2")
	next := NewManager(root)
	if n, _ := next.EnablePersistence(state); n != 0 {
		t.Fatalf("restored %d sessions from a previous boot, want 0", n)
	}
	if got := next.PinnedPrefixes(); len(got) != 0 {
		t.Fatalf("pins from a previous boot survived: %v", got)
	}
}

// A corrupt state file starts empty rather than failing the daemon.
func TestPersistenceCorruptFile(t *testing.T) {
	state := filepath.Join(t.TempDir(), "sessions.json")
	if err := os.WriteFile(state, []byte("{not json"), 0o644); err != nil {
		t.Fatal(err)
	}
	m := NewManager(tempFuseRoot(t))
	if n, err := m.EnablePersistence(state); n != 0 || err != nil {
		t.Fatalf("corrupt state: n=%d err=%v, want 0, nil", n, err)
	}
}

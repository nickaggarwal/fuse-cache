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

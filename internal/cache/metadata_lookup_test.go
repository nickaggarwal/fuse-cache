package cache

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"fuse-client/internal/coordinator"
)

// countingCoordinator serves a fixed location set and records how often the
// list endpoint was hit, so tests can assert the cached view is honored.
type countingCoordinator struct {
	warmupCoordinator
	calls    int
	prefixes []string
	points   int
	err      error
}

func (c *countingCoordinator) ListFileLocations(ctx context.Context, prefix string) ([]*coordinator.FileLocation, error) {
	c.calls++
	c.prefixes = append(c.prefixes, prefix)
	if c.err != nil {
		return nil, c.err
	}
	return c.warmupCoordinator.ListFileLocations(ctx, prefix)
}

func (c *countingCoordinator) GetFileLocation(_ context.Context, path string) ([]*coordinator.FileLocation, error) {
	c.points++
	if c.err != nil {
		return nil, c.err
	}
	for _, loc := range c.locations {
		if loc.FilePath == path {
			return []*coordinator.FileLocation{loc}, nil
		}
	}
	return []*coordinator.FileLocation{}, nil
}

// fullLists counts list calls with an empty prefix: downloads of the entire
// cluster's file table.
func (c *countingCoordinator) fullLists() int {
	n := 0
	for _, p := range c.prefixes {
		if p == "" {
			n++
		}
	}
	return n
}

func lookupTestManager(t *testing.T, coord Coordinator) *DefaultCacheManager {
	t.Helper()
	cm := newTestCacheManager()
	cm.config.Coordinator = coord
	cm.config.MetadataRefreshTTL = time.Minute
	return cm
}

func TestLookupEntry_LocalBeatsRemote(t *testing.T) {
	coord := &countingCoordinator{warmupCoordinator: warmupCoordinator{
		locations: []*coordinator.FileLocation{
			{FilePath: "/a.bin", StorageTier: "cloud", FileSize: 99},
		},
	}}
	cm := lookupTestManager(t, coord)
	cm.entries.put(&CacheEntry{FilePath: "/a.bin", Size: 7, Tier: TierNVMe})

	entry, ok := cm.LookupEntry(context.Background(), "/a.bin", false)
	if !ok || entry.Size != 7 || entry.Tier != TierNVMe {
		t.Fatalf("got %+v, want the local NVMe entry", entry)
	}
	// A local hit must not cost a coordinator round-trip — this is the
	// per-syscall path.
	if coord.calls != 0 {
		t.Fatalf("local hit made %d coordinator calls, want 0", coord.calls)
	}
}

func TestLookupEntry_FallsBackToRemote(t *testing.T) {
	coord := &countingCoordinator{warmupCoordinator: warmupCoordinator{
		locations: []*coordinator.FileLocation{
			{FilePath: "/peer-only.bin", StorageTier: "peer", FileSize: 42},
		},
	}}
	cm := lookupTestManager(t, coord)
	ctx := context.Background()

	entry, ok := cm.LookupEntry(ctx, "/peer-only.bin", false)
	if !ok || entry.Size != 42 || entry.Tier != TierPeer {
		t.Fatalf("got %+v ok=%v, want the remote entry", entry, ok)
	}
	if coord.calls != 1 {
		t.Fatalf("coordinator calls = %d, want 1 (cold view)", coord.calls)
	}

	// Second lookup rides the cached view within the TTL.
	if _, ok := cm.LookupEntry(ctx, "/peer-only.bin", false); !ok {
		t.Fatal("second lookup missed")
	}
	if coord.calls != 1 {
		t.Fatalf("coordinator calls = %d, want the cached view reused", coord.calls)
	}

	if _, ok := cm.LookupEntry(ctx, "/nowhere.bin", false); ok {
		t.Fatal("unknown path resolved")
	}
}

// Coordinator outages must degrade to the last good view, never to ENOENT for
// a path that view still knows: all coordination here is advisory.
func TestLookupEntry_CoordinatorFailureKeepsLastView(t *testing.T) {
	coord := &countingCoordinator{warmupCoordinator: warmupCoordinator{
		locations: []*coordinator.FileLocation{
			{FilePath: "/remote.bin", StorageTier: "cloud", FileSize: 5},
		},
	}}
	cm := lookupTestManager(t, coord)
	ctx := context.Background()

	if _, ok := cm.LookupEntry(ctx, "/remote.bin", false); !ok {
		t.Fatal("warm-up lookup missed")
	}
	coord.err = errors.New("coordinator down")

	if _, ok := cm.LookupEntry(ctx, "/remote.bin", true); !ok {
		t.Fatal("forced refresh during an outage dropped a known path")
	}
}

func TestReadDir_MergesLocalAndRemote(t *testing.T) {
	coord := &countingCoordinator{warmupCoordinator: warmupCoordinator{
		locations: []*coordinator.FileLocation{
			{FilePath: "/data/remote.bin", StorageTier: "cloud", FileSize: 1},
			{FilePath: "/data/sub/deep.bin", StorageTier: "cloud", FileSize: 1},
			{FilePath: "/elsewhere/x.bin", StorageTier: "cloud", FileSize: 1},
		},
	}}
	cm := lookupTestManager(t, coord)
	cm.entries.put(&CacheEntry{FilePath: "/data/local.bin", Size: 1, Tier: TierNVMe})

	got, err := cm.ReadDir(context.Background(), "/data")
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	want := []DirEntry{
		{Name: "local.bin"},
		{Name: "remote.bin"},
		{Name: "sub", IsDir: true},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("ReadDir = %v, want %v", got, want)
	}
}

// Chunks are an internal representation of one file, not siblings of it.
// Listing them beside their parent made an 8MB-strided file look like
// thousands of files through the mount, and made readdir cost scale with
// chunk count. They are hidden whether they come from local state or from
// coordinator-published rows.
func TestReadDir_HidesChunks(t *testing.T) {
	coord := &countingCoordinator{warmupCoordinator: warmupCoordinator{
		locations: []*coordinator.FileLocation{
			{FilePath: "/remote.bin", StorageTier: "cloud", FileSize: 1},
			{FilePath: "/remote.bin_chunk_0", StorageTier: "cloud", FileSize: 1},
		},
	}}
	cm := lookupTestManager(t, coord)
	cm.entries.put(&CacheEntry{FilePath: "/big.bin", Size: 100, Tier: TierNVMe, IsChunked: true, NumChunks: 2})
	cm.entries.put(&CacheEntry{FilePath: "/big.bin_chunk_0", Size: 50, Tier: TierNVMe})
	cm.entries.put(&CacheEntry{FilePath: "/big.bin_chunk_1", Size: 50, Tier: TierNVMe})

	got, err := cm.ReadDir(context.Background(), "/")
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	want := []DirEntry{{Name: "big.bin"}, {Name: "remote.bin"}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("ReadDir = %v, want %v", got, want)
	}
}

func TestDirExists(t *testing.T) {
	coord := &countingCoordinator{warmupCoordinator: warmupCoordinator{
		locations: []*coordinator.FileLocation{
			{FilePath: "/remote-dir/x.bin", StorageTier: "cloud", FileSize: 1},
		},
	}}
	cm := lookupTestManager(t, coord)
	cm.entries.put(&CacheEntry{FilePath: "/local-dir/y.bin", Size: 1, Tier: TierNVMe})
	ctx := context.Background()

	if !cm.DirExists(ctx, "/") {
		t.Fatal("root must always exist")
	}
	if !cm.DirExists(ctx, "/local-dir") {
		t.Fatal("local directory not found")
	}
	// Populate the remote view, then check a directory only peers hold.
	cm.LookupEntry(ctx, "/remote-dir/x.bin", true)
	if !cm.DirExists(ctx, "/remote-dir") {
		t.Fatal("remote-only directory not found")
	}
	if cm.DirExists(ctx, "/no-such-dir") {
		t.Fatal("nonexistent directory reported present")
	}
}

// A forced refresh of one path must never download the whole file table.
// Every Create checks existence with forceRefresh=true, so a full list here
// turns a burst of new files into concurrent downloads of the entire
// cluster's file table; on 2026-10-10 that OOM-killed every coordinator
// replica. A miss may still list that path's own subtree (it could be a
// directory held only by peers), which for a not-yet-created name is empty.
func TestLookupEntry_ForcedRefreshNeverListsWholeTable(t *testing.T) {
	coord := &countingCoordinator{warmupCoordinator: warmupCoordinator{
		locations: []*coordinator.FileLocation{
			{FilePath: "/kv/abc", StorageTier: "peer", FileSize: 11},
			{FilePath: "/kv/dir/child", StorageTier: "peer", FileSize: 3},
		},
	}}
	cm := lookupTestManager(t, coord)
	ctx := context.Background()

	if _, ok := cm.LookupEntry(ctx, "/kv/new", true); ok {
		t.Fatal("absent path resolved")
	}
	entry, ok := cm.LookupEntry(ctx, "/kv/abc", true)
	if !ok || entry.Size != 11 || entry.Tier != TierPeer {
		t.Fatalf("got %+v ok=%v, want the remote entry", entry, ok)
	}
	// A remote-only directory is still discoverable right after a forced miss.
	if _, ok := cm.LookupEntry(ctx, "/kv/dir", true); ok {
		t.Fatal("directory resolved as a file")
	}
	if !cm.DirExists(ctx, "/kv/dir") {
		t.Fatal("remote-only directory not visible after forced lookup")
	}
	if n := coord.fullLists(); n != 0 {
		t.Fatalf("forced lookups made %d full-table lists (prefixes %q), want 0", n, coord.prefixes)
	}
	if coord.points != 3 {
		t.Fatalf("point lookups = %d, want 3", coord.points)
	}
}

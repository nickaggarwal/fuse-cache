package cache

import (
	"reflect"
	"testing"
)

// mustEntry returns the live entry for path, failing the test if absent.
// Tests that reach into the entry table for a single field used to index the
// map directly; this keeps that terse while the table owns the lookup.
func mustEntry(t *testing.T, cm *DefaultCacheManager, path string) *CacheEntry {
	t.Helper()
	entry, ok := cm.entries.get(path)
	if !ok || entry == nil {
		t.Fatalf("no cache entry for %s", path)
	}
	return entry
}

func TestEntryTableChunkIndex(t *testing.T) {
	tbl := newEntryTable()
	// Interleave two parents so a prefix scan and the index disagree if the
	// index is wrong.
	for _, p := range []string{
		"/a.bin", "/a.bin_chunk_0", "/a.bin_chunk_1", "/a.bin_chunk_10",
		"/b.bin", "/b.bin_chunk_0",
	} {
		tbl.put(&CacheEntry{FilePath: p})
	}

	got := tbl.chunkKeys("/a.bin")
	want := []string{"/a.bin_chunk_0", "/a.bin_chunk_1", "/a.bin_chunk_10"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("chunkKeys = %v, want %v (ascending index, not lexical)", got, want)
	}

	tbl.del("/a.bin_chunk_1")
	if got := tbl.chunkKeys("/a.bin"); len(got) != 2 {
		t.Fatalf("after delete chunkKeys = %v, want 2 entries", got)
	}
	tbl.del("/a.bin_chunk_0")
	tbl.del("/a.bin_chunk_10")
	if got := tbl.chunkKeys("/a.bin"); got != nil {
		t.Fatalf("empty chunk set should return nil, got %v", got)
	}
	if _, leaked := tbl.chunks["/a.bin"]; leaked {
		t.Fatal("empty chunk set left behind in index")
	}
	if got := tbl.chunkKeys("/b.bin"); len(got) != 1 {
		t.Fatalf("sibling parent chunks disturbed: %v", got)
	}
}

// A file whose name merely contains "_chunk_" without a numeric suffix is a
// user file, not a chunk. The prefix scan this index replaced would have
// deleted it along with its apparent parent.
func TestEntryTableChunkIndexIgnoresNonNumericSuffix(t *testing.T) {
	tbl := newEntryTable()
	tbl.put(&CacheEntry{FilePath: "/report"})
	tbl.put(&CacheEntry{FilePath: "/report_chunk_draft"})
	tbl.put(&CacheEntry{FilePath: "/report_chunk_2"})

	got := tbl.chunkKeys("/report")
	if !reflect.DeepEqual(got, []string{"/report_chunk_2"}) {
		t.Fatalf("chunkKeys = %v, want only the numeric chunk", got)
	}
}

func TestEntryTableDirIndex(t *testing.T) {
	tbl := newEntryTable()
	for _, p := range []string{"/top.txt", "/dir/a.txt", "/dir/b.txt", "/dir/sub/c.txt"} {
		tbl.put(&CacheEntry{FilePath: p})
	}

	root := tbl.children("/")
	want := []DirEntry{{Name: "dir", IsDir: true}, {Name: "top.txt"}}
	if !reflect.DeepEqual(root, want) {
		t.Fatalf("children(/) = %v, want %v", root, want)
	}

	dir := tbl.children("/dir")
	wantDir := []DirEntry{{Name: "a.txt"}, {Name: "b.txt"}, {Name: "sub", IsDir: true}}
	if !reflect.DeepEqual(dir, wantDir) {
		t.Fatalf("children(/dir) = %v, want %v", dir, wantDir)
	}

	// Trailing slash and unrooted forms address the same directory.
	if !reflect.DeepEqual(tbl.children("/dir/"), wantDir) {
		t.Fatal("trailing slash changed the directory key")
	}
	if !tbl.hasChildren("/dir/sub") {
		t.Fatal("intermediate directory not visible")
	}
}

// Directories are refcounted, not derived: they must survive until their last
// descendant is gone and disappear exactly then.
func TestEntryTableDirRefcount(t *testing.T) {
	tbl := newEntryTable()
	tbl.put(&CacheEntry{FilePath: "/dir/a.txt"})
	tbl.put(&CacheEntry{FilePath: "/dir/b.txt"})

	tbl.del("/dir/a.txt")
	if !tbl.hasChildren("/dir") {
		t.Fatal("/dir vanished while /dir/b.txt still exists")
	}
	tbl.del("/dir/b.txt")
	if tbl.hasChildren("/dir") {
		t.Fatal("/dir outlived its last child")
	}
	if len(tbl.children("/")) != 0 {
		t.Fatalf("root still lists %v", tbl.children("/"))
	}
	if len(tbl.dirs) != 0 {
		t.Fatalf("dirs index leaked: %v", tbl.dirs)
	}
}

// A name can be both a file and a directory prefix ("/a" and "/a/b"). Lookup
// resolved files before directories before the index existed, so the file
// must keep winning, and removing it must reveal the directory.
func TestEntryTableFileAndDirSameName(t *testing.T) {
	tbl := newEntryTable()
	tbl.put(&CacheEntry{FilePath: "/a"})
	tbl.put(&CacheEntry{FilePath: "/a/b"})

	root := tbl.children("/")
	if len(root) != 1 || root[0].Name != "a" || root[0].IsDir {
		t.Fatalf("children(/) = %v, want a single file-kind child 'a'", root)
	}

	tbl.del("/a")
	root = tbl.children("/")
	if len(root) != 1 || !root[0].IsDir {
		t.Fatalf("after deleting the file, children(/) = %v, want 'a' as a dir", root)
	}
}

// put on an existing key must not double-count the index references.
func TestEntryTablePutIsIdempotent(t *testing.T) {
	tbl := newEntryTable()
	tbl.put(&CacheEntry{FilePath: "/dir/a_chunk_0", Size: 1})
	tbl.put(&CacheEntry{FilePath: "/dir/a_chunk_0", Size: 2})

	if got := tbl.len(); got != 1 {
		t.Fatalf("len = %d, want 1", got)
	}
	entry, _ := tbl.get("/dir/a_chunk_0")
	if entry.Size != 2 {
		t.Fatalf("put did not replace the entry: size %d", entry.Size)
	}
	tbl.del("/dir/a_chunk_0")
	if len(tbl.chunkKeys("/dir/a")) != 0 {
		t.Fatal("repeated put leaked a chunk index reference")
	}

	tbl.put(&CacheEntry{FilePath: "/dir/plain.bin"})
	tbl.put(&CacheEntry{FilePath: "/dir/plain.bin"})
	tbl.del("/dir/plain.bin")
	if tbl.hasChildren("/dir") {
		t.Fatal("repeated put leaked a directory index reference")
	}
}

// Chunks must not appear as directory children: they are one file's internal
// representation, and indexing them would put readdir cost back on a
// per-chunk footing.
func TestEntryTableChunksAreNotDirChildren(t *testing.T) {
	tbl := newEntryTable()
	tbl.put(&CacheEntry{FilePath: "/dir/big.bin"})
	tbl.put(&CacheEntry{FilePath: "/dir/big.bin_chunk_0"})
	tbl.put(&CacheEntry{FilePath: "/dir/big.bin_chunk_1"})

	got := tbl.children("/dir")
	if !reflect.DeepEqual(got, []DirEntry{{Name: "big.bin"}}) {
		t.Fatalf("children(/dir) = %v, want only the parent file", got)
	}
	// The chunks are still addressable by point lookup and by index.
	if _, ok := tbl.get("/dir/big.bin_chunk_1"); !ok {
		t.Fatal("chunk not retrievable by path")
	}
	if len(tbl.chunkKeys("/dir/big.bin")) != 2 {
		t.Fatal("chunk index lost entries")
	}
}

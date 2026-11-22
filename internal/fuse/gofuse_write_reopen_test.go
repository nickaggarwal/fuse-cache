package fuse

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"testing"
	"time"

	gfuse "github.com/hanwen/go-fuse/v2/fuse"

	"fuse-client/internal/cache"
)

// stagedStore is the slice of DefaultCacheManager the go-fuse write path
// uses: staged write files that PutFromFile *moves* into the cache, plus the
// two ways a node can read committed bytes back (local file, range read).
type stagedStore struct {
	mu        sync.Mutex
	dir       string
	files     map[string]string // fuse path -> committed local file
	noLocal   bool              // hide LocalFilePath (evicted/remote-only)
	noRange   bool              // hide ReadRange too
	putSizes  []int64
	putCounts int
}

func newStagedStore(t *testing.T) *stagedStore {
	return &stagedStore{dir: t.TempDir(), files: map[string]string{}}
}

func (s *stagedStore) CreateWriteFile(_ context.Context, _ string) (*os.File, string, error) {
	f, err := os.CreateTemp(s.dir, "stage-*")
	if err != nil {
		return nil, "", err
	}
	return f, f.Name(), nil
}

func (s *stagedStore) PutFromFile(_ context.Context, filePath, stagedPath string, size int64, _ time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	final := filepath.Join(s.dir, "committed", filepath.Base(filePath))
	if err := os.MkdirAll(filepath.Dir(final), 0o755); err != nil {
		return err
	}
	// Same contract as DefaultCacheManager.putFromFile: the staged file is
	// renamed into the cache, so it no longer exists afterwards.
	if err := os.Rename(stagedPath, final); err != nil {
		return err
	}
	s.files[filePath] = final
	s.putSizes = append(s.putSizes, size)
	s.putCounts++
	return nil
}

func (s *stagedStore) LocalFilePath(_ context.Context, filePath string) (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.noLocal {
		return "", false
	}
	p, ok := s.files[filePath]
	return p, ok
}

func (s *stagedStore) ReadRange(_ context.Context, filePath string, off int64, size int) ([]byte, error) {
	s.mu.Lock()
	p, ok := s.files[filePath]
	noRange := s.noRange
	s.mu.Unlock()
	if noRange || !ok {
		return nil, cache.ErrNotFound
	}
	f, err := os.Open(p)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	buf := make([]byte, size)
	n, err := f.ReadAt(buf, off)
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, err
	}
	return buf[:n], nil
}

func (s *stagedStore) committed(t *testing.T, filePath string) []byte {
	t.Helper()
	s.mu.Lock()
	p, ok := s.files[filePath]
	s.mu.Unlock()
	if !ok {
		t.Fatalf("%s was never committed", filePath)
	}
	b, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// CacheManager boilerplate; the write path under test never calls these.
func (s *stagedStore) Get(context.Context, string) (*cache.CacheEntry, error) {
	return nil, cache.ErrNotFound
}
func (s *stagedStore) Put(context.Context, *cache.CacheEntry) error      { return nil }
func (s *stagedStore) Delete(context.Context, string) error              { return nil }
func (s *stagedStore) List(context.Context) ([]*cache.CacheEntry, error) { return nil, nil }
func (s *stagedStore) Evict(context.Context, cache.CacheTier) error      { return nil }
func (s *stagedStore) Stats() (int64, int64)                             { return 0, 0 }
func (s *stagedStore) WriteTo(context.Context, string, io.Writer) (int64, error) {
	return 0, nil
}
func (s *stagedStore) Shutdown() {}

// rangeOnly hides LocalFilePath from the type assertion entirely, the way a
// cache manager without a local tier would.
type rangeOnly struct{ *stagedStore }

func (rangeOnly) LocalFilePath() {} // shadows the resolver method: wrong signature

func newWriteNode(cm cache.CacheManager, path string) *goFuseNode {
	return &goFuseNode{
		gfs:   NewGoFuseFileSystem(cm),
		path:  path,
		kind:  goFuseNodeFile,
		entry: &cache.CacheEntry{FilePath: path, StoragePath: path, LastAccessed: time.Now()},
		isNew: true,
	}
}

func pattern(n int, seed byte) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = seed + byte(i%251)
	}
	return b
}

func mustWrite(t *testing.T, n *goFuseNode, data []byte, off int64) {
	t.Helper()
	if got, errno := n.Write(context.Background(), nil, data, off); errno != 0 || int(got) != len(data) {
		t.Fatalf("Write(%d bytes @%d) = %d, %v", len(data), off, got, errno)
	}
}

func mustFlush(t *testing.T, n *goFuseNode) {
	t.Helper()
	if errno := n.Flush(context.Background(), nil); errno != 0 {
		t.Fatalf("Flush = %v", errno)
	}
}

// The kernel sends FUSE_FLUSH on every close() of a descriptor that shares
// the open file -- a dup, or a forked child exiting -- while the file stays
// open for writing through the others. Flush commits the staged bytes with
// PutFromFile, which moves the staging file into the cache. A write after
// that used to open a brand-new, empty staging file and write at the
// original offset, so the next Flush committed a file of the right size
// whose earlier bytes were a hole of zeros. Seen on Nebius with a CRIU dump:
// checkpoint/files.img was flushed at 40,538 bytes, extended by 1,053, and
// read back as 41,591 bytes of zeros.
func TestWriteAfterFlushKeepsEarlierBytes(t *testing.T) {
	store := newStagedStore(t)
	n := newWriteNode(store, "/ckpt/checkpoint/files.img")

	first, second := pattern(40538, 1), pattern(1053, 7)
	mustWrite(t, n, first, 0)
	mustFlush(t, n) // close() of a dup'd fd: file stays open
	mustWrite(t, n, second, int64(len(first)))
	mustFlush(t, n)

	got := store.committed(t, n.path)
	want := append(append([]byte{}, first...), second...)
	if !bytes.Equal(got, want) {
		zeros := 0
		for _, c := range got[:len(first)] {
			if c == 0 {
				zeros++
			}
		}
		t.Fatalf("committed %d bytes, want %d; first %d bytes hold %d zeros (earlier write lost)",
			len(got), len(want), len(first), zeros)
	}
}

// A rewrite inside the already-flushed region must land on top of the
// earlier bytes, not on top of zeros.
func TestOverwriteAfterFlushPatchesInPlace(t *testing.T) {
	store := newStagedStore(t)
	n := newWriteNode(store, "/ckpt/pstree.img")

	base := pattern(8192, 3)
	mustWrite(t, n, base, 0)
	mustFlush(t, n)
	patch := []byte("PATCH")
	mustWrite(t, n, patch, 100)
	mustFlush(t, n)

	want := append([]byte{}, base...)
	copy(want[100:], patch)
	if got := store.committed(t, n.path); !bytes.Equal(got, want) {
		t.Fatalf("overwrite after flush corrupted the file (len %d, want %d)", len(got), len(want))
	}
}

// With no local copy the earlier bytes come back through ReadRange.
func TestWriteAfterFlushSeedsFromRangeReader(t *testing.T) {
	store := newStagedStore(t)
	cm := rangeOnly{store}
	n := newWriteNode(cm, "/ckpt/remote.img")

	first, second := pattern(5000, 9), pattern(300, 2)
	mustWrite(t, n, first, 0)
	mustFlush(t, n)
	mustWrite(t, n, second, int64(len(first)))
	mustFlush(t, n)

	want := append(append([]byte{}, first...), second...)
	if got := store.committed(t, n.path); !bytes.Equal(got, want) {
		t.Fatalf("range-reader seed lost data (len %d, want %d)", len(got), len(want))
	}
}

// If the earlier bytes cannot be read back, the write must fail. Committing
// a hole would be silent corruption; EIO is a failure the writer sees.
func TestWriteAfterFlushFailsWhenEarlierBytesUnreadable(t *testing.T) {
	store := newStagedStore(t)
	n := newWriteNode(store, "/ckpt/lost.img")

	mustWrite(t, n, pattern(4096, 5), 0)
	mustFlush(t, n)
	store.mu.Lock()
	store.noLocal, store.noRange = true, true
	store.mu.Unlock()

	if _, errno := n.Write(context.Background(), nil, []byte("more"), 4096); errno != syscall.EIO {
		t.Fatalf("Write after flush with unreadable earlier bytes = %v, want EIO", errno)
	}
	if store.putCounts != 1 {
		t.Fatalf("committed %d times; the failed write must not commit a holed file", store.putCounts)
	}
}

// Truncating to zero after a flush starts over: no seed copy, and the
// committed file holds only what was written after the truncate.
func TestTruncateToZeroAfterFlushSkipsSeed(t *testing.T) {
	store := newStagedStore(t)
	n := newWriteNode(store, "/ckpt/rewrite.img")

	mustWrite(t, n, pattern(4096, 4), 0)
	mustFlush(t, n)
	store.mu.Lock()
	store.noLocal, store.noRange = true, true // a seed attempt would fail
	store.mu.Unlock()

	in := &gfuse.SetAttrIn{SetAttrInCommon: gfuse.SetAttrInCommon{Valid: gfuse.FATTR_SIZE, Size: 0}}
	if errno := n.Setattr(context.Background(), nil, in, &gfuse.AttrOut{}); errno != 0 {
		t.Fatalf("truncate to 0 = %v", errno)
	}
	fresh := []byte("fresh")
	mustWrite(t, n, fresh, 0)
	mustFlush(t, n)
	if got := store.committed(t, n.path); !bytes.Equal(got, fresh) {
		t.Fatalf("after truncate committed %q, want %q", got, fresh)
	}
}

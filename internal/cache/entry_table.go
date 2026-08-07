package cache

import (
	"math"
	"sort"
	"strconv"
	"strings"
)

// DirEntry is one direct child of a directory, as served to the FUSE layer.
type DirEntry struct {
	Name  string
	IsDir bool
}

// chunkIndexOf extracts N from "<parent>_chunk_N". Callers only reach it with
// keys parentFilePathFromChunkPath already accepted; the MaxInt64 fallback
// just keeps an unparseable key from sorting first.
func chunkIndexOf(chunkPath string) int64 {
	idx := strings.LastIndex(chunkPath, "_chunk_")
	if idx < 0 {
		return math.MaxInt64
	}
	n, err := strconv.ParseInt(chunkPath[idx+len("_chunk_"):], 10, 64)
	if err != nil {
		return math.MaxInt64
	}
	return n
}

// entryTable holds file metadata keyed by path plus two indexes that turn
// what used to be whole-map scans into point lookups:
//
//   - chunks: parent path -> its "<parent>_chunk_N" keys. Delete and
//     overwrite need every chunk of one file; finding them with a string
//     prefix scan cost O(all cached objects) per write, and a node holding a
//     single 100GB file at an 8MB stride holds 12800 chunk entries.
//   - dirs: directory -> direct children. FUSE lookup and readdir used to
//     copy the entire map and then scan the copy (twice, for lookup); both
//     are now O(children of that directory).
//
// byPath is unexported and every mutation goes through put/del, so the
// indexes cannot drift: a raw map assignment elsewhere would silently
// reintroduce the scan.
type entryTable struct {
	byPath map[string]*CacheEntry
	chunks map[string]map[string]struct{}
	dirs   map[string]map[string]*dirChild
}

// dirChild is one name inside a directory. The two counters exist because the
// same name can be kept alive by both a file ("/a") and a subtree ("/a/b"):
// refcounting lets an intermediate directory disappear exactly when its last
// descendant does, and fileRefs winning the kind matches the pre-index lookup
// order, which checked for an exact file match before any prefix match.
type dirChild struct {
	fileRefs int
	dirRefs  int
}

func (c *dirChild) isDir() bool { return c.fileRefs == 0 && c.dirRefs > 0 }

func (c *dirChild) alive() bool { return c.fileRefs > 0 || c.dirRefs > 0 }

func newEntryTable() *entryTable {
	return &entryTable{
		byPath: make(map[string]*CacheEntry),
		chunks: make(map[string]map[string]struct{}),
		dirs:   make(map[string]map[string]*dirChild),
	}
}

func (t *entryTable) len() int { return len(t.byPath) }

// get returns the live entry pointer for path. It mirrors map indexing (the
// bool reports presence) so callers read the same way they did when entries
// was a bare map. Callers holding only a read lock must not mutate the result.
func (t *entryTable) get(path string) (*CacheEntry, bool) {
	entry, ok := t.byPath[path]
	return entry, ok
}

// put inserts or replaces the entry keyed by its own FilePath.
func (t *entryTable) put(entry *CacheEntry) {
	if entry == nil || entry.FilePath == "" {
		return
	}
	if _, exists := t.byPath[entry.FilePath]; !exists {
		t.index(entry.FilePath)
	}
	t.byPath[entry.FilePath] = entry
}

// del removes path and releases its index references. No-op if absent.
func (t *entryTable) del(path string) {
	if _, exists := t.byPath[path]; !exists {
		return
	}
	delete(t.byPath, path)
	t.unindex(path)
}

// each visits every entry. Returning false stops the walk. Iteration order is
// map order, i.e. random.
func (t *entryTable) each(fn func(path string, entry *CacheEntry) bool) {
	for path, entry := range t.byPath {
		if !fn(path, entry) {
			return
		}
	}
}

// snapshot returns value copies of every entry, so callers can read them
// after releasing the lock.
func (t *entryTable) snapshot() []*CacheEntry {
	out := make([]*CacheEntry, 0, len(t.byPath))
	for _, entry := range t.byPath {
		if entry == nil {
			continue
		}
		entryCopy := *entry
		out = append(out, &entryCopy)
	}
	return out
}

// chunkKeys returns the known chunk keys of parent, in ascending index order.
//
// Membership is the strict definition used everywhere else in the cache
// (parentFilePathFromChunkPath): the suffix after "_chunk_" must parse as an
// integer. The prefix scan this replaces was looser and would have swept up a
// user file literally named "report_chunk_draft" when "report" was deleted.
func (t *entryTable) chunkKeys(parent string) []string {
	set := t.chunks[parent]
	if len(set) == 0 {
		return nil
	}
	keys := make([]string, 0, len(set))
	for key := range set {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		return chunkIndexOf(keys[i]) < chunkIndexOf(keys[j])
	})
	return keys
}

// children returns the direct children of dir, or nil if dir holds nothing.
// Names are returned sorted so readdir output is stable across calls.
func (t *entryTable) children(dir string) []DirEntry {
	kids := t.dirs[normalizeDirPath(dir)]
	if len(kids) == 0 {
		return nil
	}
	out := make([]DirEntry, 0, len(kids))
	for name, child := range kids {
		out = append(out, DirEntry{Name: name, IsDir: child.isDir()})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

// hasChildren reports whether dir has any descendants, i.e. whether it exists
// as a directory at all.
func (t *entryTable) hasChildren(dir string) bool {
	return len(t.dirs[normalizeDirPath(dir)]) > 0
}

func (t *entryTable) index(path string) {
	if parent, ok := parentFilePathFromChunkPath(path); ok {
		set := t.chunks[parent]
		if set == nil {
			set = make(map[string]struct{}, 4)
			t.chunks[parent] = set
		}
		set[path] = struct{}{}
		// Chunks stay out of the directory index: they are an internal
		// representation of one file, not siblings of it. Indexing them would
		// also make readdir cost scale with chunk count again — a single
		// 100GB object at an 8MB stride adds ~12800 names to its directory.
		return
	}
	t.walkAncestors(path, true)
}

func (t *entryTable) unindex(path string) {
	if parent, ok := parentFilePathFromChunkPath(path); ok {
		if set := t.chunks[parent]; set != nil {
			delete(set, path)
			if len(set) == 0 {
				delete(t.chunks, parent)
			}
		}
		return
	}
	t.walkAncestors(path, false)
}

// walkAncestors adds or releases one reference to path in every directory
// above it, creating and collecting intermediate directories as needed.
func (t *entryTable) walkAncestors(path string, add bool) {
	body := strings.Trim(path, "/")
	if body == "" {
		return
	}
	segments := strings.Split(body, "/")
	dir := "/"
	for i, segment := range segments {
		if segment == "" {
			// Defensive: "//" in a path would otherwise create a nameless child.
			return
		}
		isDir := i < len(segments)-1
		if add {
			t.addChild(dir, segment, isDir)
		} else {
			t.releaseChild(dir, segment, isDir)
		}
		if dir == "/" {
			dir = "/" + segment
		} else {
			dir += "/" + segment
		}
	}
}

func (t *entryTable) addChild(dir, name string, isDir bool) {
	kids := t.dirs[dir]
	if kids == nil {
		kids = make(map[string]*dirChild, 4)
		t.dirs[dir] = kids
	}
	child := kids[name]
	if child == nil {
		child = &dirChild{}
		kids[name] = child
	}
	if isDir {
		child.dirRefs++
	} else {
		child.fileRefs++
	}
}

func (t *entryTable) releaseChild(dir, name string, isDir bool) {
	kids := t.dirs[dir]
	if kids == nil {
		return
	}
	child := kids[name]
	if child == nil {
		return
	}
	if isDir {
		if child.dirRefs > 0 {
			child.dirRefs--
		}
	} else if child.fileRefs > 0 {
		child.fileRefs--
	}
	if child.alive() {
		return
	}
	delete(kids, name)
	if len(kids) == 0 {
		delete(t.dirs, dir)
	}
}

// normalizeDirPath maps a directory path to the form used as a dirs key:
// rooted, no trailing slash, with the root itself staying "/".
func normalizeDirPath(dir string) string {
	if dir == "" {
		return "/"
	}
	if !strings.HasPrefix(dir, "/") {
		dir = "/" + dir
	}
	if dir != "/" {
		dir = strings.TrimRight(dir, "/")
	}
	if dir == "" {
		return "/"
	}
	return dir
}

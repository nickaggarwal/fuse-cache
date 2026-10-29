package cache

import (
	"context"
	"errors"
	"sort"
	"strings"
	"time"
)

// Metadata point lookups for the FUSE layer.
//
// FUSE resolves one path at a time: lookup asks about a single name, readdir
// about one directory, getattr about one file. The old shape answered all
// three from List(ctx), which copies every cached entry (one per chunk, so a
// node holding a single 100GB file at an 8MB stride copies ~12800 structs)
// and then scans the copy linearly — twice, in the lookup case. Both costs
// are per syscall.
//
// These answer from the entry table's indexes, falling back to the
// coordinator's cached metadata view only when the path is not local, which
// preserves the previous behavior of surfacing files held only by peers.

var errNoCoordinator = errors.New("no coordinator configured")

// LookupEntry returns a copy of the metadata for filePath. forceRefresh asks
// for a coordinator round-trip even when the cached remote view is fresh,
// matching what the FUSE lookup path used ListFresh for.
func (cm *DefaultCacheManager) LookupEntry(ctx context.Context, filePath string, forceRefresh bool) (*CacheEntry, bool) {
	cm.mu.RLock()
	if entry, ok := cm.entries.get(filePath); ok && entry != nil {
		entryCopy := *entry
		cm.mu.RUnlock()
		return &entryCopy, true
	}
	view := cm.metadataView
	stale := time.Since(cm.metadataAt) >= cm.config.MetadataRefreshTTL
	cm.mu.RUnlock()

	if !forceRefresh {
		if entry, ok := findInView(view, filePath); ok {
			return entry, true
		}
	}
	if !forceRefresh && !stale {
		return nil, false
	}
	// A forced refresh asks about ONE path, so ask the coordinator about one
	// path. Refreshing the whole view here downloads every file location in
	// the cluster (measured: 18 MB / ~62k keys, ~0.8 s) for each lookup of a
	// name that doesn't exist yet. Every Create does such a lookup, so a burst
	// of new files turns into concurrent full-table lists, which OOM-killed
	// all three coordinator replicas (2026-10-10). The periodic, TTL-driven
	// view refresh below still serves Readdir and unforced lookups.
	if forceRefresh {
		entry, found, err := cm.lookupRemoteEntry(ctx, filePath)
		if err != nil {
			// Coordinator outages must not turn into ENOENT for a path the
			// last good view still knows about.
			return findInView(view, filePath)
		}
		if found {
			cm.mergeIntoView([]*CacheEntry{entry})
			return entry, true
		}
		// Not a file. It may be a directory whose children live only on
		// peers: DirExists answers from the view, and the full refresh this
		// replaced used to put them there as a side effect. Ask for this
		// path's subtree only. For a name that doesn't exist yet (the
		// Create case) that is an empty etcd range, not the whole table.
		if children, err := cm.config.Coordinator.ListFileLocations(ctx, normalizeDirPath(filePath)+"/"); err == nil {
			cm.mergeIntoView(cm.entriesFromLocations(children))
		}
		// Opt-in (-global-read) and forced lookups only: a path published
		// by another cluster resolves through the mount, so FUSE can stat
		// and open it (reads go through the global data path). Off, or not
		// in the mirror, it stays ENOENT. Merged into the view so open and
		// ReadRange find the shape; the next periodic refresh drops it and
		// the next forced lookup re-learns it.
		if loc, ok := cm.resolveGlobalFile(ctx, filePath); ok {
			entry := cm.entryFromGlobal(loc)
			cm.mergeIntoView([]*CacheEntry{entry})
			entryCopy := *entry
			return &entryCopy, true
		}
		return nil, false
	}
	refreshed, err := cm.refreshMetadataView(ctx)
	if err != nil {
		return findInView(view, filePath)
	}
	return findInView(refreshed, filePath)
}

// mergeIntoView adds entries learned by point or subtree lookups to the cached
// remote view, replacing same-path rows. metadataAt is left alone so the
// periodic full refresh still runs on schedule and drops anything stale.
func (cm *DefaultCacheManager) mergeIntoView(entries []*CacheEntry) {
	if len(entries) == 0 {
		return
	}
	fresh := make(map[string]*CacheEntry, len(entries))
	for _, e := range entries {
		if e != nil {
			fresh[e.FilePath] = e
		}
	}
	cm.mu.Lock()
	defer cm.mu.Unlock()
	merged := make([]*CacheEntry, 0, len(cm.metadataView)+len(fresh))
	for _, e := range cm.metadataView {
		if e == nil {
			continue
		}
		if _, replaced := fresh[e.FilePath]; !replaced {
			merged = append(merged, e)
		}
	}
	for _, e := range fresh {
		merged = append(merged, e)
	}
	cm.metadataView = merged
}

// lookupRemoteEntry is a single-path coordinator lookup. found=false with a
// nil error means the coordinator positively does not know the path.
func (cm *DefaultCacheManager) lookupRemoteEntry(ctx context.Context, filePath string) (*CacheEntry, bool, error) {
	coord := cm.config.Coordinator
	if coord == nil {
		return nil, false, errNoCoordinator
	}
	locations, err := coord.GetFileLocation(ctx, filePath)
	if err != nil {
		return nil, false, err
	}
	// A path can have several rows (one per holder and tier). Some carry no
	// size: a node that landed one chunk advertises itself as a holder of the
	// parent before it knows the parent's size. Taking the first row blindly
	// made a blob look 0 bytes long once the sized rows were gone (a store
	// quarantining its copy retracts only its own row), and every read of it
	// failed ("empty blob"). Prefer a row that knows the size; a size-0 row
	// still proves the path exists, so it is the last resort, not a miss.
	var best *CacheEntry
	for _, entry := range cm.entriesFromLocations(locations) {
		if entry.FilePath != filePath {
			continue
		}
		if best == nil || (best.Size <= 0 && entry.Size > 0) {
			best = entry
		}
	}
	if best == nil {
		return nil, false, nil
	}
	return best, true, nil
}

// DirExists reports whether path names a directory, i.e. whether anything
// known lives beneath it. The root always exists.
func (cm *DefaultCacheManager) DirExists(ctx context.Context, path string) bool {
	if normalizeDirPath(path) == "/" {
		return true
	}
	cm.mu.RLock()
	local := cm.entries.hasChildren(path)
	view := cm.metadataView
	cm.mu.RUnlock()
	if local {
		return true
	}
	prefix := normalizeDirPath(path) + "/"
	for _, entry := range view {
		if entry != nil && strings.HasPrefix(entry.FilePath, prefix) {
			return true
		}
	}
	return false
}

// ReadDir returns the direct children of dir, merging local entries with the
// coordinator's metadata view. Names are unique and sorted.
func (cm *DefaultCacheManager) ReadDir(ctx context.Context, dir string) ([]DirEntry, error) {
	if cm.config.Coordinator != nil {
		cm.mu.RLock()
		stale := time.Since(cm.metadataAt) >= cm.config.MetadataRefreshTTL
		cm.mu.RUnlock()
		if stale {
			if _, err := cm.refreshMetadataView(ctx); err != nil {
				cm.logger.Printf("Readdir metadata refresh failed for %s (serving cached view): %v", dir, err)
			}
		}
	}

	cm.mu.RLock()
	local := cm.entries.children(dir)
	view := cm.metadataView
	cm.mu.RUnlock()

	isDirByName := make(map[string]bool, len(local))
	for _, child := range local {
		isDirByName[child.Name] = child.IsDir
	}
	// A remote path can name a child this node does not hold. Local wins on
	// conflict: it carries the live tier and size.
	prefix := normalizeDirPath(dir)
	if prefix != "/" {
		prefix += "/"
	}
	for _, entry := range view {
		if entry == nil || !strings.HasPrefix(entry.FilePath, prefix) {
			continue
		}
		// Chunks are an internal representation of one file, not siblings of
		// it. The local side drops them in the index; the coordinator can
		// still publish chunk rows, so drop them here too.
		if _, isChunk := parentFilePathFromChunkPath(entry.FilePath); isChunk {
			continue
		}
		name, _, hasSlash := strings.Cut(strings.TrimPrefix(entry.FilePath, prefix), "/")
		if name == "" {
			continue
		}
		if _, seen := isDirByName[name]; seen {
			continue
		}
		isDirByName[name] = hasSlash
	}

	out := make([]DirEntry, 0, len(isDirByName))
	for name, isDir := range isDirByName {
		out = append(out, DirEntry{Name: name, IsDir: isDir})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

// refreshMetadataView pulls the coordinator's file list and replaces the
// cached remote view. Returns the new view.
func (cm *DefaultCacheManager) refreshMetadataView(ctx context.Context) ([]*CacheEntry, error) {
	coord := cm.config.Coordinator
	if coord == nil {
		return nil, errNoCoordinator
	}
	locations, err := coord.ListFileLocations(ctx, "")
	if err != nil {
		return nil, err
	}
	remote := cm.entriesFromLocations(locations)
	cm.mu.Lock()
	cm.metadataView = remote
	cm.metadataAt = time.Now()
	cm.mu.Unlock()
	return remote, nil
}

func findInView(view []*CacheEntry, filePath string) (*CacheEntry, bool) {
	var best *CacheEntry
	for _, entry := range view {
		if entry == nil || entry.FilePath != filePath {
			continue
		}
		// Same rule as lookupRemoteEntry: a row that knows the size wins
		// over a holder row advertised before the size was known.
		if best == nil || (best.Size <= 0 && entry.Size > 0) {
			best = entry
		}
	}
	if best == nil {
		return nil, false
	}
	entryCopy := *best
	return &entryCopy, true
}

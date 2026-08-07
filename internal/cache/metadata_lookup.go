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
	refreshed, err := cm.refreshMetadataView(ctx)
	if err != nil {
		// Coordinator outages must not turn into ENOENT for a path the last
		// good view still knows about.
		return findInView(view, filePath)
	}
	return findInView(refreshed, filePath)
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
	for _, entry := range view {
		if entry == nil || entry.FilePath != filePath {
			continue
		}
		entryCopy := *entry
		return &entryCopy, true
	}
	return nil, false
}

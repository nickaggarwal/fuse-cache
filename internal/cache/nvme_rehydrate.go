package cache

import (
	"io/fs"
	"path/filepath"
	"strings"
	"time"
)

// Startup rehydration of NVMe accounting.
//
// The entries map and nvmeUsed live in memory; after a client restart the
// cache dir still holds every file the previous process landed, but the new
// process believes usage is zero. Consequences observed live (2026-08-02):
// the watermark evictor under-sees local usage until files are re-touched,
// and pre-restart files are invisible to LRU eviction entirely — only
// device-level statfs pressure would ever reclaim them.
//
// rehydrateNVMeAccounting walks the cache dir once at startup and rebuilds
// entries + nvmeUsed from what is actually on disk. Rehydrated entries carry
// PersistedToCloud=false — the eviction path's cloud size-probe (evictionSafe)
// is the arbiter of durability, so pre-restart files become evictable exactly
// when cloud confirms them, never before.

// rehydrateSkipFile reports whether name is cache-internal state rather than
// a cached object: checksum sidecars and the three staging prefixes.
func rehydrateSkipFile(name string) bool {
	if strings.HasSuffix(name, ".sha256") {
		return true
	}
	return strings.HasPrefix(name, ".nvme-write-") ||
		strings.HasPrefix(name, ".nvme-stream-") ||
		strings.Contains(name, ".stage-")
}

// rehydrateNVMeAccounting scans the NVMe dir and registers every unknown
// file. Existing entries (e.g. tests pre-seeding state) are left untouched.
// Returns files adopted and bytes accounted.
func (cm *DefaultCacheManager) rehydrateNVMeAccounting() (int, int64) {
	base := cm.config.NVMePath
	if base == "" {
		return 0, 0
	}

	type found struct {
		key     string
		size    int64
		modTime time.Time
	}
	var files []found
	// Walk outside the lock: dir can be large and IO-slow; the manager is
	// not serving yet at construction time, and the merge below re-checks
	// under the lock anyway.
	walkErr := filepath.WalkDir(base, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			// Unreadable subtree: skip it, keep accounting for the rest.
			if d != nil && d.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}
		if d.IsDir() {
			return nil
		}
		name := d.Name()
		if rehydrateSkipFile(name) {
			return nil
		}
		info, ierr := d.Info()
		if ierr != nil {
			return nil
		}
		rel, rerr := filepath.Rel(base, path)
		if rerr != nil {
			return nil
		}
		files = append(files, found{
			key:     "/" + filepath.ToSlash(rel),
			size:    info.Size(),
			modTime: info.ModTime(),
		})
		return nil
	})
	if walkErr != nil {
		cm.logger.Printf("NVMe rehydration walk failed for %s: %v", base, walkErr)
	}

	adopted := 0
	var bytes int64
	cm.mu.Lock()
	for _, f := range files {
		if existing, ok := cm.entries[f.key]; ok && existing != nil {
			continue
		}
		cm.entries[f.key] = &CacheEntry{
			FilePath:    f.key,
			StoragePath: f.key,
			Size:        f.size,
			// LastAccessed from mtime keeps pre-restart files at their true
			// LRU age instead of all looking brand new.
			LastAccessed: f.modTime,
			Tier:         TierNVMe,
			// Not marked persisted: evictionSafe's cloud probe decides.
		}
		cm.nvmeUsed += f.size
		adopted++
		bytes += f.size
	}
	cm.mu.Unlock()

	if adopted > 0 {
		cm.logger.Printf("NVMe rehydration: adopted %d files (%d MB) from %s",
			adopted, bytes/(1024*1024), base)
	}
	return adopted, bytes
}

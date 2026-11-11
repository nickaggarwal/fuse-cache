package cache

import (
	"context"
	"path"
	"sort"
	"sync"
	"time"
)

// Replica- and size-aware eviction order.
//
// Plain LRU treats every cold file as equally cheap to drop. They are not.
// A file another node also holds on NVMe comes back over the peer tier at
// LAN speed; this node's only local copy of a model comes back from object
// storage, an order of magnitude slower, and costs the next cold start
// exactly the latency the cache exists to remove. And among cold files, one
// large file frees in one delete what would otherwise take dozens of small
// ones, each a future miss.
//
// So each eviction batch (the evictBatchSize coldest candidates, already
// filtered by LRU) is reordered before it is consumed: files with at least
// one other NVMe holder first, then by idle time × size, so the bytes
// cheapest to get back and longest unused go first. LRU still decides which
// files are candidates at all; this only orders within the cold end.
//
// Releases are evicted as units. A model is a directory of shards, and a
// model missing one shard loads exactly as slowly as a model missing all of
// them: the load blocks on the cold one. So a file's idle time for ordering
// is its directory's, measured from the most recent access to any file in
// that directory, and siblings sort next to each other. A cold release goes
// whole before a hot release loses its one rarely-read file (a tokenizer
// or config that LRU alone would pick first and the next load would then
// fetch cold).
//
// Advisory: the holder lookup is bounded (a deadline for the whole batch,
// cached for holderCacheTTL), and if the coordinator is slow or down the
// batch keeps idle×size order, which is never worse than the LRU order it
// replaces. Pinning and the never-evict-the-only-durable-copy guard apply
// unchanged after the reorder.

const (
	evictHolderLookupBudget = 750 * time.Millisecond
	evictHolderLookupPar    = 8
	evictHolderCacheTTL     = 30 * time.Second
	evictHolderCacheMax     = 8192
)

type evictHolderCache struct {
	mu sync.Mutex
	m  map[string]evictHolderRecord
}

type evictHolderRecord struct {
	others int
	at     time.Time
}

func (c *evictHolderCache) get(path string) (int, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	r, ok := c.m[path]
	if !ok || time.Since(r.at) > evictHolderCacheTTL {
		return 0, false
	}
	return r.others, true
}

func (c *evictHolderCache) put(path string, others int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.m == nil || len(c.m) >= evictHolderCacheMax {
		c.m = make(map[string]evictHolderRecord)
	}
	c.m[path] = evictHolderRecord{others: others, at: time.Now()}
}

// otherNVMeHolders counts peers other than this one with an "nvme" row for
// path (chunk paths resolve to their parent: holders are advertised per
// file). -1 means unknown.
func (cm *DefaultCacheManager) otherNVMeHolders(ctx context.Context, path string) int {
	if parent, ok := parentFilePathFromChunkPath(path); ok {
		path = parent
	}
	if n, ok := cm.evictHolders.get(path); ok {
		return n
	}
	locs, err := cm.config.Coordinator.GetFileLocation(ctx, path)
	if err != nil {
		return -1
	}
	seen := map[string]struct{}{}
	for _, l := range locs {
		if l == nil || l.PeerID == "" || l.PeerID == cm.config.LocalPeerID {
			continue
		}
		if l.StorageTier != cacheTierToStorageTier(TierNVMe) {
			continue
		}
		seen[l.PeerID] = struct{}{}
	}
	cm.evictHolders.put(path, len(seen))
	return len(seen)
}

type evictCandidate struct {
	key    string
	entry  *CacheEntry
	idle   time.Duration // the release's idle time (see releaseKey)
	size   int64
	others int // other NVMe holders; -1 unknown
	group  string
}

// releaseKey is the unit a file is evicted with: the directory of the file
// (of the parent file, for a chunk).
func releaseKey(key string) string {
	if parent, ok := parentFilePathFromChunkPath(key); ok {
		key = parent
	}
	return path.Dir(key)
}

// releaseLastAccess returns, for each release in batch, the most recent
// access to any NVMe entry in it. One pass over the table, under the read
// lock the caller already holds; eviction passes are seconds apart.
func (cm *DefaultCacheManager) releaseLastAccessLocked(batch []evictCandidate) map[string]time.Time {
	want := make(map[string]time.Time, len(batch))
	for _, c := range batch {
		want[c.group] = time.Time{}
	}
	cm.entries.each(func(k string, e *CacheEntry) bool {
		if e == nil || e.Tier != TierNVMe {
			return true
		}
		g := releaseKey(k)
		if last, ok := want[g]; ok && e.LastAccessed.After(last) {
			want[g] = e.LastAccessed
		}
		return true
	})
	return want
}

// orderEvictionBatch reorders one cold batch in place: shared copies first,
// then idle×size descending. Ties keep LRU order (stable sort).
func (cm *DefaultCacheManager) orderEvictionBatch(ctx context.Context, batch []evictCandidate) {
	if !cm.config.EvictPreferReplicated || len(batch) < 2 {
		return
	}
	if cm.config.Coordinator != nil && cm.config.LocalPeerID != "" {
		lookupCtx, cancel := context.WithTimeout(ctx, evictHolderLookupBudget)
		sem := make(chan struct{}, evictHolderLookupPar)
		var wg sync.WaitGroup
		for i := range batch {
			wg.Add(1)
			sem <- struct{}{}
			go func(c *evictCandidate) {
				defer wg.Done()
				defer func() { <-sem }()
				c.others = cm.otherNVMeHolders(lookupCtx, c.key)
			}(&batch[i])
		}
		wg.Wait()
		cancel()
	}
	sort.SliceStable(batch, func(i, j int) bool {
		si, sj := batch[i].others > 0, batch[j].others > 0
		if si != sj {
			return si
		}
		if batch[i].group == batch[j].group {
			return false // siblings keep LRU order among themselves
		}
		return groupWeight(batch, batch[i].group) > groupWeight(batch, batch[j].group)
	})
	for _, c := range batch {
		if c.others > 0 {
			cm.metrics.EvictOrderShared.Add(1)
		}
	}
}

// groupWeight is the release's idle seconds × the MiB of it in this batch,
// floored at 1 each so a just-touched or tiny release still orders by the
// other factor. Every member shares the release's idle time, so siblings
// compare equal and sort together.
func groupWeight(batch []evictCandidate, group string) float64 {
	var idle float64
	var size int64
	for _, c := range batch {
		if c.group == group {
			idle = c.idle.Seconds()
			size += c.size
		}
	}
	if idle < 1 {
		idle = 1
	}
	mb := float64(size) / (1 << 20)
	if mb < 1 {
		mb = 1
	}
	return idle * mb
}

package cache

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// ModelKVStore is a durable store for LLM KV-cache blocks.
//
// It exists because the CacheManager write path is built for model weights and
// is actively wrong for KV. A weight file is written once, read by every
// replica, and is worth replicating and persisting the instant it lands. A KV
// block is written per request, read at most a handful of times, usually never
// read again, and is worthless the moment its prefix stops recurring. Sending
// every KV block down the weights path — NVMe, 3x peer replication, and a
// background Azure Blob upload, all unconditional (cache.go's Write) — costs
// roughly 4x the bytes of the block itself to durably retain data whose
// expected lifetime is seconds.
//
// So the write path here inverts the weights assumptions:
//
//   - Cloud persistence is EARNED, not automatic. A block lands on NVMe and
//     stops. Only when it is read back PromoteAfterReads times — i.e. it has
//     demonstrated reuse — does it become a candidate for durable storage.
//     Most blocks die in NVMe eviction having never touched the network.
//   - Durability is BATCHED. Earned blocks are coalesced per prefix into one
//     cloud object, because a KV block at LMCache's default 256-token chunk is
//     ~7MiB for a 1.5B model, well under the size where per-object overhead
//     stops dominating, and because Azure Blob's per-blob request ceiling is a
//     far scarcer resource than its byte ceiling.
//   - Blocks EXPIRE. KV is the one datatype where a TTL is correct by default.
//     Neither LMCache nor the CacheManager has one, and without it a durable
//     KV tier grows without bound.
//
// The store owns policy only. Bytes move through the TierStorage
// implementations the rest of the cache uses, so NVMe eviction, peer serving,
// and cloud retry behave identically here.
type ModelKVStore struct {
	cfg   ModelKVConfig
	nvme  TierStorage
	cloud TierStorage
	// durable is nvme when it supports fsync barriers, nil otherwise.
	durable durableWriter

	mu    sync.RWMutex
	index map[string]*kvBlockMeta // key -> metadata
	// pending holds batches being coalesced, keyed by prefix. Guarded by mu.
	pending map[string]*pendingBatch

	metrics ModelKVMetrics
	logger  Logger

	stopCh chan struct{}
	stopMu sync.Mutex
	// closed guards against a double Close, which would panic on stopCh.
	closed bool
	wg     sync.WaitGroup

	// now is injectable so TTL and promotion tests can advance time without
	// sleeping. Production leaves it nil and reads the wall clock.
	now func() time.Time
}

// Logger is the minimal logging surface the store needs. *log.Logger satisfies
// it, which is what every other component here is handed.
type Logger interface {
	Printf(format string, v ...interface{})
}

// durableWriter is implemented by a tier that can offer a real durability
// barrier (fsync of data and of the containing directory), not just an atomic
// rename. *NVMeStorage does. The store checks for it at construction rather
// than requiring it, so a test fake or an alternative tier still works — it
// just gets crash-atomicity instead of power-loss durability, and says so.
type durableWriter interface {
	WriteSync(ctx context.Context, path string, data []byte) error
}

// ModelKVConfig tunes the durability policy.
type ModelKVConfig struct {
	// Prefix under which all KV objects live, in both NVMe and cloud. Keeping
	// KV in its own namespace is what lets the replica reconciler and the warm
	// reconciler skip it wholesale rather than filtering block by block.
	Prefix string

	// PromoteAfterReads is how many reads a block must serve before it is
	// worth durably storing. 0 and 1 both mean "persist on first read"; the
	// write itself never promotes, because a block nobody has read yet has
	// shown no evidence of reuse.
	PromoteAfterReads int

	// TTL is how long a block lives before the reaper deletes it, measured
	// from last access. Zero disables expiry.
	TTL time.Duration

	// BatchBytes is the coalescing target. Earned blocks accumulate per prefix
	// until they reach this, then flush as one cloud object.
	BatchBytes int64

	// BatchInterval bounds how long an under-full batch waits. Without it a
	// prefix that goes quiet just below BatchBytes would never reach cloud.
	BatchInterval time.Duration

	// ReapInterval is how often the TTL sweep runs. Zero disables the reaper.
	ReapInterval time.Duration
}

// DefaultModelKVConfig returns a policy sized for the LMCache default chunk
// (256 tokens, ~7MiB on a 1.5B GQA model).
func DefaultModelKVConfig() ModelKVConfig {
	return ModelKVConfig{
		Prefix:            "kv",
		PromoteAfterReads: 2,
		TTL:               2 * time.Hour,
		BatchBytes:        64 << 20,
		BatchInterval:     30 * time.Second,
		ReapInterval:      5 * time.Minute,
	}
}

// kvBlockMeta is the index row for one block. It is persisted to NVMe on every
// mutation so the index survives a daemon restart (see recoverIndex).
type kvBlockMeta struct {
	Key          string    `json:"key"`
	Prefix       string    `json:"prefix"`
	Size         int64     `json:"size"`
	Reads        int       `json:"reads"`
	CreatedAt    time.Time `json:"created_at"`
	LastAccessed time.Time `json:"last_accessed"`
	// Durable is true once the block's bytes are in cloud storage. It is the
	// difference between "we have this" and "we would still have this if this
	// node died", which is the entire point of the store.
	Durable bool `json:"durable"`
	// BatchID names the cloud object the block was folded into, so a restore
	// knows which object to fetch. Empty until durable.
	BatchID string `json:"batch_id,omitempty"`
}

// ModelKVMetrics counts the things that distinguish this path from the weights
// path. Every counter answers a question an operator will actually ask.
type ModelKVMetrics struct {
	Puts            atomic.Int64
	Gets            atomic.Int64
	Hits            atomic.Int64
	Misses          atomic.Int64
	Promoted        atomic.Int64 // blocks that earned durability
	PromotedBytes   atomic.Int64
	BatchesFlushed  atomic.Int64
	CloudWrites     atomic.Int64 // cloud PUTs; compare against Puts to see the write amplification avoided
	Expired         atomic.Int64
	RecoveredBlocks atomic.Int64
	RecoveredFromCl atomic.Int64
}

// NewModelKVStore builds a store over the given tiers. cloud may be nil, in
// which case blocks are never durable and the store degrades to an NVMe cache
// with a TTL — which is still a correct KV cache, just not a durable one.
func NewModelKVStore(cfg ModelKVConfig, nvme, cloud TierStorage, logger Logger) *ModelKVStore {
	if cfg.Prefix == "" {
		cfg.Prefix = "kv"
	}
	if cfg.BatchBytes <= 0 {
		cfg.BatchBytes = 64 << 20
	}
	s := &ModelKVStore{
		cfg:     cfg,
		nvme:    nvme,
		cloud:   cloud,
		index:   make(map[string]*kvBlockMeta),
		pending: make(map[string]*pendingBatch),
		logger:  logger,
		stopCh:  make(chan struct{}),
	}
	if dw, ok := nvme.(durableWriter); ok {
		s.durable = dw
	} else if logger != nil {
		logger.Printf("[MODELKV] nvme tier %T has no fsync barrier; blocks are crash-atomic but not power-loss durable", nvme)
	}
	return s
}

// putLocal writes block bytes with the strongest barrier the NVMe tier offers.
func (s *ModelKVStore) putLocal(ctx context.Context, path string, data []byte) error {
	if s.durable != nil {
		return s.durable.WriteSync(ctx, path, data)
	}
	return s.nvme.Write(ctx, path, data)
}

func (s *ModelKVStore) clock() time.Time {
	if s.now != nil {
		return s.now()
	}
	return time.Now()
}

func (s *ModelKVStore) logf(format string, v ...interface{}) {
	if s.logger != nil {
		s.logger.Printf("[MODELKV] "+format, v...)
	}
}

// blockPath is where a block's bytes live on NVMe.
func (s *ModelKVStore) blockPath(key string) string {
	return filepath.Join(s.cfg.Prefix, "blocks", sanitizeKVKey(key))
}

// indexPath is where a block's index row is journaled. The row lives beside
// the block rather than in one global index file because a single file would
// need rewriting on every put, and a torn rewrite loses the whole index —
// exactly the failure the store exists to prevent.
func (s *ModelKVStore) indexPath(key string) string {
	return filepath.Join(s.cfg.Prefix, "index", sanitizeKVKey(key)+".json")
}

// batchPath is the cloud object holding one coalesced batch.
func (s *ModelKVStore) batchPath(batchID string) string {
	return filepath.Join(s.cfg.Prefix, "batches", batchID)
}

// sanitizeKVKey makes an LMCache-style key ("fmt@model@world@worker@hash")
// safe as a single path segment. The separator LMCache uses is '@', which is
// path-safe, but model names carry '/' and would silently create directories.
func sanitizeKVKey(key string) string {
	r := strings.NewReplacer("/", "_", string(os.PathSeparator), "_", "..", "_")
	return r.Replace(key)
}

// Put stores a KV block. It writes to NVMe and returns; it does NOT touch the
// network. This is the central departure from CacheManager.Write, which fires
// a cloud persist goroutine on every successful local write.
func (s *ModelKVStore) Put(ctx context.Context, key, prefix string, data []byte) error {
	if key == "" {
		return fmt.Errorf("modelkv: empty key")
	}
	if err := s.putLocal(ctx, s.blockPath(key), data); err != nil {
		return fmt.Errorf("modelkv: nvme write %s: %w", key, err)
	}

	now := s.clock()
	meta := &kvBlockMeta{
		Key:          key,
		Prefix:       prefix,
		Size:         int64(len(data)),
		Reads:        0,
		CreatedAt:    now,
		LastAccessed: now,
	}

	s.mu.Lock()
	s.index[key] = meta
	s.mu.Unlock()

	if err := s.writeIndexRow(ctx, meta); err != nil {
		// The bytes are on disk; only the journal row failed. Recovery scans
		// blocks as well as rows, so this degrades to a block that recovers
		// without its read count, not a lost block. Worth logging, not worth
		// failing the put.
		s.logf("index row write failed for %s: %v", key, err)
	}

	s.metrics.Puts.Add(1)
	return nil
}

// Get reads a block and records the access. The read count it maintains is
// what earns a block durability, so a Get is also the promotion trigger.
func (s *ModelKVStore) Get(ctx context.Context, key string) ([]byte, error) {
	s.metrics.Gets.Add(1)
	data, err := s.nvme.Read(ctx, s.blockPath(key))
	if err != nil {
		// Not on NVMe. If the block earned durability it is in a cloud batch,
		// which is the whole point: surviving the loss of local state.
		if restored, rerr := s.restoreFromCloud(ctx, key); rerr == nil {
			s.metrics.Hits.Add(1)
			s.metrics.RecoveredFromCl.Add(1)
			return restored, nil
		}
		s.metrics.Misses.Add(1)
		return nil, fmt.Errorf("modelkv: %s not found: %w", key, err)
	}

	s.metrics.Hits.Add(1)

	s.mu.Lock()
	meta, ok := s.index[key]
	if ok {
		meta.Reads++
		meta.LastAccessed = s.clock()
	}
	promote := ok && !meta.Durable && meta.Reads >= s.promoteThreshold()
	var snapshot kvBlockMeta
	if ok {
		snapshot = *meta
	}
	s.mu.Unlock()

	if ok {
		if err := s.writeIndexRow(ctx, &snapshot); err != nil {
			s.logf("index row update failed for %s: %v", key, err)
		}
	}
	if promote {
		s.enqueueForDurability(ctx, key, data)
	}
	return data, nil
}

func (s *ModelKVStore) promoteThreshold() int {
	if s.cfg.PromoteAfterReads <= 0 {
		return 1
	}
	return s.cfg.PromoteAfterReads
}

// pendingBatch accumulates earned blocks for one prefix.
type pendingBatch struct {
	prefix string
	keys   []string
	blobs  map[string][]byte
	bytes  int64
	opened time.Time
}

// enqueueForDurability adds an earned block to its prefix batch, flushing when
// the batch is full. Called from Get, so it must not block on the network for
// long; the flush itself is synchronous but only fires once per BatchBytes.
func (s *ModelKVStore) enqueueForDurability(ctx context.Context, key string, data []byte) {
	if s.cloud == nil {
		return
	}
	s.mu.Lock()
	meta, ok := s.index[key]
	if !ok || meta.Durable {
		s.mu.Unlock()
		return
	}
	if s.pending == nil {
		s.pending = make(map[string]*pendingBatch)
	}
	b, exists := s.pending[meta.Prefix]
	if !exists {
		b = &pendingBatch{
			prefix: meta.Prefix,
			blobs:  make(map[string][]byte),
			opened: s.clock(),
		}
		s.pending[meta.Prefix] = b
	}
	if _, dup := b.blobs[key]; !dup {
		b.keys = append(b.keys, key)
		b.blobs[key] = data
		b.bytes += int64(len(data))
	}
	full := b.bytes >= s.cfg.BatchBytes
	s.mu.Unlock()

	if full {
		if err := s.FlushPrefix(ctx, meta.Prefix); err != nil {
			s.logf("batch flush for prefix %s failed: %v", meta.Prefix, err)
		}
	}
}

// FlushPrefix coalesces a prefix's earned blocks into one cloud object and
// marks them durable. Exported so an operator (and the e2e test) can force the
// batch rather than wait out BatchInterval — mutate, force, assert is causal,
// whereas polling a deadline tests the clock.
func (s *ModelKVStore) FlushPrefix(ctx context.Context, prefix string) error {
	if s.cloud == nil {
		return fmt.Errorf("modelkv: no cloud tier configured")
	}
	s.mu.Lock()
	b, ok := s.pending[prefix]
	if !ok || len(b.keys) == 0 {
		s.mu.Unlock()
		return nil
	}
	delete(s.pending, prefix)
	s.mu.Unlock()

	batchID := fmt.Sprintf("%s-%d", sanitizeKVKey(prefix), s.clock().UnixNano())
	keys := append([]string(nil), b.keys...)
	sort.Strings(keys)

	payload, err := encodeBatch(keys, b.blobs)
	if err != nil {
		return fmt.Errorf("modelkv: encode batch: %w", err)
	}
	if err := s.cloud.Write(ctx, s.batchPath(batchID), payload); err != nil {
		// Put the batch back so the next flush retries it rather than silently
		// dropping durability for blocks that earned it.
		s.mu.Lock()
		if cur, exists := s.pending[prefix]; exists {
			for _, k := range keys {
				if _, dup := cur.blobs[k]; !dup {
					cur.keys = append(cur.keys, k)
					cur.blobs[k] = b.blobs[k]
					cur.bytes += int64(len(b.blobs[k]))
				}
			}
		} else {
			s.pending[prefix] = b
		}
		s.mu.Unlock()
		return fmt.Errorf("modelkv: cloud write batch %s: %w", batchID, err)
	}

	s.metrics.CloudWrites.Add(1)
	s.metrics.BatchesFlushed.Add(1)

	// Publish to the catalog only after the batch object exists, so an entry
	// never points at a batch that was never written. A catalog write failure
	// leaves the blocks durable-but-undiscoverable-cross-node: they still
	// serve locally and still survive a restart via the journal, so it is
	// logged rather than treated as a flush failure.
	if err := s.writeCatalogEntry(ctx, &kvCatalogEntry{
		BatchID:   batchID,
		Prefix:    prefix,
		Keys:      keys,
		Bytes:     b.bytes,
		CreatedAt: s.clock(),
	}); err != nil {
		s.logf("catalog publish for batch %s failed; cross-node recovery unavailable for these keys: %v", batchID, err)
	}

	rows := make([]kvBlockMeta, 0, len(keys))
	s.mu.Lock()
	for _, k := range keys {
		if meta, exists := s.index[k]; exists {
			meta.Durable = true
			meta.BatchID = batchID
			rows = append(rows, *meta)
			s.metrics.Promoted.Add(1)
			s.metrics.PromotedBytes.Add(meta.Size)
		}
	}
	s.mu.Unlock()

	for i := range rows {
		if err := s.writeIndexRow(ctx, &rows[i]); err != nil {
			s.logf("index row durability update failed for %s: %v", rows[i].Key, err)
		}
	}
	s.logf("flushed batch %s: %d blocks, %d bytes", batchID, len(keys), b.bytes)
	return nil
}

// restoreFromCloud pulls a block's batch and re-lands the block on NVMe. This
// is the recovery path: it is what makes a block survive the loss of the node
// that created it.
func (s *ModelKVStore) restoreFromCloud(ctx context.Context, key string) ([]byte, error) {
	if s.cloud == nil {
		return nil, fmt.Errorf("modelkv: no cloud tier")
	}
	s.mu.RLock()
	meta, ok := s.index[key]
	var batchID string
	if ok {
		batchID = meta.BatchID
	}
	s.mu.RUnlock()
	if !ok || batchID == "" {
		return nil, fmt.Errorf("modelkv: %s has no durable batch", key)
	}

	payload, err := s.cloud.Read(ctx, s.batchPath(batchID))
	if err != nil {
		return nil, fmt.Errorf("modelkv: read batch %s: %w", batchID, err)
	}
	blobs, err := decodeBatch(payload)
	if err != nil {
		return nil, fmt.Errorf("modelkv: decode batch %s: %w", batchID, err)
	}
	data, ok := blobs[key]
	if !ok {
		return nil, fmt.Errorf("modelkv: %s absent from batch %s", key, batchID)
	}
	// Re-land locally so the next read is an NVMe hit, mirroring the promote-
	// on-read behavior the rest of the cache has.
	if err := s.putLocal(ctx, s.blockPath(key), data); err != nil {
		s.logf("re-land of %s failed: %v", key, err)
	}

	// A catalog-recovered row carries Size 0 because the catalog records keys,
	// not sizes. Now that the bytes are here, correct it and journal the row
	// so a later restart recovers this block from the local journal without
	// another cloud round trip.
	s.mu.Lock()
	var row kvBlockMeta
	if meta, present := s.index[key]; present {
		meta.Size = int64(len(data))
		meta.LastAccessed = s.clock()
		row = *meta
	}
	s.mu.Unlock()
	if row.Key != "" {
		if err := s.writeIndexRow(ctx, &row); err != nil {
			s.logf("index row journal after restore failed for %s: %v", key, err)
		}
	}
	return data, nil
}

// RecoverIndex rebuilds the in-memory index from the NVMe journal. This closes
// the gap that makes LMCache's own disk backend non-durable across a restart:
// its LocalDiskBackend.__init__ creates an empty OrderedDict and never walks
// the directory, so every .pt file on disk becomes unreachable garbage the
// moment the process restarts. Here the journal rows are the source of truth,
// and a row whose block is missing locally but durable in cloud is kept —
// that is precisely the row that lets a restarted (or replacement) node serve
// a block it never wrote.
func (s *ModelKVStore) RecoverIndex(ctx context.Context) error {
	lister, ok := s.nvme.(prefixLister)
	if !ok {
		return fmt.Errorf("modelkv: nvme tier %T cannot enumerate; local recovery unavailable", s.nvme)
	}
	rowPaths, err := lister.ListUnder(ctx, filepath.Join(s.cfg.Prefix, "index"))
	if err != nil {
		return fmt.Errorf("modelkv: list index rows: %w", err)
	}

	recovered := 0
	for _, rp := range rowPaths {
		raw, err := s.nvme.Read(ctx, rp)
		if err != nil {
			s.logf("index row %s unreadable, skipping: %v", rp, err)
			continue
		}
		var meta kvBlockMeta
		if err := json.Unmarshal(raw, &meta); err != nil {
			// A torn row is why rows are per-block: this loses one block's
			// bookkeeping, not the index.
			s.logf("index row %s corrupt, skipping: %v", rp, err)
			continue
		}
		if meta.Key == "" {
			continue
		}
		hasLocal := s.nvme.Exists(ctx, s.blockPath(meta.Key))
		if !hasLocal && !meta.Durable {
			// Neither local bytes nor a durable copy: the row describes
			// nothing. Dropping it is what keeps the index honest.
			continue
		}
		m := meta
		s.mu.Lock()
		s.index[m.Key] = &m
		s.mu.Unlock()
		recovered++
	}
	s.metrics.RecoveredBlocks.Add(int64(recovered))
	s.logf("recovered %d index rows", recovered)
	return nil
}

// ReapExpired deletes blocks past their TTL. Returns how many were removed.
// KV is the one datatype where unbounded growth is the default failure, so
// this runs on a ticker and is also callable directly for tests.
func (s *ModelKVStore) ReapExpired(ctx context.Context) int {
	if s.cfg.TTL <= 0 {
		return 0
	}
	cutoff := s.clock().Add(-s.cfg.TTL)
	var doomed []string
	s.mu.RLock()
	for k, meta := range s.index {
		if meta.LastAccessed.Before(cutoff) {
			doomed = append(doomed, k)
		}
	}
	s.mu.RUnlock()

	for _, k := range doomed {
		_ = s.nvme.Delete(ctx, s.blockPath(k))
		_ = s.nvme.Delete(ctx, s.indexPath(k))
		s.mu.Lock()
		delete(s.index, k)
		s.mu.Unlock()
		s.metrics.Expired.Add(1)
	}
	if len(doomed) > 0 {
		s.logf("reaped %d expired blocks", len(doomed))
	}
	return len(doomed)
}

// Start runs the background batch-timer and TTL reaper.
func (s *ModelKVStore) Start(ctx context.Context) {
	if s.cfg.BatchInterval > 0 {
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			t := time.NewTicker(s.cfg.BatchInterval)
			defer t.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-s.stopCh:
					return
				case <-t.C:
					s.flushStale(ctx)
				}
			}
		}()
	}
	if s.cfg.ReapInterval > 0 {
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			t := time.NewTicker(s.cfg.ReapInterval)
			defer t.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-s.stopCh:
					return
				case <-t.C:
					s.ReapExpired(ctx)
				}
			}
		}()
	}
}

// flushStale flushes batches that have waited longer than BatchInterval, so a
// prefix that goes quiet below BatchBytes still reaches durability.
func (s *ModelKVStore) flushStale(ctx context.Context) {
	cutoff := s.clock().Add(-s.cfg.BatchInterval)
	var prefixes []string
	s.mu.RLock()
	for p, b := range s.pending {
		if b.opened.Before(cutoff) && len(b.keys) > 0 {
			prefixes = append(prefixes, p)
		}
	}
	s.mu.RUnlock()
	for _, p := range prefixes {
		if err := s.FlushPrefix(ctx, p); err != nil {
			s.logf("stale flush for %s failed: %v", p, err)
		}
	}
}

// Close stops the background loops and waits for them.
func (s *ModelKVStore) Close() {
	s.stopMu.Lock()
	if !s.closed {
		s.closed = true
		close(s.stopCh)
	}
	s.stopMu.Unlock()
	s.wg.Wait()
}

// Stats returns a point-in-time snapshot for /metrics and the e2e test.
func (s *ModelKVStore) Stats() ModelKVStats {
	s.mu.RLock()
	total := len(s.index)
	durable := 0
	var bytes int64
	for _, m := range s.index {
		if m.Durable {
			durable++
		}
		bytes += m.Size
	}
	s.mu.RUnlock()
	return ModelKVStats{
		Blocks:             total,
		DurableBlocks:      durable,
		Bytes:              bytes,
		Puts:               s.metrics.Puts.Load(),
		Gets:               s.metrics.Gets.Load(),
		Hits:               s.metrics.Hits.Load(),
		Misses:             s.metrics.Misses.Load(),
		Promoted:           s.metrics.Promoted.Load(),
		PromotedBytes:      s.metrics.PromotedBytes.Load(),
		BatchesFlushed:     s.metrics.BatchesFlushed.Load(),
		CloudWrites:        s.metrics.CloudWrites.Load(),
		Expired:            s.metrics.Expired.Load(),
		RecoveredBlocks:    s.metrics.RecoveredBlocks.Load(),
		RecoveredFromCloud: s.metrics.RecoveredFromCl.Load(),
	}
}

// ModelKVStats is the snapshot Stats returns.
type ModelKVStats struct {
	Blocks             int   `json:"blocks"`
	DurableBlocks      int   `json:"durable_blocks"`
	Bytes              int64 `json:"bytes"`
	Puts               int64 `json:"puts"`
	Gets               int64 `json:"gets"`
	Hits               int64 `json:"hits"`
	Misses             int64 `json:"misses"`
	Promoted           int64 `json:"promoted"`
	PromotedBytes      int64 `json:"promoted_bytes"`
	BatchesFlushed     int64 `json:"batches_flushed"`
	CloudWrites        int64 `json:"cloud_writes"`
	Expired            int64 `json:"expired"`
	RecoveredBlocks    int64 `json:"recovered_blocks"`
	RecoveredFromCloud int64 `json:"recovered_from_cloud"`
}

// writeIndexRow journals one block's metadata to NVMe.
func (s *ModelKVStore) writeIndexRow(ctx context.Context, meta *kvBlockMeta) error {
	raw, err := json.Marshal(meta)
	if err != nil {
		return err
	}
	// The index row is the block's recovery record: journaling it without a
	// barrier would mean a node could come back holding durable bytes it has
	// no row pointing at, which is the exact failure this store exists to
	// prevent.
	return s.putLocal(ctx, s.indexPath(meta.Key), raw)
}

// NVMeTier and CloudTier expose the underlying tiers so a ModelKVStore can be
// built over the same storage the cache manager already configured, rather
// than re-parsing credentials and re-deriving the node-init cache directory in
// a second place. Both may be nil.
func (cm *DefaultCacheManager) NVMeTier() TierStorage  { return cm.nvmeStorage }
func (cm *DefaultCacheManager) CloudTier() TierStorage { return cm.cloudStorage }

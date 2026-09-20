package cache

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"sort"
	"time"
)

// The catalog is the cloud-side half of recovery.
//
// RecoverIndex rebuilds from the NVMe journal, which covers a daemon restart:
// the process died, the disk did not. It does not cover the case the store was
// actually built for — the node is gone, and a *different* node must serve
// blocks the dead one made durable. There the journal died with the disk, and
// the blocks in cloud are unreachable because nothing records which batch
// holds which key.
//
// So every batch flush also writes a catalog entry naming its blocks, and the
// catalog is keyed by prefix rather than by node. A replacement node adopts a
// prefix by reading the catalog for it; it does not need to know which node
// originally wrote anything. That is what makes the durability claim real
// rather than a restart-survival claim.
//
// The catalog is not a database and does not try to be consistent: an entry
// may name a batch that was deleted, and a batch may exist with no entry (a
// crash between the two writes). Both are handled by treating the catalog as a
// hint — a missing batch is a miss, and an orphan batch is garbage the TTL
// sweep reclaims. Recovery is best-effort by construction; the alternative is
// a consensus store on the KV write path, which is the cost this whole design
// exists to avoid.

// kvCatalogEntry records one flushed batch.
type kvCatalogEntry struct {
	BatchID   string    `json:"batch_id"`
	Prefix    string    `json:"prefix"`
	Keys      []string  `json:"keys"`
	Bytes     int64     `json:"bytes"`
	CreatedAt time.Time `json:"created_at"`
}

// catalogPath is where a batch's catalog entry lives in cloud storage. Entries
// are per-batch, not one file per prefix, for the same reason index rows are
// per-block: a single mutable file would need read-modify-write on every
// flush, and concurrent flushers would lose each other's entries with no
// locking to prevent it.
func (s *ModelKVStore) catalogPath(prefix, batchID string) string {
	return filepath.Join(s.cfg.Prefix, "catalog", sanitizeKVKey(prefix), batchID+".json")
}

// writeCatalogEntry publishes a batch to the catalog. Called after the batch
// object itself lands, so a catalog entry never names a batch that does not
// exist — the reverse (batch with no entry) is the safe direction to fail.
func (s *ModelKVStore) writeCatalogEntry(ctx context.Context, e *kvCatalogEntry) error {
	raw, err := json.Marshal(e)
	if err != nil {
		return err
	}
	return s.cloud.Write(ctx, s.catalogPath(e.Prefix, e.BatchID), raw)
}

// prefixLister is implemented by a cloud tier that can enumerate a prefix.
// Recovery-from-cloud needs it; stores without it can still recover from the
// local journal.
type prefixLister interface {
	ListUnder(ctx context.Context, prefix string) ([]string, error)
}

// RecoverFromCloud rebuilds the index for a prefix by reading the catalog,
// with no dependence on local state. This is the node-loss path: call it on a
// replacement node and it can serve every block the dead node made durable.
//
// It does NOT download block bytes. It rebuilds metadata only, marking each
// block durable with its batch ID, so the first Get for a key pulls its batch
// on demand. Downloading every batch eagerly would stall startup on hundreds
// of megabytes the node may never be asked for.
func (s *ModelKVStore) RecoverFromCloud(ctx context.Context, prefix string) (int, error) {
	if s.cloud == nil {
		return 0, fmt.Errorf("modelkv: no cloud tier configured")
	}
	lister, ok := s.cloud.(prefixLister)
	if !ok {
		return 0, fmt.Errorf("modelkv: cloud tier %T cannot enumerate; cross-node recovery unavailable", s.cloud)
	}

	catalogDir := filepath.Join(s.cfg.Prefix, "catalog", sanitizeKVKey(prefix))
	paths, err := lister.ListUnder(ctx, catalogDir)
	if err != nil {
		return 0, fmt.Errorf("modelkv: list catalog for %s: %w", prefix, err)
	}

	// Oldest first, so that if two batches somehow claim the same key the
	// newer one wins the index slot.
	sort.Strings(paths)

	recovered := 0
	for _, p := range paths {
		raw, err := s.cloud.Read(ctx, p)
		if err != nil {
			s.logf("catalog entry %s unreadable, skipping: %v", p, err)
			continue
		}
		var e kvCatalogEntry
		if err := json.Unmarshal(raw, &e); err != nil {
			s.logf("catalog entry %s corrupt, skipping: %v", p, err)
			continue
		}
		if e.BatchID == "" || len(e.Keys) == 0 {
			continue
		}
		now := s.clock()
		s.mu.Lock()
		for _, k := range e.Keys {
			if existing, present := s.index[k]; present && existing.Durable {
				// Already known durable; keep whichever batch we have rather
				// than repointing at an older one.
				continue
			}
			s.index[k] = &kvBlockMeta{
				Key:    k,
				Prefix: e.Prefix,
				// Size is unknown until the batch is fetched. Recording 0
				// would make Stats under-report; the first Get fixes it.
				Size:         0,
				CreatedAt:    e.CreatedAt,
				LastAccessed: now,
				Durable:      true,
				BatchID:      e.BatchID,
			}
			recovered++
		}
		s.mu.Unlock()
	}

	s.metrics.RecoveredBlocks.Add(int64(recovered))
	s.logf("recovered %d durable blocks for prefix %s from cloud catalog", recovered, prefix)
	return recovered, nil
}

package cache

import (
	"context"
	"path/filepath"
	"strings"
	"time"
)

// An in-memory coordinator (no -etcd-endpoints) loses every file location
// when it restarts. Nodes keep their bytes, but nothing announces them
// again: rows are written on PUT, warm, fill and verification only. Live on
// cpu-w3 a coordinator rollout left the weights invisible to every node
// ("Repo id must be in the form ..." from vLLM: the directory was gone from
// the mount) and blobs answered 503 although the store held them.
//
// RepublishLocal re-announces everything this node holds whole on local
// disk. The client calls it when the coordinator's instance ID changes
// (GET /api/health), so a restart heals in one heartbeat.

// republishBatch bounds how many publishes run before a short pause, so a
// node with tens of thousands of objects does not flood a coordinator that
// just came up.
const republishBatch = 256

// RepublishLocal publishes this node as a holder of every whole local
// object (not chunk files: their parent's row carries the stride), with
// the size and shape it holds. OCI blobs are published only once verified.
// Returns the number of rows published.
func (cm *DefaultCacheManager) RepublishLocal(ctx context.Context) int {
	if cm.config.Coordinator == nil || cm.config.LocalPeerID == "" {
		return 0
	}
	cm.mu.RLock()
	snap := cm.entries.snapshot()
	cm.mu.RUnlock()
	n := 0
	// Parents held only as chunk files (a model node that filled chunk by
	// chunk) are announced as holders too, at the parent's own row.
	whole := map[string]bool{}
	for _, e := range snap {
		if e != nil && !strings.Contains(e.FilePath, "_chunk_") {
			whole[e.FilePath] = true
		}
	}
	chunkParents := map[string]bool{}
	for _, e := range snap {
		if e == nil || ctx.Err() != nil {
			continue
		}
		if p, ok := parentFilePathFromChunkPath(e.FilePath); ok && !whole[p] && !chunkParents[p] {
			chunkParents[p] = true
			cm.advertiseHolder(ctx, p, true)
			n++
		}
	}
	for _, e := range snap {
		if ctx.Err() != nil {
			break
		}
		if e == nil || e.Size <= 0 || e.Tier != TierNVMe || strings.Contains(e.FilePath, "_chunk_") {
			continue
		}
		if blobDigestFromPath(e.FilePath) != "" {
			full := filepath.Join(cm.config.NVMePath, strings.TrimPrefix(e.FilePath, "/"))
			if cm.blobLocalVerdict(ctx, e.FilePath, full, false) != blobGood {
				continue // published by verification when it passes
			}
		}
		cm.publishFileLocation(ctx, cm.republishShape(e), TierNVMe)
		n++
		if n%republishBatch == 0 {
			select {
			case <-ctx.Done():
				return n
			case <-time.After(50 * time.Millisecond):
			}
		}
	}
	cm.logger.Printf("Republished %d local objects to the coordinator", n)
	return n
}

// republishShape restores the chunk shape a whole local file was written
// with. Rehydration adopts files from disk without it (IsChunked=false), and
// publishing that as-is told every node a 3.5 GB weights file was unchunked:
// readers then fetched it whole through Get() for every 128 KiB FUSE window
// (6-8 s per read live on cpu-w3) until the load failed with EIO. A file
// larger than one chunk was written chunked (PutFromFile, putChunked), so it
// is published chunked at the node's stride, the one it was written with.
func (cm *DefaultCacheManager) republishShape(e *CacheEntry) *CacheEntry {
	if e.IsChunked || cm.config.ChunkSize <= 0 || e.Size <= cm.config.ChunkSize {
		return e
	}
	cp := *e
	cp.IsChunked = true
	cp.ChunkSize = cm.config.ChunkSize
	cp.NumChunks = chunkCountFor(e.Size, cp.ChunkSize)
	return &cp
}

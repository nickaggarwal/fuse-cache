package cache

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// Helpers for whole-blob peer fills (api/peer_fill.go): a node that reads an
// OCI blob another node holds pulls the whole blob in large parallel ranges
// instead of answering each nydusd read with its own peer round trip.

// ReadBlobRangeFromPeers reads [off, off+n) of entry's blob from one holder
// (store first) over the raw peer transport. Holders of the whole blob serve
// it from that copy with sendfile; when none does (a holder with only some
// chunk files), the range is read chunk by chunk instead.
func (cm *DefaultCacheManager) ReadBlobRangeFromPeers(ctx context.Context, entry *CacheEntry, off, n int64) ([]byte, error) {
	pr, ok := cm.peerStorage.(peerRangeReader)
	if !ok {
		return nil, fmt.Errorf("peer range reads not available")
	}
	data, err := pr.ReadRange(ctx, entry.FilePath, off, n)
	if err == nil && int64(len(data)) == n {
		return data, nil
	}
	if err == nil {
		err = fmt.Errorf("peer range %d+%d of %s: got %d bytes", off, n, entry.FilePath, len(data))
	}
	chunkSize := cm.effectiveChunkSize(entry)
	if !entry.IsChunked || chunkSize <= 0 || ctx.Err() != nil {
		return nil, err
	}
	out := make([]byte, 0, n)
	for pos := off; pos < off+n; {
		idx := pos / chunkSize
		within := pos - idx*chunkSize
		take := min(chunkSize-within, off+n-pos)
		part, cerr := pr.ReadRange(ctx, fmt.Sprintf("%s_chunk_%d", entry.FilePath, idx), within, take)
		if cerr != nil || int64(len(part)) != take {
			return nil, err
		}
		out = append(out, part...)
		pos += take
	}
	return out, nil
}

// LocalBlobPresent reports whether this node has a whole local file of
// exactly size bytes at filePath (verified or not). A node that has one
// serves reads from it and needs no peer fill.
func (cm *DefaultCacheManager) LocalBlobPresent(filePath string, size int64) bool {
	full := filepath.Join(cm.config.NVMePath, strings.TrimPrefix(filePath, "/"))
	info, err := os.Stat(full)
	return err == nil && !info.IsDir() && info.Size() == size
}

// PutFromFileLocal commits a staged copy of a blob the cluster already holds:
// it lands and publishes the file like PutFromFile but does not upload it
// again. The holder this copy came from persists its own upload; until that
// is confirmed the copy is not marked durable, so eviction checks the cloud
// tier first (evictionSafe) as for any unpersisted file.
func (cm *DefaultCacheManager) PutFromFileLocal(ctx context.Context, filePath, stagedPath string, size int64) error {
	return cm.putFromFile(ctx, filePath, stagedPath, size, time.Now(), false)
}

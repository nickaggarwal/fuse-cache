package cache

import (
	"context"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"fuse-client/internal/coordinator"
)

// Cross-cluster reads, the data-plane half of the global control plane.
//
// The shape of this file follows one decision: the global path is object
// storage only. There is no cross-cluster peer traffic, no new transport,
// and no new tier. A global read is a cloud read against a different key
// prefix, issued only after the local cloud read has already missed.
//
// That is why there is no TierGlobal. tierPerfTracker.order()
// (tier_perf.go:103-115) is structurally two-tier -- named t.peer and
// t.cloud fields compared pairwise -- so a third tier would mean reworking
// the adaptive ordering, the hedging decision, and the read-order helpers,
// all on the hot path, in exchange for modelling something that is not
// actually a third source of bytes.
//
// The miss taxonomy is preserved exactly: a global miss is still a miss. It
// feeds tierPerf.recordMiss through the caller's existing path and never the
// failure EWMA, per the rule in tier_miss.go:35-45.

// rawKeyReader is the optional capability a cloud backend needs to serve a
// cross-cluster read: fetch by an absolute object key rather than by a
// mount path run through this cluster's own KeyMapper.
//
// Optional rather than part of TierStorage because NVMe and peer have no
// notion of an object key, and widening the interface they all implement
// for something only the cloud tier can answer would make two of the three
// implementations return an error for a method they should not have.
type rawKeyReader interface {
	// ReadKey fetches exactly key from this backend's bucket. No candidate
	// spellings, no scoping: the caller already knows the full key.
	ReadKey(ctx context.Context, key string) ([]byte, error)
	// BucketName identifies what this backend is pointed at, so a caller
	// can tell whether a remote ObjectRef is reachable with the credentials
	// already in hand.
	BucketName() string
}

// globalReadStats counts cross-cluster activity. Separate counters for
// "looked", "found a row", and "actually served bytes" because they fail in
// different places and a single counter cannot tell a mirror that knows
// nothing from a bucket that refuses the read.
type globalReadStats struct {
	lookups       atomic.Int64
	rowsFound     atomic.Int64
	hits          atomic.Int64
	misses        atomic.Int64
	unreachable   atomic.Int64
	resolveErrors atomic.Int64

	warnOnce sync.Once
}

// globalResolver returns the coordinator's cross-cluster capability when the
// feature is on and the transport supports it, else nil. Same optional-
// interface pattern as fetchLeaser.
func (cm *DefaultCacheManager) globalResolver() coordinator.GlobalResolver {
	if !cm.config.GlobalReadEnabled || cm.config.Coordinator == nil {
		return nil
	}
	gr, ok := cm.config.Coordinator.(coordinator.GlobalResolver)
	if !ok {
		return nil
	}
	return gr
}

// globalReadTimeout bounds the resolve call. It is the same order as every
// other advisory coordinator call on the read path (metadata_lookup.go uses
// 5s for a lookup a reader waits on); this one is shorter because it runs
// after a cloud miss has already been paid for.
const globalReadTimeout = 2 * time.Second

// getFromGlobal attempts to serve path from another cluster's object
// storage. It is called only when the local cloud read missed.
//
// Returns (nil, false) for every outcome that is not a hit -- there is no
// error return on purpose. Every failure here means the same thing to the
// caller: the object was not found, which it already concluded before
// calling. Returning an error would invite a caller to treat a resolve
// timeout as a read failure and feed it to a health scorer, which is the
// exact bug tier_miss.go exists to prevent.
func (cm *DefaultCacheManager) getFromGlobal(ctx context.Context, path string) ([]byte, bool) {
	resolver := cm.globalResolver()
	if resolver == nil {
		return nil, false
	}
	reader, ok := cm.cloudStorage.(rawKeyReader)
	if !ok {
		return nil, false
	}

	cm.globalStats.lookups.Add(1)
	callCtx, cancel := context.WithTimeout(ctx, globalReadTimeout)
	res, err := resolver.ResolveGlobal(callCtx, path)
	cancel()
	if err != nil {
		// Advisory: a coordinator that cannot answer leaves the read exactly
		// where it was, a miss.
		cm.globalStats.resolveErrors.Add(1)
		return nil, false
	}
	if res == nil || len(res.Locations) == 0 {
		cm.globalStats.misses.Add(1)
		return nil, false
	}
	cm.globalStats.rowsFound.Add(1)

	localBucket := reader.BucketName()
	for _, loc := range res.Locations {
		key, ok := remoteObjectKey(loc, path)
		if !ok {
			continue
		}
		if !sameBucket(loc.Bucket, localBucket) {
			// Separate buckets need per-cluster read credentials, which this
			// stage deliberately does not distribute. Warn once rather than
			// per read: a misconfigured pair would otherwise log on every
			// miss for every path.
			cm.globalStats.unreachable.Add(1)
			cm.globalStats.warnOnce.Do(func() {
				cm.logger.Printf("Global read: cluster %s publishes bucket %q but this cluster reads %q; "+
					"cross-bucket reads need per-cluster credentials and are not configured",
					loc.ClusterID, loc.Bucket, localBucket)
			})
			continue
		}
		data, rerr := reader.ReadKey(ctx, key)
		if rerr != nil {
			if isCloudMiss(rerr) {
				// The index is stale: the row says the object is there and
				// it is not. Still a miss, not a failure.
				cm.globalStats.misses.Add(1)
				continue
			}
			cm.logger.Printf("Global read of %s from cluster %s (key %s) failed: %v",
				path, loc.ClusterID, key, rerr)
			continue
		}
		cm.globalStats.hits.Add(1)
		cm.logger.Printf("Global hit: served %s (%d bytes) from cluster %s under key %s",
			path, len(data), loc.ClusterID, key)
		return data, true
	}
	return nil, false
}

// remoteObjectKey builds the key to read for path, given a row published by
// another cluster.
//
// Two cases, and the second is why this is not a one-liner. A row for path
// itself is a direct hit. A row for path's PARENT means path is a chunk of a
// chunked file: chunk objects are not published individually (one row per
// file, not per chunk -- a 640-chunk checkpoint would otherwise put 640 rows
// in a global index whose whole premise is that it stays small), so the
// chunk key is derived from the parent's prefix. Every cluster lays chunk
// keys out identically -- "<file>_chunk_<n>" -- so the derivation needs
// nothing from the remote cluster but its prefix.
func remoteObjectKey(loc *coordinator.GlobalLocation, path string) (string, bool) {
	if loc == nil || loc.Bucket == "" {
		return "", false
	}
	mapper := NewKeyMapper(loc.KeyPrefix)
	switch {
	case loc.FilePath == path:
		return scopedRemoteKey(mapper, loc.Provider, path), true
	default:
		parent, isChunk := parentFilePathFromChunkPath(path)
		if !isChunk || parent != loc.FilePath {
			return "", false
		}
		return scopedRemoteKey(mapper, loc.Provider, path), true
	}
}

// scopedRemoteKey applies the remote cluster's prefix with the same
// per-provider normalization that cluster used when writing.
//
// The S3 backend trims the leading slash before scoping and the Azure and
// GCS backends do not (cloud_storage.go:326, azure_storage.go:48). Reading
// without reproducing that asymmetry would look under a key that differs
// from the written one by exactly one character, and the read would miss
// with no indication why.
func scopedRemoteKey(m KeyMapper, provider, path string) string {
	if strings.EqualFold(strings.TrimSpace(provider), "s3") {
		return m.Key(normalizeS3Key(path))
	}
	return m.Key(path)
}

// sameBucket is a plain comparison with whitespace tolerance. It is a
// separate function so the reason it is not fuzzier is written down: an
// endpoint- or region-aware match would let a read be attempted against a
// bucket of the same name in a different account, which fails slowly and
// looks like a stale index rather than a misconfiguration.
func sameBucket(remote, local string) bool {
	return strings.TrimSpace(remote) != "" && strings.TrimSpace(remote) == strings.TrimSpace(local)
}

// GlobalReadStats reports cross-cluster read activity for /metrics and
// operator debugging.
type GlobalReadStats struct {
	Lookups       int64 `json:"lookups"`
	RowsFound     int64 `json:"rows_found"`
	Hits          int64 `json:"hits"`
	Misses        int64 `json:"misses"`
	Unreachable   int64 `json:"unreachable_bucket"`
	ResolveErrors int64 `json:"resolve_errors"`
}

// GlobalReadSnapshot returns the counters. RowsFound well above Hits means
// the global index is advertising objects this cluster cannot read -- stale
// rows, or a bucket it has no credentials for -- which is invisible from hit
// rate alone.
func (cm *DefaultCacheManager) GlobalReadSnapshot() GlobalReadStats {
	return GlobalReadStats{
		Lookups:       cm.globalStats.lookups.Load(),
		RowsFound:     cm.globalStats.rowsFound.Load(),
		Hits:          cm.globalStats.hits.Load(),
		Misses:        cm.globalStats.misses.Load(),
		Unreachable:   cm.globalStats.unreachable.Load(),
		ResolveErrors: cm.globalStats.resolveErrors.Load(),
	}
}

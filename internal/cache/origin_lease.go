package cache

import (
	"context"
	"sync/atomic"
	"time"

	"fuse-client/internal/coordinator"
)

// Cross-node single-flight for origin (cloud) pulls — Phase 2 of
// docs/peer-coordination-thundering-herd.md.
//
// Before a node pulls an object/chunk from cloud, it takes a short-TTL lease
// in the coordinator. Losing the race means another node is already pulling
// the same key; the loser waits briefly, retries the peer tier (the leader is
// about to hold the data and Phase 3 promotes+advertises it), and only falls
// back to cloud if the peer tier still misses. Leases are strictly advisory:
// any coordinator error or timeout degrades to a normal cloud read.
const (
	// originLeaseTTL bounds how long a crashed leader suppresses other pulls.
	originLeaseTTL = 10 * time.Second
	// originLeaseFollowerWaitMin/Max is the jittered wait a lease follower
	// gives the leader before probing the peer tier. Long enough for a chunk
	// fetch + NVMe promote to typically land, short enough to keep p99 sane
	// when it doesn't.
	originLeaseFollowerWaitMin = 100 * time.Millisecond
	originLeaseFollowerWaitMax = 400 * time.Millisecond
	// originLeaseCallTimeout bounds the coordinator lease RPC itself.
	originLeaseCallTimeout = 2 * time.Second
)

// herdControlStats aggregates Phase 2/3 counters for metrics exposure.
type herdControlStats struct {
	leaseGranted          atomic.Int64
	leaseDenied           atomic.Int64
	leaseErrors           atomic.Int64
	followerPeerHits      atomic.Int64
	followerCloudFallback atomic.Int64
	advertisePublished    atomic.Int64
	reconcileRuns         atomic.Int64
	reconcileReplications atomic.Int64
	reconcileSkippedBusy  atomic.Int64
	// Ordered chunk fallback: busy-peer retries attempted / recovered
	// (each hit is a cloud round-trip avoided).
	busyChunkRetries   atomic.Int64
	busyChunkRetryHits atomic.Int64
}

// HerdControlSnapshot is the exported view of Phase 2/3 counters.
type HerdControlSnapshot struct {
	LeaseGrantedTotal          int64
	LeaseDeniedTotal           int64
	LeaseErrorsTotal           int64
	FollowerPeerHitsTotal      int64
	FollowerCloudFallbackTotal int64
	AdvertisePublishedTotal    int64
	ReconcileRunsTotal         int64
	ReconcileReplicationsTotal int64
	ReconcileSkippedBusyTotal  int64
	BusyChunkRetriesTotal      int64
	BusyChunkRetryHitsTotal    int64
}

// HerdControlSnapshot returns Phase 2/3 counters.
func (cm *DefaultCacheManager) HerdControlSnapshot() HerdControlSnapshot {
	return HerdControlSnapshot{
		LeaseGrantedTotal:          cm.herdStats.leaseGranted.Load(),
		LeaseDeniedTotal:           cm.herdStats.leaseDenied.Load(),
		LeaseErrorsTotal:           cm.herdStats.leaseErrors.Load(),
		FollowerPeerHitsTotal:      cm.herdStats.followerPeerHits.Load(),
		FollowerCloudFallbackTotal: cm.herdStats.followerCloudFallback.Load(),
		AdvertisePublishedTotal:    cm.herdStats.advertisePublished.Load(),
		ReconcileRunsTotal:         cm.herdStats.reconcileRuns.Load(),
		ReconcileReplicationsTotal: cm.herdStats.reconcileReplications.Load(),
		ReconcileSkippedBusyTotal:  cm.herdStats.reconcileSkippedBusy.Load(),
		BusyChunkRetriesTotal:      cm.herdStats.busyChunkRetries.Load(),
		BusyChunkRetryHitsTotal:    cm.herdStats.busyChunkRetryHits.Load(),
	}
}

// fetchLeaser returns the coordinator's lease capability when enabled and
// supported, else nil (all lease logic degrades to plain cloud reads).
func (cm *DefaultCacheManager) fetchLeaser() coordinator.FetchLeaser {
	if !cm.config.FetchLeaseEnabled || cm.config.Coordinator == nil || cm.config.LocalPeerID == "" {
		return nil
	}
	if fl, ok := cm.config.Coordinator.(coordinator.FetchLeaser); ok {
		return fl
	}
	return nil
}

// getFromCloudLeased reads path from the cloud tier under the cross-node
// single-flight lease. It returns the tier the data actually came from
// (TierPeer when a lease follower picked it up from the leader) so callers
// record metrics and tier-perf against the right tier.
func (cm *DefaultCacheManager) getFromCloudLeased(ctx context.Context, path string) (*CacheEntry, CacheTier, error) {
	leaser := cm.fetchLeaser()
	if leaser == nil {
		entry, err := cm.getFromTier(ctx, path, TierCloud)
		return entry, TierCloud, err
	}

	acquireCtx, cancel := context.WithTimeout(ctx, originLeaseCallTimeout)
	holder, granted, err := leaser.AcquireFetchLease(acquireCtx, path, cm.config.LocalPeerID, originLeaseTTL)
	cancel()
	if err != nil {
		// Advisory: coordinator trouble must never block the read.
		cm.herdStats.leaseErrors.Add(1)
		entry, rerr := cm.getFromTier(ctx, path, TierCloud)
		return entry, TierCloud, rerr
	}

	if granted {
		cm.herdStats.leaseGranted.Add(1)
		entry, rerr := cm.getFromTier(ctx, path, TierCloud)
		releaseCtx, cancel := context.WithTimeout(context.Background(), originLeaseCallTimeout)
		if relErr := leaser.ReleaseFetchLease(releaseCtx, path, cm.config.LocalPeerID); relErr != nil {
			cm.logger.Printf("Failed to release fetch lease for %s: %v (expires in %v)", path, relErr, originLeaseTTL)
		}
		cancel()
		return entry, TierCloud, rerr
	}

	// Follower: the holder is pulling this key from origin right now. Give it
	// a jittered head start, then read from the peer tier — Phase 3 promotes
	// the leader's chunk to NVMe and peer reads scan all active peers, so the
	// leader is reachable even before advertisement lands.
	cm.herdStats.leaseDenied.Add(1)
	_ = holder
	if err := sleepWithJitter(ctx, originLeaseFollowerWaitMin, originLeaseFollowerWaitMax); err != nil {
		return nil, TierCloud, err
	}
	if entry, perr := cm.getFromTier(ctx, path, TierPeer); perr == nil {
		cm.herdStats.followerPeerHits.Add(1)
		return entry, TierPeer, nil
	}
	// Leader slow or failed: advisory lease must not strand the read.
	cm.herdStats.followerCloudFallback.Add(1)
	entry, rerr := cm.getFromTier(ctx, path, TierCloud)
	return entry, TierCloud, rerr
}

// getFromRemoteTier is getFromTier with the origin lease applied to cloud
// reads. Ordered (non-hedged) remote fallbacks go through here so a herd of
// simultaneous misses collapses to one origin pull; hedged reads keep their
// own bounded concurrency and stay lease-free. The returned tier is where the
// data actually came from.
func (cm *DefaultCacheManager) getFromRemoteTier(ctx context.Context, path string, tier CacheTier) (*CacheEntry, CacheTier, error) {
	if tier == TierCloud {
		return cm.getFromCloudLeased(ctx, path)
	}
	entry, err := cm.getFromTier(ctx, path, tier)
	return entry, tier, err
}

// peerBusyChunkRetryMin/Max bound the jittered wait before re-trying a
// busy peer tier on the ordered chunk fallback. Short on purpose: it must
// stay well under one cloud chunk fetch (~400ms for 4MiB at 10MB/s observed)
// or the retry costs more than the fallback it avoids.
const (
	peerBusyChunkRetryMinWait = 20 * time.Millisecond
	peerBusyChunkRetryMaxWait = 80 * time.Millisecond
)

// getFromRemoteTierWithBusyRetry is getFromRemoteTier plus one jittered
// same-tier retry when the peer tier failed busy. Rationale: on the chunk
// fallback path, "peer busy" almost always means the (often only) holder is
// momentarily at its serve gate — the data is provably there, since sibling
// chunks are being served from it. Falling straight to cloud turns a ~5ms
// peer read into a ~400ms origin read (observed 79-197 cloud leaks per 5GB
// cold read). One short retry drains the vast majority of these; a genuinely
// overloaded peer still fails busy again and the caller proceeds to cloud.
func (cm *DefaultCacheManager) getFromRemoteTierWithBusyRetry(ctx context.Context, path string, tier CacheTier) (*CacheEntry, CacheTier, error) {
	entry, servedTier, err := cm.getFromRemoteTier(ctx, path, tier)
	if err == nil || tier != TierPeer || !isPeerBusy(err) || ctx.Err() != nil {
		return entry, servedTier, err
	}
	cm.herdStats.busyChunkRetries.Add(1)
	if serr := sleepWithJitter(ctx, peerBusyChunkRetryMinWait, peerBusyChunkRetryMaxWait); serr != nil {
		return nil, tier, err
	}
	entry, servedTier, retryErr := cm.getFromRemoteTier(ctx, path, tier)
	if retryErr == nil {
		cm.herdStats.busyChunkRetryHits.Add(1)
	}
	return entry, servedTier, retryErr
}

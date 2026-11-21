package coordinator

import (
	"context"
	"errors"
	"sync/atomic"
	"time"
)

// Holder lifecycle: generations and leases on location rows.
//
// A location row used to mean "this peer held the file at some point": it
// was written on admission and removed only by an eviction notice, which a
// node that crashed, lost the network or dropped a failed retraction never
// sent. Readers were sent to holders that were gone, and the replica
// reconciler counted them.
//
// Each node-local ("nvme") row now carries:
//
//   - Generation: the publishing node's admission counter for that path. A
//     retraction names the generation it evicted and removes only rows at or
//     below it, and an admission older than the row already stored is
//     ignored. A late eviction notice therefore cannot remove the row of a
//     copy admitted again after it, in either order of arrival.
//   - ExpiresAt: a lease. The holder renews it while it keeps the bytes
//     (the node's inventory reconcile). Past it the row is hidden from every
//     reader and holder count, and the coordinator's sweep removes it after
//     a grace period. A node that dies stops renewing, so its rows age out
//     without any notice from it.
//   - Partial: the node holds some chunks of the file. Readers may still ask
//     it for those chunks; it never counts as a whole copy.
//
// Rows from older clients carry neither (generation 0, no lease) and behave
// as before: never expire, any retraction removes them.

// leaseSweepGrace is how long an expired row is kept (hidden) before the
// sweep deletes it, so a node that renews just late keeps its row.
const leaseSweepGrace = 2 * time.Minute

// sweepMaxPerPass bounds deletions per sweep so a partition that expires a
// whole node's rows at once becomes a steady trickle of metadata writes.
const sweepMaxPerPass = 2000

// expired reports whether loc's lease has lapsed at now.
func expired(loc *FileLocation, now time.Time) bool {
	return loc != nil && !loc.ExpiresAt.IsZero() && now.After(loc.ExpiresAt)
}

// liveLocations drops rows whose lease has lapsed.
func liveLocations(locs []*FileLocation, now time.Time) []*FileLocation {
	out := make([]*FileLocation, 0, len(locs))
	for _, l := range locs {
		if l == nil || expired(l, now) {
			continue
		}
		lc := *l
		out = append(out, &lc)
	}
	return out
}

// liveLocationMap applies liveLocations per path, dropping paths left empty.
func liveLocationMap(m map[string][]*FileLocation, now time.Time) map[string][]*FileLocation {
	out := make(map[string][]*FileLocation, len(m))
	for p, locs := range m {
		if live := liveLocations(locs, now); len(live) > 0 {
			out[p] = live
		}
	}
	return out
}

// retractCovers reports whether a retraction at generation removes loc: an
// unconditional retraction (0) removes any row, a conditional one only rows
// at or below its generation.
func retractCovers(loc *FileLocation, generation uint64) bool {
	return generation == 0 || loc.Generation <= generation
}

// staleAdmission reports whether incoming is an older admission than the row
// already stored for the same peer and tier (a delayed publish that must not
// replace a newer one). Rows without a generation never block.
func staleAdmission(existing, incoming *FileLocation) bool {
	return existing.Generation != 0 && incoming.Generation != 0 && incoming.Generation < existing.Generation
}

// DeletePeerTierLocationGen retracts peerID's rows in one tier at or below
// generation (0: all of them).
func (cs *CoordinatorService) DeletePeerTierLocationGen(ctx context.Context, filePath, peerID, tier string, generation uint64) error {
	return cs.store.DeletePeerTierLocation(ctx, filePath, peerID, tier, generation)
}

// LeaseStats is exported on the coordinator's /metrics.
type LeaseStats struct {
	Swept, SweepPasses, SweepCapped, AuditRemoved int64
}

type leaseSweeper struct {
	swept, passes, capped, audited atomic.Int64
}

// SweepExpiredLocations removes rows whose lease lapsed more than
// leaseSweepGrace ago, at most sweepMaxPerPass per call. Readers already do
// not see them; this only reclaims the metadata.
func (cs *CoordinatorService) SweepExpiredLocations(ctx context.Context) int {
	cs.leases.passes.Add(1)
	all, err := cs.store.RangeFileLocations(ctx, "")
	if err != nil {
		return 0
	}
	cutoff := time.Now().Add(-leaseSweepGrace)
	n := 0
	for path, locs := range all {
		for _, l := range locs {
			if l == nil || l.ExpiresAt.IsZero() || !cutoff.After(l.ExpiresAt) {
				continue
			}
			if n >= sweepMaxPerPass {
				cs.leases.capped.Add(1)
				return n
			}
			if err := cs.store.DeletePeerTierLocation(ctx, path, l.PeerID, l.StorageTier, l.Generation); err == nil {
				n++
				cs.leases.swept.Add(1)
			}
		}
	}
	return n
}

// LeaseStats returns sweep counters.
func (cs *CoordinatorService) LeaseStats() LeaseStats {
	return LeaseStats{Swept: cs.leases.swept.Load(), SweepPasses: cs.leases.passes.Load(), SweepCapped: cs.leases.capped.Load(), AuditRemoved: cs.leases.audited.Load()}
}

// runLeaseSweeper sweeps expired rows every interval until ctx ends.
func (cs *CoordinatorService) runLeaseSweeper(ctx context.Context, interval time.Duration) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if n := cs.SweepExpiredLocations(ctx); n > 0 {
				cs.logger.Printf("Lease sweep: removed %d expired holder rows", n)
			}
		}
	}
}

// Holder audit: the coordinator compares a node's rows with the node's own
// inventory.
//
// Leases bound how long a wrong row can live, but only from the node's side:
// a node that renews keeps every row it once published alive, including
// rows for files it no longer has whose retraction was lost and is no longer
// pending (a restart drops the pending set). The renewal pass now ends with
// the node's full inventory (POST /api/holders/audit): the coordinator
// removes this node's nvme rows for paths not in it, at or below the row's
// own generation, so the node's newest view always wins.

// HolderAuditRequest is a node's inventory: every parent path it holds on
// local NVMe (whole or partial).
type HolderAuditRequest struct {
	PeerID string   `json:"peer_id"`
	Paths  []string `json:"paths"`
}

// HolderAuditResult reports what the audit removed.
type HolderAuditResult struct {
	Checked int      `json:"checked"`
	Removed int      `json:"removed"`
	Paths   []string `json:"paths,omitempty"`
}

// auditMaxPaths bounds one request (a node with more objects sends its
// inventory but the audit only trusts it up to this size).
const auditMaxPaths = 200000

// AuditHolder removes peerID's nvme rows for paths not in held.
func (cs *CoordinatorService) AuditHolder(ctx context.Context, req HolderAuditRequest) (HolderAuditResult, error) {
	var res HolderAuditResult
	if req.PeerID == "" || len(req.Paths) > auditMaxPaths {
		return res, errAuditRequest
	}
	held := make(map[string]struct{}, len(req.Paths))
	for _, p := range req.Paths {
		held[p] = struct{}{}
	}
	all, err := cs.store.RangeFileLocations(ctx, "")
	if err != nil {
		return res, err
	}
	for path, locs := range all {
		for _, l := range locs {
			if l == nil || l.PeerID != req.PeerID || l.StorageTier != "nvme" {
				continue
			}
			res.Checked++
			if _, ok := held[path]; ok {
				continue
			}
			if err := cs.store.DeletePeerTierLocation(ctx, path, req.PeerID, "nvme", l.Generation); err == nil {
				res.Removed++
				if len(res.Paths) < 50 {
					res.Paths = append(res.Paths, path)
				}
			}
		}
	}
	cs.leases.audited.Add(int64(res.Removed))
	if res.Removed > 0 {
		cs.logger.Printf("Holder audit %s: removed %d rows the node no longer backs", req.PeerID, res.Removed)
	}
	return res, nil
}

var errAuditRequest = errors.New("audit needs peer_id and at most 200000 paths")

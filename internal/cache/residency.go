package cache

import (
	"context"
	"path"
	"strings"
	"time"
)

// Node-local residency reporting.
//
// "Is this dataset on this node, and how completely" is the question an
// external scheduler has to answer before it can prefer a node that can
// cold-start at NVMe speed instead of paying peer or cloud latency. Nothing
// existing answers it honestly:
//
//   - Coordinator location metadata over-claims. A node publishes itself as a
//     holder on write and on warm, but LRU eviction (deleteLocalNVMeFootprint)
//     does NOT retract those rows — only an explicit delete or rename does.
//     So a location row means "held at some point", not "held now".
//   - GetWorldView aggregates the same rows, so it inherits the same optimism.
//   - A warm intent's last-pass result is a claim about a moment, and the
//     moment may have passed.
//
// The only authority on current residency is the node's own disk. This walks
// the coordinator's enumeration for the prefix and stats each target locally,
// which is a filesystem stat per file — cheap next to the peer/cloud fetch a
// wrong routing decision costs, but not free, so callers should cache it (the
// coordinator side does).
//
// The report deliberately carries both numbers: what the node actually holds,
// and what the intent claimed. A router that sees them diverge learns
// something true about the node; a router shown only the claim cannot.

// ResidencyReport is this node's answer for one prefix.
type ResidencyReport struct {
	Prefix string `json:"prefix"`
	PeerID string `json:"peer_id,omitempty"`
	// Files/ResidentFiles and Bytes/ResidentBytes come from stat-ing each
	// enumerated target against local NVMe — measured, not claimed.
	Files         int     `json:"files"`
	ResidentFiles int     `json:"resident_files"`
	Bytes         int64   `json:"bytes"`
	ResidentBytes int64   `json:"resident_bytes"`
	Fraction      float64 `json:"fraction"` // ResidentBytes/Bytes, 0..1
	// Pinned reports whether an active session pins this prefix, i.e. whether
	// eviction is allowed to take these bytes back.
	Pinned bool `json:"pinned"`
	// Declared/LastVerified describe the warm intent covering this prefix, if
	// any. LastVerified is the last CLEAN reconcile pass — never refreshed by
	// a failed one — so a stale timestamp beside a high Fraction means "the
	// bytes are here but nobody has re-checked recently", which is a different
	// state from "verified empty" and must not be flattened into it.
	Declared     bool      `json:"declared"`
	LastVerified time.Time `json:"last_verified,omitempty"`
	MeasuredAt   time.Time `json:"measured_at"`
}

// PrefixResidency reports what this node currently holds under prefix.
// Enumeration comes from the coordinator (the same source warmup uses), so a
// coordinator failure is an error rather than a silently empty report — an
// empty report is indistinguishable from "node holds nothing", which is the
// exact confusion this endpoint exists to prevent.
func (cm *DefaultCacheManager) PrefixResidency(ctx context.Context, prefix string) (*ResidencyReport, error) {
	prefix = path.Clean("/" + strings.TrimSpace(prefix))
	rep := &ResidencyReport{
		Prefix:     prefix,
		PeerID:     cm.config.LocalPeerID,
		MeasuredAt: time.Now(),
	}
	if cm.config.Coordinator == nil {
		return nil, errNoCoordinator
	}
	locations, err := cm.config.Coordinator.ListFileLocations(ctx, prefix)
	if err != nil {
		return nil, err
	}

	// Fold chunk rows into their parent, matching warmup enumeration: a
	// chunked file is resident when the assembled parent is on disk.
	sizeByPath := make(map[string]int64)
	for _, loc := range locations {
		if loc == nil || loc.FilePath == "" {
			continue
		}
		if _, isChunk := parentFilePathFromChunkPath(loc.FilePath); isChunk {
			continue
		}
		if loc.FileSize > sizeByPath[loc.FilePath] {
			sizeByPath[loc.FilePath] = loc.FileSize
		}
	}

	for p, size := range sizeByPath {
		rep.Files++
		rep.Bytes += size
		if _, ok := cm.LocalFilePath(ctx, p); !ok {
			continue
		}
		rep.ResidentFiles++
		// Trust the local file's own size over the coordinator's when we can
		// read it: the row may predate the last write.
		if actual, serr := cm.nvmeStorage.Size(ctx, p); serr == nil && actual > 0 {
			rep.ResidentBytes += actual
		} else {
			rep.ResidentBytes += size
		}
	}
	if rep.Bytes > 0 {
		rep.Fraction = float64(rep.ResidentBytes) / float64(rep.Bytes)
		if rep.Fraction > 1 {
			rep.Fraction = 1
		}
	} else if rep.Files > 0 && rep.ResidentFiles == rep.Files {
		// Zero-byte files: every one present is full residency, not 0%.
		rep.Fraction = 1
	}

	if cm.config.PinnedPrefixesFn != nil {
		rep.Pinned = isPathPinned(prefix, cm.config.PinnedPrefixesFn())
	}
	// Attach the covering warm intent, if one declared this prefix or an
	// ancestor of it.
	for _, in := range cm.WarmIntents() {
		if in.Prefix == prefix || strings.HasPrefix(prefix, strings.TrimSuffix(in.Prefix, "/")+"/") {
			rep.Declared = true
			if in.LastVerified.After(rep.LastVerified) {
				rep.LastVerified = in.LastVerified
			}
		}
	}
	return rep, nil
}

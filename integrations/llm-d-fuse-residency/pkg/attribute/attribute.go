// Package attribute declares the FuseResidency attribute that carries, for one
// candidate endpoint, how completely the node under it already holds the model
// weights on local NVMe.
//
// It is a separate package from the extractor and the scorer for the same
// reason llm-d splits attribute/topology from extractor/topology: it is the
// only thing the producer and the consumer must agree on, and keeping it free
// of both their dependencies means neither can drag the other into its imports.
package attribute

import (
	fwkdl "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/datalayer"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/interface/plugin"
)

const (
	// ExtractorType is the plugin type identifier for the residency extractor.
	ExtractorType = "fuse-residency-extractor"
	// ScorerType is the plugin type identifier for the residency scorer.
	ScorerType = "fuse-residency-scorer"
)

// AttributeKey identifies the FuseResidency attribute stored on each endpoint.
var AttributeKey = plugin.NewDataKey("FuseResidency", ExtractorType)

// FuseResidency is what the fuse cache knows about one node's copy of a model.
//
// The three-state design is the whole point. A scorer that receives only a
// float cannot tell "this node measured 0% resident" from "nobody could measure
// this node", and those call for opposite routing decisions. So Known carries
// that distinction explicitly rather than encoding it as a sentinel fraction.
type FuseResidency struct {
	// Known is false when the fuse cache could not report on this node — the
	// coordinator was unreachable, the peer timed out, or no fuse peer is
	// registered on this node at all. When false, Fraction is meaningless and
	// must not be read as 0.
	Known bool

	// Fraction is the byte-weighted share of the model prefix resident on this
	// node's local NVMe, in [0,1]. Byte-weighted because load time tracks bytes:
	// a node holding 9 small config files and missing the one multi-GB shard is
	// nearly cold, though a file count would call it 90% warm.
	Fraction float64

	// ResidentBytes and TotalBytes are the raw measurements behind Fraction,
	// kept for logging and metrics. A router that only ever sees a ratio cannot
	// tell a fully-warm 200MB model from a fully-warm 70GB one, and the second
	// is where this signal is worth acting on.
	ResidentBytes int64
	TotalBytes    int64

	// Pinned reports that a warm intent pins this prefix on the node, so LRU
	// eviction will not reclaim it. An unpinned high fraction is a snapshot; a
	// pinned one is a commitment.
	Pinned bool

	// Declared reports that a warm intent covers this prefix on the node, and
	// LastVerifiedAgeSec is how long ago that intent last completed a CLEAN
	// reconcile pass — nonzero only when Declared. Measured residency and the
	// declared claim are carried side by side on purpose: when they diverge, a
	// consumer learns something true about the node, whereas one shown only the
	// claim cannot tell the difference.
	Declared           bool
	LastVerifiedAgeSec float64

	// Prefix is the model path this residency was measured against, carried so
	// a mismatch between what was polled and what a request asks for is
	// detectable at score time rather than silently scoring the wrong model.
	Prefix string

	// NodeIP is the node whose disk was measured — the value this attribute was
	// joined on. Retained for debugging a bad join, which otherwise presents as
	// uniformly zero scores with no indication of why.
	NodeIP string
}

// Clone implements fwkdl.Cloneable.
func (r *FuseResidency) Clone() fwkdl.Cloneable {
	if r == nil {
		return nil
	}
	cp := *r
	return &cp
}

// Package scorer provides the Affinity-category scorer that prefers endpoints
// whose node already holds the requested model's weights on local NVMe.
//
// # What this scores, and what it does not
//
// llm-d's prefix-cache scorers answer "who already has this conversation's KV
// blocks in GPU memory". This one answers a different question: "who can load
// this model's weights at NVMe speed instead of pulling them from object
// storage". The two compose rather than compete — one is about a warm request,
// the other about a warm node — which is why this is registered alongside
// prefix-cache-scorer with its own weight rather than replacing it.
//
// It matters most where cold is catastrophic rather than merely slow: JuiceFS
// measured 8 IOPS on 4KB cold random reads against object storage versus about
// 12,000 warmed into local cache. For a multi-GB model that is the difference
// between a pod serving in seconds and one serving in minutes.
//
// # Why unknown scores neutral rather than zero
//
// The framework accumulates `weightedScore[endpoint] += score * weight` across
// scorers, and an endpoint this scorer omits from its map contributes nothing —
// which is arithmetically identical to scoring it 0. So "I don't know" cannot
// be expressed by staying silent, and expressing it as 0 would actively
// penalize a node for the crime of being unreachable by the coordinator for a
// few seconds. It is scored at a configurable neutral value instead: enough not
// to bury an unmeasured node, not enough to beat a measured warm one.
package scorer

import (
	"context"
	"encoding/json"
	"fmt"

	fwkdl "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/datalayer"
	fwkplugin "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/plugin"
	fwksched "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/scheduling"

	"github.com/nilesh/llm-d-fuse-residency/pkg/attribute"
	"github.com/nilesh/llm-d-fuse-residency/pkg/extractor"
)

// defaultUnknownScore places an unmeasured node just below the midpoint: it
// loses to any node measured above 40% resident and beats anything colder. The
// alternative of 0 would make a coordinator blip look like a cold node, and 1
// would make unmeasured nodes win outright — the failure mode where a broken
// signal silently becomes the strongest one.
const defaultUnknownScore = 0.4

type parameters struct {
	// ExtractorName selects which residency extractor instance to read from.
	// Defaults to the extractor's default producer name.
	ExtractorName string `json:"extractorName,omitempty"`
	// UnknownScore is the score given to an endpoint whose residency could not
	// be measured. Defaults to 0.4. Set explicitly to 0 only if you would
	// rather deroute unmeasured nodes than risk sending traffic to a cold one.
	UnknownScore *float64 `json:"unknownScore,omitempty"`
	// PinnedBonus is added to the score of a fully-resident node whose prefix
	// is pinned by a warm intent, then clamped to 1. Defaults to 0 — pinning
	// predicts that residency will persist, which matters for a long-lived
	// scheduling decision but not for scoring a single request.
	PinnedBonus float64 `json:"pinnedBonus,omitempty"`
}

var (
	_ fwksched.Scorer          = (*Scorer)(nil)
	_ fwkplugin.ConsumerPlugin = (*Scorer)(nil)
)

// Scorer grades endpoints by how completely their node holds the requested
// model.
type Scorer struct {
	typedName    fwkplugin.TypedName
	dataKey      fwkplugin.DataKey
	unknownScore float64
	pinnedBonus  float64
}

// Factory creates the fuse residency scorer.
func Factory(name string, rawParameters *json.Decoder, _ fwkplugin.Handle) (fwkplugin.Plugin, error) {
	p := parameters{}
	if rawParameters != nil {
		if err := rawParameters.Decode(&p); err != nil {
			return nil, fmt.Errorf("failed to parse the parameters of the '%s' scorer - %w", attribute.ScorerType, err)
		}
	}
	unknown := defaultUnknownScore
	if p.UnknownScore != nil {
		if *p.UnknownScore < 0 || *p.UnknownScore > 1 {
			return nil, fmt.Errorf("'%s': unknownScore must be in [0,1], got %v", attribute.ScorerType, *p.UnknownScore)
		}
		unknown = *p.UnknownScore
	}
	if p.PinnedBonus < 0 || p.PinnedBonus > 1 {
		return nil, fmt.Errorf("'%s': pinnedBonus must be in [0,1], got %v", attribute.ScorerType, p.PinnedBonus)
	}
	if name == "" {
		name = attribute.ScorerType
	}
	return &Scorer{
		typedName:    fwkplugin.TypedName{Type: attribute.ScorerType, Name: name},
		dataKey:      attribute.AttributeKey.WithNonEmptyProducerName(p.ExtractorName),
		unknownScore: unknown,
		pinnedBonus:  p.PinnedBonus,
	}, nil
}

// TypedName returns the plugin type and name.
func (s *Scorer) TypedName() fwkplugin.TypedName {
	return s.typedName
}

// Consumes declares the residency attribute as optional. If no extractor is
// configured the framework logs a startup warning rather than failing: this
// scorer degrades to scoring every endpoint neutrally, which is a survivable
// state, and refusing to start the router over a missing cache-locality hint
// would be a worse outcome than routing without it.
func (s *Scorer) Consumes() fwkplugin.DataDependencies {
	return fwkplugin.DataDependencies{
		Optional: map[fwkplugin.DataKey]any{s.dataKey: extractor.ModelResidency{}},
	}
}

// Category reports Affinity: this scorer pulls requests toward nodes with
// existing locality, so the framework can balance it against Distribution
// scorers rather than letting it silently monopolize one warm node.
func (s *Scorer) Category() fwksched.ScorerCategory {
	return fwksched.Affinity
}

// Score returns the byte-weighted resident fraction of the requested model on
// each endpoint's node.
//
// Every endpoint is always present in the returned map — including the unknown
// ones. Omitting an endpoint would let the framework's `+=` accumulation treat
// it as a hard zero, which is the opposite of what an absent measurement means.
func (s *Scorer) Score(_ context.Context, request *fwksched.InferenceRequest, endpoints []fwksched.Endpoint) map[fwksched.Endpoint]float64 {
	scores := make(map[fwksched.Endpoint]float64, len(endpoints))

	// No target model means nothing to look up. Neutral across the board rather
	// than zero: this scorer has no opinion, and an opinion of "all bad" is not
	// the same as no opinion.
	model := ""
	if request != nil {
		model = request.TargetModel
	}
	if model == "" {
		for _, ep := range endpoints {
			scores[ep] = s.unknownScore
		}
		return scores
	}

	for _, ep := range endpoints {
		scores[ep] = s.scoreOne(ep, model)
	}
	return scores
}

func (s *Scorer) scoreOne(ep fwksched.Endpoint, model string) float64 {
	set, ok := fwkdl.ReadAttribute[extractor.ModelResidency](ep, s.dataKey.String())
	if !ok {
		return s.unknownScore
	}
	// A model absent from the set was never configured on the extractor, so
	// nothing is known about its residency anywhere — unknown, not cold.
	r, ok := set[model]
	if !ok || r == nil || !r.Known {
		return s.unknownScore
	}

	score := r.Fraction
	// The pinned bonus applies only to a fully-resident prefix. Pinning a
	// partially-warm node predicts that the fraction will hold, not that it
	// will grow, so rewarding a pinned-but-incomplete node would advertise a
	// completeness it does not have.
	if s.pinnedBonus > 0 && r.Pinned && r.Fraction >= 1 {
		score += s.pinnedBonus
	}
	switch {
	case score < 0:
		return 0
	case score > 1:
		return 1
	default:
		return score
	}
}

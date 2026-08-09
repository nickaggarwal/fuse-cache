package scorer

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	fwkdl "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/datalayer"
	fwkplugin "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/plugin"
	fwksched "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/scheduling"
	"k8s.io/apimachinery/pkg/types"

	"github.com/nilesh/llm-d-fuse-residency/pkg/attribute"
	"github.com/nilesh/llm-d-fuse-residency/pkg/extractor"
)

const testModel = "Qwen/Qwen2.5-7B-Instruct"

func newScorer(t *testing.T, params string) *Scorer {
	t.Helper()
	var dec *json.Decoder
	if params != "" {
		dec = fwkplugin.StrictDecoder(json.RawMessage(params))
	}
	p, err := Factory("", dec, nil)
	if err != nil {
		t.Fatalf("Factory(%s): %v", params, err)
	}
	s, ok := p.(*Scorer)
	if !ok {
		t.Fatalf("Factory returned %T, want *Scorer", p)
	}
	return s
}

// endpointWith builds a candidate endpoint carrying the given residency set.
// A nil set means the extractor never stamped this endpoint at all.
func endpointWith(name string, set extractor.ModelResidency) fwksched.Endpoint {
	meta := &fwkdl.EndpointMetadata{ID: types.NamespacedName{Name: name, Namespace: "default"}}
	attrs := fwkdl.NewAttributes()
	ep := fwksched.NewEndpoint(meta, &fwkdl.Metrics{}, attrs)
	if set != nil {
		ep.Put(attribute.AttributeKey.String(), set)
	}
	return ep
}

func resident(fraction float64) extractor.ModelResidency {
	return extractor.ModelResidency{testModel: &attribute.FuseResidency{
		Known: true, Fraction: fraction, Prefix: "models/qwen/", NodeIP: "10.224.0.7",
	}}
}

func request(model string) *fwksched.InferenceRequest {
	return &fwksched.InferenceRequest{TargetModel: model}
}

// TestScore_TracksResidentFraction: the score IS the byte-weighted fraction.
// Anything else would make the number mean something other than what the
// coordinator measured, and the whole design rests on the score being a
// faithful pass-through of a measurement rather than a derived heuristic.
func TestScore_TracksResidentFraction(t *testing.T) {
	warm := endpointWith("warm", resident(1.0))
	partial := endpointWith("partial", resident(0.75))
	cold := endpointWith("cold", resident(0.0))

	s := newScorer(t, "")
	got := s.Score(context.Background(), request(testModel), []fwksched.Endpoint{warm, partial, cold})

	for ep, want := range map[fwksched.Endpoint]float64{warm: 1.0, partial: 0.75, cold: 0.0} {
		if got[ep] != want {
			t.Errorf("score = %v, want %v", got[ep], want)
		}
	}
}

// TestScore_UnknownIsNeutralNotZero is the central invariant.
//
// The framework accumulates `weighted[ep] += score * weight`, so an endpoint
// scored 0 and an endpoint omitted from the map are arithmetically identical.
// That means "unmeasured" MUST be a distinct positive value, or a node that the
// coordinator simply could not reach for a few seconds gets penalized exactly
// as hard as one verified to hold none of the model. Those two states call for
// opposite routing decisions.
func TestScore_UnknownIsNeutralNotZero(t *testing.T) {
	// Each case names the model to ask about, because "model absent" is only
	// unknown when the request asks for a model the extractor never polled.
	cases := map[string]struct {
		ep    fwksched.Endpoint
		model string
	}{
		"never stamped":     {endpointWith("nostamp", nil), testModel},
		"stamped not known": {endpointWith("unknown", extractor.ModelResidency{testModel: &attribute.FuseResidency{Known: false}}), testModel},
		"model absent":      {endpointWith("othermodel", resident(1.0)), "meta-llama/Llama-3-8B"},
		"nil reading":       {endpointWith("nilreading", extractor.ModelResidency{testModel: nil}), testModel},
	}

	s := newScorer(t, "")
	for label, tc := range cases {
		ep := tc.ep
		// The reference point: a node measured to hold none of the model asked
		// about. Built per-case so it is always scored against the same model.
		cold := endpointWith("cold", extractor.ModelResidency{tc.model: &attribute.FuseResidency{Known: true, Fraction: 0}})
		got := s.Score(context.Background(), request(tc.model), []fwksched.Endpoint{ep, cold})

		if _, present := got[ep]; !present {
			t.Errorf("%s: endpoint missing from score map — omission reads as a hard zero to the framework", label)
		}
		if got[ep] != defaultUnknownScore {
			t.Errorf("%s: score = %v, want neutral %v", label, got[ep], defaultUnknownScore)
		}
		if got[ep] <= got[cold] {
			t.Errorf("%s: unmeasured (%v) must outrank measured-cold (%v)", label, got[ep], got[cold])
		}
	}
}

// TestScore_UnknownLosesToAMeasuredWarmNode: neutral must not be so generous
// that an unmeasured node beats a node known to be warm. If it did, a broken
// signal would quietly become the strongest one in the chain.
func TestScore_UnknownLosesToAMeasuredWarmNode(t *testing.T) {
	unknown := endpointWith("unknown", nil)
	warm := endpointWith("warm", resident(1.0))
	half := endpointWith("half", resident(0.5))

	s := newScorer(t, "")
	got := s.Score(context.Background(), request(testModel), []fwksched.Endpoint{unknown, warm, half})

	if got[unknown] >= got[warm] {
		t.Errorf("unknown (%v) must lose to fully resident (%v)", got[unknown], got[warm])
	}
	if got[unknown] >= got[half] {
		t.Errorf("unknown (%v) must lose to half resident (%v)", got[unknown], got[half])
	}
}

// TestScore_NoTargetModelIsNeutralEverywhere: with no model named there is
// nothing to look up, so the scorer has no opinion. Expressing no-opinion as
// all-zeros would be an opinion — it would drag every endpoint's total down by
// this scorer's full weight and shift the balance toward the other scorers in a
// way that depends on their weights.
func TestScore_NoTargetModelIsNeutralEverywhere(t *testing.T) {
	warm := endpointWith("warm", resident(1.0))
	cold := endpointWith("cold", resident(0.0))

	s := newScorer(t, "")
	got := s.Score(context.Background(), request(""), []fwksched.Endpoint{warm, cold})

	if got[warm] != defaultUnknownScore || got[cold] != defaultUnknownScore {
		t.Errorf("no target model: got warm=%v cold=%v, want both %v", got[warm], got[cold], defaultUnknownScore)
	}
}

// TestScore_PinnedBonusOnlyWhenFullyResident: pinning predicts that residency
// will persist, not that it will grow. Boosting a pinned-but-partial node would
// advertise a completeness it does not have.
func TestScore_PinnedBonusOnlyWhenFullyResident(t *testing.T) {
	pinnedFull := endpointWith("pinned-full", extractor.ModelResidency{testModel: &attribute.FuseResidency{
		Known: true, Fraction: 1.0, Pinned: true,
	}})
	pinnedPartial := endpointWith("pinned-partial", extractor.ModelResidency{testModel: &attribute.FuseResidency{
		Known: true, Fraction: 0.6, Pinned: true,
	}})
	unpinnedFull := endpointWith("unpinned-full", resident(1.0))

	s := newScorer(t, `{"pinnedBonus":0.2}`)
	got := s.Score(context.Background(), request(testModel), []fwksched.Endpoint{pinnedFull, pinnedPartial, unpinnedFull})

	// Clamped to 1: the bonus breaks ties within the framework's range, it does
	// not escape it.
	if got[pinnedFull] != 1.0 {
		t.Errorf("pinned+full = %v, want 1.0 (clamped)", got[pinnedFull])
	}
	if got[pinnedPartial] != 0.6 {
		t.Errorf("pinned+partial = %v, want unmodified 0.6", got[pinnedPartial])
	}
	if got[unpinnedFull] != 1.0 {
		t.Errorf("unpinned+full = %v, want 1.0", got[unpinnedFull])
	}
}

// TestScore_ClampsOutOfRangeFraction: the framework clamps too, but a scorer
// that can emit >1 would silently overweight itself against every other scorer
// on any framework version that stopped clamping.
func TestScore_ClampsOutOfRangeFraction(t *testing.T) {
	over := endpointWith("over", extractor.ModelResidency{testModel: &attribute.FuseResidency{Known: true, Fraction: 1.8}})
	under := endpointWith("under", extractor.ModelResidency{testModel: &attribute.FuseResidency{Known: true, Fraction: -0.5}})

	s := newScorer(t, "")
	got := s.Score(context.Background(), request(testModel), []fwksched.Endpoint{over, under})

	if got[over] != 1.0 {
		t.Errorf("fraction 1.8 scored %v, want clamp to 1.0", got[over])
	}
	if got[under] != 0.0 {
		t.Errorf("fraction -0.5 scored %v, want clamp to 0.0", got[under])
	}
}

// TestScore_EveryEndpointAppears: no code path may drop an endpoint, for the
// same += reason. This guards the property directly rather than relying on each
// individual case above to remember to check it.
func TestScore_EveryEndpointAppears(t *testing.T) {
	eps := []fwksched.Endpoint{
		endpointWith("a", resident(1.0)),
		endpointWith("b", nil),
		endpointWith("c", extractor.ModelResidency{}),
		endpointWith("d", resident(0.0)),
	}
	s := newScorer(t, "")
	got := s.Score(context.Background(), request(testModel), eps)

	if len(got) != len(eps) {
		t.Fatalf("scored %d endpoints, want %d", len(got), len(eps))
	}
	for _, ep := range eps {
		if _, ok := got[ep]; !ok {
			t.Errorf("endpoint %v absent from score map", ep.GetMetadata().ID)
		}
	}
}

func TestScorer_Category(t *testing.T) {
	if got := newScorer(t, "").Category(); got != fwksched.Affinity {
		t.Errorf("Category() = %v, want Affinity", got)
	}
}

// TestScorer_ConsumesIsOptional: a missing extractor must warn, not fail
// startup. Refusing to route at all because a cache-locality hint is
// unavailable would be a worse outcome than routing without the hint.
func TestScorer_ConsumesIsOptional(t *testing.T) {
	c := newScorer(t, "").Consumes()
	if len(c.Required) != 0 {
		t.Errorf("Required = %v, want empty", c.Required)
	}
	if len(c.Optional) != 1 {
		t.Fatalf("Optional has %d keys, want 1", len(c.Optional))
	}
}

func TestFactory_RejectsOutOfRangeParameters(t *testing.T) {
	for _, params := range []string{`{"unknownScore":1.5}`, `{"unknownScore":-0.1}`, `{"pinnedBonus":2}`} {
		if _, err := Factory("", fwkplugin.StrictDecoder(json.RawMessage(params)), nil); err == nil {
			t.Errorf("Factory(%s) succeeded, want a range error", params)
		}
	}
}

// TestFactory_UnknownScoreZeroIsRespected: 0 is a legitimate explicit choice
// (deroute anything unmeasured), and it must not be swallowed by the
// zero-value-means-default reading — hence the *float64.
func TestFactory_UnknownScoreZeroIsRespected(t *testing.T) {
	s := newScorer(t, `{"unknownScore":0}`)
	if s.unknownScore != 0 {
		t.Errorf("unknownScore = %v, want the explicitly configured 0", s.unknownScore)
	}
}

func TestFactory_RejectsUnknownFields(t *testing.T) {
	_, err := Factory("", fwkplugin.StrictDecoder(json.RawMessage(`{"unknownScor":0.5}`)), nil)
	if err == nil {
		t.Fatal("a typo'd parameter was accepted; strict decoding should reject it")
	}
	if !strings.Contains(err.Error(), "unknownScor") {
		t.Errorf("error %q should name the offending field", err)
	}
}

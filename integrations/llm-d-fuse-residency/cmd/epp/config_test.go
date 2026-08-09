package main

import (
	"context"
	"os"
	"strings"
	"testing"

	"github.com/llm-d/llm-d-router/pkg/epp/config/loader"
	fwkplugin "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/plugin"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/scheduling/filter/bylabel"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/scheduling/picker/maxscore"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/scheduling/profilehandler/single"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/scheduling/scorer/prefix"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/scheduling/scorer/queuedepth"
	testutils "github.com/llm-d/llm-d-router/test/utils"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"

	"github.com/nilesh/llm-d-fuse-residency/pkg/attribute"
)

// registerInTreePlugins registers the stock plugins the shipped config sits
// alongside. The binary gets these from Runner.Run, whose registration step is
// unexported, so the test does it directly — which also keeps the test honest
// about the claim in the README: the fuse plugins are purely additive and the
// in-tree ones still work beside them.
func registerInTreePlugins() {
	fwkplugin.Register(prefix.PrefixCacheScorerPluginType, fwkplugin.StabilityBeta, prefix.PrefixCachePluginFactory)
	fwkplugin.Register(queuedepth.QueueScorerType, fwkplugin.StabilityBeta, queuedepth.QueueScorerFactory)
	fwkplugin.Register(bylabel.DecodeRoleType, fwkplugin.StabilityBeta, bylabel.DecodeRoleFactory)
	fwkplugin.Register(maxscore.MaxScorePickerType, fwkplugin.StabilityBeta, maxscore.MaxScorePickerFactory)
	fwkplugin.Register(single.SingleProfileHandlerType, fwkplugin.StabilityBeta, single.SingleProfileHandlerFactory)
}

// TestShippedConfigInstantiates is the test that catches what unit tests
// cannot: that the EndpointPickerConfig we tell operators to deploy parses and
// builds every plugin it declares, against the pinned llm-d-router, with the
// registrations main() actually makes.
//
// A config YAML has no compiler. A renamed parameter, a plugin type string that
// drifted from its Go constant, or a profile referencing a plugin that was
// never declared all fail at pod startup in a cluster, which is the expensive
// place to find out.
//
// It deliberately stops at instantiation rather than calling
// InstantiateAndConfigure. That would additionally pull in the flow-control,
// saturation-detector, and parser default graph, whose registration list is
// upstream's and churns independently of anything we control — the test would
// then fail on llm-d bumps for reasons that say nothing about this config.
func TestShippedConfigInstantiates(t *testing.T) {
	// The registry is a package-level map, so this test cannot run in parallel
	// with anything else that touches it.
	registerPlugins()
	registerInTreePlugins()

	raw, err := os.ReadFile("../../deploy/epp-config.yaml")
	if err != nil {
		t.Fatalf("read shipped config: %v", err)
	}

	logger := zap.New(zap.UseDevMode(true))
	cfg, _, err := loader.LoadRawConfig(raw, logger)
	if err != nil {
		t.Fatalf("LoadRawConfig on the shipped config: %v", err)
	}

	// Cancelled up front so the extractor's poll goroutine exits immediately
	// rather than spending the test hammering a coordinator URL that resolves
	// to nothing.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	handle := testutils.NewTestHandle(ctx)

	for _, p := range cfg.Plugins {
		factory, ok := fwkplugin.Registry[p.Type]
		if !ok {
			t.Errorf("config declares plugin type %q, which nothing registers", p.Type)
			continue
		}
		if _, err := factory(p.Name, fwkplugin.StrictDecoder(p.Parameters), handle); err != nil {
			t.Errorf("plugin %q from the shipped config failed to build: %v", p.Type, err)
		}
	}

	// Every pluginRef in a profile must name a plugin the config declares.
	declared := map[string]bool{}
	for _, p := range cfg.Plugins {
		declared[p.Name] = true
	}
	for _, profile := range cfg.SchedulingProfiles {
		for _, ref := range profile.Plugins {
			if !declared[ref.PluginRef] {
				t.Errorf("profile %q references undeclared plugin %q", profile.Name, ref.PluginRef)
			}
		}
	}
}

// TestShippedConfigScoresResidency: the scorer must actually be in the
// scheduling profile with a non-zero weight. A config that declares the plugins
// but forgets the pluginRef loads perfectly and routes exactly as it did
// before — the failure this whole module exists to avoid, in its most literal
// form.
func TestShippedConfigScoresResidency(t *testing.T) {
	raw, err := os.ReadFile("../../deploy/epp-config.yaml")
	if err != nil {
		t.Fatalf("read shipped config: %v", err)
	}
	cfg, _, err := loader.LoadRawConfig(raw, zap.New(zap.UseDevMode(true)))
	if err != nil {
		t.Fatalf("LoadRawConfig: %v", err)
	}

	var weight float64
	for _, profile := range cfg.SchedulingProfiles {
		for _, ref := range profile.Plugins {
			if ref.PluginRef == attribute.ScorerType && ref.Weight != nil {
				weight = float64(*ref.Weight)
			}
		}
	}
	if weight <= 0 {
		t.Errorf("%s has weight %v in the shipped profiles; it would be declared but never consulted",
			attribute.ScorerType, weight)
	}
}

// TestRegisterPlugins_UsesTheTypeNamesTheConfigReferences pins the link between
// the Go constants and the YAML strings. They are the same words in two files
// with no compiler between them.
func TestRegisterPlugins_UsesTheTypeNamesTheConfigReferences(t *testing.T) {
	raw, err := os.ReadFile("../../deploy/epp-config.yaml")
	if err != nil {
		t.Fatalf("read shipped config: %v", err)
	}
	for _, typ := range []string{attribute.ExtractorType, attribute.ScorerType} {
		if !strings.Contains(string(raw), typ) {
			t.Errorf("shipped config never mentions plugin type %q", typ)
		}
	}
}

// TestFactoriesAreRegisteredAlpha: both plugins gate behind
// --allow-experimental-plugins on purpose. Registering them Beta or GA would
// silently remove that opt-in from every deployment.
func TestFactoriesAreRegisteredAlpha(t *testing.T) {
	registerPlugins()

	for _, typ := range []string{attribute.ExtractorType, attribute.ScorerType} {
		if fwkplugin.Registry[typ] == nil {
			t.Errorf("plugin type %q is not in the registry after registerPlugins", typ)
		}
		if got := fwkplugin.GetPluginStability(typ); got != fwkplugin.StabilityAlpha {
			t.Errorf("plugin type %q registered %v, want Alpha — anything else drops the operator opt-in", typ, got)
		}
	}
}

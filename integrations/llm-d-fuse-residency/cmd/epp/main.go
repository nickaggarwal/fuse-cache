// Command epp is a custom llm-d EndpointPicker that additionally knows about
// fuse cache residency.
//
// llm-d has no dynamic plugin loading: the plugin registry is a package-level
// map populated by init-time calls, and the config YAML can only reference
// types already present in the binary. Integrating out-of-tree therefore means
// shipping your own EPP image.
//
// It does NOT mean forking llm-d-router, which is what the sparse docs on this
// imply. fwkplugin.Register is exported, the registry map is never reset, and
// runner.NewRunner().Run() is public — so registering here, before Run, is
// enough. Everything below is additive: every in-tree plugin still registers
// itself when the runner processes its configuration, and an EndpointPickerConfig
// that does not mention the fuse plugins behaves exactly like upstream EPP.
package main

import (
	"os"

	fwkplugin "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/plugin"
	ctrl "sigs.k8s.io/controller-runtime"

	"github.com/llm-d/llm-d-router/cmd/epp/runner"

	"github.com/nilesh/llm-d-fuse-residency/pkg/attribute"
	"github.com/nilesh/llm-d-fuse-residency/pkg/extractor"
	"github.com/nilesh/llm-d-fuse-residency/pkg/scorer"
)

func main() {
	os.Exit(run())
}

func run() int {
	ctx := ctrl.SetupSignalHandler()
	registerPlugins()

	if err := runner.NewRunner().Run(ctx); err != nil {
		return 1
	}
	return 0
}

// registerPlugins adds the two fuse plugin types to llm-d's package-level
// registry. Split out of run so a test can exercise the shipped
// EndpointPickerConfig against exactly the registrations the binary makes —
// a config YAML has no compiler, and a drifted type name or parameter would
// otherwise first surface as a CrashLoopBackOff.
//
// Both register Alpha, so a deployment must pass --allow-experimental-plugins.
// That gate is appropriate: the residency signal is new, and an operator should
// have to say so before routing decisions depend on it.
func registerPlugins() {
	fwkplugin.Register(attribute.ExtractorType, fwkplugin.StabilityAlpha, extractor.Factory)
	fwkplugin.Register(attribute.ScorerType, fwkplugin.StabilityAlpha, scorer.Factory)
}

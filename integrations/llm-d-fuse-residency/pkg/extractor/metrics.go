package extractor

import (
	"github.com/prometheus/client_golang/prometheus"

	fwkplugin "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/plugin"
	"github.com/nilesh/llm-d-fuse-residency/pkg/residency"
)

// pluginMetrics exposes what an operator needs to tell "the signal is absent"
// apart from "the signal is zero" — the same distinction the attribute encodes,
// carried out to the dashboard.
//
// The load-bearing one is nodes_unknown. If it climbs while nodes_known stays
// flat, the scorer is quietly contributing nothing and routing has fallen back
// to the other scorers' preferences, with no error anywhere to show for it.
type pluginMetrics struct {
	pollTotal    *prometheus.CounterVec
	pollErrors   *prometheus.CounterVec
	nodesKnown   *prometheus.GaugeVec
	nodesUnknown *prometheus.GaugeVec
	bestFraction *prometheus.GaugeVec
}

func newPluginMetrics(reg fwkplugin.MetricsRecorder) *pluginMetrics {
	m := &pluginMetrics{
		pollTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "fuse_residency_polls_total",
			Help: "Residency polls issued to the fuse coordinator, by model.",
		}, []string{"model"}),
		pollErrors: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "fuse_residency_poll_errors_total",
			Help: "Residency polls that failed. The previous reading is retained until it goes stale.",
		}, []string{"model"}),
		nodesKnown: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "fuse_residency_nodes_known",
			Help: "Nodes with a measured residency reading in the last successful poll.",
		}, []string{"model"}),
		nodesUnknown: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "fuse_residency_nodes_unknown",
			Help: "Nodes the coordinator could not measure. These score neutral, not zero.",
		}, []string{"model"}),
		bestFraction: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "fuse_residency_best_fraction",
			Help: "Highest byte-weighted residency fraction across nodes for a model.",
		}, []string{"model"}),
	}
	for _, c := range []prometheus.Collector{m.pollTotal, m.pollErrors, m.nodesKnown, m.nodesUnknown, m.bestFraction} {
		// A duplicate registration means two extractor instances share a name;
		// the metric is still usable, so this is not worth failing startup for.
		_ = reg.Register(c)
	}
	return m
}

// All methods tolerate a nil receiver: the framework may hand the factory a
// handle with no metrics recorder, and metrics are not worth a nil check at
// every call site.

func (m *pluginMetrics) pollOK(model string, report *residency.Report) {
	if m == nil {
		return
	}
	m.pollTotal.WithLabelValues(model).Inc()

	var known, unknown int
	var best float64
	for _, n := range report.Nodes {
		if n.Unknown || n.NodeIP == "" {
			unknown++
			continue
		}
		known++
		if n.Fraction > best {
			best = n.Fraction
		}
	}
	m.nodesKnown.WithLabelValues(model).Set(float64(known))
	m.nodesUnknown.WithLabelValues(model).Set(float64(unknown))
	m.bestFraction.WithLabelValues(model).Set(best)
}

func (m *pluginMetrics) pollFailed(model string) {
	if m == nil {
		return
	}
	m.pollTotal.WithLabelValues(model).Inc()
	m.pollErrors.WithLabelValues(model).Inc()
}

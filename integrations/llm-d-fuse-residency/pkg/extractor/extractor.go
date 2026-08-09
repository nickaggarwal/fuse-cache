// Package extractor provides the datalayer plugin that stamps each endpoint
// with a FuseResidency attribute describing how much of a model's weights the
// endpoint's node already holds on local NVMe.
//
// # Why a background poller rather than a per-endpoint scrape
//
// The framework's polling sources scrape each endpoint individually. Residency
// is not per-endpoint data: the coordinator answers for the whole cluster in
// one fan-out, and it already coalesces concurrent callers behind a 5s TTL
// cache. Polling it once per endpoint would multiply an intentionally-shared
// query by the fleet size to get N copies of one answer. So this plugin owns a
// single goroutine per configured prefix and joins the result onto endpoints as
// they arrive, using the endpoint-lifecycle source only to learn which
// endpoints exist.
//
// # The join
//
// EPP endpoints carry pod.Status.HostIP (EndpointMetadata.NodeAddress) and no
// node name. Fuse peers register their own status.hostIP as NodeIP. Those are
// the same value for two pods on one node, and they are the only pair that is:
// the model-server pod and the fuse client pod have unrelated pod IPs. That
// makes NodeAddress↔NodeIP the join, and it is why NodeIP had to be added to
// PeerInfo — NodeName alone is unusable from inside an EndpointPicker.
package extractor

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/go-logr/logr"
	fwkdl "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/datalayer"
	fwkplugin "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/plugin"
	sourcenotifications "github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/datalayer/source/notifications"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"

	"github.com/nilesh/llm-d-fuse-residency/pkg/attribute"
	"github.com/nilesh/llm-d-fuse-residency/pkg/residency"
)

const (
	defaultPollInterval = 10 * time.Second
	// defaultQueryTimeout is generous because the coordinator fans out to every
	// peer before answering. It must stay below pollInterval so a slow query
	// cannot overlap the next tick.
	defaultQueryTimeout = 4 * time.Second
	// defaultStaleAfter bounds how long a residency reading is trusted after
	// the last successful poll. Past it, endpoints revert to unknown rather
	// than keeping a stale value: routing on a measurement nobody has been able
	// to refresh for minutes is the failure this whole mechanism exists to
	// avoid. Unknown is honest; stale-but-confident is not.
	defaultStaleAfter = 90 * time.Second
)

var (
	_ fwkplugin.Plugin        = (*Extractor)(nil)
	_ fwkdl.Registrant        = (*Extractor)(nil)
	_ fwkdl.EndpointExtractor = (*endpointHandler)(nil)
)

// ModelPrefix maps a served model name to the cache path holding its weights.
type ModelPrefix struct {
	// Model is the value that arrives as InferenceRequest.TargetModel, e.g.
	// "Qwen/Qwen2.5-7B-Instruct".
	Model string `json:"model"`
	// Prefix is the fuse cache path the weights live under, e.g.
	// "models/Qwen2.5-7B-Instruct/". This is the string handed to
	// GET /api/residency?prefix=.
	Prefix string `json:"prefix"`
}

type params struct {
	// CoordinatorURL is the fuse coordinator base URL, e.g.
	// "http://coordinator.fuse-system.svc.cluster.local:8080".
	CoordinatorURL string `json:"coordinatorURL"`
	// APIKey is sent as X-API-Key. Optional; the coordinator only requires it
	// when started with -api-key.
	APIKey string `json:"apiKey,omitempty"`
	// Models maps served model names to cache prefixes. At least one is
	// required — with none configured the plugin has nothing to poll and would
	// score every endpoint 0, which is indistinguishable from a working
	// deployment where nothing is cached.
	Models []ModelPrefix `json:"models"`
	// PollIntervalSec is how often to refresh residency. Defaults to 10s.
	// Residency changes on the timescale of a warm or an eviction (seconds to
	// minutes), so polling faster buys nothing and the coordinator's own 5s TTL
	// cache would absorb it anyway.
	PollIntervalSec int `json:"pollIntervalSec,omitempty"`
	// StaleAfterSec is how long a reading stays trusted after the last
	// successful poll. Defaults to 90s. Set 0 to use the default; negative
	// disables staleness expiry (not recommended — see defaultStaleAfter).
	StaleAfterSec int `json:"staleAfterSec,omitempty"`
}

// Extractor polls the fuse coordinator for model residency and stamps each
// endpoint with the reading for the node it runs on.
type Extractor struct {
	typedName fwkplugin.TypedName
	dk        fwkplugin.DataKey

	client      *residency.Client
	models      []ModelPrefix
	pollEvery   time.Duration
	staleAfter  time.Duration
	logger      logr.Logger
	metrics     *pluginMetrics
	pollStarted sync.Once

	// mu guards endpoints and readings.
	mu sync.Mutex
	// endpoints tracks live endpoints by identity so a poll can restamp them
	// without waiting for the next lifecycle event.
	endpoints map[types.NamespacedName]fwkdl.Endpoint
	// readings is the last successful poll, keyed model → nodeIP. Absence of a
	// node here means unknown, never zero.
	readings map[string]map[string]*attribute.FuseResidency
	// fetchedAt is the last successful poll time per model, used for staleness.
	fetchedAt map[string]time.Time
}

// Factory creates the residency extractor.
func Factory(name string, rawParameters *json.Decoder, handle fwkplugin.Handle) (fwkplugin.Plugin, error) {
	p := params{}
	if rawParameters != nil {
		if err := rawParameters.Decode(&p); err != nil {
			return nil, fmt.Errorf("failed to parse the parameters of the '%s' plugin - %w", attribute.ExtractorType, err)
		}
	}
	if p.CoordinatorURL == "" {
		return nil, fmt.Errorf("'%s': coordinatorURL is required", attribute.ExtractorType)
	}
	// Reject an empty model list rather than defaulting to "poll nothing". A
	// plugin that silently scores every endpoint 0 looks exactly like a
	// correctly-wired one on a cold cluster, and that ambiguity would only be
	// noticed as unexplained routing behavior weeks later.
	if len(p.Models) == 0 {
		return nil, fmt.Errorf("'%s': at least one models[] entry is required", attribute.ExtractorType)
	}
	for i, m := range p.Models {
		if m.Model == "" || m.Prefix == "" {
			return nil, fmt.Errorf("'%s': models[%d] needs both model and prefix", attribute.ExtractorType, i)
		}
	}

	pollEvery := defaultPollInterval
	if p.PollIntervalSec > 0 {
		pollEvery = time.Duration(p.PollIntervalSec) * time.Second
	}
	staleAfter := defaultStaleAfter
	switch {
	case p.StaleAfterSec < 0:
		staleAfter = 0 // explicit opt-out
	case p.StaleAfterSec > 0:
		staleAfter = time.Duration(p.StaleAfterSec) * time.Second
	}
	// A stale window shorter than the poll interval expires every reading
	// before it can be refreshed, scoring everything unknown forever. Catch it
	// at startup rather than shipping a scorer that silently never fires.
	if staleAfter > 0 && staleAfter <= pollEvery {
		return nil, fmt.Errorf("'%s': staleAfterSec (%v) must exceed pollIntervalSec (%v), or every reading expires before the next poll",
			attribute.ExtractorType, staleAfter, pollEvery)
	}

	queryTimeout := defaultQueryTimeout
	if queryTimeout >= pollEvery {
		queryTimeout = pollEvery / 2
	}

	if name == "" {
		name = attribute.ExtractorType
	}
	e := &Extractor{
		typedName:  fwkplugin.TypedName{Type: attribute.ExtractorType, Name: name},
		dk:         attribute.AttributeKey.WithNonEmptyProducerName(name),
		client:     residency.New(p.CoordinatorURL, p.APIKey, queryTimeout),
		models:     p.Models,
		pollEvery:  pollEvery,
		staleAfter: staleAfter,
		logger:     ctrl.Log.WithName(attribute.ExtractorType),
		endpoints:  make(map[types.NamespacedName]fwkdl.Endpoint),
		readings:   make(map[string]map[string]*attribute.FuseResidency),
		fetchedAt:  make(map[string]time.Time),
	}
	if handle != nil {
		if rec := handle.Metrics(); rec != nil {
			e.metrics = newPluginMetrics(rec)
		}
		if ctx := handle.Context(); ctx != nil {
			e.start(ctx)
		}
	}
	return e, nil
}

// TypedName returns the plugin type and name.
func (e *Extractor) TypedName() fwkplugin.TypedName {
	return e.typedName
}

// RegisterDependencies wires this extractor to the endpoint-lifecycle source so
// it learns which endpoints exist. It needs no metrics scrape of its own: the
// data comes from the coordinator, not from the model servers.
func (e *Extractor) RegisterDependencies(r fwkdl.Registrar) error {
	return r.Register(fwkdl.PendingRegistration{
		Owner:      e.typedName,
		SourceType: sourcenotifications.EndpointNotificationSourceType,
		Extractor:  &endpointHandler{ext: e},
		DefaultSource: sourcenotifications.NewEndpointDataSource(
			sourcenotifications.EndpointNotificationSourceType,
			sourcenotifications.EndpointNotificationSourceType,
		),
	})
}

// start launches the polling goroutine. Guarded by a sync.Once so that a
// second call (or a test constructing the plugin twice) cannot double-poll.
func (e *Extractor) start(ctx context.Context) {
	e.pollStarted.Do(func() {
		go e.pollLoop(ctx)
	})
}

func (e *Extractor) pollLoop(ctx context.Context) {
	// Poll once immediately: waiting a full interval would leave every endpoint
	// unknown for the first N seconds after EPP start, which is exactly when a
	// rollout is dispatching its first requests.
	e.pollOnce(ctx)

	ticker := time.NewTicker(e.pollEvery)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			e.pollOnce(ctx)
		}
	}
}

// pollOnce refreshes every configured model and restamps all live endpoints.
func (e *Extractor) pollOnce(ctx context.Context) {
	for _, m := range e.models {
		report, err := e.client.Fetch(ctx, m.Prefix)
		if err != nil {
			// Keep the previous reading and let staleness expire it. Dropping
			// it here would swing routing away from the warm nodes on a single
			// failed poll, which is a much worse error than acting on a
			// slightly old measurement for one interval.
			e.logger.V(1).Info("residency poll failed; retaining previous reading until it goes stale",
				"model", m.Model, "prefix", m.Prefix, "error", err)
			e.metrics.pollFailed(m.Model)
			continue
		}
		e.absorb(m, report)
		e.metrics.pollOK(m.Model, report)
	}
	e.restampAll()
}

// absorb converts one coordinator report into the per-node readings map.
func (e *Extractor) absorb(m ModelPrefix, report *residency.Report) {
	byNode := make(map[string]*attribute.FuseResidency, len(report.Nodes))
	for _, n := range report.Nodes {
		// A peer with no NodeIP cannot be joined to an endpoint — it is an
		// older client, or one running outside Kubernetes. Skipping it leaves
		// its endpoints unknown, which is the correct answer: we genuinely
		// cannot tell whether that node holds the model.
		if n.NodeIP == "" {
			continue
		}
		// An unreachable peer stays absent from the map rather than being
		// recorded as 0.0. Absent means unknown at score time.
		if n.Unknown {
			continue
		}
		r := &attribute.FuseResidency{
			Known:         true,
			Fraction:      clamp01(n.Fraction),
			ResidentBytes: n.ResidentBytes,
			TotalBytes:    n.TotalBytes,
			Pinned:        n.Pinned,
			Declared:      n.Declared,
			Prefix:        m.Prefix,
			NodeIP:        n.NodeIP,
		}
		if n.Declared && !n.LastVerified.IsZero() {
			r.LastVerifiedAgeSec = time.Since(n.LastVerified).Seconds()
		}
		byNode[n.NodeIP] = r
	}

	e.mu.Lock()
	defer e.mu.Unlock()
	e.readings[m.Model] = byNode
	e.fetchedAt[m.Model] = time.Now()
}

// lookup returns the current reading for one model on one node, or nil when it
// is unknown — never polled, not in the last report, or gone stale.
func (e *Extractor) lookup(model, nodeIP string) *attribute.FuseResidency {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.lookupLocked(model, nodeIP)
}

func (e *Extractor) lookupLocked(model, nodeIP string) *attribute.FuseResidency {
	if e.staleAfter > 0 {
		at, ok := e.fetchedAt[model]
		if !ok || time.Since(at) > e.staleAfter {
			return nil
		}
	}
	return e.readings[model][nodeIP]
}

// stamp writes the residency attribute for every configured model onto one
// endpoint.
//
// Every model gets an entry, including the unknown ones. An endpoint that is
// simply missing the attribute is indistinguishable from one measured at zero
// once the framework sums weighted scores, so absence has to be represented
// rather than implied.
func (e *Extractor) stamp(ep fwkdl.Endpoint) {
	meta := ep.GetMetadata()
	if meta == nil {
		return
	}
	nodeIP := meta.NodeAddress

	e.mu.Lock()
	defer e.mu.Unlock()
	set := make(ModelResidency, len(e.models))
	for _, m := range e.models {
		if nodeIP == "" {
			// No host IP on the endpoint — file-based discovery, or a
			// non-Kubernetes source. Unknown, not zero.
			set[m.Model] = &attribute.FuseResidency{Prefix: m.Prefix}
			continue
		}
		if r := e.lookupLocked(m.Model, nodeIP); r != nil {
			set[m.Model] = r
			continue
		}
		set[m.Model] = &attribute.FuseResidency{Prefix: m.Prefix, NodeIP: nodeIP}
	}
	ep.GetAttributes().Put(e.dk.String(), set)
}

func (e *Extractor) restampAll() {
	e.mu.Lock()
	eps := make([]fwkdl.Endpoint, 0, len(e.endpoints))
	for _, ep := range e.endpoints {
		eps = append(eps, ep)
	}
	e.mu.Unlock()

	for _, ep := range eps {
		e.stamp(ep)
	}
}

// ModelResidency is the attribute value: one reading per configured model,
// keyed by served model name. It is keyed by model rather than being a single
// reading because one endpoint can serve several models and the request names
// which one it wants only at score time.
type ModelResidency map[string]*attribute.FuseResidency

// Clone implements fwkdl.Cloneable.
func (m ModelResidency) Clone() fwkdl.Cloneable {
	if m == nil {
		return nil
	}
	cp := make(ModelResidency, len(m))
	for k, v := range m {
		if v == nil {
			cp[k] = nil
			continue
		}
		c := *v
		cp[k] = &c
	}
	return cp
}

// endpointHandler tracks endpoint lifecycle so the poller knows what to stamp.
type endpointHandler struct {
	ext *Extractor
}

func (h *endpointHandler) TypedName() fwkplugin.TypedName {
	tn := h.ext.typedName
	tn.Name += "/endpoint"
	return tn
}

func (h *endpointHandler) Extract(_ context.Context, event fwkdl.EndpointEvent) error {
	meta := event.Endpoint.GetMetadata()
	if meta == nil {
		return nil
	}
	key := meta.ID

	if event.Type == fwkdl.EventDelete {
		h.ext.mu.Lock()
		delete(h.ext.endpoints, key)
		h.ext.mu.Unlock()
		return nil
	}

	h.ext.mu.Lock()
	h.ext.endpoints[key] = event.Endpoint
	h.ext.mu.Unlock()

	// Stamp immediately from the cached reading so a newly-discovered endpoint
	// is scoreable on its first request rather than after the next poll tick.
	h.ext.stamp(event.Endpoint)
	return nil
}

func clamp01(f float64) float64 {
	switch {
	case f < 0:
		return 0
	case f > 1:
		return 1
	default:
		return f
	}
}

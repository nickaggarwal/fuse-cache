package extractor

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	fwkdl "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/datalayer"
	fwkplugin "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/plugin"
	"k8s.io/apimachinery/pkg/types"

	"github.com/nilesh/llm-d-fuse-residency/pkg/attribute"
)

const (
	model  = "Qwen/Qwen2.5-7B-Instruct"
	prefix = "models/qwen/"
	// warmNodeIP is the node both the fuse peer and the model-server pod run
	// on. The whole join hinges on these being the same string.
	warmNodeIP = "10.224.0.7"
	coldNodeIP = "10.224.0.8"
)

// coordinator is a stub of GET /api/residency whose body and status the test
// swaps between polls. Every handoff in these tests keys off its call counter
// rather than a wall-clock sleep: polls are driven by calling pollOnce
// directly, so ordering is causal and stays that way under -race.
type coordinator struct {
	*httptest.Server
	calls atomic.Int64
	body  atomic.Value // string
	code  atomic.Int64
}

func newCoordinator(t *testing.T, body string) *coordinator {
	t.Helper()
	c := &coordinator{}
	c.body.Store(body)
	c.code.Store(int64(http.StatusOK))
	c.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		c.calls.Add(1)
		if code := int(c.code.Load()); code != http.StatusOK {
			w.WriteHeader(code)
			return
		}
		_, _ = w.Write([]byte(c.body.Load().(string)))
	}))
	t.Cleanup(c.Close)
	return c
}

func (c *coordinator) serve(body string) { c.body.Store(body); c.code.Store(int64(http.StatusOK)) }
func (c *coordinator) fail(code int)     { c.code.Store(int64(code)) }

func report(nodes ...string) string {
	return fmt.Sprintf(`{"prefix":%q,"nodes":[%s]}`, prefix, strings.Join(nodes, ","))
}

func node(ip string, fraction float64) string {
	return fmt.Sprintf(`{"node_name":"n-%s","node_ip":%q,"fraction":%v,"resident_bytes":%d,"total_bytes":100}`,
		ip, ip, fraction, int64(fraction*100))
}

func unknownNode(ip string) string {
	return fmt.Sprintf(`{"node_name":"n-%s","node_ip":%q,"unknown":true,"error":"dial timeout"}`, ip, ip)
}

func newExtractor(t *testing.T, url string, extra map[string]any) *Extractor {
	t.Helper()
	p := map[string]any{
		"coordinatorURL": url,
		"models":         []map[string]string{{"model": model, "prefix": prefix}},
	}
	for k, v := range extra {
		p[k] = v
	}
	raw, err := json.Marshal(p)
	if err != nil {
		t.Fatalf("marshal params: %v", err)
	}
	// A nil handle means no background goroutine starts, so each test drives
	// pollOnce itself.
	plug, err := Factory("", fwkplugin.StrictDecoder(raw), nil)
	if err != nil {
		t.Fatalf("Factory: %v", err)
	}
	return plug.(*Extractor)
}

func residencyOf(t *testing.T, e *Extractor, ep fwkdl.Endpoint) *attribute.FuseResidency {
	t.Helper()
	v, ok := ep.GetAttributes().Get(e.dk.String())
	if !ok {
		t.Fatalf("endpoint carries no residency attribute at key %s", e.dk.String())
	}
	set, ok := v.(ModelResidency)
	if !ok {
		t.Fatalf("attribute is %T, want ModelResidency", v)
	}
	r, ok := set[model]
	if !ok {
		t.Fatalf("residency set has no entry for %s; every configured model must get one", model)
	}
	return r
}

func TestFactory_RejectsSilentMisconfigurations(t *testing.T) {
	cases := map[string]string{
		// Without a URL there is nothing to poll, and the plugin would stamp
		// unknown forever while looking healthy.
		"no coordinator": `{"models":[{"model":"m","prefix":"p/"}]}`,
		// An empty model list is indistinguishable at runtime from a correctly
		// wired plugin on a cold cluster.
		"no models":       `{"coordinatorURL":"http://c:8080"}`,
		"empty models":    `{"coordinatorURL":"http://c:8080","models":[]}`,
		"model no prefix": `{"coordinatorURL":"http://c:8080","models":[{"model":"m"}]}`,
		"prefix no model": `{"coordinatorURL":"http://c:8080","models":[{"prefix":"p/"}]}`,
		// staleAfter <= pollInterval expires every reading before the next poll
		// can replace it, so the scorer never once sees a known value.
		"stale window shorter than poll": `{"coordinatorURL":"http://c:8080","models":[{"model":"m","prefix":"p/"}],
			"pollIntervalSec":30,"staleAfterSec":10}`,
		"stale window equals poll": `{"coordinatorURL":"http://c:8080","models":[{"model":"m","prefix":"p/"}],
			"pollIntervalSec":30,"staleAfterSec":30}`,
	}
	for label, params := range cases {
		if _, err := Factory("", fwkplugin.StrictDecoder(json.RawMessage(params)), nil); err == nil {
			t.Errorf("%s: Factory accepted %s", label, params)
		}
	}
}

func TestFactory_AcceptsAValidConfig(t *testing.T) {
	e := newExtractor(t, "http://c:8080", map[string]any{"pollIntervalSec": 5, "staleAfterSec": 60})
	if e.pollEvery != 5*time.Second || e.staleAfter != 60*time.Second {
		t.Errorf("pollEvery=%v staleAfter=%v, want 5s/60s", e.pollEvery, e.staleAfter)
	}
}

// TestPoll_JoinsOnNodeIP is the core of the integration: an endpoint knows only
// its host IP, a fuse peer reports only its host IP, and those must meet.
func TestPoll_JoinsOnNodeIP(t *testing.T) {
	c := newCoordinator(t, report(node(warmNodeIP, 1.0), node(coldNodeIP, 0.0)))
	e := newExtractor(t, c.URL, nil)

	warm := trackEndpoint(t, e, "vllm-0", warmNodeIP)
	cold := trackEndpoint(t, e, "vllm-1", coldNodeIP)

	e.pollOnce(context.Background())

	if r := residencyOf(t, e, warm); !r.Known || r.Fraction != 1.0 {
		t.Errorf("warm endpoint: known=%v fraction=%v, want true/1.0", r.Known, r.Fraction)
	}
	if r := residencyOf(t, e, cold); !r.Known || r.Fraction != 0.0 {
		t.Errorf("cold endpoint: known=%v fraction=%v, want true/0.0", r.Known, r.Fraction)
	}
}

// TestPoll_UnjoinableNodesStayUnknown covers every way the join can fail. All
// three must land as Known=false rather than Fraction=0, because a scorer
// cannot tell those apart and they call for opposite decisions.
func TestPoll_UnjoinableNodesStayUnknown(t *testing.T) {
	c := newCoordinator(t, report(
		unknownNode(warmNodeIP),                 // peer the coordinator could not reach
		`{"node_name":"legacy","fraction":1.0}`, // older client: no node_ip at all
	))
	e := newExtractor(t, c.URL, nil)

	unreachable := trackEndpoint(t, e, "vllm-0", warmNodeIP)
	legacy := trackEndpoint(t, e, "vllm-1", "10.224.0.9")
	// An endpoint with no host IP of its own — file-based discovery, or a
	// non-Kubernetes source.
	noHostIP := trackEndpoint(t, e, "vllm-2", "")

	e.pollOnce(context.Background())

	for label, ep := range map[string]fwkdl.Endpoint{
		"coordinator could not reach the peer": unreachable,
		"peer predates node_ip":                legacy,
		"endpoint has no host IP":              noHostIP,
	} {
		r := residencyOf(t, e, ep)
		if r.Known {
			t.Errorf("%s: Known=true — an unjoinable node must not be reported as measured", label)
		}
		if r.Prefix != prefix {
			t.Errorf("%s: prefix %q lost on the unknown reading", label, r.Prefix)
		}
	}
}

// TestPoll_FailureRetainsThePreviousReading: one failed poll must not swing
// routing away from the nodes that were warm a second ago. The reading is kept
// and left for staleness to expire.
func TestPoll_FailureRetainsThePreviousReading(t *testing.T) {
	c := newCoordinator(t, report(node(warmNodeIP, 1.0)))
	e := newExtractor(t, c.URL, nil)
	warm := trackEndpoint(t, e, "vllm-0", warmNodeIP)

	e.pollOnce(context.Background())
	if r := residencyOf(t, e, warm); !r.Known || r.Fraction != 1.0 {
		t.Fatalf("setup poll: known=%v fraction=%v", r.Known, r.Fraction)
	}

	c.fail(http.StatusServiceUnavailable)
	e.pollOnce(context.Background())

	r := residencyOf(t, e, warm)
	if !r.Known || r.Fraction != 1.0 {
		t.Errorf("after a failed poll: known=%v fraction=%v, want the retained 1.0", r.Known, r.Fraction)
	}
}

// TestPoll_StaleReadingExpiresToUnknown is the other half: retention is bounded.
// A measurement nobody has been able to refresh is eventually worse than
// admitting ignorance, so it must decay to unknown rather than to zero.
func TestPoll_StaleReadingExpiresToUnknown(t *testing.T) {
	c := newCoordinator(t, report(node(warmNodeIP, 1.0)))
	e := newExtractor(t, c.URL, nil)
	warm := trackEndpoint(t, e, "vllm-0", warmNodeIP)

	e.pollOnce(context.Background())
	if r := residencyOf(t, e, warm); !r.Known {
		t.Fatal("setup poll produced no known reading")
	}

	// Age the reading past the stale window by rewriting its timestamp — the
	// alternative, sleeping out a real 90s window, is exactly the wall-clock
	// race this project's tests avoid.
	e.mu.Lock()
	e.fetchedAt[model] = time.Now().Add(-e.staleAfter - time.Second)
	e.mu.Unlock()

	c.fail(http.StatusServiceUnavailable)
	e.pollOnce(context.Background())

	if r := residencyOf(t, e, warm); r.Known {
		t.Error("a reading older than staleAfter is still reported as known")
	}
}

// TestPoll_RecoversAfterFailure: nothing latches. Once the coordinator answers
// again the readings come back without an EPP restart.
func TestPoll_RecoversAfterFailure(t *testing.T) {
	c := newCoordinator(t, report(node(warmNodeIP, 0.5)))
	e := newExtractor(t, c.URL, nil)
	warm := trackEndpoint(t, e, "vllm-0", warmNodeIP)

	c.fail(http.StatusServiceUnavailable)
	e.pollOnce(context.Background())
	if r := residencyOf(t, e, warm); r.Known {
		t.Fatal("known reading appeared from a failing coordinator")
	}

	c.serve(report(node(warmNodeIP, 0.9)))
	e.pollOnce(context.Background())

	if r := residencyOf(t, e, warm); !r.Known || r.Fraction != 0.9 {
		t.Errorf("after recovery: known=%v fraction=%v, want true/0.9", r.Known, r.Fraction)
	}
}

// TestPoll_TracksEvictionDownward: residency is a live measurement, not a
// mount-time claim. When LRU evicts part of a warmed prefix the score must fall
// — that divergence between declared and measured is the whole point of the
// endpoint.
func TestPoll_TracksEvictionDownward(t *testing.T) {
	c := newCoordinator(t, report(node(warmNodeIP, 1.0)))
	e := newExtractor(t, c.URL, nil)
	warm := trackEndpoint(t, e, "vllm-0", warmNodeIP)

	e.pollOnce(context.Background())
	c.serve(report(node(warmNodeIP, 0.25)))
	e.pollOnce(context.Background())

	if r := residencyOf(t, e, warm); r.Fraction != 0.25 {
		t.Errorf("fraction = %v after eviction, want 0.25", r.Fraction)
	}
}

// TestEndpointLifecycle_StampsOnArrivalAndForgetsOnDelete: a newly discovered
// endpoint is scoreable on its first request rather than after the next tick,
// and a deleted one stops being restamped so the map cannot grow without bound
// across a rollout.
func TestEndpointLifecycle_StampsOnArrivalAndForgetsOnDelete(t *testing.T) {
	c := newCoordinator(t, report(node(warmNodeIP, 1.0)))
	e := newExtractor(t, c.URL, nil)
	h := &endpointHandler{ext: e}

	// Poll first, then let the endpoint arrive: it must pick up the cached
	// reading immediately.
	e.pollOnce(context.Background())
	ep := makeEndpoint("vllm-0", warmNodeIP)
	if err := h.Extract(context.Background(), fwkdl.EndpointEvent{Type: fwkdl.EventAddOrUpdate, Endpoint: ep}); err != nil {
		t.Fatalf("Extract(add): %v", err)
	}
	if r := residencyOf(t, e, ep); !r.Known || r.Fraction != 1.0 {
		t.Errorf("endpoint stamped on arrival: known=%v fraction=%v, want true/1.0", r.Known, r.Fraction)
	}

	if err := h.Extract(context.Background(), fwkdl.EndpointEvent{Type: fwkdl.EventDelete, Endpoint: ep}); err != nil {
		t.Fatalf("Extract(delete): %v", err)
	}
	e.mu.Lock()
	n := len(e.endpoints)
	e.mu.Unlock()
	if n != 0 {
		t.Errorf("%d endpoints tracked after delete, want 0", n)
	}
}

// TestPoll_ClampsOutOfRangeFraction: a fraction outside [0,1] would mean the
// coordinator's accounting is off, but the score range is a contract with the
// framework and must hold regardless.
func TestPoll_ClampsOutOfRangeFraction(t *testing.T) {
	c := newCoordinator(t, report(node(warmNodeIP, 1.4)))
	e := newExtractor(t, c.URL, nil)
	warm := trackEndpoint(t, e, "vllm-0", warmNodeIP)

	e.pollOnce(context.Background())
	if r := residencyOf(t, e, warm); r.Fraction != 1.0 {
		t.Errorf("fraction = %v, want clamp to 1.0", r.Fraction)
	}
}

func TestNodeNotInReportIsUnknown(t *testing.T) {
	c := newCoordinator(t, report(node(warmNodeIP, 1.0)))
	e := newExtractor(t, c.URL, nil)
	// This endpoint's node has no fuse peer at all — no client DaemonSet pod
	// there, or it has not registered yet.
	orphan := trackEndpoint(t, e, "vllm-9", "10.224.0.99")

	e.pollOnce(context.Background())

	if r := residencyOf(t, e, orphan); r.Known {
		t.Error("a node absent from the report is reported known; absence is unknown, not zero")
	}
}

func TestModelResidency_CloneIsDeep(t *testing.T) {
	orig := ModelResidency{model: &attribute.FuseResidency{Known: true, Fraction: 1}}
	cp := orig.Clone().(ModelResidency)
	cp[model].Fraction = 0

	if orig[model].Fraction != 1 {
		t.Error("Clone shares the reading pointer; a consumer mutation would corrupt the poller's state")
	}
	if ModelResidency(nil).Clone() != nil {
		t.Error("nil Clone should stay nil")
	}
}

// trackEndpoint registers an endpoint with the extractor the way the
// lifecycle source would, and returns it.
func trackEndpoint(t *testing.T, e *Extractor, name, nodeIP string) fwkdl.Endpoint {
	t.Helper()
	ep := makeEndpoint(name, nodeIP)
	h := &endpointHandler{ext: e}
	if err := h.Extract(context.Background(), fwkdl.EndpointEvent{Type: fwkdl.EventAddOrUpdate, Endpoint: ep}); err != nil {
		t.Fatalf("track %s: %v", name, err)
	}
	return ep
}

// makeEndpoint builds an endpoint carrying only its host IP, which is all an
// EPP endpoint ever has available to join on — it has no node name.
func makeEndpoint(name, nodeIP string) fwkdl.Endpoint {
	return fwkdl.NewEndpoint(&fwkdl.EndpointMetadata{
		ID:          types.NamespacedName{Namespace: "default", Name: name},
		NodeAddress: nodeIP,
	}, nil)
}

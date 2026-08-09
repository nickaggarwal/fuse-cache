package coordinator

// Tests for the cluster-wide residency fan-out. The behaviors worth pinning:
// a node that does not answer is reported as unknown rather than as zero, the
// TTL cache and in-flight coalescing keep a polling router from multiplying
// into a peer request storm, and node identity rides through so an external
// scheduler can join the answer back to a Kubernetes node.

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// residencyPeer is a fake client daemon answering GET /api/cache/residency.
type residencyPeer struct {
	srv    *httptest.Server
	calls  atomic.Int64
	body   atomic.Pointer[string]
	status atomic.Int64
	// gate, when non-nil, blocks each response until closed — used to hold a
	// fan-out open long enough to observe coalescing.
	gate chan struct{}
	// prefixes records what each caller asked for.
	mu       sync.Mutex
	prefixes []string
}

func newResidencyPeer(t *testing.T, body string) *residencyPeer {
	t.Helper()
	p := &residencyPeer{}
	p.body.Store(&body)
	p.status.Store(http.StatusOK)
	p.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/cache/residency" {
			http.NotFound(w, r)
			return
		}
		p.calls.Add(1)
		p.mu.Lock()
		p.prefixes = append(p.prefixes, r.URL.Query().Get("prefix"))
		p.mu.Unlock()
		if p.gate != nil {
			<-p.gate
		}
		code := int(p.status.Load())
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(code)
		fmt.Fprint(w, *p.body.Load())
	}))
	t.Cleanup(p.srv.Close)
	return p
}

func (p *residencyPeer) addr() string { return strings.TrimPrefix(p.srv.URL, "http://") }

func residencyReportJSON(files, resident int, bytes, residentBytes int64, fraction float64) string {
	return fmt.Sprintf(`{"files":%d,"resident_files":%d,"bytes":%d,"resident_bytes":%d,"fraction":%v,"pinned":true,"declared":true,"last_verified":"2026-08-09T10:00:00Z"}`,
		files, resident, bytes, residentBytes, fraction)
}

func TestPrefixResidency_AggregatesAndRanks(t *testing.T) {
	ctx := context.Background()
	full := newResidencyPeer(t, residencyReportJSON(4, 4, 1000, 1000, 1.0))
	partial := newResidencyPeer(t, residencyReportJSON(4, 2, 1000, 400, 0.4))
	cold := newResidencyPeer(t, residencyReportJSON(4, 0, 1000, 0, 0))

	cs := NewCoordinatorService()
	for _, tc := range []struct{ id, node, addr string }{
		{"peer-cold", "aks-node-3", cold.addr()},
		{"peer-full", "aks-node-1", full.addr()},
		{"peer-part", "aks-node-2", partial.addr()},
	} {
		if err := cs.RegisterPeer(ctx, &PeerInfo{ID: tc.id, NodeName: tc.node, Address: tc.addr}); err != nil {
			t.Fatalf("register %s: %v", tc.id, err)
		}
	}

	res, err := cs.PrefixResidency(ctx, "/models/llama", "")
	if err != nil {
		t.Fatalf("PrefixResidency: %v", err)
	}
	if len(res.Nodes) != 3 {
		t.Fatalf("got %d nodes, want 3", len(res.Nodes))
	}
	// Ranked most-resident first: that ordering IS the routing answer.
	if res.Nodes[0].PeerID != "peer-full" || res.Nodes[1].PeerID != "peer-part" {
		t.Fatalf("nodes not ranked by fraction: %+v", res.Nodes)
	}
	if res.Nodes[0].NodeName != "aks-node-1" {
		t.Fatalf("node name did not survive: %+v", res.Nodes[0])
	}
	if res.Nodes[0].ResidentBytes != 1000 || res.Nodes[1].ResidentBytes != 400 {
		t.Fatalf("bytes wrong: %+v", res.Nodes)
	}
	if !res.Nodes[0].Declared || res.Nodes[0].LastVerified.IsZero() {
		t.Fatalf("intent fields did not survive: %+v", res.Nodes[0])
	}
	want := ResidencySummary{Peers: 3, FullyResident: 1, Partial: 1, Unknown: 0, BestFraction: 1}
	if res.Summary != want {
		t.Fatalf("summary = %+v, want %+v", res.Summary, want)
	}
	// Each peer was asked about the prefix the caller named.
	full.mu.Lock()
	got := full.prefixes
	full.mu.Unlock()
	if len(got) != 1 || got[0] != "/models/llama" {
		t.Fatalf("peer asked for %v, want [/models/llama]", got)
	}
}

func TestPrefixResidency_UnreachablePeerIsUnknownNotZero(t *testing.T) {
	ctx := context.Background()
	good := newResidencyPeer(t, residencyReportJSON(2, 2, 500, 500, 1.0))
	broken := newResidencyPeer(t, `{"files":0}`)
	broken.status.Store(http.StatusServiceUnavailable)

	cs := NewCoordinatorService()
	if err := cs.RegisterPeer(ctx, &PeerInfo{ID: "good", Address: good.addr()}); err != nil {
		t.Fatal(err)
	}
	if err := cs.RegisterPeer(ctx, &PeerInfo{ID: "broken", Address: broken.addr()}); err != nil {
		t.Fatal(err)
	}
	// A peer whose address does not resolve at all.
	if err := cs.RegisterPeer(ctx, &PeerInfo{ID: "gone", Address: "127.0.0.1:1"}); err != nil {
		t.Fatal(err)
	}

	res, err := cs.PrefixResidency(ctx, "/models", "")
	if err != nil {
		t.Fatalf("PrefixResidency: %v", err)
	}
	byID := map[string]NodeResidency{}
	for _, n := range res.Nodes {
		byID[n.PeerID] = n
	}
	for _, id := range []string{"broken", "gone"} {
		n, ok := byID[id]
		if !ok {
			t.Fatalf("peer %s missing from report", id)
		}
		// The distinction the whole endpoint rests on: a node we could not ask
		// must not look like a node that answered "I hold nothing", or a
		// transient blip reads as a deliberate routing signal.
		if !n.Unknown {
			t.Fatalf("peer %s not marked unknown: %+v", id, n)
		}
		if n.Error == "" {
			t.Fatalf("peer %s unknown without a reason", id)
		}
	}
	if byID["good"].Unknown {
		t.Fatalf("healthy peer marked unknown: %+v", byID["good"])
	}
	if res.Summary.Unknown != 2 || res.Summary.FullyResident != 1 {
		t.Fatalf("summary = %+v, want 2 unknown / 1 full", res.Summary)
	}
	// One live node at 1.0 means locality IS available, even with two nodes
	// unreachable — the router should still be told that.
	if res.Summary.BestFraction != 1 {
		t.Fatalf("best fraction = %v, want 1", res.Summary.BestFraction)
	}
}

func TestPrefixResidency_CachedWithinTTL(t *testing.T) {
	ctx := context.Background()
	peer := newResidencyPeer(t, residencyReportJSON(1, 1, 10, 10, 1.0))
	cs := NewCoordinatorService()
	if err := cs.RegisterPeer(ctx, &PeerInfo{ID: "p", Address: peer.addr()}); err != nil {
		t.Fatal(err)
	}

	first, err := cs.PrefixResidency(ctx, "/models", "")
	if err != nil {
		t.Fatal(err)
	}
	if first.Cached {
		t.Fatal("first query reported itself cached")
	}
	for i := 0; i < 5; i++ {
		res, err := cs.PrefixResidency(ctx, "/models", "")
		if err != nil {
			t.Fatal(err)
		}
		if !res.Cached {
			t.Fatalf("query %d was not served from cache", i)
		}
	}
	if got := peer.calls.Load(); got != 1 {
		t.Fatalf("peer hit %d times, want 1 — the TTL cache is not holding", got)
	}

	// A different prefix is a different question and must not be served the
	// cached answer.
	if _, err := cs.PrefixResidency(ctx, "/datasets", ""); err != nil {
		t.Fatal(err)
	}
	if got := peer.calls.Load(); got != 2 {
		t.Fatalf("peer hit %d times after a new prefix, want 2", got)
	}
}

func TestPrefixResidency_ConcurrentCallersCoalesce(t *testing.T) {
	ctx := context.Background()
	peer := newResidencyPeer(t, residencyReportJSON(1, 1, 10, 10, 1.0))
	peer.gate = make(chan struct{})

	cs := NewCoordinatorService()
	if err := cs.RegisterPeer(ctx, &PeerInfo{ID: "p", Address: peer.addr()}); err != nil {
		t.Fatal(err)
	}

	const callers = 8
	var wg sync.WaitGroup
	errs := make([]error, callers)
	results := make([]*ClusterResidency, callers)
	started := make(chan struct{}, callers)
	for i := 0; i < callers; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			started <- struct{}{}
			results[i], errs[i] = cs.PrefixResidency(ctx, "/models", "")
		}()
	}
	for i := 0; i < callers; i++ {
		<-started
	}
	// Release once every caller is running. The leader is blocked inside the
	// peer handler, so the followers must have taken the inflight path; if
	// they had each fanned out, calls would exceed 1.
	//
	// Keying the release off the peer having been reached (rather than a
	// sleep) is what makes this deterministic under -race.
	for peer.calls.Load() < 1 {
		runtime.Gosched()
	}
	close(peer.gate)
	wg.Wait()

	for i := range errs {
		if errs[i] != nil {
			t.Fatalf("caller %d: %v", i, errs[i])
		}
		if results[i] == nil || len(results[i].Nodes) != 1 {
			t.Fatalf("caller %d got %+v", i, results[i])
		}
	}
	if got := peer.calls.Load(); got != 1 {
		t.Fatalf("%d concurrent callers produced %d peer requests, want 1", callers, got)
	}
}

func TestPrefixResidency_SkipsInactiveAndStalePeers(t *testing.T) {
	ctx := context.Background()
	live := newResidencyPeer(t, residencyReportJSON(1, 1, 10, 10, 1.0))
	dead := newResidencyPeer(t, residencyReportJSON(1, 1, 10, 10, 1.0))
	stale := newResidencyPeer(t, residencyReportJSON(1, 1, 10, 10, 1.0))

	cs := NewCoordinatorService()
	for _, p := range []*PeerInfo{
		{ID: "live", Address: live.addr()},
		{ID: "dead", Address: dead.addr()},
		{ID: "stale", Address: stale.addr()},
	} {
		if err := cs.RegisterPeer(ctx, p); err != nil {
			t.Fatal(err)
		}
	}
	if err := cs.UpdatePeerStatus(ctx, "dead", "inactive", 0, 0); err != nil {
		t.Fatal(err)
	}
	// A pod that crashed keeps "active" until its lease expires. Asking it
	// costs a full timeout per query and can only ever answer unknown.
	peers, err := cs.store.ListPeers(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range peers {
		if p.ID == "stale" {
			p.LastHeartbeat = time.Now().Add(-2 * warmPeerStaleAfter)
			if err := cs.store.PutPeer(ctx, p); err != nil {
				t.Fatal(err)
			}
		}
	}

	res, err := cs.PrefixResidency(ctx, "/models", "")
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Nodes) != 1 || res.Nodes[0].PeerID != "live" {
		t.Fatalf("queried %+v, want only the live peer", res.Nodes)
	}
	if dead.calls.Load() != 0 || stale.calls.Load() != 0 {
		t.Fatalf("inactive/stale peers were queried: dead=%d stale=%d", dead.calls.Load(), stale.calls.Load())
	}
}

func TestPrefixResidency_RejectsEmptyPrefix(t *testing.T) {
	cs := NewCoordinatorService()
	if _, err := cs.PrefixResidency(context.Background(), "   ", ""); err == nil {
		t.Fatal("expected an error for an empty prefix")
	}
}

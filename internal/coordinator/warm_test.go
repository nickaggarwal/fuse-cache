package coordinator

// Tests for cluster-wide warm fan-out: peer selection by nodes/labels/
// percentage and the HTTP trigger to each selected peer.

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

func warmTestService(t *testing.T) *CoordinatorService {
	t.Helper()
	cs := NewCoordinatorService()
	ctx := context.Background()
	peers := []*PeerInfo{
		{ID: "gpu-a", Address: "gpu-a:8081", Labels: map[string]string{"pool": "gpu", "zone": "a"}},
		{ID: "gpu-b", Address: "gpu-b:8081", Labels: map[string]string{"pool": "gpu", "zone": "b"}},
		{ID: "cpu-a", Address: "cpu-a:8081", Labels: map[string]string{"pool": "cpu", "zone": "a"}},
		{ID: "plain", Address: "plain:8081"},
	}
	for _, p := range peers {
		if err := cs.RegisterPeer(ctx, p); err != nil {
			t.Fatalf("register %s: %v", p.ID, err)
		}
	}
	// One dead peer that must never be selected.
	if err := cs.UpdatePeerStatus(ctx, "gpu-b", "inactive", 0, 0); err != nil {
		t.Fatalf("deactivate: %v", err)
	}
	return cs
}

func targetIDs(peers []*PeerInfo) []string {
	out := make([]string, len(peers))
	for i, p := range peers {
		out[i] = p.ID
	}
	return out
}

func TestSelectWarmTargets(t *testing.T) {
	cs := warmTestService(t)
	ctx := context.Background()

	cases := []struct {
		name string
		sel  WarmSelector
		want string // comma-joined expected IDs (sorted)
	}{
		{"all active", WarmSelector{}, "cpu-a,gpu-a,plain"},
		{"label pool=gpu skips inactive", WarmSelector{Labels: map[string]string{"pool": "gpu"}}, "gpu-a"},
		{"label zone=a", WarmSelector{Labels: map[string]string{"zone": "a"}}, "cpu-a,gpu-a"},
		{"labels intersect", WarmSelector{Labels: map[string]string{"pool": "cpu", "zone": "a"}}, "cpu-a"},
		{"explicit nodes", WarmSelector{Nodes: []string{"plain", "cpu-a"}}, "cpu-a,plain"},
		{"nodes ∩ labels empty", WarmSelector{Nodes: []string{"plain"}, Labels: map[string]string{"pool": "gpu"}}, ""},
		{"percentage 34 of 3 = ceil 2", WarmSelector{Percentage: 34}, "cpu-a,gpu-a"},
		{"percentage 1 keeps at least one", WarmSelector{Percentage: 1}, "cpu-a"},
	}
	for _, tc := range cases {
		got, err := cs.SelectWarmTargets(ctx, tc.sel)
		if err != nil {
			t.Fatalf("%s: %v", tc.name, err)
		}
		if joined := strings.Join(targetIDs(got), ","); joined != tc.want {
			t.Fatalf("%s: selected %q, want %q", tc.name, joined, tc.want)
		}
	}
}

func TestWarmPeers_FanOutHitsSelectedPeers(t *testing.T) {
	type seen struct {
		body   map[string]interface{}
		apiKey string
	}
	var (
		mu   sync.Mutex
		hits = map[string]seen{}
	)
	newPeerServer := func(id string, status int) *httptest.Server {
		return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path != "/api/cache/warm" {
				http.NotFound(w, r)
				return
			}
			raw, _ := io.ReadAll(r.Body)
			var body map[string]interface{}
			json.Unmarshal(raw, &body)
			mu.Lock()
			hits[id] = seen{body: body, apiKey: r.Header.Get("X-API-Key")}
			mu.Unlock()
			w.WriteHeader(status)
		}))
	}
	okSrv := newPeerServer("warm-ok", http.StatusAccepted)
	defer okSrv.Close()
	badSrv := newPeerServer("warm-bad", http.StatusInternalServerError)
	defer badSrv.Close()

	cs := NewCoordinatorService()
	ctx := context.Background()
	cs.RegisterPeer(ctx, &PeerInfo{ID: "warm-ok", Address: strings.TrimPrefix(okSrv.URL, "http://"),
		Labels: map[string]string{"pool": "gpu"}})
	cs.RegisterPeer(ctx, &PeerInfo{ID: "warm-bad", Address: strings.TrimPrefix(badSrv.URL, "http://"),
		Labels: map[string]string{"pool": "gpu"}})
	cs.RegisterPeer(ctx, &PeerInfo{ID: "other", Address: "127.0.0.1:1",
		Labels: map[string]string{"pool": "cpu"}})

	result, err := cs.WarmPeers(ctx, WarmRequest{
		Prefix:       "/models/llama",
		Source:       "cloud-first",
		Bandwidth:    "max",
		WarmSelector: WarmSelector{Labels: map[string]string{"pool": "gpu"}},
		APIKey:       "sekrit",
	})
	if err != nil {
		t.Fatalf("WarmPeers: %v", err)
	}
	if result.Selected != 2 {
		t.Fatalf("selected = %d, want 2 (cpu pool excluded)", result.Selected)
	}
	if len(result.Accepted) != 1 || result.Accepted[0] != "warm-ok" {
		t.Fatalf("accepted = %v, want [warm-ok]", result.Accepted)
	}
	if _, failed := result.Failed["warm-bad"]; !failed {
		t.Fatalf("failed = %v, want warm-bad present", result.Failed)
	}

	mu.Lock()
	defer mu.Unlock()
	got := hits["warm-ok"]
	if got.apiKey != "sekrit" {
		t.Fatalf("api key not forwarded: %q", got.apiKey)
	}
	if got.body["prefix"] != "/models/llama" || got.body["mode"] != "full" ||
		got.body["source"] != "cloud-first" || got.body["bandwidth"] != "max" || got.body["async"] != true {
		t.Fatalf("peer saw body %v", got.body)
	}
	if _, hit := hits["other"]; hit {
		t.Fatal("unselected peer was called")
	}

	// Empty prefix and empty selection error out.
	if _, err := cs.WarmPeers(ctx, WarmRequest{}); err == nil {
		t.Fatal("empty prefix must error")
	}
	if _, err := cs.WarmPeers(ctx, WarmRequest{Prefix: "/x",
		WarmSelector: WarmSelector{Labels: map[string]string{"pool": "none"}}}); err == nil {
		t.Fatal("no matching peers must error")
	}
}

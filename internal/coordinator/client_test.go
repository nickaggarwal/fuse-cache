package coordinator

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"
)

func TestCoordinatorClient_UpdatePeerStatusWithNetwork(t *testing.T) {
	var got map[string]interface{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut || r.URL.Path != "/api/peers/status" {
			t.Fatalf("unexpected request: method=%s path=%s", r.Method, r.URL.Path)
		}
		if err := json.NewDecoder(r.Body).Decode(&got); err != nil {
			t.Fatalf("decode body: %v", err)
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	u, err := url.Parse(srv.URL)
	if err != nil {
		t.Fatalf("parse server url: %v", err)
	}
	client := NewCoordinatorClient(u.Host, 2*time.Second)

	metrics := &PeerNetworkMetrics{
		SpeedMBps:   345.6,
		LatencyMs:   1.9,
		ProbeBytes:  1048576,
		ProbeTarget: "peer-b",
		ProbedAt:    time.Now(),
	}
	if err := client.UpdatePeerStatusWithNetwork(context.Background(), "peer-a", "active", 1000, 200, metrics); err != nil {
		t.Fatalf("UpdatePeerStatusWithNetwork: %v", err)
	}

	if got["peer_id"] != "peer-a" {
		t.Fatalf("peer_id=%v, want peer-a", got["peer_id"])
	}
	if got["status"] != "active" {
		t.Fatalf("status=%v, want active", got["status"])
	}
	if got["network_speed_mbps"] == nil {
		t.Fatalf("network_speed_mbps missing from payload")
	}
	if got["network_latency_ms"] == nil {
		t.Fatalf("network_latency_ms missing from payload")
	}
	if got["network_probe_target"] != "peer-b" {
		t.Fatalf("network_probe_target=%v, want peer-b", got["network_probe_target"])
	}
}

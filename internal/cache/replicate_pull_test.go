package cache

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"fuse-client/internal/coordinator"
)

// pullPeersCoordinator lists the given peers as active.
type pullPeersCoordinator struct {
	*deleteCoordinator
	peers []*coordinator.PeerInfo
}

func (c *pullPeersCoordinator) GetPeers(context.Context, string) ([]*coordinator.PeerInfo, error) {
	return c.peers, nil
}

// requestPull asks peers outside the exclude set to warm the file, async and
// peer-first, and counts only accepted requests.
func TestRequestPullAsksTargets(t *testing.T) {
	var mu sync.Mutex
	got := map[string]map[string]any{}
	mk := func(id string, code int) *httptest.Server {
		return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path != "/api/cache/warm" {
				http.NotFound(w, r)
				return
			}
			var body map[string]any
			_ = json.NewDecoder(r.Body).Decode(&body)
			mu.Lock()
			got[id] = body
			mu.Unlock()
			w.WriteHeader(code)
		}))
	}
	ok1, ok2, busy, held := mk("ok1", 202), mk("ok2", 202), mk("busy", 503), mk("held", 202)
	for _, s := range []*httptest.Server{ok1, ok2, busy, held} {
		defer s.Close()
	}
	addr := func(s *httptest.Server) string { return strings.TrimPrefix(s.URL, "http://") }
	coord := &pullPeersCoordinator{deleteCoordinator: newDeleteCoordinator(), peers: []*coordinator.PeerInfo{
		{ID: "ok1", Address: addr(ok1), Status: "active"}, {ID: "ok2", Address: addr(ok2), Status: "active"},
		{ID: "busy", Address: addr(busy), Status: "active"}, {ID: "held", Address: addr(held), Status: "active"},
	}}
	ps, err := NewPeerStorage(coord, 5*time.Second, "me", false, false, true, "")
	if err != nil {
		t.Fatal(err)
	}
	n, _ := ps.requestPull(context.Background(), "/m/big.bin", map[string]struct{}{"held": {}}, 3)
	if n != 2 {
		t.Fatalf("accepted = %d, want 2 (busy refused, held excluded)", n)
	}
	mu.Lock()
	defer mu.Unlock()
	if _, asked := got["held"]; asked {
		t.Fatal("a current holder was asked to pull")
	}
	b := got["ok1"]
	if b == nil || b["prefix"] != "/m/big.bin" || b["async"] != true || b["source"] != "peer-first" || b["bandwidth"] != "background" {
		t.Fatalf("pull request body = %v", b)
	}
}

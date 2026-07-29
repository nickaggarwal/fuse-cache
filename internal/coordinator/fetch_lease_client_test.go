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

// newLeaseTestServer emulates the coordinator's /api/fetch-lease endpoint
// backed by a real CoordinatorService, mirroring cmd/coordinator/main.go.
func newLeaseTestServer(t *testing.T) (*httptest.Server, *CoordinatorService) {
	t.Helper()
	cs := NewCoordinatorService()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/fetch-lease" {
			http.NotFound(w, r)
			return
		}
		var req struct {
			Key        string `json:"key"`
			PeerID     string `json:"peer_id"`
			TTLSeconds int64  `json:"ttl_seconds,omitempty"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Key == "" || req.PeerID == "" {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		switch r.Method {
		case http.MethodPost:
			holder, granted, err := cs.AcquireFetchLease(r.Context(), req.Key, req.PeerID, time.Duration(req.TTLSeconds)*time.Second)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			json.NewEncoder(w).Encode(map[string]interface{}{"holder": holder, "granted": granted})
		case http.MethodDelete:
			if err := cs.ReleaseFetchLease(r.Context(), req.Key, req.PeerID); err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			w.Write([]byte(`{"success": true}`))
		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	}))
	return srv, cs
}

// TestFetchLeaseHTTPRoundTrip drives the HTTP client against the emulated
// endpoint: winner granted, loser told the holder, release frees the key.
func TestFetchLeaseHTTPRoundTrip(t *testing.T) {
	srv, _ := newLeaseTestServer(t)
	defer srv.Close()
	u, _ := url.Parse(srv.URL)
	client := NewCoordinatorClient(u.Host, 2*time.Second)
	ctx := context.Background()

	holder, granted, err := client.AcquireFetchLease(ctx, "/x_chunk_0", "peer-a", 5*time.Second)
	if err != nil || !granted || holder != "peer-a" {
		t.Fatalf("acquire = (%q, %v, %v), want (peer-a, true, nil)", holder, granted, err)
	}

	holder, granted, err = client.AcquireFetchLease(ctx, "/x_chunk_0", "peer-b", 5*time.Second)
	if err != nil || granted || holder != "peer-a" {
		t.Fatalf("contended acquire = (%q, %v, %v), want (peer-a, false, nil)", holder, granted, err)
	}

	if err := client.ReleaseFetchLease(ctx, "/x_chunk_0", "peer-a"); err != nil {
		t.Fatalf("release: %v", err)
	}
	_, granted, err = client.AcquireFetchLease(ctx, "/x_chunk_0", "peer-b", 5*time.Second)
	if err != nil || !granted {
		t.Fatalf("post-release acquire granted=%v err=%v, want true/nil", granted, err)
	}
}

// TestFetchLeaseHTTP_ServerErrorSurfaces ensures HTTP failures come back as
// errors (the cache layer treats them as advisory and reads origin anyway).
func TestFetchLeaseHTTP_ServerErrorSurfaces(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "boom", http.StatusInternalServerError)
	}))
	defer srv.Close()
	u, _ := url.Parse(srv.URL)
	client := NewCoordinatorClient(u.Host, 2*time.Second)

	if _, _, err := client.AcquireFetchLease(context.Background(), "/k", "p", time.Second); err == nil {
		t.Fatal("expected error on 500")
	}
	if err := client.ReleaseFetchLease(context.Background(), "/k", "p"); err == nil {
		t.Fatal("expected error on 500")
	}
}

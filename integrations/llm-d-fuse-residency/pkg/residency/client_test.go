package residency

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestFetch_DecodesTheCoordinatorContract(t *testing.T) {
	var gotPrefix, gotKey string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPrefix = r.URL.Query().Get("prefix")
		gotKey = r.Header.Get("X-API-Key")
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{
			"prefix": "models/qwen/",
			"generated_at": "2026-08-09T10:00:00Z",
			"cached": true,
			"nodes": [
				{"node_name":"n1","node_ip":"10.224.0.7","peer_id":"10.244.1.5:8081",
				 "files":2,"resident_files":1,"total_bytes":16777216,"resident_bytes":12582912,
				 "fraction":0.75,"pinned":true,"declared":true,
				 "last_verified":"2026-08-09T09:59:30Z"},
				{"node_name":"n2","node_ip":"10.224.0.8","unknown":true,"error":"dial timeout"}
			]}`))
	}))
	defer srv.Close()

	rep, err := New(srv.URL, "sekret", 2*time.Second).Fetch(context.Background(), "models/qwen/")
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	if gotPrefix != "models/qwen/" {
		t.Errorf("prefix sent as %q", gotPrefix)
	}
	if gotKey != "sekret" {
		t.Errorf("X-API-Key sent as %q", gotKey)
	}
	if len(rep.Nodes) != 2 {
		t.Fatalf("got %d nodes, want 2", len(rep.Nodes))
	}

	n1 := rep.Nodes[0]
	if n1.NodeIP != "10.224.0.7" {
		t.Errorf("node_ip = %q — without it there is no join key to an EPP endpoint", n1.NodeIP)
	}
	// Byte-weighted, not file-counted: 12MiB of 16MiB is 0.75 even though only
	// 1 of 2 files is resident.
	if n1.Fraction != 0.75 {
		t.Errorf("fraction = %v, want 0.75", n1.Fraction)
	}
	if !n1.Pinned || !n1.Declared || n1.LastVerified.IsZero() {
		t.Errorf("intent fields lost: pinned=%v declared=%v lastVerified=%v", n1.Pinned, n1.Declared, n1.LastVerified)
	}

	// The unreachable node must survive decoding as unknown. If Unknown were
	// dropped it would read as a node measured at 0.0 — the exact conflation
	// this client exists to prevent.
	n2 := rep.Nodes[1]
	if !n2.Unknown {
		t.Error("unknown flag lost in decode; an unreachable node would look measured-cold")
	}
	if n2.Fraction != 0 || n2.Error == "" {
		t.Errorf("unknown node = %+v, want zeroed counts with the error preserved", n2)
	}
}

// TestFetch_NonOKIsAnError: the coordinator returns 503 when it cannot
// enumerate. Returning an empty report instead would tell the caller "no node
// holds anything", which is a confident claim built on no data.
func TestFetch_NonOKIsAnError(t *testing.T) {
	for _, code := range []int{http.StatusServiceUnavailable, http.StatusInternalServerError, http.StatusUnauthorized} {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(code)
			_, _ = w.Write([]byte(`{"nodes":[]}`))
		}))
		rep, err := New(srv.URL, "", time.Second).Fetch(context.Background(), "p/")
		srv.Close()

		if err == nil {
			t.Errorf("HTTP %d returned report %+v with no error", code, rep)
		}
		if rep != nil {
			t.Errorf("HTTP %d returned a non-nil report alongside the error", code)
		}
	}
}

func TestFetch_UnreachableCoordinatorIsAnError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	url := srv.URL
	srv.Close() // nothing is listening now

	if _, err := New(url, "", 500*time.Millisecond).Fetch(context.Background(), "p/"); err == nil {
		t.Fatal("Fetch against a dead coordinator succeeded")
	}
}

// TestFetch_UnknownWireFieldsAreIgnored keeps the coordinator free to add
// fields without a lockstep release of this out-of-tree module.
func TestFetch_UnknownWireFieldsAreIgnored(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"prefix":"p/","future_field":42,
			"nodes":[{"node_ip":"10.0.0.1","fraction":1,"some_new_thing":"x"}]}`))
	}))
	defer srv.Close()

	rep, err := New(srv.URL, "", time.Second).Fetch(context.Background(), "p/")
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	if len(rep.Nodes) != 1 || rep.Nodes[0].Fraction != 1 {
		t.Errorf("report = %+v, want the known fields decoded", rep)
	}
}

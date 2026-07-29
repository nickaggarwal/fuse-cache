package cache

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"
)

func newRawPeerStorage() *PeerStorage {
	ps, _ := NewPeerStorage(nil, 5*time.Second, "local", false, false, true, "k")
	return ps
}

// TestReadFromPeerRaw_Success verifies the reader fetches the exact bytes, sizes
// its buffer from Content-Length, and sends the API key.
func TestReadFromPeerRaw_Success(t *testing.T) {
	payload := []byte(strings.Repeat("xy", 4096)) // 8KiB
	var gotKey, gotPath string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotKey = r.Header.Get("X-API-Key")
		gotPath = r.URL.Query().Get("path")
		w.Header().Set("Content-Length", strconv.Itoa(len(payload)))
		w.WriteHeader(http.StatusOK)
		w.Write(payload)
	}))
	defer srv.Close()

	ps := newRawPeerStorage()
	addr := strings.TrimPrefix(srv.URL, "http://")
	data, err := ps.readFromPeerRaw(context.Background(), addr, "/file_chunk_3")
	if err != nil {
		t.Fatalf("readFromPeerRaw: %v", err)
	}
	if string(data) != string(payload) {
		t.Fatalf("data mismatch: got %d bytes", len(data))
	}
	if gotKey != "k" {
		t.Fatalf("X-API-Key = %q, want k", gotKey)
	}
	if gotPath != "/file_chunk_3" {
		t.Fatalf("path = %q, want /file_chunk_3", gotPath)
	}
}

// TestReadFromPeerRaw_ErrorStatus confirms a non-200 surfaces an error so the
// caller falls back to gRPC.
func TestReadFromPeerRaw_ErrorStatus(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "nope", http.StatusNotFound)
	}))
	defer srv.Close()

	ps := newRawPeerStorage()
	addr := strings.TrimPrefix(srv.URL, "http://")
	if _, err := ps.readFromPeerRaw(context.Background(), addr, "/missing"); err == nil {
		t.Fatal("expected error on 404, got nil")
	}
}

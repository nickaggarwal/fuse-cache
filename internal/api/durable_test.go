package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"fuse-client/internal/cache"
)

// receiptCacheManager is a mock cache with a scripted receipt.
type receiptCacheManager struct {
	*mockCacheManager
	got     cache.DurabilityRequest
	receipt *cache.DurabilityReceipt
	err     error
}

func (r *receiptCacheManager) DurabilityReceipt(_ context.Context, req cache.DurabilityRequest) (*cache.DurabilityReceipt, error) {
	r.got = req
	return r.receipt, r.err
}

func TestDurableEndpointStatusCodesAndWireShape(t *testing.T) {
	cm := &receiptCacheManager{mockCacheManager: newMockCacheManager()}
	router := NewHandler(cm, nil, "node-1", "").SetupRoutes()

	cm.receipt = &cache.DurabilityReceipt{Durable: true, Paths: []cache.PathDurability{
		{Path: "/ckpt/a", State: cache.DurabilityPersisted, Size: 10, Objects: 1, Generation: 42, Source: "tracker"},
	}}
	w := httptest.NewRecorder()
	router.ServeHTTP(w, httptest.NewRequest("POST", "/api/cache/durable",
		strings.NewReader(`{"paths":["/ckpt/a"],"timeout_seconds":2.5,"retry_failed":true}`)))
	if w.Code != http.StatusOK {
		t.Fatalf("durable receipt: code %d body %s", w.Code, w.Body)
	}
	if cm.got.Wait != 2500*time.Millisecond || !cm.got.RetryFailed || len(cm.got.Paths) != 1 {
		t.Fatalf("request not passed through: %+v", cm.got)
	}
	var body map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	// The contract fields pod-snapshotter consumes.
	if body["durable"] != true || body["peer_id"] != "node-1" {
		t.Fatalf("wire shape: %v", body)
	}
	p := body["paths"].([]any)[0].(map[string]any)
	for _, k := range []string{"path", "state", "size", "is_chunked", "objects", "generation", "source"} {
		if _, ok := p[k]; !ok {
			t.Fatalf("receipt path entry missing %q: %v", k, p)
		}
	}

	cm.receipt = &cache.DurabilityReceipt{Durable: false, Paths: []cache.PathDurability{
		{Path: "/ckpt/a", State: cache.DurabilityUploading},
	}}
	w = httptest.NewRecorder()
	router.ServeHTTP(w, httptest.NewRequest("GET", "/api/cache/durable?path=/ckpt/a&retry_failed=true", nil))
	if w.Code != http.StatusAccepted {
		t.Fatalf("pending receipt must be 202, got %d", w.Code)
	}
	if cm.got.RetryFailed || cm.got.Wait != 0 {
		t.Fatalf("GET must not wait or kick retries: %+v", cm.got)
	}

	cm.err = fmt.Errorf("%w: prefix must not be the root", cache.ErrDurabilityBadRequest)
	w = httptest.NewRecorder()
	router.ServeHTTP(w, httptest.NewRequest("POST", "/api/cache/durable", strings.NewReader(`{"prefix":"/"}`)))
	if w.Code != http.StatusBadRequest {
		t.Fatalf("bad request must be 400, got %d", w.Code)
	}

	w = httptest.NewRecorder()
	router.ServeHTTP(w, httptest.NewRequest("POST", "/api/cache/durable", strings.NewReader(`{"pathz":["/x"]}`)))
	if w.Code != http.StatusBadRequest {
		t.Fatalf("unknown field must be 400 (a typo must not read as an empty request), got %d", w.Code)
	}
}

func TestDurableEndpointWithoutCapabilityIs501(t *testing.T) {
	router := NewHandler(newMockCacheManager(), nil, "n", "").SetupRoutes()
	w := httptest.NewRecorder()
	router.ServeHTTP(w, httptest.NewRequest("POST", "/api/cache/durable", strings.NewReader(`{"paths":["/a"]}`)))
	if w.Code != http.StatusNotImplemented {
		t.Fatalf("got %d", w.Code)
	}
}

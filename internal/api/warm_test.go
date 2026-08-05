package api

// Tests for POST /api/cache/warm: sync result passthrough, async 202,
// validation, and strategy fields reaching the cache manager.

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"fuse-client/internal/cache"
)

// warmMockCache adds PrefixWarmer to the standard mock.
type warmMockCache struct {
	*mockCacheManager
	mu    sync.Mutex
	calls []struct {
		prefix string
		opts   cache.WarmupOptions
	}
}

func (m *warmMockCache) WarmPrefixOpts(_ context.Context, prefix string, opts cache.WarmupOptions) (*cache.WarmupResult, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls = append(m.calls, struct {
		prefix string
		opts   cache.WarmupOptions
	}{prefix, opts})
	return &cache.WarmupResult{Mode: opts.Mode, Files: 3, Warmed: 2, AlreadyLocal: 1}, nil
}

func (m *warmMockCache) callCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.calls)
}

func TestHandleCacheWarm_SyncAsyncAndValidation(t *testing.T) {
	cm := &warmMockCache{mockCacheManager: newMockCacheManager()}
	h := NewHandler(cm, nil, "test-peer", "")
	router := h.SetupRoutes()

	do := func(body string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodPost, "/api/cache/warm", bytes.NewBufferString(body))
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req)
		return rec
	}

	// Sync: full result comes back, strategy fields reach the manager.
	rec := do(`{"prefix":"/models/x","source":"cloud-only","bandwidth":"max"}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("sync status = %d body=%s", rec.Code, rec.Body.String())
	}
	var res cache.WarmupResult
	if err := json.Unmarshal(rec.Body.Bytes(), &res); err != nil || res.Warmed != 2 {
		t.Fatalf("sync result = %+v err=%v", res, err)
	}
	cm.mu.Lock()
	call := cm.calls[0]
	cm.mu.Unlock()
	if call.prefix != "/models/x" || call.opts.Mode != "full" ||
		call.opts.Source != "cloud-only" || call.opts.Bandwidth != "max" {
		t.Fatalf("manager saw %+v", call)
	}

	// Async: 202 immediately, warm happens in the background.
	rec = do(`{"prefix":"/models/y","async":true}`)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("async status = %d", rec.Code)
	}
	deadline := time.Now().Add(2 * time.Second)
	for cm.callCount() < 2 {
		if time.Now().After(deadline) {
			t.Fatal("async warm never reached the cache manager")
		}
		time.Sleep(5 * time.Millisecond)
	}

	// Validation: missing prefix and bad JSON are 400s.
	if rec := do(`{}`); rec.Code != http.StatusBadRequest {
		t.Fatalf("missing prefix status = %d", rec.Code)
	}
	if rec := do(`{nope`); rec.Code != http.StatusBadRequest {
		t.Fatalf("bad JSON status = %d", rec.Code)
	}

	// A manager without PrefixWarmer: 501.
	plain := NewHandler(newMockCacheManager(), nil, "test-peer", "")
	req := httptest.NewRequest(http.MethodPost, "/api/cache/warm", bytes.NewBufferString(`{"prefix":"/x"}`))
	recPlain := httptest.NewRecorder()
	plain.SetupRoutes().ServeHTTP(recPlain, req)
	if recPlain.Code != http.StatusNotImplemented {
		t.Fatalf("non-warmer manager status = %d", recPlain.Code)
	}
}

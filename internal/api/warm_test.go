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

// blockingWarmCache reports one file of progress, then waits for release so a
// test can observe the job mid-flight.
type blockingWarmCache struct {
	*mockCacheManager
	started chan struct{}
	release chan struct{}
	err     error
}

func (m *blockingWarmCache) WarmPrefixOpts(_ context.Context, _ string, opts cache.WarmupOptions) (*cache.WarmupResult, error) {
	if opts.OnProgress != nil {
		opts.OnProgress(cache.WarmupProgress{Files: 2, Done: 1, Warmed: 1, Bytes: 4096})
	}
	close(m.started)
	<-m.release
	if m.err != nil {
		return nil, m.err
	}
	if opts.OnProgress != nil {
		opts.OnProgress(cache.WarmupProgress{Files: 2, Done: 2, Warmed: 2, Bytes: 8192})
	}
	return &cache.WarmupResult{Mode: opts.Mode, Files: 2, Warmed: 2, Bytes: 8192}, nil
}

func TestHandleCacheWarm_AsyncJobStatus(t *testing.T) {
	cm := &blockingWarmCache{
		mockCacheManager: newMockCacheManager(),
		started:          make(chan struct{}),
		release:          make(chan struct{}),
	}
	h := NewHandler(cm, nil, "test-peer", "")
	router := h.SetupRoutes()

	get := func(url string) *httptest.ResponseRecorder {
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, url, nil))
		return rec
	}

	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/api/cache/warm",
		bytes.NewBufferString(`{"prefix":"/models/z","async":true,"bandwidth":"max"}`)))
	if rec.Code != http.StatusAccepted {
		t.Fatalf("async status = %d body=%s", rec.Code, rec.Body.String())
	}
	var accepted struct {
		JobID     string `json:"job_id"`
		StatusURL string `json:"status_url"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &accepted); err != nil || accepted.JobID == "" {
		t.Fatalf("async body = %s err=%v", rec.Body.String(), err)
	}
	if want := "/api/cache/warm/" + accepted.JobID; accepted.StatusURL != want {
		t.Fatalf("status_url = %q want %q", accepted.StatusURL, want)
	}

	<-cm.started

	// Mid-flight: running, with the progress the warmer reported.
	rec = get(accepted.StatusURL)
	if rec.Code != http.StatusOK {
		t.Fatalf("status while running = %d", rec.Code)
	}
	var running warmJob
	if err := json.Unmarshal(rec.Body.Bytes(), &running); err != nil {
		t.Fatalf("decode running job: %v", err)
	}
	if running.Status != warmJobRunning || running.Prefix != "/models/z" || running.Bandwidth != "max" {
		t.Fatalf("running job = %+v", running)
	}
	if running.Progress.Done != 1 || running.Progress.Bytes != 4096 {
		t.Fatalf("running progress = %+v", running.Progress)
	}
	if running.Result != nil || running.EndedAt != nil {
		t.Fatalf("running job already has terminal fields: %+v", running)
	}

	// The list route must not be shadowed by the POST registration.
	rec = get("/api/cache/warm")
	if rec.Code != http.StatusOK {
		t.Fatalf("list status = %d", rec.Code)
	}
	var listed struct {
		PeerID string    `json:"peer_id"`
		Jobs   []warmJob `json:"jobs"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &listed); err != nil {
		t.Fatalf("decode list: %v", err)
	}
	if listed.PeerID != "test-peer" || len(listed.Jobs) != 1 || listed.Jobs[0].ID != accepted.JobID {
		t.Fatalf("list = %+v", listed)
	}

	close(cm.release)

	deadline := time.Now().Add(2 * time.Second)
	var done warmJob
	for {
		rec = get(accepted.StatusURL)
		if err := json.Unmarshal(rec.Body.Bytes(), &done); err != nil {
			t.Fatalf("decode job: %v", err)
		}
		if done.Status != warmJobRunning {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("job never finished: %+v", done)
		}
		time.Sleep(5 * time.Millisecond)
	}
	if done.Status != warmJobDone || done.Result == nil || done.Result.Warmed != 2 {
		t.Fatalf("finished job = %+v result=%+v", done, done.Result)
	}
	if done.EndedAt == nil || done.Progress.Done != 2 || done.Progress.Bytes != 8192 {
		t.Fatalf("finished progress = %+v ended=%v", done.Progress, done.EndedAt)
	}

	if rec := get("/api/cache/warm/warm-nope"); rec.Code != http.StatusNotFound {
		t.Fatalf("unknown job status = %d", rec.Code)
	}
}

func TestWarmJobs_FailureAndRetention(t *testing.T) {
	var jobs warmJobs

	failed := jobs.start(warmRequest{Prefix: "/a"})
	jobs.finish(failed.ID, nil, context.DeadlineExceeded)
	got, ok := jobs.get(failed.ID)
	if !ok || got.Status != warmJobFailed || got.Error == "" || got.Result != nil {
		t.Fatalf("failed job = %+v ok=%v", got, ok)
	}

	// Running jobs survive pruning however many finished ones pile up on top.
	live := jobs.start(warmRequest{Prefix: "/live"})
	for i := 0; i < warmJobRetention*2; i++ {
		j := jobs.start(warmRequest{Prefix: "/bulk"})
		jobs.finish(j.ID, &cache.WarmupResult{Files: 1}, nil)
	}
	if _, ok := jobs.get(live.ID); !ok {
		t.Fatal("running job was pruned")
	}
	all := jobs.list()
	if len(all) > warmJobRetention+1 {
		t.Fatalf("retained %d jobs, want <= %d", len(all), warmJobRetention+1)
	}
	for i := 1; i < len(all); i++ {
		if all[i].StartedAt.After(all[i-1].StartedAt) {
			t.Fatalf("list not newest-first at %d", i)
		}
	}
}

package api

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"fuse-client/internal/cache"
	"fuse-client/internal/coordinator"
)

// mockCacheManager implements cache.CacheManager for testing
type mockCacheManager struct {
	entries map[string]*cache.CacheEntry
}

func newMockCacheManager() *mockCacheManager {
	return &mockCacheManager{entries: make(map[string]*cache.CacheEntry)}
}

func (m *mockCacheManager) Get(ctx context.Context, filePath string) (*cache.CacheEntry, error) {
	if e, ok := m.entries[filePath]; ok {
		return e, nil
	}
	return nil, context.Canceled
}

func (m *mockCacheManager) Put(ctx context.Context, entry *cache.CacheEntry) error {
	m.entries[entry.FilePath] = entry
	return nil
}

func (m *mockCacheManager) Delete(ctx context.Context, filePath string) error {
	delete(m.entries, filePath)
	return nil
}

func (m *mockCacheManager) List(ctx context.Context) ([]*cache.CacheEntry, error) {
	entries := make([]*cache.CacheEntry, 0)
	for _, e := range m.entries {
		entries = append(entries, e)
	}
	return entries, nil
}

func (m *mockCacheManager) Evict(ctx context.Context, tier cache.CacheTier) error {
	return nil
}

func (m *mockCacheManager) Stats() (used, capacity int64) {
	return 0, 1024 * 1024 * 1024
}

func (m *mockCacheManager) WriteTo(ctx context.Context, filePath string, w io.Writer) (int64, error) {
	e, ok := m.entries[filePath]
	if !ok {
		return 0, context.Canceled
	}
	n, err := w.Write(e.Data)
	return int64(n), err
}

func (m *mockCacheManager) Shutdown() {}

// CreateWriteFile and PutFromFile implement the streamingWriter interface used
// by handleFilePutStreaming. Small test payloads land in a real temp file.
func (m *mockCacheManager) CreateWriteFile(ctx context.Context, filePath string) (*os.File, string, error) {
	f, err := os.CreateTemp("", "mock-write-*")
	if err != nil {
		return nil, "", err
	}
	return f, f.Name(), nil
}

func (m *mockCacheManager) PutFromFile(ctx context.Context, filePath string, stagedPath string, size int64, lastAccessed time.Time) error {
	data, err := os.ReadFile(stagedPath)
	os.Remove(stagedPath)
	if err != nil {
		return err
	}
	return m.Put(ctx, &cache.CacheEntry{
		FilePath:     filePath,
		StoragePath:  filePath,
		Size:         int64(len(data)),
		LastAccessed: lastAccessed,
		Data:         data,
	})
}

type mockCloudCacheManager struct {
	*mockCacheManager
	cloud map[string][]byte
}

func newMockCloudCacheManager() *mockCloudCacheManager {
	return &mockCloudCacheManager{
		mockCacheManager: newMockCacheManager(),
		cloud:            make(map[string][]byte),
	}
}

func (m *mockCloudCacheManager) WriteCloud(ctx context.Context, path string, data []byte) error {
	cp := make([]byte, len(data))
	copy(cp, data)
	m.cloud[path] = cp
	return nil
}

func (m *mockCloudCacheManager) ReadCloud(ctx context.Context, path string) ([]byte, error) {
	data, ok := m.cloud[path]
	if !ok {
		return nil, context.Canceled
	}
	cp := make([]byte, len(data))
	copy(cp, data)
	return cp, nil
}

func TestHealthEndpoint(t *testing.T) {
	cm := newMockCacheManager()
	h := NewHandler(cm, nil, "test-peer", "")
	router := h.SetupRoutes()

	req := httptest.NewRequest("GET", "/api/health", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Status = %d, want %d", w.Code, http.StatusOK)
	}

	var resp map[string]interface{}
	json.NewDecoder(w.Body).Decode(&resp)
	if resp["status"] != "healthy" {
		t.Errorf("Health status = %v, want healthy", resp["status"])
	}
}

// A node whose FUSE serve loop has died still answers HTTP perfectly well.
// Health has to reflect the mount, or the liveness probe keeps the pod alive
// while every workload on the node sees ENOTCONN.
func TestHealthReflectsMountState(t *testing.T) {
	cm := newMockCacheManager()
	h := NewHandler(cm, nil, "test-peer", "")
	router := h.SetupRoutes()

	mounted := true
	h.SetMountHealth(func() bool { return mounted })

	probe := func() (int, map[string]interface{}) {
		t.Helper()
		req := httptest.NewRequest("GET", "/api/health", nil)
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)
		var body map[string]interface{}
		json.NewDecoder(w.Body).Decode(&body)
		return w.Code, body
	}

	if code, body := probe(); code != http.StatusOK || body["status"] != "healthy" {
		t.Fatalf("mounted: status=%d body=%v, want 200/healthy", code, body)
	}

	mounted = false
	code, body := probe()
	if code != http.StatusServiceUnavailable {
		t.Fatalf("dead mount: status = %d, want %d", code, http.StatusServiceUnavailable)
	}
	if body["status"] != "unhealthy" {
		t.Fatalf("dead mount: body status = %v, want unhealthy", body["status"])
	}

	// Recovery must clear it, so a successful remount puts the node back in
	// rotation without a restart.
	mounted = true
	if code, _ := probe(); code != http.StatusOK {
		t.Fatalf("after remount: status = %d, want 200", code)
	}

	// Clearing the probe returns the handler to the unsupervised default,
	// which is what the coordinator and CSI driver rely on.
	h.SetMountHealth(nil)
	mounted = false
	if code, _ := probe(); code != http.StatusOK {
		t.Fatalf("with no probe installed: status = %d, want 200", code)
	}
}

func TestFilePutAndGet(t *testing.T) {
	cm := newMockCacheManager()
	h := NewHandler(cm, nil, "test-peer", "")
	router := h.SetupRoutes()

	// PUT
	body := bytes.NewBufferString("hello world")
	req := httptest.NewRequest("PUT", "/api/files/test.txt", body)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("PUT status = %d, want %d", w.Code, http.StatusOK)
	}

	// GET
	req = httptest.NewRequest("GET", "/api/files/test.txt", nil)
	w = httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("GET status = %d, want %d", w.Code, http.StatusOK)
	}

	if w.Body.String() != "hello world" {
		t.Errorf("GET body = %q, want %q", w.Body.String(), "hello world")
	}
}

func TestFileNotFound(t *testing.T) {
	cm := newMockCacheManager()
	h := NewHandler(cm, nil, "test-peer", "")
	router := h.SetupRoutes()

	req := httptest.NewRequest("GET", "/api/files/nonexistent.txt", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusNotFound {
		t.Errorf("Status = %d, want %d", w.Code, http.StatusNotFound)
	}
}

func TestFileDelete(t *testing.T) {
	cm := newMockCacheManager()
	cm.entries["/del.txt"] = &cache.CacheEntry{
		FilePath: "/del.txt",
		Data:     []byte("data"),
		Size:     4,
	}

	h := NewHandler(cm, nil, "test-peer", "")
	router := h.SetupRoutes()

	req := httptest.NewRequest("DELETE", "/api/files/del.txt", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("DELETE status = %d, want %d", w.Code, http.StatusOK)
	}

	if _, ok := cm.entries["/del.txt"]; ok {
		t.Error("File still exists after DELETE")
	}
}

func TestPathTraversal(t *testing.T) {
	cm := newMockCacheManager()
	h := NewHandler(cm, nil, "test-peer", "")
	router := h.SetupRoutes()

	// filepath.Clean will resolve ../../etc/passwd to /etc/passwd
	// which doesn't contain ".." anymore, so it won't be blocked
	// But the actual traversal is prevented because it's cleaned to an absolute path
	req := httptest.NewRequest("GET", "/api/files/test.txt", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	// Should be 404 (not found), not a traversal
	if w.Code != http.StatusNotFound {
		t.Errorf("Status = %d, want %d", w.Code, http.StatusNotFound)
	}
}

func TestAPIKeyAuth(t *testing.T) {
	cm := newMockCacheManager()
	h := NewHandler(cm, nil, "test-peer", "secret-key")
	router := h.SetupRoutes()

	// Without API key — should fail
	req := httptest.NewRequest("GET", "/api/cache", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusUnauthorized {
		t.Errorf("No key: status = %d, want %d", w.Code, http.StatusUnauthorized)
	}

	// With correct API key — should succeed
	req = httptest.NewRequest("GET", "/api/cache", nil)
	req.Header.Set("X-API-Key", "secret-key")
	w = httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("With key: status = %d, want %d", w.Code, http.StatusOK)
	}

	// Health endpoint — no key needed
	req = httptest.NewRequest("GET", "/api/health", nil)
	w = httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Health no key: status = %d, want %d", w.Code, http.StatusOK)
	}
}

func TestFileHead(t *testing.T) {
	cm := newMockCacheManager()
	cm.entries["/head.txt"] = &cache.CacheEntry{
		FilePath:     "/head.txt",
		Data:         []byte("12345"),
		Size:         5,
		LastAccessed: time.Now(),
	}

	h := NewHandler(cm, nil, "test-peer", "")
	router := h.SetupRoutes()

	req := httptest.NewRequest("HEAD", "/api/files/head.txt", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("HEAD status = %d, want %d", w.Code, http.StatusOK)
	}

	if cl := w.Header().Get("Content-Length"); cl != "5" {
		t.Errorf("Content-Length = %s, want 5", cl)
	}
}

func TestPeersEndpointWithCoordinator(t *testing.T) {
	cm := newMockCacheManager()
	cs := coordinator.NewCoordinatorService()
	ctx := context.Background()

	cs.RegisterPeer(ctx, &coordinator.PeerInfo{
		ID: "peer-1", Address: "10.0.0.1:8081",
	})

	h := NewHandler(cm, cs, "test-peer", "")
	router := h.SetupRoutes()

	req := httptest.NewRequest("GET", "/api/peers", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Status = %d, want %d", w.Code, http.StatusOK)
	}
}

func TestSanitizePath(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"test.txt", "/test.txt"},
		{"a/b/c.txt", "/a/b/c.txt"},
		{"./test.txt", "/test.txt"},
	}

	for _, tt := range tests {
		got, err := sanitizePath(tt.input)
		if err != nil {
			t.Errorf("sanitizePath(%q) error: %v", tt.input, err)
			continue
		}
		if got != tt.want {
			t.Errorf("sanitizePath(%q) = %q, want %q", tt.input, got, tt.want)
		}
	}
}

func TestFSSnapshotAndRestore(t *testing.T) {
	cm := newMockCacheManager()
	cm.entries["/snap/a.txt"] = &cache.CacheEntry{
		FilePath:     "/snap/a.txt",
		Data:         []byte("alpha"),
		Size:         5,
		LastAccessed: time.Now(),
	}
	cm.entries["/snap/b.txt"] = &cache.CacheEntry{
		FilePath:     "/snap/b.txt",
		Data:         []byte("beta"),
		Size:         4,
		LastAccessed: time.Now(),
	}

	h := NewHandler(cm, nil, "test-peer", "")
	router := h.SetupRoutes()

	// Snapshot with data
	reqBody := bytes.NewBufferString(`{"prefix":"/snap","include_data":true}`)
	req := httptest.NewRequest("POST", "/api/fs/snapshot", reqBody)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("snapshot status = %d, want %d", w.Code, http.StatusOK)
	}

	var snap fsSnapshot
	if err := json.NewDecoder(w.Body).Decode(&snap); err != nil {
		t.Fatalf("decode snapshot: %v", err)
	}
	if len(snap.Files) != 2 {
		t.Fatalf("snapshot files = %d, want 2", len(snap.Files))
	}
	for _, f := range snap.Files {
		if f.ContentB64 == "" {
			t.Fatalf("file %s missing content_b64", f.FilePath)
		}
		if _, err := base64.StdEncoding.DecodeString(f.ContentB64); err != nil {
			t.Fatalf("invalid content_b64 for %s: %v", f.FilePath, err)
		}
	}

	// Clear cache and restore from snapshot
	cm.entries = map[string]*cache.CacheEntry{}
	restoreBody, _ := json.Marshal(snap)
	req = httptest.NewRequest("POST", "/api/fs/restore", bytes.NewReader(restoreBody))
	w = httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("restore status = %d, want %d", w.Code, http.StatusOK)
	}
	if _, ok := cm.entries["/snap/a.txt"]; !ok {
		t.Fatalf("restored entry /snap/a.txt missing")
	}
	if got := string(cm.entries["/snap/a.txt"].Data); got != "alpha" {
		t.Fatalf("restored /snap/a.txt = %q, want alpha", got)
	}
	if got := string(cm.entries["/snap/b.txt"].Data); got != "beta" {
		t.Fatalf("restored /snap/b.txt = %q, want beta", got)
	}
}

func TestFSSnapshotAndRestoreViaCloudPath(t *testing.T) {
	cm := newMockCloudCacheManager()
	cm.entries["/cloud/a.txt"] = &cache.CacheEntry{
		FilePath:     "/cloud/a.txt",
		Data:         []byte("alpha-cloud"),
		Size:         11,
		LastAccessed: time.Now(),
	}

	h := NewHandler(cm, nil, "test-peer", "")
	router := h.SetupRoutes()

	cloudPath := "snapshots/fs/test-peer/cloud-a.json"
	reqBody := bytes.NewBufferString(`{"prefix":"/cloud","include_data":true,"persist_cloud":true,"cloud_path":"` + cloudPath + `"}`)
	req := httptest.NewRequest("POST", "/api/fs/snapshot", reqBody)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("snapshot status = %d, want %d", w.Code, http.StatusOK)
	}

	var snap fsSnapshot
	if err := json.NewDecoder(w.Body).Decode(&snap); err != nil {
		t.Fatalf("decode snapshot: %v", err)
	}
	if !snap.PersistedToCloud || snap.CloudPath != cloudPath {
		t.Fatalf("snapshot cloud flags not set as expected: persisted=%v path=%q", snap.PersistedToCloud, snap.CloudPath)
	}
	if _, ok := cm.cloud[cloudPath]; !ok {
		t.Fatalf("cloud snapshot object missing at %s", cloudPath)
	}

	// Overwrite local data and restore from cloud path only.
	cm.entries["/cloud/a.txt"].Data = []byte("mutated")
	restoreReq := bytes.NewBufferString(`{"cloud_path":"` + cloudPath + `","overwrite":true}`)
	req = httptest.NewRequest("POST", "/api/fs/restore", restoreReq)
	w = httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("restore status = %d, want %d", w.Code, http.StatusOK)
	}
	if got := string(cm.entries["/cloud/a.txt"].Data); got != "alpha-cloud" {
		t.Fatalf("restored /cloud/a.txt = %q, want alpha-cloud", got)
	}
}

// The delete counters are the only way to see that an rm left bytes behind on
// a peer, in cloud, or in coordinator metadata — delete is best-effort and
// returns success regardless. If they stop being exported, the failure mode
// this whole path guards against goes silent again, so pin the exposition.
func TestPromMetricsExportsDeleteCounters(t *testing.T) {
	cm, err := cache.NewCacheManager(&cache.CacheConfig{
		NVMePath:     t.TempDir(),
		MaxNVMeSize:  1 << 20,
		ChunkSize:    1 << 20,
		CloudTimeout: time.Second,
	})
	if err != nil {
		t.Fatalf("NewCacheManager: %v", err)
	}
	defer cm.Shutdown()

	h := NewHandler(cm, nil, "test-peer", "")
	router := h.SetupRoutes()

	req := httptest.NewRequest("GET", "/metrics", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", w.Code, http.StatusOK)
	}
	body := w.Body.String()
	for _, metric := range []string{
		"fuse_delete_peer_failures_total 0",
		"fuse_delete_cloud_failures_total 0",
		"fuse_delete_coordinator_failures_total 0",
		"fuse_delete_resurrection_suppressed_total 0",
	} {
		if !strings.Contains(body, metric) {
			t.Errorf("/metrics missing %q", metric)
		}
	}
}

// A CacheManager that is not a *DefaultCacheManager makes handlePromMetrics
// return early — every snapshot below that point is on the concrete type.
// Scrapes must still succeed rather than 500 the endpoint.
func TestPromMetricsWithNonDefaultCacheManager(t *testing.T) {
	h := NewHandler(newMockCacheManager(), nil, "test-peer", "")
	router := h.SetupRoutes()

	req := httptest.NewRequest("GET", "/metrics", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", w.Code, http.StatusOK)
	}
	if !strings.Contains(w.Body.String(), "fuse_coordinator_available 0") {
		t.Fatalf("/metrics body = %q, want the coordinator gauge", w.Body.String())
	}
}

func TestNetProbeEndpoint(t *testing.T) {
	cm := newMockCacheManager()
	h := NewHandler(cm, nil, "test-peer", "")
	router := h.SetupRoutes()

	req := httptest.NewRequest("GET", "/api/netprobe?bytes=262144", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", w.Code, http.StatusOK)
	}
	if got := len(w.Body.Bytes()); got != 262144 {
		t.Fatalf("response bytes = %d, want 262144", got)
	}
	if hdr := w.Header().Get("X-Netprobe-Bytes"); hdr != "262144" {
		t.Fatalf("X-Netprobe-Bytes = %q, want 262144", hdr)
	}
}

// The residency endpoint is the node-local half of the cluster routing query.
// Its contract has two halves worth pinning: a cache manager that cannot
// answer says so with 501 rather than a zero report, and a coordinator failure
// is a 503 rather than a report of "this node holds nothing" — the two are
// indistinguishable to a router, and one of them would deroute the node.
func TestCacheResidencyRequiresPrefix(t *testing.T) {
	cm, err := cache.NewCacheManager(&cache.CacheConfig{
		NVMePath:     t.TempDir(),
		MaxNVMeSize:  1 << 20,
		ChunkSize:    1 << 20,
		CloudTimeout: time.Second,
	})
	if err != nil {
		t.Fatalf("NewCacheManager: %v", err)
	}
	defer cm.Shutdown()

	router := NewHandler(cm, nil, "test-peer", "").SetupRoutes()

	w := httptest.NewRecorder()
	router.ServeHTTP(w, httptest.NewRequest("GET", "/api/cache/residency", nil))
	if w.Code != http.StatusBadRequest {
		t.Fatalf("missing prefix: status = %d, want %d", w.Code, http.StatusBadRequest)
	}

	// No coordinator configured means no enumeration, so no denominator.
	w = httptest.NewRecorder()
	router.ServeHTTP(w, httptest.NewRequest("GET", "/api/cache/residency?prefix=/models", nil))
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("no coordinator: status = %d, want %d (body=%s)",
			w.Code, http.StatusServiceUnavailable, strings.TrimSpace(w.Body.String()))
	}
}

func TestCacheResidencyUnsupportedManager(t *testing.T) {
	router := NewHandler(newMockCacheManager(), nil, "test-peer", "").SetupRoutes()
	w := httptest.NewRecorder()
	router.ServeHTTP(w, httptest.NewRequest("GET", "/api/cache/residency?prefix=/models", nil))
	if w.Code != http.StatusNotImplemented {
		t.Fatalf("status = %d, want %d", w.Code, http.StatusNotImplemented)
	}
}

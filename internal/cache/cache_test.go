package cache

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"fuse-client/internal/coordinator"
)

// mockStorage is a simple in-memory TierStorage for testing
type mockStorage struct {
	mu   sync.RWMutex
	data map[string][]byte
}

func newMockStorage() *mockStorage {
	return &mockStorage{data: make(map[string][]byte)}
}

func (m *mockStorage) Read(ctx context.Context, path string) ([]byte, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if d, ok := m.data[path]; ok {
		out := make([]byte, len(d))
		copy(out, d)
		return out, nil
	}
	return nil, context.Canceled
}

func (m *mockStorage) Write(ctx context.Context, path string, data []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.data[path] = append([]byte(nil), data...)
	return nil
}

func (m *mockStorage) Delete(ctx context.Context, path string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.data, path)
	return nil
}

func (m *mockStorage) Exists(ctx context.Context, path string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	_, ok := m.data[path]
	return ok
}

func (m *mockStorage) Size(ctx context.Context, path string) (int64, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if d, ok := m.data[path]; ok {
		return int64(len(d)), nil
	}
	return 0, context.Canceled
}

func newTestCacheManager() *DefaultCacheManager {
	nvme := newMockStorage()
	peer := newMockStorage()
	cloud := newMockStorage()
	return &DefaultCacheManager{
		config: &CacheConfig{
			NVMePath:           "/tmp/test",
			MaxNVMeSize:        1024 * 1024, // 1MB
			ChunkSize:          4 * 1024 * 1024,
			CloudRetryCount:    1,
			CloudRetryBaseWait: time.Millisecond,
			MinPeerReplicas:    3,
			CloudTimeout:       5 * time.Second,
		},
		nvmeStorage:  nvme,
		peerStorage:  peer,
		cloudStorage: cloud,
		entries:      make(map[string]*CacheEntry),
		logger:       log.New(log.Writer(), "[CACHE-TEST] ", log.LstdFlags),
		metrics:      NewCacheMetrics(),
		rangeChunks:  make(map[string]*chunkFileCache),
		hybridHints:  make(map[string]hybridReadHint),
	}
}

func TestCacheManager_PutAndGet(t *testing.T) {
	cm := newTestCacheManager()
	ctx := context.Background()

	entry := &CacheEntry{
		FilePath:     "/test.txt",
		Size:         5,
		LastAccessed: time.Now(),
		Data:         []byte("hello"),
	}

	if err := cm.Put(ctx, entry); err != nil {
		t.Fatalf("Put: %v", err)
	}

	got, err := cm.Get(ctx, "/test.txt")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}

	if string(got.Data) != "hello" {
		t.Errorf("Get data = %q, want %q", got.Data, "hello")
	}
}

func TestCacheManager_Delete(t *testing.T) {
	cm := newTestCacheManager()
	ctx := context.Background()

	entry := &CacheEntry{
		FilePath:     "/del.txt",
		Size:         4,
		LastAccessed: time.Now(),
		Data:         []byte("data"),
	}

	cm.Put(ctx, entry)

	if err := cm.Delete(ctx, "/del.txt"); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	// Verify entry removed from entries map
	cm.mu.RLock()
	_, exists := cm.entries["/del.txt"]
	cm.mu.RUnlock()

	if exists {
		t.Error("Entry still in cache entries map after delete")
	}
}

func TestCacheManager_List(t *testing.T) {
	cm := newTestCacheManager()
	ctx := context.Background()

	for _, name := range []string{"/a.txt", "/b.txt", "/c.txt"} {
		cm.Put(ctx, &CacheEntry{
			FilePath:     name,
			Size:         1,
			LastAccessed: time.Now(),
			Data:         []byte("x"),
		})
	}

	entries, err := cm.List(ctx)
	if err != nil {
		t.Fatalf("List: %v", err)
	}

	if len(entries) != 3 {
		t.Errorf("List returned %d entries, want 3", len(entries))
	}
}

func TestCacheManager_Evict(t *testing.T) {
	cm := newTestCacheManager()
	cm.config.MaxNVMeSize = 100 // 100 bytes
	ctx := context.Background()

	// Fill NVMe to capacity
	for i := 0; i < 5; i++ {
		entry := &CacheEntry{
			FilePath:     fmt.Sprintf("/file%d.txt", i),
			Size:         30,
			LastAccessed: time.Now().Add(time.Duration(i) * time.Second),
			Data:         make([]byte, 30),
		}
		cm.nvmeStorage.Write(ctx, entry.FilePath, entry.Data)
		cm.mu.Lock()
		cm.entries[entry.FilePath] = entry
		entry.Tier = TierNVMe
		cm.nvmeUsed += entry.Size
		cm.mu.Unlock()
	}

	// nvmeUsed should be 150
	if cm.nvmeUsed != 150 {
		t.Fatalf("nvmeUsed = %d, want 150", cm.nvmeUsed)
	}

	if err := cm.Evict(ctx, TierNVMe); err != nil {
		t.Fatalf("Evict: %v", err)
	}

	// Should evict down to 90 bytes (90% of 100)
	if cm.nvmeUsed > 90 {
		t.Errorf("After eviction nvmeUsed = %d, want <= 90", cm.nvmeUsed)
	}
}

func TestCacheManager_GetFallsToLowerTiers(t *testing.T) {
	cm := newTestCacheManager()
	ctx := context.Background()

	// Put data only in cloud tier
	cm.cloudStorage.Write(ctx, "/cloud-only.txt", []byte("cloud data"))

	got, err := cm.Get(ctx, "/cloud-only.txt")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}

	if string(got.Data) != "cloud data" {
		t.Errorf("Get data = %q, want %q", got.Data, "cloud data")
	}

	if got.Tier != TierCloud {
		t.Errorf("Tier = %d, want TierCloud (%d)", got.Tier, TierCloud)
	}
}

func TestCacheManager_CloudRetry(t *testing.T) {
	cm := newTestCacheManager()
	cm.config.CloudRetryCount = 3
	cm.config.CloudRetryBaseWait = time.Millisecond

	failingCloud := &failNTimesStorage{
		inner:     cm.cloudStorage.(*mockStorage),
		failCount: 2,
	}
	cm.cloudStorage = failingCloud

	ctx := context.Background()
	entry := &CacheEntry{
		FilePath:     "/retry-test.txt",
		Size:         5,
		LastAccessed: time.Now(),
		Data:         []byte("retry"),
	}

	if err := cm.Put(ctx, entry); err != nil {
		t.Fatalf("Put: %v", err)
	}

	// Wait for background persist to complete
	time.Sleep(100 * time.Millisecond)

	// Should have been called 3 times (2 failures + 1 success)
	if calls := failingCloud.calls.Load(); calls != 3 {
		t.Errorf("Cloud write called %d times, want 3", calls)
	}

	// Verify data was persisted to cloud
	data, err := failingCloud.inner.Read(ctx, "/retry-test.txt")
	if err != nil {
		t.Fatalf("Cloud read: %v", err)
	}
	if string(data) != "retry" {
		t.Errorf("Cloud data = %q, want %q", data, "retry")
	}
}

// failNTimesStorage wraps a storage and fails the first N Write calls
type failNTimesStorage struct {
	inner     *mockStorage
	failCount int32
	calls     atomic.Int32
}

func (f *failNTimesStorage) Read(ctx context.Context, path string) ([]byte, error) {
	return f.inner.Read(ctx, path)
}

func (f *failNTimesStorage) Write(ctx context.Context, path string, data []byte) error {
	n := f.calls.Add(1)
	if n <= f.failCount {
		return fmt.Errorf("simulated failure %d", n)
	}
	return f.inner.Write(ctx, path, data)
}

func (f *failNTimesStorage) Delete(ctx context.Context, path string) error {
	return f.inner.Delete(ctx, path)
}

func (f *failNTimesStorage) Exists(ctx context.Context, path string) bool {
	return f.inner.Exists(ctx, path)
}

func (f *failNTimesStorage) Size(ctx context.Context, path string) (int64, error) {
	return f.inner.Size(ctx, path)
}

type countingStorage struct {
	inner        *mockStorage
	readCalls    atomic.Int32
	readDelay    time.Duration
	mu           sync.Mutex
	perPathReads map[string]int
}

func newCountingStorage() *countingStorage {
	return &countingStorage{
		inner:        newMockStorage(),
		perPathReads: make(map[string]int),
	}
}

func newCountingStorageWithDelay(readDelay time.Duration) *countingStorage {
	s := newCountingStorage()
	s.readDelay = readDelay
	return s
}

func (c *countingStorage) Read(ctx context.Context, path string) ([]byte, error) {
	if c.readDelay > 0 {
		time.Sleep(c.readDelay)
	}
	c.readCalls.Add(1)
	c.mu.Lock()
	c.perPathReads[path]++
	c.mu.Unlock()
	return c.inner.Read(ctx, path)
}

func (c *countingStorage) Write(ctx context.Context, path string, data []byte) error {
	return c.inner.Write(ctx, path, data)
}

func (c *countingStorage) Delete(ctx context.Context, path string) error {
	return c.inner.Delete(ctx, path)
}

func (c *countingStorage) Exists(ctx context.Context, path string) bool {
	return c.inner.Exists(ctx, path)
}

func (c *countingStorage) Size(ctx context.Context, path string) (int64, error) {
	return c.inner.Size(ctx, path)
}

func (c *countingStorage) reads() int32 {
	return c.readCalls.Load()
}

func (c *countingStorage) readsFor(path string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.perPathReads[path]
}

func TestCacheManager_Get_UsesPeerFirstAt5GBBoundary(t *testing.T) {
	cm := newTestCacheManager()
	ctx := context.Background()

	const peerLimit = int64(5 * 1024 * 1024 * 1024)
	path := "/large-5gb.bin"

	peer := newCountingStorage()
	cloud := newCountingStorage()
	cm.peerStorage = peer
	cm.cloudStorage = cloud
	cm.config.MaxPeerSize = peerLimit

	if err := peer.Write(ctx, path, []byte("from-peer")); err != nil {
		t.Fatalf("peer write: %v", err)
	}
	if err := cloud.Write(ctx, path, []byte("from-cloud")); err != nil {
		t.Fatalf("cloud write: %v", err)
	}

	cm.mu.Lock()
	cm.entries[path] = &CacheEntry{FilePath: path, Size: peerLimit}
	cm.mu.Unlock()

	got, err := cm.Get(ctx, path)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if string(got.Data) != "from-peer" {
		t.Fatalf("Get data = %q, want peer path first", string(got.Data))
	}
	if peer.reads() != 1 {
		t.Fatalf("peer reads = %d, want 1", peer.reads())
	}
}

func TestCacheManager_Get_UsesPeerFirstBelowPeerLimit(t *testing.T) {
	cm := newTestCacheManager()
	ctx := context.Background()

	path := "/small.bin"
	peer := newCountingStorage()
	cloud := newCountingStorage()
	cm.peerStorage = peer
	cm.cloudStorage = cloud
	cm.config.MaxPeerSize = int64(5 * 1024 * 1024 * 1024)

	if err := peer.Write(ctx, path, []byte("from-peer")); err != nil {
		t.Fatalf("peer write: %v", err)
	}
	if err := cloud.Write(ctx, path, []byte("from-cloud")); err != nil {
		t.Fatalf("cloud write: %v", err)
	}

	cm.mu.Lock()
	cm.entries[path] = &CacheEntry{FilePath: path, Size: 64 * 1024 * 1024}
	cm.mu.Unlock()

	got, err := cm.Get(ctx, path)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if string(got.Data) != "from-peer" {
		t.Fatalf("Get data = %q, want peer path first", string(got.Data))
	}
	if peer.reads() != 1 {
		t.Fatalf("peer reads = %d, want 1", peer.reads())
	}
	if cloud.reads() != 0 {
		t.Fatalf("cloud reads = %d, want 0", cloud.reads())
	}
}

func TestCacheManager_Get_ChunkUsesParentSizeHint(t *testing.T) {
	cm := newTestCacheManager()
	ctx := context.Background()

	const peerLimit = int64(5 * 1024 * 1024 * 1024)
	parent := "/large-parent.bin"
	chunkPath := parent + "_chunk_0"

	peer := newCountingStorage()
	cloud := newCountingStorage()
	cm.peerStorage = peer
	cm.cloudStorage = cloud
	cm.config.MaxPeerSize = peerLimit

	if err := peer.Write(ctx, chunkPath, []byte("chunk-from-peer")); err != nil {
		t.Fatalf("peer write: %v", err)
	}
	if err := cloud.Write(ctx, chunkPath, []byte("chunk-from-cloud")); err != nil {
		t.Fatalf("cloud write: %v", err)
	}

	cm.mu.Lock()
	cm.entries[parent] = &CacheEntry{FilePath: parent, Size: peerLimit + 1}
	cm.mu.Unlock()

	got, err := cm.Get(ctx, chunkPath)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if string(got.Data) != "chunk-from-peer" {
		t.Fatalf("Get data = %q, want peer-first for large parent", string(got.Data))
	}
	if peer.reads() != 1 {
		t.Fatalf("peer reads = %d, want 1", peer.reads())
	}
}

func TestCacheManager_ReadRange_HybridSplitsPeerAndCloud(t *testing.T) {
	cm := newTestCacheManager()
	ctx := context.Background()

	peer := newCountingStorage()
	cloud := newCountingStorage()
	cm.peerStorage = peer
	cm.cloudStorage = cloud
	cm.config.ChunkSize = 4
	cm.config.RangePrefetchChunks = 0
	cm.config.MaxPeerSize = int64(10 * 1024 * 1024 * 1024) // keep peer-first base order
	cm.config.PeerReadThroughputBytesPerSec = 200 * 1024 * 1024
	cm.config.MetadataRefreshTTL = 5 * time.Second

	coord := coordinator.NewCoordinatorService()
	cm.config.Coordinator = coord

	path := "/hybrid.bin"
	chunk0 := path + "_chunk_0"
	chunk1 := path + "_chunk_1"

	if err := peer.Write(ctx, chunk0, []byte("ABCD")); err != nil {
		t.Fatalf("peer write chunk0: %v", err)
	}
	if err := cloud.Write(ctx, chunk0, []byte("abcd")); err != nil {
		t.Fatalf("cloud write chunk0: %v", err)
	}
	if err := cloud.Write(ctx, chunk1, []byte("ijkl")); err != nil {
		t.Fatalf("cloud write chunk1: %v", err)
	}

	// 500MB > 2 peers * 200MB/s => enable hybrid mode.
	fileSize := int64(500 * 1024 * 1024)
	for _, loc := range []*coordinator.FileLocation{
		{FilePath: path, PeerID: "peer-1", StorageTier: "peer", FileSize: fileSize},
		{FilePath: path, PeerID: "peer-2", StorageTier: "peer", FileSize: fileSize},
		{FilePath: path, PeerID: "cloud-1", StorageTier: "cloud", FileSize: fileSize},
	} {
		if err := coord.UpdateFileLocation(ctx, loc); err != nil {
			t.Fatalf("UpdateFileLocation: %v", err)
		}
	}

	cm.mu.Lock()
	cm.entries[path] = &CacheEntry{
		FilePath:  path,
		Size:      fileSize,
		IsChunked: true,
		NumChunks: 2,
	}
	cm.mu.Unlock()

	got, err := cm.ReadRange(ctx, path, 0, 8)
	if err != nil {
		t.Fatalf("ReadRange: %v", err)
	}
	if string(got) != "ABCDijkl" {
		t.Fatalf("ReadRange data = %q, want %q", string(got), "ABCDijkl")
	}
	// Chunk 0 should come from peer; chunk 1 should fall back to cloud.
	if peer.reads() < 1 {
		t.Fatalf("peer reads = %d, want >= 1", peer.reads())
	}
	if cloud.reads() < 1 {
		t.Fatalf("cloud reads = %d, want >= 1", cloud.reads())
	}
}

func TestCacheManager_ReadRange_DedupesConcurrentChunkFetches(t *testing.T) {
	cm := newTestCacheManager()
	ctx := context.Background()

	path := "/hot.bin"
	chunkPath := path + "_chunk_0"
	peer := newCountingStorageWithDelay(100 * time.Millisecond)
	cm.peerStorage = peer
	cm.cloudStorage = newCountingStorage()
	cm.config.ChunkSize = 16
	cm.config.RangePrefetchChunks = 0

	if err := peer.Write(ctx, chunkPath, []byte("0123456789ABCDEF")); err != nil {
		t.Fatalf("peer write chunk: %v", err)
	}

	cm.mu.Lock()
	cm.entries[path] = &CacheEntry{
		FilePath:  path,
		Size:      16,
		IsChunked: true,
		NumChunks: 1,
	}
	cm.mu.Unlock()

	start := make(chan struct{})
	errCh := make(chan error, 2)
	var wg sync.WaitGroup

	readFn := func(offset int64) {
		defer wg.Done()
		<-start
		got, err := cm.ReadRange(ctx, path, offset, 8)
		if err != nil {
			errCh <- err
			return
		}
		if len(got) != 8 {
			errCh <- fmt.Errorf("unexpected read size %d", len(got))
		}
	}

	wg.Add(2)
	go readFn(0)
	go readFn(8)
	close(start)
	wg.Wait()
	close(errCh)

	for err := range errCh {
		t.Fatalf("ReadRange: %v", err)
	}

	if reads := peer.readsFor(chunkPath); reads != 1 {
		t.Fatalf("peer reads for %s = %d, want 1", chunkPath, reads)
	}
}

func TestCacheManager_ChunkedReassembly(t *testing.T) {
	cm := newTestCacheManager()
	cm.config.ChunkSize = 10 // 10 bytes per chunk
	ctx := context.Background()

	// Create data larger than chunk size
	data := []byte("abcdefghijklmnopqrstuvwxyz") // 26 bytes = 3 chunks

	entry := &CacheEntry{
		FilePath:     "/chunked.txt",
		Size:         int64(len(data)),
		LastAccessed: time.Now(),
		Data:         data,
	}

	if err := cm.Put(ctx, entry); err != nil {
		t.Fatalf("Put chunked: %v", err)
	}

	// Verify the entry is marked as chunked
	cm.mu.RLock()
	e := cm.entries["/chunked.txt"]
	cm.mu.RUnlock()

	if !e.IsChunked {
		t.Error("Entry not marked as chunked")
	}
	if e.NumChunks != 3 {
		t.Errorf("NumChunks = %d, want 3", e.NumChunks)
	}

	// Read it back
	got, err := cm.Get(ctx, "/chunked.txt")
	if err != nil {
		t.Fatalf("Get chunked: %v", err)
	}

	if !bytes.Equal(got.Data, data) {
		t.Errorf("Get data = %q, want %q", got.Data, data)
	}
}

func TestCacheManager_PutFromReader_Chunked(t *testing.T) {
	cm := newTestCacheManager()
	cm.config.ChunkSize = 4
	ctx := context.Background()

	data := []byte("hello-world")
	if err := cm.PutFromReader(ctx, "/stream.bin", bytes.NewReader(data), int64(len(data)), time.Now()); err != nil {
		t.Fatalf("PutFromReader: %v", err)
	}

	cm.mu.RLock()
	meta := cm.entries["/stream.bin"]
	cm.mu.RUnlock()
	if meta == nil {
		t.Fatalf("missing metadata entry")
	}
	if !meta.IsChunked {
		t.Fatalf("metadata IsChunked = false, want true")
	}
	if meta.NumChunks != 3 {
		t.Fatalf("NumChunks = %d, want 3", meta.NumChunks)
	}
	if len(meta.Data) != 0 {
		t.Fatalf("metadata data retained, want empty")
	}

	got, err := cm.ReadRange(ctx, "/stream.bin", 0, len(data))
	if err != nil {
		t.Fatalf("ReadRange: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("ReadRange = %q, want %q", string(got), string(data))
	}
}

func TestCacheManager_Checksum(t *testing.T) {
	cm := newTestCacheManager()
	ctx := context.Background()

	data := []byte("checksum test data")
	entry := &CacheEntry{
		FilePath:     "/checksum.txt",
		Size:         int64(len(data)),
		LastAccessed: time.Now(),
		Data:         data,
	}

	if err := cm.Put(ctx, entry); err != nil {
		t.Fatalf("Put: %v", err)
	}

	// Verify checksum was computed
	if entry.Checksum == "" {
		t.Error("Checksum was not computed")
	}

	h := sha256.Sum256(data)
	expected := hex.EncodeToString(h[:])
	if entry.Checksum != expected {
		t.Errorf("Checksum = %s, want %s", entry.Checksum, expected)
	}

	// Verify sidecar was written
	sidecarData, err := cm.nvmeStorage.Read(ctx, "/checksum.txt.sha256")
	if err != nil {
		t.Fatalf("Read sidecar: %v", err)
	}
	if string(sidecarData) != expected {
		t.Errorf("Sidecar checksum = %s, want %s", string(sidecarData), expected)
	}

	// Read back and verify checksum validation passes
	got, err := cm.Get(ctx, "/checksum.txt")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if string(got.Data) != string(data) {
		t.Errorf("Get data = %q, want %q", got.Data, data)
	}
}

func TestCacheManager_ChecksumMismatch(t *testing.T) {
	cm := newTestCacheManager()
	ctx := context.Background()

	data := []byte("original data")
	entry := &CacheEntry{
		FilePath:     "/corrupt.txt",
		Size:         int64(len(data)),
		LastAccessed: time.Now(),
		Data:         data,
	}

	if err := cm.Put(ctx, entry); err != nil {
		t.Fatalf("Put: %v", err)
	}

	// Wait for background cloud persist
	time.Sleep(50 * time.Millisecond)

	// Remove from cloud and peer so only NVMe is available
	cm.cloudStorage.Delete(ctx, "/corrupt.txt")
	cm.peerStorage.Delete(ctx, "/corrupt.txt")

	// Corrupt the data on NVMe
	cm.nvmeStorage.Write(ctx, "/corrupt.txt", []byte("corrupted data"))

	// Get should fail due to checksum mismatch on NVMe and miss on other tiers
	_, err := cm.Get(ctx, "/corrupt.txt")
	if err == nil {
		t.Error("Expected error for corrupted data, got nil")
	}
}

func TestCacheMetrics(t *testing.T) {
	cm := newTestCacheManager()
	ctx := context.Background()

	// Put a file
	cm.Put(ctx, &CacheEntry{
		FilePath:     "/metric.txt",
		Size:         5,
		LastAccessed: time.Now(),
		Data:         []byte("hello"),
	})

	// Wait for background persist
	time.Sleep(50 * time.Millisecond)

	// Get it (NVMe hit)
	cm.Get(ctx, "/metric.txt")

	// Miss (not found)
	cm.Get(ctx, "/nonexistent.txt")

	snapshot := cm.GetMetrics()

	if snapshot["write_count"].(int64) != 1 {
		t.Errorf("write_count = %v, want 1", snapshot["write_count"])
	}
	if snapshot["write_bytes"].(int64) != 5 {
		t.Errorf("write_bytes = %v, want 5", snapshot["write_bytes"])
	}
	if snapshot["nvme_hits"].(int64) != 1 {
		t.Errorf("nvme_hits = %v, want 1", snapshot["nvme_hits"])
	}
	// nonexistent file should miss all 3 tiers
	if snapshot["nvme_misses"].(int64) < 1 {
		t.Errorf("nvme_misses = %v, want >= 1", snapshot["nvme_misses"])
	}
}

func TestCacheManager_Stats(t *testing.T) {
	cm := newTestCacheManager()
	ctx := context.Background()

	used, capacity := cm.Stats()
	if used != 0 {
		t.Errorf("Initial used = %d, want 0", used)
	}
	if capacity != 1024*1024 {
		t.Errorf("Capacity = %d, want %d", capacity, 1024*1024)
	}

	cm.Put(ctx, &CacheEntry{
		FilePath:     "/stats.txt",
		Size:         100,
		LastAccessed: time.Now(),
		Data:         make([]byte, 100),
	})

	used, _ = cm.Stats()
	if used != 100 {
		t.Errorf("After put used = %d, want 100", used)
	}
}

// newLargeFileCacheManager creates a cache manager sized for large file tests.
// NVMe capacity is set to max+1GB so writes land on NVMe without fallback.
func newLargeFileCacheManager(maxNVMe int64) *DefaultCacheManager {
	nvme := newMockStorage()
	peer := newMockStorage()
	cloud := newMockStorage()
	return &DefaultCacheManager{
		config: &CacheConfig{
			NVMePath:           "/tmp/test-large",
			MaxNVMeSize:        maxNVMe,
			ChunkSize:          4 * 1024 * 1024, // 4MB chunks
			CloudRetryCount:    1,
			CloudRetryBaseWait: time.Millisecond,
			MinPeerReplicas:    3,
			CloudTimeout:       60 * time.Second,
		},
		nvmeStorage:  nvme,
		peerStorage:  peer,
		cloudStorage: cloud,
		entries:      make(map[string]*CacheEntry),
		logger:       log.New(log.Writer(), "[CACHE-LARGE] ", log.LstdFlags),
		metrics:      NewCacheMetrics(),
		rangeChunks:  make(map[string]*chunkFileCache),
		hybridHints:  make(map[string]hybridReadHint),
	}
}

// randomData generates n bytes of random data
func randomData(t *testing.T, n int) []byte {
	t.Helper()
	data := make([]byte, n)
	if _, err := io.ReadFull(rand.Reader, data); err != nil {
		t.Fatalf("Failed to generate random data: %v", err)
	}
	return data
}

func TestLargeFile_10MB(t *testing.T) {
	const size = 10 * 1024 * 1024 // 10MB
	cm := newLargeFileCacheManager(size + 1024*1024*1024)
	ctx := context.Background()

	data := randomData(t, size)
	checksum := sha256.Sum256(data)

	entry := &CacheEntry{
		FilePath:     "/large-10mb.bin",
		Size:         int64(len(data)),
		LastAccessed: time.Now(),
		Data:         data,
	}

	// Put should chunk the file (10MB > 4MB chunk size)
	if err := cm.Put(ctx, entry); err != nil {
		t.Fatalf("Put 10MB: %v", err)
	}

	// Wait for background cloud persist
	time.Sleep(200 * time.Millisecond)

	// Verify entry is chunked
	cm.mu.RLock()
	e := cm.entries["/large-10mb.bin"]
	cm.mu.RUnlock()

	if !e.IsChunked {
		t.Error("10MB file should be chunked")
	}

	expectedChunks := int64(3) // ceil(10MB / 4MB) = 3
	if e.NumChunks != expectedChunks {
		t.Errorf("NumChunks = %d, want %d", e.NumChunks, expectedChunks)
	}

	// Read it back
	got, err := cm.Get(ctx, "/large-10mb.bin")
	if err != nil {
		t.Fatalf("Get 10MB: %v", err)
	}

	if int64(len(got.Data)) != size {
		t.Errorf("Got %d bytes, want %d", len(got.Data), size)
	}

	// Verify data integrity via checksum
	gotChecksum := sha256.Sum256(got.Data)
	if checksum != gotChecksum {
		t.Error("10MB file data corrupted: checksum mismatch")
	}

	// Verify each chunk was persisted to cloud
	for i := int64(0); i < expectedChunks; i++ {
		chunkPath := fmt.Sprintf("/large-10mb.bin_chunk_%d", i)
		if !cm.cloudStorage.Exists(ctx, chunkPath) {
			t.Errorf("Chunk %d not persisted to cloud", i)
		}
	}

	// Verify stats
	used, _ := cm.Stats()
	if used <= 0 {
		t.Error("Stats should report non-zero usage after 10MB write")
	}

	// Verify metrics
	snapshot := cm.GetMetrics()
	if snapshot["write_count"].(int64) < 3 {
		t.Errorf("write_count = %v, want >= 3 (one per chunk)", snapshot["write_count"])
	}

	t.Logf("10MB test passed: %d chunks, checksum OK", expectedChunks)
}

func TestLargeFile_100MB(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping 100MB test in short mode")
	}

	const size = 100 * 1024 * 1024 // 100MB
	cm := newLargeFileCacheManager(size + 1024*1024*1024)
	ctx := context.Background()

	data := randomData(t, size)
	checksum := sha256.Sum256(data)

	entry := &CacheEntry{
		FilePath:     "/large-100mb.bin",
		Size:         int64(len(data)),
		LastAccessed: time.Now(),
		Data:         data,
	}

	if err := cm.Put(ctx, entry); err != nil {
		t.Fatalf("Put 100MB: %v", err)
	}

	// Wait for background cloud persist (longer for 100MB)
	time.Sleep(2 * time.Second)

	// Verify chunking
	cm.mu.RLock()
	e := cm.entries["/large-100mb.bin"]
	cm.mu.RUnlock()

	if !e.IsChunked {
		t.Error("100MB file should be chunked")
	}

	expectedChunks := int64(25) // ceil(100MB / 4MB) = 25
	if e.NumChunks != expectedChunks {
		t.Errorf("NumChunks = %d, want %d", e.NumChunks, expectedChunks)
	}

	// Read it back and verify integrity
	got, err := cm.Get(ctx, "/large-100mb.bin")
	if err != nil {
		t.Fatalf("Get 100MB: %v", err)
	}

	if int64(len(got.Data)) != size {
		t.Errorf("Got %d bytes, want %d", len(got.Data), size)
	}

	gotChecksum := sha256.Sum256(got.Data)
	if checksum != gotChecksum {
		t.Error("100MB file data corrupted: checksum mismatch")
	}

	// Verify all chunks persisted to cloud
	for i := int64(0); i < expectedChunks; i++ {
		chunkPath := fmt.Sprintf("/large-100mb.bin_chunk_%d", i)
		if !cm.cloudStorage.Exists(ctx, chunkPath) {
			t.Errorf("Chunk %d not persisted to cloud", i)
		}
	}

	// Verify we can delete the chunked file and its individual chunks
	for i := int64(0); i < expectedChunks; i++ {
		chunkPath := fmt.Sprintf("/large-100mb.bin_chunk_%d", i)
		if err := cm.Delete(ctx, chunkPath); err != nil {
			t.Errorf("Delete chunk %d: %v", i, err)
		}
	}
	if err := cm.Delete(ctx, "/large-100mb.bin"); err != nil {
		t.Errorf("Delete parent entry: %v", err)
	}

	// Verify everything is cleaned up
	cm.mu.RLock()
	remaining := len(cm.entries)
	cm.mu.RUnlock()
	if remaining != 0 {
		t.Errorf("After delete, %d entries remain, want 0", remaining)
	}

	t.Logf("100MB test passed: %d chunks, checksum OK", expectedChunks)
}

func TestLargeFile_1GB(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping 1GB test in short mode")
	}

	const size = 1024 * 1024 * 1024 // 1GB
	cm := newLargeFileCacheManager(2 * size)
	ctx := context.Background()

	// Use a deterministic pattern instead of crypto/rand for speed on 1GB
	// Fill with a repeating 1MB block of random data
	block := make([]byte, 1024*1024)
	if _, err := io.ReadFull(rand.Reader, block); err != nil {
		t.Fatalf("Failed to generate random block: %v", err)
	}

	data := make([]byte, size)
	for off := 0; off < size; off += len(block) {
		copy(data[off:], block)
	}
	checksum := sha256.Sum256(data)

	entry := &CacheEntry{
		FilePath:     "/large-1gb.bin",
		Size:         int64(len(data)),
		LastAccessed: time.Now(),
		Data:         data,
	}

	start := time.Now()
	if err := cm.Put(ctx, entry); err != nil {
		t.Fatalf("Put 1GB: %v", err)
	}
	putDuration := time.Since(start)

	// Wait for background cloud persist
	time.Sleep(5 * time.Second)

	// Verify chunking
	cm.mu.RLock()
	e := cm.entries["/large-1gb.bin"]
	cm.mu.RUnlock()

	if !e.IsChunked {
		t.Error("1GB file should be chunked")
	}

	expectedChunks := int64(256) // 1GB / 4MB = 256
	if e.NumChunks != expectedChunks {
		t.Errorf("NumChunks = %d, want %d", e.NumChunks, expectedChunks)
	}

	// Read it back
	start = time.Now()
	got, err := cm.Get(ctx, "/large-1gb.bin")
	if err != nil {
		t.Fatalf("Get 1GB: %v", err)
	}
	getDuration := time.Since(start)

	if int64(len(got.Data)) != size {
		t.Errorf("Got %d bytes, want %d", len(got.Data), size)
	}

	// Verify data integrity
	gotChecksum := sha256.Sum256(got.Data)
	if checksum != gotChecksum {
		t.Error("1GB file data corrupted: checksum mismatch")
	}

	// Spot-check: verify a few individual chunks exist in cloud
	for _, idx := range []int64{0, 127, 255} {
		chunkPath := fmt.Sprintf("/large-1gb.bin_chunk_%d", idx)
		if !cm.cloudStorage.Exists(ctx, chunkPath) {
			t.Errorf("Chunk %d not persisted to cloud", idx)
		}
	}

	// Verify stats
	used, capacity := cm.Stats()
	if used <= 0 {
		t.Error("Stats should report non-zero NVMe usage")
	}
	if capacity != 2*size {
		t.Errorf("Capacity = %d, want %d", capacity, 2*size)
	}

	// Verify metrics
	snapshot := cm.GetMetrics()
	if snapshot["write_count"].(int64) < 256 {
		t.Errorf("write_count = %v, want >= 256", snapshot["write_count"])
	}

	t.Logf("1GB test passed: %d chunks, put=%v get=%v, checksum OK",
		expectedChunks, putDuration, getDuration)
}

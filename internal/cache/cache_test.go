package cache

import (
	"context"
	"fmt"
	"log"
	"testing"
	"time"
)

// mockStorage is a simple in-memory TierStorage for testing
type mockStorage struct {
	data map[string][]byte
}

func newMockStorage() *mockStorage {
	return &mockStorage{data: make(map[string][]byte)}
}

func (m *mockStorage) Read(ctx context.Context, path string) ([]byte, error) {
	if d, ok := m.data[path]; ok {
		out := make([]byte, len(d))
		copy(out, d)
		return out, nil
	}
	return nil, context.Canceled
}

func (m *mockStorage) Write(ctx context.Context, path string, data []byte) error {
	m.data[path] = append([]byte(nil), data...)
	return nil
}

func (m *mockStorage) Delete(ctx context.Context, path string) error {
	delete(m.data, path)
	return nil
}

func (m *mockStorage) Exists(ctx context.Context, path string) bool {
	_, ok := m.data[path]
	return ok
}

func (m *mockStorage) Size(ctx context.Context, path string) (int64, error) {
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
			NVMePath:    "/tmp/test",
			MaxNVMeSize: 1024 * 1024, // 1MB
			ChunkSize:   4 * 1024 * 1024,
		},
		nvmeStorage:  nvme,
		peerStorage:  peer,
		cloudStorage: cloud,
		entries:      make(map[string]*CacheEntry),
		logger:       log.New(log.Writer(), "[CACHE-TEST] ", log.LstdFlags),
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

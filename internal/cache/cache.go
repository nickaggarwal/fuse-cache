package cache

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sort"
	"sync"
	"time"
)

// CacheTier represents different cache tiers
type CacheTier int

const (
	TierNVMe CacheTier = iota
	TierPeer
	TierCloud
)

// CacheEntry represents a cached file entry
type CacheEntry struct {
	FilePath     string
	StoragePath  string
	Size         int64
	LastAccessed time.Time
	Tier         CacheTier
	Data         []byte
}

// CacheManager manages the 3-tier cache system
type CacheManager interface {
	Get(ctx context.Context, filePath string) (*CacheEntry, error)
	Put(ctx context.Context, entry *CacheEntry) error
	Delete(ctx context.Context, filePath string) error
	List(ctx context.Context) ([]*CacheEntry, error)
	Evict(ctx context.Context, tier CacheTier) error
}

// TierStorage represents storage interface for each tier
type TierStorage interface {
	Read(ctx context.Context, path string) ([]byte, error)
	Write(ctx context.Context, path string, data []byte) error
	Delete(ctx context.Context, path string) error
	Exists(ctx context.Context, path string) bool
	Size(ctx context.Context, path string) (int64, error)
}

// CacheConfig holds configuration for cache tiers
type CacheConfig struct {
	NVMePath        string
	MaxNVMeSize     int64
	MaxPeerSize     int64
	PeerTimeout     time.Duration
	CloudTimeout    time.Duration
	CoordinatorAddr string
	ChunkSize       int64

	// Cloud provider: "s3" or "azure"
	CloudProvider string

	// S3 config
	S3Bucket string
	S3Region string

	// Azure config
	AzureStorageAccount   string
	AzureStorageKey       string
	AzureContainerName    string
}

// DefaultCacheManager implements CacheManager
type DefaultCacheManager struct {
	config       *CacheConfig
	nvmeStorage  TierStorage
	peerStorage  TierStorage
	cloudStorage TierStorage
	entries      map[string]*CacheEntry
	nvmeUsed     int64
	mu           sync.RWMutex
	logger       *log.Logger
}

// NewCacheManager creates a new cache manager
func NewCacheManager(config *CacheConfig) (*DefaultCacheManager, error) {
	if config.NVMePath == "" {
		return nil, errors.New("NVME path is required")
	}

	// Set default chunk size if not provided
	if config.ChunkSize <= 0 {
		config.ChunkSize = 4 * 1024 * 1024 // 4MB default
	}

	cm := &DefaultCacheManager{
		config:  config,
		entries: make(map[string]*CacheEntry),
		logger:  log.New(log.Writer(), "[CACHE] ", log.LstdFlags),
	}

	// Initialize storage tiers
	var err error
	cm.nvmeStorage, err = NewNVMeStorage(config.NVMePath)
	if err != nil {
		return nil, err
	}

	cm.peerStorage, err = NewPeerStorage(config.CoordinatorAddr, config.PeerTimeout)
	if err != nil {
		return nil, err
	}

	// Initialize cloud storage based on provider
	switch config.CloudProvider {
	case "azure":
		cm.cloudStorage, err = NewAzureStorage(
			config.AzureStorageAccount,
			config.AzureStorageKey,
			config.AzureContainerName,
			config.CloudTimeout,
		)
	default: // "s3" or empty
		cm.cloudStorage, err = NewCloudStorage(config.S3Bucket, config.S3Region, config.CloudTimeout)
	}
	if err != nil {
		return nil, err
	}

	return cm, nil
}

// Get retrieves a file from the cache hierarchy
func (cm *DefaultCacheManager) Get(ctx context.Context, filePath string) (*CacheEntry, error) {
	// Check Tier 1: NVME
	if entry, err := cm.getFromTier(ctx, filePath, TierNVMe); err == nil {
		return entry, nil
	}

	// Check Tier 2: Peers
	if entry, err := cm.getFromTier(ctx, filePath, TierPeer); err == nil {
		go cm.promoteToNVMe(context.Background(), entry)
		return entry, nil
	}

	// Check Tier 3: Cloud
	if entry, err := cm.getFromTier(ctx, filePath, TierCloud); err == nil {
		go cm.promoteToNVMe(context.Background(), entry)
		return entry, nil
	}

	return nil, errors.New("file not found in any tier")
}

// Put stores a file in the cache
func (cm *DefaultCacheManager) Put(ctx context.Context, entry *CacheEntry) error {
	// Try to store in NVME first (with eviction if needed)
	if err := cm.putToNVMeWithEviction(ctx, entry); err == nil {
		cm.mu.Lock()
		cm.entries[entry.FilePath] = entry
		cm.mu.Unlock()

		// Background write-through to cloud for durability
		go cm.persistToCloud(entry)

		return nil
	}

	// Check if file needs chunking
	if int64(len(entry.Data)) > cm.config.ChunkSize {
		return cm.putChunked(ctx, entry)
	}

	// Parallel Write Strategy: Race Peer and Cloud
	type writeResult struct {
		tier CacheTier
		err  error
	}

	resultChan := make(chan writeResult, 2)
	writeCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	go func() {
		entryCopy := *entry
		entryCopy.Tier = TierPeer
		err := cm.putToTier(writeCtx, &entryCopy, TierPeer)
		resultChan <- writeResult{TierPeer, err}
	}()

	go func() {
		entryCopy := *entry
		entryCopy.Tier = TierCloud
		err := cm.putToTier(writeCtx, &entryCopy, TierCloud)
		resultChan <- writeResult{TierCloud, err}
	}()

	// Wait for first success or all failures
	for i := 0; i < 2; i++ {
		res := <-resultChan
		if res.err == nil {
			cm.mu.Lock()
			entry.Tier = res.tier
			cm.entries[entry.FilePath] = entry
			cm.mu.Unlock()
			return nil
		}
	}

	return errors.New("failed to store file in Peer or Cloud tier")
}

// Delete removes a file from cache
func (cm *DefaultCacheManager) Delete(ctx context.Context, filePath string) error {
	cm.mu.RLock()
	entry, exists := cm.entries[filePath]
	cm.mu.RUnlock()
	if !exists {
		return errors.New("file not found in cache")
	}

	storage := cm.getStorageForTier(entry.Tier)
	if err := storage.Delete(ctx, entry.StoragePath); err != nil {
		return err
	}

	cm.mu.Lock()
	if entry.Tier == TierNVMe {
		cm.nvmeUsed -= entry.Size
	}
	delete(cm.entries, filePath)
	cm.mu.Unlock()
	return nil
}

// List returns all cached entries
func (cm *DefaultCacheManager) List(ctx context.Context) ([]*CacheEntry, error) {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	entries := make([]*CacheEntry, 0, len(cm.entries))
	for _, entry := range cm.entries {
		entries = append(entries, entry)
	}
	return entries, nil
}

// Evict removes the least recently accessed entries from a tier to free the given number of bytes.
// If bytesNeeded is 0, it evicts entries until usage is below 90% of MaxNVMeSize.
func (cm *DefaultCacheManager) Evict(ctx context.Context, tier CacheTier) error {
	if tier != TierNVMe {
		return nil
	}

	cm.mu.Lock()
	defer cm.mu.Unlock()

	target := cm.config.MaxNVMeSize * 9 / 10 // evict down to 90%
	if cm.nvmeUsed <= target {
		return nil
	}

	// Collect NVMe entries and sort by LastAccessed (oldest first)
	type entryRef struct {
		key   string
		entry *CacheEntry
	}
	var nvmeEntries []entryRef
	for k, e := range cm.entries {
		if e.Tier == TierNVMe {
			nvmeEntries = append(nvmeEntries, entryRef{k, e})
		}
	}
	sort.Slice(nvmeEntries, func(i, j int) bool {
		return nvmeEntries[i].entry.LastAccessed.Before(nvmeEntries[j].entry.LastAccessed)
	})

	for _, ref := range nvmeEntries {
		if cm.nvmeUsed <= target {
			break
		}
		storage := cm.getStorageForTier(TierNVMe)
		if err := storage.Delete(ctx, ref.entry.FilePath); err != nil {
			cm.logger.Printf("Eviction delete failed for %s: %v", ref.key, err)
			continue
		}
		cm.nvmeUsed -= ref.entry.Size
		delete(cm.entries, ref.key)
		cm.logger.Printf("Evicted %s (%d bytes)", ref.key, ref.entry.Size)
	}

	return nil
}

// persistToCloud asynchronously writes a copy of the entry to cloud storage for durability.
// This runs in the background after a successful NVMe write.
func (cm *DefaultCacheManager) persistToCloud(entry *CacheEntry) {
	ctx, cancel := context.WithTimeout(context.Background(), cm.config.CloudTimeout)
	defer cancel()

	// Make a copy of data to avoid races with the caller
	dataCopy := make([]byte, len(entry.Data))
	copy(dataCopy, entry.Data)

	if err := cm.cloudStorage.Write(ctx, entry.FilePath, dataCopy); err != nil {
		cm.logger.Printf("Background cloud persist failed for %s: %v", entry.FilePath, err)
		return
	}
	cm.logger.Printf("Persisted %s (%d bytes) to cloud storage", entry.FilePath, len(dataCopy))
}

// putToNVMeWithEviction tries NVMe write, evicting if necessary
func (cm *DefaultCacheManager) putToNVMeWithEviction(ctx context.Context, entry *CacheEntry) error {
	cm.mu.RLock()
	wouldExceed := cm.nvmeUsed+entry.Size > cm.config.MaxNVMeSize
	cm.mu.RUnlock()

	if wouldExceed {
		cm.Evict(ctx, TierNVMe)
		// Re-check after eviction
		cm.mu.RLock()
		stillExceed := cm.nvmeUsed+entry.Size > cm.config.MaxNVMeSize
		cm.mu.RUnlock()
		if stillExceed {
			return errors.New("NVMe storage full after eviction")
		}
	}

	if err := cm.putToTier(ctx, entry, TierNVMe); err != nil {
		return err
	}

	cm.mu.Lock()
	cm.nvmeUsed += entry.Size
	cm.mu.Unlock()
	return nil
}

// Helper methods
func (cm *DefaultCacheManager) getFromTier(ctx context.Context, filePath string, tier CacheTier) (*CacheEntry, error) {
	storage := cm.getStorageForTier(tier)

	if !storage.Exists(ctx, filePath) {
		return nil, errors.New("file not found in tier")
	}

	data, err := storage.Read(ctx, filePath)
	if err != nil {
		return nil, err
	}

	size, err := storage.Size(ctx, filePath)
	if err != nil {
		return nil, err
	}

	return &CacheEntry{
		FilePath:     filePath,
		StoragePath:  filePath,
		Size:         size,
		LastAccessed: time.Now(),
		Tier:         tier,
		Data:         data,
	}, nil
}

func (cm *DefaultCacheManager) putToTier(ctx context.Context, entry *CacheEntry, tier CacheTier) error {
	storage := cm.getStorageForTier(tier)
	return storage.Write(ctx, entry.FilePath, entry.Data)
}

func (cm *DefaultCacheManager) getStorageForTier(tier CacheTier) TierStorage {
	switch tier {
	case TierNVMe:
		return cm.nvmeStorage
	case TierPeer:
		return cm.peerStorage
	case TierCloud:
		return cm.cloudStorage
	default:
		return cm.nvmeStorage
	}
}

func (cm *DefaultCacheManager) promoteToNVMe(ctx context.Context, entry *CacheEntry) {
	newEntry := &CacheEntry{
		FilePath:     entry.FilePath,
		StoragePath:  entry.StoragePath,
		Size:         entry.Size,
		LastAccessed: time.Now(),
		Tier:         TierNVMe,
		Data:         entry.Data,
	}

	if err := cm.putToNVMeWithEviction(ctx, newEntry); err != nil {
		cm.logger.Printf("Promotion to NVMe failed for %s: %v", entry.FilePath, err)
		return
	}

	cm.mu.Lock()
	cm.entries[entry.FilePath] = newEntry
	cm.mu.Unlock()
}

func (cm *DefaultCacheManager) putChunked(ctx context.Context, entry *CacheEntry) error {
	dataLen := int64(len(entry.Data))
	numChunks := (dataLen + cm.config.ChunkSize - 1) / cm.config.ChunkSize

	var wg sync.WaitGroup
	errChan := make(chan error, numChunks)

	for i := int64(0); i < numChunks; i++ {
		start := i * cm.config.ChunkSize
		end := start + cm.config.ChunkSize
		if end > dataLen {
			end = dataLen
		}

		chunkData := entry.Data[start:end]
		chunkPath := fmt.Sprintf("%s_chunk_%d", entry.FilePath, i)

		wg.Add(1)
		go func(path string, data []byte) {
			defer wg.Done()

			chunkEntry := &CacheEntry{
				FilePath:     path,
				StoragePath:  path,
				Size:         int64(len(data)),
				LastAccessed: time.Now(),
				Data:         data,
			}

			if err := cm.Put(ctx, chunkEntry); err != nil {
				errChan <- err
			}
		}(chunkPath, chunkData)
	}

	wg.Wait()
	close(errChan)

	for err := range errChan {
		if err != nil {
			return fmt.Errorf("failed to upload chunk: %v", err)
		}
	}

	cm.mu.Lock()
	cm.entries[entry.FilePath] = entry
	cm.mu.Unlock()

	return nil
}

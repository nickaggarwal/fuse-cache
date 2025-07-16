package cache

import (
	"context"
	"errors"
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
}

// DefaultCacheManager implements CacheManager
type DefaultCacheManager struct {
	config       *CacheConfig
	nvmeStorage  TierStorage
	peerStorage  TierStorage
	cloudStorage TierStorage
	entries      map[string]*CacheEntry
}

// NewCacheManager creates a new cache manager
func NewCacheManager(config *CacheConfig) (*DefaultCacheManager, error) {
	if config.NVMePath == "" {
		return nil, errors.New("NVME path is required")
	}

	cm := &DefaultCacheManager{
		config:  config,
		entries: make(map[string]*CacheEntry),
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

	cm.cloudStorage, err = NewCloudStorage(config.CloudTimeout)
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
		// Promote to NVME if there's space
		go cm.promoteToNVMe(ctx, entry)
		return entry, nil
	}

	// Check Tier 3: Cloud
	if entry, err := cm.getFromTier(ctx, filePath, TierCloud); err == nil {
		// Promote to NVME if there's space
		go cm.promoteToNVMe(ctx, entry)
		return entry, nil
	}

	return nil, errors.New("file not found in any tier")
}

// Put stores a file in the cache
func (cm *DefaultCacheManager) Put(ctx context.Context, entry *CacheEntry) error {
	// Try to store in NVME first
	if err := cm.putToTier(ctx, entry, TierNVMe); err == nil {
		cm.entries[entry.FilePath] = entry
		return nil
	}

	// Fall back to peer storage
	entry.Tier = TierPeer
	if err := cm.putToTier(ctx, entry, TierPeer); err == nil {
		cm.entries[entry.FilePath] = entry
		return nil
	}

	// Fall back to cloud storage
	entry.Tier = TierCloud
	if err := cm.putToTier(ctx, entry, TierCloud); err == nil {
		cm.entries[entry.FilePath] = entry
		return nil
	}

	return errors.New("failed to store file in any tier")
}

// Delete removes a file from cache
func (cm *DefaultCacheManager) Delete(ctx context.Context, filePath string) error {
	entry, exists := cm.entries[filePath]
	if !exists {
		return errors.New("file not found in cache")
	}

	storage := cm.getStorageForTier(entry.Tier)
	if err := storage.Delete(ctx, entry.StoragePath); err != nil {
		return err
	}

	delete(cm.entries, filePath)
	return nil
}

// List returns all cached entries
func (cm *DefaultCacheManager) List(ctx context.Context) ([]*CacheEntry, error) {
	entries := make([]*CacheEntry, 0, len(cm.entries))
	for _, entry := range cm.entries {
		entries = append(entries, entry)
	}
	return entries, nil
}

// Evict removes entries from a specific tier
func (cm *DefaultCacheManager) Evict(ctx context.Context, tier CacheTier) error {
	// Implementation for LRU eviction
	// This would be more complex in a real implementation
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
	// Promote file to NVME storage
	newEntry := &CacheEntry{
		FilePath:     entry.FilePath,
		StoragePath:  entry.StoragePath,
		Size:         entry.Size,
		LastAccessed: time.Now(),
		Tier:         TierNVMe,
		Data:         entry.Data,
	}

	if err := cm.putToTier(ctx, newEntry, TierNVMe); err == nil {
		cm.entries[entry.FilePath] = newEntry
	}
}

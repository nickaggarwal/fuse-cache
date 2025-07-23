package cache

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// EnhancedCacheManager extends DefaultCacheManager with directory tree integration
type EnhancedCacheManager struct {
	*DefaultCacheManager
	
	// Enhanced components
	directoryTree    *DirectoryTree
	consistencyMgr   *ConsistencyManager
	
	// Configuration
	config          *EnhancedCacheConfig
	
	// Metrics
	cacheHits       int64
	cacheMisses     int64
	instantLookups  int64
}

// EnhancedCacheConfig holds configuration for the enhanced cache manager
type EnhancedCacheConfig struct {
	*CacheConfig
	
	// Directory tree settings
	UseDirectoryTree      bool
	SmallFileCacheThreshold int64
	MetadataCacheTTL     time.Duration
	DataCacheTTL         time.Duration
	
	// Consistency settings
	ConsistencyLevel     ConsistencyLevel
	ValidationInterval   time.Duration
	StaleThreshold      time.Duration
}

// ConsistencyLevel defines consistency requirements
type ConsistencyLevel int

const (
	EventualConsistency ConsistencyLevel = iota
	SessionConsistency
	StrongConsistency
)

// NewEnhancedCacheManager creates a new enhanced cache manager
func NewEnhancedCacheManager(config *EnhancedCacheConfig) (*EnhancedCacheManager, error) {
	if config == nil || config.CacheConfig == nil {
		return nil, errors.New("cache config is required")
	}

	// Create base cache manager
	baseCM, err := NewCacheManager(config.CacheConfig)
	if err != nil {
		return nil, err
	}

	// Create enhanced cache manager
	ecm := &EnhancedCacheManager{
		DefaultCacheManager: baseCM,
		config:             config,
	}

	// Initialize directory tree if enabled
	if config.UseDirectoryTree {
		treeConfig := &DirectoryTreeConfig{
			SmallFileCacheThreshold: config.SmallFileCacheThreshold,
			MetadataTTL:            config.MetadataCacheTTL,
			DataCacheTTL:           config.DataCacheTTL,
			ValidationInterval:     config.ValidationInterval,
			MaxCachedFiles:         1000,
		}
		ecm.directoryTree = NewDirectoryTree(treeConfig)

		// Create consistency manager
		consistencyConfig := &ConsistencyConfig{
			ValidationInterval:   config.ValidationInterval,
			StaleThreshold:      config.StaleThreshold,
			MaxConcurrentChecks: 5,
			PeerNotificationTTL: 5 * time.Second,
		}
		ecm.consistencyMgr = NewConsistencyManager(ecm.directoryTree, ecm, nil, consistencyConfig)
		ecm.consistencyMgr.Start()

		// Integrate with existing snapshot manager
		ecm.integrateWithSnapshot()
	}

	return ecm, nil
}

// integrateWithSnapshot integrates the directory tree with the existing snapshot manager
func (ecm *EnhancedCacheManager) integrateWithSnapshot() {
	// We'll populate the directory tree from existing snapshot data
	// The integration happens through the Put/Delete method overrides
	go ecm.populateDirectoryTreeFromSnapshot()
}

// populateDirectoryTreeFromSnapshot populates the directory tree with existing snapshot data
func (ecm *EnhancedCacheManager) populateDirectoryTreeFromSnapshot() {
	if ecm.directoryTree == nil {
		return
	}

	// Get current local entries using the inherited List method
	ctx := context.Background()
	localEntries, err := ecm.DefaultCacheManager.List(ctx)
	if err != nil {
		fmt.Printf("[ENHANCED_CACHE] Failed to get local entries: %v\n", err)
		return
	}
	
	fmt.Printf("[ENHANCED_CACHE] Populating directory tree with %d entries from snapshot\n", len(localEntries))
	
	for _, entry := range localEntries {
		ecm.directoryTree.AddOrUpdateFile(entry)
	}
	
	fmt.Printf("[ENHANCED_CACHE] Directory tree populated with %d entries\n", len(localEntries))
}

// Get retrieves a file using enhanced caching with directory tree optimization
func (ecm *EnhancedCacheManager) Get(ctx context.Context, filePath string) (*CacheEntry, error) {
	// Try directory tree first if enabled
	if ecm.config.UseDirectoryTree && ecm.directoryTree != nil {
		if node := ecm.directoryTree.GetFileNode(filePath); node != nil {
			// Check if we can serve from cached data
			if entry := ecm.tryServeFromNode(node); entry != nil {
				ecm.cacheHits++
				ecm.instantLookups++
				return entry, nil
			}
			
			// Check if we need consistency validation
			if ecm.shouldValidateNode(node) {
				if entry, err := ecm.validateAndRefreshNode(ctx, node); err == nil {
					ecm.cacheHits++
					return entry, nil
				}
			}
		}
	}

	// Fall back to default cache manager
	ecm.cacheMisses++
	entry, err := ecm.DefaultCacheManager.Get(ctx, filePath)
	
	// Update directory tree with fresh data
	if err == nil && ecm.directoryTree != nil {
		ecm.directoryTree.AddOrUpdateFile(entry)
	}
	
	return entry, err
}

// tryServeFromNode attempts to serve a request from a directory tree node
func (ecm *EnhancedCacheManager) tryServeFromNode(node *FileNode) *CacheEntry {
	node.Mu.RLock()
	defer node.Mu.RUnlock()

	// Check if we have valid cached data
	if node.DataCache != nil && node.DataCache.IsValid() {
		if data, valid := node.DataCache.GetCachedData(); valid {
			return &CacheEntry{
				FilePath:     node.Path,
				StoragePath:  node.Path,
				Size:         node.Size,
				LastAccessed: node.ModTime,
				Tier:         node.LocalTier,
				Data:         data,
			}
		}
	}

	// Check if metadata is still valid (no cached data needed)
	if ecm.isMetadataValid(node) {
		return &CacheEntry{
			FilePath:     node.Path,
			StoragePath:  node.Path,
			Size:         node.Size,
			LastAccessed: node.ModTime,
			Tier:         node.LocalTier,
			Data:         nil, // Will be loaded by storage layer
		}
	}

	return nil
}

// shouldValidateNode determines if a node needs validation
func (ecm *EnhancedCacheManager) shouldValidateNode(node *FileNode) bool {
	switch ecm.config.ConsistencyLevel {
	case StrongConsistency:
		return true
	case SessionConsistency:
		return ecm.consistencyMgr != nil && ecm.consistencyMgr.ShouldValidateFile(node)
	case EventualConsistency:
		node.Mu.RLock()
		stale := node.IsStale
		node.Mu.RUnlock()
		return stale
	default:
		return false
	}
}

// isMetadataValid checks if node metadata is still valid
func (ecm *EnhancedCacheManager) isMetadataValid(node *FileNode) bool {
	if node.IsStale {
		return false
	}
	
	switch ecm.config.ConsistencyLevel {
	case StrongConsistency:
		return false // Always need fresh data
	case SessionConsistency:
		return time.Since(node.LastValidated) < ecm.config.MetadataCacheTTL
	case EventualConsistency:
		return time.Since(node.LastValidated) < 2*ecm.config.MetadataCacheTTL
	default:
		return true
	}
}

// validateAndRefreshNode validates a node and refreshes if needed
func (ecm *EnhancedCacheManager) validateAndRefreshNode(ctx context.Context, node *FileNode) (*CacheEntry, error) {
	// Try to get fresh data
	entry, err := ecm.DefaultCacheManager.Get(ctx, node.Path)
	if err != nil {
		return nil, err
	}

	node.Mu.Lock()
	defer node.Mu.Unlock()

	// Check if file has changed
	if entry.Size != node.Size || !entry.LastAccessed.Equal(node.ModTime) {
		// File has changed, update node
		node.Size = entry.Size
		node.ModTime = entry.LastAccessed
		node.LocalTier = entry.Tier
		node.IsStale = false
		
		// Update or create cached data for small files
		if entry.Size <= ecm.config.SmallFileCacheThreshold && entry.Data != nil {
			if node.DataCache == nil {
				node.DataCache = &TimedCache{}
			}
			
			node.DataCache.Mu.Lock()
			node.DataCache.Data = append([]byte(nil), entry.Data...) // Deep copy
			node.DataCache.CachedAt = time.Now()
			node.DataCache.TTL = ecm.config.DataCacheTTL
			node.DataCache.Valid = true
			node.DataCache.Size = entry.Size
			node.DataCache.Mu.Unlock()
		}
	}

	node.LastValidated = time.Now()
	return entry, nil
}

// Put stores a file with directory tree integration
func (ecm *EnhancedCacheManager) Put(ctx context.Context, entry *CacheEntry) error {
	// Store using default cache manager
	if err := ecm.DefaultCacheManager.Put(ctx, entry); err != nil {
		return err
	}

	// Update directory tree
	if ecm.directoryTree != nil {
		ecm.directoryTree.AddOrUpdateFile(entry)
	}

	return nil
}

// PutWithoutReplication stores a file without replication and updates directory tree
func (ecm *EnhancedCacheManager) PutWithoutReplication(ctx context.Context, entry *CacheEntry) error {
	// Store using default cache manager
	if err := ecm.DefaultCacheManager.PutWithoutReplication(ctx, entry); err != nil {
		return err
	}

	// Update directory tree
	if ecm.directoryTree != nil {
		ecm.directoryTree.AddOrUpdateFile(entry)
	}

	return nil
}

// Delete removes a file from cache and directory tree
func (ecm *EnhancedCacheManager) Delete(ctx context.Context, filePath string) error {
	// Delete using default cache manager
	if err := ecm.DefaultCacheManager.Delete(ctx, filePath); err != nil {
		return err
	}

	// Remove from directory tree
	if ecm.directoryTree != nil {
		ecm.directoryTree.RemoveFile(filePath)
	}

	return nil
}

// List returns cached entries using directory tree if available
func (ecm *EnhancedCacheManager) List(ctx context.Context) ([]*CacheEntry, error) {
	if ecm.config.UseDirectoryTree && ecm.directoryTree != nil {
		// Use directory tree for instant listing
		entries := []*CacheEntry{}
		
		ecm.directoryTree.Mu.RLock()
		for _, node := range ecm.directoryTree.fileMap {
			node.Mu.RLock()
			entry := &CacheEntry{
				FilePath:     node.Path,
				StoragePath:  node.Path,
				Size:         node.Size,
				LastAccessed: node.ModTime,
				Tier:         node.LocalTier,
				Data:         nil, // Don't include data in listings
			}
			entries = append(entries, entry)
			node.Mu.RUnlock()
		}
		ecm.directoryTree.Mu.RUnlock()
		
		return entries, nil
	}

	// Fall back to default implementation
	return ecm.DefaultCacheManager.List(ctx)
}

// Shutdown gracefully shuts down the enhanced cache manager
func (ecm *EnhancedCacheManager) Shutdown() {
	// Stop consistency manager
	if ecm.consistencyMgr != nil {
		ecm.consistencyMgr.Stop()
	}

	// Shutdown base cache manager
	ecm.DefaultCacheManager.Shutdown()
}

// GetDirectoryTree returns the directory tree for FUSE integration
func (ecm *EnhancedCacheManager) GetDirectoryTree() *DirectoryTree {
	return ecm.directoryTree
}

// GetConsistencyManager returns the consistency manager
func (ecm *EnhancedCacheManager) GetConsistencyManager() *ConsistencyManager {
	return ecm.consistencyMgr
}

// GetStats returns enhanced cache manager statistics
func (ecm *EnhancedCacheManager) GetStats() map[string]interface{} {
	baseStats := ecm.DefaultCacheManager.GetSnapshotStatus()
	
	enhancedStats := map[string]interface{}{
		"cache_hits":         ecm.cacheHits,
		"cache_misses":       ecm.cacheMisses,
		"instant_lookups":    ecm.instantLookups,
		"hit_ratio":          float64(ecm.cacheHits) / float64(ecm.cacheHits + ecm.cacheMisses),
		"directory_tree":     ecm.config.UseDirectoryTree,
		"consistency_level":  ecm.config.ConsistencyLevel,
		"base_stats":         baseStats,
	}

	if ecm.consistencyMgr != nil {
		enhancedStats["consistency_stats"] = ecm.consistencyMgr.GetStats()
	}

	return enhancedStats
}

// DefaultEnhancedCacheConfig returns a default enhanced cache configuration
func DefaultEnhancedCacheConfig(baseConfig *CacheConfig) *EnhancedCacheConfig {
	return &EnhancedCacheConfig{
		CacheConfig:             baseConfig,
		UseDirectoryTree:        true,
		SmallFileCacheThreshold: 64 * 1024,        // 64KB
		MetadataCacheTTL:       30 * time.Second,
		DataCacheTTL:           5 * time.Second,
		ConsistencyLevel:       SessionConsistency,
		ValidationInterval:     10 * time.Second,
		StaleThreshold:        30 * time.Second,
	}
} 
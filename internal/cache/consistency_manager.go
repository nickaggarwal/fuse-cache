package cache

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// ConsistencyManager manages consistency across the distributed filesystem
type ConsistencyManager struct {
	directoryTree   *DirectoryTree
	cacheManager    CacheManager
	peerNotifier    PeerNotifier
	
	// Background workers
	ctx             context.Context
	cancel          context.CancelFunc
	wg              sync.WaitGroup
	
	// Configuration
	config          *ConsistencyConfig
	
	// Metrics
	validationCount int64
	invalidations   int64
	peerUpdates     int64
	
	logger func(string, ...interface{})
}

// ConsistencyConfig holds configuration for consistency management
type ConsistencyConfig struct {
	ValidationInterval    time.Duration // How often to validate stale files
	StaleThreshold       time.Duration // When to consider metadata stale
	MaxConcurrentChecks  int           // Max concurrent validation checks
	PeerNotificationTTL  time.Duration // How long to wait for peer notifications
	RetryInterval        time.Duration // Retry interval for failed operations
	MaxRetries           int           // Maximum retry attempts
}

// PeerNotifier interface for notifying peers about file changes
type PeerNotifier interface {
	NotifyPeerUpdate(ctx context.Context, event InvalidationEvent) error
	BroadcastFileChange(ctx context.Context, filePath string, version int64) error
}

// NewConsistencyManager creates a new consistency manager
func NewConsistencyManager(dt *DirectoryTree, cm CacheManager, pn PeerNotifier, config *ConsistencyConfig) *ConsistencyManager {
	if config == nil {
		config = &ConsistencyConfig{
			ValidationInterval:   10 * time.Second,
			StaleThreshold:      30 * time.Second,
			MaxConcurrentChecks: 5,
			PeerNotificationTTL: 5 * time.Second,
			RetryInterval:       2 * time.Second,
			MaxRetries:          3,
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	
	consistencyMgr := &ConsistencyManager{
		directoryTree: dt,
		cacheManager:  cm,
		peerNotifier:  pn,
		ctx:          ctx,
		cancel:       cancel,
		config:       config,
		logger: func(format string, args ...interface{}) {
			fmt.Printf("[CONSISTENCY] "+format+"\n", args...)
		},
	}
	
	return consistencyMgr
}

// Start begins the consistency management background processes
func (cm *ConsistencyManager) Start() {
	cm.logger("Starting consistency manager...")
	
	// Start background validation of stale files
	cm.wg.Add(1)
	go cm.validateStaleFiles()
	
	// Start processing invalidation events
	cm.wg.Add(1)
	go cm.processInvalidations()
	
	// Start cleanup of expired cache entries
	cm.wg.Add(1)
	go cm.cleanupExpiredCache()
	
	cm.logger("Consistency manager started")
}

// Stop gracefully shuts down the consistency manager
func (cm *ConsistencyManager) Stop() {
	cm.logger("Stopping consistency manager...")
	cm.cancel()
	cm.wg.Wait()
	cm.logger("Consistency manager stopped")
}

// validateStaleFiles periodically checks for and validates stale files
func (cm *ConsistencyManager) validateStaleFiles() {
	defer cm.wg.Done()
	
	ticker := time.NewTicker(cm.config.ValidationInterval)
	defer ticker.Stop()
	
	semaphore := make(chan struct{}, cm.config.MaxConcurrentChecks)
	
	for {
		select {
		case <-cm.ctx.Done():
			return
		case <-ticker.C:
			staleFiles := cm.findStaleFiles()
			cm.logger("Found %d potentially stale files", len(staleFiles))
			
			for _, filePath := range staleFiles {
				select {
				case semaphore <- struct{}{}:
					go cm.validateAndRefreshFile(filePath, semaphore)
				case <-cm.ctx.Done():
					return
				default:
					// Skip if all workers are busy
					cm.logger("Skipping validation of %s - all workers busy", filePath)
				}
			}
		}
	}
}

// findStaleFiles identifies files that need validation
func (cm *ConsistencyManager) findStaleFiles() []string {
	var staleFiles []string
	threshold := time.Now().Add(-cm.config.StaleThreshold)
	
	cm.directoryTree.Mu.RLock()
	for path, node := range cm.directoryTree.fileMap {
		node.Mu.RLock()
		if node.LastValidated.Before(threshold) || node.IsStale {
			staleFiles = append(staleFiles, path)
		}
		node.Mu.RUnlock()
	}
	cm.directoryTree.Mu.RUnlock()
	
	return staleFiles
}

// validateAndRefreshFile validates a single file and refreshes if needed
func (cm *ConsistencyManager) validateAndRefreshFile(filePath string, semaphore chan struct{}) {
	defer func() { <-semaphore }()
	
	node := cm.directoryTree.GetFileNode(filePath)
	if node == nil {
		return
	}
	
	// Create validation context with timeout
	ctx, cancel := context.WithTimeout(cm.ctx, cm.config.PeerNotificationTTL)
	defer cancel()
	
	// Try to get fresh data from storage tiers
	entry, err := cm.cacheManager.Get(ctx, filePath)
	if err != nil {
		cm.logger("Failed to validate file %s: %v", filePath, err)
		return
	}
	
	node.Mu.Lock()
	defer node.Mu.Unlock()
	
	// Check if the file has actually changed
	if entry.Size != node.Size || !entry.LastAccessed.Equal(node.ModTime) {
		cm.logger("File %s has changed - updating metadata", filePath)
		
		// Update the directory tree with fresh data
		cm.directoryTree.AddOrUpdateFile(entry)
		
		// Notify peers about the change
		if cm.peerNotifier != nil {
			go cm.notifyPeersAsync(InvalidationEvent{
				FilePath: filePath,
				Version:  node.Version,
			})
		}
	} else {
		// File hasn't changed, just update validation timestamp
		node.LastValidated = time.Now()
		node.IsStale = false
	}
	
	cm.validationCount++
}

// processInvalidations handles invalidation events from peers
func (cm *ConsistencyManager) processInvalidations() {
	defer cm.wg.Done()
	
	for {
		select {
		case <-cm.ctx.Done():
			return
		case event := <-cm.directoryTree.invalidations:
			cm.handleInvalidationEvent(event)
		}
	}
}

// handleInvalidationEvent processes a single invalidation event
func (cm *ConsistencyManager) handleInvalidationEvent(event InvalidationEvent) {
	cm.logger("Processing invalidation event for %s (version %d)", event.FilePath, event.Version)
	
	node := cm.directoryTree.GetFileNode(event.FilePath)
	if node == nil {
		cm.logger("Invalidation event for unknown file: %s", event.FilePath)
		return
	}
	
	node.Mu.Lock()
	defer node.Mu.Unlock()
	
	// Check if this is a newer version
	if event.Version > node.Version {
		cm.logger("File %s is stale (local: %d, remote: %d)", event.FilePath, node.Version, event.Version)
		
		// Mark as stale and invalidate cached data
		node.IsStale = true
		if node.DataCache != nil {
			node.DataCache.Invalidate()
		}
		
		// Trigger immediate validation
		go cm.validateAndRefreshFile(event.FilePath, make(chan struct{}, 1))
	}
	
	cm.invalidations++
}

// cleanupExpiredCache removes expired cache entries
func (cm *ConsistencyManager) cleanupExpiredCache() {
	defer cm.wg.Done()
	
	ticker := time.NewTicker(2 * time.Minute) // Run every 2 minutes
	defer ticker.Stop()
	
	for {
		select {
		case <-cm.ctx.Done():
			return
		case <-ticker.C:
			cm.performCacheCleanup()
		}
	}
}

// performCacheCleanup removes expired cached data
func (cm *ConsistencyManager) performCacheCleanup() {
	cleanedCount := 0
	
	cm.directoryTree.Mu.RLock()
	filesToClean := make([]*FileNode, 0)
	for _, node := range cm.directoryTree.fileMap {
		node.Mu.RLock()
		if node.DataCache != nil && !node.DataCache.IsValid() {
			filesToClean = append(filesToClean, node)
		}
		node.Mu.RUnlock()
	}
	cm.directoryTree.Mu.RUnlock()
	
	for _, node := range filesToClean {
		node.Mu.Lock()
		if node.DataCache != nil && !node.DataCache.IsValid() {
			node.DataCache = nil // Remove expired cache
			cleanedCount++
		}
		node.Mu.Unlock()
	}
	
	if cleanedCount > 0 {
		cm.logger("Cleaned up %d expired cache entries", cleanedCount)
	}
}

// notifyPeersAsync asynchronously notifies peers about file changes
func (cm *ConsistencyManager) notifyPeersAsync(event InvalidationEvent) {
	if cm.peerNotifier == nil {
		return
	}
	
	ctx, cancel := context.WithTimeout(context.Background(), cm.config.PeerNotificationTTL)
	defer cancel()
	
	for attempt := 0; attempt < cm.config.MaxRetries; attempt++ {
		if err := cm.peerNotifier.NotifyPeerUpdate(ctx, event); err == nil {
			cm.logger("Successfully notified peers about %s", event.FilePath)
			cm.peerUpdates++
			return
		} else {
			cm.logger("Failed to notify peers about %s (attempt %d/%d): %v", 
				event.FilePath, attempt+1, cm.config.MaxRetries, err)
			
			if attempt < cm.config.MaxRetries-1 {
				select {
				case <-time.After(cm.config.RetryInterval):
					continue
				case <-ctx.Done():
					return
				}
			}
		}
	}
	
	cm.logger("Failed to notify peers about %s after %d attempts", event.FilePath, cm.config.MaxRetries)
}

// HandlePeerUpdate processes an update notification from a peer
func (cm *ConsistencyManager) HandlePeerUpdate(update PeerUpdateNotification) {
	event := InvalidationEvent{
		FilePath: update.FilePath,
		Version:  update.Version,
		PeerID:   update.PeerID,
	}
	
	select {
	case cm.directoryTree.invalidations <- event:
		cm.logger("Queued peer update for %s from %s", update.FilePath, update.PeerID)
	default:
		cm.logger("Invalidation queue full, dropping update for %s", update.FilePath)
	}
}

// ShouldValidateFile determines if a file needs validation based on its metadata
func (cm *ConsistencyManager) ShouldValidateFile(node *FileNode) bool {
	if node == nil {
		return false
	}
	
	node.Mu.RLock()
	defer node.Mu.RUnlock()
	
	// Always validate if marked as stale
	if node.IsStale {
		return true
	}
	
	// Validate if not checked recently
	if time.Since(node.LastValidated) > cm.config.StaleThreshold {
		return true
	}
	
	return false
}

// GetStats returns consistency manager statistics
func (cm *ConsistencyManager) GetStats() map[string]interface{} {
	return map[string]interface{}{
		"validation_count":     cm.validationCount,
		"invalidations":        cm.invalidations,
		"peer_updates":         cm.peerUpdates,
		"validation_interval":  cm.config.ValidationInterval.String(),
		"stale_threshold":      cm.config.StaleThreshold.String(),
		"max_concurrent_checks": cm.config.MaxConcurrentChecks,
	}
}

// PeerUpdateNotification represents an update notification from a peer
type PeerUpdateNotification struct {
	FilePath  string
	Version   int64
	PeerID    string
	Timestamp time.Time
	Size      int64
	Operation string // "create", "update", "delete"
} 
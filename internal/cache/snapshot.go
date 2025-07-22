package cache

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"
)

// DirectorySnapshot represents a cached directory listing
type DirectorySnapshot struct {
	Entries     []*CacheEntry
	LastUpdated time.Time
	mu          sync.RWMutex
}

// SnapshotManager manages directory snapshots with background synchronization
type SnapshotManager struct {
	localEntries     map[string]*CacheEntry
	peerEntries      map[string]*CacheEntry // Entries discovered from peers
	coordinatorAddr  string
	currentPeerID    string
	syncInterval     time.Duration
	lastSync         time.Time
	mu               sync.RWMutex
	ctx              context.Context
	cancel           context.CancelFunc
	client           *http.Client
	logger           func(string, ...interface{})
}

// NewSnapshotManager creates a new snapshot manager
func NewSnapshotManager(coordinatorAddr, peerID string, syncInterval time.Duration) *SnapshotManager {
	ctx, cancel := context.WithCancel(context.Background())
	
	sm := &SnapshotManager{
		localEntries:    make(map[string]*CacheEntry),
		peerEntries:     make(map[string]*CacheEntry),
		coordinatorAddr: coordinatorAddr,
		currentPeerID:   peerID,
		syncInterval:    syncInterval,
		ctx:             ctx,
		cancel:          cancel,
		client: &http.Client{
			Timeout: 10 * time.Second,
		},
		logger: func(format string, args ...interface{}) {
			fmt.Printf("[SNAPSHOT] "+format+"\n", args...)
		},
	}

	// Start background synchronization
	go sm.backgroundSync()
	
	return sm
}

// AddLocalEntry adds a local cache entry to the snapshot
func (sm *SnapshotManager) AddLocalEntry(entry *CacheEntry) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	
	sm.localEntries[entry.FilePath] = entry
	sm.logger("Added local entry: %s", entry.FilePath)
}

// RemoveLocalEntry removes a local cache entry from the snapshot
func (sm *SnapshotManager) RemoveLocalEntry(filePath string) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	
	delete(sm.localEntries, filePath)
	sm.logger("Removed local entry: %s", filePath)
}

// GetSnapshot returns the current directory snapshot (local entries only to avoid loops)
func (sm *SnapshotManager) GetSnapshot() []*CacheEntry {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	
	entries := make([]*CacheEntry, 0, len(sm.localEntries)+len(sm.peerEntries))
	
	// Add local entries
	for _, entry := range sm.localEntries {
		entries = append(entries, entry)
	}
	
	// Add peer entries (only if they don't conflict with local entries)
	for path, entry := range sm.peerEntries {
		if _, exists := sm.localEntries[path]; !exists {
			entries = append(entries, entry)
		}
	}
	
	return entries
}

// GetLocalSnapshot returns only local entries (used by FUSE operations to prevent loops)
func (sm *SnapshotManager) GetLocalSnapshot() []*CacheEntry {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	
	entries := make([]*CacheEntry, 0, len(sm.localEntries))
	for _, entry := range sm.localEntries {
		entries = append(entries, entry)
	}
	
	return entries
}

// backgroundSync performs periodic synchronization with peers
func (sm *SnapshotManager) backgroundSync() {
	ticker := time.NewTicker(sm.syncInterval)
	defer ticker.Stop()
	
	sm.logger("Started background synchronization (interval: %v)", sm.syncInterval)
	
	for {
		select {
		case <-sm.ctx.Done():
			sm.logger("Background sync stopped")
			return
		case <-ticker.C:
			sm.syncWithPeers()
		}
	}
}

// syncWithPeers synchronizes directory information with peers
func (sm *SnapshotManager) syncWithPeers() {
	ctx, cancel := context.WithTimeout(sm.ctx, 30*time.Second)
	defer cancel()
	
	sm.logger("Starting peer synchronization...")
	
	peers, err := sm.getPeersFromCoordinator(ctx)
	if err != nil {
		sm.logger("Failed to get peers for sync: %v", err)
		return
	}
	
	sm.logger("Found %d peers for synchronization", len(peers))
	
	newPeerEntries := make(map[string]*CacheEntry)
	
	// Query each peer for their cached files
	for _, peer := range peers {
		if peer.Status != "active" {
			continue
		}
		
		// Skip self to avoid circular calls
		if peer.ID == sm.currentPeerID {
			continue
		}
		
		entries, err := sm.queryPeerCache(ctx, peer.Address)
		if err != nil {
			sm.logger("Failed to sync with peer %s: %v", peer.ID, err)
			continue
		}
		
		sm.logger("Synced %d entries from peer %s", len(entries), peer.ID)
		
		// Add entries to the peer cache (avoiding duplicates)
		for _, entry := range entries {
			if _, exists := newPeerEntries[entry.FilePath]; !exists {
				newPeerEntries[entry.FilePath] = entry
			}
		}
	}
	
	// Update peer entries atomically
	sm.mu.Lock()
	sm.peerEntries = newPeerEntries
	sm.lastSync = time.Now()
	sm.mu.Unlock()
	
	sm.logger("Peer synchronization completed. Total peer entries: %d", len(newPeerEntries))
}

// queryPeerCache queries a specific peer's cache via HTTP (non-recursive endpoint)
func (sm *SnapshotManager) queryPeerCache(ctx context.Context, peerAddr string) ([]*CacheEntry, error) {
	// Use a special endpoint that only returns local entries to avoid recursion
	url := fmt.Sprintf("http://%s/api/cache/local", peerAddr)
	
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, err
	}
	
	resp, err := sm.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("peer returned status: %d", resp.StatusCode)
	}
	
	var entries []*CacheEntry
	if err := json.NewDecoder(resp.Body).Decode(&entries); err != nil {
		return nil, err
	}
	
	return entries, nil
}

// getPeersFromCoordinator gets the peer list from the coordinator
func (sm *SnapshotManager) getPeersFromCoordinator(ctx context.Context) ([]PeerInfo, error) {
	if sm.coordinatorAddr == "" {
		return nil, fmt.Errorf("coordinator address not configured")
	}
	
	url := fmt.Sprintf("http://%s/api/peers", sm.coordinatorAddr)
	
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, err
	}
	
	resp, err := sm.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("coordinator returned status: %d", resp.StatusCode)
	}
	
	var peers []PeerInfo
	if err := json.NewDecoder(resp.Body).Decode(&peers); err != nil {
		return nil, err
	}
	
	return peers, nil
}

// GetSyncStatus returns information about the last synchronization
func (sm *SnapshotManager) GetSyncStatus() map[string]interface{} {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	
	return map[string]interface{}{
		"last_sync":      sm.lastSync,
		"local_entries":  len(sm.localEntries),
		"peer_entries":   len(sm.peerEntries),
		"sync_interval":  sm.syncInterval.String(),
	}
}

// Stop stops the background synchronization
func (sm *SnapshotManager) Stop() {
	sm.logger("Stopping snapshot manager...")
	sm.cancel()
} 
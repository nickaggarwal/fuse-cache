package coordinator

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// PeerInfo represents a peer in the system
type PeerInfo struct {
	ID             string    `json:"id"`
	Address        string    `json:"address"`
	NVMePath       string    `json:"nvme_path"`
	AvailableSpace int64     `json:"available_space"`
	UsedSpace      int64     `json:"used_space"`
	Status         string    `json:"status"`
	LastHeartbeat  time.Time `json:"last_heartbeat"`
}

// ChunkInfo represents a file chunk
type ChunkInfo struct {
	Index   int    `json:"index"`
	PeerID  string `json:"peer_id"`
	ChunkID string `json:"chunk_id"`
}

// FileLocation represents the location of a file
type FileLocation struct {
	FilePath     string      `json:"file_path"`
	PeerID       string      `json:"peer_id"`
	StorageTier  string      `json:"storage_tier"`
	StoragePath  string      `json:"storage_path"`
	FileSize     int64       `json:"file_size"`
	LastAccessed time.Time   `json:"last_accessed"`
	IsChunked    bool        `json:"is_chunked"`
	Chunks       []ChunkInfo `json:"chunks,omitempty"`
}

// Ensure CoordinatorService implements the Coordinator interface
var _ Coordinator = (*CoordinatorService)(nil)

// CoordinatorService manages peer registration and file metadata
type CoordinatorService struct {
	peers         map[string]*PeerInfo
	fileLocations map[string][]*FileLocation
	mu            sync.RWMutex
	logger        *log.Logger
}

// NewCoordinatorService creates a new coordinator service
func NewCoordinatorService() *CoordinatorService {
	return &CoordinatorService{
		peers:         make(map[string]*PeerInfo),
		fileLocations: make(map[string][]*FileLocation),
		logger:        log.New(log.Writer(), "[COORDINATOR] ", log.LstdFlags),
	}
}

// RegisterPeer registers a new peer in the system
func (cs *CoordinatorService) RegisterPeer(ctx context.Context, peer *PeerInfo) error {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	peer.LastHeartbeat = time.Now()
	peer.Status = "active"

	cs.peers[peer.ID] = peer
	cs.logger.Printf("Registered peer: %s at %s", peer.ID, peer.Address)

	return nil
}

// GetPeers returns all active peers
func (cs *CoordinatorService) GetPeers(ctx context.Context, requesterID string) ([]*PeerInfo, error) {
	cs.mu.RLock()
	defer cs.mu.RUnlock()

	peers := make([]*PeerInfo, 0, len(cs.peers))
	for _, peer := range cs.peers {
		if peer.ID != requesterID && peer.Status == "active" {
			peers = append(peers, peer)
		}
	}

	return peers, nil
}

// UpdatePeerStatus updates the status of a peer
func (cs *CoordinatorService) UpdatePeerStatus(ctx context.Context, peerID string, status string, availableSpace, usedSpace int64) error {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	peer, exists := cs.peers[peerID]
	if !exists {
		cs.logger.Printf("Peer not found: %s", peerID)
		return nil
	}

	peer.Status = status
	peer.AvailableSpace = availableSpace
	peer.UsedSpace = usedSpace
	peer.LastHeartbeat = time.Now()

	cs.logger.Printf("Updated peer status: %s -> %s", peerID, status)
	return nil
}

// GetFileLocation returns the locations of a file
func (cs *CoordinatorService) GetFileLocation(ctx context.Context, filePath string) ([]*FileLocation, error) {
	cs.mu.RLock()
	defer cs.mu.RUnlock()

	locations := cs.fileLocations[filePath]
	if locations == nil {
		return []*FileLocation{}, nil
	}

	result := make([]*FileLocation, len(locations))
	copy(result, locations)
	return result, nil
}

// UpdateFileLocation updates the location of a file
func (cs *CoordinatorService) UpdateFileLocation(ctx context.Context, location *FileLocation) error {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	locations := cs.fileLocations[location.FilePath]
	if locations == nil {
		locations = make([]*FileLocation, 0)
	}

	for i, loc := range locations {
		if loc.PeerID == location.PeerID && loc.StorageTier == location.StorageTier {
			locations[i] = location
			cs.fileLocations[location.FilePath] = locations
			return nil
		}
	}

	locations = append(locations, location)
	cs.fileLocations[location.FilePath] = locations

	cs.logger.Printf("Updated file location: %s on peer %s", location.FilePath, location.PeerID)
	return nil
}

// CleanupInactivePeers removes peers that haven't sent heartbeats
func (cs *CoordinatorService) CleanupInactivePeers(timeout time.Duration) {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	now := time.Now()
	for peerID, peer := range cs.peers {
		if now.Sub(peer.LastHeartbeat) > timeout {
			peer.Status = "inactive"
			cs.logger.Printf("Peer %s marked as inactive", peerID)
		}
	}
}

// GetPeerStats returns statistics about registered peers
func (cs *CoordinatorService) GetPeerStats() map[string]interface{} {
	cs.mu.RLock()
	defer cs.mu.RUnlock()

	activePeers := 0
	totalSpace := int64(0)
	usedSpace := int64(0)

	for _, peer := range cs.peers {
		if peer.Status == "active" {
			activePeers++
			totalSpace += peer.AvailableSpace
			usedSpace += peer.UsedSpace
		}
	}

	return map[string]interface{}{
		"active_peers":    activePeers,
		"total_peers":     len(cs.peers),
		"total_space":     totalSpace,
		"used_space":      usedSpace,
		"available_space": totalSpace - usedSpace,
		"file_count":      len(cs.fileLocations),
	}
}

// Start starts the coordinator service with periodic cleanup and persistence
func (cs *CoordinatorService) Start(ctx context.Context) {
	if err := cs.LoadState(); err != nil {
		cs.logger.Printf("Failed to load state: %v", err)
	} else {
		cs.logger.Printf("Loaded state from disk")
	}

	go func() {
		cleanupTicker := time.NewTicker(30 * time.Second)
		saveTicker := time.NewTicker(1 * time.Minute)
		defer cleanupTicker.Stop()
		defer saveTicker.Stop()

		for {
			select {
			case <-ctx.Done():
				cs.SaveState()
				return
			case <-cleanupTicker.C:
				cs.CleanupInactivePeers(60 * time.Second)
			case <-saveTicker.C:
				if err := cs.SaveState(); err != nil {
					cs.logger.Printf("Failed to save state: %v", err)
				}
			}
		}
	}()

	cs.logger.Printf("Coordinator service started")
}

// State represents the persistent state of the coordinator
type State struct {
	Peers         map[string]*PeerInfo       `json:"peers"`
	FileLocations map[string][]*FileLocation `json:"file_locations"`
}

// SaveState saves the coordinator state to disk atomically
func (cs *CoordinatorService) SaveState() error {
	// Snapshot state under lock
	cs.mu.RLock()
	state := State{
		Peers:         make(map[string]*PeerInfo, len(cs.peers)),
		FileLocations: make(map[string][]*FileLocation, len(cs.fileLocations)),
	}
	for k, v := range cs.peers {
		peerCopy := *v
		state.Peers[k] = &peerCopy
	}
	for k, v := range cs.fileLocations {
		locsCopy := make([]*FileLocation, len(v))
		for i, loc := range v {
			locCopy := *loc
			locsCopy[i] = &locCopy
		}
		state.FileLocations[k] = locsCopy
	}
	cs.mu.RUnlock()

	// Marshal outside the lock
	data, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return err
	}

	// Write to temp file, then atomic rename
	stateFile := "coordinator_state.json"
	tmpFile := stateFile + ".tmp"
	if err := os.WriteFile(tmpFile, data, 0644); err != nil {
		return err
	}

	// Ensure parent dir for rename (same dir, so no issue)
	return os.Rename(tmpFile, filepath.Clean(stateFile))
}

// LoadState loads the coordinator state from disk
func (cs *CoordinatorService) LoadState() error {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	data, err := os.ReadFile("coordinator_state.json")
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	var state State
	if err := json.Unmarshal(data, &state); err != nil {
		return err
	}

	cs.peers = state.Peers
	cs.fileLocations = state.FileLocations
	return nil
}

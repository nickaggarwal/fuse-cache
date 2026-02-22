package coordinator

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

const (
	defaultStateFilePath = "coordinator_state.json"
)

// PeerInfo represents a peer in the system
type PeerInfo struct {
	ID                 string    `json:"id"`
	Address            string    `json:"address"`
	GRPCAddress        string    `json:"grpc_address"`
	NVMePath           string    `json:"nvme_path"`
	AvailableSpace     int64     `json:"available_space"`
	UsedSpace          int64     `json:"used_space"`
	Status             string    `json:"status"`
	LastHeartbeat      time.Time `json:"last_heartbeat"`
	NetworkSpeedMBps   float64   `json:"network_speed_mbps,omitempty"`
	NetworkLatencyMs   float64   `json:"network_latency_ms,omitempty"`
	NetworkProbeBytes  int64     `json:"network_probe_bytes,omitempty"`
	NetworkProbeTarget string    `json:"network_probe_target,omitempty"`
	NetworkProbedAt    time.Time `json:"network_probed_at,omitempty"`
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

// WorldView captures global metadata state across peers and files.
type WorldView struct {
	GeneratedAt time.Time        `json:"generated_at"`
	Summary     WorldViewSummary `json:"summary"`
	Peers       []*PeerInfo      `json:"peers"`
	Files       []*WorldFileView `json:"files"`
}

type WorldViewSummary struct {
	ActivePeers int `json:"active_peers"`
	TotalPeers  int `json:"total_peers"`
	TotalFiles  int `json:"total_files"`
}

type WorldFileView struct {
	FilePath      string          `json:"file_path"`
	FileSize      int64           `json:"file_size"`
	IsChunked     bool            `json:"is_chunked"`
	ReplicaCount  int             `json:"replica_count"`
	TierReplicas  map[string]int  `json:"tier_replicas"`
	ChunkReplicas map[int]int     `json:"chunk_replicas,omitempty"`
	Locations     []*FileLocation `json:"locations"`
}

// SeedCacheRequest requests coordinator-orchestrated seeding for a file path.
type SeedCacheRequest struct {
	FilePath       string `json:"file_path"`
	SeedPercentage int    `json:"seed_percentage"`
	SourcePeerID   string `json:"source_peer_id,omitempty"`
	TimeoutSeconds int    `json:"timeout_seconds,omitempty"`
}

type SeedCacheResult struct {
	FilePath       string   `json:"file_path"`
	SourcePeerID   string   `json:"source_peer_id"`
	RequestedSeeds int      `json:"requested_seeds"`
	AttemptedSeeds int      `json:"attempted_seeds"`
	Successful     []string `json:"successful"`
	Failed         []string `json:"failed"`
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
	return cs.UpdatePeerStatusWithNetwork(ctx, peerID, status, availableSpace, usedSpace, nil)
}

// UpdatePeerStatusWithNetwork updates peer status and optional network telemetry.
func (cs *CoordinatorService) UpdatePeerStatusWithNetwork(ctx context.Context, peerID string, status string, availableSpace, usedSpace int64, metrics *PeerNetworkMetrics) error {
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
	if metrics != nil {
		if metrics.SpeedMBps > 0 {
			peer.NetworkSpeedMBps = metrics.SpeedMBps
		}
		if metrics.LatencyMs > 0 {
			peer.NetworkLatencyMs = metrics.LatencyMs
		}
		if metrics.ProbeBytes > 0 {
			peer.NetworkProbeBytes = metrics.ProbeBytes
		}
		if metrics.ProbeTarget != "" {
			peer.NetworkProbeTarget = metrics.ProbeTarget
		}
		if !metrics.ProbedAt.IsZero() {
			peer.NetworkProbedAt = metrics.ProbedAt
		} else {
			peer.NetworkProbedAt = time.Now()
		}
	}

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

// ListFileLocations returns one metadata location per file path, filtered by prefix.
func (cs *CoordinatorService) ListFileLocations(ctx context.Context, prefix string) ([]*FileLocation, error) {
	cs.mu.RLock()
	defer cs.mu.RUnlock()

	result := make([]*FileLocation, 0, len(cs.fileLocations))
	for path, locations := range cs.fileLocations {
		if prefix != "" && !strings.HasPrefix(path, prefix) {
			continue
		}
		if len(locations) == 0 {
			continue
		}
		locCopy := *locations[0]
		result = append(result, &locCopy)
	}
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
	networkSampleCount := 0
	networkSpeedSum := 0.0
	networkLatencySum := 0.0

	for _, peer := range cs.peers {
		if peer.Status == "active" {
			activePeers++
			totalSpace += peer.AvailableSpace
			usedSpace += peer.UsedSpace
			if peer.NetworkSpeedMBps > 0 {
				networkSampleCount++
				networkSpeedSum += peer.NetworkSpeedMBps
			}
			if peer.NetworkLatencyMs > 0 {
				networkLatencySum += peer.NetworkLatencyMs
			}
		}
	}

	avgNetworkSpeed := 0.0
	avgNetworkLatency := 0.0
	if networkSampleCount > 0 {
		avgNetworkSpeed = networkSpeedSum / float64(networkSampleCount)
		avgNetworkLatency = networkLatencySum / float64(networkSampleCount)
	}

	return map[string]interface{}{
		"active_peers":           activePeers,
		"total_peers":            len(cs.peers),
		"total_space":            totalSpace,
		"used_space":             usedSpace,
		"available_space":        totalSpace - usedSpace,
		"file_count":             len(cs.fileLocations),
		"network_sampled_peers":  networkSampleCount,
		"avg_network_speed_mbps": avgNetworkSpeed,
		"avg_network_latency_ms": avgNetworkLatency,
	}
}

// GetWorldView returns a global metadata view including peers, files, and chunk replica counts.
func (cs *CoordinatorService) GetWorldView(ctx context.Context, prefix string) (*WorldView, error) {
	cs.mu.RLock()
	defer cs.mu.RUnlock()

	peers := make([]*PeerInfo, 0, len(cs.peers))
	activePeers := 0
	peerIDs := make([]string, 0, len(cs.peers))
	for id := range cs.peers {
		peerIDs = append(peerIDs, id)
	}
	sort.Strings(peerIDs)
	for _, id := range peerIDs {
		p := cs.peers[id]
		pCopy := *p
		peers = append(peers, &pCopy)
		if p.Status == "active" {
			activePeers++
		}
	}

	filePaths := make([]string, 0, len(cs.fileLocations))
	for path := range cs.fileLocations {
		if prefix != "" && !strings.HasPrefix(path, prefix) {
			continue
		}
		filePaths = append(filePaths, path)
	}
	sort.Strings(filePaths)

	files := make([]*WorldFileView, 0, len(filePaths))
	for _, path := range filePaths {
		locations := cs.fileLocations[path]
		if len(locations) == 0 {
			continue
		}
		view := &WorldFileView{
			FilePath:      path,
			TierReplicas:  make(map[string]int),
			ChunkReplicas: make(map[int]int),
			Locations:     make([]*FileLocation, 0, len(locations)),
		}
		for _, loc := range locations {
			if loc == nil {
				continue
			}
			locCopy := *loc
			view.Locations = append(view.Locations, &locCopy)

			view.ReplicaCount++
			tier := strings.ToLower(strings.TrimSpace(loc.StorageTier))
			if tier == "" {
				tier = "unknown"
			}
			view.TierReplicas[tier]++

			if loc.FileSize > view.FileSize {
				view.FileSize = loc.FileSize
			}
			view.IsChunked = view.IsChunked || loc.IsChunked
			for _, ch := range loc.Chunks {
				view.ChunkReplicas[ch.Index]++
			}
		}
		if len(view.ChunkReplicas) == 0 {
			view.ChunkReplicas = nil
		}
		files = append(files, view)
	}

	return &WorldView{
		GeneratedAt: time.Now(),
		Summary: WorldViewSummary{
			ActivePeers: activePeers,
			TotalPeers:  len(peers),
			TotalFiles:  len(files),
		},
		Peers: peers,
		Files: files,
	}, nil
}

// SeedPathToPeers reads file data from a source peer and writes it to a percentage of active seed peers.
func (cs *CoordinatorService) SeedPathToPeers(ctx context.Context, req SeedCacheRequest) (*SeedCacheResult, error) {
	filePath := strings.TrimSpace(req.FilePath)
	if filePath == "" {
		return nil, errors.New("file_path is required")
	}
	if req.SeedPercentage <= 0 || req.SeedPercentage > 100 {
		return nil, errors.New("seed_percentage must be between 1 and 100")
	}

	timeout := 60 * time.Second
	if req.TimeoutSeconds > 0 {
		timeout = time.Duration(req.TimeoutSeconds) * time.Second
	}
	callCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	cs.mu.RLock()
	activePeers := make(map[string]*PeerInfo, len(cs.peers))
	activeIDs := make([]string, 0, len(cs.peers))
	for id, peer := range cs.peers {
		if peer == nil || peer.Status != "active" {
			continue
		}
		pCopy := *peer
		activePeers[id] = &pCopy
		activeIDs = append(activeIDs, id)
	}
	sort.Strings(activeIDs)

	locations := cs.fileLocations[filePath]
	locCopies := make([]*FileLocation, 0, len(locations))
	for _, loc := range locations {
		if loc == nil {
			continue
		}
		lc := *loc
		locCopies = append(locCopies, &lc)
	}
	cs.mu.RUnlock()

	if len(locCopies) == 0 {
		return nil, fmt.Errorf("file not found in coordinator metadata: %s", filePath)
	}

	sourceID := strings.TrimSpace(req.SourcePeerID)
	if sourceID == "" {
		for _, loc := range locCopies {
			if loc.StorageTier == "cloud" {
				continue
			}
			if _, ok := activePeers[loc.PeerID]; ok {
				sourceID = loc.PeerID
				break
			}
		}
	}
	if sourceID == "" {
		return nil, fmt.Errorf("no active source peer found for %s", filePath)
	}
	sourcePeer, ok := activePeers[sourceID]
	if !ok {
		return nil, fmt.Errorf("source peer is not active: %s", sourceID)
	}

	targetCandidates := make([]string, 0, len(activeIDs))
	hasLocation := make(map[string]struct{}, len(locCopies))
	for _, loc := range locCopies {
		hasLocation[loc.PeerID] = struct{}{}
	}
	for _, id := range activeIDs {
		if id == sourceID {
			continue
		}
		if _, exists := hasLocation[id]; exists {
			continue
		}
		targetCandidates = append(targetCandidates, id)
	}
	if len(targetCandidates) == 0 {
		return &SeedCacheResult{
			FilePath:       filePath,
			SourcePeerID:   sourceID,
			RequestedSeeds: 0,
			AttemptedSeeds: 0,
			Successful:     []string{},
			Failed:         []string{},
		}, nil
	}

	requestedSeeds := int(math.Ceil(float64(len(targetCandidates)) * float64(req.SeedPercentage) / 100.0))
	if requestedSeeds < 1 {
		requestedSeeds = 1
	}
	if requestedSeeds > len(targetCandidates) {
		requestedSeeds = len(targetCandidates)
	}
	targetIDs := targetCandidates[:requestedSeeds]

	data, err := fetchFileFromPeer(callCtx, sourcePeer.Address, filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch source file from %s: %w", sourceID, err)
	}

	result := &SeedCacheResult{
		FilePath:       filePath,
		SourcePeerID:   sourceID,
		RequestedSeeds: requestedSeeds,
		AttemptedSeeds: len(targetIDs),
		Successful:     make([]string, 0, len(targetIDs)),
		Failed:         make([]string, 0),
	}
	for _, targetID := range targetIDs {
		target := activePeers[targetID]
		if target == nil {
			result.Failed = append(result.Failed, fmt.Sprintf("%s:peer-not-found", targetID))
			continue
		}
		if err := putFileToPeer(callCtx, target.Address, filePath, data); err != nil {
			result.Failed = append(result.Failed, fmt.Sprintf("%s:%v", targetID, err))
			continue
		}
		result.Successful = append(result.Successful, targetID)
	}
	return result, nil
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

func (cs *CoordinatorService) buildStateSnapshot() State {
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
	return state
}

// SaveState saves the coordinator state to disk atomically
func (cs *CoordinatorService) SaveState() error {
	return cs.SaveStateToPath(defaultStateFilePath)
}

// SnapshotState marshals coordinator state into JSON.
func (cs *CoordinatorService) SnapshotState() ([]byte, error) {
	cs.mu.RLock()
	state := cs.buildStateSnapshot()
	cs.mu.RUnlock()
	return json.MarshalIndent(state, "", "  ")
}

// RestoreState loads coordinator state from snapshot JSON bytes.
func (cs *CoordinatorService) RestoreState(data []byte) error {
	if len(data) == 0 {
		return errors.New("empty snapshot data")
	}
	var state State
	if err := json.Unmarshal(data, &state); err != nil {
		return err
	}
	if state.Peers == nil {
		state.Peers = make(map[string]*PeerInfo)
	}
	if state.FileLocations == nil {
		state.FileLocations = make(map[string][]*FileLocation)
	}

	cs.mu.Lock()
	cs.peers = state.Peers
	cs.fileLocations = state.FileLocations
	cs.mu.Unlock()
	return nil
}

// SaveStateToPath saves coordinator state to a specific file path atomically.
func (cs *CoordinatorService) SaveStateToPath(stateFile string) error {
	data, err := cs.SnapshotState()
	if err != nil {
		return err
	}

	// Write to temp file, then atomic rename
	tmpFile := stateFile + ".tmp"
	if err := os.WriteFile(tmpFile, data, 0644); err != nil {
		return err
	}

	// Ensure parent dir for rename (same dir, so no issue)
	return os.Rename(tmpFile, filepath.Clean(stateFile))
}

// LoadState loads the coordinator state from disk
func (cs *CoordinatorService) LoadState() error {
	return cs.LoadStateFromPath(defaultStateFilePath)
}

// LoadStateFromPath loads coordinator state from a specific file path.
func (cs *CoordinatorService) LoadStateFromPath(stateFile string) error {
	data, err := os.ReadFile(stateFile)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	return cs.RestoreState(data)
}

func peerFileURL(peerAddr, filePath string) string {
	parts := strings.Split(strings.TrimPrefix(filePath, "/"), "/")
	escaped := make([]string, 0, len(parts))
	for _, part := range parts {
		if part == "" {
			continue
		}
		escaped = append(escaped, url.PathEscape(part))
	}
	joined := strings.Join(escaped, "/")
	if joined == "" {
		return fmt.Sprintf("http://%s/api/files/", peerAddr)
	}
	return fmt.Sprintf("http://%s/api/files/%s", peerAddr, joined)
}

func fetchFileFromPeer(ctx context.Context, peerAddr, filePath string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, peerFileURL(peerAddr, filePath), nil)
	if err != nil {
		return nil, err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("GET failed status=%d body=%s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	return io.ReadAll(resp.Body)
}

func putFileToPeer(ctx context.Context, peerAddr, filePath string, data []byte) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, peerFileURL(peerAddr, filePath), bytes.NewReader(data))
	if err != nil {
		return err
	}
	req.ContentLength = int64(len(data))
	req.Header.Set("Content-Type", "application/octet-stream")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("PUT failed status=%d body=%s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	return nil
}

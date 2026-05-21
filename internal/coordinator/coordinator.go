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

// CoordinatorService manages peer registration and file metadata. All state
// lives in the underlying Store, which may be process-local or distributed.
type CoordinatorService struct {
	store  Store
	logger *log.Logger
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

// NewCoordinatorService creates a coordinator backed by an in-memory store.
// For HA deployments, use NewCoordinatorServiceWithStore with an EtcdStore.
func NewCoordinatorService() *CoordinatorService {
	return NewCoordinatorServiceWithStore(NewInMemoryStore())
}

// NewCoordinatorServiceWithStore creates a coordinator with the given store.
func NewCoordinatorServiceWithStore(store Store) *CoordinatorService {
	return &CoordinatorService{
		store:  store,
		logger: log.New(log.Writer(), "[COORDINATOR] ", log.LstdFlags),
	}
}

// RegisterPeer registers a new peer in the system
func (cs *CoordinatorService) RegisterPeer(ctx context.Context, peer *PeerInfo) error {
	peer.LastHeartbeat = time.Now()
	peer.Status = "active"
	if err := cs.store.PutPeer(ctx, peer); err != nil {
		return err
	}
	cs.logger.Printf("Registered peer: %s at %s", peer.ID, peer.Address)
	return nil
}

// GetPeers returns all active peers
func (cs *CoordinatorService) GetPeers(ctx context.Context, requesterID string) ([]*PeerInfo, error) {
	all, err := cs.store.ListPeers(ctx)
	if err != nil {
		return nil, err
	}
	peers := make([]*PeerInfo, 0, len(all))
	for _, peer := range all {
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
	peer, err := cs.store.GetPeer(ctx, peerID)
	if err != nil {
		return err
	}
	if peer == nil {
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

	if err := cs.store.PutPeer(ctx, peer); err != nil {
		return err
	}
	cs.logger.Printf("Updated peer status: %s -> %s", peerID, status)
	return nil
}

// GetFileLocation returns the locations of a file
func (cs *CoordinatorService) GetFileLocation(ctx context.Context, filePath string) ([]*FileLocation, error) {
	return cs.store.GetFileLocations(ctx, filePath)
}

// ListFileLocations returns one metadata location per file path, filtered by prefix.
func (cs *CoordinatorService) ListFileLocations(ctx context.Context, prefix string) ([]*FileLocation, error) {
	return cs.store.ListFileLocations(ctx, prefix)
}

// UpdateFileLocation updates the location of a file
func (cs *CoordinatorService) UpdateFileLocation(ctx context.Context, location *FileLocation) error {
	if err := cs.store.PutFileLocation(ctx, location); err != nil {
		return err
	}
	cs.logger.Printf("Updated file location: %s on peer %s", location.FilePath, location.PeerID)
	return nil
}

// CleanupInactivePeers marks peers whose heartbeats are older than timeout as
// inactive. With an etcd-backed store this is a no-op because lease expiry
// removes stale peers automatically.
func (cs *CoordinatorService) CleanupInactivePeers(timeout time.Duration) {
	cutoff := time.Now().Add(-timeout)
	if err := cs.store.MarkInactivePeersBefore(context.Background(), cutoff); err != nil {
		cs.logger.Printf("CleanupInactivePeers: %v", err)
	}
}

// GetPeerStats returns statistics about registered peers
func (cs *CoordinatorService) GetPeerStats() map[string]interface{} {
	ctx := context.Background()
	peers, err := cs.store.ListPeers(ctx)
	if err != nil {
		return map[string]interface{}{"error": err.Error()}
	}
	files, err := cs.store.ListFileLocations(ctx, "")
	if err != nil {
		return map[string]interface{}{"error": err.Error()}
	}

	activePeers := 0
	totalSpace := int64(0)
	usedSpace := int64(0)
	networkSampleCount := 0
	networkSpeedSum := 0.0
	networkLatencySum := 0.0

	for _, peer := range peers {
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
		"total_peers":            len(peers),
		"total_space":            totalSpace,
		"used_space":             usedSpace,
		"available_space":        totalSpace - usedSpace,
		"file_count":             len(files),
		"network_sampled_peers":  networkSampleCount,
		"avg_network_speed_mbps": avgNetworkSpeed,
		"avg_network_latency_ms": avgNetworkLatency,
	}
}

// GetWorldView returns a global metadata view including peers, files, and chunk replica counts.
func (cs *CoordinatorService) GetWorldView(ctx context.Context, prefix string) (*WorldView, error) {
	allPeers, err := cs.store.ListPeers(ctx)
	if err != nil {
		return nil, err
	}
	peerByID := make(map[string]*PeerInfo, len(allPeers))
	peerIDs := make([]string, 0, len(allPeers))
	for _, p := range allPeers {
		peerByID[p.ID] = p
		peerIDs = append(peerIDs, p.ID)
	}
	sort.Strings(peerIDs)

	peers := make([]*PeerInfo, 0, len(peerIDs))
	activePeers := 0
	for _, id := range peerIDs {
		p := peerByID[id]
		peers = append(peers, p)
		if p.Status == "active" {
			activePeers++
		}
	}

	fileMap, err := cs.store.RangeFileLocations(ctx, prefix)
	if err != nil {
		return nil, err
	}
	filePaths := make([]string, 0, len(fileMap))
	for path := range fileMap {
		filePaths = append(filePaths, path)
	}
	sort.Strings(filePaths)

	files := make([]*WorldFileView, 0, len(filePaths))
	for _, path := range filePaths {
		locations := fileMap[path]
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

	allPeers, err := cs.store.ListPeers(callCtx)
	if err != nil {
		return nil, fmt.Errorf("list peers: %w", err)
	}
	activePeers := make(map[string]*PeerInfo, len(allPeers))
	activeIDs := make([]string, 0, len(allPeers))
	for _, peer := range allPeers {
		if peer == nil || peer.Status != "active" {
			continue
		}
		activePeers[peer.ID] = peer
		activeIDs = append(activeIDs, peer.ID)
	}
	sort.Strings(activeIDs)

	locCopies, err := cs.store.GetFileLocations(callCtx, filePath)
	if err != nil {
		return nil, fmt.Errorf("get file locations: %w", err)
	}

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

// Start starts the coordinator service. For the in-memory store it loads any
// existing JSON snapshot from disk and runs periodic cleanup + save loops. For
// distributed stores (etcd) those are no-ops: etcd is the source of truth and
// loading a stale local JSON would clobber live cluster state.
func (cs *CoordinatorService) Start(ctx context.Context) {
	if _, inMemory := cs.store.(*InMemoryStore); !inMemory {
		cs.logger.Printf("Coordinator service started (distributed store; skipping disk persistence)")
		return
	}

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
	return cs.SaveStateToPath(defaultStateFilePath)
}

// SnapshotState marshals coordinator state into JSON.
func (cs *CoordinatorService) SnapshotState() ([]byte, error) {
	state, err := cs.store.Snapshot(context.Background())
	if err != nil {
		return nil, err
	}
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
	return cs.store.Restore(context.Background(), state)
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

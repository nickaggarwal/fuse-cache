package api

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"fuse-client/internal/cache"
	"fuse-client/internal/coordinator"

	"github.com/gorilla/mux"
)

const maxUploadSize = 2 * 1024 * 1024 * 1024 // 2GB

const (
	defaultNetProbeBytes = 1 * 1024 * 1024
	maxNetProbeBytes     = 32 * 1024 * 1024
	netProbeChunkSize    = 64 * 1024
)

// Handler handles HTTP requests for the peer API
type Handler struct {
	cacheManager cache.CacheManager
	coordinator  coordinator.Coordinator
	peerID       string
	apiKey       string
	logger       *log.Logger
}

type fsSnapshotRequest struct {
	Prefix       string `json:"prefix"`
	IncludeData  *bool  `json:"include_data,omitempty"`
	MaxFiles     int    `json:"max_files,omitempty"`
	PersistCloud *bool  `json:"persist_cloud,omitempty"`
	CloudPath    string `json:"cloud_path,omitempty"`
}

type fsSnapshot struct {
	Version          int              `json:"version"`
	CreatedAt        time.Time        `json:"created_at"`
	PeerID           string           `json:"peer_id"`
	Prefix           string           `json:"prefix,omitempty"`
	Files            []fsSnapshotFile `json:"files"`
	PersistedToCloud bool             `json:"persisted_to_cloud,omitempty"`
	CloudPath        string           `json:"cloud_path,omitempty"`
}

type fsSnapshotFile struct {
	FilePath     string    `json:"file_path"`
	Size         int64     `json:"size"`
	LastAccessed time.Time `json:"last_accessed"`
	IsChunked    bool      `json:"is_chunked"`
	ContentB64   string    `json:"content_b64,omitempty"`
}

type fsRestoreRequest struct {
	Snapshot  fsSnapshot `json:"snapshot"`
	Overwrite *bool      `json:"overwrite,omitempty"`
	CloudPath string     `json:"cloud_path,omitempty"`
}

type cloudObjectStore interface {
	WriteCloud(ctx context.Context, path string, data []byte) error
	ReadCloud(ctx context.Context, path string) ([]byte, error)
}

// NewHandler creates a new API handler
func NewHandler(cacheManager cache.CacheManager, coord coordinator.Coordinator, peerID string, apiKey string) *Handler {
	return &Handler{
		cacheManager: cacheManager,
		coordinator:  coord,
		peerID:       peerID,
		apiKey:       apiKey,
		logger:       log.New(log.Writer(), "[API] ", log.LstdFlags),
	}
}

// authMiddleware checks API key if configured
func (h *Handler) authMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Health endpoint is always public
		if r.URL.Path == "/api/health" || r.URL.Path == "/metrics" || r.URL.Path == "/api/netprobe" {
			next.ServeHTTP(w, r)
			return
		}
		if h.apiKey != "" {
			key := r.Header.Get("X-API-Key")
			if key != h.apiKey {
				http.Error(w, "Unauthorized", http.StatusUnauthorized)
				return
			}
		}
		next.ServeHTTP(w, r)
	})
}

// SetupRoutes sets up the HTTP routes
func (h *Handler) SetupRoutes() *mux.Router {
	router := mux.NewRouter()

	// Apply auth middleware
	router.Use(h.authMiddleware)

	// File operations
	router.HandleFunc("/api/files/{path:.*}", h.handleFile).Methods("GET", "PUT", "DELETE", "HEAD")
	router.HandleFunc("/api/files/{path:.*}/size", h.handleFileSize).Methods("GET")

	// Peer operations
	router.HandleFunc("/api/peers", h.handlePeers).Methods("GET")
	router.HandleFunc("/api/peers/{peerID}", h.handlePeer).Methods("GET")
	router.HandleFunc("/api/peers/{peerID}/heartbeat", h.handleHeartbeat).Methods("POST")

	// Cache operations
	router.HandleFunc("/api/cache", h.handleCache).Methods("GET")
	router.HandleFunc("/api/cache/stats", h.handleCacheStats).Methods("GET")
	router.HandleFunc("/api/fs/snapshot", h.handleFSSnapshot).Methods("POST")
	router.HandleFunc("/api/fs/restore", h.handleFSRestore).Methods("POST")

	// Health check
	router.HandleFunc("/api/health", h.handleHealth).Methods("GET")
	router.HandleFunc("/api/netprobe", h.handleNetProbe).Methods("GET")
	router.HandleFunc("/metrics", h.handlePromMetrics).Methods("GET")

	return router
}

// sanitizePath validates and cleans a file path to prevent path traversal
func sanitizePath(raw string) (string, error) {
	cleaned := filepath.Clean("/" + raw)
	if strings.Contains(cleaned, "..") {
		return "", fmt.Errorf("invalid path: contains traversal")
	}
	return cleaned, nil
}

// handleFile handles file operations
func (h *Handler) handleFile(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	filePath, err := sanitizePath(vars["path"])
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	ctx := r.Context()

	switch r.Method {
	case "GET":
		h.handleFileGet(w, ctx, filePath)
	case "PUT":
		h.handleFilePut(w, r, ctx, filePath)
	case "DELETE":
		h.handleFileDelete(w, ctx, filePath)
	case "HEAD":
		h.handleFileHead(w, ctx, filePath)
	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

// handleFileGet handles GET requests for files
func (h *Handler) handleFileGet(w http.ResponseWriter, ctx context.Context, filePath string) {
	w.Header().Set("Content-Type", "application/octet-stream")

	// Use WriteTo to stream data — avoids buffering multi-GB chunked files
	n, err := h.cacheManager.WriteTo(ctx, filePath, w)
	if err != nil {
		if n == 0 {
			// Nothing written yet, safe to send error response
			h.logger.Printf("File not found: %s", filePath)
			http.Error(w, "File not found", http.StatusNotFound)
			return
		}
		// Partial write already happened, just log
		h.logger.Printf("Partial stream for %s: wrote %d bytes before error: %v", filePath, n, err)
		return
	}

	h.logger.Printf("Served file: %s (%d bytes)", filePath, n)
}

// handleFilePut handles PUT requests for files
func (h *Handler) handleFilePut(w http.ResponseWriter, r *http.Request, ctx context.Context, filePath string) {
	// Early rejection based on Content-Length header
	if r.ContentLength > maxUploadSize {
		http.Error(w, "File too large", http.StatusRequestEntityTooLarge)
		return
	}

	// Limit upload size
	r.Body = http.MaxBytesReader(w, r.Body, maxUploadSize)

	data, err := io.ReadAll(r.Body)
	if err != nil {
		if err.Error() == "http: request body too large" {
			http.Error(w, "File too large", http.StatusRequestEntityTooLarge)
			return
		}
		http.Error(w, "Failed to read request body", http.StatusBadRequest)
		return
	}

	entry := &cache.CacheEntry{
		FilePath:     filePath,
		StoragePath:  filePath,
		Size:         int64(len(data)),
		LastAccessed: time.Now(),
		Data:         data,
	}

	if err := h.cacheManager.Put(ctx, entry); err != nil {
		h.logger.Printf("Failed to store file: %s", err)
		http.Error(w, "Failed to store file", http.StatusInternalServerError)
		return
	}

	// Update file location in coordinator if available
	if h.coordinator != nil {
		location := &coordinator.FileLocation{
			FilePath:     filePath,
			PeerID:       h.peerID,
			StorageTier:  "nvme",
			StoragePath:  filePath,
			FileSize:     entry.Size,
			LastAccessed: time.Now(),
		}
		h.coordinator.UpdateFileLocation(ctx, location)
	}

	w.WriteHeader(http.StatusOK)
	h.logger.Printf("Stored file: %s (%d bytes)", filePath, len(data))
}

// handleFileDelete handles DELETE requests for files
func (h *Handler) handleFileDelete(w http.ResponseWriter, ctx context.Context, filePath string) {
	if err := h.cacheManager.Delete(ctx, filePath); err != nil {
		h.logger.Printf("Failed to delete file: %s", err)
		http.Error(w, "Failed to delete file", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	h.logger.Printf("Deleted file: %s", filePath)
}

// handleFileHead handles HEAD requests for files
func (h *Handler) handleFileHead(w http.ResponseWriter, ctx context.Context, filePath string) {
	entry, err := h.cacheManager.Get(ctx, filePath)
	if err != nil {
		http.Error(w, "File not found", http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Length", strconv.FormatInt(entry.Size, 10))
	w.WriteHeader(http.StatusOK)
}

// handleFileSize handles requests for file size
func (h *Handler) handleFileSize(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	filePath, err := sanitizePath(vars["path"])
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	ctx := r.Context()

	entry, err := h.cacheManager.Get(ctx, filePath)
	if err != nil {
		http.Error(w, "File not found", http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(entry.Size)
}

// handlePeers handles requests for peer list
func (h *Handler) handlePeers(w http.ResponseWriter, r *http.Request) {
	if h.coordinator == nil {
		http.Error(w, "Coordinator not available", http.StatusServiceUnavailable)
		return
	}

	ctx := r.Context()
	peers, err := h.coordinator.GetPeers(ctx, h.peerID)
	if err != nil {
		http.Error(w, "Failed to get peers", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(peers)
}

// handlePeer handles requests for specific peer info
func (h *Handler) handlePeer(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	peerID := vars["peerID"]

	if h.coordinator == nil {
		http.Error(w, "Coordinator not available", http.StatusServiceUnavailable)
		return
	}

	ctx := r.Context()
	peers, err := h.coordinator.GetPeers(ctx, "")
	if err != nil {
		http.Error(w, "Failed to get peers", http.StatusInternalServerError)
		return
	}

	for _, peer := range peers {
		if peer.ID == peerID {
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(peer)
			return
		}
	}

	http.Error(w, "Peer not found", http.StatusNotFound)
}

// handleHeartbeat handles heartbeat requests
func (h *Handler) handleHeartbeat(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	peerID := vars["peerID"]

	if h.coordinator == nil {
		http.Error(w, "Coordinator not available", http.StatusServiceUnavailable)
		return
	}

	var heartbeat struct {
		AvailableSpace     int64     `json:"available_space"`
		UsedSpace          int64     `json:"used_space"`
		Status             string    `json:"status"`
		NetworkSpeedMBps   float64   `json:"network_speed_mbps,omitempty"`
		NetworkLatencyMs   float64   `json:"network_latency_ms,omitempty"`
		NetworkProbeBytes  int64     `json:"network_probe_bytes,omitempty"`
		NetworkProbeTarget string    `json:"network_probe_target,omitempty"`
		NetworkProbedAt    time.Time `json:"network_probed_at,omitempty"`
	}

	if err := json.NewDecoder(r.Body).Decode(&heartbeat); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	ctx := r.Context()
	if updater, ok := h.coordinator.(coordinator.NetworkStatusUpdater); ok {
		metrics := &coordinator.PeerNetworkMetrics{
			SpeedMBps:   heartbeat.NetworkSpeedMBps,
			LatencyMs:   heartbeat.NetworkLatencyMs,
			ProbeBytes:  heartbeat.NetworkProbeBytes,
			ProbeTarget: strings.TrimSpace(heartbeat.NetworkProbeTarget),
			ProbedAt:    heartbeat.NetworkProbedAt,
		}
		err := updater.UpdatePeerStatusWithNetwork(ctx, peerID, heartbeat.Status, heartbeat.AvailableSpace, heartbeat.UsedSpace, metrics)
		if err != nil {
			http.Error(w, "Failed to update peer status", http.StatusInternalServerError)
			return
		}
	} else {
		err := h.coordinator.UpdatePeerStatus(ctx, peerID, heartbeat.Status, heartbeat.AvailableSpace, heartbeat.UsedSpace)
		if err != nil {
			http.Error(w, "Failed to update peer status", http.StatusInternalServerError)
			return
		}
	}

	w.WriteHeader(http.StatusOK)
}

// handleCache handles cache listing requests
func (h *Handler) handleCache(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	entries, err := h.cacheManager.List(ctx)
	if err != nil {
		http.Error(w, "Failed to list cache", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(entries)
}

// handleCacheStats handles cache statistics requests
func (h *Handler) handleCacheStats(w http.ResponseWriter, r *http.Request) {
	stats := make(map[string]interface{})

	// Add coordinator stats if available
	if h.coordinator != nil {
		for k, v := range h.coordinator.GetPeerStats() {
			stats[k] = v
		}
	}

	// Add local cache metrics if available
	if dcm, ok := h.cacheManager.(*cache.DefaultCacheManager); ok {
		used, capacity := dcm.Stats()
		stats["nvme_used"] = used
		stats["nvme_capacity"] = capacity
		for k, v := range dcm.GetMetrics() {
			stats[k] = v
		}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(stats)
}

func (h *Handler) handleFSSnapshot(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	req := fsSnapshotRequest{}
	if r.ContentLength > 0 {
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "Invalid request body", http.StatusBadRequest)
			return
		}
	}

	includeData := true
	if req.IncludeData != nil {
		includeData = *req.IncludeData
	}

	entries, err := h.cacheManager.List(ctx)
	if err != nil {
		http.Error(w, "Failed to list cache", http.StatusInternalServerError)
		return
	}

	sort.Slice(entries, func(i, j int) bool {
		return entries[i].FilePath < entries[j].FilePath
	})

	resp := fsSnapshot{
		Version:   1,
		CreatedAt: time.Now(),
		PeerID:    h.peerID,
		Prefix:    req.Prefix,
		Files:     make([]fsSnapshotFile, 0, len(entries)),
	}
	for _, entry := range entries {
		if entry == nil {
			continue
		}
		if req.Prefix != "" && !strings.HasPrefix(entry.FilePath, req.Prefix) {
			continue
		}
		if req.MaxFiles > 0 && len(resp.Files) >= req.MaxFiles {
			break
		}
		file := fsSnapshotFile{
			FilePath:     entry.FilePath,
			Size:         entry.Size,
			LastAccessed: entry.LastAccessed,
			IsChunked:    entry.IsChunked,
		}

		if includeData {
			var buf bytes.Buffer
			n, err := h.cacheManager.WriteTo(ctx, entry.FilePath, &buf)
			if err != nil {
				http.Error(w, fmt.Sprintf("Failed to snapshot file %s: %v", entry.FilePath, err), http.StatusInternalServerError)
				return
			}
			file.Size = n
			file.ContentB64 = base64.StdEncoding.EncodeToString(buf.Bytes())
		}

		resp.Files = append(resp.Files, file)
	}

	persistCloud := req.PersistCloud != nil && *req.PersistCloud
	if strings.TrimSpace(req.CloudPath) != "" {
		persistCloud = true
	}
	if persistCloud {
		cloudPath := strings.TrimSpace(req.CloudPath)
		if cloudPath == "" {
			cloudPath = fmt.Sprintf("snapshots/fs/%s/%d.json", h.peerID, time.Now().Unix())
		}
		store, ok := h.cacheManager.(cloudObjectStore)
		if !ok {
			http.Error(w, "Cloud snapshot persistence is not supported by cache manager", http.StatusNotImplemented)
			return
		}
		payload, err := json.Marshal(resp)
		if err != nil {
			http.Error(w, "Failed to serialize snapshot payload", http.StatusInternalServerError)
			return
		}
		if err := store.WriteCloud(ctx, cloudPath, payload); err != nil {
			http.Error(w, "Failed to persist snapshot to cloud", http.StatusInternalServerError)
			return
		}
		resp.PersistedToCloud = true
		resp.CloudPath = cloudPath
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

func (h *Handler) handleFSRestore(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "Failed to read request body", http.StatusBadRequest)
		return
	}

	req := fsRestoreRequest{}
	if err := json.Unmarshal(body, &req); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	cloudPath := strings.TrimSpace(req.CloudPath)
	if cloudPath != "" && len(req.Snapshot.Files) == 0 {
		store, ok := h.cacheManager.(cloudObjectStore)
		if !ok {
			http.Error(w, "Cloud snapshot restore is not supported by cache manager", http.StatusNotImplemented)
			return
		}
		snapData, err := store.ReadCloud(ctx, cloudPath)
		if err != nil {
			http.Error(w, "Failed to read snapshot from cloud", http.StatusInternalServerError)
			return
		}
		if err := json.Unmarshal(snapData, &req.Snapshot); err != nil {
			http.Error(w, "Invalid snapshot payload in cloud object", http.StatusBadRequest)
			return
		}
	}

	// Support posting snapshot payload directly (without wrapping in {"snapshot":...}).
	if len(req.Snapshot.Files) == 0 {
		var direct fsSnapshot
		if err := json.Unmarshal(body, &direct); err == nil && len(direct.Files) > 0 {
			req.Snapshot = direct
		}
	}
	if len(req.Snapshot.Files) == 0 {
		http.Error(w, "snapshot.files is required", http.StatusBadRequest)
		return
	}

	overwrite := true
	if req.Overwrite != nil {
		overwrite = *req.Overwrite
	}

	type restoreFailure struct {
		FilePath string `json:"file_path"`
		Error    string `json:"error"`
	}
	failures := make([]restoreFailure, 0)
	restored := 0
	skipped := 0

	for _, f := range req.Snapshot.Files {
		if strings.TrimSpace(f.FilePath) == "" {
			failures = append(failures, restoreFailure{FilePath: f.FilePath, Error: "file_path is required"})
			continue
		}
		safePath, err := sanitizePath(strings.TrimPrefix(f.FilePath, "/"))
		if err != nil {
			failures = append(failures, restoreFailure{FilePath: f.FilePath, Error: err.Error()})
			continue
		}
		if !overwrite {
			if _, err := h.cacheManager.Get(ctx, safePath); err == nil {
				skipped++
				continue
			}
		}

		if f.ContentB64 == "" {
			failures = append(failures, restoreFailure{FilePath: safePath, Error: "content_b64 is required for restore"})
			continue
		}
		data, err := base64.StdEncoding.DecodeString(f.ContentB64)
		if err != nil {
			failures = append(failures, restoreFailure{FilePath: safePath, Error: fmt.Sprintf("invalid content_b64: %v", err)})
			continue
		}
		lastAccessed := f.LastAccessed
		if lastAccessed.IsZero() {
			lastAccessed = time.Now()
		}

		entry := &cache.CacheEntry{
			FilePath:     safePath,
			StoragePath:  safePath,
			Size:         int64(len(data)),
			LastAccessed: lastAccessed,
			Data:         data,
		}
		if err := h.cacheManager.Put(ctx, entry); err != nil {
			failures = append(failures, restoreFailure{FilePath: safePath, Error: err.Error()})
			continue
		}
		restored++

		if h.coordinator != nil {
			_ = h.coordinator.UpdateFileLocation(ctx, &coordinator.FileLocation{
				FilePath:     safePath,
				PeerID:       h.peerID,
				StorageTier:  "nvme",
				StoragePath:  safePath,
				FileSize:     entry.Size,
				LastAccessed: time.Now(),
			})
		}
	}

	result := map[string]interface{}{
		"success":          len(failures) == 0,
		"restored_files":   restored,
		"skipped_files":    skipped,
		"failed_files":     len(failures),
		"overwrite":        overwrite,
		"snapshot_version": req.Snapshot.Version,
	}
	if cloudPath != "" {
		result["cloud_path"] = cloudPath
	}
	if len(failures) > 0 {
		result["failures"] = failures
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(result)
}

// handleHealth handles health check requests
func (h *Handler) handleHealth(w http.ResponseWriter, r *http.Request) {
	health := map[string]interface{}{
		"status":    "healthy",
		"timestamp": time.Now().Unix(),
		"peer_id":   h.peerID,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(health)
}

// handleNetProbe streams a fixed-size payload for network throughput probes.
func (h *Handler) handleNetProbe(w http.ResponseWriter, r *http.Request) {
	size := defaultNetProbeBytes
	if raw := strings.TrimSpace(r.URL.Query().Get("bytes")); raw != "" {
		v, err := strconv.Atoi(raw)
		if err == nil && v > 0 {
			size = v
		}
	}
	if size > maxNetProbeBytes {
		size = maxNetProbeBytes
	}
	if size < 4*1024 {
		size = 4 * 1024
	}

	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("X-Netprobe-Bytes", strconv.Itoa(size))
	w.Header().Set("Cache-Control", "no-store")

	buf := make([]byte, netProbeChunkSize)
	remaining := size
	for remaining > 0 {
		n := len(buf)
		if remaining < n {
			n = remaining
		}
		wrote, err := w.Write(buf[:n])
		if err != nil {
			return
		}
		remaining -= wrote
	}
}

// handlePromMetrics exposes cache/perf metrics in Prometheus text format.
func (h *Handler) handlePromMetrics(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")

	// Coordinator availability metric
	if h.coordinator != nil {
		fmt.Fprintln(w, "fuse_coordinator_available 1")
	} else {
		fmt.Fprintln(w, "fuse_coordinator_available 0")
	}

	dcm, ok := h.cacheManager.(*cache.DefaultCacheManager)
	if !ok {
		return
	}

	used, capacity := dcm.Stats()
	fmt.Fprintf(w, "fuse_nvme_used_bytes %d\n", used)
	fmt.Fprintf(w, "fuse_nvme_capacity_bytes %d\n", capacity)

	metrics := dcm.GetMetrics()
	writeMetricLine(w, "fuse_cache_nvme_hits_total", metrics["nvme_hits"])
	writeMetricLine(w, "fuse_cache_nvme_misses_total", metrics["nvme_misses"])
	writeMetricLine(w, "fuse_cache_peer_hits_total", metrics["peer_hits"])
	writeMetricLine(w, "fuse_cache_peer_misses_total", metrics["peer_misses"])
	writeMetricLine(w, "fuse_cache_cloud_hits_total", metrics["cloud_hits"])
	writeMetricLine(w, "fuse_cache_cloud_misses_total", metrics["cloud_misses"])
	writeMetricLine(w, "fuse_cache_nvme_read_bytes_total", metrics["nvme_read_bytes"])
	writeMetricLine(w, "fuse_cache_nvme_read_seconds_total", nanosToSeconds(metrics["nvme_read_nanos"]))
	writeMetricLine(w, "fuse_cache_nvme_read_ops_total", metrics["nvme_read_ops"])
	writeMetricLine(w, "fuse_cache_nvme_read_mbps", metrics["nvme_read_mbps"])
	writeMetricLine(w, "fuse_cache_peer_read_bytes_total", metrics["peer_read_bytes"])
	writeMetricLine(w, "fuse_cache_peer_read_seconds_total", nanosToSeconds(metrics["peer_read_nanos"]))
	writeMetricLine(w, "fuse_cache_peer_read_ops_total", metrics["peer_read_ops"])
	writeMetricLine(w, "fuse_cache_peer_read_mbps", metrics["peer_read_mbps"])
	writeMetricLine(w, "fuse_cache_cloud_read_bytes_total", metrics["cloud_read_bytes"])
	writeMetricLine(w, "fuse_cache_cloud_read_seconds_total", nanosToSeconds(metrics["cloud_read_nanos"]))
	writeMetricLine(w, "fuse_cache_cloud_read_ops_total", metrics["cloud_read_ops"])
	writeMetricLine(w, "fuse_cache_cloud_read_mbps", metrics["cloud_read_mbps"])
	writeMetricLine(w, "fuse_cache_read_wall_bytes_total", metrics["read_wall_bytes"])
	writeMetricLine(w, "fuse_cache_read_wall_seconds_total", nanosToSeconds(metrics["read_wall_nanos"]))
	writeMetricLine(w, "fuse_cache_read_wall_ops_total", metrics["read_wall_ops"])
	writeMetricLine(w, "fuse_cache_read_wall_mbps", metrics["read_wall_mbps"])
	writeMetricLine(w, "fuse_cache_nvme_read_wall_bytes_total", metrics["nvme_read_wall_bytes"])
	writeMetricLine(w, "fuse_cache_peer_read_wall_bytes_total", metrics["peer_read_wall_bytes"])
	writeMetricLine(w, "fuse_cache_cloud_read_wall_bytes_total", metrics["cloud_read_wall_bytes"])
	writeMetricLine(w, "fuse_cache_read_wall_other_bytes_total", metrics["read_wall_other_bytes"])
	writeMetricLine(w, "fuse_cache_nvme_read_wall_mbps", metrics["nvme_read_wall_mbps"])
	writeMetricLine(w, "fuse_cache_peer_read_wall_mbps", metrics["peer_read_wall_mbps"])
	writeMetricLine(w, "fuse_cache_cloud_read_wall_mbps", metrics["cloud_read_wall_mbps"])
	writeMetricLine(w, "fuse_cache_read_wall_other_mbps", metrics["read_wall_other_mbps"])
	writeMetricLine(w, "fuse_cache_write_count_total", metrics["write_count"])
	writeMetricLine(w, "fuse_cache_write_bytes_total", metrics["write_bytes"])
	writeMetricLine(w, "fuse_cache_evictions_total", metrics["eviction_count"])
}

func nanosToSeconds(value interface{}) float64 {
	switch v := value.(type) {
	case int64:
		return float64(v) / float64(time.Second)
	case int:
		return float64(v) / float64(time.Second)
	default:
		return 0
	}
}

func writeMetricLine(w io.Writer, name string, value interface{}) {
	switch v := value.(type) {
	case int:
		fmt.Fprintf(w, "%s %d\n", name, v)
	case int64:
		fmt.Fprintf(w, "%s %d\n", name, v)
	case float64:
		fmt.Fprintf(w, "%s %f\n", name, v)
	default:
		// omit non-numeric values
	}
}

// StartServer starts the HTTP API server
func (h *Handler) StartServer(ctx context.Context, port int) error {
	router := h.SetupRoutes()

	server := &http.Server{
		Addr:    fmt.Sprintf(":%d", port),
		Handler: router,
	}

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		server.Shutdown(shutdownCtx)
	}()

	h.logger.Printf("API server starting on port %d", port)
	return server.ListenAndServe()
}

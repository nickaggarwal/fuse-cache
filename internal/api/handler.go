package api

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"fuse-client/internal/cache"
	"fuse-client/internal/coordinator"

	"github.com/gorilla/mux"
)

const maxUploadSize = 2 * 1024 * 1024 * 1024 // 2GB

// Handler handles HTTP requests for the peer API
type Handler struct {
	cacheManager cache.CacheManager
	coordinator  coordinator.Coordinator
	peerID       string
	apiKey       string
	logger       *log.Logger
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
		if r.URL.Path == "/api/health" {
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

	// Health check
	router.HandleFunc("/api/health", h.handleHealth).Methods("GET")

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
		AvailableSpace int64  `json:"available_space"`
		UsedSpace      int64  `json:"used_space"`
		Status         string `json:"status"`
	}

	if err := json.NewDecoder(r.Body).Decode(&heartbeat); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	ctx := r.Context()
	err := h.coordinator.UpdatePeerStatus(ctx, peerID, heartbeat.Status, heartbeat.AvailableSpace, heartbeat.UsedSpace)
	if err != nil {
		http.Error(w, "Failed to update peer status", http.StatusInternalServerError)
		return
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

package api

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"strconv"
	"time"

	"fuse-client/internal/cache"
	"fuse-client/internal/coordinator"

	"github.com/gorilla/mux"
)

// Handler handles HTTP requests for the peer API
type Handler struct {
	cacheManager cache.CacheManager
	coordinator  *coordinator.CoordinatorService
	peerID       string
	logger       *log.Logger
}

// NewHandler creates a new API handler
func NewHandler(cacheManager cache.CacheManager, coordinator *coordinator.CoordinatorService, peerID string) *Handler {
	return &Handler{
		cacheManager: cacheManager,
		coordinator:  coordinator,
		peerID:       peerID,
		logger:       log.New(log.Writer(), "[API] ", log.LstdFlags),
	}
}

// SetupRoutes sets up the HTTP routes
func (h *Handler) SetupRoutes() *mux.Router {
	router := mux.NewRouter()

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

// handleFile handles file operations
func (h *Handler) handleFile(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	filePath := "/" + vars["path"]

	ctx := r.Context()

	switch r.Method {
	case "GET":
		h.handleFileGet(w, r, ctx, filePath)
	case "PUT":
		h.handleFilePut(w, r, ctx, filePath)
	case "DELETE":
		h.handleFileDelete(w, r, ctx, filePath)
	case "HEAD":
		h.handleFileHead(w, r, ctx, filePath)
	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

// handleFileGet handles GET requests for files
func (h *Handler) handleFileGet(w http.ResponseWriter, r *http.Request, ctx context.Context, filePath string) {
	entry, err := h.cacheManager.Get(ctx, filePath)
	if err != nil {
		h.logger.Printf("File not found: %s", filePath)
		http.Error(w, "File not found", http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Length", strconv.FormatInt(entry.Size, 10))
	w.Write(entry.Data)

	h.logger.Printf("Served file: %s (%d bytes)", filePath, entry.Size)
}

// handleFilePut handles PUT requests for files
func (h *Handler) handleFilePut(w http.ResponseWriter, r *http.Request, ctx context.Context, filePath string) {
	data, err := ioutil.ReadAll(r.Body)
	if err != nil {
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
func (h *Handler) handleFileDelete(w http.ResponseWriter, r *http.Request, ctx context.Context, filePath string) {
	if err := h.cacheManager.Delete(ctx, filePath); err != nil {
		h.logger.Printf("Failed to delete file: %s", err)
		http.Error(w, "Failed to delete file", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	h.logger.Printf("Deleted file: %s", filePath)
}

// handleFileHead handles HEAD requests for files
func (h *Handler) handleFileHead(w http.ResponseWriter, r *http.Request, ctx context.Context, filePath string) {
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
	filePath := "/" + vars["path"]

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
	if h.coordinator == nil {
		http.Error(w, "Coordinator not available", http.StatusServiceUnavailable)
		return
	}

	stats := h.coordinator.GetPeerStats()
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

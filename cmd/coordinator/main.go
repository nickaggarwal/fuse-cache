package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"fuse-client/internal/cache"
	"fuse-client/internal/coordinator"
	pb "fuse-client/internal/pb"

	"google.golang.org/grpc"
)

func main() {
	var (
		port     = flag.Int("port", 8080, "Port for the coordinator HTTP server")
		grpcPort = flag.Int("grpc-port", 9080, "Port for the coordinator gRPC server")
		help     = flag.Bool("help", false, "Show help")
	)
	flag.Parse()

	if *help {
		flag.Usage()
		return
	}

	logger := log.New(os.Stdout, "[COORDINATOR] ", log.LstdFlags)
	logger.Printf("Starting coordinator on HTTP port %d, gRPC port %d", *port, *grpcPort)

	// Create context for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Create coordinator service
	coordinatorService := coordinator.NewCoordinatorService()
	coordinatorService.Start(ctx)

	// Start gRPC server
	grpcListener, err := net.Listen("tcp", fmt.Sprintf(":%d", *grpcPort))
	if err != nil {
		logger.Fatalf("Failed to listen on gRPC port %d: %v", *grpcPort, err)
	}
	grpcSrv := grpc.NewServer()
	pb.RegisterCoordinatorServiceServer(grpcSrv, coordinator.NewGRPCServer(coordinatorService))

	go func() {
		logger.Printf("gRPC server listening on :%d", *grpcPort)
		if err := grpcSrv.Serve(grpcListener); err != nil {
			logger.Printf("gRPC server error: %v", err)
		}
	}()

	// Create HTTP server
	mux := http.NewServeMux()
	setupRoutes(mux, coordinatorService, newSnapshotCloudStoreFromEnv(logger))

	server := &http.Server{
		Addr:    fmt.Sprintf(":%d", *port),
		Handler: mux,
	}

	// Start server in goroutine
	go func() {
		if err := server.ListenAndServe(); err != http.ErrServerClosed {
			logger.Printf("HTTP server error: %v", err)
		}
	}()

	logger.Printf("Coordinator started on HTTP port %d, gRPC port %d", *port, *grpcPort)

	// Wait for interrupt signal to gracefully shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	logger.Println("Shutting down coordinator...")

	// Graceful shutdown
	cancel()
	grpcSrv.GracefulStop()

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		logger.Printf("Server shutdown error: %v", err)
	}

	// Save state before exiting
	if err := coordinatorService.SaveState(); err != nil {
		logger.Printf("Failed to save state: %v", err)
	}

	logger.Println("Coordinator stopped")
}

type snapshotCloudStore interface {
	Read(ctx context.Context, path string) ([]byte, error)
	Write(ctx context.Context, path string, data []byte) error
}

func envInt(name string, fallback int) int {
	v := strings.TrimSpace(os.Getenv(name))
	if v == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(v)
	if err != nil || parsed <= 0 {
		return fallback
	}
	return parsed
}

func envBool(name string, fallback bool) bool {
	v := strings.TrimSpace(os.Getenv(name))
	if v == "" {
		return fallback
	}
	parsed, err := strconv.ParseBool(v)
	if err != nil {
		return fallback
	}
	return parsed
}

func newSnapshotCloudStoreFromEnv(logger *log.Logger) snapshotCloudStore {
	provider := strings.ToLower(strings.TrimSpace(os.Getenv("CLOUD_PROVIDER")))
	if provider == "" {
		logger.Printf("Coordinator snapshot cloud store disabled: CLOUD_PROVIDER is empty")
		return nil
	}
	timeout := 30 * time.Second

	var (
		store cache.TierStorage
		err   error
	)

	switch provider {
	case "azure":
		account := strings.TrimSpace(os.Getenv("AZURE_STORAGE_ACCOUNT"))
		key := strings.TrimSpace(os.Getenv("AZURE_STORAGE_KEY"))
		container := strings.TrimSpace(os.Getenv("AZURE_CONTAINER_NAME"))
		if account == "" || key == "" || container == "" {
			logger.Printf("Coordinator snapshot cloud store disabled: missing Azure credentials or container")
			return nil
		}
		store, err = cache.NewAzureStorage(
			account,
			key,
			container,
			timeout,
			envInt("FUSE_AZURE_DOWNLOAD_CONCURRENCY", 8),
			int64(envInt("FUSE_AZURE_DOWNLOAD_BLOCK_SIZE_MB", 4))*1024*1024,
			int64(envInt("FUSE_AZURE_PARALLEL_DOWNLOAD_MIN_SIZE_MB", 8))*1024*1024,
		)
	case "gcp":
		bucket := strings.TrimSpace(os.Getenv("GCP_BUCKET"))
		if bucket == "" {
			logger.Printf("Coordinator snapshot cloud store disabled: GCP_BUCKET is empty")
			return nil
		}
		store, err = cache.NewGCPStorage(bucket, timeout)
	case "s3":
		store, err = cache.NewCloudStorageWithTuning(
			strings.TrimSpace(os.Getenv("S3_BUCKET")),
			strings.TrimSpace(os.Getenv("S3_REGION")),
			timeout,
			cache.S3TransferTuning{
				DownloadConcurrency: envInt("FUSE_S3_DOWNLOAD_CONCURRENCY", 32),
				DownloadPartSize:    int64(envInt("FUSE_S3_DOWNLOAD_PART_SIZE_MB", 8)) * 1024 * 1024,
				UploadConcurrency:   envInt("FUSE_S3_UPLOAD_CONCURRENCY", 16),
				UploadPartSize:      int64(envInt("FUSE_S3_UPLOAD_PART_SIZE_MB", 8)) * 1024 * 1024,
				Endpoint:            strings.TrimSpace(os.Getenv("S3_ENDPOINT")),
				ForcePathStyle:      envBool("FUSE_S3_FORCE_PATH_STYLE", false),
			},
		)
	default:
		logger.Printf("Coordinator snapshot cloud store disabled: unsupported provider %q", provider)
		return nil
	}
	if err != nil {
		logger.Printf("Coordinator snapshot cloud store initialization failed: %v", err)
		return nil
	}
	logger.Printf("Coordinator snapshot cloud store enabled for provider=%s", provider)
	return store
}

func setupRoutes(mux *http.ServeMux, coordinatorService *coordinator.CoordinatorService, snapshotStore snapshotCloudStore) {
	// Register peer endpoint
	mux.HandleFunc("/api/peers/register", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		var peer coordinator.PeerInfo
		if err := parseJSONRequest(r, &peer); err != nil {
			http.Error(w, "Invalid request", http.StatusBadRequest)
			return
		}

		if err := coordinatorService.RegisterPeer(r.Context(), &peer); err != nil {
			http.Error(w, "Failed to register peer", http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"success": true}`))
	})

	// Get peers endpoint
	mux.HandleFunc("/api/peers", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		requesterID := r.URL.Query().Get("requester_id")
		peers, err := coordinatorService.GetPeers(r.Context(), requesterID)
		if err != nil {
			http.Error(w, "Failed to get peers", http.StatusInternalServerError)
			return
		}

		writeJSONResponse(w, peers)
	})

	// Update peer status endpoint
	mux.HandleFunc("/api/peers/status", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		var request struct {
			PeerID             string    `json:"peer_id"`
			Status             string    `json:"status"`
			AvailableSpace     int64     `json:"available_space"`
			UsedSpace          int64     `json:"used_space"`
			NetworkSpeedMBps   float64   `json:"network_speed_mbps,omitempty"`
			NetworkLatencyMs   float64   `json:"network_latency_ms,omitempty"`
			NetworkProbeBytes  int64     `json:"network_probe_bytes,omitempty"`
			NetworkProbeTarget string    `json:"network_probe_target,omitempty"`
			NetworkProbedAt    time.Time `json:"network_probed_at,omitempty"`
		}

		if err := parseJSONRequest(r, &request); err != nil {
			http.Error(w, "Invalid request", http.StatusBadRequest)
			return
		}

		var metrics *coordinator.PeerNetworkMetrics
		if request.NetworkSpeedMBps > 0 || request.NetworkLatencyMs > 0 || request.NetworkProbeBytes > 0 || strings.TrimSpace(request.NetworkProbeTarget) != "" || !request.NetworkProbedAt.IsZero() {
			metrics = &coordinator.PeerNetworkMetrics{
				SpeedMBps:   request.NetworkSpeedMBps,
				LatencyMs:   request.NetworkLatencyMs,
				ProbeBytes:  request.NetworkProbeBytes,
				ProbeTarget: strings.TrimSpace(request.NetworkProbeTarget),
				ProbedAt:    request.NetworkProbedAt,
			}
		}
		err := coordinatorService.UpdatePeerStatusWithNetwork(r.Context(), request.PeerID, request.Status, request.AvailableSpace, request.UsedSpace, metrics)
		if err != nil {
			http.Error(w, "Failed to update peer status", http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"success": true}`))
	})

	// File location endpoint (GET and PUT)
	mux.HandleFunc("/api/files/location", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			filePath := r.URL.Query().Get("path")
			if filePath == "" {
				http.Error(w, "File path is required", http.StatusBadRequest)
				return
			}

			locations, err := coordinatorService.GetFileLocation(r.Context(), filePath)
			if err != nil {
				http.Error(w, "Failed to get file location", http.StatusInternalServerError)
				return
			}

			writeJSONResponse(w, locations)

		case http.MethodPut:
			var location coordinator.FileLocation
			if err := parseJSONRequest(r, &location); err != nil {
				http.Error(w, "Invalid request", http.StatusBadRequest)
				return
			}

			if err := coordinatorService.UpdateFileLocation(r.Context(), &location); err != nil {
				http.Error(w, "Failed to update file location", http.StatusInternalServerError)
				return
			}

			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"success": true}`))

		default:
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		}
	})

	// File metadata listing endpoint (GET)
	mux.HandleFunc("/api/files/locations", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		prefix := r.URL.Query().Get("prefix")
		locations, err := coordinatorService.ListFileLocations(r.Context(), prefix)
		if err != nil {
			http.Error(w, "Failed to list file locations", http.StatusInternalServerError)
			return
		}

		writeJSONResponse(w, locations)
	})

	// Full world view endpoint
	mux.HandleFunc("/api/worldview", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		prefix := r.URL.Query().Get("prefix")
		view, err := coordinatorService.GetWorldView(r.Context(), prefix)
		if err != nil {
			http.Error(w, "Failed to build world view", http.StatusInternalServerError)
			return
		}
		writeJSONResponse(w, view)
	})

	// Seed a file path to a percentage of active peers.
	mux.HandleFunc("/api/cache/seed", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var req coordinator.SeedCacheRequest
		if err := parseJSONRequest(r, &req); err != nil {
			http.Error(w, "Invalid request", http.StatusBadRequest)
			return
		}
		result, err := coordinatorService.SeedPathToPeers(r.Context(), req)
		if err != nil {
			http.Error(w, fmt.Sprintf("Failed to seed path: %v", err), http.StatusBadRequest)
			return
		}
		writeJSONResponse(w, result)
	})

	// Snapshot coordinator metadata state to disk.
	mux.HandleFunc("/api/snapshot", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var req struct {
			Path         string `json:"path"`
			PersistCloud *bool  `json:"persist_cloud,omitempty"`
			CloudPath    string `json:"cloud_path,omitempty"`
		}
		if r.ContentLength > 0 {
			if err := parseJSONRequest(r, &req); err != nil {
				http.Error(w, "Invalid request", http.StatusBadRequest)
				return
			}
		}
		path := req.Path
		if path == "" {
			path = "coordinator_state.json"
		}
		if err := coordinatorService.SaveStateToPath(path); err != nil {
			http.Error(w, "Failed to create snapshot", http.StatusInternalServerError)
			return
		}

		persistCloud := (req.PersistCloud != nil && *req.PersistCloud) || strings.TrimSpace(req.CloudPath) != ""
		cloudPath := strings.TrimSpace(req.CloudPath)
		if persistCloud {
			if snapshotStore == nil {
				http.Error(w, "Cloud snapshot store is not configured", http.StatusNotImplemented)
				return
			}
			if cloudPath == "" {
				cloudPath = fmt.Sprintf("snapshots/coordinator/%d.json", time.Now().Unix())
			}
			data, err := coordinatorService.SnapshotState()
			if err != nil {
				http.Error(w, "Failed to serialize snapshot", http.StatusInternalServerError)
				return
			}
			if err := snapshotStore.Write(r.Context(), cloudPath, data); err != nil {
				http.Error(w, "Failed to persist snapshot to cloud", http.StatusInternalServerError)
				return
			}
		}

		resp := map[string]interface{}{
			"success": true,
			"path":    path,
		}
		if persistCloud {
			resp["persisted_to_cloud"] = true
			resp["cloud_path"] = cloudPath
		}
		writeJSONResponse(w, resp)
	})

	// Restore coordinator metadata state from disk.
	mux.HandleFunc("/api/restore", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var req struct {
			Path      string `json:"path"`
			CloudPath string `json:"cloud_path,omitempty"`
		}
		if err := parseJSONRequest(r, &req); err != nil {
			http.Error(w, "Invalid request", http.StatusBadRequest)
			return
		}

		cloudPath := strings.TrimSpace(req.CloudPath)
		if cloudPath != "" {
			if snapshotStore == nil {
				http.Error(w, "Cloud snapshot store is not configured", http.StatusNotImplemented)
				return
			}
			data, err := snapshotStore.Read(r.Context(), cloudPath)
			if err != nil {
				http.Error(w, "Failed to read cloud snapshot", http.StatusInternalServerError)
				return
			}
			if err := coordinatorService.RestoreState(data); err != nil {
				http.Error(w, "Failed to restore cloud snapshot", http.StatusInternalServerError)
				return
			}
			if strings.TrimSpace(req.Path) != "" {
				_ = os.WriteFile(req.Path, data, 0644)
			}
			writeJSONResponse(w, map[string]interface{}{
				"success":    true,
				"cloud_path": cloudPath,
				"path":       strings.TrimSpace(req.Path),
			})
			return
		}
		if req.Path == "" {
			http.Error(w, "path is required", http.StatusBadRequest)
			return
		}
		if err := coordinatorService.LoadStateFromPath(req.Path); err != nil {
			http.Error(w, "Failed to restore snapshot", http.StatusInternalServerError)
			return
		}
		writeJSONResponse(w, map[string]interface{}{
			"success": true,
			"path":    req.Path,
		})
	})

	// Get statistics endpoint
	mux.HandleFunc("/api/stats", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		stats := coordinatorService.GetPeerStats()
		writeJSONResponse(w, stats)
	})

	// Health check endpoint
	mux.HandleFunc("/api/health", func(w http.ResponseWriter, r *http.Request) {
		health := map[string]interface{}{
			"status":    "healthy",
			"timestamp": time.Now().Unix(),
			"service":   "coordinator",
		}
		writeJSONResponse(w, health)
	})
}

func parseJSONRequest(r *http.Request, v interface{}) error {
	return json.NewDecoder(r.Body).Decode(v)
}

func writeJSONResponse(w http.ResponseWriter, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(v); err != nil {
		log.Printf("Failed to write response: %v", err)
	}
}

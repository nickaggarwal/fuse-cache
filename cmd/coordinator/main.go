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

	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
)

func main() {
	var (
		port             = flag.Int("port", 8080, "Port for the coordinator HTTP server")
		grpcPort         = flag.Int("grpc-port", 9080, "Port for the coordinator gRPC server")
		etcdEndpoints    = flag.String("etcd-endpoints", "", "Comma-separated etcd endpoints (e.g. http://etcd-0:2379,http://etcd-1:2379). If empty, uses in-memory state.")
		etcdPrefix       = flag.String("etcd-prefix", "/fuse", "etcd key prefix for coordinator state")
		etcdLeaseTTL     = flag.Int("etcd-peer-lease-ttl", 30, "Peer liveness lease TTL in seconds (etcd-backed only)")
		etcdDialTimeout  = flag.Duration("etcd-dial-timeout", 5*time.Second, "etcd dial timeout")
		help             = flag.Bool("help", false, "Show help")
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

	// Build the coordinator service, optionally backed by etcd.
	store, err := buildStore(ctx, *etcdEndpoints, *etcdPrefix, int64(*etcdLeaseTTL), *etcdDialTimeout, logger)
	if err != nil {
		logger.Fatalf("Failed to build coordinator store: %v", err)
	}
	coordinatorService := coordinator.NewCoordinatorServiceWithStore(store)
	coordinatorService.Start(ctx)
	defer func() {
		if err := store.Close(); err != nil {
			logger.Printf("Store close: %v", err)
		}
	}()

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

// buildStore returns an etcd-backed store if endpoints are configured, otherwise
// the legacy in-memory store. Endpoints may be comma-separated.
func buildStore(ctx context.Context, endpoints, prefix string, leaseTTL int64, dialTimeout time.Duration, logger *log.Logger) (coordinator.Store, error) {
	endpoints = strings.TrimSpace(endpoints)
	if endpoints == "" {
		logger.Printf("Coordinator store: in-memory (set -etcd-endpoints for HA)")
		return coordinator.NewInMemoryStore(), nil
	}
	eps := make([]string, 0)
	for _, ep := range strings.Split(endpoints, ",") {
		if e := strings.TrimSpace(ep); e != "" {
			eps = append(eps, e)
		}
	}
	if len(eps) == 0 {
		return nil, fmt.Errorf("etcd endpoints flag set but no valid endpoints parsed: %q", endpoints)
	}
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   eps,
		DialTimeout: dialTimeout,
		Context:     ctx,
	})
	if err != nil {
		return nil, fmt.Errorf("etcd client: %w", err)
	}
	logger.Printf("Coordinator store: etcd endpoints=%v prefix=%s peer-lease-ttl=%ds", eps, prefix, leaseTTL)
	return coordinator.NewEtcdStore(cli, coordinator.EtcdStoreConfig{
		Prefix:          prefix,
		PeerLeaseTTLSec: leaseTTL,
	}), nil
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

	// Snapshot: write a manifest of the file paths the coordinator currently
	// tracks (path + size + chunking info) to local disk and/or cloud. This is
	// a portable cache-restore plan, not a full state dump.
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
			path = "coordinator_manifest.json"
		}

		manifest, err := coordinatorService.BuildManifest(r.Context())
		if err != nil {
			http.Error(w, "Failed to build manifest", http.StatusInternalServerError)
			return
		}
		data, err := json.MarshalIndent(manifest, "", "  ")
		if err != nil {
			http.Error(w, "Failed to serialize manifest", http.StatusInternalServerError)
			return
		}

		tmp := path + ".tmp"
		if err := os.WriteFile(tmp, data, 0644); err != nil {
			http.Error(w, "Failed to write manifest", http.StatusInternalServerError)
			return
		}
		if err := os.Rename(tmp, path); err != nil {
			http.Error(w, "Failed to publish manifest", http.StatusInternalServerError)
			return
		}

		persistCloud := (req.PersistCloud != nil && *req.PersistCloud) || strings.TrimSpace(req.CloudPath) != ""
		cloudPath := strings.TrimSpace(req.CloudPath)
		if persistCloud {
			if snapshotStore == nil {
				http.Error(w, "Cloud store is not configured", http.StatusNotImplemented)
				return
			}
			if cloudPath == "" {
				cloudPath = fmt.Sprintf("manifests/coordinator/%d.json", time.Now().Unix())
			}
			if err := snapshotStore.Write(r.Context(), cloudPath, data); err != nil {
				http.Error(w, "Failed to persist manifest to cloud", http.StatusInternalServerError)
				return
			}
		}

		resp := map[string]interface{}{
			"success":     true,
			"path":        path,
			"total_files": len(manifest.Files),
		}
		if persistCloud {
			resp["persisted_to_cloud"] = true
			resp["cloud_path"] = cloudPath
		}
		writeJSONResponse(w, resp)
	})

	// Restore: read a manifest from disk or cloud, then for each file fetch the
	// bytes from cloud (durable tier) and seed them onto a percentage of active
	// peers, matching SeedPathToPeers semantics. Chunked files are fetched and
	// seeded chunk-by-chunk using the <path>_chunk_<i> key convention.
	mux.HandleFunc("/api/restore", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var req struct {
			Path           string `json:"path"`
			CloudPath      string `json:"cloud_path,omitempty"`
			SeedPercentage int    `json:"seed_percentage,omitempty"`
			TimeoutSeconds int    `json:"timeout_seconds,omitempty"`
			Concurrency    int    `json:"concurrency,omitempty"`
		}
		if err := parseJSONRequest(r, &req); err != nil {
			http.Error(w, "Invalid request", http.StatusBadRequest)
			return
		}

		var manifestBytes []byte
		cloudPath := strings.TrimSpace(req.CloudPath)
		switch {
		case cloudPath != "":
			if snapshotStore == nil {
				http.Error(w, "Cloud store is not configured", http.StatusNotImplemented)
				return
			}
			b, err := snapshotStore.Read(r.Context(), cloudPath)
			if err != nil {
				http.Error(w, "Failed to read cloud manifest", http.StatusInternalServerError)
				return
			}
			manifestBytes = b
		case strings.TrimSpace(req.Path) != "":
			b, err := os.ReadFile(req.Path)
			if err != nil {
				http.Error(w, "Failed to read local manifest", http.StatusInternalServerError)
				return
			}
			manifestBytes = b
		default:
			http.Error(w, "path or cloud_path is required", http.StatusBadRequest)
			return
		}

		var manifest coordinator.FileManifest
		if err := json.Unmarshal(manifestBytes, &manifest); err != nil {
			http.Error(w, "Invalid manifest JSON", http.StatusBadRequest)
			return
		}

		if snapshotStore == nil {
			http.Error(w, "Cloud store is not configured (required to fetch file bytes)", http.StatusNotImplemented)
			return
		}
		result, err := coordinatorService.RestoreFromManifest(r.Context(), &manifest, snapshotStore, coordinator.RestoreOptions{
			SeedPercentage: req.SeedPercentage,
			TimeoutSeconds: req.TimeoutSeconds,
			Concurrency:    req.Concurrency,
		})
		if err != nil {
			http.Error(w, fmt.Sprintf("Restore failed: %v", err), http.StatusInternalServerError)
			return
		}
		writeJSONResponse(w, result)
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

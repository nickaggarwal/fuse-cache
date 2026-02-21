package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/exec"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"fuse-client/internal/api"
	"fuse-client/internal/cache"
	"fuse-client/internal/coordinator"
	fusefs "fuse-client/internal/fuse"
	pb "fuse-client/internal/pb"

	fusepkg "bazil.org/fuse"
	"google.golang.org/grpc"
)

func main() {
	var (
		mountPoint      = flag.String("mount", "/tmp/fuse-client", "Mount point for FUSE filesystem")
		nvmePath        = flag.String("nvme", "/tmp/nvme-cache", "Path for NVME cache storage")
		coordinatorAddr = flag.String("coordinator", "localhost:8080", "Coordinator HTTP address")
		coordinatorGRPC = flag.String("coordinator-grpc", "localhost:9080", "Coordinator gRPC address")
		peerPort        = flag.Int("port", 8081, "Port for peer HTTP API server")
		grpcPort        = flag.Int("grpc-port", 9081, "Port for peer gRPC server")
		peerID          = flag.String("peer-id", "", "Peer ID (auto-generated if not provided)")
		chunkSizeMB     = flag.Int("chunk-size", 8, "Chunk size in MB")
		apiKey          = flag.String("api-key", "", "API key for authentication (optional)")

		// Cloud provider selection
		cloudProvider = flag.String("cloud-provider", "s3", "Cloud storage provider: s3, azure, or gcp")

		// S3 config
		s3Bucket = flag.String("s3-bucket", "fuse-client-cache", "S3 bucket name")
		s3Region = flag.String("s3-region", "us-east-1", "S3 region")

		// Azure config
		azureAccount   = flag.String("azure-account", "", "Azure storage account name")
		azureKey       = flag.String("azure-key", "", "Azure storage account key")
		azureContainer = flag.String("azure-container", "fuse-cache", "Azure blob container name")

		// GCP config
		gcpBucket = flag.String("gcp-bucket", "fuse-client-cache", "GCS bucket name")

		// NVMe capacity
		nvmeMaxGB       = flag.Int("nvme-max-gb", 10, "Maximum NVMe cache size in GB")
		peerMaxGB       = flag.Int("peer-max-gb", 5, "File size threshold in GB for peer-first reads; larger files prefer cloud-first")
		peerReadMBps    = flag.Int64("peer-read-mbps", 200, "Assumed per-peer read throughput in MB/s for hybrid peer+cloud read decisions")
		parallelReads   = flag.Int("parallel-range-reads", 8, "Parallel workers for multi-chunk range reads")
		prefetchChunks  = flag.Int("range-prefetch-chunks", 2, "How many sequential chunks to prefetch")
		rangeChunkCache = flag.Int("range-chunk-cache-size", 16, "Max cached chunks per file for range reads")
		mountRetries    = flag.Int("mount-retries", 8, "Number of retries for FUSE mount recovery")
		mountDelayS     = flag.Int("mount-retry-delay-sec", 2, "Base delay in seconds between FUSE mount retries")

		help = flag.Bool("help", false, "Show help")
	)
	flag.Parse()

	if *help {
		flag.Usage()
		return
	}

	// Generate peer ID if not provided
	if *peerID == "" {
		*peerID = fmt.Sprintf("peer-%d", time.Now().Unix())
	}

	// Override from env vars if set
	if v := os.Getenv("AZURE_STORAGE_ACCOUNT"); v != "" && *azureAccount == "" {
		*azureAccount = v
	}
	if v := os.Getenv("AZURE_STORAGE_KEY"); v != "" && *azureKey == "" {
		*azureKey = v
	}
	if v := os.Getenv("AZURE_CONTAINER_NAME"); v != "" && *azureContainer == "" {
		*azureContainer = v
	}
	if v := os.Getenv("GCP_BUCKET"); v != "" && *gcpBucket == "fuse-client-cache" {
		*gcpBucket = v
	}
	if v := os.Getenv("COORDINATOR_GRPC_ADDR"); v != "" {
		*coordinatorGRPC = v
	}

	logger := log.New(os.Stdout, "[CLIENT] ", log.LstdFlags)
	logger.Printf("Starting FUSE client with ID: %s", *peerID)

	// Create context for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Create gRPC coordinator client
	var coordClient coordinator.Coordinator
	var grpcCoordClient *coordinator.GRPCCoordinatorClient
	if *coordinatorGRPC != "" {
		var err error
		grpcCoordClient, err = coordinator.NewGRPCCoordinatorClient(*coordinatorGRPC)
		if err != nil {
			logger.Printf("WARNING: Failed to create gRPC coordinator client: %v, falling back to HTTP", err)
			coordClient = coordinator.NewCoordinatorClient(*coordinatorAddr, 10*time.Second)
		} else {
			coordClient = grpcCoordClient
			logger.Printf("Using gRPC coordinator client at %s", *coordinatorGRPC)
		}
	} else if *coordinatorAddr != "" {
		coordClient = coordinator.NewCoordinatorClient(*coordinatorAddr, 10*time.Second)
	}

	// Create cache configuration
	cacheConfig := &cache.CacheConfig{
		NVMePath:            *nvmePath,
		MaxNVMeSize:         int64(*nvmeMaxGB) * 1024 * 1024 * 1024,
		MaxPeerSize:         int64(*peerMaxGB) * 1024 * 1024 * 1024,
		PeerTimeout:         30 * time.Second,
		CloudTimeout:        60 * time.Second,
		CoordinatorAddr:     *coordinatorAddr,
		Coordinator:         coordClient,
		ChunkSize:           int64(*chunkSizeMB) * 1024 * 1024,
		ParallelRangeReads:  *parallelReads,
		RangePrefetchChunks: *prefetchChunks,
		RangeChunkCacheSize: *rangeChunkCache,

		CloudProvider:                 *cloudProvider,
		S3Bucket:                      *s3Bucket,
		S3Region:                      *s3Region,
		AzureStorageAccount:           *azureAccount,
		AzureStorageKey:               *azureKey,
		AzureContainerName:            *azureContainer,
		GCPBucket:                     *gcpBucket,
		LocalPeerID:                   *peerID,
		PeerReadThroughputBytesPerSec: *peerReadMBps * 1024 * 1024,
	}
	if v := os.Getenv("FUSE_PEER_SIZE"); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil && n > 0 {
			cacheConfig.MaxPeerSize = n
		} else if err != nil {
			logger.Printf("WARNING: invalid FUSE_PEER_SIZE value %q: %v", v, err)
		}
	}
	if v := os.Getenv("FUSE_PEER_READ_MBPS"); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil && n > 0 {
			cacheConfig.PeerReadThroughputBytesPerSec = n * 1024 * 1024
		} else if err != nil {
			logger.Printf("WARNING: invalid FUSE_PEER_READ_MBPS value %q: %v", v, err)
		}
	}
	if v := os.Getenv("FUSE_PARALLEL_RANGE_READS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			cacheConfig.ParallelRangeReads = n
			*parallelReads = n
		} else if err != nil {
			logger.Printf("WARNING: invalid FUSE_PARALLEL_RANGE_READS value %q: %v", v, err)
		}
	}
	if v := os.Getenv("FUSE_RANGE_PREFETCH_CHUNKS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 0 {
			cacheConfig.RangePrefetchChunks = n
			*prefetchChunks = n
		} else if err != nil {
			logger.Printf("WARNING: invalid FUSE_RANGE_PREFETCH_CHUNKS value %q: %v", v, err)
		}
	}
	if v := os.Getenv("FUSE_RANGE_CHUNK_CACHE_SIZE"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			cacheConfig.RangeChunkCacheSize = n
			*rangeChunkCache = n
		} else if err != nil {
			logger.Printf("WARNING: invalid FUSE_RANGE_CHUNK_CACHE_SIZE value %q: %v", v, err)
		}
	}
	if v := os.Getenv("FUSE_MOUNT_RETRIES"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			*mountRetries = n
		} else if err != nil {
			logger.Printf("WARNING: invalid FUSE_MOUNT_RETRIES value %q: %v", v, err)
		}
	}
	if v := os.Getenv("FUSE_MOUNT_RETRY_DELAY_SEC"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			*mountDelayS = n
		} else if err != nil {
			logger.Printf("WARNING: invalid FUSE_MOUNT_RETRY_DELAY_SEC value %q: %v", v, err)
		}
	}

	// Initialize cache manager
	cacheManager, err := cache.NewCacheManager(cacheConfig)
	if err != nil {
		logger.Fatalf("Failed to create cache manager: %v", err)
	}

	// Start peer gRPC server
	grpcListener, err := net.Listen("tcp", fmt.Sprintf(":%d", *grpcPort))
	if err != nil {
		logger.Fatalf("Failed to listen on gRPC port %d: %v", *grpcPort, err)
	}
	grpcSrv := grpc.NewServer()
	pb.RegisterPeerServiceServer(grpcSrv, cache.NewPeerGRPCServer(cacheManager))

	go func() {
		logger.Printf("Peer gRPC server listening on :%d", *grpcPort)
		if err := grpcSrv.Serve(grpcListener); err != nil {
			logger.Printf("Peer gRPC server error: %v", err)
		}
	}()

	// Register this peer with the coordinator
	if coordClient != nil {
		go registerPeer(ctx, coordClient, *peerID, *peerPort, *grpcPort, *nvmePath, cacheManager, logger)
	}

	// Create API handler
	apiHandler := api.NewHandler(cacheManager, coordClient, *peerID, *apiKey)

	// Start API server
	go func() {
		if err := apiHandler.StartServer(ctx, *peerPort); err != nil {
			logger.Printf("API server error: %v", err)
		}
	}()

	// Create FUSE filesystem
	filesystem := fusefs.NewFileSystem(cacheManager)

	// Mount the FUSE filesystem
	logger.Printf("Mounting FUSE filesystem at: %s", *mountPoint)
	conn, err := mountWithRetry(ctx, filesystem, *mountPoint, *mountRetries, time.Duration(*mountDelayS)*time.Second, logger)
	if err != nil {
		logger.Fatalf("Failed to mount FUSE filesystem: %v", err)
	}

	// Start serving the filesystem in a goroutine
	go func() {
		if err := filesystem.Serve(ctx, conn); err != nil {
			logger.Printf("FUSE filesystem server error: %v", err)
		}
	}()

	logger.Printf("Client started successfully")
	logger.Printf("- Peer ID: %s", *peerID)
	logger.Printf("- Mount point: %s", *mountPoint)
	logger.Printf("- NVME cache: %s", *nvmePath)
	logger.Printf("- HTTP API port: %d", *peerPort)
	logger.Printf("- gRPC port: %d", *grpcPort)
	logger.Printf("- Coordinator HTTP: %s", *coordinatorAddr)
	logger.Printf("- Coordinator gRPC: %s", *coordinatorGRPC)
	logger.Printf("- Cloud provider: %s", *cloudProvider)
	logger.Printf("- Peer read threshold bytes: %d", cacheConfig.MaxPeerSize)
	logger.Printf("- Assumed per-peer read throughput MB/s: %d", *peerReadMBps)
	logger.Printf("- Parallel range reads: %d", cacheConfig.ParallelRangeReads)
	logger.Printf("- Range prefetch chunks: %d", cacheConfig.RangePrefetchChunks)
	logger.Printf("- Range chunk cache size: %d", cacheConfig.RangeChunkCacheSize)
	logger.Printf("- FUSE mount retries: %d", *mountRetries)
	logger.Printf("- FUSE mount retry delay sec: %d", *mountDelayS)

	// Wait for interrupt signal to gracefully shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	logger.Println("Shutting down client...")

	// Graceful shutdown
	cancel()
	grpcSrv.GracefulStop()

	if grpcCoordClient != nil {
		grpcCoordClient.Close()
	}

	// Unmount the FUSE filesystem
	if err := filesystem.Unmount(*mountPoint); err != nil {
		logger.Printf("Failed to unmount filesystem: %v", err)
	} else {
		logger.Printf("Unmounted filesystem from: %s", *mountPoint)
	}

	logger.Println("Client stopped")
}

func registerPeer(ctx context.Context, coordClient coordinator.Coordinator, peerID string, port, grpcPort int, nvmePath string, cm cache.CacheManager, logger *log.Logger) {
	used, capacity := cm.Stats()
	availableSpace := capacity - used
	usedSpace := used

	// Use POD_IP env var if available (Kubernetes), otherwise detect local IP
	host := os.Getenv("POD_IP")
	if host == "" {
		host = getLocalIP()
	}

	peerInfo := &coordinator.PeerInfo{
		ID:             peerID,
		Address:        fmt.Sprintf("%s:%d", host, port),
		GRPCAddress:    fmt.Sprintf("%s:%d", host, grpcPort),
		NVMePath:       nvmePath,
		AvailableSpace: availableSpace,
		UsedSpace:      usedSpace,
		Status:         "active",
	}

	// Keep retrying registration until success or shutdown.
	for attempt := 1; ; attempt++ {
		if err := coordClient.RegisterPeer(ctx, peerInfo); err != nil {
			wait := time.Duration(minInt(attempt, 10)) * 2 * time.Second
			logger.Printf("Failed to register peer (attempt %d): %v; retrying in %v", attempt, err, wait)
			select {
			case <-ctx.Done():
				return
			case <-time.After(wait):
				continue
			}
		}
		logger.Printf("Peer registered successfully at %s (gRPC: %s)", peerInfo.Address, peerInfo.GRPCAddress)
		break
	}

	// Send periodic heartbeats
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			used, capacity := cm.Stats()
			if err := coordClient.UpdatePeerStatus(ctx, peerID, "active", capacity-used, used); err != nil {
				logger.Printf("Failed to update peer status: %v (attempting re-register)", err)
				peerInfo.AvailableSpace = capacity - used
				peerInfo.UsedSpace = used
				if regErr := coordClient.RegisterPeer(ctx, peerInfo); regErr != nil {
					logger.Printf("Re-register failed for peer %s: %v", peerID, regErr)
				}
			}
		}
	}
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func getLocalIP() string {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return "localhost"
	}
	for _, addr := range addrs {
		if ipNet, ok := addr.(*net.IPNet); ok && !ipNet.IP.IsLoopback() && ipNet.IP.To4() != nil {
			return ipNet.IP.String()
		}
	}
	return "localhost"
}

func mountWithRetry(ctx context.Context, filesystem *fusefs.FileSystem, mountPoint string, retries int, baseDelay time.Duration, logger *log.Logger) (*fusepkg.Conn, error) {
	if retries < 1 {
		retries = 1
	}
	if baseDelay <= 0 {
		baseDelay = 2 * time.Second
	}

	var lastErr error
	for attempt := 1; attempt <= retries; attempt++ {
		if err := cleanupStaleMountpoint(ctx, mountPoint, logger); err != nil {
			logger.Printf("Mount cleanup warning for %s: %v", mountPoint, err)
		}

		conn, err := filesystem.Mount(ctx, mountPoint)
		if err == nil {
			return conn, nil
		}
		lastErr = err
		logger.Printf("FUSE mount attempt %d/%d failed at %s: %v", attempt, retries, mountPoint, err)

		if attempt == retries {
			break
		}

		wait := time.Duration(attempt) * baseDelay
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(wait):
		}
	}
	return nil, fmt.Errorf("mount retries exhausted for %s: %w", mountPoint, lastErr)
}

func cleanupStaleMountpoint(ctx context.Context, mountPoint string, logger *log.Logger) error {
	commands := [][]string{
		{"fusermount3", "-u", "-z", mountPoint},
		{"fusermount", "-u", "-z", mountPoint},
		{"umount", "-l", mountPoint},
	}

	var lastErr error
	for _, cmdArgs := range commands {
		cmd := exec.CommandContext(ctx, cmdArgs[0], cmdArgs[1:]...)
		out, err := cmd.CombinedOutput()
		if err != nil {
			lastErr = err
			output := strings.TrimSpace(string(out))
			if output != "" {
				logger.Printf("Mount cleanup command failed: %s (%v): %s", strings.Join(cmdArgs, " "), err, output)
			}
			continue
		}
		lastErr = nil
	}

	if err := os.MkdirAll(mountPoint, 0755); err != nil {
		return err
	}
	return lastErr
}

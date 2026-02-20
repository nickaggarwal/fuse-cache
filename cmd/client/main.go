package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"fuse-client/internal/api"
	"fuse-client/internal/cache"
	"fuse-client/internal/coordinator"
	"fuse-client/internal/fuse"
)

func main() {
	var (
		mountPoint      = flag.String("mount", "/tmp/fuse-client", "Mount point for FUSE filesystem")
		nvmePath        = flag.String("nvme", "/tmp/nvme-cache", "Path for NVME cache storage")
		coordinatorAddr = flag.String("coordinator", "localhost:8080", "Coordinator address")
		peerPort        = flag.Int("port", 8081, "Port for peer API server")
		peerID          = flag.String("peer-id", "", "Peer ID (auto-generated if not provided)")
		chunkSizeMB     = flag.Int("chunk-size", 4, "Chunk size in MB")
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
		nvmeMaxGB = flag.Int("nvme-max-gb", 10, "Maximum NVMe cache size in GB")

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

	logger := log.New(os.Stdout, "[CLIENT] ", log.LstdFlags)
	logger.Printf("Starting FUSE client with ID: %s", *peerID)

	// Create context for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Create cache configuration
	cacheConfig := &cache.CacheConfig{
		NVMePath:        *nvmePath,
		MaxNVMeSize:     int64(*nvmeMaxGB) * 1024 * 1024 * 1024,
		MaxPeerSize:     5 * 1024 * 1024 * 1024, // 5GB
		PeerTimeout:     30 * time.Second,
		CloudTimeout:    60 * time.Second,
		CoordinatorAddr: *coordinatorAddr,
		ChunkSize:       int64(*chunkSizeMB) * 1024 * 1024,

		CloudProvider:       *cloudProvider,
		S3Bucket:            *s3Bucket,
		S3Region:            *s3Region,
		AzureStorageAccount: *azureAccount,
		AzureStorageKey:     *azureKey,
		AzureContainerName:  *azureContainer,
		GCPBucket:           *gcpBucket,
	}

	// Initialize cache manager
	cacheManager, err := cache.NewCacheManager(cacheConfig)
	if err != nil {
		logger.Fatalf("Failed to create cache manager: %v", err)
	}

	// Create coordinator client (HTTP-based, talks to remote coordinator)
	var coordClient coordinator.Coordinator
	if *coordinatorAddr != "" {
		coordClient = coordinator.NewCoordinatorClient(*coordinatorAddr, 10*time.Second)

		// Register this peer with the remote coordinator
		go registerPeer(ctx, coordClient, *peerID, *peerPort, *nvmePath, cacheManager, logger)
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
	filesystem := fuse.NewFileSystem(cacheManager)

	// Mount the FUSE filesystem
	logger.Printf("Mounting FUSE filesystem at: %s", *mountPoint)
	conn, err := filesystem.Mount(ctx, *mountPoint)
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
	logger.Printf("- API port: %d", *peerPort)
	logger.Printf("- Coordinator: %s", *coordinatorAddr)
	logger.Printf("- Cloud provider: %s", *cloudProvider)

	// Wait for interrupt signal to gracefully shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	logger.Println("Shutting down client...")

	// Graceful shutdown
	cancel()

	// Unmount the FUSE filesystem
	if err := filesystem.Unmount(*mountPoint); err != nil {
		logger.Printf("Failed to unmount filesystem: %v", err)
	} else {
		logger.Printf("Unmounted filesystem from: %s", *mountPoint)
	}

	logger.Println("Client stopped")
}

func registerPeer(ctx context.Context, coordClient coordinator.Coordinator, peerID string, port int, nvmePath string, cm cache.CacheManager, logger *log.Logger) {
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
		NVMePath:       nvmePath,
		AvailableSpace: availableSpace,
		UsedSpace:      usedSpace,
		Status:         "active",
	}

	// Retry registration with backoff
	for attempt := 0; attempt < 5; attempt++ {
		if err := coordClient.RegisterPeer(ctx, peerInfo); err != nil {
			logger.Printf("Failed to register peer (attempt %d): %v", attempt+1, err)
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Duration(attempt+1) * 2 * time.Second):
				continue
			}
		}
		logger.Printf("Peer registered successfully at %s", peerInfo.Address)
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
				logger.Printf("Failed to update peer status: %v", err)
			}
		}
	}
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

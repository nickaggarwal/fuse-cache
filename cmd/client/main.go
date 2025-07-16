package main

import (
	"context"
	"flag"
	"fmt"
	"log"
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
		help            = flag.Bool("help", false, "Show help")
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

	logger := log.New(os.Stdout, "[CLIENT] ", log.LstdFlags)
	logger.Printf("Starting FUSE client with ID: %s", *peerID)

	// Create context for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Create cache configuration
	cacheConfig := &cache.CacheConfig{
		NVMePath:        *nvmePath,
		MaxNVMeSize:     10 * 1024 * 1024 * 1024, // 10GB
		MaxPeerSize:     5 * 1024 * 1024 * 1024,  // 5GB
		PeerTimeout:     30 * time.Second,
		CloudTimeout:    60 * time.Second,
		CoordinatorAddr: *coordinatorAddr,
	}

	// Initialize cache manager
	cacheManager, err := cache.NewCacheManager(cacheConfig)
	if err != nil {
		logger.Fatalf("Failed to create cache manager: %v", err)
	}

	// Create coordinator service (optional for clients)
	var coordinatorService *coordinator.CoordinatorService
	if *coordinatorAddr != "" {
		coordinatorService = coordinator.NewCoordinatorService()
		coordinatorService.Start(ctx)

		// Register this peer with the coordinator
		go registerPeer(ctx, coordinatorService, *peerID, *peerPort, *nvmePath, logger)
	}

	// Create API handler
	apiHandler := api.NewHandler(cacheManager, coordinatorService, *peerID)

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

	// Add a test file to the cache
	go func() {
		// Wait a bit for the filesystem to be ready
		time.Sleep(2 * time.Second)

		// Add a test file
		testFilePath := "/test-file.txt"
		testData := []byte("Hello, FUSE!")

		entry := &cache.CacheEntry{
			FilePath:     testFilePath,
			Size:         int64(len(testData)),
			LastAccessed: time.Now(),
			Data:         testData,
		}

		if err := cacheManager.Put(ctx, entry); err != nil {
			logger.Printf("Failed to put test file: %v", err)
		} else {
			logger.Printf("Test file stored in cache: %s", testFilePath)
		}
	}()

	logger.Printf("Client started successfully")
	logger.Printf("- Peer ID: %s", *peerID)
	logger.Printf("- Mount point: %s", *mountPoint)
	logger.Printf("- NVME cache: %s", *nvmePath)
	logger.Printf("- API port: %d", *peerPort)
	logger.Printf("- Coordinator: %s", *coordinatorAddr)

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

func registerPeer(ctx context.Context, coordinatorService *coordinator.CoordinatorService, peerID string, port int, nvmePath string, logger *log.Logger) {
	// Calculate available space (simplified)
	availableSpace := int64(10 * 1024 * 1024 * 1024) // 10GB
	usedSpace := int64(0)

	peerInfo := &coordinator.PeerInfo{
		ID:             peerID,
		Address:        fmt.Sprintf("localhost:%d", port),
		NVMePath:       nvmePath,
		AvailableSpace: availableSpace,
		UsedSpace:      usedSpace,
		Status:         "active",
	}

	// Register peer
	if err := coordinatorService.RegisterPeer(ctx, peerInfo); err != nil {
		logger.Printf("Failed to register peer: %v", err)
		return
	}

	logger.Printf("Peer registered successfully")

	// Send periodic heartbeats
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// Update peer status
			if err := coordinatorService.UpdatePeerStatus(ctx, peerID, "active", availableSpace, usedSpace); err != nil {
				logger.Printf("Failed to update peer status: %v", err)
			}
		}
	}
}

func calculateDiskUsage(path string) (int64, int64) {
	// Simplified disk usage calculation
	// In a real implementation, you would use syscall.Statfs or similar
	return 10 * 1024 * 1024 * 1024, 0 // 10GB available, 0 used
}

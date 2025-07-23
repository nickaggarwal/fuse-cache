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
	"fuse-client/internal/fuse"
	
	fusepkg "bazil.org/fuse"
)

func main() {
	var (
		mountPoint      = flag.String("mount", "/tmp/enhanced-fuse", "Mount point for enhanced FUSE filesystem")
		nvmePath        = flag.String("nvme", "/tmp/enhanced-nvme-cache", "Path for NVME cache storage")
		coordinatorAddr = flag.String("coordinator", "localhost:8080", "Coordinator address")
		peerPort        = flag.Int("port", 8091, "Port for peer API server")
		peerID          = flag.String("peer-id", "", "Peer ID (auto-generated if not provided)")
		enhanced        = flag.Bool("enhanced", true, "Use enhanced filesystem with in-memory directory structure")
		consistencyLevel = flag.String("consistency", "session", "Consistency level: eventual, session, strong")
		help            = flag.Bool("help", false, "Show help")
	)
	flag.Parse()

	if *help {
		flag.Usage()
		fmt.Println("\nEnhanced FUSE Client Features:")
		fmt.Println("- In-memory directory structure for instant metadata operations")
		fmt.Println("- Smart caching with configurable consistency levels")
		fmt.Println("- Background consistency validation")
		fmt.Println("- Small file data caching")
		fmt.Println("- Performance metrics and monitoring")
		return
	}

	// Generate peer ID if not provided
	if *peerID == "" {
		*peerID = fmt.Sprintf("enhanced-peer-%d", time.Now().Unix())
	}

	logger := log.New(os.Stdout, "[ENHANCED_CLIENT] ", log.LstdFlags)
	logger.Printf("Starting Enhanced FUSE client with ID: %s", *peerID)

	// Create context for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Create base cache configuration
	baseCacheConfig := &cache.CacheConfig{
		NVMePath:        *nvmePath,
		MaxNVMeSize:     10 * 1024 * 1024 * 1024, // 10GB
		MaxPeerSize:     5 * 1024 * 1024 * 1024,  // 5GB
		PeerTimeout:     30 * time.Second,
		CloudTimeout:    60 * time.Second,
		CoordinatorAddr: *coordinatorAddr,
		PeerID:          *peerID,
	}

	var cacheManager cache.CacheManager
	var err error

	if *enhanced {
		// Create enhanced cache configuration
		enhancedConfig := cache.DefaultEnhancedCacheConfig(baseCacheConfig)
		
		// Parse consistency level
		switch *consistencyLevel {
		case "eventual":
			enhancedConfig.ConsistencyLevel = cache.EventualConsistency
		case "session":
			enhancedConfig.ConsistencyLevel = cache.SessionConsistency
		case "strong":
			enhancedConfig.ConsistencyLevel = cache.StrongConsistency
		default:
			logger.Printf("Unknown consistency level: %s, using session", *consistencyLevel)
			enhancedConfig.ConsistencyLevel = cache.SessionConsistency
		}

		// Create enhanced cache manager
		enhancedCacheManager, err := cache.NewEnhancedCacheManager(enhancedConfig)
		if err != nil {
			logger.Fatalf("Failed to create enhanced cache manager: %v", err)
		}
		cacheManager = enhancedCacheManager

		logger.Printf("Enhanced cache manager created with consistency level: %s", *consistencyLevel)
	} else {
		// Create standard cache manager
		cacheManager, err = cache.NewCacheManager(baseCacheConfig)
		if err != nil {
			logger.Fatalf("Failed to create cache manager: %v", err)
		}
		logger.Printf("Standard cache manager created")
	}

	// Create API handler
	apiHandler := api.NewHandler(cacheManager, nil, *peerID)

	// Start API server
	go func() {
		if err := apiHandler.StartServer(ctx, *peerPort); err != nil {
			logger.Printf("API server error: %v", err)
		}
	}()

	var filesystem interface {
		Mount(ctx context.Context, mountPoint string) (*fusepkg.Conn, error)
		Serve(ctx context.Context, conn *fusepkg.Conn) error
	}

	if *enhanced {
		// Create enhanced FUSE filesystem
		enhancedFS := fuse.NewEnhancedFileSystem(cacheManager, &fuse.EnhancedFSConfig{
			SmallFileCacheThreshold: 64 * 1024,        // 64KB
			MetadataCacheTTL:       30 * time.Second,
			DataCacheTTL:           5 * time.Second,
			MaxConcurrentReads:     10,
		})
		filesystem = enhancedFS
		logger.Printf("Enhanced FUSE filesystem created")
	} else {
		// Create standard FUSE filesystem
		filesystem = fuse.NewFileSystem(cacheManager)
		logger.Printf("Standard FUSE filesystem created")
	}

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

	// Add test files to demonstrate the system
	go func() {
		time.Sleep(2 * time.Second)
		createTestFiles(cacheManager, logger)
	}()

	// Start performance monitoring
	if *enhanced {
		go monitorPerformance(cacheManager, logger)
	}

	logger.Printf("Enhanced client started successfully")
	logger.Printf("- Peer ID: %s", *peerID)
	logger.Printf("- Mount point: %s", *mountPoint)
	logger.Printf("- NVME cache: %s", *nvmePath)
	logger.Printf("- API port: %d", *peerPort)
	logger.Printf("- Coordinator: %s", *coordinatorAddr)
	logger.Printf("- Enhanced mode: %v", *enhanced)
	if *enhanced {
		logger.Printf("- Consistency level: %s", *consistencyLevel)
	}

	// Wait for interrupt signal to gracefully shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	logger.Println("Shutting down enhanced client...")
	
	// Cancel the main context to stop goroutines
	cancel()

	// Shutdown cache manager
	logger.Println("Shutting down cache manager...")
	cacheManager.Shutdown()

	// Unmount the FUSE filesystem
	logger.Printf("Unmounting FUSE filesystem from: %s", *mountPoint)
	if unmounter, ok := filesystem.(interface{ Unmount(string) error }); ok {
		if err := unmounter.Unmount(*mountPoint); err != nil {
			logger.Printf("Failed to unmount filesystem: %v", err)
		} else {
			logger.Printf("Successfully unmounted filesystem")
		}
	} else {
		logger.Printf("Filesystem does not support unmounting")
	}

	logger.Println("Enhanced client stopped")
}

// createTestFiles creates some test files to demonstrate the system
func createTestFiles(cacheManager cache.CacheManager, logger *log.Logger) {
	ctx := context.Background()
	
	testFiles := []struct {
		path string
		data string
		size string
	}{
		{"/small-file.txt", "This is a small test file for caching", "small"},
		{"/medium-file.txt", generateData(1024), "medium"},  // 1KB
		{"/large-file.dat", generateData(100*1024), "large"}, // 100KB
		{"/config.json", `{"enhanced": true, "cache_size": "64KB"}`, "config"},
		{"/readme.md", "# Enhanced FUSE Client\n\nThis demonstrates the enhanced filesystem features.", "readme"},
	}

	for _, file := range testFiles {
		entry := &cache.CacheEntry{
			FilePath:     file.path,
			Size:         int64(len(file.data)),
			LastAccessed: time.Now(),
			Data:         []byte(file.data),
		}

		if err := cacheManager.Put(ctx, entry); err != nil {
			logger.Printf("Failed to create test file %s: %v", file.path, err)
		} else {
			logger.Printf("Created test file: %s (%s, %d bytes)", file.path, file.size, len(file.data))
		}
	}

	logger.Printf("Test files created. You can now:")
	logger.Printf("  ls %s", "/tmp/enhanced-fuse")
	logger.Printf("  cat %s/small-file.txt", "/tmp/enhanced-fuse")
	logger.Printf("  head %s/large-file.dat", "/tmp/enhanced-fuse")
}

// generateData generates test data of specified size
func generateData(size int) string {
	data := make([]byte, size)
	for i := 0; i < size; i++ {
		data[i] = byte('A' + (i % 26))
	}
	return string(data)
}

// monitorPerformance monitors and reports performance metrics
func monitorPerformance(cacheManager cache.CacheManager, logger *log.Logger) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			// Try to get stats if available
			if statsProvider, ok := cacheManager.(interface{ GetStats() map[string]interface{} }); ok {
				stats := statsProvider.GetStats()
				logger.Printf("=== Performance Statistics ===")
				
				if cacheHits, ok := stats["cache_hits"].(int64); ok {
					logger.Printf("Cache hits: %d", cacheHits)
				}
				if cacheMisses, ok := stats["cache_misses"].(int64); ok {
					logger.Printf("Cache misses: %d", cacheMisses)
				}
				if instantLookups, ok := stats["instant_lookups"].(int64); ok {
					logger.Printf("Instant lookups: %d", instantLookups)
				}
				if hitRatio, ok := stats["hit_ratio"].(float64); ok {
					logger.Printf("Hit ratio: %.2f%%", hitRatio*100)
				}
				
				logger.Printf("=============================")
			}
		}
	}
}

// Note: Unmount is now handled directly by the filesystem instance 
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/exec"
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
		PeerID:          *peerID,
	}

	// Initialize cache manager
	cacheManager, err := cache.NewCacheManager(cacheConfig)
	if err != nil {
		logger.Fatalf("Failed to create cache manager: %v", err)
	}

	// Register this peer with the coordinator
	if *coordinatorAddr != "" {
		go registerPeer(ctx, *coordinatorAddr, *peerID, *peerPort, *nvmePath, logger)
	}

	// Create API handler
	apiHandler := api.NewHandler(cacheManager, nil, *peerID)

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
	
	// Graceful shutdown with timeout
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer shutdownCancel()

	// Cancel the main context to stop goroutines
	cancel()

	// Shutdown cache manager and snapshot sync
	logger.Println("Shutting down cache manager...")
	cacheManager.Shutdown()

	// Unregister from coordinator
	if *coordinatorAddr != "" {
		logger.Println("Unregistering from coordinator...")
		if err := unregisterPeerHTTP(shutdownCtx, *coordinatorAddr, *peerID); err != nil {
			logger.Printf("Failed to unregister from coordinator: %v", err)
		} else {
			logger.Println("Successfully unregistered from coordinator")
		}
	}

	// Unmount the FUSE filesystem with retries
	logger.Printf("Unmounting FUSE filesystem from: %s", *mountPoint)
	if err := unmountWithRetries(shutdownCtx, filesystem, *mountPoint, logger); err != nil {
		logger.Printf("Failed to unmount filesystem after retries: %v", err)
		// Force unmount as last resort
		logger.Println("Attempting force unmount...")
		forceUnmount(*mountPoint, logger)
	} else {
		logger.Printf("Successfully unmounted filesystem from: %s", *mountPoint)
	}

	logger.Println("Client stopped")
}

func registerPeer(ctx context.Context, coordinatorAddr string, peerID string, port int, nvmePath string, logger *log.Logger) {
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

	// Register peer via HTTP API
	if err := registerPeerHTTP(ctx, coordinatorAddr, peerInfo); err != nil {
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
			// Update peer status via HTTP API
			if err := updatePeerStatusHTTP(ctx, coordinatorAddr, peerID, "active", availableSpace, usedSpace); err != nil {
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

// registerPeerHTTP registers a peer with the coordinator via HTTP
func registerPeerHTTP(ctx context.Context, coordinatorAddr string, peerInfo *coordinator.PeerInfo) error {
	url := fmt.Sprintf("http://%s/api/peers/register", coordinatorAddr)
	
	jsonData, err := json.Marshal(peerInfo)
	if err != nil {
		return err
	}
	
	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewBuffer(jsonData))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	
	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("coordinator returned status: %d", resp.StatusCode)
	}
	
	return nil
}

// unregisterPeerHTTP unregisters a peer from the coordinator via HTTP
func unregisterPeerHTTP(ctx context.Context, coordinatorAddr string, peerID string) error {
	url := fmt.Sprintf("http://%s/api/peers/unregister", coordinatorAddr)
	
	unregisterData := map[string]interface{}{
		"peer_id": peerID,
	}
	
	jsonData, err := json.Marshal(unregisterData)
	if err != nil {
		return err
	}
	
	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewBuffer(jsonData))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	
	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("coordinator returned status: %d", resp.StatusCode)
	}
	
	return nil
}

// unmountWithRetries attempts to unmount the filesystem with multiple retries
func unmountWithRetries(ctx context.Context, filesystem *fuse.FileSystem, mountPoint string, logger *log.Logger) error {
	maxRetries := 3
	retryDelay := 2 * time.Second
	
	for i := 0; i < maxRetries; i++ {
		if i > 0 {
			logger.Printf("Unmount attempt %d/%d...", i+1, maxRetries)
		}
		
		err := filesystem.Unmount(mountPoint)
		if err == nil {
			return nil
		}
		
		logger.Printf("Unmount attempt %d failed: %v", i+1, err)
		
		// If this is the last attempt, don't wait
		if i == maxRetries-1 {
			return err
		}
		
		// Wait before retry, but check context cancellation
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(retryDelay):
			// Continue to next retry
		}
	}
	
	return fmt.Errorf("failed to unmount after %d retries", maxRetries)
}

// forceUnmount attempts to force unmount the filesystem using system commands
func forceUnmount(mountPoint string, logger *log.Logger) {
	// Try fusermount -uz (lazy unmount)
	cmd := exec.Command("fusermount", "-uz", mountPoint)
	if err := cmd.Run(); err != nil {
		logger.Printf("fusermount -uz failed: %v", err)
		
		// Try umount -l (lazy unmount)
		cmd = exec.Command("umount", "-l", mountPoint)
		if err := cmd.Run(); err != nil {
			logger.Printf("umount -l failed: %v", err)
		} else {
			logger.Printf("Force unmount with umount -l successful")
		}
	} else {
		logger.Printf("Force unmount with fusermount -uz successful")
	}
}

// updatePeerStatusHTTP updates peer status with the coordinator via HTTP
func updatePeerStatusHTTP(ctx context.Context, coordinatorAddr string, peerID string, status string, availableSpace, usedSpace int64) error {
	url := fmt.Sprintf("http://%s/api/peers/status", coordinatorAddr)
	
	statusData := map[string]interface{}{
		"peer_id":         peerID,
		"status":          status,
		"available_space": availableSpace,
		"used_space":      usedSpace,
	}
	
	jsonData, err := json.Marshal(statusData)
	if err != nil {
		return err
	}
	
	req, err := http.NewRequestWithContext(ctx, "PUT", url, bytes.NewBuffer(jsonData))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	
	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("coordinator returned status: %d", resp.StatusCode)
	}
	
	return nil
}

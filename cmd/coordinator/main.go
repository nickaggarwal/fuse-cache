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
	"syscall"
	"time"

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
	setupRoutes(mux, coordinatorService)

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

func setupRoutes(mux *http.ServeMux, coordinatorService *coordinator.CoordinatorService) {
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
			PeerID         string `json:"peer_id"`
			Status         string `json:"status"`
			AvailableSpace int64  `json:"available_space"`
			UsedSpace      int64  `json:"used_space"`
		}

		if err := parseJSONRequest(r, &request); err != nil {
			http.Error(w, "Invalid request", http.StatusBadRequest)
			return
		}

		err := coordinatorService.UpdatePeerStatus(r.Context(), request.PeerID, request.Status, request.AvailableSpace, request.UsedSpace)
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

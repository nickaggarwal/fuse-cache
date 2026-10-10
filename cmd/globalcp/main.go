// Command globalcp runs the global control plane: the service that makes a
// published subset of each cluster's cache discoverable to other clusters.
//
// It is deployable in its own Kubernetes cluster, reachable by every
// participating cluster's coordinator. It never holds file bytes and never
// connects to a cluster -- coordinators dial out to it and hold the stream,
// so a private cluster needs no inbound rule.
//
// SECURITY: -allow-insecure-identity lets a caller name its own cluster via
// a header. That is for the embedded/dev mode only. Until mTLS lands
// (stage 4), do not expose this service on a network you do not control.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"google.golang.org/grpc"

	"fuse-client/internal/globalcp"
	pb "fuse-client/internal/pb"
)

func main() {
	var (
		port          = flag.Int("port", 8090, "Port for the read-only HTTP observability server")
		grpcPort      = flag.Int("grpc-port", 9090, "Port for the GlobalControlPlane gRPC server")
		allowInsecure = flag.Bool("allow-insecure-identity", false, "Accept header-declared cluster identity on connections with no verified client certificate (DEV ONLY: any caller can then claim any cluster ID)")
		maxPrefixes   = flag.Int("max-prefixes", 64, "Maximum publish/subscribe prefixes one cluster may register")
		help          = flag.Bool("help", false, "Show help")
	)
	flag.Parse()
	if *help {
		flag.Usage()
		return
	}

	logger := log.New(os.Stdout, "[GLOBALCP] ", log.LstdFlags)

	// Flag wins, env is the default -- one rule for every new setting.
	insecureIdentity := *allowInsecure
	if !insecureIdentity && strings.EqualFold(strings.TrimSpace(os.Getenv("FUSE_GLOBALCP_ALLOW_INSECURE_IDENTITY")), "true") {
		insecureIdentity = true
	}

	svc := globalcp.NewService(globalcp.Config{
		Store:                 globalcp.NewMemStore(),
		Logger:                logger,
		MaxPublishPrefixes:    *maxPrefixes,
		AllowInsecureIdentity: insecureIdentity,
	})
	if insecureIdentity {
		logger.Printf("WARNING: insecure identity enabled — any caller may claim any cluster ID. Do not expose this on an untrusted network.")
	}

	grpcListener, err := net.Listen("tcp", fmt.Sprintf(":%d", *grpcPort))
	if err != nil {
		logger.Fatalf("Failed to listen on gRPC port %d: %v", *grpcPort, err)
	}
	grpcSrv := grpc.NewServer()
	pb.RegisterGlobalControlPlaneServer(grpcSrv, svc)
	go func() {
		logger.Printf("gRPC server listening on :%d", *grpcPort)
		if err := grpcSrv.Serve(grpcListener); err != nil {
			logger.Printf("gRPC server error: %v", err)
		}
	}()

	mux := http.NewServeMux()
	globalcp.RegisterRoutes(mux, svc)
	server := &http.Server{Addr: fmt.Sprintf(":%d", *port), Handler: mux}
	go func() {
		if err := server.ListenAndServe(); err != http.ErrServerClosed {
			logger.Printf("HTTP server error: %v", err)
		}
	}()

	logger.Printf("Global control plane started on HTTP :%d, gRPC :%d", *port, *grpcPort)

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	logger.Println("Shutting down global control plane...")
	grpcSrv.GracefulStop()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		logger.Printf("Server shutdown error: %v", err)
	}
	if err := svc.Store().Close(); err != nil {
		logger.Printf("Store close: %v", err)
	}
	logger.Println("Global control plane stopped")
}

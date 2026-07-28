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

	"fuse-client/internal/csidriver"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc"
)

func main() {
	var (
		endpoint    = flag.String("endpoint", "unix:///csi/csi.sock", "CSI gRPC endpoint")
		nodeID      = flag.String("node-id", "", "Node ID (required)")
		agentSocket = flag.String("agent-socket", "/var/run/fuse-client/agent.sock", "Agent gRPC socket path")
		fuseRoot    = flag.String("fuse-root", "/host/mnt/fuse", "Host path of the FUSE mount")
		driverName  = flag.String("driver-name", "fuse.csi.storage.io", "CSI driver name")
	)
	flag.Parse()

	if *nodeID == "" {
		*nodeID = os.Getenv("NODE_NAME")
	}
	if *nodeID == "" {
		log.Fatal("--node-id or NODE_NAME is required")
	}

	logger := log.New(os.Stdout, "[CSI] ", log.LstdFlags)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	driver := csidriver.New(*driverName, *nodeID, *agentSocket, *fuseRoot, logger)

	// Parse endpoint: expect "unix:///path"
	socketPath, err := parseEndpoint(*endpoint)
	if err != nil {
		log.Fatalf("Invalid endpoint %q: %v", *endpoint, err)
	}

	// Remove stale socket.
	os.Remove(socketPath)

	lis, err := net.Listen("unix", socketPath)
	if err != nil {
		log.Fatalf("Listen on %s: %v", socketPath, err)
	}

	srv := grpc.NewServer()
	csi.RegisterIdentityServer(srv, driver)
	csi.RegisterNodeServer(srv, driver)

	go func() {
		<-ctx.Done()
		srv.GracefulStop()
	}()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigChan
		logger.Println("Shutting down CSI driver...")
		cancel()
	}()

	logger.Printf("CSI driver %s starting on %s (node=%s)", *driverName, socketPath, *nodeID)
	if err := srv.Serve(lis); err != nil {
		log.Fatalf("gRPC server failed: %v", err)
	}
}

func parseEndpoint(endpoint string) (string, error) {
	const prefix = "unix://"
	if len(endpoint) <= len(prefix) {
		return "", fmt.Errorf("expected unix:///path, got %q", endpoint)
	}
	if endpoint[:len(prefix)] != prefix {
		return "", fmt.Errorf("only unix:// endpoints supported, got %q", endpoint)
	}
	return endpoint[len(prefix):], nil
}

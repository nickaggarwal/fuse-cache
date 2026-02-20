package coordinator

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	pb "fuse-client/internal/pb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func startTestGRPCServer(t *testing.T) (pb.CoordinatorServiceClient, func()) {
	t.Helper()

	service := NewCoordinatorService()
	srv := grpc.NewServer()
	pb.RegisterCoordinatorServiceServer(srv, NewGRPCServer(service))

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}

	go srv.Serve(lis)

	conn, err := grpc.Dial(lis.Addr().String(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		srv.Stop()
		t.Fatalf("failed to dial: %v", err)
	}

	client := pb.NewCoordinatorServiceClient(conn)
	cleanup := func() {
		conn.Close()
		srv.Stop()
	}
	return client, cleanup
}

func TestGRPC_RegisterAndGetPeers(t *testing.T) {
	client, cleanup := startTestGRPCServer(t)
	defer cleanup()

	ctx := context.Background()

	// Register a peer
	_, err := client.RegisterPeer(ctx, &pb.RegisterPeerRequest{
		Peer: &pb.PeerInfo{
			Id:          "peer-1",
			Address:     "10.0.0.1:8081",
			GrpcAddress: "10.0.0.1:9081",
			NvmePath:    "/mnt/nvme",
			Status:      "active",
		},
	})
	if err != nil {
		t.Fatalf("RegisterPeer: %v", err)
	}

	// Get peers
	resp, err := client.GetPeers(ctx, &pb.GetPeersRequest{RequesterId: "other"})
	if err != nil {
		t.Fatalf("GetPeers: %v", err)
	}

	if len(resp.Peers) != 1 {
		t.Fatalf("expected 1 peer, got %d", len(resp.Peers))
	}
	if resp.Peers[0].Id != "peer-1" {
		t.Errorf("peer ID = %q, want %q", resp.Peers[0].Id, "peer-1")
	}
	if resp.Peers[0].GrpcAddress != "10.0.0.1:9081" {
		t.Errorf("grpc_address = %q, want %q", resp.Peers[0].GrpcAddress, "10.0.0.1:9081")
	}
}

func TestGRPC_UpdatePeerStatus(t *testing.T) {
	client, cleanup := startTestGRPCServer(t)
	defer cleanup()

	ctx := context.Background()

	// Register
	client.RegisterPeer(ctx, &pb.RegisterPeerRequest{
		Peer: &pb.PeerInfo{Id: "peer-1", Address: "10.0.0.1:8081"},
	})

	// Update status
	_, err := client.UpdatePeerStatus(ctx, &pb.UpdatePeerStatusRequest{
		PeerId:         "peer-1",
		Status:         "active",
		AvailableSpace: 1000,
		UsedSpace:      500,
	})
	if err != nil {
		t.Fatalf("UpdatePeerStatus: %v", err)
	}
}

func TestGRPC_FileLocation(t *testing.T) {
	client, cleanup := startTestGRPCServer(t)
	defer cleanup()

	ctx := context.Background()

	// Update file location
	_, err := client.UpdateFileLocation(ctx, &pb.UpdateFileLocationRequest{
		Location: &pb.FileLocation{
			FilePath:    "/test.txt",
			PeerId:      "peer-1",
			StorageTier: "nvme",
			StoragePath: "/mnt/nvme/test.txt",
			FileSize:    1024,
		},
	})
	if err != nil {
		t.Fatalf("UpdateFileLocation: %v", err)
	}

	// Get file location
	resp, err := client.GetFileLocation(ctx, &pb.GetFileLocationRequest{FilePath: "/test.txt"})
	if err != nil {
		t.Fatalf("GetFileLocation: %v", err)
	}

	if len(resp.Locations) != 1 {
		t.Fatalf("expected 1 location, got %d", len(resp.Locations))
	}
	if resp.Locations[0].PeerId != "peer-1" {
		t.Errorf("peer_id = %q, want %q", resp.Locations[0].PeerId, "peer-1")
	}
	if resp.Locations[0].FileSize != 1024 {
		t.Errorf("file_size = %d, want 1024", resp.Locations[0].FileSize)
	}
}

func TestGRPC_GetPeerStats(t *testing.T) {
	client, cleanup := startTestGRPCServer(t)
	defer cleanup()

	ctx := context.Background()

	// Register peers
	for i := 0; i < 3; i++ {
		client.RegisterPeer(ctx, &pb.RegisterPeerRequest{
			Peer: &pb.PeerInfo{
				Id:             fmt.Sprintf("peer-%d", i),
				Address:        fmt.Sprintf("10.0.0.%d:8081", i+1),
				AvailableSpace: 1000,
				UsedSpace:      500,
			},
		})
	}

	resp, err := client.GetPeerStats(ctx, &pb.GetPeerStatsRequest{})
	if err != nil {
		t.Fatalf("GetPeerStats: %v", err)
	}

	if resp.ActivePeers != 3 {
		t.Errorf("active_peers = %d, want 3", resp.ActivePeers)
	}
	if resp.TotalPeers != 3 {
		t.Errorf("total_peers = %d, want 3", resp.TotalPeers)
	}
}

func TestGRPC_GetPeersExcludesRequester(t *testing.T) {
	client, cleanup := startTestGRPCServer(t)
	defer cleanup()

	ctx := context.Background()

	client.RegisterPeer(ctx, &pb.RegisterPeerRequest{
		Peer: &pb.PeerInfo{Id: "peer-1", Address: "10.0.0.1:8081"},
	})
	client.RegisterPeer(ctx, &pb.RegisterPeerRequest{
		Peer: &pb.PeerInfo{Id: "peer-2", Address: "10.0.0.2:8081"},
	})

	resp, err := client.GetPeers(ctx, &pb.GetPeersRequest{RequesterId: "peer-1"})
	if err != nil {
		t.Fatalf("GetPeers: %v", err)
	}

	if len(resp.Peers) != 1 {
		t.Fatalf("expected 1 peer (excluding requester), got %d", len(resp.Peers))
	}
	if resp.Peers[0].Id != "peer-2" {
		t.Errorf("peer ID = %q, want %q", resp.Peers[0].Id, "peer-2")
	}
}

func TestGRPC_CoordinatorClientInterface(t *testing.T) {
	// Start a real gRPC server
	service := NewCoordinatorService()
	srv := grpc.NewServer()
	pb.RegisterCoordinatorServiceServer(srv, NewGRPCServer(service))

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	go srv.Serve(lis)
	defer srv.Stop()

	// Create the GRPCCoordinatorClient
	grpcClient, err := NewGRPCCoordinatorClient(lis.Addr().String())
	if err != nil {
		t.Fatalf("NewGRPCCoordinatorClient: %v", err)
	}
	defer grpcClient.Close()

	// Verify it satisfies the Coordinator interface
	var _ Coordinator = grpcClient

	ctx := context.Background()

	// Test all interface methods
	err = grpcClient.RegisterPeer(ctx, &PeerInfo{
		ID:          "test-peer",
		Address:     "127.0.0.1:8081",
		GRPCAddress: "127.0.0.1:9081",
		NVMePath:    "/tmp/nvme",
	})
	if err != nil {
		t.Fatalf("RegisterPeer via gRPC client: %v", err)
	}

	peers, err := grpcClient.GetPeers(ctx, "other")
	if err != nil {
		t.Fatalf("GetPeers: %v", err)
	}
	if len(peers) != 1 || peers[0].ID != "test-peer" {
		t.Errorf("unexpected peers: %+v", peers)
	}
	if peers[0].GRPCAddress != "127.0.0.1:9081" {
		t.Errorf("GRPCAddress = %q, want %q", peers[0].GRPCAddress, "127.0.0.1:9081")
	}

	err = grpcClient.UpdatePeerStatus(ctx, "test-peer", "active", 1000, 500)
	if err != nil {
		t.Fatalf("UpdatePeerStatus: %v", err)
	}

	err = grpcClient.UpdateFileLocation(ctx, &FileLocation{
		FilePath: "/file.txt",
		PeerID:   "test-peer",
		FileSize: 2048,
	})
	if err != nil {
		t.Fatalf("UpdateFileLocation: %v", err)
	}

	locs, err := grpcClient.GetFileLocation(ctx, "/file.txt")
	if err != nil {
		t.Fatalf("GetFileLocation: %v", err)
	}
	if len(locs) != 1 || locs[0].FileSize != 2048 {
		t.Errorf("unexpected locations: %+v", locs)
	}

	stats := grpcClient.GetPeerStats()
	if _, ok := stats["active_peers"]; !ok {
		t.Error("GetPeerStats missing active_peers key")
	}

	_ = time.Now() // keep time import used
}

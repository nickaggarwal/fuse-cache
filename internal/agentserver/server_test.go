package agentserver

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	pb "fuse-client/internal/pb"
	"fuse-client/internal/session"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// testEnv sets up an agent server on a temp Unix socket and returns a connected
// gRPC client. Cleans up on test completion.
type testEnv struct {
	fuseRoot   string
	socketPath string
	server     *Server
	grpcServer *grpc.Server
	client     pb.AgentServiceClient
	conn       *grpc.ClientConn
}

func setupTestEnv(t *testing.T) *testEnv {
	t.Helper()

	fuseRoot := t.TempDir()
	// Use /tmp directly to keep socket path under 108 chars (macOS limit).
	socketDir, err := os.MkdirTemp("/tmp", "agenttest")
	if err != nil {
		t.Fatalf("MkdirTemp: %v", err)
	}
	t.Cleanup(func() { os.RemoveAll(socketDir) })
	socketPath := filepath.Join(socketDir, "a.sock")

	sessMgr := session.NewManager(fuseRoot)
	srv := New(sessMgr, socketPath)

	os.Remove(socketPath)
	lis, err := net.Listen("unix", socketPath)
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}

	grpcSrv := grpc.NewServer()
	pb.RegisterAgentServiceServer(grpcSrv, srv)

	go grpcSrv.Serve(lis)

	// Connect client.
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	conn, err := grpc.DialContext(ctx, "unix://"+socketPath,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	if err != nil {
		grpcSrv.Stop()
		t.Fatalf("Dial: %v", err)
	}

	client := pb.NewAgentServiceClient(conn)

	t.Cleanup(func() {
		conn.Close()
		grpcSrv.GracefulStop()
	})

	return &testEnv{
		fuseRoot:   fuseRoot,
		socketPath: socketPath,
		server:     srv,
		grpcServer: grpcSrv,
		client:     client,
		conn:       conn,
	}
}

func TestCreateSession(t *testing.T) {
	env := setupTestEnv(t)
	ctx := context.Background()

	resp, err := env.client.CreateSession(ctx, &pb.CreateSessionRequest{
		VolumeId: "vol-1",
		RootPath: "/models/llama",
		ReadOnly: true,
		Policy: &pb.CachePolicy{
			CacheMode: "readonly",
			Pinned:    true,
		},
	})
	if err != nil {
		t.Fatalf("CreateSession: %v", err)
	}
	if resp.VolumeId != "vol-1" {
		t.Errorf("VolumeId = %q, want vol-1", resp.VolumeId)
	}
	expected := filepath.Join(env.fuseRoot, "models/llama")
	if resp.HostPath != expected {
		t.Errorf("HostPath = %q, want %q", resp.HostPath, expected)
	}
}

func TestGetSession(t *testing.T) {
	env := setupTestEnv(t)
	ctx := context.Background()

	env.client.CreateSession(ctx, &pb.CreateSessionRequest{
		VolumeId: "vol-1",
		RootPath: "/data",
		ReadOnly: false,
		Policy: &pb.CachePolicy{
			CacheMode: "writethrough",
			Warmup:    "metadata",
		},
	})

	info, err := env.client.GetSession(ctx, &pb.GetSessionRequest{VolumeId: "vol-1"})
	if err != nil {
		t.Fatalf("GetSession: %v", err)
	}
	if info.VolumeId != "vol-1" {
		t.Errorf("VolumeId = %q", info.VolumeId)
	}
	if info.RootPath != "/data" {
		t.Errorf("RootPath = %q", info.RootPath)
	}
	if info.ReadOnly {
		t.Error("ReadOnly should be false")
	}
	if info.Policy.CacheMode != "writethrough" {
		t.Errorf("CacheMode = %q", info.Policy.CacheMode)
	}
	if info.RefCount != 1 {
		t.Errorf("RefCount = %d", info.RefCount)
	}
}

func TestGetSessionNotFound(t *testing.T) {
	env := setupTestEnv(t)
	ctx := context.Background()

	_, err := env.client.GetSession(ctx, &pb.GetSessionRequest{VolumeId: "nonexistent"})
	if err == nil {
		t.Fatal("Expected error for nonexistent session")
	}
	st, ok := status.FromError(err)
	if !ok {
		t.Fatalf("Expected gRPC status error, got %v", err)
	}
	if st.Code() != codes.NotFound {
		t.Errorf("Expected NotFound, got %v", st.Code())
	}
}

func TestDeleteSession(t *testing.T) {
	env := setupTestEnv(t)
	ctx := context.Background()

	env.client.CreateSession(ctx, &pb.CreateSessionRequest{
		VolumeId: "vol-1",
		RootPath: "/data",
	})

	_, err := env.client.DeleteSession(ctx, &pb.DeleteSessionRequest{VolumeId: "vol-1"})
	if err != nil {
		t.Fatalf("DeleteSession: %v", err)
	}

	_, err = env.client.GetSession(ctx, &pb.GetSessionRequest{VolumeId: "vol-1"})
	if err == nil {
		t.Error("Session should be gone after delete")
	}
}

func TestListSessions(t *testing.T) {
	env := setupTestEnv(t)
	ctx := context.Background()

	env.client.CreateSession(ctx, &pb.CreateSessionRequest{VolumeId: "vol-1", RootPath: "/a"})
	env.client.CreateSession(ctx, &pb.CreateSessionRequest{VolumeId: "vol-2", RootPath: "/b"})

	resp, err := env.client.ListSessions(ctx, &pb.ListSessionsRequest{})
	if err != nil {
		t.Fatalf("ListSessions: %v", err)
	}
	if len(resp.Sessions) != 2 {
		t.Errorf("ListSessions returned %d, want 2", len(resp.Sessions))
	}
}

func TestCreateSessionEmptyVolumeID(t *testing.T) {
	env := setupTestEnv(t)
	ctx := context.Background()

	_, err := env.client.CreateSession(ctx, &pb.CreateSessionRequest{
		VolumeId: "",
		RootPath: "/data",
	})
	if err == nil {
		t.Fatal("Expected error for empty volume_id")
	}
	st, ok := status.FromError(err)
	if !ok {
		t.Fatalf("Expected gRPC status error, got %v", err)
	}
	if st.Code() != codes.InvalidArgument {
		t.Errorf("Expected InvalidArgument, got %v", st.Code())
	}
}

func TestRefcountViaGRPC(t *testing.T) {
	env := setupTestEnv(t)
	ctx := context.Background()

	// Create same volume twice → refcount should be 2.
	env.client.CreateSession(ctx, &pb.CreateSessionRequest{VolumeId: "vol-1", RootPath: "/data"})
	env.client.CreateSession(ctx, &pb.CreateSessionRequest{VolumeId: "vol-1", RootPath: "/data"})

	info, _ := env.client.GetSession(ctx, &pb.GetSessionRequest{VolumeId: "vol-1"})
	if info.RefCount != 2 {
		t.Errorf("RefCount = %d, want 2", info.RefCount)
	}

	// Delete once → refcount should be 1.
	env.client.DeleteSession(ctx, &pb.DeleteSessionRequest{VolumeId: "vol-1"})

	info, _ = env.client.GetSession(ctx, &pb.GetSessionRequest{VolumeId: "vol-1"})
	if info.RefCount != 1 {
		t.Errorf("RefCount = %d, want 1", info.RefCount)
	}

	// Delete again → should be fully removed.
	env.client.DeleteSession(ctx, &pb.DeleteSessionRequest{VolumeId: "vol-1"})

	_, err := env.client.GetSession(ctx, &pb.GetSessionRequest{VolumeId: "vol-1"})
	if err == nil {
		t.Error("Session should be gone")
	}
}

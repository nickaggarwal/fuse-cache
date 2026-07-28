package csidriver

import (
	"context"
	"log"
	"net"
	"os"
	"path/filepath"
	"testing"

	pb "fuse-client/internal/pb"
	"fuse-client/internal/session"

	"fuse-client/internal/agentserver"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// startTestAgent starts an agent gRPC server on a temp socket and returns the
// socket path. Cleans up on test completion.
func startTestAgent(t *testing.T, fuseRoot string) string {
	t.Helper()

	// Use /tmp directly to keep socket path under 108 chars (macOS limit).
	socketDir, err := os.MkdirTemp("/tmp", "csitest")
	if err != nil {
		t.Fatalf("MkdirTemp: %v", err)
	}
	t.Cleanup(func() { os.RemoveAll(socketDir) })
	socketPath := filepath.Join(socketDir, "a.sock")

	sessMgr := session.NewManager(fuseRoot)
	srv := agentserver.New(sessMgr, socketPath)

	lis, err := net.Listen("unix", socketPath)
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}

	grpcSrv := grpc.NewServer()
	pb.RegisterAgentServiceServer(grpcSrv, srv)

	go grpcSrv.Serve(lis)
	t.Cleanup(func() { grpcSrv.GracefulStop() })

	return socketPath
}

func TestIdentityGetPluginInfo(t *testing.T) {
	d := New("test.csi.driver", "node-1", "/tmp/agent.sock", "/tmp/fuse", log.Default())

	resp, err := d.GetPluginInfo(context.Background(), &csi.GetPluginInfoRequest{})
	if err != nil {
		t.Fatalf("GetPluginInfo: %v", err)
	}
	if resp.Name != "test.csi.driver" {
		t.Errorf("Name = %q", resp.Name)
	}
	if resp.VendorVersion != version {
		t.Errorf("Version = %q", resp.VendorVersion)
	}
}

func TestIdentityProbe(t *testing.T) {
	d := New("test.csi.driver", "node-1", "/tmp/agent.sock", "/tmp/fuse", log.Default())

	_, err := d.Probe(context.Background(), &csi.ProbeRequest{})
	if err != nil {
		t.Fatalf("Probe: %v", err)
	}
}

func TestNodeGetInfo(t *testing.T) {
	d := New("test.csi.driver", "node-42", "/tmp/agent.sock", "/tmp/fuse", log.Default())

	resp, err := d.NodeGetInfo(context.Background(), &csi.NodeGetInfoRequest{})
	if err != nil {
		t.Fatalf("NodeGetInfo: %v", err)
	}
	if resp.NodeId != "node-42" {
		t.Errorf("NodeId = %q", resp.NodeId)
	}
}

func TestNodeGetCapabilities(t *testing.T) {
	d := New("test.csi.driver", "node-1", "/tmp/agent.sock", "/tmp/fuse", log.Default())

	resp, err := d.NodeGetCapabilities(context.Background(), &csi.NodeGetCapabilitiesRequest{})
	if err != nil {
		t.Fatalf("NodeGetCapabilities: %v", err)
	}
	if len(resp.Capabilities) != 0 {
		t.Errorf("expected 0 capabilities, got %d", len(resp.Capabilities))
	}
}

func TestNodePublishVolumeValidation(t *testing.T) {
	d := New("test.csi.driver", "node-1", "/tmp/agent.sock", "/tmp/fuse", log.Default())
	ctx := context.Background()

	// Missing volume_id.
	_, err := d.NodePublishVolume(ctx, &csi.NodePublishVolumeRequest{
		TargetPath: "/tmp/target",
	})
	assertGRPCCode(t, err, codes.InvalidArgument)

	// Missing target_path.
	_, err = d.NodePublishVolume(ctx, &csi.NodePublishVolumeRequest{
		VolumeId: "vol-1",
	})
	assertGRPCCode(t, err, codes.InvalidArgument)
}

func TestNodeUnpublishVolumeValidation(t *testing.T) {
	d := New("test.csi.driver", "node-1", "/tmp/agent.sock", "/tmp/fuse", log.Default())
	ctx := context.Background()

	_, err := d.NodeUnpublishVolume(ctx, &csi.NodeUnpublishVolumeRequest{
		TargetPath: "/tmp/target",
	})
	assertGRPCCode(t, err, codes.InvalidArgument)

	_, err = d.NodeUnpublishVolume(ctx, &csi.NodeUnpublishVolumeRequest{
		VolumeId: "vol-1",
	})
	assertGRPCCode(t, err, codes.InvalidArgument)
}

func TestNodePublishWithAgent(t *testing.T) {
	fuseRoot := t.TempDir()
	socketPath := startTestAgent(t, fuseRoot)

	d := New("test.csi.driver", "node-1", socketPath, fuseRoot, log.Default())
	ctx := context.Background()

	targetPath := filepath.Join(t.TempDir(), "target")

	// NodePublishVolume should succeed up to the bind mount step.
	// Since we're not running as root, the bind mount will fail, but the
	// session should be created in the agent.
	_, err := d.NodePublishVolume(ctx, &csi.NodePublishVolumeRequest{
		VolumeId:   "vol-1",
		TargetPath: targetPath,
		Readonly:   true,
		VolumeContext: map[string]string{
			"rootPath":  "/models/llama",
			"cacheMode": "readonly",
			"warmup":    "metadata",
			"pinned":    "true",
		},
	})

	// We expect the bind mount to fail (not root / no FUSE mount),
	// but the session should have been created in the agent.
	if err != nil {
		// Verify the error is about bind mount, not session creation.
		st, ok := status.FromError(err)
		if !ok || st.Code() != codes.Internal {
			t.Fatalf("Expected Internal error (bind mount), got %v", err)
		}
		t.Logf("Expected bind mount failure: %v", err)
	}

	// Verify the session was created by connecting directly to agent.
	agentClient, err := agentserver.NewClient(socketPath)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer agentClient.Close()

	info, err := agentClient.GetSession(ctx, "vol-1")
	if err != nil {
		t.Fatalf("GetSession: %v", err)
	}
	if info.RootPath != "/models/llama" {
		t.Errorf("RootPath = %q, want /models/llama", info.RootPath)
	}
	if !info.ReadOnly {
		t.Error("ReadOnly should be true")
	}
	if !info.Policy.Pinned {
		t.Error("Pinned should be true")
	}
}

func TestNodePublishDefaultRootPath(t *testing.T) {
	fuseRoot := t.TempDir()
	socketPath := startTestAgent(t, fuseRoot)

	d := New("test.csi.driver", "node-1", socketPath, fuseRoot, log.Default())
	ctx := context.Background()

	targetPath := filepath.Join(t.TempDir(), "target")

	// No rootPath in volume context → should default to "/".
	d.NodePublishVolume(ctx, &csi.NodePublishVolumeRequest{
		VolumeId:      "vol-default",
		TargetPath:    targetPath,
		VolumeContext: map[string]string{},
	})

	agentClient, err := agentserver.NewClient(socketPath)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer agentClient.Close()

	info, err := agentClient.GetSession(ctx, "vol-default")
	if err != nil {
		t.Fatalf("GetSession: %v", err)
	}
	if info.RootPath != "/" {
		t.Errorf("RootPath = %q, want /", info.RootPath)
	}
}

func TestNodeUnpublishCleansSession(t *testing.T) {
	fuseRoot := t.TempDir()
	socketPath := startTestAgent(t, fuseRoot)

	d := New("test.csi.driver", "node-1", socketPath, fuseRoot, log.Default())
	ctx := context.Background()

	// Create a session via agent directly first.
	agentClient, err := agentserver.NewClient(socketPath)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer agentClient.Close()

	agentClient.CreateSession(ctx, "vol-cleanup", "/data", false, nil)

	// Create a target dir that's NOT a mount point.
	targetPath := filepath.Join(t.TempDir(), "target")
	os.MkdirAll(targetPath, 0755)

	// NodeUnpublish should clean up the session even if unmount fails.
	_, err = d.NodeUnpublishVolume(ctx, &csi.NodeUnpublishVolumeRequest{
		VolumeId:   "vol-cleanup",
		TargetPath: targetPath,
	})
	if err != nil {
		t.Fatalf("NodeUnpublishVolume: %v", err)
	}

	// Session should be gone.
	_, err = agentClient.GetSession(ctx, "vol-cleanup")
	if err == nil {
		t.Error("Session should have been deleted")
	}
}

func TestAttrOr(t *testing.T) {
	attrs := map[string]string{
		"key1": "value1",
		"key2": "",
	}

	if v := attrOr(attrs, "key1", "default"); v != "value1" {
		t.Errorf("key1 = %q", v)
	}
	if v := attrOr(attrs, "key2", "default"); v != "default" {
		t.Errorf("empty key2 should use default, got %q", v)
	}
	if v := attrOr(attrs, "missing", "default"); v != "default" {
		t.Errorf("missing should use default, got %q", v)
	}
}

func assertGRPCCode(t *testing.T, err error, expected codes.Code) {
	t.Helper()
	if err == nil {
		t.Fatalf("expected error with code %v, got nil", expected)
	}
	st, ok := status.FromError(err)
	if !ok {
		t.Fatalf("expected gRPC error, got %v", err)
	}
	if st.Code() != expected {
		t.Errorf("code = %v, want %v (msg: %s)", st.Code(), expected, st.Message())
	}
}

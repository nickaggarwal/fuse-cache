package csidriver

import (
	"context"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"sync"

	"fuse-client/internal/agentserver"
	pb "fuse-client/internal/pb"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const version = "0.1.0"

// Driver implements CSI Identity and Node services.
type Driver struct {
	csi.UnimplementedIdentityServer
	csi.UnimplementedNodeServer

	name        string
	nodeID      string
	agentSocket string
	fuseRoot    string
	logger      *log.Logger

	agentMu sync.Mutex
	agent   *agentserver.Client
}

// New creates a new CSI driver.
func New(name, nodeID, agentSocket, fuseRoot string, logger *log.Logger) *Driver {
	return &Driver{
		name:        name,
		nodeID:      nodeID,
		agentSocket: agentSocket,
		fuseRoot:    fuseRoot,
		logger:      logger,
	}
}

// getAgent returns a lazily-initialized agent client, reconnecting if needed.
func (d *Driver) getAgent() (*agentserver.Client, error) {
	d.agentMu.Lock()
	defer d.agentMu.Unlock()
	if d.agent != nil {
		return d.agent, nil
	}
	client, err := agentserver.NewClient(d.agentSocket)
	if err != nil {
		return nil, err
	}
	d.agent = client
	return d.agent, nil
}

// resetAgent closes and clears the cached agent client on error.
func (d *Driver) resetAgent() {
	d.agentMu.Lock()
	defer d.agentMu.Unlock()
	if d.agent != nil {
		d.agent.Close()
		d.agent = nil
	}
}

// --- Identity Service ---

func (d *Driver) GetPluginInfo(_ context.Context, _ *csi.GetPluginInfoRequest) (*csi.GetPluginInfoResponse, error) {
	return &csi.GetPluginInfoResponse{
		Name:          d.name,
		VendorVersion: version,
	}, nil
}

func (d *Driver) GetPluginCapabilities(_ context.Context, _ *csi.GetPluginCapabilitiesRequest) (*csi.GetPluginCapabilitiesResponse, error) {
	// No controller service, no volume expansion.
	return &csi.GetPluginCapabilitiesResponse{}, nil
}

func (d *Driver) Probe(_ context.Context, _ *csi.ProbeRequest) (*csi.ProbeResponse, error) {
	return &csi.ProbeResponse{}, nil
}

// --- Node Service ---

func (d *Driver) NodeGetCapabilities(_ context.Context, _ *csi.NodeGetCapabilitiesRequest) (*csi.NodeGetCapabilitiesResponse, error) {
	return &csi.NodeGetCapabilitiesResponse{
		Capabilities: []*csi.NodeServiceCapability{},
	}, nil
}

func (d *Driver) NodeGetInfo(_ context.Context, _ *csi.NodeGetInfoRequest) (*csi.NodeGetInfoResponse, error) {
	return &csi.NodeGetInfoResponse{
		NodeId: d.nodeID,
	}, nil
}

func (d *Driver) NodePublishVolume(ctx context.Context, req *csi.NodePublishVolumeRequest) (*csi.NodePublishVolumeResponse, error) {
	volumeID := req.GetVolumeId()
	targetPath := req.GetTargetPath()

	if volumeID == "" {
		return nil, status.Error(codes.InvalidArgument, "volume_id is required")
	}
	if targetPath == "" {
		return nil, status.Error(codes.InvalidArgument, "target_path is required")
	}

	attrs := req.GetVolumeContext()
	rootPath := attrs["rootPath"]
	if rootPath == "" {
		rootPath = "/"
	}

	policy := &pb.CachePolicy{
		CacheMode:    attrOr(attrs, "cacheMode", "readonly"),
		Warmup:       attrOr(attrs, "warmup", "none"),
		Pinned:       attrOr(attrs, "pinned", "false") == "true",
		SourcePolicy: attrOr(attrs, "sourcePolicy", "peer-first"),
	}

	readOnly := req.GetReadonly()

	d.logger.Printf("NodePublishVolume: volume=%s target=%s root=%s readonly=%t",
		volumeID, targetPath, rootPath, readOnly)

	// 1. Ask the agent to create a session and get the host path.
	agent, err := d.getAgent()
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "agent not available: %v", err)
	}

	resp, err := agent.CreateSession(ctx, volumeID, rootPath, readOnly, policy)
	if err != nil {
		d.resetAgent()
		return nil, status.Errorf(codes.Internal, "create session: %v", err)
	}

	hostPath := resp.HostPath
	d.logger.Printf("NodePublishVolume: session created, hostPath=%s", hostPath)

	// 2. Ensure host path directory exists (it may be a subtree that hasn't
	//    been accessed yet — the FUSE layer will create it on first access).
	//    We create it so the bind mount has a source.
	if err := os.MkdirAll(hostPath, 0755); err != nil {
		d.logger.Printf("NodePublishVolume: warning: mkdir hostPath %s: %v", hostPath, err)
		// Not fatal — FUSE may serve it dynamically.
	}

	// 3. Create target path and bind mount.
	if err := os.MkdirAll(targetPath, 0755); err != nil {
		return nil, status.Errorf(codes.Internal, "mkdir target %s: %v", targetPath, err)
	}

	if err := bindMount(hostPath, targetPath, readOnly); err != nil {
		return nil, status.Errorf(codes.Internal, "bind mount %s → %s: %v", hostPath, targetPath, err)
	}

	d.logger.Printf("NodePublishVolume: bind mount %s → %s complete", hostPath, targetPath)
	return &csi.NodePublishVolumeResponse{}, nil
}

func (d *Driver) NodeUnpublishVolume(ctx context.Context, req *csi.NodeUnpublishVolumeRequest) (*csi.NodeUnpublishVolumeResponse, error) {
	volumeID := req.GetVolumeId()
	targetPath := req.GetTargetPath()

	if volumeID == "" {
		return nil, status.Error(codes.InvalidArgument, "volume_id is required")
	}
	if targetPath == "" {
		return nil, status.Error(codes.InvalidArgument, "target_path is required")
	}

	d.logger.Printf("NodeUnpublishVolume: volume=%s target=%s", volumeID, targetPath)

	// 1. Unmount.
	if err := unmount(targetPath); err != nil {
		d.logger.Printf("NodeUnpublishVolume: unmount warning: %v", err)
		// Continue to clean up session anyway.
	}

	// 2. Remove target directory.
	os.Remove(targetPath)

	// 3. Delete agent session.
	agent, err := d.getAgent()
	if err != nil {
		d.logger.Printf("NodeUnpublishVolume: agent unavailable, session %s not cleaned: %v", volumeID, err)
		return &csi.NodeUnpublishVolumeResponse{}, nil
	}
	if err := agent.DeleteSession(ctx, volumeID); err != nil {
		d.logger.Printf("NodeUnpublishVolume: delete session %s: %v", volumeID, err)
		d.resetAgent()
	}

	d.logger.Printf("NodeUnpublishVolume: complete for volume=%s", volumeID)
	return &csi.NodeUnpublishVolumeResponse{}, nil
}

// --- Helpers ---

func attrOr(attrs map[string]string, key, fallback string) string {
	if v, ok := attrs[key]; ok && v != "" {
		return v
	}
	return fallback
}

// bindMount performs a bind mount from source to target.
// On Linux this uses mount(2) via exec. On other platforms it's a no-op for
// testing (the CSI driver only runs on Linux nodes).
func bindMount(source, target string, readOnly bool) error {
	args := []string{"--bind", source, target}
	if err := exec.Command("mount", args...).Run(); err != nil {
		return err
	}
	if readOnly {
		roArgs := []string{"-o", "remount,bind,ro", target}
		if err := exec.Command("mount", roArgs...).Run(); err != nil {
			return err
		}
	}
	return nil
}

// unmount unmounts a path.
func unmount(target string) error {
	// Check if actually mounted first.
	if _, err := os.Stat(target); os.IsNotExist(err) {
		return nil
	}
	return exec.Command("umount", target).Run()
}

// isMountPoint checks if a path is a mount point.
func isMountPoint(path string) bool {
	abspath := filepath.Clean(path)
	out, err := exec.Command("mountpoint", "-q", abspath).CombinedOutput()
	_ = out
	return err == nil
}

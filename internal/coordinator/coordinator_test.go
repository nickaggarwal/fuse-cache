package coordinator

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestRegisterPeer(t *testing.T) {
	cs := NewCoordinatorService()
	ctx := context.Background()

	peer := &PeerInfo{
		ID:             "peer-1",
		Address:        "10.0.0.1:8081",
		NVMePath:       "/mnt/nvme",
		AvailableSpace: 1000,
		UsedSpace:      100,
	}

	if err := cs.RegisterPeer(ctx, peer); err != nil {
		t.Fatalf("RegisterPeer: %v", err)
	}

	peers, err := cs.GetPeers(ctx, "")
	if err != nil {
		t.Fatalf("GetPeers: %v", err)
	}

	if len(peers) != 1 {
		t.Fatalf("GetPeers returned %d, want 1", len(peers))
	}

	if peers[0].ID != "peer-1" {
		t.Errorf("Peer ID = %s, want peer-1", peers[0].ID)
	}
	if peers[0].Status != "active" {
		t.Errorf("Peer status = %s, want active", peers[0].Status)
	}
}

func TestGetPeersExcludesRequester(t *testing.T) {
	cs := NewCoordinatorService()
	ctx := context.Background()

	cs.RegisterPeer(ctx, &PeerInfo{ID: "peer-1", Address: "10.0.0.1:8081"})
	cs.RegisterPeer(ctx, &PeerInfo{ID: "peer-2", Address: "10.0.0.2:8081"})

	peers, _ := cs.GetPeers(ctx, "peer-1")
	if len(peers) != 1 {
		t.Fatalf("Expected 1 peer (excluding requester), got %d", len(peers))
	}
	if peers[0].ID != "peer-2" {
		t.Errorf("Expected peer-2, got %s", peers[0].ID)
	}
}

func TestUpdatePeerStatus(t *testing.T) {
	cs := NewCoordinatorService()
	ctx := context.Background()

	cs.RegisterPeer(ctx, &PeerInfo{ID: "peer-1", Address: "10.0.0.1:8081"})

	err := cs.UpdatePeerStatus(ctx, "peer-1", "inactive", 500, 200)
	if err != nil {
		t.Fatalf("UpdatePeerStatus: %v", err)
	}

	// Inactive peer shouldn't show in GetPeers
	peers, _ := cs.GetPeers(ctx, "")
	if len(peers) != 0 {
		t.Errorf("Expected 0 active peers, got %d", len(peers))
	}
}

func TestUpdatePeerStatusWithNetwork(t *testing.T) {
	cs := NewCoordinatorService()
	ctx := context.Background()

	if err := cs.RegisterPeer(ctx, &PeerInfo{ID: "peer-1", Address: "10.0.0.1:8081"}); err != nil {
		t.Fatalf("RegisterPeer: %v", err)
	}

	err := cs.UpdatePeerStatusWithNetwork(ctx, "peer-1", "active", 900, 100, &PeerNetworkMetrics{
		SpeedMBps:   876.5,
		LatencyMs:   2.3,
		ProbeBytes:  1048576,
		ProbeTarget: "peer-2",
		ProbedAt:    time.Now(),
	})
	if err != nil {
		t.Fatalf("UpdatePeerStatusWithNetwork: %v", err)
	}

	peers, err := cs.GetPeers(ctx, "")
	if err != nil {
		t.Fatalf("GetPeers: %v", err)
	}
	if len(peers) != 1 {
		t.Fatalf("GetPeers returned %d, want 1", len(peers))
	}
	if peers[0].NetworkSpeedMBps <= 0 {
		t.Fatalf("NetworkSpeedMBps = %f, want > 0", peers[0].NetworkSpeedMBps)
	}
	if peers[0].NetworkLatencyMs <= 0 {
		t.Fatalf("NetworkLatencyMs = %f, want > 0", peers[0].NetworkLatencyMs)
	}
	if peers[0].NetworkProbeBytes != 1048576 {
		t.Fatalf("NetworkProbeBytes = %d, want 1048576", peers[0].NetworkProbeBytes)
	}
	if peers[0].NetworkProbeTarget != "peer-2" {
		t.Fatalf("NetworkProbeTarget = %q, want peer-2", peers[0].NetworkProbeTarget)
	}

	stats := cs.GetPeerStats()
	if stats["network_sampled_peers"] != 1 {
		t.Fatalf("network_sampled_peers = %v, want 1", stats["network_sampled_peers"])
	}
	if stats["avg_network_speed_mbps"] == 0.0 {
		t.Fatalf("avg_network_speed_mbps = %v, want > 0", stats["avg_network_speed_mbps"])
	}
}

func TestFileLocation(t *testing.T) {
	cs := NewCoordinatorService()
	ctx := context.Background()

	location := &FileLocation{
		FilePath:    "/test.txt",
		PeerID:      "peer-1",
		StorageTier: "nvme",
		StoragePath: "/test.txt",
		FileSize:    100,
	}

	if err := cs.UpdateFileLocation(ctx, location); err != nil {
		t.Fatalf("UpdateFileLocation: %v", err)
	}

	locs, err := cs.GetFileLocation(ctx, "/test.txt")
	if err != nil {
		t.Fatalf("GetFileLocation: %v", err)
	}

	if len(locs) != 1 {
		t.Fatalf("Expected 1 location, got %d", len(locs))
	}

	if locs[0].PeerID != "peer-1" {
		t.Errorf("PeerID = %s, want peer-1", locs[0].PeerID)
	}
}

func TestFileLocationUpdate(t *testing.T) {
	cs := NewCoordinatorService()
	ctx := context.Background()

	cs.UpdateFileLocation(ctx, &FileLocation{
		FilePath: "/test.txt", PeerID: "peer-1", StorageTier: "nvme", FileSize: 100,
	})
	cs.UpdateFileLocation(ctx, &FileLocation{
		FilePath: "/test.txt", PeerID: "peer-1", StorageTier: "nvme", FileSize: 200,
	})

	locs, _ := cs.GetFileLocation(ctx, "/test.txt")
	if len(locs) != 1 {
		t.Fatalf("Expected 1 location (updated), got %d", len(locs))
	}
	if locs[0].FileSize != 200 {
		t.Errorf("FileSize = %d, want 200", locs[0].FileSize)
	}
}

func TestListFileLocations(t *testing.T) {
	cs := NewCoordinatorService()
	ctx := context.Background()

	cs.UpdateFileLocation(ctx, &FileLocation{
		FilePath: "/dir/a.txt", PeerID: "peer-1", StorageTier: "cloud", FileSize: 10,
	})
	cs.UpdateFileLocation(ctx, &FileLocation{
		FilePath: "/dir/b.txt", PeerID: "peer-2", StorageTier: "peer", FileSize: 20,
	})
	cs.UpdateFileLocation(ctx, &FileLocation{
		FilePath: "/other/c.txt", PeerID: "peer-3", StorageTier: "nvme", FileSize: 30,
	})

	locs, err := cs.ListFileLocations(ctx, "/dir/")
	if err != nil {
		t.Fatalf("ListFileLocations: %v", err)
	}
	if len(locs) != 2 {
		t.Fatalf("ListFileLocations returned %d, want 2", len(locs))
	}
}

func TestCleanupInactivePeers(t *testing.T) {
	cs := NewCoordinatorService()
	ctx := context.Background()

	cs.RegisterPeer(ctx, &PeerInfo{ID: "old-peer", Address: "10.0.0.1:8081"})

	// Manually set heartbeat to the past
	cs.mu.Lock()
	cs.peers["old-peer"].LastHeartbeat = time.Now().Add(-2 * time.Minute)
	cs.mu.Unlock()

	cs.CleanupInactivePeers(60 * time.Second)

	peers, _ := cs.GetPeers(ctx, "")
	if len(peers) != 0 {
		t.Errorf("Expected 0 active peers after cleanup, got %d", len(peers))
	}
}

func TestGetPeerStats(t *testing.T) {
	cs := NewCoordinatorService()
	ctx := context.Background()

	cs.RegisterPeer(ctx, &PeerInfo{
		ID: "peer-1", Address: "10.0.0.1:8081",
		AvailableSpace: 1000, UsedSpace: 200,
	})
	cs.RegisterPeer(ctx, &PeerInfo{
		ID: "peer-2", Address: "10.0.0.2:8081",
		AvailableSpace: 2000, UsedSpace: 500,
	})

	stats := cs.GetPeerStats()

	if stats["active_peers"] != 2 {
		t.Errorf("active_peers = %v, want 2", stats["active_peers"])
	}
	if stats["total_peers"] != 2 {
		t.Errorf("total_peers = %v, want 2", stats["total_peers"])
	}
}

func TestGetWorldView(t *testing.T) {
	cs := NewCoordinatorService()
	ctx := context.Background()

	cs.RegisterPeer(ctx, &PeerInfo{ID: "peer-a", Address: "10.0.0.1:8081"})
	cs.RegisterPeer(ctx, &PeerInfo{ID: "peer-b", Address: "10.0.0.2:8081"})

	cs.UpdateFileLocation(ctx, &FileLocation{
		FilePath:    "/data/a.bin",
		PeerID:      "peer-a",
		StorageTier: "nvme",
		FileSize:    1024,
		IsChunked:   true,
		Chunks: []ChunkInfo{
			{Index: 0, PeerID: "peer-a", ChunkID: "c0"},
			{Index: 1, PeerID: "peer-a", ChunkID: "c1"},
		},
	})
	cs.UpdateFileLocation(ctx, &FileLocation{
		FilePath:    "/data/a.bin",
		PeerID:      "peer-b",
		StorageTier: "peer",
		FileSize:    1024,
		IsChunked:   true,
		Chunks: []ChunkInfo{
			{Index: 1, PeerID: "peer-b", ChunkID: "c1b"},
		},
	})

	view, err := cs.GetWorldView(ctx, "/data")
	if err != nil {
		t.Fatalf("GetWorldView: %v", err)
	}
	if view.Summary.TotalPeers != 2 {
		t.Fatalf("TotalPeers = %d, want 2", view.Summary.TotalPeers)
	}
	if view.Summary.TotalFiles != 1 {
		t.Fatalf("TotalFiles = %d, want 1", view.Summary.TotalFiles)
	}
	if len(view.Files) != 1 {
		t.Fatalf("len(Files) = %d, want 1", len(view.Files))
	}
	f := view.Files[0]
	if f.ReplicaCount != 2 {
		t.Fatalf("ReplicaCount = %d, want 2", f.ReplicaCount)
	}
	if f.ChunkReplicas[1] != 2 {
		t.Fatalf("chunk 1 replicas = %d, want 2", f.ChunkReplicas[1])
	}
}

func TestSeedPathToPeers(t *testing.T) {
	cs := NewCoordinatorService()
	ctx := context.Background()

	var srcGets atomic.Int32
	sourceSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		srcGets.Add(1)
		_, _ = w.Write([]byte("seed-data"))
	}))
	defer sourceSrv.Close()

	var target1Puts atomic.Int32
	target1Srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		target1Puts.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer target1Srv.Close()

	var target2Puts atomic.Int32
	target2Srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		target2Puts.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer target2Srv.Close()

	srcAddr := mustHostPort(t, sourceSrv.URL)
	t1Addr := mustHostPort(t, target1Srv.URL)
	t2Addr := mustHostPort(t, target2Srv.URL)

	cs.RegisterPeer(ctx, &PeerInfo{ID: "peer-1", Address: srcAddr, Status: "active"})
	cs.RegisterPeer(ctx, &PeerInfo{ID: "peer-2", Address: t1Addr, Status: "active"})
	cs.RegisterPeer(ctx, &PeerInfo{ID: "peer-3", Address: t2Addr, Status: "active"})
	cs.UpdateFileLocation(ctx, &FileLocation{
		FilePath:    "/x/file.bin",
		PeerID:      "peer-1",
		StorageTier: "nvme",
		FileSize:    9,
	})

	result, err := cs.SeedPathToPeers(ctx, SeedCacheRequest{
		FilePath:       "/x/file.bin",
		SeedPercentage: 50, // of two candidates => one target
	})
	if err != nil {
		t.Fatalf("SeedPathToPeers: %v", err)
	}

	if srcGets.Load() != 1 {
		t.Fatalf("source GET count = %d, want 1", srcGets.Load())
	}
	if result.AttemptedSeeds != 1 {
		t.Fatalf("AttemptedSeeds = %d, want 1", result.AttemptedSeeds)
	}
	totalPuts := target1Puts.Load() + target2Puts.Load()
	if totalPuts != 1 {
		t.Fatalf("target PUT count total = %d, want 1", totalPuts)
	}
}

func TestSaveAndLoadStatePath(t *testing.T) {
	cs := NewCoordinatorService()
	ctx := context.Background()

	if err := cs.RegisterPeer(ctx, &PeerInfo{ID: "peer-1", Address: "10.0.0.1:8081"}); err != nil {
		t.Fatalf("RegisterPeer: %v", err)
	}
	if err := cs.UpdateFileLocation(ctx, &FileLocation{
		FilePath: "/f.txt", PeerID: "peer-1", StorageTier: "nvme", FileSize: 10,
	}); err != nil {
		t.Fatalf("UpdateFileLocation: %v", err)
	}

	statePath := filepath.Join(t.TempDir(), "state.json")
	if err := cs.SaveStateToPath(statePath); err != nil {
		t.Fatalf("SaveStateToPath: %v", err)
	}

	restored := NewCoordinatorService()
	if err := restored.LoadStateFromPath(statePath); err != nil {
		t.Fatalf("LoadStateFromPath: %v", err)
	}

	peers, err := restored.GetPeers(ctx, "")
	if err != nil {
		t.Fatalf("GetPeers: %v", err)
	}
	if len(peers) != 1 {
		t.Fatalf("len(peers) = %d, want 1", len(peers))
	}
	locs, err := restored.GetFileLocation(ctx, "/f.txt")
	if err != nil {
		t.Fatalf("GetFileLocation: %v", err)
	}
	if len(locs) != 1 {
		t.Fatalf("len(locs) = %d, want 1", len(locs))
	}
}

func TestSnapshotAndRestoreStateBytes(t *testing.T) {
	cs := NewCoordinatorService()
	ctx := context.Background()

	if err := cs.RegisterPeer(ctx, &PeerInfo{ID: "peer-snap", Address: "10.0.0.9:8081"}); err != nil {
		t.Fatalf("RegisterPeer: %v", err)
	}
	if err := cs.UpdateFileLocation(ctx, &FileLocation{
		FilePath: "/snap/a.bin", PeerID: "peer-snap", StorageTier: "nvme", FileSize: 42,
	}); err != nil {
		t.Fatalf("UpdateFileLocation: %v", err)
	}

	data, err := cs.SnapshotState()
	if err != nil {
		t.Fatalf("SnapshotState: %v", err)
	}

	restored := NewCoordinatorService()
	if err := restored.RestoreState(data); err != nil {
		t.Fatalf("RestoreState: %v", err)
	}

	peers, err := restored.GetPeers(ctx, "")
	if err != nil {
		t.Fatalf("GetPeers: %v", err)
	}
	if len(peers) != 1 || peers[0].ID != "peer-snap" {
		t.Fatalf("restored peers mismatch: %+v", peers)
	}
	locs, err := restored.GetFileLocation(ctx, "/snap/a.bin")
	if err != nil {
		t.Fatalf("GetFileLocation: %v", err)
	}
	if len(locs) != 1 || locs[0].FileSize != 42 {
		t.Fatalf("restored file locations mismatch: %+v", locs)
	}
}

func mustHostPort(t *testing.T, raw string) string {
	t.Helper()
	u, err := url.Parse(raw)
	if err != nil {
		t.Fatalf("parse URL %q: %v", raw, err)
	}
	return strings.TrimSpace(u.Host)
}

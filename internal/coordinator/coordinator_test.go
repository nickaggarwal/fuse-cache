package coordinator

import (
	"context"
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

package agentserver

import (
	"context"
	"testing"

	pb "fuse-client/internal/pb"
)

func TestClientRoundtrip(t *testing.T) {
	env := setupTestEnv(t)
	ctx := context.Background()

	client, err := NewClient(env.socketPath)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer client.Close()

	// Create.
	resp, err := client.CreateSession(ctx, "vol-rt", "/roundtrip", true, &pb.CachePolicy{
		CacheMode:    "readonly",
		Warmup:       "full",
		Pinned:       true,
		SourcePolicy: "cloud-first",
	})
	if err != nil {
		t.Fatalf("CreateSession: %v", err)
	}
	if resp.VolumeId != "vol-rt" {
		t.Errorf("VolumeId = %q", resp.VolumeId)
	}

	// Get.
	info, err := client.GetSession(ctx, "vol-rt")
	if err != nil {
		t.Fatalf("GetSession: %v", err)
	}
	if info.RootPath != "/roundtrip" {
		t.Errorf("RootPath = %q", info.RootPath)
	}
	if !info.ReadOnly {
		t.Error("ReadOnly should be true")
	}
	if info.Policy.CacheMode != "readonly" {
		t.Errorf("CacheMode = %q", info.Policy.CacheMode)
	}
	if info.Policy.Warmup != "full" {
		t.Errorf("Warmup = %q", info.Policy.Warmup)
	}
	if !info.Policy.Pinned {
		t.Error("Pinned should be true")
	}
	if info.Policy.SourcePolicy != "cloud-first" {
		t.Errorf("SourcePolicy = %q", info.Policy.SourcePolicy)
	}

	// List.
	sessions, err := client.ListSessions(ctx)
	if err != nil {
		t.Fatalf("ListSessions: %v", err)
	}
	if len(sessions) != 1 {
		t.Errorf("ListSessions returned %d, want 1", len(sessions))
	}

	// Delete.
	if err := client.DeleteSession(ctx, "vol-rt"); err != nil {
		t.Fatalf("DeleteSession: %v", err)
	}

	sessions, _ = client.ListSessions(ctx)
	if len(sessions) != 0 {
		t.Errorf("After delete, ListSessions returned %d, want 0", len(sessions))
	}
}

func TestClientConnectFailure(t *testing.T) {
	_, err := NewClient("/nonexistent/socket.sock")
	if err == nil {
		t.Error("Expected error connecting to nonexistent socket")
	}
}

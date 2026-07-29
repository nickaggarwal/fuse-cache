package cache

import (
	"bytes"
	"context"
	"io"
	"log"
	"net"
	"sync"
	"testing"
	"time"

	"fuse-client/internal/coordinator"
	pb "fuse-client/internal/pb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// herdCoordinator is a minimal in-memory Coordinator for peer-storage tests.
type herdCoordinator struct {
	mu    sync.Mutex
	peers []*coordinator.PeerInfo
}

func (c *herdCoordinator) RegisterPeer(context.Context, *coordinator.PeerInfo) error { return nil }
func (c *herdCoordinator) GetPeers(context.Context, string) ([]*coordinator.PeerInfo, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]*coordinator.PeerInfo, len(c.peers))
	copy(out, c.peers)
	return out, nil
}
func (c *herdCoordinator) UpdatePeerStatus(context.Context, string, string, int64, int64) error {
	return nil
}
func (c *herdCoordinator) GetFileLocation(context.Context, string) ([]*coordinator.FileLocation, error) {
	return nil, nil
}
func (c *herdCoordinator) ListFileLocations(context.Context, string) ([]*coordinator.FileLocation, error) {
	return nil, nil
}
func (c *herdCoordinator) UpdateFileLocation(context.Context, *coordinator.FileLocation) error {
	return nil
}
func (c *herdCoordinator) GetPeerStats() map[string]interface{} { return nil }

// startHerdPeer starts an in-process peer gRPC server whose manager has the
// given serve-gate capacity, pre-loaded with entries.
func startHerdPeer(t *testing.T, gateCap int, entries map[string][]byte) (addr string, cm *DefaultCacheManager, stop func()) {
	t.Helper()

	cm = &DefaultCacheManager{
		config: &CacheConfig{
			NVMePath:     t.TempDir(),
			MaxNVMeSize:  64 * 1024 * 1024,
			ChunkSize:    4 * 1024 * 1024,
			CloudTimeout: 5 * time.Second,
		},
		nvmeStorage:   newMockStorage(),
		peerStorage:   newMockStorage(),
		cloudStorage:  newMockStorage(),
		entries:       make(map[string]*CacheEntry),
		logger:        log.New(io.Discard, "", 0),
		metrics:       NewCacheMetrics(),
		peerServeGate: newPeerServeGate(gateCap),
	}
	cm.shutdownCtx, cm.shutdownCancel = context.WithCancel(context.Background())
	for p, data := range entries {
		cm.entries[p] = &CacheEntry{
			FilePath:     p,
			Size:         int64(len(data)),
			LastAccessed: time.Now(),
			Data:         data,
		}
		// GetLocal serves from the NVMe tier, so back the entry with data there.
		if err := cm.nvmeStorage.Write(context.Background(), p, data); err != nil {
			t.Fatalf("seed nvme: %v", err)
		}
	}

	srv := grpc.NewServer()
	pb.RegisterPeerServiceServer(srv, NewPeerGRPCServer(cm))
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	go srv.Serve(lis)
	return lis.Addr().String(), cm, srv.Stop
}

func newHerdPeerStorage(t *testing.T, coord coordinator.Coordinator) *PeerStorage {
	t.Helper()
	ps, err := NewPeerStorage(coord, 5*time.Second, "local", false, false, false, "")
	if err != nil {
		t.Fatalf("NewPeerStorage: %v", err)
	}
	ps.replicationStagger = time.Millisecond // keep tests fast
	return ps
}

// TestPeerGRPC_BusyWhenGateFull holds the single serve slot and verifies the
// server rejects with RESOURCE_EXHAUSTED instead of queueing.
func TestPeerGRPC_BusyWhenGateFull(t *testing.T) {
	addr, cm, stop := startHerdPeer(t, 1, map[string][]byte{"/hot.bin": []byte("payload")})
	defer stop()

	release, ok := cm.TryAcquirePeerServe()
	if !ok {
		t.Fatal("gate acquire should succeed")
	}

	conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()
	client := pb.NewPeerServiceClient(conn)

	stream, err := client.ReadFile(context.Background(), &pb.ReadFileRequest{Path: "/hot.bin"})
	if err == nil {
		_, err = stream.Recv()
	}
	if status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("ReadFile under full gate: err = %v, want RESOURCE_EXHAUSTED", err)
	}

	// WriteFile must be gated too.
	wstream, err := client.WriteFile(context.Background())
	if err != nil {
		t.Fatalf("WriteFile stream: %v", err)
	}
	_ = wstream.Send(&pb.WriteFileRequest{Path: "/w.bin", Data: []byte("x")})
	_, err = wstream.CloseAndRecv()
	if status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("WriteFile under full gate: err = %v, want RESOURCE_EXHAUSTED", err)
	}

	snap := cm.PeerLoadSnapshot()
	if snap.ServeRejectedTotal < 2 {
		t.Fatalf("rejected total = %d, want >= 2", snap.ServeRejectedTotal)
	}

	// After release the same read succeeds.
	release()
	stream, err = client.ReadFile(context.Background(), &pb.ReadFileRequest{Path: "/hot.bin"})
	if err != nil {
		t.Fatalf("ReadFile after release: %v", err)
	}
	var buf bytes.Buffer
	for {
		chunk, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("Recv after release: %v", err)
		}
		buf.Write(chunk.Data)
	}
	if buf.String() != "payload" {
		t.Fatalf("data = %q, want payload", buf.String())
	}
}

// TestPeerRead_FailsOverFromBusyPeer verifies the requester skips a busy
// holder and gets the data from the next candidate without waiting.
func TestPeerRead_FailsOverFromBusyPeer(t *testing.T) {
	payload := []byte("failover payload")

	busyAddr, busyCM, stopBusy := startHerdPeer(t, 1, map[string][]byte{"/f.bin": payload})
	defer stopBusy()
	freeAddr, _, stopFree := startHerdPeer(t, 8, map[string][]byte{"/f.bin": payload})
	defer stopFree()

	// Saturate the busy peer's gate.
	release, ok := busyCM.TryAcquirePeerServe()
	if !ok {
		t.Fatal("gate acquire should succeed")
	}
	defer release()

	coord := &herdCoordinator{peers: []*coordinator.PeerInfo{
		{ID: "busy", Status: "active", GRPCAddress: busyAddr},
		{ID: "free", Status: "active", GRPCAddress: freeAddr},
	}}
	ps := newHerdPeerStorage(t, coord)
	defer ps.Close()

	data, err := ps.Read(context.Background(), "/f.bin")
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if !bytes.Equal(data, payload) {
		t.Fatalf("data mismatch: got %d bytes", len(data))
	}
}

// TestPeerRead_JitteredRetryAfterAllBusy verifies that when every holder is
// busy, the requester sleeps with jitter and retries the busy holders once —
// succeeding when the source has drained in the meantime.
func TestPeerRead_JitteredRetryAfterAllBusy(t *testing.T) {
	payload := []byte("retry payload")
	addr, cm, stop := startHerdPeer(t, 1, map[string][]byte{"/r.bin": payload})
	defer stop()

	release, ok := cm.TryAcquirePeerServe()
	if !ok {
		t.Fatal("gate acquire should succeed")
	}
	// Drain the gate shortly after the first (rejected) attempt.
	go func() {
		time.Sleep(5 * time.Millisecond)
		release()
	}()

	coord := &herdCoordinator{peers: []*coordinator.PeerInfo{
		{ID: "only", Status: "active", GRPCAddress: addr},
	}}
	ps := newHerdPeerStorage(t, coord)
	defer ps.Close()

	data, err := ps.Read(context.Background(), "/r.bin")
	if err != nil {
		t.Fatalf("Read after busy retry: %v", err)
	}
	if !bytes.Equal(data, payload) {
		t.Fatalf("data mismatch: got %d bytes", len(data))
	}
	if ps.busySkipsTotal.Load() == 0 {
		t.Fatal("busy skip counter should have incremented")
	}
	if ps.jitterRetriesTotal.Load() == 0 {
		t.Fatal("jitter retry counter should have incremented")
	}
}

// TestPeerWrite_SkipsBusyTargetAndStaggers verifies write replication skips a
// busy target (counting it) and still reaches the replica goal via the
// remaining peers, staggering successive replica RPCs.
func TestPeerWrite_SkipsBusyTargetAndStaggers(t *testing.T) {
	busyAddr, busyCM, stopBusy := startHerdPeer(t, 1, nil)
	defer stopBusy()
	okAddr1, okCM1, stopOK1 := startHerdPeer(t, 8, nil)
	defer stopOK1()
	okAddr2, okCM2, stopOK2 := startHerdPeer(t, 8, nil)
	defer stopOK2()

	release, ok := busyCM.TryAcquirePeerServe()
	if !ok {
		t.Fatal("gate acquire should succeed")
	}
	defer release()

	// Give the busy peer the highest headroom score so replication targets it
	// first deterministically (the shuffle is followed by a stable score sort).
	coord := &herdCoordinator{peers: []*coordinator.PeerInfo{
		{ID: "busy", Status: "active", GRPCAddress: busyAddr, AvailableSpace: 1000 << 30},
		{ID: "ok1", Status: "active", GRPCAddress: okAddr1, AvailableSpace: 10 << 30},
		{ID: "ok2", Status: "active", GRPCAddress: okAddr2, AvailableSpace: 10 << 30},
	}}
	ps := newHerdPeerStorage(t, coord)
	defer ps.Close()
	ps.minReplicationCount = 2

	if err := ps.Write(context.Background(), "/repl.bin", []byte("replicate me")); err != nil {
		t.Fatalf("Write: %v", err)
	}

	stored := 0
	for _, cm := range []*DefaultCacheManager{okCM1, okCM2} {
		cm.mu.RLock()
		if _, ok := cm.entries["/repl.bin"]; ok {
			stored++
		}
		cm.mu.RUnlock()
	}
	if stored != 2 {
		t.Fatalf("replicas on non-busy peers = %d, want 2", stored)
	}
	busyCM.mu.RLock()
	_, onBusy := busyCM.entries["/repl.bin"]
	busyCM.mu.RUnlock()
	if onBusy {
		t.Fatal("busy peer should not have received a replica")
	}
	if ps.replBusySkipsTotal.Load() != 1 {
		t.Fatalf("replication busy skips = %d, want 1", ps.replBusySkipsTotal.Load())
	}
	if ps.replStaggersTotal.Load() == 0 {
		t.Fatal("stagger counter should have incremented for the second replica")
	}
}

// TestPeerWrite_ContextCancelledDuringStagger keeps the partial replica set
// rather than erroring when the context dies mid-stagger.
func TestPeerWrite_ContextCancelledDuringStagger(t *testing.T) {
	okAddr, okCM, stopOK := startHerdPeer(t, 8, nil)
	defer stopOK()
	okAddr2, okCM2, stopOK2 := startHerdPeer(t, 8, nil)
	defer stopOK2()

	coord := &herdCoordinator{peers: []*coordinator.PeerInfo{
		{ID: "ok1", Status: "active", GRPCAddress: okAddr},
		{ID: "ok2", Status: "active", GRPCAddress: okAddr2},
	}}
	ps := newHerdPeerStorage(t, coord)
	defer ps.Close()
	ps.minReplicationCount = 2
	ps.replicationStagger = 200 * time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		// Cancel while the writer sleeps between replica 1 and replica 2.
		time.Sleep(20 * time.Millisecond)
		cancel()
	}()

	if err := ps.Write(ctx, "/partial.bin", []byte("partial")); err != nil {
		t.Fatalf("Write with mid-stagger cancel should keep partial success, got %v", err)
	}
	// The shuffle decides which equal-scored peer takes the first replica, so
	// accept it on either node.
	written := 0
	for _, cm := range []*DefaultCacheManager{okCM, okCM2} {
		cm.mu.RLock()
		if _, ok := cm.entries["/partial.bin"]; ok {
			written++
		}
		cm.mu.RUnlock()
	}
	if written != 1 {
		t.Fatalf("replicas written before cancellation = %d, want exactly 1", written)
	}
}

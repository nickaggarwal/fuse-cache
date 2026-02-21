package cache

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"log"
	"math/big"
	"sync"
	"time"

	"fuse-client/internal/coordinator"
	pb "fuse-client/internal/pb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// PeerStorage implements TierStorage for peer-to-peer storage via gRPC.
type PeerStorage struct {
	coordinator         coordinator.Coordinator
	timeout             time.Duration
	minReplicationCount int
	localPeerID         string
	connMu              sync.RWMutex
	connPool            map[string]*grpc.ClientConn
	metaMu              sync.RWMutex
	peersCache          []*coordinator.PeerInfo
	peersCacheAt        time.Time
	peersCacheTTL       time.Duration
	fileHints           map[string]fileHintCacheEntry
	fileHintsTTL        time.Duration
}

type fileHintCacheEntry struct {
	peerIDs   map[string]struct{}
	expiresAt time.Time
}

// NewPeerStorage creates a new peer storage instance.
func NewPeerStorage(coord coordinator.Coordinator, timeout time.Duration, localPeerID string) (*PeerStorage, error) {
	return &PeerStorage{
		coordinator:         coord,
		timeout:             timeout,
		minReplicationCount: 3,
		localPeerID:         localPeerID,
		connPool:            make(map[string]*grpc.ClientConn),
		peersCacheTTL:       2 * time.Second,
		fileHints:           make(map[string]fileHintCacheEntry),
		fileHintsTTL:        5 * time.Second,
	}, nil
}

// getOrDial returns a cached gRPC connection or dials a new one.
func (ps *PeerStorage) getOrDial(addr string) (*grpc.ClientConn, error) {
	ps.connMu.RLock()
	conn, ok := ps.connPool[addr]
	ps.connMu.RUnlock()
	if ok {
		return conn, nil
	}

	ps.connMu.Lock()
	defer ps.connMu.Unlock()

	// Double-check after acquiring write lock
	if conn, ok := ps.connPool[addr]; ok {
		return conn, nil
	}

	conn, err := grpc.Dial(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to dial peer %s: %v", addr, err)
	}
	ps.connPool[addr] = conn
	return conn, nil
}

// Read reads a file from peer storage using parallel fan-out.
func (ps *PeerStorage) Read(ctx context.Context, path string) ([]byte, error) {
	peers, err := ps.getPeers(ctx)
	if err != nil {
		return nil, err
	}
	preferred, fallback := ps.partitionPeersForPath(ctx, path, peers)

	ordered := make([]*coordinator.PeerInfo, 0, len(preferred)+len(fallback))
	ordered = append(ordered, preferred...)
	ordered = append(ordered, fallback...)
	if len(ordered) == 0 {
		return nil, fmt.Errorf("no active peers available")
	}

	var lastErr error
	for _, peer := range ordered {
		if peer == nil || peer.Status != "active" || peer.GRPCAddress == "" {
			continue
		}
		data, err := ps.readFromPeer(ctx, peer.GRPCAddress, path)
		if err == nil {
			return data, nil
		}
		lastErr = err
	}

	if lastErr != nil {
		return nil, lastErr
	}
	return nil, fmt.Errorf("file not found on any peer")
}

func (ps *PeerStorage) partitionPeersForPath(ctx context.Context, path string, peers []*coordinator.PeerInfo) ([]*coordinator.PeerInfo, []*coordinator.PeerInfo) {
	if len(peers) <= 1 {
		return peers, nil
	}
	peerIDs := ps.peerIDsForPath(ctx, path)
	if len(peerIDs) == 0 {
		return nil, peers
	}

	prioritized := make([]*coordinator.PeerInfo, 0, len(peers))
	rest := make([]*coordinator.PeerInfo, 0, len(peers))
	for _, peer := range peers {
		if peer == nil || peer.ID == "" {
			continue
		}
		if _, ok := peerIDs[peer.ID]; ok {
			prioritized = append(prioritized, peer)
		} else {
			rest = append(rest, peer)
		}
	}
	if len(prioritized) == 0 {
		return nil, peers
	}
	return prioritized, rest
}

func (ps *PeerStorage) peerIDsForPath(ctx context.Context, path string) map[string]struct{} {
	now := time.Now()
	ps.metaMu.RLock()
	if hint, ok := ps.fileHints[path]; ok && now.Before(hint.expiresAt) {
		peerIDs := clonePeerIDSet(hint.peerIDs)
		ps.metaMu.RUnlock()
		return peerIDs
	}
	ps.metaMu.RUnlock()

	out := make(map[string]struct{})
	collect := func(target string) bool {
		if target == "" {
			return false
		}
		callCtx, cancel := context.WithTimeout(ctx, ps.timeout)
		defer cancel()
		locations, err := ps.coordinator.GetFileLocation(callCtx, target)
		if err != nil {
			return false
		}
		found := false
		for _, loc := range locations {
			if loc == nil || loc.PeerID == "" {
				continue
			}
			if loc.PeerID == ps.localPeerID {
				continue
			}
			out[loc.PeerID] = struct{}{}
			found = true
		}
		return found
	}

	if collect(path) {
		ps.putFileHint(path, out)
		return out
	}
	if parent, ok := parentFilePathFromChunkPath(path); ok {
		_ = collect(parent)
		if len(out) > 0 {
			ps.putFileHint(path, out)
		}
	}
	if len(out) == 0 {
		ps.putFileHint(path, out)
	}
	return out
}

func (ps *PeerStorage) putFileHint(path string, peerIDs map[string]struct{}) {
	ps.metaMu.Lock()
	ps.fileHints[path] = fileHintCacheEntry{
		peerIDs:   clonePeerIDSet(peerIDs),
		expiresAt: time.Now().Add(ps.fileHintsTTL),
	}
	ps.metaMu.Unlock()
}

func clonePeerIDSet(src map[string]struct{}) map[string]struct{} {
	dst := make(map[string]struct{}, len(src))
	for k := range src {
		dst[k] = struct{}{}
	}
	return dst
}

// Write writes a file to peer storage with replication.
func (ps *PeerStorage) Write(ctx context.Context, path string, data []byte) error {
	peers, err := ps.getPeers(ctx)
	if err != nil {
		return err
	}

	cryptoShuffle(peers)

	successCount := 0
	var lastErr error

	for _, peer := range peers {
		if successCount >= ps.minReplicationCount {
			break
		}
		if peer.Status != "active" || peer.GRPCAddress == "" {
			continue
		}
		if err := ps.writeToPeer(ctx, peer.GRPCAddress, path, data); err == nil {
			successCount++
		} else {
			lastErr = err
		}
	}

	if successCount > 0 {
		if successCount < ps.minReplicationCount {
			log.Printf("[CACHE] WARNING: peer replication for %s: %d/%d replicas written",
				path, successCount, ps.minReplicationCount)
		}
		return nil
	}

	if lastErr != nil {
		return fmt.Errorf("failed to write to any peer: %v", lastErr)
	}
	return fmt.Errorf("no active peers available")
}

// Delete removes a file from peer storage.
func (ps *PeerStorage) Delete(ctx context.Context, path string) error {
	peers, err := ps.getPeers(ctx)
	if err != nil {
		return err
	}

	var lastErr error
	for _, peer := range peers {
		if peer.Status != "active" || peer.GRPCAddress == "" {
			continue
		}
		if err := ps.deleteFromPeer(ctx, peer.GRPCAddress, path); err != nil {
			lastErr = err
		}
	}
	return lastErr
}

// Exists checks if a file exists in peer storage.
func (ps *PeerStorage) Exists(ctx context.Context, path string) bool {
	peers, err := ps.getPeers(ctx)
	if err != nil {
		return false
	}

	for _, peer := range peers {
		if peer.Status != "active" || peer.GRPCAddress == "" {
			continue
		}
		if ps.existsOnPeer(ctx, peer.GRPCAddress, path) {
			return true
		}
	}
	return false
}

// Size returns the size of a file in peer storage.
func (ps *PeerStorage) Size(ctx context.Context, path string) (int64, error) {
	peers, err := ps.getPeers(ctx)
	if err != nil {
		return 0, err
	}

	for _, peer := range peers {
		if peer.Status != "active" || peer.GRPCAddress == "" {
			continue
		}
		if size, err := ps.sizeOnPeer(ctx, peer.GRPCAddress, path); err == nil {
			return size, nil
		}
	}
	return 0, fmt.Errorf("file not found on any peer")
}

// Close closes all pooled gRPC connections.
func (ps *PeerStorage) Close() {
	ps.connMu.Lock()
	defer ps.connMu.Unlock()
	for addr, conn := range ps.connPool {
		conn.Close()
		delete(ps.connPool, addr)
	}
}

// cryptoShuffle performs a Fisher-Yates shuffle using crypto/rand.
func cryptoShuffle(peers []*coordinator.PeerInfo) {
	for i := len(peers) - 1; i > 0; i-- {
		n, err := rand.Int(rand.Reader, big.NewInt(int64(i+1)))
		if err != nil {
			continue
		}
		j := int(n.Int64())
		peers[i], peers[j] = peers[j], peers[i]
	}
}

func (ps *PeerStorage) getPeers(ctx context.Context) ([]*coordinator.PeerInfo, error) {
	ps.metaMu.RLock()
	if time.Since(ps.peersCacheAt) < ps.peersCacheTTL && len(ps.peersCache) > 0 {
		cached := make([]*coordinator.PeerInfo, 0, len(ps.peersCache))
		for _, peer := range ps.peersCache {
			if peer != nil {
				cached = append(cached, peer)
			}
		}
		ps.metaMu.RUnlock()
		return cached, nil
	}
	ps.metaMu.RUnlock()

	callCtx, cancel := context.WithTimeout(ctx, ps.timeout)
	defer cancel()
	peers, err := ps.coordinator.GetPeers(callCtx, "")
	if err != nil {
		return nil, err
	}
	if ps.localPeerID == "" {
		ps.metaMu.Lock()
		ps.peersCache = peers
		ps.peersCacheAt = time.Now()
		ps.metaMu.Unlock()
		return peers, nil
	}

	filtered := make([]*coordinator.PeerInfo, 0, len(peers))
	for _, peer := range peers {
		if peer == nil || peer.ID == "" {
			continue
		}
		if peer.ID == ps.localPeerID {
			continue
		}
		filtered = append(filtered, peer)
	}
	ps.metaMu.Lock()
	ps.peersCache = filtered
	ps.peersCacheAt = time.Now()
	ps.metaMu.Unlock()
	return filtered, nil
}

func (ps *PeerStorage) readFromPeer(ctx context.Context, grpcAddr, path string) ([]byte, error) {
	conn, err := ps.getOrDial(grpcAddr)
	if err != nil {
		return nil, err
	}

	client := pb.NewPeerServiceClient(conn)
	callCtx, cancel := context.WithTimeout(ctx, ps.timeout)
	defer cancel()
	stream, err := client.ReadFile(callCtx, &pb.ReadFileRequest{Path: path})
	if err != nil {
		return nil, err
	}

	var buf bytes.Buffer
	for {
		chunk, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		buf.Write(chunk.Data)
	}
	return buf.Bytes(), nil
}

func (ps *PeerStorage) writeToPeer(ctx context.Context, grpcAddr, path string, data []byte) error {
	conn, err := ps.getOrDial(grpcAddr)
	if err != nil {
		return err
	}

	client := pb.NewPeerServiceClient(conn)
	callCtx, cancel := context.WithTimeout(ctx, ps.timeout)
	defer cancel()
	stream, err := client.WriteFile(callCtx)
	if err != nil {
		return err
	}

	// Send first message with metadata + first chunk
	chunkSize := grpcChunkSize
	firstEnd := chunkSize
	if firstEnd > len(data) {
		firstEnd = len(data)
	}

	if err := stream.Send(&pb.WriteFileRequest{
		Path:      path,
		TotalSize: int64(len(data)),
		Data:      data[:firstEnd],
	}); err != nil {
		return err
	}

	// Send remaining chunks
	for offset := firstEnd; offset < len(data); offset += chunkSize {
		end := offset + chunkSize
		if end > len(data) {
			end = len(data)
		}
		if err := stream.Send(&pb.WriteFileRequest{
			Data: data[offset:end],
		}); err != nil {
			return err
		}
	}

	_, err = stream.CloseAndRecv()
	return err
}

func (ps *PeerStorage) deleteFromPeer(ctx context.Context, grpcAddr, path string) error {
	conn, err := ps.getOrDial(grpcAddr)
	if err != nil {
		return err
	}
	client := pb.NewPeerServiceClient(conn)
	callCtx, cancel := context.WithTimeout(ctx, ps.timeout)
	defer cancel()
	_, err = client.DeleteFile(callCtx, &pb.DeleteFileRequest{Path: path})
	return err
}

func (ps *PeerStorage) existsOnPeer(ctx context.Context, grpcAddr, path string) bool {
	conn, err := ps.getOrDial(grpcAddr)
	if err != nil {
		return false
	}
	client := pb.NewPeerServiceClient(conn)
	callCtx, cancel := context.WithTimeout(ctx, ps.timeout)
	defer cancel()
	resp, err := client.FileExists(callCtx, &pb.FileExistsRequest{Path: path})
	if err != nil {
		return false
	}
	return resp.Exists
}

func (ps *PeerStorage) sizeOnPeer(ctx context.Context, grpcAddr, path string) (int64, error) {
	conn, err := ps.getOrDial(grpcAddr)
	if err != nil {
		return 0, err
	}
	client := pb.NewPeerServiceClient(conn)
	callCtx, cancel := context.WithTimeout(ctx, ps.timeout)
	defer cancel()
	resp, err := client.FileSize(callCtx, &pb.FileSizeRequest{Path: path})
	if err != nil {
		return 0, err
	}
	return resp.Size, nil
}

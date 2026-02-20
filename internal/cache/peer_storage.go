package cache

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math/big"
	"net/http"
	"time"
)

// PeerStorage implements TierStorage for peer-to-peer storage
type PeerStorage struct {
	coordinatorAddr    string
	timeout            time.Duration
	client             *http.Client
	minReplicationCount int
}

// NewPeerStorage creates a new peer storage instance
func NewPeerStorage(coordinatorAddr string, timeout time.Duration) (*PeerStorage, error) {
	return &PeerStorage{
		coordinatorAddr:    coordinatorAddr,
		timeout:            timeout,
		minReplicationCount: 3,
		client: &http.Client{
			Timeout: timeout,
		},
	}, nil
}

// PeerInfo represents peer information
type PeerInfo struct {
	ID            string `json:"id"`
	Address       string `json:"address"`
	NVMePath      string `json:"nvme_path"`
	AvailSpace    int64  `json:"available_space"`
	UsedSpace     int64  `json:"used_space"`
	Status        string `json:"status"`
	LastHeartbeat int64  `json:"last_heartbeat"`
}

// Read reads a file from peer storage using parallel fan-out
func (ps *PeerStorage) Read(ctx context.Context, path string) ([]byte, error) {
	peers, err := ps.getPeers(ctx)
	if err != nil {
		return nil, err
	}

	type readResult struct {
		data []byte
		err  error
	}

	readCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	resultChan := make(chan readResult, len(peers))
	active := 0

	for _, peer := range peers {
		if peer.Status != "active" {
			continue
		}
		active++
		go func(addr string) {
			data, err := ps.readFromPeer(readCtx, addr, path)
			resultChan <- readResult{data, err}
		}(peer.Address)
	}

	if active == 0 {
		return nil, fmt.Errorf("no active peers available")
	}

	for i := 0; i < active; i++ {
		res := <-resultChan
		if res.err == nil {
			cancel() // cancel remaining reads
			return res.data, nil
		}
	}

	return nil, fmt.Errorf("file not found on any peer")
}

// Write writes a file to peer storage with replication
func (ps *PeerStorage) Write(ctx context.Context, path string, data []byte) error {
	peers, err := ps.getPeers(ctx)
	if err != nil {
		return err
	}

	// Cryptographically secure shuffle
	cryptoShuffle(peers)

	successCount := 0
	var lastErr error

	for _, peer := range peers {
		if successCount >= ps.minReplicationCount {
			break
		}

		if peer.Status != "active" {
			continue
		}

		if err := ps.writeToPeer(ctx, peer.Address, path, data); err == nil {
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

// Delete removes a file from peer storage
func (ps *PeerStorage) Delete(ctx context.Context, path string) error {
	peers, err := ps.getPeers(ctx)
	if err != nil {
		return err
	}

	var lastErr error
	for _, peer := range peers {
		if peer.Status != "active" {
			continue
		}

		if err := ps.deleteFromPeer(ctx, peer.Address, path); err != nil {
			lastErr = err
		}
	}

	return lastErr
}

// Exists checks if a file exists in peer storage
func (ps *PeerStorage) Exists(ctx context.Context, path string) bool {
	peers, err := ps.getPeers(ctx)
	if err != nil {
		return false
	}

	for _, peer := range peers {
		if peer.Status != "active" {
			continue
		}

		if ps.existsOnPeer(ctx, peer.Address, path) {
			return true
		}
	}

	return false
}

// Size returns the size of a file in peer storage
func (ps *PeerStorage) Size(ctx context.Context, path string) (int64, error) {
	peers, err := ps.getPeers(ctx)
	if err != nil {
		return 0, err
	}

	for _, peer := range peers {
		if peer.Status != "active" {
			continue
		}

		if size, err := ps.sizeOnPeer(ctx, peer.Address, path); err == nil {
			return size, nil
		}
	}

	return 0, fmt.Errorf("file not found on any peer")
}

// cryptoShuffle performs a Fisher-Yates shuffle using crypto/rand
func cryptoShuffle(peers []PeerInfo) {
	for i := len(peers) - 1; i > 0; i-- {
		n, err := rand.Int(rand.Reader, big.NewInt(int64(i+1)))
		if err != nil {
			continue
		}
		j := int(n.Int64())
		peers[i], peers[j] = peers[j], peers[i]
	}
}

func (ps *PeerStorage) getPeers(ctx context.Context) ([]PeerInfo, error) {
	url := fmt.Sprintf("http://%s/api/peers", ps.coordinatorAddr)

	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, err
	}

	resp, err := ps.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var peers []PeerInfo
	if err := json.NewDecoder(resp.Body).Decode(&peers); err != nil {
		return nil, err
	}

	return peers, nil
}

func (ps *PeerStorage) readFromPeer(ctx context.Context, peerAddr, path string) ([]byte, error) {
	url := fmt.Sprintf("http://%s/api/files/%s", peerAddr, path)

	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, err
	}

	resp, err := ps.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("peer returned status: %d", resp.StatusCode)
	}

	return io.ReadAll(resp.Body)
}

func (ps *PeerStorage) writeToPeer(ctx context.Context, peerAddr, path string, data []byte) error {
	url := fmt.Sprintf("http://%s/api/files/%s", peerAddr, path)

	req, err := http.NewRequestWithContext(ctx, "PUT", url, bytes.NewReader(data))
	if err != nil {
		return err
	}

	resp, err := ps.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("peer returned status: %d", resp.StatusCode)
	}

	return nil
}

func (ps *PeerStorage) deleteFromPeer(ctx context.Context, peerAddr, path string) error {
	url := fmt.Sprintf("http://%s/api/files/%s", peerAddr, path)

	req, err := http.NewRequestWithContext(ctx, "DELETE", url, nil)
	if err != nil {
		return err
	}

	resp, err := ps.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	return nil
}

func (ps *PeerStorage) existsOnPeer(ctx context.Context, peerAddr, path string) bool {
	url := fmt.Sprintf("http://%s/api/files/%s", peerAddr, path)

	req, err := http.NewRequestWithContext(ctx, "HEAD", url, nil)
	if err != nil {
		return false
	}

	resp, err := ps.client.Do(req)
	if err != nil {
		return false
	}
	defer resp.Body.Close()

	return resp.StatusCode == http.StatusOK
}

func (ps *PeerStorage) sizeOnPeer(ctx context.Context, peerAddr, path string) (int64, error) {
	url := fmt.Sprintf("http://%s/api/files/%s/size", peerAddr, path)

	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return 0, err
	}

	resp, err := ps.client.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("peer returned status: %d", resp.StatusCode)
	}

	var size int64
	if err := json.NewDecoder(resp.Body).Decode(&size); err != nil {
		return 0, err
	}

	return size, nil
}

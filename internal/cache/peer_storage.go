package cache

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
	"time"
)

// PeerStorage implements TierStorage for peer-to-peer storage
type PeerStorage struct {
	coordinatorAddr string
	currentPeerID   string
	timeout         time.Duration
	client          *http.Client
}

// NewPeerStorage creates a new peer storage instance
func NewPeerStorage(coordinatorAddr string, peerID string, timeout time.Duration) (*PeerStorage, error) {
	return &PeerStorage{
		coordinatorAddr: coordinatorAddr,
		currentPeerID:   peerID,
		timeout:         timeout,
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

// Read reads a file from peer storage
func (ps *PeerStorage) Read(ctx context.Context, path string) ([]byte, error) {
	fmt.Printf("[PEER_STORAGE] Reading file %s from peers...\n", path)
	
	peers, err := ps.getPeers(ctx)
	if err != nil {
		fmt.Printf("[PEER_STORAGE] Failed to get peers for Read: %v\n", err)
		return nil, err
	}

	// Try to read from each peer (except self)
	for _, peer := range peers {
		if peer.Status != "active" {
			continue
		}

		// Skip self to avoid reading from ourselves
		if peer.ID == ps.currentPeerID {
			continue
		}

		fmt.Printf("[PEER_STORAGE] Attempting to read from peer %s\n", peer.ID)
		data, err := ps.readFromPeer(ctx, peer.Address, path)
		if err == nil {
			fmt.Printf("[PEER_STORAGE] Successfully read %s from peer %s (%d bytes)\n", path, peer.ID, len(data))
			return data, nil
		} else {
			fmt.Printf("[PEER_STORAGE] Failed to read from peer %s: %v\n", peer.ID, err)
		}
	}

	return nil, fmt.Errorf("file not found on any peer")
}

// Write writes a file to peer storage
func (ps *PeerStorage) Write(ctx context.Context, path string, data []byte) error {
	fmt.Printf("[PEER_STORAGE] Writing file %s (%d bytes) to peers...\n", path, len(data))
	
	peers, err := ps.getPeers(ctx)
	if err != nil {
		fmt.Printf("[PEER_STORAGE] Failed to get peers: %v\n", err)
		return err
	}

	fmt.Printf("[PEER_STORAGE] Found %d peers to replicate to\n", len(peers))

	// Try to write to all available peers (except self)
	successCount := 0
	for _, peer := range peers {
		if peer.Status != "active" {
			fmt.Printf("[PEER_STORAGE] Skipping inactive peer: %s\n", peer.ID)
			continue
		}

		// Skip self to avoid infinite loops
		if peer.ID == ps.currentPeerID {
			fmt.Printf("[PEER_STORAGE] Skipping self peer: %s\n", peer.ID)
			continue
		}

		fmt.Printf("[PEER_STORAGE] Attempting to write to peer %s at %s\n", peer.ID, peer.Address)
		if err := ps.writeToPeer(ctx, peer.Address, path, data); err == nil {
			fmt.Printf("[PEER_STORAGE] Successfully wrote to peer %s\n", peer.ID)
			successCount++
		} else {
			fmt.Printf("[PEER_STORAGE] Failed to write to peer %s: %v\n", peer.ID, err)
		}
	}

	if successCount > 0 {
		fmt.Printf("[PEER_STORAGE] Successfully replicated to %d peers\n", successCount)
		return nil
	}

	return fmt.Errorf("failed to write to any peer")
}

// Delete removes a file from peer storage
func (ps *PeerStorage) Delete(ctx context.Context, path string) error {
	peers, err := ps.getPeers(ctx)
	if err != nil {
		return err
	}

	// Try to delete from all peers
	for _, peer := range peers {
		if peer.Status != "active" {
			continue
		}

		ps.deleteFromPeer(ctx, peer.Address, path)
	}

	return nil
}

// Exists checks if a file exists in peer storage
func (ps *PeerStorage) Exists(ctx context.Context, path string) bool {
	fmt.Printf("[PEER_STORAGE] Checking if file %s exists on peers...\n", path)
	
	peers, err := ps.getPeers(ctx)
	if err != nil {
		fmt.Printf("[PEER_STORAGE] Failed to get peers for Exists check: %v\n", err)
		return false
	}

	fmt.Printf("[PEER_STORAGE] Checking %d peers for file existence\n", len(peers))

	for _, peer := range peers {
		if peer.Status != "active" {
			continue
		}

		// Skip self to avoid checking ourselves
		if peer.ID == ps.currentPeerID {
			continue
		}

		fmt.Printf("[PEER_STORAGE] Checking peer %s for file %s\n", peer.ID, path)
		if ps.existsOnPeer(ctx, peer.Address, path) {
			fmt.Printf("[PEER_STORAGE] File %s found on peer %s\n", path, peer.ID)
			return true
		}
	}

	fmt.Printf("[PEER_STORAGE] File %s not found on any peer\n", path)
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

		// Skip self
		if peer.ID == ps.currentPeerID {
			continue
		}

		if size, err := ps.sizeOnPeer(ctx, peer.Address, path); err == nil {
			return size, nil
		} else {
			// Add verbose logging for debugging
			fmt.Printf("[PEER_STORAGE] Failed to get size for %s from peer %s: %v\n", path, peer.ID, err)
		}
	}

	return 0, fmt.Errorf("file not found on any peer")
}

// Helper methods
func (ps *PeerStorage) getPeers(ctx context.Context) ([]PeerInfo, error) {
	// This would normally make a gRPC call to the coordinator
	// For now, we'll use a REST API call
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
	// Trim leading slash from path to avoid double slashes in URL
	trimmedPath := strings.TrimPrefix(path, "/")
	url := fmt.Sprintf("http://%s/api/files/%s", peerAddr, trimmedPath)

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

	return ioutil.ReadAll(resp.Body)
}

func (ps *PeerStorage) writeToPeer(ctx context.Context, peerAddr, path string, data []byte) error {
	// Trim leading slash from path to avoid double slashes in URL
	trimmedPath := strings.TrimPrefix(path, "/")
	url := fmt.Sprintf("http://%s/api/files/%s", peerAddr, trimmedPath)

	req, err := http.NewRequestWithContext(ctx, "PUT", url, strings.NewReader(string(data)))
	if err != nil {
		return err
	}

	// Add header to indicate this is a peer replication write (prevents infinite loops)
	req.Header.Set("X-Replication", "true")
	req.Header.Set("Content-Type", "application/octet-stream")

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
	// Trim leading slash from path to avoid double slashes in URL
	trimmedPath := strings.TrimPrefix(path, "/")
	url := fmt.Sprintf("http://%s/api/files/%s", peerAddr, trimmedPath)

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
	// Trim leading slash from path to avoid double slashes in URL
	trimmedPath := strings.TrimPrefix(path, "/")
	url := fmt.Sprintf("http://%s/api/files/%s", peerAddr, trimmedPath)

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
	// Trim leading slash from path to avoid double slashes in URL
	trimmedPath := strings.TrimPrefix(path, "/")
	url := fmt.Sprintf("http://%s/api/files/%s/size", peerAddr, trimmedPath)

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

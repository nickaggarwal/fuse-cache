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
	timeout         time.Duration
	client          *http.Client
}

// NewPeerStorage creates a new peer storage instance
func NewPeerStorage(coordinatorAddr string, timeout time.Duration) (*PeerStorage, error) {
	return &PeerStorage{
		coordinatorAddr: coordinatorAddr,
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
	peers, err := ps.getPeers(ctx)
	if err != nil {
		return nil, err
	}

	// Try to read from each peer
	for _, peer := range peers {
		if peer.Status != "active" {
			continue
		}

		data, err := ps.readFromPeer(ctx, peer.Address, path)
		if err == nil {
			return data, nil
		}
	}

	return nil, fmt.Errorf("file not found on any peer")
}

// Write writes a file to peer storage
func (ps *PeerStorage) Write(ctx context.Context, path string, data []byte) error {
	peers, err := ps.getPeers(ctx)
	if err != nil {
		return err
	}

	// Try to write to the first available peer
	for _, peer := range peers {
		if peer.Status != "active" {
			continue
		}

		if err := ps.writeToPeer(ctx, peer.Address, path, data); err == nil {
			return nil
		}
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

	return ioutil.ReadAll(resp.Body)
}

func (ps *PeerStorage) writeToPeer(ctx context.Context, peerAddr, path string, data []byte) error {
	url := fmt.Sprintf("http://%s/api/files/%s", peerAddr, path)

	req, err := http.NewRequestWithContext(ctx, "PUT", url, strings.NewReader(string(data)))
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

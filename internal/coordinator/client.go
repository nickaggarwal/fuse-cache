package coordinator

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

// Coordinator defines the interface for coordinator operations.
// Both CoordinatorService (server-side) and CoordinatorClient (HTTP client) implement this.
type Coordinator interface {
	RegisterPeer(ctx context.Context, peer *PeerInfo) error
	GetPeers(ctx context.Context, requesterID string) ([]*PeerInfo, error)
	UpdatePeerStatus(ctx context.Context, peerID string, status string, availableSpace, usedSpace int64) error
	GetFileLocation(ctx context.Context, filePath string) ([]*FileLocation, error)
	UpdateFileLocation(ctx context.Context, location *FileLocation) error
	GetPeerStats() map[string]interface{}
}

// CoordinatorClient implements Coordinator via HTTP calls to the remote coordinator.
type CoordinatorClient struct {
	addr   string
	client *http.Client
}

// NewCoordinatorClient creates a new HTTP-based coordinator client.
func NewCoordinatorClient(addr string, timeout time.Duration) *CoordinatorClient {
	return &CoordinatorClient{
		addr: addr,
		client: &http.Client{
			Timeout: timeout,
		},
	}
}

func (cc *CoordinatorClient) RegisterPeer(ctx context.Context, peer *PeerInfo) error {
	body, err := json.Marshal(peer)
	if err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(ctx, "POST", fmt.Sprintf("http://%s/api/peers/register", cc.addr), bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := cc.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("register peer failed: %s", string(b))
	}
	return nil
}

func (cc *CoordinatorClient) GetPeers(ctx context.Context, requesterID string) ([]*PeerInfo, error) {
	url := fmt.Sprintf("http://%s/api/peers", cc.addr)
	if requesterID != "" {
		url += "?requester_id=" + requesterID
	}

	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, err
	}

	resp, err := cc.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var peers []*PeerInfo
	if err := json.NewDecoder(resp.Body).Decode(&peers); err != nil {
		return nil, err
	}
	return peers, nil
}

func (cc *CoordinatorClient) UpdatePeerStatus(ctx context.Context, peerID string, status string, availableSpace, usedSpace int64) error {
	payload := map[string]interface{}{
		"peer_id":         peerID,
		"status":          status,
		"available_space": availableSpace,
		"used_space":      usedSpace,
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(ctx, "PUT", fmt.Sprintf("http://%s/api/peers/status", cc.addr), bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := cc.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("update peer status failed: %s", string(b))
	}
	return nil
}

func (cc *CoordinatorClient) GetFileLocation(ctx context.Context, filePath string) ([]*FileLocation, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", fmt.Sprintf("http://%s/api/files/location?path=%s", cc.addr, filePath), nil)
	if err != nil {
		return nil, err
	}

	resp, err := cc.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var locations []*FileLocation
	if err := json.NewDecoder(resp.Body).Decode(&locations); err != nil {
		return nil, err
	}
	return locations, nil
}

func (cc *CoordinatorClient) UpdateFileLocation(ctx context.Context, location *FileLocation) error {
	body, err := json.Marshal(location)
	if err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(ctx, "PUT", fmt.Sprintf("http://%s/api/files/location", cc.addr), bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := cc.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("update file location failed: %s", string(b))
	}
	return nil
}

func (cc *CoordinatorClient) GetPeerStats() map[string]interface{} {
	resp, err := cc.client.Get(fmt.Sprintf("http://%s/api/stats", cc.addr))
	if err != nil {
		return map[string]interface{}{"error": err.Error()}
	}
	defer resp.Body.Close()

	var stats map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&stats); err != nil {
		return map[string]interface{}{"error": err.Error()}
	}
	return stats
}

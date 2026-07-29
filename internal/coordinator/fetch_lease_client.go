package coordinator

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

// Compile-time checks: both coordinator clients speak fetch leases.
var (
	_ FetchLeaser = (*CoordinatorClient)(nil)
	_ FetchLeaser = (*GRPCCoordinatorClient)(nil)
	_ FetchLeaser = (*CoordinatorService)(nil)
)

type fetchLeaseRequest struct {
	Key        string `json:"key"`
	PeerID     string `json:"peer_id"`
	TTLSeconds int64  `json:"ttl_seconds,omitempty"`
}

type fetchLeaseResponse struct {
	Holder  string `json:"holder"`
	Granted bool   `json:"granted"`
}

func fetchLeaseHTTP(ctx context.Context, client *http.Client, addr, method, key, peerID string, ttl time.Duration) (string, bool, error) {
	body, err := json.Marshal(fetchLeaseRequest{
		Key:        key,
		PeerID:     peerID,
		TTLSeconds: int64(ttl / time.Second),
	})
	if err != nil {
		return "", false, err
	}
	req, err := http.NewRequestWithContext(ctx, method, fmt.Sprintf("http://%s/api/fetch-lease", addr), bytes.NewReader(body))
	if err != nil {
		return "", false, err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		return "", false, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", false, fmt.Errorf("fetch lease %s: http %d", method, resp.StatusCode)
	}
	if method == http.MethodDelete {
		return "", false, nil
	}
	var out fetchLeaseResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return "", false, err
	}
	return out.Holder, out.Granted, nil
}

// AcquireFetchLease implements FetchLeaser over the coordinator HTTP API.
func (cc *CoordinatorClient) AcquireFetchLease(ctx context.Context, key, peerID string, ttl time.Duration) (string, bool, error) {
	return fetchLeaseHTTP(ctx, cc.client, cc.addr, http.MethodPost, key, peerID, ttl)
}

// ReleaseFetchLease implements FetchLeaser over the coordinator HTTP API.
func (cc *CoordinatorClient) ReleaseFetchLease(ctx context.Context, key, peerID string) error {
	_, _, err := fetchLeaseHTTP(ctx, cc.client, cc.addr, http.MethodDelete, key, peerID, 0)
	return err
}

// AcquireFetchLease implements FetchLeaser. Leases ride the HTTP fallback path
// (like ListFileLocations / UpdatePeerStatusWithNetwork) — the gRPC surface
// does not carry them yet.
func (c *GRPCCoordinatorClient) AcquireFetchLease(ctx context.Context, key, peerID string, ttl time.Duration) (string, bool, error) {
	return fetchLeaseHTTP(ctx, c.httpClient, c.httpAddr, http.MethodPost, key, peerID, ttl)
}

// ReleaseFetchLease implements FetchLeaser over the HTTP fallback path.
func (c *GRPCCoordinatorClient) ReleaseFetchLease(ctx context.Context, key, peerID string) error {
	_, _, err := fetchLeaseHTTP(ctx, c.httpClient, c.httpAddr, http.MethodDelete, key, peerID, 0)
	return err
}

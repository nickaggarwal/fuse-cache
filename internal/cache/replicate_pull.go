package cache

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"fuse-client/internal/coordinator"
)

// Replicating objects too large to push.
//
// The reconciler pushes small objects (<= reconcileMaxObjectBytes) by
// writing their bytes to a peer. A model's weights are GBs: pushing them
// through one write would hold the whole object in memory, so they were
// never re-replicated at all and spread only by being read. When the only
// fast copy of a hot 15 GB weights file went with a node, the next start
// fetched it from blob storage.
//
// For a large hot object below its replica target the reconciler now asks a
// chosen peer to pull it: POST /api/cache/warm {prefix: <file>, async} on
// that peer, which fetches the whole file peer-first in the background I/O
// class (the same path as a declared warmup), lands it, and publishes itself
// as a holder. The pull counts against the same per-pass budget and holds a
// cluster-wide replication slot while it is requested.

// replicatePullMaxBytes bounds what is asked to be pulled (the warm path's
// headroom guard on the target still applies).
const replicatePullMaxBytes = 200 << 30

// requestPull asks up to count peers not in exclude to pull path. Returns how
// many accepted.
func (ps *PeerStorage) requestPull(ctx context.Context, path string, exclude map[string]struct{}, count int) (int, error) {
	if count <= 0 || ps.httpClient == nil {
		return 0, nil
	}
	peers, err := ps.getPeers(ctx)
	if err != nil {
		return 0, err
	}
	candidates := make([]*coordinator.PeerInfo, 0, len(peers))
	for _, p := range peers {
		if p == nil || p.Status != "active" || p.Address == "" {
			continue
		}
		if _, held := exclude[p.ID]; held {
			continue
		}
		candidates = append(candidates, p)
	}
	cryptoShuffle(candidates)
	sortPeersByReplicationScore(candidates)
	body, _ := json.Marshal(map[string]any{"prefix": path, "mode": "full", "source": "peer-first", "bandwidth": "background", "async": true})
	accepted := 0
	var lastErr error
	for _, p := range candidates {
		if accepted >= count {
			break
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, fmt.Sprintf("http://%s/api/cache/warm", p.Address), bytes.NewReader(body))
		if err != nil {
			return accepted, err
		}
		req.Header.Set("Content-Type", "application/json")
		if ps.apiKey != "" {
			req.Header.Set("X-API-Key", ps.apiKey)
		}
		resp, err := ps.httpClient.Do(req)
		if err != nil {
			lastErr = err
			continue
		}
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4<<10))
		resp.Body.Close()
		if resp.StatusCode == http.StatusAccepted || resp.StatusCode == http.StatusOK {
			accepted++
			continue
		}
		lastErr = fmt.Errorf("pull request to %s: http %d", p.ID, resp.StatusCode)
	}
	return accepted, lastErr
}

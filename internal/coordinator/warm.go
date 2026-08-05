package coordinator

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"sort"
	"strings"
	"sync"
)

// Cluster-wide declarative warmup: POST /api/warm selects peers by explicit
// node IDs, labels, and/or a percentage, then fans the warm request out to
// each selected peer's /api/cache/warm endpoint (async on the peer side).
// Selection is intersection semantics: nodes ∩ labels first, then the
// percentage is applied to what remains. All fields empty = every active
// peer, matching RestoreOptions.SeedPercentage defaults.

// WarmSelector picks which active peers to warm.
type WarmSelector struct {
	// Nodes are explicit peer IDs. Empty = no ID filter.
	Nodes []string `json:"nodes,omitempty"`
	// Labels must all match the peer's registered labels. Empty = no filter.
	Labels map[string]string `json:"labels,omitempty"`
	// Percentage (1..100) of the filtered set to warm, ceil-rounded over the
	// ID-sorted list so the choice is deterministic. <=0 or >100 = 100.
	Percentage int `json:"percentage,omitempty"`
}

// WarmRequest is the coordinator-side fan-out request.
type WarmRequest struct {
	Prefix    string `json:"prefix"`
	Mode      string `json:"mode,omitempty"`      // default "full"
	Source    string `json:"source,omitempty"`    // peer-first | cloud-first | cloud-only
	Bandwidth string `json:"bandwidth,omitempty"` // background | max
	WarmSelector
	// APIKey is forwarded as X-API-Key to peers whose client API requires it.
	APIKey string `json:"api_key,omitempty"`
}

// WarmFanoutResult reports which peers accepted the warm trigger.
type WarmFanoutResult struct {
	Prefix   string            `json:"prefix"`
	Selected int               `json:"selected"`
	Accepted []string          `json:"accepted"`
	Failed   map[string]string `json:"failed,omitempty"`
}

// SelectWarmTargets returns the active peers matching sel, ID-sorted.
func (cs *CoordinatorService) SelectWarmTargets(ctx context.Context, sel WarmSelector) ([]*PeerInfo, error) {
	peers, err := cs.store.ListPeers(ctx)
	if err != nil {
		return nil, err
	}
	wantIDs := make(map[string]struct{}, len(sel.Nodes))
	for _, id := range sel.Nodes {
		wantIDs[id] = struct{}{}
	}

	var out []*PeerInfo
	for _, p := range peers {
		if p == nil || p.Status != "active" {
			continue
		}
		if len(wantIDs) > 0 {
			if _, ok := wantIDs[p.ID]; !ok {
				continue
			}
		}
		if !labelsMatch(p.Labels, sel.Labels) {
			continue
		}
		out = append(out, p)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })

	if pct := sel.Percentage; pct > 0 && pct < 100 && len(out) > 0 {
		n := int(math.Ceil(float64(len(out)) * float64(pct) / 100.0))
		if n < 1 {
			n = 1
		}
		out = out[:n]
	}
	return out, nil
}

func labelsMatch(have, want map[string]string) bool {
	for k, v := range want {
		if have[k] != v {
			return false
		}
	}
	return true
}

// WarmPeers fans req out to every selected peer. Peer-side warms run async;
// this returns as soon as every peer has accepted or refused the trigger.
func (cs *CoordinatorService) WarmPeers(ctx context.Context, req WarmRequest) (*WarmFanoutResult, error) {
	if strings.TrimSpace(req.Prefix) == "" {
		return nil, errors.New("prefix is required")
	}
	if req.Mode == "" {
		req.Mode = "full"
	}
	targets, err := cs.SelectWarmTargets(ctx, req.WarmSelector)
	if err != nil {
		return nil, fmt.Errorf("select peers: %w", err)
	}
	if len(targets) == 0 {
		return nil, errors.New("no active peers match the selector")
	}

	body, err := json.Marshal(map[string]interface{}{
		"prefix":    req.Prefix,
		"mode":      req.Mode,
		"source":    req.Source,
		"bandwidth": req.Bandwidth,
		"async":     true,
	})
	if err != nil {
		return nil, err
	}

	result := &WarmFanoutResult{
		Prefix:   req.Prefix,
		Selected: len(targets),
		Failed:   make(map[string]string),
	}
	var (
		mu sync.Mutex
		wg sync.WaitGroup
	)
	for _, target := range targets {
		target := target
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := postWarmToPeer(ctx, target.Address, body, req.APIKey)
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				result.Failed[target.ID] = err.Error()
				cs.logger.Printf("warm %s -> %s: %v", req.Prefix, target.ID, err)
			} else {
				result.Accepted = append(result.Accepted, target.ID)
			}
		}()
	}
	wg.Wait()
	sort.Strings(result.Accepted)
	if len(result.Failed) == 0 {
		result.Failed = nil
	}
	return result, nil
}

func postWarmToPeer(ctx context.Context, peerAddr string, body []byte, apiKey string) error {
	url := fmt.Sprintf("http://%s/api/cache/warm", peerAddr)
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return err
	}
	httpReq.Header.Set("Content-Type", "application/json")
	if apiKey != "" {
		httpReq.Header.Set("X-API-Key", apiKey)
	}
	resp, err := coordinatorHTTPClient.Do(httpReq)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted && resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return fmt.Errorf("status=%d body=%s", resp.StatusCode, strings.TrimSpace(string(b)))
	}
	return nil
}

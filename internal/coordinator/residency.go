package coordinator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	neturl "net/url"
	"sort"
	"strings"
	"sync"
	"time"
)

// Cluster-wide residency: GET /api/residency?prefix=<p> answers "which nodes
// currently hold this prefix, and how completely" — the signal an external
// scheduler needs to prefer a node that can cold-start from NVMe instead of
// paying peer or cloud latency.
//
// This deliberately does NOT answer from the coordinator's own location
// metadata, even though that would be a single store walk with no network.
// Location rows are published on write and on warm but are never retracted by
// LRU eviction (only explicit delete/rename retract them), so the store
// systematically over-claims residency, and GetWorldView inherits that
// optimism. The only authority is each node's own disk, so this fans out and
// asks. See internal/cache/residency.go for the node-local half.
//
// A router polls this on a request-adjacent path, so the fan-out is behind a
// short TTL cache plus in-flight coalescing: N concurrent pollers for the same
// prefix produce one round of peer requests, not N.

const (
	// residencyTTL is how long a fan-out result is reused. Short enough that a
	// node dropping out of residency is reflected within a couple of routing
	// decisions, long enough that a busy router does not turn every inference
	// request into a cluster-wide fan-out.
	residencyTTL = 5 * time.Second
	// residencyPeerTimeout bounds one peer's answer. Residency is advisory: a
	// slow node is reported as unknown rather than allowed to stall the whole
	// query, because a router blocked on this is worse than a router routing
	// without it.
	residencyPeerTimeout = 2 * time.Second
)

// residencyHTTPClient is separate from coordinatorHTTPClient, whose 5-minute
// timeout is sized for seeding large files and would let one wedged peer hold
// a routing query open indefinitely.
var residencyHTTPClient = &http.Client{
	Timeout: residencyPeerTimeout,
	Transport: &http.Transport{
		DialContext:           (&net.Dialer{Timeout: 1 * time.Second}).DialContext,
		ResponseHeaderTimeout: residencyPeerTimeout,
		MaxIdleConns:          64,
		MaxIdleConnsPerHost:   8,
	},
}

// NodeResidency is one node's answer.
type NodeResidency struct {
	// NodeName is the Kubernetes node name, the join key an external scheduler
	// needs. Empty on peers registered outside Kubernetes or by older clients.
	NodeName string `json:"node_name,omitempty"`
	PeerID   string `json:"peer_id"`
	Address  string `json:"address,omitempty"`

	Files         int     `json:"files"`
	ResidentFiles int     `json:"resident_files"`
	TotalBytes    int64   `json:"total_bytes"`
	ResidentBytes int64   `json:"resident_bytes"`
	Fraction      float64 `json:"fraction"`

	Pinned bool `json:"pinned"`
	// Declared reports that a warm intent covers this prefix on that node;
	// LastVerified is that intent's last CLEAN reconcile pass. A high Fraction
	// with an old LastVerified means "the bytes are here, nobody re-checked
	// recently" — routable, but a router that wants freshness can discount it.
	Declared     bool      `json:"declared"`
	LastVerified time.Time `json:"last_verified,omitempty"`

	// Unknown marks a node that did not answer. It is NOT the same as a node
	// that answered zero, and a scorer must not treat it as such: dropping
	// unreachable nodes silently would make a transient blip look like a
	// deliberate "this node holds nothing".
	Unknown bool   `json:"unknown,omitempty"`
	Error   string `json:"error,omitempty"`
}

// ClusterResidency is the aggregated answer for one prefix.
type ClusterResidency struct {
	Prefix      string           `json:"prefix"`
	GeneratedAt time.Time        `json:"generated_at"`
	Nodes       []NodeResidency  `json:"nodes"`
	Summary     ResidencySummary `json:"summary"`
	// Cached reports that this was served from the TTL cache rather than a
	// fresh fan-out, so a caller can tell staleness apart from agreement.
	Cached bool `json:"cached,omitempty"`
}

type ResidencySummary struct {
	Peers int `json:"peers"`
	// FullyResident counts nodes at fraction >= residencyFullThreshold, the
	// ones that can serve the whole prefix locally.
	FullyResident int `json:"fully_resident"`
	Partial       int `json:"partial"`
	Unknown       int `json:"unknown"`
	// BestFraction is the highest fraction any node reported — the headline a
	// router wants when deciding whether locality is available at all.
	BestFraction float64 `json:"best_fraction"`
}

// residencyFullThreshold treats "essentially all of it" as full. An exact 1.0
// requirement makes the summary flap on a single in-flight file.
const residencyFullThreshold = 0.999

type residencyEntry struct {
	result *ClusterResidency
	at     time.Time
}

type residencyCache struct {
	mu      sync.Mutex
	entries map[string]residencyEntry
	// inflight coalesces concurrent queries for the same prefix onto one
	// fan-out. Without it, a router polling from several replicas multiplies
	// a cache miss by the number of pollers.
	inflight map[string]chan struct{}
}

func (c *residencyCache) get(prefix string, ttl time.Duration) (*ClusterResidency, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.entries[prefix]
	if !ok || time.Since(e.at) > ttl {
		return nil, false
	}
	return e.result, true
}

func (c *residencyCache) put(prefix string, res *ClusterResidency) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.entries == nil {
		c.entries = make(map[string]residencyEntry)
	}
	c.entries[prefix] = residencyEntry{result: res, at: time.Now()}
	// Bound the map: a router may query many distinct prefixes over a long
	// run, and nothing else ever evicts from here.
	if len(c.entries) > 256 {
		cutoff := time.Now().Add(-10 * residencyTTL)
		for k, v := range c.entries {
			if v.at.Before(cutoff) {
				delete(c.entries, k)
			}
		}
	}
}

// PrefixResidency asks every active peer how much of prefix it currently holds
// and aggregates the answers. Results are cached for residencyTTL and
// concurrent callers for the same prefix share one fan-out.
//
// apiKey is forwarded as X-API-Key to peers whose client API requires it.
func (cs *CoordinatorService) PrefixResidency(ctx context.Context, prefix, apiKey string) (*ClusterResidency, error) {
	prefix = strings.TrimSpace(prefix)
	if prefix == "" {
		return nil, errors.New("prefix is required")
	}
	if cached, ok := cs.residency.get(prefix, residencyTTL); ok {
		copied := *cached
		copied.Cached = true
		return &copied, nil
	}

	// Coalesce: whoever installs the channel does the fan-out, everyone else
	// waits for it and then re-reads the cache.
	cs.residency.mu.Lock()
	if cs.residency.inflight == nil {
		cs.residency.inflight = make(map[string]chan struct{})
	}
	if wait, running := cs.residency.inflight[prefix]; running {
		cs.residency.mu.Unlock()
		select {
		case <-wait:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		if cached, ok := cs.residency.get(prefix, residencyTTL); ok {
			copied := *cached
			copied.Cached = true
			return &copied, nil
		}
		// The leader failed and cached nothing; fall through and do it
		// ourselves rather than reporting an error we never observed.
	} else {
		done := make(chan struct{})
		cs.residency.inflight[prefix] = done
		cs.residency.mu.Unlock()
		defer func() {
			cs.residency.mu.Lock()
			delete(cs.residency.inflight, prefix)
			cs.residency.mu.Unlock()
			close(done)
		}()
	}

	res, err := cs.queryPrefixResidency(ctx, prefix, apiKey)
	if err != nil {
		return nil, err
	}
	cs.residency.put(prefix, res)
	return res, nil
}

func (cs *CoordinatorService) queryPrefixResidency(ctx context.Context, prefix, apiKey string) (*ClusterResidency, error) {
	peers, err := cs.store.ListPeers(ctx)
	if err != nil {
		return nil, fmt.Errorf("list peers: %w", err)
	}
	staleBefore := time.Now().Add(-warmPeerStaleAfter)
	var targets []*PeerInfo
	for _, p := range peers {
		if p == nil || p.Status != "active" {
			continue
		}
		// Same staleness rule as warm fan-out: a crashed pod keeps "active"
		// until its lease expires, and reporting it as unknown-but-present
		// would keep a dead node in the routing candidate set for 90s.
		if !p.LastHeartbeat.IsZero() && p.LastHeartbeat.Before(staleBefore) {
			continue
		}
		targets = append(targets, p)
	}

	out := &ClusterResidency{
		Prefix:      prefix,
		GeneratedAt: time.Now(),
		Nodes:       make([]NodeResidency, len(targets)),
	}
	var wg sync.WaitGroup
	for i, p := range targets {
		i, p := i, p
		wg.Add(1)
		go func() {
			defer wg.Done()
			n := NodeResidency{NodeName: p.NodeName, PeerID: p.ID, Address: p.Address}
			rep, err := fetchPeerResidency(ctx, p.Address, prefix, apiKey)
			if err != nil {
				n.Unknown = true
				n.Error = err.Error()
			} else {
				n.Files = rep.Files
				n.ResidentFiles = rep.ResidentFiles
				n.TotalBytes = rep.Bytes
				n.ResidentBytes = rep.ResidentBytes
				n.Fraction = rep.Fraction
				n.Pinned = rep.Pinned
				n.Declared = rep.Declared
				n.LastVerified = rep.LastVerified
			}
			out.Nodes[i] = n
		}()
	}
	wg.Wait()

	sort.Slice(out.Nodes, func(i, j int) bool {
		if out.Nodes[i].Fraction != out.Nodes[j].Fraction {
			return out.Nodes[i].Fraction > out.Nodes[j].Fraction
		}
		return out.Nodes[i].PeerID < out.Nodes[j].PeerID
	})
	out.Summary.Peers = len(out.Nodes)
	for _, n := range out.Nodes {
		switch {
		case n.Unknown:
			out.Summary.Unknown++
		case n.Fraction >= residencyFullThreshold:
			out.Summary.FullyResident++
		case n.Fraction > 0:
			out.Summary.Partial++
		}
		if !n.Unknown && n.Fraction > out.Summary.BestFraction {
			out.Summary.BestFraction = n.Fraction
		}
	}
	return out, nil
}

// peerResidencyReport mirrors cache.ResidencyReport's JSON. It is duplicated
// rather than imported because internal/coordinator must not depend on
// internal/cache (cache already imports coordinator).
type peerResidencyReport struct {
	Files         int       `json:"files"`
	ResidentFiles int       `json:"resident_files"`
	Bytes         int64     `json:"bytes"`
	ResidentBytes int64     `json:"resident_bytes"`
	Fraction      float64   `json:"fraction"`
	Pinned        bool      `json:"pinned"`
	Declared      bool      `json:"declared"`
	LastVerified  time.Time `json:"last_verified"`
}

func fetchPeerResidency(ctx context.Context, peerAddr, prefix, apiKey string) (*peerResidencyReport, error) {
	ctx, cancel := context.WithTimeout(ctx, residencyPeerTimeout)
	defer cancel()

	endpoint := fmt.Sprintf("http://%s/api/cache/residency?prefix=%s", peerAddr, neturl.QueryEscape(prefix))
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, err
	}
	if apiKey != "" {
		req.Header.Set("X-API-Key", apiKey)
	}
	resp, err := residencyHTTPClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return nil, fmt.Errorf("status=%d body=%s", resp.StatusCode, strings.TrimSpace(string(b)))
	}
	var rep peerResidencyReport
	if err := json.NewDecoder(io.LimitReader(resp.Body, 1<<16)).Decode(&rep); err != nil {
		return nil, fmt.Errorf("decode: %w", err)
	}
	return &rep, nil
}

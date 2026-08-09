// Package residency is a client for the fuse-client coordinator's
// GET /api/residency?prefix=<p> endpoint.
//
// It is deliberately free of any llm-d dependency so the wire contract can be
// tested on its own, and so a future consumer that is not an EndpointPicker can
// reuse it.
//
// The contract that matters here is that the coordinator distinguishes three
// states per node, and this client must not collapse them:
//
//	resident   — the node was asked and answered with a measured fraction
//	unknown    — the node could not be asked (timeout, refused, 5xx)
//	absent     — the node is not in the report at all
//
// "Unknown" and "zero" look identical to a scorer and mean opposite things: a
// node that briefly failed to answer is probably still the best node in the
// cluster, while a node measured at 0.0 is the worst. Conflating them deroutes
// a healthy node on a transient blip. The coordinator is careful about this
// (`Unknown bool` on each entry rather than a zero fraction) and so is this
// client.
package residency

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// NodeResidency mirrors one entry of the coordinator's response. Field names
// track internal/coordinator/residency.go; unknown fields are ignored, so the
// coordinator may add more without breaking this client.
type NodeResidency struct {
	NodeName string `json:"node_name"`
	// NodeIP is status.hostIP of the node the peer runs on. This is the join
	// key: an EndpointPicker sees pod.Status.HostIP on its candidate endpoints
	// and no node name at all.
	NodeIP  string `json:"node_ip"`
	PeerID  string `json:"peer_id"`
	Address string `json:"address"`

	Files         int   `json:"files"`
	ResidentFiles int   `json:"resident_files"`
	TotalBytes    int64 `json:"total_bytes"`
	ResidentBytes int64 `json:"resident_bytes"`
	// Fraction is byte-weighted, not file-counted: a node holding the 12MiB
	// half of a 4MiB+12MiB pair reads 0.75, not 0.5. For model weights the
	// bytes are what determine load time, so a file count would be the wrong
	// signal even though it is the easier one to compute.
	Fraction float64 `json:"fraction"`

	Pinned bool `json:"pinned"`
	// Declared reports that a warm intent covers this prefix on that node.
	// LastVerified is that intent's last CLEAN reconcile pass — deliberately
	// not "last attempted", so a stale timestamp means nobody has confirmed
	// residency recently rather than that the check merely ran.
	Declared     bool      `json:"declared"`
	LastVerified time.Time `json:"last_verified"`

	// Unknown is set when the coordinator could not reach this peer. When it
	// is true every count above is meaningless, not zero.
	Unknown bool   `json:"unknown"`
	Error   string `json:"error"`
}

// Report is the coordinator's whole answer for one prefix.
type Report struct {
	Prefix      string          `json:"prefix"`
	GeneratedAt time.Time       `json:"generated_at"`
	Nodes       []NodeResidency `json:"nodes"`
	Cached      bool            `json:"cached"`
}

// Client queries one coordinator.
type Client struct {
	baseURL string
	apiKey  string
	http    *http.Client
}

// New returns a Client for the given coordinator base URL (e.g.
// "http://coordinator.fuse-system.svc:8080").
//
// timeout bounds a single query. It should be well under the extractor's poll
// interval: the coordinator fans out to every peer, and a wedged peer costs it
// a 2s peer timeout, so this call is slower than a typical HTTP GET and must
// never be allowed to pile up.
func New(baseURL, apiKey string, timeout time.Duration) *Client {
	return &Client{
		baseURL: strings.TrimSuffix(baseURL, "/"),
		apiKey:  apiKey,
		http:    &http.Client{Timeout: timeout},
	}
}

// Fetch returns the residency report for prefix.
//
// An error here means the whole report is unavailable — the caller must treat
// every node as unknown and leave previously-stamped values alone, not zero
// them. Zeroing on a coordinator blip would swing routing away from the warm
// nodes at exactly the moment there is no evidence to justify it.
func (c *Client) Fetch(ctx context.Context, prefix string) (*Report, error) {
	endpoint := c.baseURL + "/api/residency?prefix=" + url.QueryEscape(prefix)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, fmt.Errorf("build residency request: %w", err)
	}
	if c.apiKey != "" {
		req.Header.Set("X-API-Key", c.apiKey)
	}

	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("query residency for %q: %w", prefix, err)
	}
	defer resp.Body.Close()

	// 503 is the coordinator's documented "I could not enumerate" answer. It is
	// an error rather than an empty report on purpose, at both ends: an empty
	// node list would read as "nothing is cached anywhere", which is the same
	// unknown-vs-zero confusion one level up.
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("residency for %q: coordinator returned %s", prefix, resp.Status)
	}

	var report Report
	if err := json.NewDecoder(resp.Body).Decode(&report); err != nil {
		return nil, fmt.Errorf("decode residency for %q: %w", prefix, err)
	}
	return &report, nil
}

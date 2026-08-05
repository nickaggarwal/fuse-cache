// Package nodelabels reads a subset of the Kubernetes Node object's labels so
// a peer can register with labels that reflect the node it runs on.
//
// The downward API cannot expose node labels — only spec.nodeName — so warm
// targeting by pool/zone otherwise has to hardcode the value per DaemonSet,
// which is wrong the moment a cluster has more than one nodepool. This reads
// the Node object directly over the in-cluster REST API using the projected
// service-account token: no client-go dependency, one GET at startup.
//
// It is advisory, like the rest of the coordination here. Any failure (no
// token, no RBAC, API server unreachable) returns no labels and the caller
// falls back to whatever -peer-labels / FUSE_PEER_LABELS supplied.
package nodelabels

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"
)

const (
	tokenPath  = "/var/run/secrets/kubernetes.io/serviceaccount/token"
	caPath     = "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"
	fetchLimit = 1 << 20 // node objects are small; cap the read anyway
)

// Config selects which node labels to import and where to read them from.
type Config struct {
	// NodeName is the node to read (normally the NODE_NAME downward-API env).
	NodeName string
	// Keys are the node label keys to import, e.g.
	// ["agentpool", "topology.kubernetes.io/zone"]. Empty means "import
	// nothing" — this never slurps the whole label set, which would put
	// dozens of kubernetes.io/* keys into the peer registry.
	Keys []string
	// Rename maps a node label key to the peer label key to publish it as,
	// e.g. {"agentpool": "pool"}. Unmapped keys keep their name.
	Rename map[string]string
	// Timeout bounds the single API call. Zero uses 5s.
	Timeout time.Duration
}

// ParseSpec parses the -peer-labels-from-node spec: a comma-separated list of
// node label keys, each optionally renamed with "peerKey=nodeKey".
//
//	"pool=agentpool,topology.kubernetes.io/zone"
//	  -> reads agentpool (published as "pool") and the zone label (as itself)
func ParseSpec(raw string) ([]string, map[string]string) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, nil
	}
	var keys []string
	rename := make(map[string]string)
	for _, part := range strings.Split(raw, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		// Rename form is peerKey=nodeKey. Node label keys can contain "/" and
		// "." but not "=", so a single Cut is unambiguous.
		if peerKey, nodeKey, ok := strings.Cut(part, "="); ok {
			peerKey, nodeKey = strings.TrimSpace(peerKey), strings.TrimSpace(nodeKey)
			if peerKey == "" || nodeKey == "" {
				continue
			}
			keys = append(keys, nodeKey)
			rename[nodeKey] = peerKey
			continue
		}
		keys = append(keys, part)
	}
	if len(rename) == 0 {
		rename = nil
	}
	return keys, rename
}

// Fetch reads cfg.Keys off the node and returns them as peer labels. A key
// absent from the node is omitted rather than published empty.
func Fetch(ctx context.Context, cfg Config) (map[string]string, error) {
	if cfg.NodeName == "" {
		return nil, fmt.Errorf("node name is empty (set NODE_NAME via the downward API)")
	}
	if len(cfg.Keys) == 0 {
		return nil, nil
	}
	token, err := os.ReadFile(tokenPath)
	if err != nil {
		return nil, fmt.Errorf("read service account token: %w", err)
	}
	client, err := inClusterClient(cfg.Timeout)
	if err != nil {
		return nil, err
	}
	host, port := os.Getenv("KUBERNETES_SERVICE_HOST"), os.Getenv("KUBERNETES_SERVICE_PORT")
	if host == "" || port == "" {
		return nil, fmt.Errorf("not running in a cluster (KUBERNETES_SERVICE_HOST/PORT unset)")
	}
	endpoint := fmt.Sprintf("https://%s/api/v1/nodes/%s",
		net.JoinHostPort(host, port), url.PathEscape(cfg.NodeName))

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+strings.TrimSpace(string(token)))
	req.Header.Set("Accept", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("GET node %s: status %d", cfg.NodeName, resp.StatusCode)
	}
	var node struct {
		Metadata struct {
			Labels map[string]string `json:"labels"`
		} `json:"metadata"`
	}
	dec := json.NewDecoder(&limitReader{r: resp.Body, n: fetchLimit})
	if err := dec.Decode(&node); err != nil {
		return nil, fmt.Errorf("decode node %s: %w", cfg.NodeName, err)
	}
	return Select(node.Metadata.Labels, cfg.Keys, cfg.Rename), nil
}

// Select projects the requested keys out of a node label map, applying rename.
// Split out from Fetch so it is testable without an API server.
func Select(nodeLabels map[string]string, keys []string, rename map[string]string) map[string]string {
	out := make(map[string]string)
	for _, key := range keys {
		v, ok := nodeLabels[key]
		if !ok || v == "" {
			continue
		}
		name := key
		if alias, ok := rename[key]; ok {
			name = alias
		}
		out[name] = v
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func inClusterClient(timeout time.Duration) (*http.Client, error) {
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	ca, err := os.ReadFile(caPath)
	if err != nil {
		return nil, fmt.Errorf("read cluster CA: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(ca) {
		return nil, fmt.Errorf("cluster CA at %s is not valid PEM", caPath)
	}
	return &http.Client{
		Timeout: timeout,
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12},
		},
	}, nil
}

// limitReader is io.LimitedReader with an error instead of a silent EOF, so an
// oversized body is a decode failure rather than truncated JSON.
type limitReader struct {
	r interface{ Read([]byte) (int, error) }
	n int64
}

func (l *limitReader) Read(p []byte) (int, error) {
	if l.n <= 0 {
		return 0, fmt.Errorf("node object exceeds %d bytes", int64(fetchLimit))
	}
	if int64(len(p)) > l.n {
		p = p[:l.n]
	}
	n, err := l.r.Read(p)
	l.n -= int64(n)
	return n, err
}

package coordinator

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// Cluster flags and peer failure reports, after Ceph's OSD map flags and its
// down-reporting rule.
//
// Flags: an operator sets "norebalance" before a rolling restart of the client
// DaemonSet. Clients read flags from GET /api/health and pause re-replication
// while it is set, so a node that is only restarting does not get its files
// copied elsewhere and then copied back. Every flag carries an expiry so a
// forgotten one clears itself.
//
// Failure reports: a client that repeatedly fails to read from a peer reports
// it. A peer is marked suspect only when at least MinFailureReporters distinct
// peers reported it within FailureReportTTL, so one badly connected node
// cannot steer the whole cluster away from a healthy holder. Suspect peers
// stay listed (their heartbeat is fine, and they may be the only holder) but
// readers try them last. Reports expire; a recovered peer stops being suspect
// without anyone clearing it.

const (
	// FlagNoRebalance pauses replica re-replication cluster-wide.
	FlagNoRebalance = "norebalance"
	// FailureReportTTL is how long one report counts.
	FailureReportTTL = 60 * time.Second
	// DefaultMinFailureReporters is Ceph's mon_osd_min_down_reporters.
	DefaultMinFailureReporters = 2
	// maxFlagTTL bounds a flag's lifetime.
	maxFlagTTL = 24 * time.Hour
)

// ClusterFlagStore is implemented by stores that keep cluster flags.
type ClusterFlagStore interface {
	SetClusterFlag(ctx context.Context, name string, until time.Time) error
	ClearClusterFlag(ctx context.Context, name string) error
	// ClusterFlags returns unexpired flags and their expiry.
	ClusterFlags(ctx context.Context) (map[string]time.Time, error)
}

// FailureReportStore is implemented by stores that keep peer failure reports.
type FailureReportStore interface {
	ReportPeerFailure(ctx context.Context, target, reporter string, ttl time.Duration) error
	// FailureReporters returns, per target, the distinct unexpired reporters.
	FailureReporters(ctx context.Context) (map[string][]string, error)
}

// ---- in-memory ----

type memPeerHealth struct {
	mu      sync.Mutex
	flags   map[string]time.Time
	reports map[string]map[string]time.Time // target -> reporter -> expiry
}

func (s *InMemoryStore) ph() *memPeerHealth {
	s.phOnce.Do(func() {
		s.peerHealth = &memPeerHealth{flags: map[string]time.Time{}, reports: map[string]map[string]time.Time{}}
	})
	return s.peerHealth
}

func (s *InMemoryStore) SetClusterFlag(_ context.Context, name string, until time.Time) error {
	h := s.ph()
	h.mu.Lock()
	defer h.mu.Unlock()
	h.flags[name] = until
	return nil
}

func (s *InMemoryStore) ClearClusterFlag(_ context.Context, name string) error {
	h := s.ph()
	h.mu.Lock()
	defer h.mu.Unlock()
	delete(h.flags, name)
	return nil
}

func (s *InMemoryStore) ClusterFlags(_ context.Context) (map[string]time.Time, error) {
	h := s.ph()
	h.mu.Lock()
	defer h.mu.Unlock()
	now := time.Now()
	out := map[string]time.Time{}
	for k, v := range h.flags {
		if now.Before(v) {
			out[k] = v
		} else {
			delete(h.flags, k)
		}
	}
	return out, nil
}

func (s *InMemoryStore) ReportPeerFailure(_ context.Context, target, reporter string, ttl time.Duration) error {
	h := s.ph()
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.reports[target] == nil {
		h.reports[target] = map[string]time.Time{}
	}
	h.reports[target][reporter] = time.Now().Add(ttl)
	return nil
}

func (s *InMemoryStore) FailureReporters(_ context.Context) (map[string][]string, error) {
	h := s.ph()
	h.mu.Lock()
	defer h.mu.Unlock()
	now := time.Now()
	out := map[string][]string{}
	for target, rs := range h.reports {
		for r, exp := range rs {
			if now.Before(exp) {
				out[target] = append(out[target], r)
			} else {
				delete(rs, r)
			}
		}
		if len(rs) == 0 {
			delete(h.reports, target)
		}
	}
	return out, nil
}

// ---- etcd ----
//
// Flags: one key per flag under <prefix>/flags/, value = expiry (RFC 3339),
// with an etcd lease so an expired flag also disappears from the store.
// Reports: <prefix>/failreports/<target>/<reporter>, each with a lease of the
// report TTL, so expiry needs no sweeper.

func (s *EtcdStore) flagKey(name string) string { return s.prefix + "/flags/" + name }
func (s *EtcdStore) reportKey(target, reporter string) string {
	return s.prefix + "/failreports/" + target + "/" + reporter
}

func (s *EtcdStore) putLeased(ctx context.Context, key, val string, ttl time.Duration) error {
	sec := int64(ttl / time.Second)
	if sec < 1 {
		sec = 1
	}
	grant, err := s.client.Grant(ctx, sec)
	if err != nil {
		return err
	}
	_, err = s.client.Put(ctx, key, val, clientv3.WithLease(grant.ID))
	return err
}

func (s *EtcdStore) SetClusterFlag(ctx context.Context, name string, until time.Time) error {
	return s.putLeased(ctx, s.flagKey(name), until.UTC().Format(time.RFC3339), time.Until(until))
}

func (s *EtcdStore) ClearClusterFlag(ctx context.Context, name string) error {
	_, err := s.client.Delete(ctx, s.flagKey(name))
	return err
}

func (s *EtcdStore) ClusterFlags(ctx context.Context) (map[string]time.Time, error) {
	resp, err := s.client.Get(ctx, s.prefix+"/flags/", clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}
	now := time.Now()
	out := map[string]time.Time{}
	for _, kv := range resp.Kvs {
		until, err := time.Parse(time.RFC3339, string(kv.Value))
		if err != nil || !now.Before(until) {
			continue
		}
		out[strings.TrimPrefix(string(kv.Key), s.prefix+"/flags/")] = until
	}
	return out, nil
}

func (s *EtcdStore) ReportPeerFailure(ctx context.Context, target, reporter string, ttl time.Duration) error {
	return s.putLeased(ctx, s.reportKey(target, reporter), "1", ttl)
}

func (s *EtcdStore) FailureReporters(ctx context.Context) (map[string][]string, error) {
	pfx := s.prefix + "/failreports/"
	resp, err := s.client.Get(ctx, pfx, clientv3.WithPrefix(), clientv3.WithKeysOnly())
	if err != nil {
		return nil, err
	}
	out := map[string][]string{}
	for _, kv := range resp.Kvs {
		target, reporter, ok := strings.Cut(strings.TrimPrefix(string(kv.Key), pfx), "/")
		if ok && target != "" && reporter != "" {
			out[target] = append(out[target], reporter)
		}
	}
	return out, nil
}

// ---- service ----

// SetFlag sets a cluster flag for ttl (capped at maxFlagTTL); ttl <= 0 clears.
func (cs *CoordinatorService) SetFlag(ctx context.Context, name string, ttl time.Duration) error {
	fs, ok := cs.store.(ClusterFlagStore)
	if !ok {
		return errUnsupported
	}
	if ttl <= 0 {
		return fs.ClearClusterFlag(ctx, name)
	}
	if ttl > maxFlagTTL {
		ttl = maxFlagTTL
	}
	cs.logger.Printf("Cluster flag %s set for %v", name, ttl)
	return fs.SetClusterFlag(ctx, name, time.Now().Add(ttl))
}

// Flags returns the active cluster flags, sorted.
func (cs *CoordinatorService) Flags(ctx context.Context) []string {
	fs, ok := cs.store.(ClusterFlagStore)
	if !ok {
		return nil
	}
	m, err := fs.ClusterFlags(ctx)
	if err != nil {
		return nil
	}
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// ReportFailure records that reporter failed to reach target. Self-reports
// and reports about unknown peers are ignored.
func (cs *CoordinatorService) ReportFailure(ctx context.Context, target, reporter string) error {
	if target == "" || reporter == "" || target == reporter {
		return nil
	}
	rs, ok := cs.store.(FailureReportStore)
	if !ok {
		return errUnsupported
	}
	if p, err := cs.store.GetPeer(ctx, target); err != nil || p == nil {
		return nil
	}
	return rs.ReportPeerFailure(ctx, target, reporter, FailureReportTTL)
}

// suspectPeers returns targets with at least minReporters distinct reporters.
func (cs *CoordinatorService) suspectPeers(ctx context.Context) map[string]int {
	rs, ok := cs.store.(FailureReportStore)
	if !ok {
		return nil
	}
	reports, err := rs.FailureReporters(ctx)
	if err != nil {
		return nil
	}
	min := cs.minFailureReporters
	if min <= 0 {
		min = DefaultMinFailureReporters
	}
	out := map[string]int{}
	for target, rep := range reports {
		if len(rep) >= min {
			out[target] = len(rep)
		}
	}
	return out
}

// SetMinFailureReporters sets how many distinct reporters mark a peer suspect.
func (cs *CoordinatorService) SetMinFailureReporters(n int) { cs.minFailureReporters = n }

// PeerHealthReport is GET /api/peers/health.
type PeerHealthReport struct {
	Flags        []string            `json:"flags"`
	Suspect      map[string]int      `json:"suspect"`
	Reporters    map[string][]string `json:"reporters"`
	MinReporters int                 `json:"min_reporters"`
}

// PeerHealth reports flags, suspects and raw reporters.
func (cs *CoordinatorService) PeerHealth(ctx context.Context) PeerHealthReport {
	rep := PeerHealthReport{Flags: cs.Flags(ctx), Suspect: cs.suspectPeers(ctx), MinReporters: cs.minFailureReporters}
	if rep.MinReporters <= 0 {
		rep.MinReporters = DefaultMinFailureReporters
	}
	if rs, ok := cs.store.(FailureReportStore); ok {
		rep.Reporters, _ = rs.FailureReporters(ctx)
	}
	return rep
}

// errUnsupported: the store does not keep this state.
var errUnsupported = errors.New("not supported by this coordinator store")

// FailureReporter is implemented by coordinator clients that can report a
// peer this node failed to read from (POST /api/peers/report-failure). Only
// use it once the coordinator advertises the "failure-report" capability.
type FailureReporter interface {
	ReportPeerFailure(ctx context.Context, target, reporter string) error
}

var (
	_ FailureReporter = (*CoordinatorClient)(nil)
	_ FailureReporter = (*GRPCCoordinatorClient)(nil)
)

func reportFailureHTTP(ctx context.Context, client *http.Client, addr, target, reporter string) error {
	body, _ := json.Marshal(map[string]string{"target": target, "reporter": reporter})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://"+addr+"/api/peers/report-failure", bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		return fmt.Errorf("report failure: http %d", resp.StatusCode)
	}
	return nil
}

func (cc *CoordinatorClient) ReportPeerFailure(ctx context.Context, target, reporter string) error {
	return reportFailureHTTP(ctx, cc.client, cc.addr, target, reporter)
}

func (c *GRPCCoordinatorClient) ReportPeerFailure(ctx context.Context, target, reporter string) error {
	return reportFailureHTTP(ctx, c.httpClient, c.httpAddr, target, reporter)
}

package coordinator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

const (
	defaultEtcdPrefix         = "/fuse"
	defaultPeerLeaseTTLSec    = 30
	defaultEtcdRequestTimeout = 5 * time.Second
	maxFileLocationCASRetries = 8
)

// Compile-time check that EtcdStore satisfies Store.
var _ Store = (*EtcdStore)(nil)

// EtcdStore is a distributed Store backed by an etcd v3 cluster. Peer records
// are bound to a TTL lease that is refreshed on every PutPeer call; file
// locations are stored as a single JSON array per path and updated via
// compare-and-swap on ModRevision so concurrent coordinator replicas don't
// clobber each other.
type EtcdStore struct {
	client       *clientv3.Client
	prefix       string
	peerLeaseTTL int64
	logger       *log.Logger
}

// EtcdStoreConfig captures EtcdStore tunables.
type EtcdStoreConfig struct {
	Prefix          string
	PeerLeaseTTLSec int64
}

// NewEtcdStore wraps an existing etcd client.
func NewEtcdStore(client *clientv3.Client, cfg EtcdStoreConfig) *EtcdStore {
	prefix := strings.TrimRight(cfg.Prefix, "/")
	if prefix == "" {
		prefix = defaultEtcdPrefix
	}
	ttl := cfg.PeerLeaseTTLSec
	if ttl <= 0 {
		ttl = defaultPeerLeaseTTLSec
	}
	return &EtcdStore{
		client:       client,
		prefix:       prefix,
		peerLeaseTTL: ttl,
		logger:       log.New(log.Writer(), "[ETCD-STORE] ", log.LstdFlags),
	}
}

func (s *EtcdStore) peerKey(id string) string  { return s.prefix + "/peers/" + id }
func (s *EtcdStore) peersPrefix() string       { return s.prefix + "/peers/" }
func (s *EtcdStore) fileKey(path string) string { return s.prefix + "/files/" + path }
func (s *EtcdStore) filesPrefix() string       { return s.prefix + "/files/" }

func (s *EtcdStore) GetPeer(ctx context.Context, peerID string) (*PeerInfo, error) {
	resp, err := s.client.Get(ctx, s.peerKey(peerID))
	if err != nil {
		return nil, err
	}
	if len(resp.Kvs) == 0 {
		return nil, nil
	}
	var p PeerInfo
	if err := json.Unmarshal(resp.Kvs[0].Value, &p); err != nil {
		return nil, fmt.Errorf("decode peer %s: %w", peerID, err)
	}
	return &p, nil
}

func (s *EtcdStore) ListPeers(ctx context.Context) ([]*PeerInfo, error) {
	resp, err := s.client.Get(ctx, s.peersPrefix(), clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}
	out := make([]*PeerInfo, 0, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		var p PeerInfo
		if err := json.Unmarshal(kv.Value, &p); err != nil {
			s.logger.Printf("decode peer %s: %v", string(kv.Key), err)
			continue
		}
		out = append(out, &p)
	}
	return out, nil
}

// PutPeer grants a fresh lease (TTL = peerLeaseTTL) and writes the peer record
// with that lease. The previous lease for this peer (if any) is revoked best
// effort; even if revocation fails, the orphaned lease expires within TTL.
func (s *EtcdStore) PutPeer(ctx context.Context, peer *PeerInfo) error {
	data, err := json.Marshal(peer)
	if err != nil {
		return err
	}

	var prevLease clientv3.LeaseID
	if existing, err := s.client.Get(ctx, s.peerKey(peer.ID)); err == nil && len(existing.Kvs) > 0 {
		prevLease = clientv3.LeaseID(existing.Kvs[0].Lease)
	}

	grant, err := s.client.Grant(ctx, s.peerLeaseTTL)
	if err != nil {
		return fmt.Errorf("grant lease: %w", err)
	}

	if _, err := s.client.Put(ctx, s.peerKey(peer.ID), string(data), clientv3.WithLease(grant.ID)); err != nil {
		return fmt.Errorf("put peer: %w", err)
	}

	if prevLease != 0 && prevLease != grant.ID {
		revokeCtx, cancel := context.WithTimeout(context.Background(), defaultEtcdRequestTimeout)
		defer cancel()
		if _, err := s.client.Revoke(revokeCtx, prevLease); err != nil {
			s.logger.Printf("revoke previous lease %x for peer %s: %v", prevLease, peer.ID, err)
		}
	}
	return nil
}

func (s *EtcdStore) DeletePeer(ctx context.Context, peerID string) error {
	resp, err := s.client.Get(ctx, s.peerKey(peerID))
	if err != nil {
		return err
	}
	if _, err := s.client.Delete(ctx, s.peerKey(peerID)); err != nil {
		return err
	}
	if len(resp.Kvs) > 0 {
		if leaseID := clientv3.LeaseID(resp.Kvs[0].Lease); leaseID != 0 {
			revokeCtx, cancel := context.WithTimeout(context.Background(), defaultEtcdRequestTimeout)
			defer cancel()
			_, _ = s.client.Revoke(revokeCtx, leaseID)
		}
	}
	return nil
}

// MarkInactivePeersBefore is a no-op for etcd-backed stores: lease expiry
// removes stale peer records automatically.
func (s *EtcdStore) MarkInactivePeersBefore(_ context.Context, _ time.Time) error {
	return nil
}

func (s *EtcdStore) GetFileLocations(ctx context.Context, filePath string) ([]*FileLocation, error) {
	resp, err := s.client.Get(ctx, s.fileKey(filePath))
	if err != nil {
		return nil, err
	}
	if len(resp.Kvs) == 0 {
		return []*FileLocation{}, nil
	}
	var locs []*FileLocation
	if err := json.Unmarshal(resp.Kvs[0].Value, &locs); err != nil {
		return nil, fmt.Errorf("decode file locations for %s: %w", filePath, err)
	}
	return locs, nil
}

func (s *EtcdStore) ListFileLocations(ctx context.Context, prefix string) ([]*FileLocation, error) {
	fullPrefix := s.filesPrefix() + prefix
	resp, err := s.client.Get(ctx, fullPrefix, clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}
	out := make([]*FileLocation, 0, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		var locs []*FileLocation
		if err := json.Unmarshal(kv.Value, &locs); err != nil {
			s.logger.Printf("decode file locations %s: %v", string(kv.Key), err)
			continue
		}
		if len(locs) == 0 {
			continue
		}
		lc := *locs[0]
		out = append(out, &lc)
	}
	return out, nil
}

func (s *EtcdStore) RangeFileLocations(ctx context.Context, prefix string) (map[string][]*FileLocation, error) {
	fullPrefix := s.filesPrefix() + prefix
	resp, err := s.client.Get(ctx, fullPrefix, clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}
	out := make(map[string][]*FileLocation, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		path := strings.TrimPrefix(string(kv.Key), s.filesPrefix())
		var locs []*FileLocation
		if err := json.Unmarshal(kv.Value, &locs); err != nil {
			s.logger.Printf("decode file locations %s: %v", string(kv.Key), err)
			continue
		}
		out[path] = locs
	}
	return out, nil
}

// PutFileLocation merges loc into the existing []FileLocation for the path
// (replace-by-(peerID, tier) or append) via CAS on ModRevision so concurrent
// coordinator replicas don't clobber each other.
func (s *EtcdStore) PutFileLocation(ctx context.Context, loc *FileLocation) error {
	key := s.fileKey(loc.FilePath)
	for attempt := 0; attempt < maxFileLocationCASRetries; attempt++ {
		resp, err := s.client.Get(ctx, key)
		if err != nil {
			return err
		}

		var locs []*FileLocation
		var modRev int64
		if len(resp.Kvs) > 0 {
			if err := json.Unmarshal(resp.Kvs[0].Value, &locs); err != nil {
				return fmt.Errorf("decode file locations: %w", err)
			}
			modRev = resp.Kvs[0].ModRevision
		}

		replaced := false
		for i, existing := range locs {
			if existing.PeerID == loc.PeerID && existing.StorageTier == loc.StorageTier {
				lc := *loc
				locs[i] = &lc
				replaced = true
				break
			}
		}
		if !replaced {
			lc := *loc
			locs = append(locs, &lc)
		}

		data, err := json.Marshal(locs)
		if err != nil {
			return err
		}

		var cmp clientv3.Cmp
		if modRev == 0 {
			cmp = clientv3.Compare(clientv3.CreateRevision(key), "=", 0)
		} else {
			cmp = clientv3.Compare(clientv3.ModRevision(key), "=", modRev)
		}

		txnResp, err := s.client.Txn(ctx).
			If(cmp).
			Then(clientv3.OpPut(key, string(data))).
			Commit()
		if err != nil {
			return err
		}
		if txnResp.Succeeded {
			return nil
		}
	}
	return errors.New("PutFileLocation: too many CAS retries")
}

func (s *EtcdStore) Snapshot(ctx context.Context) (State, error) {
	state := State{
		Peers:         make(map[string]*PeerInfo),
		FileLocations: make(map[string][]*FileLocation),
	}
	peers, err := s.ListPeers(ctx)
	if err != nil {
		return state, err
	}
	for _, p := range peers {
		state.Peers[p.ID] = p
	}
	files, err := s.RangeFileLocations(ctx, "")
	if err != nil {
		return state, err
	}
	state.FileLocations = files
	return state, nil
}

func (s *EtcdStore) Restore(ctx context.Context, state State) error {
	for _, p := range state.Peers {
		if err := s.PutPeer(ctx, p); err != nil {
			return fmt.Errorf("restore peer %s: %w", p.ID, err)
		}
	}
	for path, locs := range state.FileLocations {
		for _, loc := range locs {
			if loc.FilePath == "" {
				loc.FilePath = path
			}
			if err := s.PutFileLocation(ctx, loc); err != nil {
				return fmt.Errorf("restore file location %s: %w", path, err)
			}
		}
	}
	return nil
}

func (s *EtcdStore) Close() error {
	if s.client == nil {
		return nil
	}
	return s.client.Close()
}

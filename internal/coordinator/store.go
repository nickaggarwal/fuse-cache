package coordinator

import (
	"context"
	"strings"
	"sync"
	"time"
)

// Store is the persistence layer for coordinator state. Implementations may be
// process-local (InMemoryStore) or distributed (EtcdStore). All methods take a
// context so remote-backed implementations can honor cancellation and timeouts.
//
// Liveness: peer entries in distributed implementations are bound to a TTL
// lease. Each PutPeer refreshes the lease; if no refresh arrives within the TTL
// window, the entry is removed automatically. Process-local implementations
// emulate this via MarkInactivePeersBefore.
type Store interface {
	GetPeer(ctx context.Context, peerID string) (*PeerInfo, error)
	ListPeers(ctx context.Context) ([]*PeerInfo, error)
	PutPeer(ctx context.Context, peer *PeerInfo) error
	DeletePeer(ctx context.Context, peerID string) error
	MarkInactivePeersBefore(ctx context.Context, cutoff time.Time) error

	GetFileLocations(ctx context.Context, filePath string) ([]*FileLocation, error)
	ListFileLocations(ctx context.Context, prefix string) ([]*FileLocation, error)
	// RangeFileLocations returns every replica grouped by path, optionally
	// filtered by prefix. Used for world-view aggregation; not a hot path.
	RangeFileLocations(ctx context.Context, prefix string) (map[string][]*FileLocation, error)
	PutFileLocation(ctx context.Context, loc *FileLocation) error

	Snapshot(ctx context.Context) (State, error)
	Restore(ctx context.Context, state State) error

	Close() error
}

// Compile-time check that InMemoryStore satisfies Store.
var _ Store = (*InMemoryStore)(nil)

// InMemoryStore is the legacy single-process store. State lives in two maps
// protected by a single RWMutex.
type InMemoryStore struct {
	mu            sync.RWMutex
	peers         map[string]*PeerInfo
	fileLocations map[string][]*FileLocation
}

func NewInMemoryStore() *InMemoryStore {
	return &InMemoryStore{
		peers:         make(map[string]*PeerInfo),
		fileLocations: make(map[string][]*FileLocation),
	}
}

func (s *InMemoryStore) GetPeer(_ context.Context, peerID string) (*PeerInfo, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	p, ok := s.peers[peerID]
	if !ok {
		return nil, nil
	}
	pCopy := *p
	return &pCopy, nil
}

func (s *InMemoryStore) ListPeers(_ context.Context) ([]*PeerInfo, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]*PeerInfo, 0, len(s.peers))
	for _, p := range s.peers {
		pCopy := *p
		out = append(out, &pCopy)
	}
	return out, nil
}

func (s *InMemoryStore) PutPeer(_ context.Context, peer *PeerInfo) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	pCopy := *peer
	s.peers[peer.ID] = &pCopy
	return nil
}

func (s *InMemoryStore) DeletePeer(_ context.Context, peerID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.peers, peerID)
	return nil
}

func (s *InMemoryStore) MarkInactivePeersBefore(_ context.Context, cutoff time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, p := range s.peers {
		if p.LastHeartbeat.Before(cutoff) && p.Status != "inactive" {
			p.Status = "inactive"
		}
	}
	return nil
}

func (s *InMemoryStore) GetFileLocations(_ context.Context, filePath string) ([]*FileLocation, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	locs := s.fileLocations[filePath]
	out := make([]*FileLocation, len(locs))
	for i, l := range locs {
		lc := *l
		out[i] = &lc
	}
	return out, nil
}

func (s *InMemoryStore) ListFileLocations(_ context.Context, prefix string) ([]*FileLocation, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]*FileLocation, 0, len(s.fileLocations))
	for path, locs := range s.fileLocations {
		if prefix != "" && !strings.HasPrefix(path, prefix) {
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

func (s *InMemoryStore) RangeFileLocations(_ context.Context, prefix string) (map[string][]*FileLocation, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make(map[string][]*FileLocation, len(s.fileLocations))
	for path, locs := range s.fileLocations {
		if prefix != "" && !strings.HasPrefix(path, prefix) {
			continue
		}
		copied := make([]*FileLocation, len(locs))
		for i, l := range locs {
			lc := *l
			copied[i] = &lc
		}
		out[path] = copied
	}
	return out, nil
}

func (s *InMemoryStore) PutFileLocation(_ context.Context, loc *FileLocation) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	locs := s.fileLocations[loc.FilePath]
	for i, existing := range locs {
		if existing.PeerID == loc.PeerID && existing.StorageTier == loc.StorageTier {
			lc := *loc
			locs[i] = &lc
			s.fileLocations[loc.FilePath] = locs
			return nil
		}
	}
	lc := *loc
	s.fileLocations[loc.FilePath] = append(locs, &lc)
	return nil
}

func (s *InMemoryStore) Snapshot(_ context.Context) (State, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	state := State{
		Peers:         make(map[string]*PeerInfo, len(s.peers)),
		FileLocations: make(map[string][]*FileLocation, len(s.fileLocations)),
	}
	for k, v := range s.peers {
		pCopy := *v
		state.Peers[k] = &pCopy
	}
	for k, v := range s.fileLocations {
		locsCopy := make([]*FileLocation, len(v))
		for i, l := range v {
			lc := *l
			locsCopy[i] = &lc
		}
		state.FileLocations[k] = locsCopy
	}
	return state, nil
}

func (s *InMemoryStore) Restore(_ context.Context, state State) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if state.Peers == nil {
		state.Peers = make(map[string]*PeerInfo)
	}
	if state.FileLocations == nil {
		state.FileLocations = make(map[string][]*FileLocation)
	}
	s.peers = state.Peers
	s.fileLocations = state.FileLocations
	return nil
}

func (s *InMemoryStore) Close() error { return nil }

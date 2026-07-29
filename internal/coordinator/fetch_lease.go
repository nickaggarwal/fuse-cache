package coordinator

import (
	"context"
	"time"
)

// Fetch leases are the cross-node single-flight primitive from
// docs/peer-coordination-thundering-herd.md Phase 2: before pulling an
// object/chunk from origin (cloud), a node takes a short-TTL lease on the key.
// Concurrent requesters that lose the race are told who holds the lease and
// back off to the peer tier (the holder is about to have the data), collapsing
// a herd of origin pulls into ~one.
//
// Leases are advisory: expiry or coordinator unavailability never blocks a
// fetch — requesters proceed to origin when in doubt. Correctness never
// depends on a lease.

const (
	// DefaultFetchLeaseTTL bounds how long a lease suppresses other origin
	// pulls. Kept short so a crashed holder only delays the herd briefly.
	DefaultFetchLeaseTTL = 10 * time.Second
	// maxFetchLeaseTTL caps client-requested TTLs.
	maxFetchLeaseTTL = 60 * time.Second
)

// FetchLeaser is the optional coordinator capability for in-flight fetch
// leases (mirrors the NetworkStatusUpdater optional-interface pattern).
type FetchLeaser interface {
	// AcquireFetchLease attempts to take the lease for key on behalf of
	// peerID. It returns the current holder and whether the caller now holds
	// the lease (re-acquiring one's own lease renews it).
	AcquireFetchLease(ctx context.Context, key, peerID string, ttl time.Duration) (holder string, granted bool, err error)
	// ReleaseFetchLease releases the lease if peerID still holds it.
	ReleaseFetchLease(ctx context.Context, key, peerID string) error
}

// FetchLeaseStore is the persistence capability for fetch leases. Stores that
// implement it (InMemoryStore, EtcdStore) enable lease support on the
// coordinator service.
type FetchLeaseStore interface {
	AcquireFetchLease(ctx context.Context, key, peerID string, ttl time.Duration) (holder string, granted bool, err error)
	ReleaseFetchLease(ctx context.Context, key, peerID string) error
}

// ClampFetchLeaseTTL normalizes a client-requested TTL.
func ClampFetchLeaseTTL(ttl time.Duration) time.Duration {
	if ttl <= 0 {
		return DefaultFetchLeaseTTL
	}
	if ttl > maxFetchLeaseTTL {
		return maxFetchLeaseTTL
	}
	return ttl
}

// AcquireFetchLease implements FetchLeaser on the coordinator service when the
// underlying store supports leases.
func (cs *CoordinatorService) AcquireFetchLease(ctx context.Context, key, peerID string, ttl time.Duration) (string, bool, error) {
	ls, ok := cs.store.(FetchLeaseStore)
	if !ok {
		// No lease support: report the caller as holder so it proceeds to
		// origin. Advisory semantics — never block the fetch.
		return peerID, true, nil
	}
	return ls.AcquireFetchLease(ctx, key, peerID, ClampFetchLeaseTTL(ttl))
}

// ReleaseFetchLease implements FetchLeaser on the coordinator service.
func (cs *CoordinatorService) ReleaseFetchLease(ctx context.Context, key, peerID string) error {
	ls, ok := cs.store.(FetchLeaseStore)
	if !ok {
		return nil
	}
	return ls.ReleaseFetchLease(ctx, key, peerID)
}

// --- InMemoryStore implementation ---

type memFetchLease struct {
	holder    string
	expiresAt time.Time
}

// fetchLeaseMu / fetchLeases live on InMemoryStore (see store.go). Lease churn
// is hot and short-TTL, so it uses its own mutex rather than contending with
// peer/file metadata under the store's RWMutex.

func (s *InMemoryStore) AcquireFetchLease(_ context.Context, key, peerID string, ttl time.Duration) (string, bool, error) {
	now := time.Now()
	s.fetchLeaseMu.Lock()
	defer s.fetchLeaseMu.Unlock()
	if s.fetchLeases == nil {
		s.fetchLeases = make(map[string]memFetchLease)
	}

	// Opportunistically drop expired entries to bound the map.
	for k, l := range s.fetchLeases {
		if now.After(l.expiresAt) {
			delete(s.fetchLeases, k)
		}
	}

	if existing, ok := s.fetchLeases[key]; ok && now.Before(existing.expiresAt) && existing.holder != peerID {
		return existing.holder, false, nil
	}
	s.fetchLeases[key] = memFetchLease{holder: peerID, expiresAt: now.Add(ttl)}
	return peerID, true, nil
}

func (s *InMemoryStore) ReleaseFetchLease(_ context.Context, key, peerID string) error {
	s.fetchLeaseMu.Lock()
	defer s.fetchLeaseMu.Unlock()
	if existing, ok := s.fetchLeases[key]; ok && existing.holder == peerID {
		delete(s.fetchLeases, key)
	}
	return nil
}

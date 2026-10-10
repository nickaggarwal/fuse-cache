package globalcp

import (
	"context"
	"sort"
	"strings"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"

	"fuse-client/internal/pb"
)

// Cluster is a registered cluster's record.
type Cluster struct {
	ID                string
	Object            *pb.ObjectRef
	PublishPrefixes   []string
	SubscribePrefixes []string
	RegisteredAt      time.Time
	LastSeen          time.Time
}

// clone deep-copies a record. A shallow copy would share the ObjectRef and
// both prefix slices with the stored row, so a caller appending to the
// allowlist it was handed would silently widen what the cluster may publish.
func (c *Cluster) clone() *Cluster {
	if c == nil {
		return nil
	}
	cp := *c
	if c.Object != nil {
		cp.Object = proto.Clone(c.Object).(*pb.ObjectRef)
	}
	cp.PublishPrefixes = append([]string(nil), c.PublishPrefixes...)
	cp.SubscribePrefixes = append([]string(nil), c.SubscribePrefixes...)
	return &cp
}

// Store is the global control plane's persistence layer.
//
// It deliberately mirrors the shape of internal/coordinator.Store -- same
// context-on-every-method rule, same in-memory default with a distributed
// implementation behind the same interface -- rather than introducing a
// second persistence idiom to the repo.
//
// Rows are keyed by (cluster_id, file_path): several clusters may publish
// the same path, and collapsing them would silently discard a copy a reader
// might prefer.
type Store interface {
	PutCluster(ctx context.Context, c *Cluster) error
	GetCluster(ctx context.Context, clusterID string) (*Cluster, error)
	ListClusters(ctx context.Context) ([]*Cluster, error)
	TouchCluster(ctx context.Context, clusterID string, at time.Time) error

	// PutLocations upserts rows. Every row must already carry the publishing
	// cluster's ID; the service sets it from the authenticated identity
	// before calling, never from the request body.
	PutLocations(ctx context.Context, locs []*pb.GlobalLocation) error
	// DeleteLocations retracts clusterID's rows for the given paths. A path
	// with no row is not an error -- withdraw must be idempotent, because a
	// reconnecting agent cannot know what the server still holds.
	DeleteLocations(ctx context.Context, clusterID string, paths []string) error
	// DeleteCluster drops a cluster's registration and all of its rows.
	DeleteCluster(ctx context.Context, clusterID string) error

	// GetLocations returns every cluster's rows for one exact path.
	GetLocations(ctx context.Context, filePath string) ([]*pb.GlobalLocation, error)
	// ListLocations returns rows under any of the given prefixes. An empty
	// prefix list returns nothing, matching the allowlist rule: a subscriber
	// that asked for nothing gets nothing, never everything.
	ListLocations(ctx context.Context, prefixes []string) ([]*pb.GlobalLocation, error)

	Close() error
}

var _ Store = (*MemStore)(nil)

// MemStore is the single-process store, and the default. It is the right
// choice for the embedded dev mode and for a single-replica deployment; a
// multi-replica global CP needs the etcd-backed one, because the subscriber
// fan-out is per-process.
type MemStore struct {
	mu       sync.RWMutex
	clusters map[string]*Cluster
	// locations[filePath][clusterID] -- keyed path-first because the hot
	// read is a point lookup by path, and withdraw-by-cluster is rare.
	locations map[string]map[string]*pb.GlobalLocation
}

func NewMemStore() *MemStore {
	return &MemStore{
		clusters:  make(map[string]*Cluster),
		locations: make(map[string]map[string]*pb.GlobalLocation),
	}
}

func (s *MemStore) PutCluster(_ context.Context, c *Cluster) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.clusters[c.ID] = c.clone()
	return nil
}

func (s *MemStore) GetCluster(_ context.Context, clusterID string) (*Cluster, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	c, ok := s.clusters[clusterID]
	if !ok {
		return nil, nil
	}
	return c.clone(), nil
}

func (s *MemStore) ListClusters(_ context.Context) ([]*Cluster, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]*Cluster, 0, len(s.clusters))
	for _, c := range s.clusters {
		out = append(out, c.clone())
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, nil
}

func (s *MemStore) TouchCluster(_ context.Context, clusterID string, at time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if c, ok := s.clusters[clusterID]; ok {
		c.LastSeen = at
	}
	return nil
}

func (s *MemStore) PutLocations(_ context.Context, locs []*pb.GlobalLocation) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, l := range locs {
		if l == nil || l.FilePath == "" || l.ClusterId == "" {
			continue
		}
		byCluster, ok := s.locations[l.FilePath]
		if !ok {
			byCluster = make(map[string]*pb.GlobalLocation)
			s.locations[l.FilePath] = byCluster
		}
		byCluster[l.ClusterId] = cloneLocation(l)
	}
	return nil
}

func (s *MemStore) DeleteLocations(_ context.Context, clusterID string, paths []string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, p := range paths {
		byCluster, ok := s.locations[p]
		if !ok {
			continue
		}
		delete(byCluster, clusterID)
		if len(byCluster) == 0 {
			delete(s.locations, p)
		}
	}
	return nil
}

func (s *MemStore) DeleteCluster(_ context.Context, clusterID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.clusters, clusterID)
	for path, byCluster := range s.locations {
		delete(byCluster, clusterID)
		if len(byCluster) == 0 {
			delete(s.locations, path)
		}
	}
	return nil
}

func (s *MemStore) GetLocations(_ context.Context, filePath string) ([]*pb.GlobalLocation, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	byCluster, ok := s.locations[filePath]
	if !ok {
		return nil, nil
	}
	out := make([]*pb.GlobalLocation, 0, len(byCluster))
	for _, l := range byCluster {
		out = append(out, cloneLocation(l))
	}
	sortLocations(out)
	return out, nil
}

func (s *MemStore) ListLocations(_ context.Context, prefixes []string) ([]*pb.GlobalLocation, error) {
	if len(prefixes) == 0 {
		return nil, nil
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []*pb.GlobalLocation
	for path, byCluster := range s.locations {
		if !MatchesAny(path, prefixes) {
			continue
		}
		for _, l := range byCluster {
			out = append(out, cloneLocation(l))
		}
	}
	sortLocations(out)
	return out, nil
}

func (s *MemStore) Close() error { return nil }

// cloneLocation copies a row on the way in and on the way out. A stored row
// handed to a caller by pointer would be mutated by a later publish from
// under a mirror that is still reading it.
//
// proto.Clone, not a struct copy: a generated message embeds a MessageState
// containing a mutex, so copying the struct copies a lock (go vet says so)
// and corrupts the protobuf internals.
func cloneLocation(l *pb.GlobalLocation) *pb.GlobalLocation {
	if l == nil {
		return nil
	}
	return proto.Clone(l).(*pb.GlobalLocation)
}

// sortLocations gives every listing a stable order, so a snapshot is
// reproducible and a test can assert on it without sorting first.
func sortLocations(ls []*pb.GlobalLocation) {
	sort.Slice(ls, func(i, j int) bool {
		if c := strings.Compare(ls[i].FilePath, ls[j].FilePath); c != 0 {
			return c < 0
		}
		return ls[i].ClusterId < ls[j].ClusterId
	})
}

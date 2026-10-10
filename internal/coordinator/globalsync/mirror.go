// Package globalsync is the coordinator's half of the global control plane:
// an agent that dials out and holds a stream, a read-only mirror of the
// global index, and a coalescing publish queue.
//
// The design property that matters most here is local autonomy. When the
// global CP is unreachable, every local operation -- read, write, peer
// fetch, cloud fetch -- must behave exactly as in a single-cluster
// deployment, and the only thing lost is discovery of NEW cross-cluster
// rows. The mirror is what makes that fall out for free rather than
// needing a special case: a lookup is a local map read that never touches
// the network, so an unreachable CP simply stops updating it.
package globalsync

import (
	"sort"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"

	"fuse-client/internal/coordinator"
	"fuse-client/internal/pb"
)

// Mirror is a read-only local copy of the global index for the prefixes
// this cluster subscribes to.
//
// Rows are keyed (path, cluster): several clusters may publish the same
// path, and collapsing them would discard a copy a reader might prefer.
type Mirror struct {
	mu sync.RWMutex
	// byPath[filePath][clusterID]
	byPath map[string]map[string]*pb.GlobalLocation

	// ready reports whether a full snapshot has ever been applied. A mirror
	// that has never synced must not be confused with one that synced and
	// found nothing -- the first means "I don't know", the second "there is
	// no remote copy", and only the second is a safe answer to act on.
	ready bool
	// lastSyncAt is when the mirror last heard anything from the CP. An
	// operator reading a stale mirror needs to see HOW stale; a mirror that
	// reports its contents without its age is the "last known good"
	// failure mode that looks like fresh truth.
	lastSyncAt time.Time
}

func NewMirror() *Mirror {
	return &Mirror{byPath: make(map[string]map[string]*pb.GlobalLocation)}
}

// Lookup returns the known remote copies of filePath, best-first.
//
// This is the only method on the data path, and it is a local map read: no
// network, no lock held across I/O, no timeout to tune. A caller gets an
// answer or an empty slice, never an error and never a stall.
func (m *Mirror) Lookup(filePath string) []*pb.GlobalLocation {
	m.mu.RLock()
	defer m.mu.RUnlock()
	byCluster, ok := m.byPath[filePath]
	if !ok {
		return nil
	}
	out := make([]*pb.GlobalLocation, 0, len(byCluster))
	for _, l := range byCluster {
		out = append(out, proto.Clone(l).(*pb.GlobalLocation))
	}
	// Newest publication first: when two clusters hold the same path, the
	// more recently published copy is likelier to be the current one. Ties
	// break on cluster ID so the order is stable rather than map-random.
	sort.Slice(out, func(i, j int) bool {
		if out[i].GetPublishedUnix() != out[j].GetPublishedUnix() {
			return out[i].GetPublishedUnix() > out[j].GetPublishedUnix()
		}
		return out[i].GetClusterId() < out[j].GetClusterId()
	})
	return out
}

// ApplySnapshot replaces the mirror's contents wholesale.
//
// Replace, not merge: a snapshot is the CP's complete view of the
// subscribed prefixes, so merging would keep rows another cluster withdrew
// while this agent was disconnected -- exactly the stale entries a
// reconnect is supposed to clear.
func (m *Mirror) ApplySnapshot(locs []*pb.GlobalLocation) {
	next := make(map[string]map[string]*pb.GlobalLocation, len(locs))
	for _, l := range locs {
		if l.GetFilePath() == "" || l.GetClusterId() == "" {
			continue
		}
		byCluster, ok := next[l.GetFilePath()]
		if !ok {
			byCluster = make(map[string]*pb.GlobalLocation)
			next[l.GetFilePath()] = byCluster
		}
		byCluster[l.GetClusterId()] = proto.Clone(l).(*pb.GlobalLocation)
	}
	m.mu.Lock()
	m.byPath = next
	m.ready = true
	m.lastSyncAt = time.Now()
	m.mu.Unlock()
}

// ApplyEvent folds one upsert or withdraw into the mirror.
func (m *Mirror) ApplyEvent(ev *pb.GlobalEvent) {
	if ev == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.lastSyncAt = time.Now()

	switch ev.GetKind() {
	case pb.GlobalEvent_UPSERT:
		l := ev.GetLocation()
		if l.GetFilePath() == "" || l.GetClusterId() == "" {
			return
		}
		byCluster, ok := m.byPath[l.GetFilePath()]
		if !ok {
			byCluster = make(map[string]*pb.GlobalLocation)
			m.byPath[l.GetFilePath()] = byCluster
		}
		byCluster[l.GetClusterId()] = proto.Clone(l).(*pb.GlobalLocation)

	case pb.GlobalEvent_WITHDRAW:
		path, cluster := ev.GetFilePath(), ev.GetClusterId()
		if path == "" || cluster == "" {
			return
		}
		byCluster, ok := m.byPath[path]
		if !ok {
			return
		}
		delete(byCluster, cluster)
		// Drop the empty map, or a long-lived mirror accumulates one entry
		// per path ever published and withdrawn.
		if len(byCluster) == 0 {
			delete(m.byPath, path)
		}
	}
}

// MarkDisconnected records that the stream dropped.
//
// It deliberately does NOT clear the mirror. Serving last-good contents
// through an outage is the autonomy requirement; emptying on disconnect
// would turn a CP restart into a cluster-wide loss of discovery for every
// path, which is precisely the coupling this design exists to avoid.
// Readiness is likewise kept: the mirror still knows what it knew.
func (m *Mirror) MarkDisconnected() {}

// Status reports the mirror's state for observability.
func (m *Mirror) Status() MirrorStatus {
	m.mu.RLock()
	defer m.mu.RUnlock()
	rows := 0
	for _, byCluster := range m.byPath {
		rows += len(byCluster)
	}
	return MirrorStatus{Ready: m.ready, Paths: len(m.byPath), Rows: rows, LastSyncAt: m.lastSyncAt}
}

// MirrorStatus is a point-in-time view of the mirror.
type MirrorStatus struct {
	// Ready is false until a full snapshot has landed. False means "I don't
	// know", which is not the same as "there is no remote copy".
	Ready      bool      `json:"ready"`
	Paths      int       `json:"paths"`
	Rows       int       `json:"rows"`
	LastSyncAt time.Time `json:"last_sync_at"`
}

// Snapshot returns every row, for the observability endpoint.
func (m *Mirror) Snapshot() []*pb.GlobalLocation {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var out []*pb.GlobalLocation
	for _, byCluster := range m.byPath {
		for _, l := range byCluster {
			out = append(out, proto.Clone(l).(*pb.GlobalLocation))
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].GetFilePath() != out[j].GetFilePath() {
			return out[i].GetFilePath() < out[j].GetFilePath()
		}
		return out[i].GetClusterId() < out[j].GetClusterId()
	})
	return out
}

// Resolve implements coordinator.GlobalMirror: the cross-cluster lookup as
// the data path sees it.
//
// This is the whole cost of a global resolve -- one read lock and a map
// hit. No dial, no RPC, no timeout to tune. It is called on a cloud miss,
// which a reader is already waiting on, so anything slower would make the
// miss path worse for every deployment in exchange for a feature most of
// them do not use.
//
// The second return is "have I ever synced", and callers must not collapse
// it into len(locations) == 0. A cluster that restarts while the global
// control plane is down would otherwise learn a permanent negative.
func (m *Mirror) Resolve(filePath string) ([]*coordinator.GlobalLocation, bool) {
	m.mu.RLock()
	ready := m.ready
	m.mu.RUnlock()

	rows := m.Lookup(filePath)
	if len(rows) == 0 {
		return nil, ready
	}
	out := make([]*coordinator.GlobalLocation, 0, len(rows))
	for _, r := range rows {
		obj := r.GetObject()
		if obj == nil || obj.GetBucket() == "" {
			// A row with no object reference names no readable bytes. The
			// global data path is object storage only, so there is nothing
			// a reader could do with it except waste a round trip.
			continue
		}
		out = append(out, &coordinator.GlobalLocation{
			ClusterID:   r.GetClusterId(),
			FilePath:    r.GetFilePath(),
			FileSize:    r.GetFileSize(),
			IsChunked:   r.GetIsChunked(),
			ChunkSize:   r.GetChunkSize(),
			Provider:    obj.GetProvider(),
			Bucket:      obj.GetBucket(),
			KeyPrefix:   obj.GetKeyPrefix(),
			Region:      obj.GetRegion(),
			Endpoint:    obj.GetEndpoint(),
			PublishedAt: r.GetPublishedUnix(),
		})
	}
	return out, ready
}

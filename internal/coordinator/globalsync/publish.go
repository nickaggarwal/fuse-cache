package globalsync

import (
	"sync"
	"time"

	"fuse-client/internal/globalcp"
	"fuse-client/internal/pb"
)

const (
	// publishCoalesceWindow bounds how often one path may be re-published.
	// Same reasoning and same number as chunkAdvertiseCoalesceWindow
	// (internal/cache/chunk_advertise.go:27): a file that is written in
	// chunks produces one cloud-persist callback per chunk, and publishing
	// each one would turn the global CP into the hotspot this design exists
	// to keep out of the data path.
	publishCoalesceWindow = 5 * time.Second

	// publishQueueDepth bounds memory when the stream is down. A full queue
	// drops the OLDEST pending row, not the newest -- see enqueue.
	publishQueueDepth = 4096

	// publishFlushInterval is how often the agent drains the queue onto the
	// stream. Cross-cluster discovery is async by requirement, so batching
	// buys a smaller write rate at a cost the design already accepts.
	publishFlushInterval = 2 * time.Second

	// publishBatchMax caps one PublishBatch message.
	publishBatchMax = 256
)

// publishQueue accumulates publish and withdraw intents between flushes.
//
// It is deliberately a queue and not a direct send: the caller is the
// coordinator's cloud-persist path, which must never block on the global
// CP's availability. Enqueue takes a mutex and returns.
type publishQueue struct {
	mu sync.Mutex
	// pending is keyed by file path so repeated writes to one path collapse
	// into one row. The value carries the intent: a nil location means
	// withdraw.
	pending map[string]*pending
	// order preserves arrival order for fair draining, so one hot path
	// cannot starve the rest when the queue is over depth.
	order []string

	// lastPub is the coalescing clock, keyed by path. Separate from
	// pending: a path can be past its window and still have nothing queued.
	lastPub map[string]time.Time

	dropped uint64
}

type pending struct {
	// loc is the row to upsert; nil means withdraw path.
	loc  *pb.GlobalLocation
	path string
}

func newPublishQueue() *publishQueue {
	return &publishQueue{
		pending: make(map[string]*pending),
		lastPub: make(map[string]time.Time),
	}
}

// Publish queues an upsert, subject to the coalescing window.
//
// Returns false when the window suppressed it. Suppression is safe for an
// upsert because the row's content -- bucket, prefix, size -- does not
// change between two writes of the same path within five seconds in any
// way a remote reader can act on; the next write past the window carries
// the current values anyway.
func (q *publishQueue) Publish(loc *pb.GlobalLocation, now time.Time) bool {
	if loc == nil || loc.GetFilePath() == "" {
		return false
	}
	q.mu.Lock()
	defer q.mu.Unlock()

	path := loc.GetFilePath()
	if last, ok := q.lastPub[path]; ok && now.Sub(last) < publishCoalesceWindow {
		// Already queued but not yet flushed: replace the payload so the
		// flush carries the freshest size rather than the stale one. This
		// is not a second publish, so the window is untouched.
		if p, queued := q.pending[path]; queued && p.loc != nil {
			p.loc = loc
			return true
		}
		return false
	}
	q.lastPub[path] = now
	q.enqueueLocked(&pending{loc: loc, path: path})
	return true
}

// Withdraw queues a retraction.
//
// Unlike Publish this is never coalesced away. A suppressed upsert costs a
// few seconds of staleness; a suppressed withdraw leaves a row advertising
// an object that no longer exists, and every cluster that acts on it takes
// a full cross-cloud round trip to discover the 404.
func (q *publishQueue) Withdraw(path string, now time.Time) {
	if path == "" {
		return
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	// A withdraw invalidates the coalescing clock: the next write of this
	// path must publish immediately rather than be suppressed as a
	// duplicate of the row we are about to delete.
	delete(q.lastPub, path)
	q.enqueueLocked(&pending{loc: nil, path: path})
}

func (q *publishQueue) enqueueLocked(p *pending) {
	if _, exists := q.pending[p.path]; !exists {
		q.order = append(q.order, p.path)
	}
	q.pending[p.path] = p

	// Over depth: drop the OLDEST entry. The newest intent is the one that
	// reflects current reality, and the global index converges on the last
	// write; dropping the newest would leave the index permanently
	// disagreeing with the cluster until that path is written again.
	for len(q.order) > publishQueueDepth {
		oldest := q.order[0]
		q.order = q.order[1:]
		delete(q.pending, oldest)
		q.dropped++
	}
}

// Drain removes up to publishBatchMax intents and returns them split into
// upserts and withdrawals.
func (q *publishQueue) Drain() (upserts []*pb.GlobalLocation, withdrawals []string) {
	q.mu.Lock()
	defer q.mu.Unlock()

	n := 0
	for len(q.order) > 0 && n < publishBatchMax {
		path := q.order[0]
		q.order = q.order[1:]
		p, ok := q.pending[path]
		if !ok {
			continue // superseded by a later intent that re-queued the path
		}
		delete(q.pending, path)
		if p.loc != nil {
			upserts = append(upserts, p.loc)
		} else {
			withdrawals = append(withdrawals, p.path)
		}
		n++
	}

	// GC the coalescing clock. Unbounded, it grows one entry per path ever
	// published for the life of the process.
	if len(q.lastPub) > 2*publishQueueDepth {
		cutoff := time.Now().Add(-10 * publishCoalesceWindow)
		for k, t := range q.lastPub {
			if t.Before(cutoff) {
				delete(q.lastPub, k)
			}
		}
	}
	return upserts, withdrawals
}

// Requeue puts a failed batch back at the FRONT of the queue.
//
// Front, not back: these intents are older than anything queued while the
// send was in flight, and appending them would let a busy path keep
// overtaking a retried one indefinitely. An intent already superseded by a
// newer one for the same path is dropped rather than resurrected.
func (q *publishQueue) Requeue(upserts []*pb.GlobalLocation, withdrawals []string) {
	q.mu.Lock()
	defer q.mu.Unlock()

	var front []string
	add := func(p *pending) {
		if _, superseded := q.pending[p.path]; superseded {
			return
		}
		q.pending[p.path] = p
		front = append(front, p.path)
	}
	for _, l := range upserts {
		add(&pending{loc: l, path: l.GetFilePath()})
	}
	for _, p := range withdrawals {
		add(&pending{loc: nil, path: p})
	}
	q.order = append(front, q.order...)
	for len(q.order) > publishQueueDepth {
		last := q.order[len(q.order)-1]
		q.order = q.order[:len(q.order)-1]
		delete(q.pending, last)
		q.dropped++
	}
}

// Len reports the queue depth, for observability.
func (q *publishQueue) Len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.order)
}

// Dropped reports how many intents were discarded for queue depth. A
// non-zero and climbing value means the stream has been down long enough
// that this cluster's slice of the global index is now incomplete, which
// an operator can only see if it is counted.
func (q *publishQueue) Dropped() uint64 {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.dropped
}

// publishable reports whether this path may cross the cluster boundary.
//
// Two conditions, both required, and the second is the one that is easy to
// forget: the bytes must actually be in object storage. The global data
// path is object storage only, so advertising a path whose only copy is on
// a local NVMe tells another cluster to fetch something it structurally
// cannot reach.
func publishable(filePath, storageTier string, allowlist []string) bool {
	if storageTier != tierCloud {
		return false
	}
	return globalcp.MatchesAny(filePath, allowlist)
}

// tierCloud is the StorageTier string the cache uses for object storage.
// Duplicated as a constant rather than imported: internal/cache imports the
// coordinator, so importing it back here would be a cycle.
const tierCloud = "cloud"

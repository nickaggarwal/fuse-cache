package globalsync

import (
	"strconv"
	"strings"
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

	// touched records when a LIVE intent (publish or withdraw) was last
	// queued for a path. Republish consults it so a row read from the
	// location table before a delete cannot be re-sent after the withdraw
	// already went out -- see Republish.
	touched map[string]time.Time
	// holdSince, when non-zero, is the table-read time of a running
	// republish pass; touched entries at or after it are not GC'd.
	holdSince time.Time

	// repPending counts republish-sourced intents still queued; repDropped
	// counts those discarded for depth. A pass is complete only when its
	// rows have left the queue by being sent.
	repPending int
	repDropped uint64
	// repInflight counts republish rows drained but not yet acknowledged as
	// sent (AckSent) or returned (Requeue). sendFailures counts Requeues of
	// upserts, so a pass can tell "left the queue by being sent" from "left
	// the queue and bounced".
	repInflight  int
	sendFailures uint64

	dropped uint64
	// droppedWithdrawals is the subset of dropped that were withdrawals.
	// Republish re-sends dropped upserts from the location table; it cannot
	// recover a dropped withdraw (the path is gone locally, and the CP does
	// not tell a cluster which of its own rows it holds), so these are the
	// drops that remain unreconciled.
	droppedWithdrawals uint64
}

type pending struct {
	// loc is the row to upsert; nil means withdraw path.
	loc  *pb.GlobalLocation
	path string
	// republish marks an intent queued by a reconciliation pass rather than
	// by the live write path. A live intent for the same path replaces it.
	republish bool
}

func newPublishQueue() *publishQueue {
	return &publishQueue{
		pending: make(map[string]*pending),
		lastPub: make(map[string]time.Time),
		touched: make(map[string]time.Time),
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
			if p.republish {
				// Now a live intent; it no longer belongs to a pass.
				p.republish = false
				q.repPending--
			}
			q.touched[path] = now
			return true
		}
		return false
	}
	q.lastPub[path] = now
	q.touched[path] = now
	q.enqueueLocked(&pending{loc: loc, path: path})
	return true
}

// Republish queues a reconciliation upsert read from the location table at
// readAt. It returns false when the row was skipped.
//
// Three rules keep it from ever making the index less correct than live
// publishing would:
//
//   - A pending intent for the path wins. It was queued by the live write
//     path and is at least as new as the table read.
//   - A live intent queued at or after readAt wins, even if it has already
//     been sent. The table read may predate that write or delete, and
//     re-sending the older row would resurrect a withdrawn path or roll back
//     a newer size.
//   - It does not touch the coalescing clock. A real write right after a
//     republish must still publish immediately, not be suppressed as a
//     duplicate of a reconciliation row.
func (q *publishQueue) Republish(loc *pb.GlobalLocation, readAt time.Time) bool {
	if loc == nil || loc.GetFilePath() == "" {
		return false
	}
	q.mu.Lock()
	defer q.mu.Unlock()

	path := loc.GetFilePath()
	if _, queued := q.pending[path]; queued {
		return false
	}
	if t, ok := q.touched[path]; ok && !t.Before(readAt) {
		return false
	}
	q.enqueueLocked(&pending{loc: loc, path: path, republish: true})
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
	q.touched[path] = now
	q.enqueueLocked(&pending{loc: nil, path: path})
}

func (q *publishQueue) enqueueLocked(p *pending) {
	if prev, exists := q.pending[p.path]; !exists {
		q.order = append(q.order, p.path)
	} else if prev.republish {
		q.repPending--
	}
	if p.republish {
		q.repPending++
	}
	q.pending[p.path] = p

	// Over depth: drop the OLDEST entry. The newest intent is the one that
	// reflects current reality, and the global index converges on the last
	// write; dropping the newest would leave the index permanently
	// disagreeing with the cluster until that path is written again.
	for len(q.order) > publishQueueDepth {
		oldest := q.order[0]
		q.order = q.order[1:]
		q.dropLocked(oldest)
	}
}

// dropLocked discards the pending intent for path and counts it. order may
// hold stale paths whose intent was already drained; those are not drops.
func (q *publishQueue) dropLocked(path string) {
	p, ok := q.pending[path]
	if !ok {
		return
	}
	delete(q.pending, path)
	q.dropped++
	if p.loc == nil {
		q.droppedWithdrawals++
	}
	if p.republish {
		q.repPending--
		q.repDropped++
	}
}

// republishPending reports republish-sourced intents still queued or in
// flight, and how many upsert sends have failed so far.
func (q *publishQueue) republishPending() (int, uint64) {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.repPending + q.repInflight, q.sendFailures
}

// AckSent records that the last drained upsert batch reached the stream.
// There is one sender, so at most one batch is ever in flight.
func (q *publishQueue) AckSent() {
	q.mu.Lock()
	q.repInflight = 0
	q.mu.Unlock()
}

// republishDropped reports republish-sourced intents ever dropped for depth.
func (q *publishQueue) republishDropped() uint64 {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.repDropped
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
		if p.republish {
			q.repPending--
			q.repInflight++
		}
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
	// Same for the live-intent clock, except that an entry newer than a
	// running republish's table read must survive: it is what stops that
	// pass from re-sending a row the live path already superseded.
	if len(q.touched) > 2*publishQueueDepth {
		cutoff := time.Now().Add(-10 * publishCoalesceWindow)
		if !q.holdSince.IsZero() && q.holdSince.Before(cutoff) {
			cutoff = q.holdSince
		}
		for k, t := range q.touched {
			if t.Before(cutoff) {
				delete(q.touched, k)
			}
		}
	}
	return upserts, withdrawals
}

// holdTouched pins live-intent timestamps at or after since against GC for
// the duration of a republish pass; release with the zero time.
func (q *publishQueue) holdTouched(since time.Time) {
	q.mu.Lock()
	q.holdSince = since
	q.mu.Unlock()
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

	// Requeued rows go back as live intents; whatever pass drained them
	// sees the failure through sendFailures and reports itself incomplete.
	if len(upserts) > 0 {
		q.repInflight = 0
		q.sendFailures++
	}

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
		q.dropLocked(last)
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

// DroppedWithdrawals reports how many of the dropped intents were
// withdrawals. Republish heals dropped upserts; it cannot heal these.
func (q *publishQueue) DroppedWithdrawals() uint64 {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.droppedWithdrawals
}

// publishable reports whether this path may cross the cluster boundary.
//
// Two conditions, both required, and the second is the one that is easy to
// forget: the bytes must actually be in object storage. The global data
// path is object storage only, so advertising a path whose only copy is on
// a local NVMe tells another cluster to fetch something it structurally
// cannot reach.
//
// A third rule: chunk objects never cross. A chunked file is published as one
// parent row (IsChunked, ChunkSize, FileSize) once every chunk is in the
// bucket, and readers derive "<file>_chunk_<n>" keys from it
// (cache.remoteObjectKey). The cache still records a cloud row per chunk in
// the LOCAL coordinator, and before this filter every one of them was queued
// here too: a 640-chunk checkpoint put 640 rows into an index whose premise
// is that it stays small, and could push real parent rows out of the
// 4096-deep queue.
func publishable(filePath, storageTier string, allowlist []string) bool {
	if storageTier != tierCloud {
		return false
	}
	if isChunkObjectPath(filePath) {
		return false
	}
	return globalcp.MatchesAny(filePath, allowlist)
}

// chunkObjectSuffix mirrors the cache's chunk key layout. Duplicated for the
// same import-cycle reason as tierCloud; the definition must stay identical
// to cache.parentFilePathFromChunkPath (suffix after the LAST "_chunk_" must
// parse as an integer), so a user file named "report_chunk_draft" still
// publishes.
const chunkObjectSuffix = "_chunk_"

func isChunkObjectPath(path string) bool {
	idx := strings.LastIndex(path, chunkObjectSuffix)
	if idx <= 0 {
		return false
	}
	_, err := strconv.ParseInt(path[idx+len(chunkObjectSuffix):], 10, 64)
	return err == nil
}

// tierCloud is the StorageTier string the cache uses for object storage.
// Duplicated as a constant rather than imported: internal/cache imports the
// coordinator, so importing it back here would be a cycle.
const tierCloud = "cloud"

package globalsync

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"fuse-client/internal/coordinator"
	"fuse-client/internal/pb"
)

// Republish: why it exists.
//
// The live publish trigger is UpdateFileLocation, so a row reaches the global
// index only when a path is written. Three things lose rows with no later
// write to restore them:
//
//   - a control-plane restart (its default store is in memory, and every
//     subscriber's mirror is replaced by the new, empty snapshot on reconnect);
//   - queue overflow while the stream is down (publishQueueDepth, oldest first);
//   - writes that land before this agent's first registration, when the
//     accepted allowlist is still empty and OnCloudPersist drops everything.
//
// Models and checkpoints are rarely rewritten, so "wait for the next write"
// means never. After every successful RegisterCluster the agent therefore
// reads its own location table under the ACCEPTED publish prefixes and
// re-queues every row the live path would have published.
//
// Scope rule: a republished row passes exactly the same publishable() filter
// as a live one (cloud tier, not a chunk object, inside the accepted
// allowlist with path-boundary matching). Republish can only re-send what
// live publishing is already allowed to send; it never widens visibility.
//
// Not done here (needs a protocol change): withdrawing rows the CP still
// holds for this cluster that no longer exist locally. The CP filters a
// cluster's own rows out of its snapshot, so the agent cannot see them.
// Dropped withdrawals are counted separately so that gap stays visible.

const (
	// defaultRepublishMinInterval spaces full passes. Each pass ranges the
	// allowlisted part of the location table, so a flapping connection must
	// not turn into a range per reconnect. A pass that is due sooner waits;
	// it is not skipped, so a CP restart is always followed by a pass.
	defaultRepublishMinInterval = 30 * time.Second

	// defaultRepublishMaxRows caps one pass. The global index is meant to stay
	// small enough to mirror; a pass that would exceed this is truncated (in
	// path order) and does not count as full.
	defaultRepublishMaxRows = 50000

	// defaultRepublishRowsPerSec paces how fast a pass hands rows to the
	// queue, so a large republish cannot flood the CP or its subscribers.
	defaultRepublishRowsPerSec = 2000

	// republishHighWater keeps a pass from filling the queue. Above it the
	// pass waits, so republish rows can never push live intents out through
	// the oldest-first drop.
	republishHighWater = publishQueueDepth / 4

	// republishPoll is how often a waiting pass re-checks the queue. Each
	// check also wakes the sender, which drains one batch.
	republishPoll = 50 * time.Millisecond
)

// LocationSource is the local location table, as the republish pass needs it.
// *coordinator.CoordinatorService implements it.
//
// RangeFileLocations (all replica rows per path) rather than
// ListFileLocations (first row per path): the first row is often a node's
// NVMe replica, and keying off it would skip cloud-backed files.
type LocationSource interface {
	RangeFileLocations(ctx context.Context, prefix string) (map[string][]*coordinator.FileLocation, error)
}

// RepublishStatus is the republish part of /api/global/status.
type RepublishStatus struct {
	// Enabled is false when no LocationSource is configured.
	Enabled bool `json:"enabled"`
	Running bool `json:"running"`
	// Passes counts passes started; Full counts passes that finished with
	// every in-scope row sent.
	Passes uint64 `json:"passes_total"`
	Full   uint64 `json:"full_passes_total"`
	// RowsTotal counts rows queued by republish across all passes.
	RowsTotal uint64 `json:"rows_total"`

	LastStartedAt  time.Time `json:"last_started_at,omitempty"`
	LastFinishedAt time.Time `json:"last_finished_at,omitempty"`
	// LastCandidates is how many in-scope rows the last pass found;
	// LastQueued how many it queued (the rest were already pending, or were
	// superseded by a newer live write or delete).
	LastCandidates int  `json:"last_candidates"`
	LastQueued     int  `json:"last_queued"`
	LastTruncated  bool `json:"last_truncated"`
	// LastError is why the last pass did not complete, if it did not.
	LastError string `json:"last_error,omitempty"`
}

// republisher holds republish state. Guarded by its own mutex so status
// reads never contend with the agent's connection state.
type republisher struct {
	mu         sync.Mutex
	st         RepublishStatus
	lastFullAt time.Time
}

func (r *republisher) snapshot() (RepublishStatus, time.Time) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.st, r.lastFullAt
}

// republishDelay is how long a pass must wait so that passes start at least
// min apart. A non-positive min means no spacing.
func republishDelay(lastStart, now time.Time, min time.Duration) time.Duration {
	if min <= 0 || lastStart.IsZero() {
		return 0
	}
	if d := lastStart.Add(min).Sub(now); d > 0 {
		return d
	}
	return 0
}

func (a *Agent) republishMinInterval() time.Duration {
	switch {
	case a.cfg.RepublishMinInterval < 0:
		return 0
	case a.cfg.RepublishMinInterval == 0:
		return defaultRepublishMinInterval
	}
	return a.cfg.RepublishMinInterval
}

func (a *Agent) republishMaxRows() int {
	if a.cfg.RepublishMaxRows > 0 {
		return a.cfg.RepublishMaxRows
	}
	return defaultRepublishMaxRows
}

func (a *Agent) republishRowsPerSec() int {
	if a.cfg.RepublishRowsPerSec > 0 {
		return a.cfg.RepublishRowsPerSec
	}
	return defaultRepublishRowsPerSec
}

// startRepublish launches one pass bound to ctx (the stream's lifetime) and
// returns a channel closed when it exits. A nil source returns a closed
// channel.
func (a *Agent) startRepublish(ctx context.Context, accepted []string) <-chan struct{} {
	done := make(chan struct{})
	if a.cfg.Locations == nil || len(accepted) == 0 {
		close(done)
		return done
	}
	go func() {
		defer close(done)
		a.runRepublish(ctx, accepted)
	}()
	return done
}

func (a *Agent) runRepublish(ctx context.Context, accepted []string) {
	r := &a.republish

	r.mu.Lock()
	lastStart := r.st.LastStartedAt
	r.mu.Unlock()
	if d := republishDelay(lastStart, time.Now(), a.republishMinInterval()); d > 0 {
		t := time.NewTimer(d)
		select {
		case <-ctx.Done():
			t.Stop()
			return
		case <-t.C:
		}
	}

	r.mu.Lock()
	r.st.Running = true
	r.st.Passes++
	r.st.LastStartedAt = time.Now()
	r.st.LastError = ""
	r.st.LastTruncated = false
	r.st.LastCandidates = 0
	r.st.LastQueued = 0
	r.mu.Unlock()

	queued, err := a.republishPass(ctx, accepted)

	r.mu.Lock()
	defer r.mu.Unlock()
	r.st.Running = false
	r.st.LastFinishedAt = time.Now()
	r.st.LastQueued = queued
	r.st.RowsTotal += uint64(queued)
	if err != nil {
		r.st.LastError = err.Error()
		a.logger.Printf("republish pass incomplete: %v (queued %d of %d)", err, queued, r.st.LastCandidates)
		return
	}
	if r.st.LastTruncated {
		r.st.LastError = fmt.Sprintf("truncated at %d rows", a.republishMaxRows())
		a.logger.Printf("republish pass truncated: %d in-scope rows, cap %d", r.st.LastCandidates, a.republishMaxRows())
		return
	}
	r.st.Full++
	r.lastFullAt = r.st.LastFinishedAt
	a.logger.Printf("republish pass complete: %d in-scope rows, %d queued", r.st.LastCandidates, queued)
}

// republishPass collects in-scope rows and feeds them to the queue, paced,
// then waits until every row it queued has been sent (or superseded).
func (a *Agent) republishPass(ctx context.Context, accepted []string) (int, error) {
	// readAt is taken BEFORE the table read: any live intent at or after it
	// may postdate what the read returns, and Republish lets it win.
	readAt := time.Now()
	a.queue.holdTouched(readAt)
	defer a.queue.holdTouched(time.Time{})
	droppedBefore := a.queue.republishDropped()
	_, failuresBefore := a.queue.republishPending()

	rows, truncated, err := a.collectRepublish(ctx, accepted)
	a.republish.mu.Lock()
	a.republish.st.LastCandidates = len(rows)
	a.republish.st.LastTruncated = truncated
	a.republish.mu.Unlock()
	if err != nil {
		return 0, err
	}

	perBatch := time.Duration(float64(time.Second) * float64(publishBatchMax) / float64(a.republishRowsPerSec()))
	batchStart := time.Now()
	queued := 0
	for i, loc := range rows {
		if err := a.waitQueueRoom(ctx); err != nil {
			return queued, err
		}
		if a.queue.Republish(loc, readAt) {
			queued++
		}
		if (i+1)%publishBatchMax == 0 {
			a.wake()
			if rest := perBatch - time.Since(batchStart); rest > 0 {
				if err := sleepCtx(ctx, rest); err != nil {
					return queued, err
				}
			}
			batchStart = time.Now()
		}
	}
	a.wake()

	// "Full" means handed to the stream, not merely queued: wait until no
	// row from this pass is queued or in flight. (A successful Send is the
	// strongest acknowledgement the protocol has; if the CP dies after it,
	// the next registration starts another pass.)
	for {
		pending, failures := a.queue.republishPending()
		if failures != failuresBefore {
			return queued, fmt.Errorf("a publish batch failed to send during the pass")
		}
		if pending == 0 {
			break
		}
		a.wake()
		if err := sleepCtx(ctx, republishPoll); err != nil {
			return queued, err
		}
	}
	if d := a.queue.republishDropped() - droppedBefore; d > 0 {
		return queued, fmt.Errorf("%d republish rows dropped from the queue", d)
	}
	return queued, nil
}

// collectRepublish ranges the location table under each accepted prefix and
// returns the rows live publishing would send, sorted by path and capped.
func (a *Agent) collectRepublish(ctx context.Context, accepted []string) ([]*pb.GlobalLocation, bool, error) {
	best := make(map[string]*coordinator.FileLocation)
	for _, prefix := range accepted {
		if prefix == "" {
			continue // matches nothing, and "" would range the whole table
		}
		byPath, err := a.cfg.Locations.RangeFileLocations(ctx, prefix)
		if err != nil {
			return nil, false, fmt.Errorf("range %s: %w", prefix, err)
		}
		for path, locs := range byPath {
			for _, l := range locs {
				if l == nil {
					continue
				}
				// The store's prefix match is a raw string prefix ("/models"
				// also returns "/models-v2/..."). publishable re-applies the
				// boundary-aware allowlist check, exactly as OnCloudPersist
				// does, so nothing outside the accepted scope survives.
				if !publishable(path, l.StorageTier, accepted) {
					continue
				}
				if cur, ok := best[path]; !ok || l.LastAccessed.After(cur.LastAccessed) {
					best[path] = l
				}
			}
		}
		if err := ctx.Err(); err != nil {
			return nil, false, err
		}
	}

	paths := make([]string, 0, len(best))
	for p := range best {
		paths = append(paths, p)
	}
	sort.Strings(paths)
	truncated := false
	if max := a.republishMaxRows(); len(paths) > max {
		paths = paths[:max]
		truncated = true
	}

	out := make([]*pb.GlobalLocation, 0, len(paths))
	for _, p := range paths {
		l := best[p]
		row := &pb.GlobalLocation{
			ClusterId: a.cfg.ClusterID,
			FilePath:  p,
			FileSize:  l.FileSize,
			IsChunked: l.IsChunked,
			ChunkSize: l.ChunkSize,
			Object:    a.cfg.Object,
		}
		// Carry the original publish time where the row has one, so a
		// republish does not make this cluster's copy look newer than
		// another cluster's in mirror ordering.
		if !l.LastAccessed.IsZero() {
			row.PublishedUnix = l.LastAccessed.Unix()
		}
		out = append(out, row)
	}
	return out, truncated, nil
}

// waitQueueRoom blocks while the queue is above the high-water mark, waking
// the sender each poll so the backlog drains.
func (a *Agent) waitQueueRoom(ctx context.Context) error {
	for a.queue.Len() >= republishHighWater {
		a.wake()
		if err := sleepCtx(ctx, republishPoll); err != nil {
			return err
		}
	}
	return ctx.Err()
}

func sleepCtx(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// Compile-time check that the coordinator satisfies LocationSource.
var _ LocationSource = (*coordinator.CoordinatorService)(nil)

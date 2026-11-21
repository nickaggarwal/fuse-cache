package api

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"fuse-client/internal/cache"
)

// Peer fill: pull a blob another node holds in large parallel ranges.
//
// A node starting an image that another node just filled read it one nydusd
// request at a time: each backend read went nydusd -> this node -> one ranged
// peer read of the same few hundred KiB, waited on in series by nydusd's few
// prefetch threads. On the Nebius H200 lab (30.6 GB image) that second node
// loaded vLLM's weights in 169 s against 88 s on the first node, which filled
// the same blob from the registry in 16 parallel 8 MiB ranges; it was slower
// than nydusd reading the registry directly (157 s). On cpu-w3 the second
// node was ~25 s behind the first for the same reason.
//
// Now the first read of a blob the cluster holds but this node does not
// starts one fill of the whole blob from its holders, with the same machinery
// as a registry fill (fill_serve.go): fillWorkers range reads of
// fillUnitBytes in parallel into one staged file, the units a waiting read
// needs (and the next few, read-ahead) fetched first, and that read answered
// from the staged file as soon as they land. When every unit is in, the file
// is hashed against its digest, committed and published, so this node serves
// the blob locally (and the Rust store-front can serve it natively).
//
// Hedging: a unit the holders cannot deliver within peerUnitTimeout, or at
// all (busy, gone, a holder that lost the copy), is fetched from the registry
// instead when the request named one (proxy form, with nydusd's token). The
// cache can then never make a start slower than reading the registry by more
// than one timeout per unit. Without a registry source a failed unit fails
// the reads waiting on it, which fall back to the per-chunk read path.
//
// The committed copy is not uploaded again (PutFromFileLocal): the holder it
// came from persists its own upload.

const (
	// peerUnitTimeout bounds one unit from the holders before the registry
	// is asked for it. A healthy holder serves 8 MiB in tens of ms.
	peerUnitTimeout = 10 * time.Second
	// peerFillMaxConcurrent caps simultaneous peer fills per node; their
	// range reads share fillNodeParallel.
	peerFillMaxConcurrent = 8
	// peerFillRetryAfter spaces attempts after a failed fill.
	peerFillRetryAfter = 30 * time.Second
	// peerFillMinBytes: smaller blobs are read whole by the planned range
	// path in one request anyway.
	peerFillMinBytes = 4 << 20
)

// peerBlobSource is the cache side of a peer fill.
type peerBlobSource interface {
	ReadBlobRangeFromPeers(ctx context.Context, entry *cache.CacheEntry, off, n int64) ([]byte, error)
	LocalBlobPresent(filePath string, size int64) bool
	CreateWriteFile(ctx context.Context, filePath string) (*os.File, string, error)
	PutFromFileLocal(ctx context.Context, filePath, stagedPath string, size int64) error
}

type peerFiller struct {
	enabled  atomic.Bool
	once     sync.Once
	sem      chan struct{}
	reqSem   chan struct{}
	mu       sync.Mutex
	inflight map[string]bool
	failedAt map[string]time.Time

	started      atomic.Int64
	completed    atomic.Int64
	failed       atomic.Int64
	rejected     atomic.Int64
	bytes        atomic.Int64
	unitsPeer    atomic.Int64
	unitsHedged  atomic.Int64 // units the registry served after the holders failed
	peerUnitErrs atomic.Int64
}

// EnablePeerFill turns whole-blob peer fills on or off.
func (h *Handler) EnablePeerFill(on bool) {
	h.peerFill.enabled.Store(on)
}

func (f *peerFiller) init() {
	f.once.Do(func() {
		f.sem = make(chan struct{}, peerFillMaxConcurrent)
		f.reqSem = make(chan struct{}, fillNodeParallel)
		f.inflight = map[string]bool{}
		f.failedAt = map[string]time.Time{}
	})
}

// PeerFillStats is exported to /metrics.
type PeerFillStats struct {
	Started, Completed, Failed, Rejected, Bytes int64
	UnitsPeer, UnitsHedged, PeerUnitErrors      int64
	Inflight                                    int
}

func (h *Handler) peerFillStats() PeerFillStats {
	f := &h.peerFill
	f.mu.Lock()
	n := len(f.inflight)
	f.mu.Unlock()
	return PeerFillStats{
		Started: f.started.Load(), Completed: f.completed.Load(), Failed: f.failed.Load(),
		Rejected: f.rejected.Load(), Bytes: f.bytes.Load(), Inflight: n,
		UnitsPeer: f.unitsPeer.Load(), UnitsHedged: f.unitsHedged.Load(), PeerUnitErrors: f.peerUnitErrs.Load(),
	}
}

// maybeStartPeerFill starts a peer fill of entry's blob when this node has
// no whole copy and none is being filled, and returns the live fill to serve
// from (nil: serve the read the per-chunk way).
func (h *Handler) maybeStartPeerFill(r *http.Request, entry *cache.CacheEntry, hexDigest string) *liveFill {
	f := &h.peerFill
	if !f.enabled.Load() || entry == nil || entry.Size < peerFillMinBytes || entry.Size > registryFillMaxBytes {
		return nil
	}
	src, ok := h.cacheManager.(peerBlobSource)
	if !ok || src.LocalBlobPresent(entry.FilePath, entry.Size) {
		return nil
	}
	f.init()
	f.mu.Lock()
	if f.inflight[hexDigest] {
		f.mu.Unlock()
		return h.awaitLiveFill(hexDigest)
	}
	if time.Since(f.failedAt[hexDigest]) < peerFillRetryAfter {
		f.mu.Unlock()
		return nil
	}
	f.inflight[hexDigest] = true
	f.mu.Unlock()

	// The registry this request named is the hedge for units the holders
	// cannot deliver.
	var reg *url.URL
	var auth string
	if h.regFill.enabled.Load() {
		if u, ok := registryFillSource(r, hexDigest); ok && h.regFill.hostAllowed(u.Host) {
			reg, auth = u, r.Header.Get("Authorization")
			if !strings.HasPrefix(auth, "Bearer ") {
				// Resolved lazily on the first hedged unit (fetchPeerUnit).
				auth = "unresolved:" + auth
			}
		}
	}

	ready := make(chan *liveFill, 1)
	f.started.Add(1)
	go func() {
		err := h.runPeerFill(src, entry, hexDigest, reg, auth, ready)
		f.mu.Lock()
		delete(f.inflight, hexDigest)
		if err != nil {
			f.failedAt[hexDigest] = time.Now()
		}
		f.mu.Unlock()
		switch {
		case err == nil:
			f.completed.Add(1)
		case errors.Is(err, errFillDigest):
			f.rejected.Add(1)
			h.logger.Printf("[PEERFILL] %s: %v", hexDigest[:12], err)
		default:
			f.failed.Add(1)
			h.logger.Printf("[PEERFILL] %s: %v", hexDigest[:12], err)
		}
	}()
	select {
	case lf := <-ready:
		return lf
	case <-time.After(time.Second):
		return nil
	}
}

// awaitLiveFill returns the live fill of a peer fill that has started but may
// not have staged its file yet (it can be queued behind other fills).
func (h *Handler) awaitLiveFill(hexDigest string) *liveFill {
	for i := 0; i < 50; i++ {
		if lf := h.liveFills.get(hexDigest); lf != nil {
			return lf
		}
		h.peerFill.mu.Lock()
		running := h.peerFill.inflight[hexDigest]
		h.peerFill.mu.Unlock()
		if !running {
			return nil
		}
		time.Sleep(20 * time.Millisecond)
	}
	return nil
}

func (h *Handler) runPeerFill(src peerBlobSource, entry *cache.CacheEntry, hexDigest string, reg *url.URL, auth string, ready chan<- *liveFill) error {
	f := &h.peerFill
	f.sem <- struct{}{}
	defer func() { <-f.sem }()
	ctx, cancel := context.WithTimeout(context.Background(), registryFillTimeout)
	defer cancel()
	filePath, size := entry.FilePath, entry.Size

	out, staged, err := src.CreateWriteFile(ctx, filePath)
	if err != nil {
		ready <- nil
		return err
	}
	defer os.Remove(staged)
	if err := out.Truncate(size); err != nil {
		out.Close()
		ready <- nil
		return err
	}
	lf := newLiveFill(hexDigest, size, fillUnitBytes, out, staged)
	h.liveFills.put(lf)
	defer h.liveFills.del(hexDigest)
	ready <- lf

	var wg sync.WaitGroup
	var firstErr error
	var errMu sync.Mutex
	for w := 0; w < fillWorkers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				u := lf.next()
				if u < 0 {
					return
				}
				uerr := h.fetchPeerUnit(ctx, src, entry, lf, u, reg, auth)
				lf.markDone(u, uerr == nil)
				if uerr != nil {
					errMu.Lock()
					if firstErr == nil {
						firstErr = uerr
					}
					errMu.Unlock()
				}
			}
		}()
	}
	wg.Wait()
	if firstErr != nil {
		lf.finish(firstErr)
		out.Close()
		return fmt.Errorf("peer ranges: %w", firstErr)
	}

	// Same rule as a registry fill: nothing is committed or published until
	// it hashes to its digest.
	if _, err := out.Seek(0, io.SeekStart); err != nil {
		lf.finish(err)
		out.Close()
		return err
	}
	digester := sha256.New()
	if _, err := io.Copy(digester, out); err != nil {
		lf.finish(err)
		out.Close()
		return err
	}
	if got := hex.EncodeToString(digester.Sum(nil)); got != hexDigest {
		lf.finish(errFillDigest)
		out.Close()
		return fmt.Errorf("%w (got %s)", errFillDigest, got[:12])
	}
	if err := out.Sync(); err != nil {
		lf.finish(err)
		out.Close()
		return err
	}
	if err := src.PutFromFileLocal(ctx, filePath, staged, size); err != nil {
		lf.finish(err)
		out.Close()
		return err
	}
	if bv, ok := h.cacheManager.(blobVerifyMarker); ok {
		bv.MarkBlobVerified(filePath)
	}
	lf.finish(nil)
	h.liveFills.del(hexDigest)
	out.Close()
	h.logger.Printf("[PEERFILL] %s: %d bytes (%d-byte units, %d from the registry)", hexDigest[:12], size, lf.unit, f.unitsHedged.Load())
	return nil
}

// fetchPeerUnit lands unit u from the holders, or from the registry when the
// holders fail or take longer than peerUnitTimeout.
func (h *Handler) fetchPeerUnit(ctx context.Context, src peerBlobSource, entry *cache.CacheEntry, lf *liveFill, u int64, reg *url.URL, auth string) error {
	f := &h.peerFill
	start := u * lf.unit
	n := min(lf.unit, lf.size-start)
	f.reqSem <- struct{}{}
	defer func() { <-f.reqSem }()

	var lastErr error
	for attempt := 0; attempt < fillUnitAttempts; attempt++ {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		uctx, cancel := context.WithTimeout(ctx, peerUnitTimeout)
		data, err := src.ReadBlobRangeFromPeers(uctx, entry, start, n)
		cancel()
		if err == nil {
			if _, err := lf.file.WriteAt(data, start); err != nil {
				return err
			}
			f.unitsPeer.Add(1)
			f.bytes.Add(int64(len(data)))
			return nil
		}
		f.peerUnitErrs.Add(1)
		lastErr = err
		if reg != nil {
			break // hedge now rather than wait out more peer attempts
		}
	}
	if reg == nil {
		return lastErr
	}
	if raw, ok := strings.CutPrefix(auth, "unresolved:"); ok {
		auth = h.regFill.resolveFillAuth(ctx, reg, raw)
	}
	if err := h.regFill.fetchUnit(ctx, reg, auth, lf, u, nil); err != nil {
		return fmt.Errorf("unit %d: peers: %v; registry: %w", u, lastErr, err)
	}
	f.unitsHedged.Add(1)
	f.bytes.Add(n)
	return nil
}

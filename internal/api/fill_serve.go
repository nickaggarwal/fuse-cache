package api

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"sync"
	"time"
)

// Serving reads from a fill in progress.
//
// A registry fill (registry_fill.go) used to answer every read of its blob
// with 503 until the whole blob had landed and hashed. On the first node to
// start an image that meant nydusd read every byte from the registry itself
// while the fill downloaded the same bytes again beside it: 0 bytes served
// from the cache, 27 GB pulled for a 13.5 GB image, and the start 10 s slower
// than not filling at all (cpu-w3, vLLM 7B). Dragonfly, which answers a read
// from the piece it is downloading, started the same image in 155 s against
// fuse-cache's 203 s.
//
// A fill now downloads the blob in fetch units (default 4 MiB, smaller than
// the 16 MiB storage chunk so a waiting read needs less) with several range
// GETs in parallel, into one staged file. A read that arrives for a blob
// being filled moves its units to the front of the queue and waits for just
// those units, then is answered from the staged file. The image is pulled
// from the registry once, by this node, and this node's reads are served
// from it as it lands. When every unit is in, the file is hashed against the
// digest and committed; only then is it published for other nodes.
//
// Integrity: bytes served before the digest check come from the registry the
// request itself named, over TLS, with the requester's token; they are what
// nydusd would have read directly. A blob whose final hash does not match is
// discarded (never committed, never shared) and later reads fall back to the
// registry. A unit that fails to download fails the reads waiting on it with
// 503, so nydusd reads that range directly.

const (
	defaultFillUnit     = 4 << 20
	fillWorkers         = 8
	fillUnitAttempts    = 3
	fillWaitTimeout     = 60 * time.Second
	fillServeMaxPending = 64
)

// fillUnitBytes is the fetch unit; set from -registry-fill-unit-mb.
var fillUnitBytes int64 = defaultFillUnit

// SetFillUnitBytes sets the fetch unit (min 256 KiB).
func SetFillUnitBytes(n int64) {
	if n >= 256<<10 {
		fillUnitBytes = n
	}
}

// liveFill is one blob being filled.
type liveFill struct {
	digest string
	size   int64
	unit   int64
	file   *os.File // staged file, written with WriteAt, read with ReadAt
	path   string

	mu       sync.Mutex
	cond     *sync.Cond
	done     []bool // unit landed
	failed   []bool // unit gave up
	queued   []bool // unit is in the queue or being fetched
	queue    []int64
	finished bool  // every unit landed (or the fill stopped)
	err      error // fill-level failure
}

func newLiveFill(digest string, size, unit int64, f *os.File, path string) *liveFill {
	n := (size + unit - 1) / unit
	lf := &liveFill{digest: digest, size: size, unit: unit, file: f, path: path,
		done: make([]bool, n), failed: make([]bool, n), queued: make([]bool, n)}
	lf.cond = sync.NewCond(&lf.mu)
	for i := int64(0); i < n; i++ {
		lf.queue = append(lf.queue, i)
		lf.queued[i] = true
	}
	return lf
}

func (lf *liveFill) units() int64 { return int64(len(lf.done)) }

// next pops the next unit to fetch, -1 when none is left.
func (lf *liveFill) next() int64 {
	lf.mu.Lock()
	defer lf.mu.Unlock()
	for len(lf.queue) > 0 {
		u := lf.queue[0]
		lf.queue = lf.queue[1:]
		if !lf.done[u] && !lf.failed[u] {
			return u
		}
	}
	return -1
}

// prioritize moves units [a,b] to the front of the queue (a waiting read).
func (lf *liveFill) prioritize(a, b int64) {
	lf.mu.Lock()
	defer lf.mu.Unlock()
	var front []int64
	for u := a; u <= b && u < lf.units(); u++ {
		if !lf.done[u] && !lf.failed[u] {
			front = append(front, u)
		}
	}
	if len(front) == 0 {
		return
	}
	rest := lf.queue[:0:0]
	want := map[int64]bool{}
	for _, u := range front {
		want[u] = true
	}
	for _, u := range lf.queue {
		if !want[u] {
			rest = append(rest, u)
		}
	}
	lf.queue = append(front, rest...)
}

func (lf *liveFill) markDone(u int64, ok bool) {
	lf.mu.Lock()
	if ok {
		lf.done[u] = true
	} else {
		lf.failed[u] = true
	}
	lf.cond.Broadcast()
	lf.mu.Unlock()
}

func (lf *liveFill) finish(err error) {
	lf.mu.Lock()
	lf.finished = true
	lf.err = err
	lf.cond.Broadcast()
	lf.mu.Unlock()
}

// waitRange blocks until every unit covering [start,end] has landed. It
// returns an error when one of them failed, the fill stopped, or ctx ends.
func (lf *liveFill) waitRange(ctx context.Context, start, end int64) error {
	a, b := start/lf.unit, end/lf.unit
	lf.prioritize(a, b)
	stop := context.AfterFunc(ctx, func() {
		lf.mu.Lock()
		lf.cond.Broadcast()
		lf.mu.Unlock()
	})
	defer stop()
	lf.mu.Lock()
	defer lf.mu.Unlock()
	for {
		all := true
		for u := a; u <= b; u++ {
			if lf.failed[u] {
				return fmt.Errorf("unit %d failed", u)
			}
			if !lf.done[u] {
				all = false
			}
		}
		if all {
			return nil
		}
		if lf.finished {
			if lf.err != nil {
				return lf.err
			}
			return errors.New("fill stopped")
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		lf.cond.Wait()
	}
}

// liveFills indexes fills in progress by digest.
type liveFills struct {
	mu sync.Mutex
	m  map[string]*liveFill
}

func (l *liveFills) get(d string) *liveFill {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.m[d]
}

func (l *liveFills) put(lf *liveFill) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.m == nil {
		l.m = map[string]*liveFill{}
	}
	l.m[lf.digest] = lf
}

func (l *liveFills) del(d string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	delete(l.m, d)
}

// blobSizeFromRegistry learns the blob size with a 1-byte range GET (a HEAD
// on a registry blob redirects to storage that may not answer HEAD).
func (f *registryFiller) blobSize(ctx context.Context, src *url.URL, auth string) (int64, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, src.String(), nil)
	if err != nil {
		return 0, err
	}
	if auth != "" {
		req.Header.Set("Authorization", auth)
	}
	req.Header.Set("Range", "bytes=0-0")
	resp, err := f.cfg.HTTPClient.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<10))
	switch resp.StatusCode {
	case http.StatusPartialContent:
		var a, b, total int64
		if _, err := fmt.Sscanf(resp.Header.Get("Content-Range"), "bytes %d-%d/%d", &a, &b, &total); err != nil || total <= 0 {
			return 0, fmt.Errorf("bad Content-Range %q", resp.Header.Get("Content-Range"))
		}
		return total, nil
	case http.StatusOK:
		if resp.ContentLength > 0 {
			return resp.ContentLength, nil
		}
	}
	return 0, fmt.Errorf("size probe: http %d", resp.StatusCode)
}

// fetchUnit downloads unit u into the staged file.
func (f *registryFiller) fetchUnit(ctx context.Context, src *url.URL, auth string, lf *liveFill, u int64, charge func(int64) error) error {
	start := u * lf.unit
	end := start + lf.unit - 1
	if end >= lf.size {
		end = lf.size - 1
	}
	var lastErr error
	for attempt := 0; attempt < fillUnitAttempts; attempt++ {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, src.String(), nil)
		if err != nil {
			return err
		}
		if auth != "" {
			req.Header.Set("Authorization", auth)
		}
		req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", start, end))
		resp, err := f.cfg.HTTPClient.Do(req)
		if err != nil {
			lastErr = err
			continue
		}
		if resp.StatusCode != http.StatusPartialContent {
			resp.Body.Close()
			lastErr = fmt.Errorf("range %d-%d: http %d", start, end, resp.StatusCode)
			continue
		}
		buf := make([]byte, end-start+1)
		_, err = io.ReadFull(resp.Body, buf)
		resp.Body.Close()
		if err != nil {
			lastErr = err
			continue
		}
		if _, err := lf.file.WriteAt(buf, start); err != nil {
			return err
		}
		if charge != nil {
			_ = charge(int64(len(buf)))
		}
		return nil
	}
	return lastErr
}

// serveFromLiveFill answers a range from a fill in progress. ok is false when
// the range cannot be served (the caller answers 503 and nydusd falls back).
func (h *Handler) serveFromLiveFill(w http.ResponseWriter, r *http.Request, lf *liveFill, rangeHeader string) bool {
	start, end, err := parseByteRange(rangeHeader, lf.size)
	if err != nil {
		return false
	}
	if end-start+1 > ociBlobMaxRange {
		end = start + ociBlobMaxRange - 1
	}
	ctx, cancel := context.WithTimeout(r.Context(), fillWaitTimeout)
	defer cancel()
	waitStart := time.Now()
	if err := lf.waitRange(ctx, start, end); err != nil {
		h.regFill.serveMiss.Add(1)
		return false
	}
	buf := make([]byte, end-start+1)
	if _, err := lf.file.ReadAt(buf, start); err != nil && err != io.EOF {
		h.regFill.serveMiss.Add(1)
		return false
	}
	h.regFill.servedBytes.Add(int64(len(buf)))
	h.regFill.servedReads.Add(1)
	h.regFill.waitNanos.Add(int64(time.Since(waitStart)))
	w.Header().Set("Accept-Ranges", "bytes")
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Length", fmt.Sprint(len(buf)))
	if rangeHeader == "" {
		w.WriteHeader(http.StatusOK)
	} else {
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, lf.size))
		w.WriteHeader(http.StatusPartialContent)
	}
	_, _ = w.Write(buf)
	return true
}

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
)

// Registry fill: fetch a blob the cluster does not hold, on its first miss.
//
// nydusd sends every backend read to this proxy as an HTTP proxy request for
// the registry URL (GET http://<registry>/v2/<repo>/blobs/sha256:<hex>, with
// a Range and nydusd's own "Authorization: Bearer" token). Until now a blob
// the cluster did not hold answered 503 and nydusd went to the registry
// itself, read by read: a cold start through fuse-cache measured the same as
// no cache at all (193 s vs 200 s to vLLM Ready on cpu-w3, 36 ms vs 38 ms per
// backend read), while Dragonfly, which fetches on demand and keeps what it
// fetched, reached 155 s.
//
// Now, with -registry-fill on, the first miss of a blob starts one background
// download of the whole blob from the registry, using the requesting
// nydusd's token for that one request (never stored past the download).
// nydusd startup touches every blob, so this pulls the whole image during
// the first start without needing the manifest. The download:
//
//   - streams to a staged file, hashed on the way in; only a body that hashes
//     to its digest is committed, exactly like PUT /api/files/oci/sha256/<hex>
//     (a wrong or truncated body is discarded, never served or shared);
//   - is single-flight per blob on this node and, through the coordinator's
//     fetch lease, across the cluster: a node that loses the lease does not
//     download, it serves the blob from the holder once it lands;
//   - commits through PutFromFile, which publishes the blob to the
//     coordinator, so every other node reads it from this one, not from the
//     registry;
//   - runs in the warm I/O class (io_sched.go): while foreground reads are
//     active it yields bandwidth to them.
//
// While the download runs, reads keep answering 503 so nydusd fetches those
// ranges directly; a range is never blocked waiting for the whole blob. Once
// the blob is committed (seconds for most layers), reads are served locally.
// A failed download is retried on a later miss after registryFillRetryAfter.

const (
	// registryFillMaxBytes bounds one fill (the largest weight layers are
	// tens of GB); the upload cap does not apply, the digest check does.
	registryFillMaxBytes = 200 << 30
	// registryFillTimeout bounds one download end to end.
	registryFillTimeout = 30 * time.Minute
	// registryFillRetryAfter spaces attempts after a failed fill.
	registryFillRetryAfter = 30 * time.Second
	// registryFillMaxConcurrent caps simultaneous blob fills per node (each
	// runs fillWorkers range GETs).
	registryFillMaxConcurrent = 4
)

// RegistryFillConfig configures fetch-on-miss.
type RegistryFillConfig struct {
	Enabled bool
	// AllowedHosts, if non-empty, limits which registries this node will
	// fetch from (exact host match). Empty allows any host a request names.
	AllowedHosts []string
	// HTTPClient is used for registry requests; nil uses a default.
	HTTPClient *http.Client
}

type registryFiller struct {
	cfg      RegistryFillConfig
	enabled  atomic.Bool
	sem      chan struct{}
	mu       sync.Mutex
	inflight map[string]bool
	failedAt map[string]time.Time

	started   atomic.Int64
	completed atomic.Int64
	failed    atomic.Int64
	rejected  atomic.Int64 // digest mismatch
	leaseLost atomic.Int64 // another node is filling it
	bytes     atomic.Int64
	// Reads answered from a fill in progress (fill_serve.go).
	servedReads atomic.Int64
	servedBytes atomic.Int64
	serveMiss   atomic.Int64
	waitNanos   atomic.Int64
}

// blobFillCommitter is the cache side of a fill: stage, commit, and mark
// verified (the same calls PUT /api/files uses).
type blobFillCommitter interface {
	CreateWriteFile(ctx context.Context, filePath string) (*os.File, string, error)
	PutFromFile(ctx context.Context, filePath string, stagedPath string, size int64, lastAccessed time.Time) error
}

// fetchLeaseTaker is the cluster single-flight (coordinator fetch lease).
type fetchLeaseTaker interface {
	TryOriginLease(ctx context.Context, key string, ttl time.Duration) (granted bool, release func())
}

// EnableRegistryFill turns fetch-on-miss on (see RegistryFillConfig).
func (h *Handler) EnableRegistryFill(cfg RegistryFillConfig) {
	if cfg.HTTPClient == nil {
		cfg.HTTPClient = &http.Client{Timeout: registryFillTimeout}
	}
	h.regFill.cfg = cfg
	h.regFill.sem = make(chan struct{}, registryFillMaxConcurrent)
	h.regFill.enabled.Store(cfg.Enabled)
}

// RegistryFillStats is exported to /metrics.
type RegistryFillStats struct {
	Started, Completed, Failed, Rejected, LeaseLost, Bytes int64
	ServedReads, ServedBytes, ServeMiss                    int64
	ServeWaitSec                                           float64
	Inflight                                               int
}

func (h *Handler) registryFillStats() RegistryFillStats {
	f := &h.regFill
	f.mu.Lock()
	n := len(f.inflight)
	f.mu.Unlock()
	return RegistryFillStats{
		Started: f.started.Load(), Completed: f.completed.Load(), Failed: f.failed.Load(),
		Rejected: f.rejected.Load(), LeaseLost: f.leaseLost.Load(), Bytes: f.bytes.Load(), Inflight: n,
		ServedReads: f.servedReads.Load(), ServedBytes: f.servedBytes.Load(), ServeMiss: f.serveMiss.Load(),
		ServeWaitSec: time.Duration(f.waitNanos.Load()).Seconds(),
	}
}

// registryFillSource extracts the upstream blob URL from a proxy-form
// request (absolute URI naming a registry blob). ok is false for anything
// else, including the plain /v2/<repo>/blobs/<digest> form, which carries no
// registry host.
func registryFillSource(r *http.Request, hexDigest string) (*url.URL, bool) {
	if r.URL == nil || r.URL.Host == "" {
		return nil, false
	}
	p := r.URL.Path
	if !strings.HasPrefix(p, "/v2/") || !strings.HasSuffix(p, "/blobs/sha256:"+hexDigest) {
		return nil, false
	}
	u := *r.URL
	// nydusd talks plain HTTP to this node-local proxy (use_http); the
	// registry itself is HTTPS.
	u.Scheme = "https"
	u.RawQuery = ""
	return &u, true
}

func (f *registryFiller) hostAllowed(host string) bool {
	if len(f.cfg.AllowedHosts) == 0 {
		return true
	}
	h := strings.ToLower(host)
	for _, a := range f.cfg.AllowedHosts {
		if strings.ToLower(strings.TrimSpace(a)) == h {
			return true
		}
	}
	return false
}

// maybeStartRegistryFill starts a background fill of the blob r names, if
// fetch-on-miss is on and no fill for it is running or recently failed.
// It never blocks the request.
func (h *Handler) maybeStartRegistryFill(r *http.Request, hexDigest string) {
	f := &h.regFill
	if !f.enabled.Load() {
		return
	}
	src, ok := registryFillSource(r, hexDigest)
	if !ok || !f.hostAllowed(src.Host) {
		return
	}
	auth := r.Header.Get("Authorization")
	f.mu.Lock()
	if f.inflight == nil {
		f.inflight = map[string]bool{}
		f.failedAt = map[string]time.Time{}
	}
	if f.inflight[hexDigest] || time.Since(f.failedAt[hexDigest]) < registryFillRetryAfter {
		f.mu.Unlock()
		return
	}
	f.inflight[hexDigest] = true
	f.mu.Unlock()
	f.started.Add(1)
	go func() {
		err := h.runRegistryFill(src, auth, hexDigest)
		f.mu.Lock()
		delete(f.inflight, hexDigest)
		if err != nil && !errors.Is(err, errFillLeaseLost) {
			f.failedAt[hexDigest] = time.Now()
		}
		f.mu.Unlock()
		switch {
		case err == nil:
			f.completed.Add(1)
		case errors.Is(err, errFillLeaseLost):
			f.leaseLost.Add(1)
		case errors.Is(err, errFillDigest):
			f.rejected.Add(1)
			h.logger.Printf("[FILL] %s: %v", hexDigest[:12], err)
		default:
			f.failed.Add(1)
			h.logger.Printf("[FILL] %s: %v", hexDigest[:12], err)
		}
	}()
}

var (
	errFillLeaseLost = errors.New("another node is filling this blob")
	errFillDigest    = errors.New("registry body does not match the blob digest")
)

// fillCharger charges fill bytes to the warm I/O class (sleeping while
// foreground reads need the bandwidth).
type fillCharger interface {
	ChargeWarmIO(ctx context.Context, n int64) error
}

func (h *Handler) runRegistryFill(src *url.URL, auth, hexDigest string) error {
	f := &h.regFill
	f.sem <- struct{}{}
	defer func() { <-f.sem }()

	filePath := OCIBlobPrefix + hexDigest
	ctx, cancel := context.WithTimeout(context.Background(), registryFillTimeout)
	defer cancel()

	// Another holder may have landed it while this one queued.
	if _, ok := h.ociBlobEntry(ctx, filePath); ok {
		return nil
	}
	if lt, ok := h.cacheManager.(fetchLeaseTaker); ok {
		granted, release := lt.TryOriginLease(ctx, filePath, 60*time.Second)
		if !granted {
			return errFillLeaseLost
		}
		defer release()
	}
	committer, ok := h.cacheManager.(blobFillCommitter)
	if !ok {
		return errors.New("cache cannot stage files")
	}
	size, err := f.blobSize(ctx, src, auth)
	if err != nil {
		return fmt.Errorf("registry size: %w", err)
	}
	if size > registryFillMaxBytes {
		return fmt.Errorf("blob larger than %d bytes", int64(registryFillMaxBytes))
	}

	out, staged, err := committer.CreateWriteFile(ctx, filePath)
	if err != nil {
		return err
	}
	defer os.Remove(staged)
	if err := out.Truncate(size); err != nil {
		out.Close()
		return err
	}
	lf := newLiveFill(hexDigest, size, fillUnitBytes, out, staged)
	h.liveFills.put(lf)
	defer h.liveFills.del(hexDigest)

	charger, _ := h.cacheManager.(fillCharger)
	charge := func(n int64) error {
		f.bytes.Add(n)
		if charger != nil {
			return charger.ChargeWarmIO(ctx, n)
		}
		return nil
	}
	// Parallel range GETs; a waiting read reorders the queue (prioritize).
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
				uerr := f.fetchUnit(ctx, src, auth, lf, u, charge)
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
		return fmt.Errorf("registry ranges: %w", firstErr)
	}

	// Every unit is in: hash the whole file against the digest before it is
	// committed or shared. Reads already served came from the registry the
	// request named; nothing unverified reaches other nodes.
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
	// Commit while readers may still hold the file open: PutFromFile renames
	// it, and an open descriptor keeps reading the same inode.
	if err := out.Sync(); err != nil {
		lf.finish(err)
		out.Close()
		return err
	}
	if err := committer.PutFromFile(ctx, filePath, staged, size, time.Now()); err != nil {
		lf.finish(err)
		out.Close()
		return err
	}
	if bv, ok := h.cacheManager.(blobVerifyMarker); ok {
		bv.MarkBlobVerified(filePath)
	}
	h.ociMisses.forget(filePath)
	lf.finish(nil)
	h.liveFills.del(hexDigest)
	// Mark the fill not running only after it is committed and indexed,
	// so a reader that sees "not running" also sees the blob.
	out.Close()
	h.logger.Printf("[FILL] %s: %d bytes from %s (%d-byte units)", hexDigest[:12], size, src.Host, lf.unit)
	return nil
}

// running reports whether a fill of hexDigest is in flight on this node.
func (f *registryFiller) running(hexDigest string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.inflight[hexDigest]
}

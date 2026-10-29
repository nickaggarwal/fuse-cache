package cache

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

// OCI blobs are content-addressed: /oci/sha256/<hex> must hash to <hex>.
// The upload path checks that (api handleFilePutStreaming, commit), but a
// copy can still go bad afterwards — a disk fault, a torn write, an operator
// mistake — and a size-only check passes it. Live, one same-size corrupted
// store copy fed nydusd bad chunks ("failed to decompress chunk") and the
// container failed with EIO. A copy that is never checked spreads: every
// node that reads it from the store or a peer gets the same bytes.
//
// So every whole local blob, and every blob a node assembles from chunk
// files, is hashed against its digest before it is trusted as a source:
//
//   - verified once per file version (path + size + mtime), cached in
//     memory and in a "<blob>.verified" marker beside it, so a restart does
//     not re-hash the store (an in-place rewrite changes the mtime and voids
//     the marker; bit rot that keeps the mtime is the scrubber's job);
//   - a mismatch quarantines the copy: it is moved aside (never deleted, so
//     an operator can inspect it), this node's location rows for it are
//     retracted, and readers fall through to the next holder or the
//     registry;
//   - verification of a large blob is single-flighted; readers that arrive
//     while it runs are served from the next tier rather than waiting, so a
//     cold start never blocks on hashing a multi-GB file.

const ociBlobPrefix = "/oci/sha256/"

// blobDigestFromPath returns the hex digest an OCI blob path names, or "".
// Chunk paths (<blob>_chunk_N) return "": a chunk has no digest of its own.
func blobDigestFromPath(filePath string) string {
	hex, ok := strings.CutPrefix(filePath, ociBlobPrefix)
	if !ok || len(hex) != 64 {
		return ""
	}
	for _, c := range hex {
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return ""
		}
	}
	return hex
}

type blobVersion struct {
	size  int64
	mtime int64
}

type blobVerdict int

const (
	blobUnknown  blobVerdict = iota // not checked yet, or a check is running
	blobGood                        // hashed to its digest
	blobBadMoved                    // mismatched; moved aside
)

// blobVerifier remembers verified file versions and runs at most one hash
// per blob at a time.
type blobVerifier struct {
	mu       sync.Mutex
	verified map[string]blobVersion // full local path -> version that hashed right
	running  map[string]bool
}

// init makes the maps; callers hold mu. The zero value is usable.
func (v *blobVerifier) init() {
	if v.running == nil {
		v.running = map[string]bool{}
		v.verified = map[string]blobVersion{}
	}
}

func (v *blobVerifier) known(path string, ver blobVersion) bool {
	v.mu.Lock()
	defer v.mu.Unlock()
	got, ok := v.verified[path]
	return ok && got == ver
}

func (v *blobVerifier) begin(path string) bool {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.init()
	if v.running[path] {
		return false
	}
	v.running[path] = true
	return true
}

func (v *blobVerifier) end(path string, ver blobVersion, good bool) {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.init()
	delete(v.running, path)
	if good {
		v.verified[path] = ver
	} else {
		delete(v.verified, path)
	}
}

func (v *blobVerifier) forget(path string) {
	v.mu.Lock()
	defer v.mu.Unlock()
	delete(v.verified, path)
}

// blobLocalVerdict decides whether the whole local blob at full may be
// served. wait=true hashes it now if needed (writers, fills, background);
// wait=false never blocks: an unverified blob reports blobUnknown and starts
// a background check, so the caller serves from another tier this time.
func (cm *DefaultCacheManager) blobLocalVerdict(ctx context.Context, filePath, full string, wait bool) blobVerdict {
	want := blobDigestFromPath(filePath)
	if want == "" {
		return blobGood // not content-addressed: nothing to check against
	}
	info, err := os.Stat(full)
	if err != nil || info.IsDir() {
		return blobUnknown
	}
	ver := blobVersion{size: info.Size(), mtime: info.ModTime().UnixNano()}
	if cm.blobVerify.known(full, ver) {
		return blobGood
	}
	if readMarker(full) == ver {
		cm.blobVerify.end(full, ver, true)
		cm.publishVerifiedBlob(ctx, filePath, ver.size)
		return blobGood
	}
	if !wait {
		cm.goBackground(func() { cm.verifyLocalBlob(context.Background(), filePath, full) })
		return blobUnknown
	}
	return cm.verifyLocalBlob(ctx, filePath, full)
}

// verifyLocalBlob hashes the whole local blob at full once. On a mismatch
// the file is quarantined. Returns blobUnknown when another check is
// running or the file vanished.
func (cm *DefaultCacheManager) verifyLocalBlob(ctx context.Context, filePath, full string) blobVerdict {
	want := blobDigestFromPath(filePath)
	if want == "" {
		return blobGood
	}
	if !cm.blobVerify.begin(full) {
		return blobUnknown
	}
	f, err := os.Open(full)
	if err != nil {
		cm.blobVerify.end(full, blobVersion{}, false)
		return blobUnknown
	}
	info, _ := f.Stat()
	ver := blobVersion{size: info.Size(), mtime: info.ModTime().UnixNano()}
	started := time.Now()
	h := sha256.New()
	_, err = io.Copy(h, &ctxReader{ctx: ctx, r: f})
	f.Close()
	if err != nil {
		cm.blobVerify.end(full, ver, false)
		return blobUnknown
	}
	got := hex.EncodeToString(h.Sum(nil))
	cm.metrics.BlobVerified.Add(1)
	cm.metrics.BlobVerifyBytes.Add(ver.size)
	if got == want {
		cm.blobVerify.end(full, ver, true)
		writeMarker(full, ver)
		cm.publishVerifiedBlob(ctx, filePath, ver.size)
		return blobGood
	}
	cm.blobVerify.end(full, ver, false)
	cm.quarantineBlob(ctx, filePath, full, got, time.Since(started))
	return blobBadMoved
}

// verifyChunkedBlob hashes a blob held as complete chunk files
// (<blob>_chunk_0..N-1 at chunkSize). A mismatch quarantines every chunk
// file: the bad bytes are in one of them, and which one is unknowable
// without per-chunk digests. Returns blobUnknown when a chunk is missing.
func (cm *DefaultCacheManager) verifyChunkedBlob(ctx context.Context, filePath string, size, chunkSize int64) blobVerdict {
	want := blobDigestFromPath(filePath)
	if want == "" || chunkSize <= 0 || size <= 0 {
		return blobUnknown
	}
	key := filepath.Join(cm.config.NVMePath, strings.TrimPrefix(filePath, "/")) + "#chunks"
	if !cm.blobVerify.begin(key) {
		return blobUnknown
	}
	n := (size + chunkSize - 1) / chunkSize
	h := sha256.New()
	var paths []string
	ver := blobVersion{size: size}
	for i := int64(0); i < n; i++ {
		p := filepath.Join(cm.config.NVMePath, strings.TrimPrefix(fmt.Sprintf("%s_chunk_%d", filePath, i), "/"))
		f, err := os.Open(p)
		if err != nil {
			cm.blobVerify.end(key, ver, false)
			return blobUnknown
		}
		info, _ := f.Stat()
		wantLen := chunkSize
		if i == n-1 {
			wantLen = size - i*chunkSize
		}
		if info.Size() != wantLen {
			f.Close()
			cm.blobVerify.end(key, ver, false)
			return blobUnknown
		}
		ver.mtime ^= info.ModTime().UnixNano() // any chunk rewritten changes the version
		_, err = io.Copy(h, &ctxReader{ctx: ctx, r: f})
		f.Close()
		if err != nil {
			cm.blobVerify.end(key, ver, false)
			return blobUnknown
		}
		paths = append(paths, p)
	}
	got := hex.EncodeToString(h.Sum(nil))
	cm.metrics.BlobVerified.Add(1)
	cm.metrics.BlobVerifyBytes.Add(size)
	if got == want {
		cm.blobVerify.end(key, ver, true)
		cm.publishVerifiedChunkedBlob(ctx, filePath, size, chunkSize)
		return blobGood
	}
	cm.blobVerify.end(key, ver, false)
	for _, p := range paths {
		cm.moveAside(p)
	}
	cm.forgetBlobChunks(filePath, n)
	cm.metrics.BlobQuarantined.Add(1)
	cm.logger.Printf("[VERIFY] blob %s assembled from %d chunks hashes to %s: chunks quarantined", want[:12], n, got[:12])
	cm.retractLocalBlobRows(ctx, filePath)
	return blobBadMoved
}

// quarantineBlob moves a mismatched whole blob aside, drops it from the
// index and retracts this node's location rows so no reader is sent here.
func (cm *DefaultCacheManager) quarantineBlob(ctx context.Context, filePath, full, got string, took time.Duration) {
	cm.moveAside(full)
	cm.mu.Lock()
	cm.accountNVMeLocked(filePath, 0)
	cm.entries.del(filePath)
	cm.mu.Unlock()
	cm.invalidateRangeCache(filePath)
	cm.metrics.BlobQuarantined.Add(1)
	cm.logger.Printf("[VERIFY] blob %s hashes to %s after %s: quarantined %s", blobDigestFromPath(filePath)[:12], got[:12], took.Round(time.Millisecond), full)
	cm.retractLocalBlobRows(ctx, filePath)
}

func (cm *DefaultCacheManager) forgetBlobChunks(filePath string, n int64) {
	cm.mu.Lock()
	for i := int64(0); i < n; i++ {
		cp := fmt.Sprintf("%s_chunk_%d", filePath, i)
		cm.accountNVMeLocked(cp, 0)
		cm.entries.del(cp)
	}
	cm.mu.Unlock()
	cm.invalidateRangeCache(filePath)
}

// retractLocalBlobRows removes only this node's rows: other holders may
// have good copies and must keep being found.
func (cm *DefaultCacheManager) retractLocalBlobRows(ctx context.Context, filePath string) {
	if cm.config.Coordinator == nil {
		return
	}
	callCtx, cancel := context.WithTimeout(ctx, coordinatorCallTimeout)
	defer cancel()
	if err := cm.config.Coordinator.DeleteFileLocation(callCtx, filePath, cm.config.LocalPeerID, false); err != nil {
		cm.logger.Printf("[VERIFY] retracting %s for %s: %v", filePath, cm.config.LocalPeerID, err)
	}
}

// MarkBlobVerified records that the local whole blob at filePath was just
// checked against its digest by the writer (PUT/commit), so it is servable
// at once without a second hash.
func (cm *DefaultCacheManager) MarkBlobVerified(filePath string) {
	if blobDigestFromPath(filePath) == "" {
		return
	}
	full := filepath.Join(cm.config.NVMePath, strings.TrimPrefix(filePath, "/"))
	info, err := os.Stat(full)
	if err != nil {
		return
	}
	ver := blobVersion{size: info.Size(), mtime: info.ModTime().UnixNano()}
	cm.blobVerify.begin(full)
	cm.blobVerify.end(full, ver, true)
	writeMarker(full, ver)
}

// VerifiedBlobVersion reports the version (size, mtime ns) of the whole
// local blob at filePath when it is verified; it never blocks, and starts a
// background check when the blob is unverified. store-front serves a blob
// natively only at exactly this version.
func (cm *DefaultCacheManager) VerifiedBlobVersion(ctx context.Context, filePath string) (int64, int64, bool) {
	full := filepath.Join(cm.config.NVMePath, strings.TrimPrefix(filePath, "/"))
	if cm.blobLocalVerdict(ctx, filePath, full, false) != blobGood {
		return 0, 0, false
	}
	info, err := os.Stat(full)
	if err != nil {
		return 0, 0, false
	}
	return info.Size(), info.ModTime().UnixNano(), true
}

// ScrubBlobs hashes every whole blob and every complete chunk set under
// oci/sha256, one at a time. force ignores the in-memory cache and markers
// (the periodic scrub, which catches bit rot that keeps the mtime); without
// it, only unverified copies are hashed (the startup pass). Bad copies are
// quarantined like any other mismatch.
func (cm *DefaultCacheManager) ScrubBlobs(ctx context.Context, force bool) (checked, bad int) {
	dir := filepath.Join(cm.config.NVMePath, strings.TrimPrefix(ociBlobPrefix, "/"))
	ents, err := os.ReadDir(dir)
	if err != nil {
		return 0, 0
	}
	type chunkSet struct{ n, total int64 }
	sets := map[string]*chunkSet{}
	for _, e := range ents {
		name := e.Name()
		if e.IsDir() || strings.Contains(name, ".") {
			continue // markers, sidecars, staging and quarantined copies
		}
		if ctx.Err() != nil {
			return
		}
		if base, _, ok := strings.Cut(name, "_chunk_"); ok {
			info, err := e.Info()
			if err == nil {
				cs := sets[base]
				if cs == nil {
					cs = &chunkSet{}
					sets[base] = cs
				}
				cs.n++
				cs.total += info.Size()
			}
			continue
		}
		filePath := ociBlobPrefix + name
		full := filepath.Join(dir, name)
		if force {
			cm.blobVerify.forget(full)
			_ = os.Remove(full + ".verified")
		}
		if v := cm.blobLocalVerdict(ctx, filePath, full, true); v != blobUnknown {
			checked++
			if v == blobBadMoved {
				bad++
			}
		}
	}
	for base, cs := range sets {
		if ctx.Err() != nil {
			return
		}
		filePath := ociBlobPrefix + base
		entry, ok := cm.LookupEntry(ctx, filePath, false)
		if !ok || entry == nil || entry.Size != cs.total {
			continue // incomplete: verified when its last chunk lands
		}
		chunkSize := cm.effectiveChunkSize(entry)
		key := filepath.Join(dir, base) + "#chunks"
		if force {
			cm.blobVerify.forget(key)
		}
		if v := cm.verifyChunkedBlob(ctx, filePath, entry.Size, chunkSize); v != blobUnknown {
			checked++
			if v == blobBadMoved {
				bad++
			}
		}
	}
	return checked, bad
}

const defaultBlobScrubInterval = 24 * time.Hour

func (cm *DefaultCacheManager) startBlobScrubber() {
	every := cm.config.BlobScrubInterval
	if every == 0 {
		every = defaultBlobScrubInterval
	}
	cm.goLoop(func() {
		ctx := cm.shutdownCtx
		if n, bad := cm.ScrubBlobs(ctx, false); n > 0 {
			cm.logger.Printf("[VERIFY] startup: %d blobs checked, %d quarantined", n, bad)
		}
		if every < 0 {
			return
		}
		t := time.NewTicker(every)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				n, bad := cm.ScrubBlobs(ctx, true)
				cm.logger.Printf("[VERIFY] scrub: %d blobs checked, %d quarantined", n, bad)
			}
		}
	})
}

// publishVerifiedBlob publishes this node as a holder of the whole blob
// with its size. A blob placed on disk outside a PUT (a restore, a copy
// from another node) is adopted by rehydration but never announced, and a
// cluster whose only rows for a blob are size-0 holder rows cannot read it
// at all; a verified copy is the authoritative source, so it says so.
func (cm *DefaultCacheManager) publishVerifiedBlob(ctx context.Context, filePath string, size int64) {
	if size <= 0 {
		return
	}
	cm.mu.RLock()
	e, ok := cm.entries.get(filePath)
	cm.mu.RUnlock()
	entry := &CacheEntry{FilePath: filePath, StoragePath: filePath, Size: size}
	if ok && e != nil && e.Size == size {
		cp := *e
		entry = &cp
	}
	cm.goBackground(func() { cm.publishFileLocation(context.Background(), entry, TierNVMe) })
}

func (cm *DefaultCacheManager) publishVerifiedChunkedBlob(ctx context.Context, filePath string, size, chunkSize int64) {
	if size <= 0 || chunkSize <= 0 {
		return
	}
	entry := &CacheEntry{FilePath: filePath, StoragePath: filePath, Size: size, IsChunked: true, ChunkSize: chunkSize, NumChunks: chunkCountFor(size, chunkSize)}
	cm.goBackground(func() { cm.publishFileLocation(context.Background(), entry, TierNVMe) })
}

func markerPath(full string) string { return full + ".verified" }

func readMarker(full string) blobVersion {
	b, err := os.ReadFile(markerPath(full))
	if err != nil {
		return blobVersion{}
	}
	var v blobVersion
	if _, err := fmt.Sscanf(string(b), "%d %d", &v.size, &v.mtime); err != nil {
		return blobVersion{}
	}
	return v
}

func writeMarker(full string, v blobVersion) {
	tmp := markerPath(full) + ".tmp"
	if os.WriteFile(tmp, []byte(fmt.Sprintf("%d %d\n", v.size, v.mtime)), 0o644) == nil {
		_ = os.Rename(tmp, markerPath(full))
	}
}

// moveAside renames a bad file to <name>.corrupt-<unix> beside it. Startup
// rehydration skips that name, and nothing serves it.
func (cm *DefaultCacheManager) moveAside(full string) {
	_ = os.Remove(markerPath(full))
	dst := full + ".corrupt-" + strconv.FormatInt(time.Now().Unix(), 10)
	if err := os.Rename(full, dst); err != nil && !os.IsNotExist(err) {
		cm.logger.Printf("[VERIFY] moving %s aside: %v; removing it", full, err)
		_ = os.Remove(full)
	}
	cm.blobVerify.forget(full)
}

// ctxReader stops a long hash when its context ends (shutdown).
type ctxReader struct {
	ctx context.Context
	r   io.Reader
}

func (c *ctxReader) Read(p []byte) (int, error) {
	if err := c.ctx.Err(); err != nil {
		return 0, err
	}
	return c.r.Read(p)
}

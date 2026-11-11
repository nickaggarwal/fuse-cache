package cache

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Background scrubbing of local copies, after Ceph's deep scrub.
//
// Checksums were checked when bytes arrived (chunk_sums.go) and when a whole
// file was read through the NVMe tier, but a file that sat unread on disk was
// never checked until a pod needed it, so disk rot surfaced as a failed or
// corrupt cold start. The scrubber walks the cache in the background and
// checks every local copy it has a checksum for:
//
//   - a whole file with a .sha256 sidecar is hashed against it;
//   - a chunk file, or a whole file assembled from chunks, is checked against
//     its file's CRC-32C manifest in the cloud tier (chunk_sums.go).
//
// OCI blobs keep their own digest scrub (blob_verify.go) and are skipped here.
// A copy that fails is moved aside as <name>.corrupt-<unix> (never deleted, so
// an operator can look), dropped from the index and its accounting, and this
// node's location row for it is retracted: the next read refetches it from a
// peer or the cloud. Pinned and open files are scrubbed too; a bad pinned
// file is worse than a missing one.
//
// The scrubber runs in the "scrub" I/O class (io_sched.go), so it yields to
// foreground reads, and checks at most scrubMaxBytesPerPass per pass. A copy
// is re-checked only after scrubRecheckAfter, tracked by size+mtime, so a
// long-lived cache is covered once per interval rather than re-hashed
// continuously.

const (
	defaultScrubInterval = 6 * time.Hour
	// scrubRecheckAfter is how long a clean result stands for the same file
	// version (Ceph's deep scrub default is weekly).
	scrubRecheckAfter = 7 * 24 * time.Hour
	// scrubMaxBytesPerPass bounds one pass; the next pass resumes after the
	// files already checked.
	scrubMaxBytesPerPass = 64 << 30
	// scrubErrorWindow is how long a finding keeps SCRUB_ERRORS raised.
	scrubErrorWindow = 24 * time.Hour
	scrubReadBuf     = 1 << 20
)

type scrubRecord struct {
	size, mtime int64
	at          time.Time
}

type scrubState struct {
	mu      sync.Mutex
	checked map[string]scrubRecord
	// lastBad is when the most recent corrupt copy was found.
	lastBad  time.Time
	badCount int

	passes  atomic.Int64
	files   atomic.Int64
	bytes   atomic.Int64
	corrupt atomic.Int64
	running atomic.Bool
}

// ScrubResult summarizes one pass.
type ScrubResult struct {
	Checked int   `json:"checked"`
	Skipped int   `json:"skipped"` // no checksum to check against, or still fresh
	Corrupt int   `json:"corrupt"`
	Bytes   int64 `json:"bytes"`
}

// errScrubBusy: a pass is already running.
var errScrubBusy = errors.New("scrub already running")

// Scrub runs one pass. force re-checks files whose clean result is still
// fresh.
func (cm *DefaultCacheManager) Scrub(ctx context.Context, force bool) (ScrubResult, error) {
	var res ScrubResult
	if !cm.scrub.running.CompareAndSwap(false, true) {
		return res, errScrubBusy
	}
	defer cm.scrub.running.Store(false)
	ctx = withIOClass(ctx, ioScrub)
	base := filepath.Clean(cm.config.NVMePath)
	if base == "" || base == "." {
		return res, nil
	}
	now := time.Now()
	err := filepath.WalkDir(base, func(full string, d os.DirEntry, err error) error {
		if err != nil {
			if d != nil && d.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if d.IsDir() {
			if filepath.Dir(full) == base {
				switch d.Name() {
				case durabilityDirName, persistedLedgerDirName:
					return filepath.SkipDir
				case "oci":
					return filepath.SkipDir // blob_verify.go scrubs blobs by digest
				}
			}
			return nil
		}
		name := d.Name()
		if rehydrateSkipFile(name) || strings.HasSuffix(name, ".tmp") {
			return nil
		}
		if res.Bytes >= scrubMaxBytesPerPass {
			return filepath.SkipAll
		}
		info, ierr := d.Info()
		if ierr != nil || !info.Mode().IsRegular() {
			return nil
		}
		rel, rerr := filepath.Rel(base, full)
		if rerr != nil {
			return nil
		}
		key := "/" + filepath.ToSlash(rel)
		if !force && cm.scrubFresh(key, info, now) {
			res.Skipped++
			return nil
		}
		ok, checked, cerr := cm.scrubOne(ctx, key, full, info)
		if cerr != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return nil // unreadable now (evicted mid-walk): next pass
		}
		if !checked {
			res.Skipped++
			return nil
		}
		res.Checked++
		res.Bytes += info.Size()
		cm.scrub.files.Add(1)
		cm.scrub.bytes.Add(info.Size())
		if ok {
			cm.scrubMark(key, info, now)
			return nil
		}
		res.Corrupt++
		cm.quarantineLocal(ctx, key, full)
		return nil
	})
	cm.scrub.passes.Add(1)
	if errors.Is(err, filepath.SkipAll) {
		err = nil
	}
	return res, err
}

func (cm *DefaultCacheManager) scrubFresh(key string, info os.FileInfo, now time.Time) bool {
	cm.scrub.mu.Lock()
	defer cm.scrub.mu.Unlock()
	r, ok := cm.scrub.checked[key]
	return ok && r.size == info.Size() && r.mtime == info.ModTime().UnixNano() && now.Sub(r.at) < scrubRecheckAfter
}

func (cm *DefaultCacheManager) scrubMark(key string, info os.FileInfo, now time.Time) {
	cm.scrub.mu.Lock()
	defer cm.scrub.mu.Unlock()
	if cm.scrub.checked == nil {
		cm.scrub.checked = make(map[string]scrubRecord)
	}
	cm.scrub.checked[key] = scrubRecord{size: info.Size(), mtime: info.ModTime().UnixNano(), at: now}
}

// scrubOne checks one local file. checked is false when there is nothing to
// check it against.
func (cm *DefaultCacheManager) scrubOne(ctx context.Context, key, full string, info os.FileInfo) (ok, checked bool, err error) {
	// A chunk file: its own entry in the file's manifest.
	if parent, isChunk := parentFilePathFromChunkPath(key); isChunk {
		idx, _ := chunkIndexFromPath(key)
		m := cm.scrubManifest(ctx, parent)
		if m == nil || idx < 0 || idx >= int64(len(m.CRC32C)) {
			return false, false, nil
		}
		crcs, _, err := cm.hashFile(ctx, full, 0, false)
		if err != nil {
			return false, false, err
		}
		return len(crcs) == 1 && chunkMatches(m, idx, crcs[0], info.Size()), true, nil
	}
	// A whole file with a sidecar.
	if want, serr := os.ReadFile(full + ".sha256"); serr == nil && len(want) > 0 {
		sum, _, err := cm.hashFile(ctx, full, 0, true)
		if err != nil {
			return false, false, err
		}
		return len(sum) == 1 && sum[0] == strings.TrimSpace(string(want)), true, nil
	}
	// A whole file assembled from chunks: every stride against the manifest.
	m := cm.scrubManifest(ctx, key)
	if m == nil || m.Size != info.Size() || m.ChunkSize <= 0 {
		return false, false, nil
	}
	crcs, _, err := cm.hashFile(ctx, full, m.ChunkSize, false)
	if err != nil {
		return false, false, err
	}
	if len(crcs) != len(m.CRC32C) {
		return false, true, nil
	}
	for i, c := range crcs {
		if c != m.CRC32C[i] {
			return false, true, nil
		}
	}
	return true, true, nil
}

// chunkMatches compares one chunk's CRC and length with the manifest.
func chunkMatches(m *chunkSumsManifest, idx int64, crc string, size int64) bool {
	want := m.ChunkSize
	if idx == int64(len(m.CRC32C))-1 {
		want = m.Size - idx*m.ChunkSize
	}
	return size == want && crc == m.CRC32C[idx]
}

// scrubManifest fetches a file's manifest when remote verification is on;
// the scrubber reuses the read path's cache.
func (cm *DefaultCacheManager) scrubManifest(ctx context.Context, parent string) *chunkSumsManifest {
	if !cm.config.VerifyRemoteChunks || cm.cloudStorage == nil {
		return nil
	}
	return cm.chunkSums.get(ctx, parent, cm.fetchChunkSums(parent))
}

// hashFile reads full once, charging the scrub class. With stride > 0 it
// returns the CRC-32C of each stride; with stride 0 and sha it returns the
// file's SHA-256; with stride 0 and !sha the CRC-32C of the whole file.
func (cm *DefaultCacheManager) hashFile(ctx context.Context, full string, stride int64, sha bool) ([]string, int64, error) {
	f, err := os.Open(full)
	if err != nil {
		return nil, 0, err
	}
	defer f.Close()
	buf := make([]byte, scrubReadBuf)
	var out []string
	var total int64
	shaH := sha256.New()
	crc := crc32.New(castagnoli)
	var inStride int64
	flushCRC := func() {
		var b [4]byte
		c := crc.Sum32()
		b[0], b[1], b[2], b[3] = byte(c>>24), byte(c>>16), byte(c>>8), byte(c)
		out = append(out, hex.EncodeToString(b[:]))
		crc.Reset()
		inStride = 0
	}
	for {
		if err := ctx.Err(); err != nil {
			return nil, total, err
		}
		n, rerr := f.Read(buf)
		if n > 0 {
			chunk := buf[:n]
			total += int64(n)
			if err := cm.sched.charge(ctx, ioScrub, int64(n)); err != nil {
				return nil, total, err
			}
			switch {
			case sha:
				shaH.Write(chunk)
			case stride > 0:
				for len(chunk) > 0 {
					take := stride - inStride
					if take > int64(len(chunk)) {
						take = int64(len(chunk))
					}
					crc.Write(chunk[:take])
					inStride += take
					chunk = chunk[take:]
					if inStride == stride {
						flushCRC()
					}
				}
			default:
				crc.Write(chunk)
			}
		}
		if rerr == io.EOF {
			break
		}
		if rerr != nil {
			return nil, total, rerr
		}
	}
	switch {
	case sha:
		out = []string{hex.EncodeToString(shaH.Sum(nil))}
	case stride > 0:
		if inStride > 0 {
			flushCRC()
		}
	default:
		flushCRC()
	}
	return out, total, nil
}

// quarantineLocal moves a corrupt local copy aside and forgets it: index,
// accounting, verification records and this node's location row. The next
// read refetches from a peer or the cloud.
func (cm *DefaultCacheManager) quarantineLocal(ctx context.Context, key, full string) {
	cm.scrub.corrupt.Add(1)
	cm.scrub.mu.Lock()
	cm.scrub.lastBad = time.Now()
	cm.scrub.badCount++
	delete(cm.scrub.checked, key)
	cm.scrub.mu.Unlock()
	cm.logger.Printf("[SCRUB] %s does not match its checksum: moved aside, will refetch", key)

	unlock := cm.lockNVMeKey(key)
	cm.moveAside(full)
	_ = os.Remove(full + ".sha256")
	cm.nvmeVerify.forget(key)
	cm.mu.Lock()
	cm.unaccountNVMeLocked(key)
	cm.entries.del(key)
	cm.mu.Unlock()
	unlock()
	cm.invalidateRangeCache(key)
	if parent, ok := parentFilePathFromChunkPath(key); ok {
		cm.invalidateRangeCache(parent)
	}
	cm.queueEvictRetraction(key)
}

// startScrubber runs Scrub every interval (jittered), after a startup delay
// so a restarting node serves first.
func (cm *DefaultCacheManager) startScrubber() {
	every := cm.config.ScrubInterval
	if every < 0 {
		return
	}
	if every == 0 {
		every = defaultScrubInterval
	}
	cm.goLoop(func() {
		select {
		case <-cm.shutdownCtx.Done():
			return
		case <-time.After(jitteredDuration(10 * time.Minute)):
		}
		for {
			res, err := cm.Scrub(cm.shutdownCtx, false)
			if err == nil && (res.Checked > 0 || res.Corrupt > 0) {
				cm.logger.Printf("[SCRUB] pass: checked=%d corrupt=%d skipped=%d bytes=%d", res.Checked, res.Corrupt, res.Skipped, res.Bytes)
			}
			select {
			case <-cm.shutdownCtx.Done():
				return
			case <-time.After(jitteredDuration(every)):
			}
		}
	})
}

// ScrubStats reports scrub counters for /metrics.
func (cm *DefaultCacheManager) ScrubStats() (passes, files, bytes, corrupt int64) {
	return cm.scrub.passes.Load(), cm.scrub.files.Load(), cm.scrub.bytes.Load(), cm.scrub.corrupt.Load()
}

func (cm *DefaultCacheManager) scrubHealthCheck(now time.Time) *HealthCheck {
	cm.scrub.mu.Lock()
	last, n := cm.scrub.lastBad, cm.scrub.badCount
	cm.scrub.mu.Unlock()
	if last.IsZero() || now.Sub(last) > scrubErrorWindow {
		return nil
	}
	return &HealthCheck{
		Code: HealthScrubErrors, Severity: "degraded", Count: n,
		Message: "the scrubber found corrupt local copies in the last 24h (moved aside as *.corrupt-*, refetched on demand); check the node's disk",
	}
}

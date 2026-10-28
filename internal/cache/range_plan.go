package cache

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// Range planning for callers that read ranges of a file and do their own
// read-ahead, such as nydusd reading image blobs through the blob endpoint.
//
// ReadRange is built for a FUSE reader: it guesses sequential streams, reads
// ahead whole chunks into a per-file range cache, and copies each window into
// a fresh buffer. nydusd already prefetches the exact ranges it needs, from 4
// threads at scattered offsets, so those guesses missed: on a 7B image cold
// start 3,538 of 4,364 read-ahead chunks (81%) were evicted unread, and hybrid
// striping pulled 9.4 GB from blob storage at 65 MB/s while the cluster store
// held the whole blob. PlanRange does none of that. It resolves each chunk the
// range touches to a local file region or, when no local copy exists, fetches
// that one chunk (store/peer first, then cloud, no hybrid split, no read-ahead)
// and promotes it like any other remote read. The pieces are resolved before
// anything is sent, so a failed fetch can still become an error status.

// RangePiece is one contiguous part of a planned range: either a region of a
// local file, which WriteTo hands to the socket (sendfile when w is an HTTP
// response), or bytes fetched from a remote tier.
type RangePiece struct {
	file *os.File
	off  int64
	n    int64
	data []byte
	Tier CacheTier
}

// Len is the number of bytes the piece contributes.
func (p RangePiece) Len() int64 {
	if p.file != nil {
		return p.n
	}
	return int64(len(p.data))
}

// WriteTo writes the piece to w.
func (p RangePiece) WriteTo(w io.Writer) (int64, error) {
	if p.file == nil {
		n, err := w.Write(p.data)
		return int64(n), err
	}
	if _, err := p.file.Seek(p.off, io.SeekStart); err != nil {
		return 0, err
	}
	// io.CopyN wraps the *os.File in an io.LimitedReader, which net/http's
	// response writer passes to the socket's ReadFrom, i.e. sendfile.
	return io.CopyN(w, p.file, p.n)
}

// PlanRange resolves [offset, offset+length) of entry's file into pieces. The
// returned release closes any files the pieces hold and must be called once
// the pieces are written (or abandoned).
func (cm *DefaultCacheManager) PlanRange(ctx context.Context, entry *CacheEntry, offset, length int64) ([]RangePiece, func(), error) {
	var files []*os.File
	release := func() {
		for _, f := range files {
			f.Close()
		}
	}
	if entry == nil || offset < 0 || length <= 0 || offset >= entry.Size {
		return nil, release, nil
	}
	end := offset + length
	if end > entry.Size {
		end = entry.Size
	}
	started := time.Now()
	cm.noteBlobRead()
	var localBytes, remoteBytes int64

	// A whole local copy (store node, warm, or assembled file) serves any range.
	if f, ok := cm.openLocalExact(entry.FilePath, entry.Size); ok {
		files = append(files, f)
		cm.metrics.BlobRangeLocalBytes.Add(end - offset)
		cm.metrics.RecordReadWall(end-offset, time.Since(started), end-offset, 0, 0)
		return []RangePiece{{file: f, off: offset, n: end - offset, Tier: TierNVMe}}, release, nil
	}

	if !entry.IsChunked {
		got, err := cm.Get(ctx, entry.FilePath)
		if err != nil {
			return nil, release, err
		}
		data := got.Data
		if end > int64(len(data)) {
			end = int64(len(data))
		}
		if offset >= end {
			return nil, release, nil
		}
		cm.metrics.BlobRangeRemoteBytes.Add(end - offset)
		return []RangePiece{{data: data[offset:end], Tier: got.Tier}}, release, nil
	}

	chunkSize := cm.effectiveChunkSize(entry)
	if chunkSize <= 0 {
		return nil, release, fmt.Errorf("no chunk size for %s", entry.FilePath)
	}
	var pieces []RangePiece
	for idx := offset / chunkSize; idx*chunkSize < end; idx++ {
		chunkStart := idx * chunkSize
		chunkLen := chunkSize
		if chunkStart+chunkLen > entry.Size {
			chunkLen = entry.Size - chunkStart
		}
		from := max(offset, chunkStart) - chunkStart
		to := min(end, chunkStart+chunkLen) - chunkStart
		chunkPath := fmt.Sprintf("%s_chunk_%d", entry.FilePath, idx)

		if f, ok := cm.openLocalExact(chunkPath, chunkLen); ok {
			files = append(files, f)
			pieces = append(pieces, RangePiece{file: f, off: from, n: to - from, Tier: TierNVMe})
			localBytes += to - from
			continue
		}
		if win, ok := cm.readBlobWindowFromPeer(ctx, entry.FilePath, chunkPath, idx, from, to-from); ok {
			pieces = append(pieces, RangePiece{data: win, Tier: TierPeer})
			remoteBytes += int64(len(win))
			continue
		}
		data, tier, err := cm.readChunkData(ctx, entry.FilePath, idx, remoteReadOrderPeerFirst, false)
		if err != nil {
			release()
			return nil, func() {}, err
		}
		if tier != TierNVMe {
			// Keep the chunk for the next start on this node and for peers,
			// but not now: see deferBlobFill.
			cm.deferBlobFill(entry.FilePath, chunkPath, idx)
		}
		from, to = clampChunkSlice(from, to, int64(len(data)))
		pieces = append(pieces, RangePiece{data: data[from:to], Tier: tier})
		if tier == TierNVMe {
			localBytes += to - from
		} else {
			remoteBytes += to - from
		}
	}
	cm.metrics.BlobRangeLocalBytes.Add(localBytes)
	cm.metrics.BlobRangeRemoteBytes.Add(remoteBytes)
	cm.metrics.RecordReadWall(localBytes+remoteBytes, time.Since(started), localBytes, remoteBytes, 0)
	cm.touchEntries(entry.FilePath)
	return pieces, release, nil
}

// openLocalExact opens path's local NVMe file when it holds exactly want
// bytes, verified against its sidecar once per file version. A short file
// (still landing) or a longer one (written with another chunk stride) is not
// used.
func (cm *DefaultCacheManager) openLocalExact(path string, want int64) (*os.File, bool) {
	opener, ok := cm.nvmeStorage.(nvmeOpener)
	if !ok || want <= 0 {
		return nil, false
	}
	full := filepath.Join(cm.config.NVMePath, strings.TrimPrefix(path, "/"))
	if info, err := os.Stat(full); err != nil || info.Size() != want {
		return nil, false
	}
	reader, err := opener.Open(path)
	if err != nil {
		return nil, false
	}
	if reader.Size() != want || !cm.verifyNVMeReader(context.Background(), opener, path, reader) {
		reader.Close()
		return nil, false
	}
	return reader.f, true
}

// Deferred blob fills. Writing fetched chunks to NVMe while the container is
// starting doubled the node's disk work: nydusd writes its own cache of the
// same bytes, and the fill wrote all 13 GB of a 7B image's chunks at the same
// time (~35 s of write at the disk's ~350 MB/s), on the disk vLLM was
// starting from. Fills now queue and run only after blob reads have been
// quiet for blobFillQuietPeriod, so the start gets the disk and the chunks
// still land for the next start and for peers.
const (
	blobFillQuietPeriod = 15 * time.Second
	blobFillQueueMax    = 4096 // chunks; ~64 GiB at 16 MiB, beyond any image
)

type blobFillKey struct {
	filePath, chunkPath string
	chunkIndex          int64
}

type blobFillQueue struct {
	mu       sync.Mutex
	pending  []blobFillKey
	queued   map[string]struct{}
	lastRead time.Time
	started  bool
}

// noteBlobRead records blob-read activity, which holds deferred fills back.
func (cm *DefaultCacheManager) noteBlobRead() {
	q := &cm.blobFillQ
	q.mu.Lock()
	q.lastRead = time.Now()
	q.mu.Unlock()
}

// deferBlobFill queues a whole-chunk fill for after reads go quiet.
func (cm *DefaultCacheManager) deferBlobFill(filePath, chunkPath string, chunkIndex int64) {
	q := &cm.blobFillQ
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.queued == nil {
		q.queued = make(map[string]struct{})
	}
	if _, ok := q.queued[chunkPath]; ok {
		return
	}
	if len(q.pending) >= blobFillQueueMax {
		cm.metrics.BlobPromoteSkipped.Add(1)
		return
	}
	q.queued[chunkPath] = struct{}{}
	q.pending = append(q.pending, blobFillKey{filePath, chunkPath, chunkIndex})
	cm.metrics.BlobFillQueued.Add(1)
	if !q.started {
		q.started = true
		cm.goLoop(cm.runBlobFills)
	}
}

// runBlobFills drains the queue whenever blob reads have gone quiet,
// checking once a second. Exits on shutdown.
func (cm *DefaultCacheManager) runBlobFills() {
	ctx := cm.shutdownCtx
	if ctx == nil { // hand-built managers (tests) have no shutdown context
		ctx = context.Background()
	}
	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-tick.C:
			cm.drainBlobFills(ctx, now)
		}
	}
}

// drainBlobFills runs queued fills one at a time while blob reads have been
// quiet for blobFillQuietPeriod as of now, stopping as soon as a read lands.
// Returns the number of fills run.
func (cm *DefaultCacheManager) drainBlobFills(ctx context.Context, now time.Time) int {
	q := &cm.blobFillQ
	n := 0
	for ctx.Err() == nil {
		q.mu.Lock()
		if now.Sub(q.lastRead) < blobFillQuietPeriod || len(q.pending) == 0 {
			q.mu.Unlock()
			return n
		}
		k := q.pending[0]
		q.pending = q.pending[1:]
		delete(q.queued, k.chunkPath)
		q.mu.Unlock()
		cm.fillBlobChunk(ctx, k.filePath, k.chunkPath, k.chunkIndex)
		n++
		now = time.Now() // a read that landed during the fill pauses the drain
	}
	return n
}

// peerRangeReader is PeerStorage's ranged read (store first).
type peerRangeReader interface {
	ReadRange(ctx context.Context, path string, off, n int64) ([]byte, error)
}

// readBlobWindowFromPeer answers a window of a missing chunk with a ranged
// read from the store or a peer, and fills the whole chunk onto NVMe in the
// background. Waiting for the whole 16 MiB chunk before answering a ~1 MiB
// window kept first reads from the store at 38-52 s of weight loading
// against Zot's 18 s on cpu-w3, which streams just the range. Reports false
// when no holder answers, so the caller takes the whole-chunk path (which
// also tries the cloud).
func (cm *DefaultCacheManager) readBlobWindowFromPeer(ctx context.Context, filePath, chunkPath string, chunkIndex, from, n int64) ([]byte, bool) {
	if _, _, cached := cm.getChunkFromRangeCache(filePath, chunkIndex); cached {
		return nil, false // a fill already landed it in memory: use that
	}
	pr, ok := cm.peerStorage.(peerRangeReader)
	if !ok || n <= 0 {
		return nil, false
	}
	data, err := pr.ReadRange(ctx, chunkPath, from, n)
	if err != nil || int64(len(data)) != n {
		return nil, false
	}
	cm.deferBlobFill(filePath, chunkPath, chunkIndex)
	return data, true
}

// fillBlobChunk fetches a whole chunk (store/peer first) and lands it on
// NVMe, once per chunk. Called from the deferred-fill loop, one at a time.
func (cm *DefaultCacheManager) fillBlobChunk(ctx context.Context, filePath, chunkPath string, chunkIndex int64) {
	if !cm.blobFills.start(chunkPath) {
		return
	}
	defer cm.blobFills.done(chunkPath)
	if _, ok := cm.openLocalExactCheck(chunkPath); ok {
		return
	}
	data, tier, err := cm.readChunkData(ctx, filePath, chunkIndex, remoteReadOrderPeerFirst, false)
	if err != nil || tier == TierNVMe {
		return
	}
	dataCopy := make([]byte, len(data)) // data is shared with the range cache
	copy(dataCopy, data)
	cm.promoteChunkAndAdvertise(ctx, filePath, chunkPath, dataCopy)
	cm.metrics.BlobPromoted.Add(1)
}

// openLocalExactCheck reports whether chunkPath already has a local file.
func (cm *DefaultCacheManager) openLocalExactCheck(chunkPath string) (int64, bool) {
	full := filepath.Join(cm.config.NVMePath, strings.TrimPrefix(chunkPath, "/"))
	info, err := os.Stat(full)
	if err != nil || info.IsDir() || info.Size() == 0 {
		return 0, false
	}
	return info.Size(), true
}

// inflightSet tracks keys with a fill in progress. The zero value is usable.
type inflightSet struct {
	mu sync.Mutex
	m  map[string]struct{}
}

func (s *inflightSet) start(k string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.m == nil {
		s.m = make(map[string]struct{})
	}
	if _, ok := s.m[k]; ok {
		return false
	}
	s.m[k] = struct{}{}
	return true
}

func (s *inflightSet) done(k string) {
	s.mu.Lock()
	delete(s.m, k)
	s.mu.Unlock()
}

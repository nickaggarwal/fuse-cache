package cache

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
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
		data, tier, err := cm.readChunkData(ctx, entry.FilePath, idx, remoteReadOrderPeerFirst, false)
		if err != nil {
			release()
			return nil, func() {}, err
		}
		if tier != TierNVMe {
			// Keep the chunk: nydusd caches what it reads, but the next
			// container start on this node (nydus cache reset, new image
			// sharing the blob) and peers reading from this node need it on
			// NVMe. The promotion gate in maybeAdvertiseFetchedChunk is for
			// FUSE read bursts and skipped most chunks of a blob read.
			cm.promoteBlobChunk(entry.FilePath, chunkPath, data)
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

// promoteBlobChunk lands a remotely fetched blob chunk on NVMe and advertises
// this node as a holder, in the background and at most blobPromoteInflight at
// a time. A chunk that cannot get a slot is dropped from promotion, not
// queued: the read already succeeded.
func (cm *DefaultCacheManager) promoteBlobChunk(filePath, chunkPath string, data []byte) {
	if len(data) == 0 {
		return
	}
	select {
	case cm.blobPromoteSem() <- struct{}{}:
	default:
		cm.metrics.BlobPromoteSkipped.Add(1)
		return
	}
	dataCopy := make([]byte, len(data)) // data is shared with the range cache
	copy(dataCopy, data)
	cm.goBackground(func() {
		defer func() { <-cm.blobPromoteSem() }()
		cm.promoteChunkAndAdvertise(cm.shutdownCtx, filePath, chunkPath, dataCopy)
		cm.metrics.BlobPromoted.Add(1)
	})
}

const blobPromoteInflight = 16

func (cm *DefaultCacheManager) blobPromoteSem() chan struct{} {
	cm.blobPromoteOnce.Do(func() { cm.blobPromote = make(chan struct{}, blobPromoteInflight) })
	return cm.blobPromote
}

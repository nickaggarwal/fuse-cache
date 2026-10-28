package cache

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// Ranged reads of whole (non-chunked) files held on NVMe.
//
// FUSE hands down one window at a time — 128KiB by default. Serving each of
// those by reading the whole object back off NVMe and re-hashing it costs
// filesize/windowsize times the bytes the caller asked for: an 8MB file read
// end to end moved 512MB through the page cache and hashed 512MB, for 8MB of
// user data. Chunked files already avoided this (ReadRange slices only the
// chunks the window touches); anything at or below the chunk size did not,
// which is most files.
//
// Two pieces fix it: os.File.ReadAt for the bytes, and a verification record
// so the sidecar checksum is checked once per file rather than once per read.

// nvmeVerifyLimit caps the verification record. It is a cache, not a ledger:
// dropping an entry costs one extra hash, so a simple bound beats tracking
// exact liveness against eviction, delete, and overwrite.
const nvmeVerifyLimit = 4096

// nvmeFileID identifies the specific bytes a verification applies to. Size and
// mtime together are what NVMeStorage's write path can change: it renames a
// fully-written temp file into place, so any new content arrives with a new
// mtime, and a record that does not match is simply not used.
type nvmeFileID struct {
	size  int64
	mtime int64
}

// nvmeVerifier remembers which NVMe files have had their sidecar checksum
// checked, so a sequential read verifies once instead of once per window. The
// zero value is usable: several construction sites build a DefaultCacheManager
// by hand, and a verifier that had to be wired up would silently degrade to
// per-read hashing in whichever one forgot.
type nvmeVerifier struct {
	mu   sync.Mutex
	seen map[string]nvmeVerifyRecord
	// busy serialises verification of one path (lockPath).
	busy map[string]*sync.Mutex
}

// lockPath serialises verification of path; the returned func releases it.
func (v *nvmeVerifier) lockPath(path string) func() {
	v.mu.Lock()
	if v.busy == nil {
		v.busy = make(map[string]*sync.Mutex)
	}
	m := v.busy[path]
	if m == nil {
		m = &sync.Mutex{}
		v.busy[path] = m
	}
	v.mu.Unlock()
	m.Lock()
	return func() {
		m.Unlock()
		v.mu.Lock()
		if len(v.busy) > nvmeVerifyLimit { // bound memory; locks are cheap to recreate
			delete(v.busy, path)
		}
		v.mu.Unlock()
	}
}

type nvmeVerifyRecord struct {
	id nvmeFileID
	at time.Time
}

// verified reports whether path was already verified in its current form.
func (v *nvmeVerifier) verified(path string, id nvmeFileID) bool {
	v.mu.Lock()
	defer v.mu.Unlock()
	rec, ok := v.seen[path]
	return ok && rec.id == id
}

func (v *nvmeVerifier) markVerified(path string, id nvmeFileID) {
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.seen == nil {
		v.seen = make(map[string]nvmeVerifyRecord)
	}
	if len(v.seen) >= nvmeVerifyLimit {
		if _, refreshing := v.seen[path]; !refreshing {
			v.evictOldestLocked()
		}
	}
	v.seen[path] = nvmeVerifyRecord{id: id, at: time.Now()}
}

// forget drops any record for path. Called when the local copy goes away or is
// replaced, so a later file at the same path is verified afresh even if it
// happens to land on the same size and mtime.
func (v *nvmeVerifier) forget(path string) {
	v.mu.Lock()
	delete(v.seen, path)
	v.mu.Unlock()
}

func (v *nvmeVerifier) evictOldestLocked() {
	var oldestPath string
	var oldestAt time.Time
	for path, rec := range v.seen {
		if oldestPath == "" || rec.at.Before(oldestAt) {
			oldestPath, oldestAt = path, rec.at
		}
	}
	if oldestPath != "" {
		delete(v.seen, oldestPath)
	}
}

// nvmeOpener is the part of NVMeStorage the ranged path needs. Tests
// substitute a mock tier that does not implement it; those callers fall back
// to the whole-file path.
type nvmeOpener interface {
	Open(path string) (*NVMeFileReader, error)
	Read(ctx context.Context, path string) ([]byte, error)
}

// readWholeFileRange serves [offset, offset+size) of a non-chunked file from
// the local NVMe copy, verifying its checksum at most once per version of the
// file. Reports ok=false when the local copy is absent or unusable, in which
// case the caller falls through to the whole-file tier path.
func (cm *DefaultCacheManager) readWholeFileRange(ctx context.Context, filePath string, offset int64, size int) ([]byte, bool) {
	opener, ok := cm.nvmeStorage.(nvmeOpener)
	if !ok {
		return nil, false
	}
	reader, err := opener.Open(filePath)
	if err != nil {
		return nil, false
	}
	defer reader.Close()

	if !cm.verifyNVMeReader(ctx, opener, filePath, reader) {
		return nil, false
	}
	total := reader.Size()
	if offset >= total {
		return []byte{}, true
	}
	if end := offset + int64(size); end > total {
		size = int(total - offset)
	}
	buf := make([]byte, size)
	n, err := reader.ReadAt(buf, offset)
	if err != nil {
		cm.logger.Printf("NVMe ranged read failed for %s at offset %d: %v", filePath, offset, err)
		return nil, false
	}
	cm.touchEntries(filePath)
	return buf[:n], true
}

// readWholeChunkedRange serves a window of a chunked file from its whole local
// copy (left by a manifest warm, chunk assembly or a FUSE write) instead of
// synthesizing the window's 16 MiB chunks one by one. It only serves a copy
// whose size equals the file's recorded size: a short or still-assembling
// file falls through to the chunk path, which decides per chunk.
func (cm *DefaultCacheManager) readWholeChunkedRange(ctx context.Context, filePath string, wantSize, offset int64, size int) ([]byte, bool) {
	opener, ok := cm.nvmeStorage.(nvmeOpener)
	if !ok || wantSize <= 0 {
		return nil, false
	}
	reader, err := opener.Open(filePath)
	if err != nil {
		return nil, false
	}
	defer reader.Close()
	if reader.Size() != wantSize || !cm.verifyNVMeReader(ctx, opener, filePath, reader) {
		return nil, false
	}
	if offset >= wantSize {
		return []byte{}, true
	}
	if end := offset + int64(size); end > wantSize {
		size = int(wantSize - offset)
	}
	buf := make([]byte, size)
	n, err := reader.ReadAt(buf, offset)
	if err != nil && n != size {
		cm.logger.Printf("NVMe ranged read of whole chunked file %s at offset %d failed: %v", filePath, offset, err)
		return nil, false
	}
	cm.touchEntries(filePath)
	return buf[:n], true
}

// verifyNVMeReader checks the sidecar checksum unless this exact file version
// was already checked. A missing sidecar means unverifiable, not corrupt — the
// whole-file path treats it the same way. A mismatch is reported as unusable
// so the caller refetches from a remote tier.
func (cm *DefaultCacheManager) verifyNVMeReader(ctx context.Context, opener nvmeOpener, filePath string, reader *NVMeFileReader) bool {
	id := nvmeFileID{size: reader.Size(), mtime: reader.ModTimeNanos()}
	if cm.nvmeVerify.verified(filePath, id) {
		return true
	}
	// One hash per file version: concurrent first readers of a fresh large
	// file (nydusd's threads, several peers) wait for it instead of each
	// streaming SHA-256 over the whole file.
	release := cm.nvmeVerify.lockPath(filePath)
	defer release()
	if cm.nvmeVerify.verified(filePath, id) {
		return true
	}
	sidecar, err := opener.Read(ctx, filePath+".sha256")
	if err != nil {
		// No sidecar: nothing to check against, same as the whole-file path.
		cm.nvmeVerify.markVerified(filePath, id)
		return true
	}
	actual, err := reader.Hash()
	if err != nil {
		return false
	}
	if actual != string(sidecar) {
		cm.logger.Printf("Checksum mismatch for %s: stored=%s actual=%s", filePath, string(sidecar), actual)
		cm.nvmeVerify.forget(filePath)
		return false
	}
	cm.nvmeVerify.markVerified(filePath, id)
	return true
}

// noteNVMeVerified records a verification the whole-file path already did, so
// a subsequent ranged read of the same bytes does not hash the file again.
func (cm *DefaultCacheManager) noteNVMeVerified(filePath string) {
	opener, ok := cm.nvmeStorage.(nvmeOpener)
	if !ok {
		return
	}
	reader, err := opener.Open(filePath)
	if err != nil {
		return
	}
	defer reader.Close()
	cm.nvmeVerify.markVerified(filePath, nvmeFileID{size: reader.Size(), mtime: reader.ModTimeNanos()})
}

// readChunkWindow serves a window that falls inside one chunk straight from
// that chunk's local file, reading only the window's bytes. The per-chunk
// path reads the whole chunk into memory for every window that misses the
// range cache: a 128 KiB random read moved 16 MiB, which was 95% of the CPU
// in the read profile and held random reads near 110 MB/s from page cache.
// Sequential streams keep the per-chunk path, whose range cache and
// read-ahead serve the following windows; see isSequentialRead.
//
// Reports ok=false when the chunk is not held locally, or its file is larger
// than the stride (the chunk-size mismatch clampChunkSlice guards against),
// so the caller takes the per-chunk path.
func (cm *DefaultCacheManager) readChunkWindow(ctx context.Context, filePath string, chunkIndex, chunkSize, offset, end int64) ([]byte, bool) {
	opener, ok := cm.nvmeStorage.(nvmeOpener)
	if !ok || chunkSize <= 0 || end <= offset {
		return nil, false
	}
	chunkPath := fmt.Sprintf("%s_chunk_%d", filePath, chunkIndex)
	reader, err := opener.Open(chunkPath)
	if err != nil {
		return nil, false
	}
	defer reader.Close()
	// The chunk file must be exactly the chunk's length at this stride: a
	// longer or shorter one was written with another stride, and slicing it
	// would return wrong or short bytes as if they were the file's.
	if want := expectedChunkLen(chunkIndex, chunkSize, cm.entrySize(filePath)); want <= 0 || reader.Size() != want ||
		!cm.verifyNVMeReader(ctx, opener, chunkPath, reader) {
		return nil, false
	}
	from := offset - chunkIndex*chunkSize
	to := end - chunkIndex*chunkSize
	from, to = clampChunkSlice(from, to, reader.Size())
	buf := make([]byte, to-from)
	n, err := reader.ReadAt(buf, from)
	if err != nil && int64(n) != to-from {
		cm.logger.Printf("NVMe window read of %s at offset %d failed: %v", chunkPath, from, err)
		return nil, false
	}
	cm.touchEntries(filePath, chunkPath)
	return buf[:n], true
}

// isSequentialRead reports whether a window continues the file's current read
// stream, using the same test observeReadPattern applies. A read at offset 0
// counts as sequential: it is how a stream starts, and the per-chunk path is
// what starts its read-ahead.
func (cm *DefaultCacheManager) isSequentialRead(filePath string, offset, chunkSize int64) bool {
	if offset == 0 {
		return true
	}
	cm.rangeMu.RLock()
	defer cm.rangeMu.RUnlock()
	fileCache, ok := cm.rangeChunks[filePath]
	if !ok || fileCache == nil || fileCache.lastReadEnd <= 0 {
		return false
	}
	return offset >= fileCache.lastReadEnd-chunkSize && offset <= fileCache.lastReadEnd+chunkSize
}

// expectedChunkLen is the byte length of chunk idx of a file of size total at
// stride chunkSize (the last chunk may be short); 0 when out of range.
func expectedChunkLen(idx, chunkSize, total int64) int64 {
	if chunkSize <= 0 || total <= 0 || idx < 0 {
		return 0
	}
	start := idx * chunkSize
	if start >= total {
		return 0
	}
	return min(chunkSize, total-start)
}

// entrySize is the recorded size of filePath, or 0 when unknown.
func (cm *DefaultCacheManager) entrySize(filePath string) int64 {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	if e, ok := cm.entries.get(filePath); ok && e != nil {
		return e.Size
	}
	return 0
}

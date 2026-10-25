package cache

import (
	"context"
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

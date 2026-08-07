package cache

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"
)

// Rename as a metadata operation.
//
// The FUSE layer used to implement rename as Get + Put + Delete: read the
// whole object into memory, write it back out under the new name, then delete
// the original. For a chunked file that is the worst possible shape — Get
// reassembles every chunk into one buffer (a 50GB file needs 50GB of RAM), Put
// re-splits and re-writes all of it to NVMe, and the background write-through
// re-uploads every chunk to the cloud. Renaming a checkpoint directory after a
// training step, which is the common case this filesystem sees, moved terabytes
// to change some strings.
//
// Nothing about the bytes changes in a rename, so nothing about the bytes needs
// to move. Locally the object is an os.Rename within one volume; in the cloud
// it is a server-side copy (the object never transits this node); in the entry
// table it is a re-key of the parent and each of its chunk entries.

// ErrNotFound reports that the cache holds no metadata for a path. Callers
// that must map cache failures onto errnos (the FUSE layer maps it to ENOENT)
// need to tell "no such file" apart from "the move failed".
var ErrNotFound = errors.New("file not found in cache")

// nvmeMover is the local-move capability of NVMeStorage. Tests substitute a
// mock tier that does not implement it, and those callers fall back to
// read-write-delete — correct, just not cheap.
type nvmeMover interface {
	Move(ctx context.Context, path, newPath string) error
}

// cloudObjectCopier is the server-side copy capability of a cloud tier: Azure
// via StartCopyFromURL, S3 and GCS via CopyObject. A backend without it (or a
// copy that fails) falls back to re-uploading the local bytes, which is what
// the ordinary write-through would have done anyway.
type cloudObjectCopier interface {
	CopyObject(ctx context.Context, srcPath, dstPath string) error
}

// renamedEntry is one key being moved, captured before anything is touched.
type renamedEntry struct {
	oldKey string
	newKey string
	entry  CacheEntry
}

// Rename moves oldPath to newPath without reading or rewriting file data.
//
// Coordination is advisory here, as everywhere else: the coordinator learns
// about the destination, but the source's location rows are left to age out —
// there is no delete in the Coordinator interface, and a stale row costs one
// miss on a peer that no longer holds the object, which the fetch path already
// treats as stale routing rather than failure.
func (cm *DefaultCacheManager) Rename(ctx context.Context, oldPath, newPath string) error {
	oldPath = strings.TrimSpace(oldPath)
	newPath = strings.TrimSpace(newPath)
	if oldPath == "" || newPath == "" {
		return errors.New("empty file path")
	}
	if oldPath == newPath {
		return nil
	}

	moves, parent, ok := cm.planRename(oldPath, newPath)
	if !ok {
		return fmt.Errorf("rename %s: %w", oldPath, ErrNotFound)
	}

	// POSIX rename replaces the destination. Clear whatever is there before
	// the source lands on top of it, so the old occupant's bytes and byte
	// accounting go away rather than being orphaned on disk.
	if _, err := cm.deleteLocalNVMeFootprint(ctx, newPath, true, true); err != nil {
		return err
	}
	cm.dropEntries(newPath)
	cm.invalidateRangeCache(oldPath)
	cm.invalidateRangeCache(newPath)

	// Local bytes first: if this fails the metadata still describes the old
	// path, which is a rename that did not happen rather than one that half
	// happened.
	for i := range moves {
		if moves[i].entry.Tier != TierNVMe {
			continue
		}
		if err := cm.moveLocalObject(ctx, moves[i].oldKey, moves[i].newKey); err != nil {
			return fmt.Errorf("rename %s -> %s: %w", moves[i].oldKey, moves[i].newKey, err)
		}
	}

	cm.mu.Lock()
	for i := range moves {
		moved := moves[i].entry
		moved.FilePath = moves[i].newKey
		moved.StoragePath = moves[i].newKey
		moved.LastAccessed = time.Now()
		cm.entries.del(moves[i].oldKey)
		cm.entries.put(&moved)
	}
	cm.mu.Unlock()

	renamedParent := parent
	renamedParent.FilePath = newPath
	renamedParent.StoragePath = newPath
	cm.goBackground(func() { cm.publishFileLocation(context.Background(), &renamedParent, renamedParent.Tier) })
	cm.goBackground(func() { cm.moveCloudObjects(cm.shutdownCtx, oldPath, newPath, parent) })

	cm.logger.Printf("Renamed %s -> %s (%d local keys, chunked=%t)", oldPath, newPath, len(moves), parent.IsChunked)
	return nil
}

// planRename captures every table key belonging to oldPath and the destination
// key it moves to. The parent is returned separately because the cloud and
// coordinator work is described by the parent's metadata, not by whichever
// chunk entries happen to be resident.
func (cm *DefaultCacheManager) planRename(oldPath, newPath string) ([]renamedEntry, CacheEntry, bool) {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	entry, exists := cm.entries.get(oldPath)
	if !exists || entry == nil {
		return nil, CacheEntry{}, false
	}
	moves := []renamedEntry{{oldKey: oldPath, newKey: newPath, entry: *entry}}

	// Chunk keys come off the index rather than from NumChunks: a partially
	// fetched file has chunk entries the parent does not count, and leaving
	// one behind would file it under a parent that no longer exists.
	for _, chunkKey := range cm.entries.chunkKeys(oldPath) {
		chunkEntry, ok := cm.entries.get(chunkKey)
		if !ok || chunkEntry == nil {
			continue
		}
		moves = append(moves, renamedEntry{
			oldKey: chunkKey,
			newKey: newPath + chunkKey[len(oldPath):],
			entry:  *chunkEntry,
		})
	}
	return moves, *entry, true
}

// dropEntries removes path and its chunk keys from the table without touching
// storage. Used to clear a rename destination whose bytes are already gone.
func (cm *DefaultCacheManager) dropEntries(path string) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	cm.removeChunkEntriesLocked(path)
	cm.entries.del(path)
}

// moveLocalObject relocates one NVMe object and its checksum sidecar. A
// missing source is not an error: the table can hold an entry whose bytes were
// evicted, and the rename of the metadata is still the right outcome.
func (cm *DefaultCacheManager) moveLocalObject(ctx context.Context, oldKey, newKey string) error {
	cm.nvmeVerify.forget(oldKey)
	cm.nvmeVerify.forget(newKey)

	if err := cm.moveNVMePath(ctx, oldKey, newKey); err != nil {
		if isDeleteNotFoundError(err) {
			return nil
		}
		return err
	}
	// Best-effort: an object written before checksums, or one whose sidecar is
	// already gone, is not a failed rename.
	_ = cm.moveNVMePath(ctx, oldKey+".sha256", newKey+".sha256")
	return nil
}

// moveNVMePath renames within the NVMe volume when the tier supports it, and
// otherwise copies the bytes through this process. The copy exists only for
// TierStorage implementations without Move (test doubles); NVMeStorage always
// takes the rename.
func (cm *DefaultCacheManager) moveNVMePath(ctx context.Context, oldKey, newKey string) error {
	if mover, ok := cm.nvmeStorage.(nvmeMover); ok {
		return mover.Move(ctx, oldKey, newKey)
	}
	data, err := cm.nvmeStorage.Read(ctx, oldKey)
	if err != nil {
		return err
	}
	if err := cm.nvmeStorage.Write(ctx, newKey, data); err != nil {
		return err
	}
	return cm.nvmeStorage.Delete(ctx, oldKey)
}

// moveCloudObjects re-points the durable copy at the new name. Every key is
// copied server-side; the originals are deleted only once all of them landed,
// because a rename must never be the reason the only durable copy disappears.
func (cm *DefaultCacheManager) moveCloudObjects(ctx context.Context, oldPath, newPath string, parent CacheEntry) {
	if cm.cloudStorage == nil {
		return
	}
	oldKeys := cloudObjectKeys(oldPath, &parent)
	newKeys := cloudObjectKeys(newPath, &parent)

	copier, canCopy := cm.cloudStorage.(cloudObjectCopier)
	if !canCopy {
		cm.repersistRenamed(ctx, newPath, parent)
		return
	}

	for i := range oldKeys {
		select {
		case <-ctx.Done():
			return
		default:
		}
		if err := copier.CopyObject(ctx, oldKeys[i], newKeys[i]); err != nil {
			if isCloudMiss(err) {
				// The source was never persisted (or was already moved). The
				// local bytes are the authority; upload them under the new
				// name and leave the source keys alone.
				cm.logger.Printf("Cloud rename source %s absent; re-persisting %s from local copy", oldKeys[i], newPath)
			} else {
				cm.logger.Printf("Cloud copy %s -> %s failed: %v (re-persisting from local copy)", oldKeys[i], newKeys[i], err)
			}
			cm.repersistRenamed(ctx, newPath, parent)
			return
		}
	}

	// Chunks first: a chunked parent only flips once every one of its chunks
	// has, so marking the parent before them would be a no-op.
	for _, key := range newKeys {
		cm.markPersistedToCloud(key)
	}
	cm.markPersistedToCloud(newPath)

	// Only now, with every destination object confirmed by the service, are
	// the originals garbage.
	for _, key := range oldKeys {
		if err := cm.cloudStorage.Delete(ctx, key); err != nil && !isCloudMiss(err) && !isDeleteNotFoundError(err) {
			cm.logger.Printf("Cloud rename left %s behind: %v", key, err)
		}
	}
}

// repersistRenamed uploads the renamed object from the local copy. This is the
// fallback when server-side copy is unavailable or fails; it deliberately does
// not delete the source objects, since nothing here confirms the upload and an
// unconfirmed upload plus a deleted original is data loss.
func (cm *DefaultCacheManager) repersistRenamed(ctx context.Context, newPath string, parent CacheEntry) {
	if !parent.IsChunked || parent.NumChunks <= 0 {
		data, err := cm.nvmeStorage.Read(ctx, newPath)
		if err != nil {
			cm.logger.Printf("Cloud re-persist of renamed %s skipped: %v", newPath, err)
			return
		}
		cm.persistToCloud(ctx, newPath, data)
		return
	}
	// Chunked files exist locally in one of two shapes: as individual chunk
	// files, or as one whole file that chunk reads are served out of
	// (PutFromFile). Persisting the wrong one uploads nothing.
	if cm.nvmeStorage.Exists(ctx, chunkPathFor(newPath, 0)) {
		cm.persistChunkedFileToCloud(ctx, newPath, parent.NumChunks)
		return
	}
	if localPath, ok := cm.LocalFilePath(ctx, newPath); ok {
		cm.persistWholeFileAsChunksToCloud(ctx, newPath, localPath, parent.NumChunks)
		return
	}
	cm.logger.Printf("Cloud re-persist of renamed %s skipped: no local copy", newPath)
}

// cloudObjectKeys lists the cloud objects that hold path's data: one per chunk
// for a chunked file, otherwise the object itself.
func cloudObjectKeys(path string, entry *CacheEntry) []string {
	if entry != nil && entry.IsChunked && entry.NumChunks > 0 {
		keys := make([]string, 0, entry.NumChunks)
		for i := int64(0); i < entry.NumChunks; i++ {
			keys = append(keys, chunkPathFor(path, i))
		}
		return keys
	}
	return []string{path}
}

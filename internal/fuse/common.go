package fuse

import (
	"context"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"fuse-client/internal/cache"
)

const (
	// Tuned for throughput on NVMe-backed AKS nodes while keeping cache staleness bounded.
	attrCacheTTL      = 5 * time.Second
	maxReadaheadBytes = 16 * 1024 * 1024 // 16 MiB
	maxBackgroundReqs = 512
	defaultBlockSize  = 4 * 1024
	defaultIOLogStep  = 512 * 1024 * 1024 // 512 MiB
)

var (
	// ioProgressStepBytes controls periodic read/write progress logging volume.
	// Set FUSE_IO_PROGRESS_MB=0 to disable progress logs.
	ioProgressStepBytes int64 = defaultIOLogStep
)

func init() {
	v := strings.TrimSpace(os.Getenv("FUSE_IO_PROGRESS_MB"))
	if v == "" {
		return
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		return
	}
	if n <= 0 {
		ioProgressStepBytes = 0
		return
	}
	ioProgressStepBytes = n * 1024 * 1024
}

func crossedProgressBoundary(prevSize, newSize int64) bool {
	return ioProgressStepBytes > 0 && (prevSize/ioProgressStepBytes) != (newSize/ioProgressStepBytes)
}

// pathToInode generates a deterministic inode number from a path using FNV-64a
func pathToInode(path string) uint64 {
	h := fnv.New64a()
	h.Write([]byte(path))
	ino := h.Sum64()
	if ino == 0 {
		ino = 1 // inode 0 is invalid
	}
	return ino
}

type rangeReader interface {
	ReadRange(ctx context.Context, filePath string, offset int64, size int) ([]byte, error)
}

type freshLister interface {
	ListFresh(ctx context.Context) ([]*cache.CacheEntry, error)
}

// metadataLookup is the point-lookup metadata API. FUSE resolves one path per
// syscall, so answering from an index beats copying and scanning the whole
// entry set (one entry per chunk) on every lookup, readdir, and getattr.
// Optional: a cache manager without it falls back to the List-based paths.
type metadataLookup interface {
	LookupEntry(ctx context.Context, filePath string, forceRefresh bool) (*cache.CacheEntry, bool)
	DirExists(ctx context.Context, path string) bool
	ReadDir(ctx context.Context, dir string) ([]cache.DirEntry, error)
}

type streamingPutter interface {
	PutFromReader(ctx context.Context, filePath string, r io.Reader, size int64, lastAccessed time.Time) error
}

type stagedWriteFileCreator interface {
	CreateWriteFile(ctx context.Context, filePath string) (*os.File, string, error)
}

type stagedFilePutter interface {
	PutFromFile(ctx context.Context, filePath string, stagedPath string, size int64, lastAccessed time.Time) error
}

// entryRenamer moves a file by re-keying its metadata and renaming its local
// bytes. Optional: without it rename falls back to read-write-delete, which
// for a chunked file means buffering the whole object in RAM and re-uploading
// every chunk.
type entryRenamer interface {
	Rename(ctx context.Context, oldPath, newPath string) error
}

// renameEntry moves oldPath to newPath, preferring the cache manager's
// metadata move and falling back to copy-then-delete.
//
// A cache.ErrNotFound return means the source does not exist (ENOENT to the
// caller); anything else is a move that failed (EIO).
func renameEntry(ctx context.Context, cm cache.CacheManager, logger *log.Logger, oldPath, newPath string) error {
	if renamer, ok := cm.(entryRenamer); ok {
		return renamer.Rename(ctx, oldPath, newPath)
	}

	entry, err := cm.Get(ctx, oldPath)
	if err != nil {
		return fmt.Errorf("%w: %v", cache.ErrNotFound, err)
	}
	newEntry := &cache.CacheEntry{
		FilePath:     newPath,
		StoragePath:  newPath,
		Size:         entry.Size,
		LastAccessed: time.Now(),
		Data:         entry.Data,
	}
	if err := cm.Put(ctx, newEntry); err != nil {
		return err
	}
	if err := cm.Delete(ctx, oldPath); err != nil {
		logger.Printf("Rename: failed to delete old path %s: %v", oldPath, err)
	}
	return nil
}

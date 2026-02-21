package fuse

import (
	"context"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"fuse-client/internal/cache"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
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

func shouldLogProgressAtOffset(offset int64) bool {
	return ioProgressStepBytes > 0 && offset >= 0 && offset%ioProgressStepBytes == 0
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

// FileSystem represents our FUSE filesystem
type FileSystem struct {
	cacheManager cache.CacheManager
	logger       *log.Logger
}

type rangeReader interface {
	ReadRange(ctx context.Context, filePath string, offset int64, size int) ([]byte, error)
}

type freshLister interface {
	ListFresh(ctx context.Context) ([]*cache.CacheEntry, error)
}

type streamingPutter interface {
	PutFromReader(ctx context.Context, filePath string, r io.Reader, size int64, lastAccessed time.Time) error
}

// NewFileSystem creates a new FUSE filesystem
func NewFileSystem(cacheManager cache.CacheManager) *FileSystem {
	return &FileSystem{
		cacheManager: cacheManager,
		logger:       log.New(log.Writer(), "[FUSE] ", log.LstdFlags),
	}
}

// Mount mounts the FUSE filesystem at the given mount point
func (fs *FileSystem) Mount(ctx context.Context, mountPoint string) (*fuse.Conn, error) {
	// Create mount point if it doesn't exist
	if err := os.MkdirAll(mountPoint, 0755); err != nil {
		return nil, err
	}

	// Mount the filesystem
	conn, err := fuse.Mount(
		mountPoint,
		fuse.FSName("fuse-client"),
		fuse.Subtype("fuse-client"),
		fuse.AsyncRead(),
		fuse.MaxReadahead(maxReadaheadBytes),
		fuse.MaxBackground(maxBackgroundReqs),
		fuse.CongestionThreshold(maxBackgroundReqs*3/4),
	)
	if err != nil {
		return nil, err
	}
	if err := waitForFuseMount(ctx, mountPoint, 5*time.Second); err != nil {
		_ = conn.Close()
		_ = fuse.Unmount(mountPoint)
		return nil, err
	}

	fs.logger.Printf("Mounted FUSE filesystem at: %s", mountPoint)
	return conn, nil
}

// Serve serves the FUSE filesystem
func (filesystem *FileSystem) Serve(ctx context.Context, conn *fuse.Conn) error {
	defer conn.Close()

	filesystem.logger.Printf("FUSE filesystem is ready")

	// Use the fs.Serve function from bazil.org/fuse/fs
	return fs.Serve(conn, filesystem)
}

// Root returns the root directory of the filesystem
func (fs *FileSystem) Root() (fs.Node, error) {
	return &Dir{
		fs:   fs,
		path: "/",
	}, nil
}

// Dir represents a directory in the FUSE filesystem
type Dir struct {
	fs   *FileSystem
	path string
}

// Attr returns the attributes of the directory
func (d *Dir) Attr(ctx context.Context, a *fuse.Attr) error {
	a.Inode = pathToInode(d.path)
	a.Mode = os.ModeDir | 0755
	a.Nlink = 2
	a.Size = 0
	a.Mtime = time.Now()
	a.Atime = time.Now()
	a.Ctime = time.Now()
	a.Valid = attrCacheTTL
	a.BlockSize = defaultBlockSize
	return nil
}

// Lookup looks up a file or directory
func (d *Dir) Lookup(ctx context.Context, name string) (fs.Node, error) {
	fullPath := filepath.Join(d.path, name)

	entries, err := d.fs.cacheManager.List(ctx)
	if err != nil {
		return nil, fuse.ENOENT
	}
	if node, ok := d.lookupNodeFromEntries(fullPath, entries); ok {
		return node, nil
	}

	// On metadata miss, force a fresh coordinator snapshot before returning ENOENT.
	if lister, ok := d.fs.cacheManager.(freshLister); ok {
		freshEntries, err := lister.ListFresh(ctx)
		if err == nil {
			if node, ok := d.lookupNodeFromEntries(fullPath, freshEntries); ok {
				return node, nil
			}
		}
	}

	return nil, fuse.ENOENT
}

func (d *Dir) lookupNodeFromEntries(fullPath string, entries []*cache.CacheEntry) (fs.Node, bool) {
	// Metadata-first lookup: find file entry without triggering data fetch.
	for _, entry := range entries {
		if entry.FilePath != fullPath {
			continue
		}
		entryCopy := *entry
		return &File{
			fs:    d.fs,
			path:  fullPath,
			entry: &entryCopy,
			isNew: false,
		}, true
	}

	// Check if it's a directory by looking for files with this prefix.
	for _, entry := range entries {
		if strings.HasPrefix(entry.FilePath, fullPath+"/") {
			return &Dir{
				fs:   d.fs,
				path: fullPath,
			}, true
		}
	}

	return nil, false
}

// ReadDirAll reads all entries in the directory
func (d *Dir) ReadDirAll(ctx context.Context) ([]fuse.Dirent, error) {
	entries, err := d.fs.cacheManager.List(ctx)
	if err != nil {
		return nil, err
	}

	var dirents []fuse.Dirent
	seen := make(map[string]bool)

	for _, entry := range entries {
		// Only include files/dirs in this directory
		if !strings.HasPrefix(entry.FilePath, d.path) {
			continue
		}

		relPath := strings.TrimPrefix(entry.FilePath, d.path)
		relPath = strings.TrimPrefix(relPath, "/")

		if relPath == "" {
			continue
		}

		// Check if it's a direct child
		parts := strings.Split(relPath, "/")
		if len(parts) > 1 {
			// It's in a subdirectory
			dirName := parts[0]
			if !seen[dirName] {
				dirents = append(dirents, fuse.Dirent{
					Name: dirName,
					Type: fuse.DT_Dir,
				})
				seen[dirName] = true
			}
		} else {
			// It's a direct file
			dirents = append(dirents, fuse.Dirent{
				Name: relPath,
				Type: fuse.DT_File,
			})
		}
	}

	return dirents, nil
}

// Create creates a new file
func (d *Dir) Create(ctx context.Context, req *fuse.CreateRequest, resp *fuse.CreateResponse) (fs.Node, fs.Handle, error) {
	fullPath := filepath.Join(d.path, req.Name)

	// Safe write-to-new semantics: refuse overwrite when metadata says object exists.
	if lister, ok := d.fs.cacheManager.(freshLister); ok {
		entries, err := lister.ListFresh(ctx)
		if err != nil {
			// Coordinator outages should not make local writes fail. Fall back to
			// local metadata for overwrite detection and continue if unavailable.
			d.fs.logger.Printf("Create metadata refresh failed for %s (fallback to local metadata): %v", fullPath, err)
			if localEntries, listErr := d.fs.cacheManager.List(ctx); listErr == nil {
				entries = localEntries
			} else {
				d.fs.logger.Printf("Create local metadata fallback failed for %s: %v", fullPath, listErr)
				entries = nil
			}
		}
		for _, entry := range entries {
			if entry.FilePath == fullPath {
				return nil, nil, fuse.EEXIST
			}
		}
	}

	entry := &cache.CacheEntry{
		FilePath:     fullPath,
		Size:         0,
		LastAccessed: time.Now(),
		Data:         []byte{},
	}

	file := &File{
		fs:    d.fs,
		path:  fullPath,
		entry: entry,
		isNew: true,
	}

	return file, file, nil
}

// Mkdir creates a new directory
func (d *Dir) Mkdir(ctx context.Context, req *fuse.MkdirRequest) (fs.Node, error) {
	fullPath := filepath.Join(d.path, req.Name)

	d.fs.logger.Printf("Created directory: %s", fullPath)

	return &Dir{
		fs:   d.fs,
		path: fullPath,
	}, nil
}

// Rename moves a file from this directory to a new name/directory
func (d *Dir) Rename(ctx context.Context, req *fuse.RenameRequest, newDir fs.Node) error {
	oldPath := filepath.Join(d.path, req.OldName)
	newDirNode, ok := newDir.(*Dir)
	if !ok {
		return fuse.EIO
	}
	newPath := filepath.Join(newDirNode.path, req.NewName)

	// Get the old entry
	entry, err := d.fs.cacheManager.Get(ctx, oldPath)
	if err != nil {
		return fuse.ENOENT
	}

	// Put with new path
	newEntry := &cache.CacheEntry{
		FilePath:     newPath,
		StoragePath:  newPath,
		Size:         entry.Size,
		LastAccessed: time.Now(),
		Data:         entry.Data,
	}
	if err := d.fs.cacheManager.Put(ctx, newEntry); err != nil {
		return fuse.EIO
	}

	// Delete old entry
	if err := d.fs.cacheManager.Delete(ctx, oldPath); err != nil {
		d.fs.logger.Printf("Rename: failed to delete old path %s: %v", oldPath, err)
	}

	d.fs.logger.Printf("Renamed %s -> %s", oldPath, newPath)
	return nil
}

// Remove removes a file or directory
func (d *Dir) Remove(ctx context.Context, req *fuse.RemoveRequest) error {
	fullPath := filepath.Join(d.path, req.Name)

	if err := d.fs.cacheManager.Delete(ctx, fullPath); err != nil {
		return fuse.EIO
	}

	d.fs.logger.Printf("Removed: %s", fullPath)
	return nil
}

// File represents a file in the FUSE filesystem
type File struct {
	fs    *FileSystem
	path  string
	entry *cache.CacheEntry
	mu    sync.RWMutex
	dirty bool
	isNew bool

	writeFile *os.File
	writePath string
}

func (f *File) ensureWriteFileLocked() error {
	if f.writeFile != nil {
		return nil
	}
	tmp, err := os.CreateTemp("", "fuse-client-write-*")
	if err != nil {
		return err
	}
	f.writeFile = tmp
	f.writePath = tmp.Name()
	return nil
}

func (f *File) cleanupWriteFileLocked() {
	if f.writeFile != nil {
		_ = f.writeFile.Close()
		f.writeFile = nil
	}
	if f.writePath != "" {
		_ = os.Remove(f.writePath)
		f.writePath = ""
	}
}

func growBuffer(data []byte, needed int) []byte {
	if needed <= len(data) {
		return data
	}
	if needed <= cap(data) {
		return data[:needed]
	}

	newCap := cap(data)
	if newCap == 0 {
		newCap = 128 * 1024
	}
	for newCap < needed {
		if newCap < 64*1024*1024 {
			newCap *= 2
		} else {
			newCap += 64 * 1024 * 1024
		}
	}

	grown := make([]byte, needed, newCap)
	copy(grown, data)
	return grown
}

// Attr returns the attributes of the file
func (f *File) Attr(ctx context.Context, a *fuse.Attr) error {
	f.mu.RLock()
	entry := f.entry
	f.mu.RUnlock()

	if entry == nil {
		entries, err := f.fs.cacheManager.List(ctx)
		if err == nil {
			for _, listed := range entries {
				if listed.FilePath == f.path {
					entry = listed
					break
				}
			}
		}
	}
	if entry == nil {
		return fuse.ENOENT
	}

	a.Inode = pathToInode(f.path)
	a.Mode = 0644
	a.Nlink = 1
	a.Size = uint64(entry.Size)
	a.Mtime = entry.LastAccessed
	a.Atime = entry.LastAccessed
	a.Ctime = entry.LastAccessed
	a.Valid = attrCacheTTL
	a.BlockSize = defaultBlockSize
	return nil
}

// Read reads data from the file
func (f *File) Read(ctx context.Context, req *fuse.ReadRequest, resp *fuse.ReadResponse) error {
	// Serve dirty writes for write-to-new handles directly from the spool file.
	f.mu.RLock()
	if f.isNew && f.dirty && f.writeFile != nil {
		size := f.entry.Size
		if req.Offset >= size {
			f.mu.RUnlock()
			return nil
		}
		end := req.Offset + int64(req.Size)
		if end > size {
			end = size
		}
		buf := make([]byte, end-req.Offset)
		n, err := f.writeFile.ReadAt(buf, req.Offset)
		f.mu.RUnlock()
		if err != nil && err != io.EOF {
			return fuse.EIO
		}
		resp.Data = buf[:n]
		if len(resp.Data) > 0 && shouldLogProgressAtOffset(req.Offset) {
			f.fs.logger.Printf("Read %d bytes from staged write %s at offset %d", len(resp.Data), f.path, req.Offset)
		}
		return nil
	}
	f.mu.RUnlock()

	// Prefer range reads to avoid full-file materialization for chunked objects.
	if rr, ok := f.fs.cacheManager.(rangeReader); ok {
		data, err := rr.ReadRange(ctx, f.path, req.Offset, req.Size)
		if err != nil {
			return fuse.EIO
		}
		resp.Data = data
		if len(data) > 0 && shouldLogProgressAtOffset(req.Offset) {
			f.fs.logger.Printf("Read %d bytes from %s at offset %d", len(data), f.path, req.Offset)
		}
		return nil
	}

	// Fallback path.
	entry, err := f.fs.cacheManager.Get(ctx, f.path)
	if err != nil {
		return fuse.EIO
	}
	if req.Offset >= int64(len(entry.Data)) {
		return nil
	}
	end := req.Offset + int64(req.Size)
	if end > int64(len(entry.Data)) {
		end = int64(len(entry.Data))
	}
	resp.Data = entry.Data[req.Offset:end]
	if len(resp.Data) > 0 && shouldLogProgressAtOffset(req.Offset) {
		f.fs.logger.Printf("Read %d bytes from %s at offset %d", len(resp.Data), f.path, req.Offset)
	}
	return nil
}

// Write writes data to the file
func (f *File) Write(ctx context.Context, req *fuse.WriteRequest, resp *fuse.WriteResponse) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if !f.isNew {
		return fuse.EPERM
	}

	if err := f.ensureWriteFileLocked(); err != nil {
		return fuse.EIO
	}
	if _, err := f.writeFile.WriteAt(req.Data, req.Offset); err != nil {
		return fuse.EIO
	}

	prevSize := f.entry.Size
	needed := req.Offset + int64(len(req.Data))
	if needed > f.entry.Size {
		f.entry.Size = needed
	}
	f.entry.LastAccessed = time.Now()
	f.dirty = true

	resp.Size = len(req.Data)
	if crossedProgressBoundary(prevSize, f.entry.Size) {
		f.fs.logger.Printf("Write progress %s: %d bytes", f.path, f.entry.Size)
	}
	return nil
}

// Flush flushes the file
func (f *File) Flush(ctx context.Context, req *fuse.FlushRequest) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.dirty {
		if f.writeFile != nil {
			reader := io.NewSectionReader(f.writeFile, 0, f.entry.Size)
			if putter, ok := f.fs.cacheManager.(streamingPutter); ok {
				if err := putter.PutFromReader(ctx, f.path, reader, f.entry.Size, time.Now()); err != nil {
					return fuse.EIO
				}
			} else {
				// Fallback path for non-default cache managers.
				data := make([]byte, int(f.entry.Size))
				if _, err := io.ReadFull(reader, data); err != nil && err != io.EOF {
					return fuse.EIO
				}
				f.entry.Data = data
				if err := f.fs.cacheManager.Put(ctx, f.entry); err != nil {
					return fuse.EIO
				}
			}
			f.entry.Data = nil
			f.cleanupWriteFileLocked()
		} else if err := f.fs.cacheManager.Put(ctx, f.entry); err != nil {
			return fuse.EIO
		}
		f.dirty = false
	}

	f.fs.logger.Printf("Flushed file: %s", f.path)
	return nil
}

// Fsync syncs the file
func (f *File) Fsync(ctx context.Context, req *fuse.FsyncRequest) error {
	return f.Flush(ctx, &fuse.FlushRequest{})
}

// Setattr sets file attributes
func (f *File) Setattr(ctx context.Context, req *fuse.SetattrRequest, resp *fuse.SetattrResponse) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if req.Valid.Size() {
		if !f.isNew {
			return fuse.EPERM
		}
		if err := f.ensureWriteFileLocked(); err != nil {
			return fuse.EIO
		}
		if err := f.writeFile.Truncate(int64(req.Size)); err != nil {
			return fuse.EIO
		}
		f.entry.Size = int64(req.Size)
		f.entry.LastAccessed = time.Now()
		f.dirty = true
	}

	return f.Attr(ctx, &resp.Attr)
}

// Open opens the file
func (f *File) Open(ctx context.Context, req *fuse.OpenRequest, resp *fuse.OpenResponse) (fs.Handle, error) {
	// Keep open metadata-only. Actual download happens in Read on first access.
	resp.Flags |= fuse.OpenKeepCache
	return f, nil
}

// Release closes any transient write spool associated with this handle.
func (f *File) Release(ctx context.Context, req *fuse.ReleaseRequest) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.cleanupWriteFileLocked()
	return nil
}

// Unmount unmounts the FUSE filesystem
func (fs *FileSystem) Unmount(mountPoint string) error {
	fs.logger.Printf("Unmounting FUSE filesystem from: %s", mountPoint)
	return fuse.Unmount(mountPoint)
}

func waitForFuseMount(ctx context.Context, mountPoint string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		ok, err := isFuseMountActive(mountPoint)
		if err != nil {
			return err
		}
		if ok {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("mountpoint %s did not become an active fuse mount within %v", mountPoint, timeout)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(150 * time.Millisecond):
		}
	}
}

func isFuseMountActive(mountPoint string) (bool, error) {
	data, err := os.ReadFile("/proc/self/mountinfo")
	if err != nil {
		return false, err
	}
	lines := strings.Split(string(data), "\n")
	for _, line := range lines {
		if line == "" {
			continue
		}
		parts := strings.Split(line, " - ")
		if len(parts) != 2 {
			continue
		}
		left := strings.Fields(parts[0])
		right := strings.Fields(parts[1])
		if len(left) < 5 || len(right) < 1 {
			continue
		}
		if left[4] != mountPoint {
			continue
		}
		return strings.HasPrefix(right[0], "fuse"), nil
	}
	return false, nil
}

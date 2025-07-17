package fuse

import (
	"context"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"fuse-client/internal/cache"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
)

// FileSystem represents our FUSE filesystem
type FileSystem struct {
	cacheManager cache.CacheManager
	logger       *log.Logger
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
	)
	if err != nil {
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
	a.Inode = 1
	a.Mode = os.ModeDir | 0755
	a.Nlink = 2
	a.Size = 0
	a.Mtime = time.Now()
	a.Atime = time.Now()
	a.Ctime = time.Now()
	return nil
}

// Lookup looks up a file or directory
func (d *Dir) Lookup(ctx context.Context, name string) (fs.Node, error) {
	fullPath := filepath.Join(d.path, name)

	// Check if it's a file in the cache
	if entry, err := d.fs.cacheManager.Get(ctx, fullPath); err == nil {
		return &File{
			fs:    d.fs,
			path:  fullPath,
			entry: entry,
		}, nil
	}

	// Check if it's a directory by looking for files with this prefix
	entries, err := d.fs.cacheManager.List(ctx)
	if err != nil {
		return nil, fuse.ENOENT
	}

	for _, entry := range entries {
		if strings.HasPrefix(entry.FilePath, fullPath+"/") {
			return &Dir{
				fs:   d.fs,
				path: fullPath,
			}, nil
		}
	}

	return nil, fuse.ENOENT
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
}

// Attr returns the attributes of the file
func (f *File) Attr(ctx context.Context, a *fuse.Attr) error {
	f.mu.RLock()
	defer f.mu.RUnlock()

	a.Inode = 2
	a.Mode = 0644
	a.Nlink = 1
	a.Size = uint64(f.entry.Size)
	a.Mtime = f.entry.LastAccessed
	a.Atime = f.entry.LastAccessed
	a.Ctime = f.entry.LastAccessed
	return nil
}

// Read reads data from the file
func (f *File) Read(ctx context.Context, req *fuse.ReadRequest, resp *fuse.ReadResponse) error {
	f.mu.RLock()
	defer f.mu.RUnlock()

	// Get fresh data from cache
	entry, err := f.fs.cacheManager.Get(ctx, f.path)
	if err != nil {
		return fuse.EIO
	}

	// Handle offset and size
	if req.Offset >= int64(len(entry.Data)) {
		return nil
	}

	end := req.Offset + int64(req.Size)
	if end > int64(len(entry.Data)) {
		end = int64(len(entry.Data))
	}

	resp.Data = entry.Data[req.Offset:end]
	f.fs.logger.Printf("Read %d bytes from %s at offset %d", len(resp.Data), f.path, req.Offset)
	return nil
}

// Write writes data to the file
func (f *File) Write(ctx context.Context, req *fuse.WriteRequest, resp *fuse.WriteResponse) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	// Resize data if necessary
	needed := int(req.Offset) + len(req.Data)
	if needed > len(f.entry.Data) {
		newData := make([]byte, needed)
		copy(newData, f.entry.Data)
		f.entry.Data = newData
	}

	// Write the data
	copy(f.entry.Data[req.Offset:], req.Data)
	f.entry.Size = int64(len(f.entry.Data))
	f.entry.LastAccessed = time.Now()

	// Store in cache
	if err := f.fs.cacheManager.Put(ctx, f.entry); err != nil {
		return fuse.EIO
	}

	resp.Size = len(req.Data)
	f.fs.logger.Printf("Wrote %d bytes to %s at offset %d", len(req.Data), f.path, req.Offset)
	return nil
}

// Flush flushes the file
func (f *File) Flush(ctx context.Context, req *fuse.FlushRequest) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	// Ensure data is stored in cache
	if err := f.fs.cacheManager.Put(ctx, f.entry); err != nil {
		return fuse.EIO
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
		// Truncate file
		if req.Size < uint64(len(f.entry.Data)) {
			f.entry.Data = f.entry.Data[:req.Size]
		} else if req.Size > uint64(len(f.entry.Data)) {
			newData := make([]byte, req.Size)
			copy(newData, f.entry.Data)
			f.entry.Data = newData
		}
		f.entry.Size = int64(req.Size)
		f.entry.LastAccessed = time.Now()

		// Store in cache
		if err := f.fs.cacheManager.Put(ctx, f.entry); err != nil {
			return fuse.EIO
		}
	}

	return f.Attr(ctx, &resp.Attr)
}

// Open opens the file
func (f *File) Open(ctx context.Context, req *fuse.OpenRequest, resp *fuse.OpenResponse) (fs.Handle, error) {
	// Check if file exists in cache
	entry, err := f.fs.cacheManager.Get(ctx, f.path)
	if err != nil {
		return nil, fuse.ENOENT
	}

	f.entry = entry
	return f, nil
}

// Unmount unmounts the FUSE filesystem
func (fs *FileSystem) Unmount(mountPoint string) error {
	fs.logger.Printf("Unmounting FUSE filesystem from: %s", mountPoint)
	return fuse.Unmount(mountPoint)
}

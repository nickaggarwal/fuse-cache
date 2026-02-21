package fuse

import (
	"context"
	"io"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"fuse-client/internal/cache"

	gfs "github.com/hanwen/go-fuse/v2/fs"
	gfuse "github.com/hanwen/go-fuse/v2/fuse"
)

const (
	goFuseDirMode  = uint32(syscall.S_IFDIR | 0o755)
	goFuseFileMode = uint32(syscall.S_IFREG | 0o644)
)

type goFuseNodeKind int

const (
	goFuseNodeDir goFuseNodeKind = iota
	goFuseNodeFile
)

// GoFuseFileSystem is an alternate fs backend using go-fuse v2.
type GoFuseFileSystem struct {
	cacheManager cache.CacheManager
	logger       *log.Logger

	enablePassthrough bool

	mu         sync.Mutex
	server     *gfuse.Server
	mountPoint string
}

func NewGoFuseFileSystem(cacheManager cache.CacheManager) *GoFuseFileSystem {
	return &GoFuseFileSystem{
		cacheManager:      cacheManager,
		logger:            log.New(log.Writer(), "[GOFUSE] ", log.LstdFlags),
		enablePassthrough: boolFromEnv("FUSE_GOFUSE_ENABLE_PASSTHROUGH", false),
	}
}

func (g *GoFuseFileSystem) Mount(ctx context.Context, mountPoint string) error {
	if err := os.MkdirAll(mountPoint, 0o755); err != nil {
		return err
	}

	root := &goFuseNode{
		gfs:  g,
		path: "/",
		kind: goFuseNodeDir,
	}
	entryTTL := attrCacheTTL
	attrTTL := attrCacheTTL
	opts := &gfs.Options{
		MountOptions: gfuse.MountOptions{
			FsName:        "fuse-client",
			Name:          "fuse-client",
			MaxReadAhead:  maxReadaheadBytes,
			MaxBackground: maxBackgroundReqs,
		},
		EntryTimeout: &entryTTL,
		AttrTimeout:  &attrTTL,
	}

	server, err := gfs.Mount(mountPoint, root, opts)
	if err != nil {
		return err
	}
	if err := server.WaitMount(); err != nil {
		_ = server.Unmount()
		return err
	}

	g.mu.Lock()
	g.server = server
	g.mountPoint = mountPoint
	g.mu.Unlock()

	g.logger.Printf("Mounted go-fuse filesystem at: %s (passthrough=%t)", mountPoint, g.enablePassthrough)
	return nil
}

func (g *GoFuseFileSystem) Serve(ctx context.Context) error {
	g.mu.Lock()
	server := g.server
	g.mu.Unlock()
	if server == nil {
		return syscall.EINVAL
	}

	done := make(chan struct{})
	go func() {
		server.Wait()
		close(done)
	}()

	select {
	case <-ctx.Done():
		return nil
	case <-done:
		return nil
	}
}

func (g *GoFuseFileSystem) Unmount(mountPoint string) error {
	g.mu.Lock()
	server := g.server
	g.mu.Unlock()
	if server == nil {
		return nil
	}
	return server.Unmount()
}

type goFuseNode struct {
	gfs.Inode

	gfs  *GoFuseFileSystem
	path string
	kind goFuseNodeKind

	mu        sync.RWMutex
	entry     *cache.CacheEntry
	isNew     bool
	dirty     bool
	writeFile *os.File
	writePath string
}

type localFilePathResolver interface {
	LocalFilePath(ctx context.Context, filePath string) (string, bool)
}

type goFusePassthroughHandle struct {
	mu   sync.Mutex
	file *os.File
}

var _ = (gfs.InodeEmbedder)((*goFuseNode)(nil))
var _ = (gfs.NodeGetattrer)((*goFuseNode)(nil))
var _ = (gfs.NodeLookuper)((*goFuseNode)(nil))
var _ = (gfs.NodeReaddirer)((*goFuseNode)(nil))
var _ = (gfs.NodeCreater)((*goFuseNode)(nil))
var _ = (gfs.NodeMkdirer)((*goFuseNode)(nil))
var _ = (gfs.NodeRenamer)((*goFuseNode)(nil))
var _ = (gfs.NodeUnlinker)((*goFuseNode)(nil))
var _ = (gfs.NodeOpener)((*goFuseNode)(nil))
var _ = (gfs.NodeReader)((*goFuseNode)(nil))
var _ = (gfs.NodeWriter)((*goFuseNode)(nil))
var _ = (gfs.NodeSetattrer)((*goFuseNode)(nil))
var _ = (gfs.NodeFlusher)((*goFuseNode)(nil))
var _ = (gfs.NodeFsyncer)((*goFuseNode)(nil))
var _ = (gfs.NodeReleaser)((*goFuseNode)(nil))
var _ = (gfs.FilePassthroughFder)((*goFusePassthroughHandle)(nil))
var _ = (gfs.FileReader)((*goFusePassthroughHandle)(nil))
var _ = (gfs.FileReleaser)((*goFusePassthroughHandle)(nil))

func (n *goFuseNode) Getattr(ctx context.Context, f gfs.FileHandle, out *gfuse.AttrOut) syscall.Errno {
	out.SetTimeout(attrCacheTTL)
	if n.kind == goFuseNodeDir {
		fillDirAttr(&out.Attr, n.path)
		return 0
	}

	entry := n.resolveEntry(ctx)
	if entry == nil {
		return syscall.ENOENT
	}
	fillFileAttr(&out.Attr, n.path, entry)
	return 0
}

func (n *goFuseNode) Lookup(ctx context.Context, name string, out *gfuse.EntryOut) (*gfs.Inode, syscall.Errno) {
	if n.kind != goFuseNodeDir {
		return nil, syscall.ENOTDIR
	}
	fullPath := filepath.Join(n.path, name)
	if child, ok := n.lookupNodeFromPath(ctx, fullPath); ok {
		setEntryTimeouts(out)
		if child.kind == goFuseNodeDir {
			fillDirAttr(&out.Attr, child.path)
		} else if entry := child.resolveEntry(ctx); entry != nil {
			fillFileAttr(&out.Attr, child.path, entry)
		} else {
			// Fallback keeps lookup resilient even if metadata refresh races.
			out.Attr.Ino = pathToInode(child.path)
			out.Attr.Mode = goFuseFileMode
			out.Attr.Blksize = defaultBlockSize
		}
		stable := stableAttrForNode(child)
		return n.NewInode(ctx, child, stable), 0
	}
	return nil, syscall.ENOENT
}

func (n *goFuseNode) Readdir(ctx context.Context) (gfs.DirStream, syscall.Errno) {
	if n.kind != goFuseNodeDir {
		return nil, syscall.ENOTDIR
	}
	entries, err := n.gfs.cacheManager.List(ctx)
	if err != nil {
		return nil, syscall.EIO
	}

	typeValByName := make(map[string]uint32)
	for _, entry := range entries {
		if !strings.HasPrefix(entry.FilePath, n.path) {
			continue
		}
		relPath := strings.TrimPrefix(entry.FilePath, n.path)
		relPath = strings.TrimPrefix(relPath, "/")
		if relPath == "" {
			continue
		}
		parts := strings.Split(relPath, "/")
		if len(parts) > 1 {
			typeValByName[parts[0]] = syscall.S_IFDIR
			continue
		}
		if _, exists := typeValByName[relPath]; !exists {
			typeValByName[relPath] = syscall.S_IFREG
		}
	}

	names := make([]string, 0, len(typeValByName))
	for name := range typeValByName {
		names = append(names, name)
	}
	sort.Strings(names)

	dirents := make([]gfuse.DirEntry, 0, len(names))
	for _, name := range names {
		mode := typeValByName[name]
		dirents = append(dirents, gfuse.DirEntry{
			Name: name,
			Mode: mode,
			Ino:  pathToInode(filepath.Join(n.path, name)),
		})
	}
	return gfs.NewListDirStream(dirents), 0
}

func (n *goFuseNode) Create(ctx context.Context, name string, flags uint32, mode uint32, out *gfuse.EntryOut) (*gfs.Inode, gfs.FileHandle, uint32, syscall.Errno) {
	if n.kind != goFuseNodeDir {
		return nil, nil, 0, syscall.ENOTDIR
	}
	fullPath := filepath.Join(n.path, name)
	if n.fileExists(ctx, fullPath) {
		return nil, nil, 0, syscall.EEXIST
	}

	entry := &cache.CacheEntry{
		FilePath:     fullPath,
		StoragePath:  fullPath,
		Size:         0,
		LastAccessed: time.Now(),
		Data:         []byte{},
	}
	child := &goFuseNode{
		gfs:   n.gfs,
		path:  fullPath,
		kind:  goFuseNodeFile,
		entry: entry,
		isNew: true,
	}
	setEntryTimeouts(out)
	inode := n.NewInode(ctx, child, stableAttrForNode(child))
	return inode, child, gfuse.FOPEN_KEEP_CACHE, 0
}

func (n *goFuseNode) Mkdir(ctx context.Context, name string, mode uint32, out *gfuse.EntryOut) (*gfs.Inode, syscall.Errno) {
	if n.kind != goFuseNodeDir {
		return nil, syscall.ENOTDIR
	}
	child := &goFuseNode{
		gfs:  n.gfs,
		path: filepath.Join(n.path, name),
		kind: goFuseNodeDir,
	}
	setEntryTimeouts(out)
	return n.NewInode(ctx, child, stableAttrForNode(child)), 0
}

func (n *goFuseNode) Rename(ctx context.Context, name string, newParent gfs.InodeEmbedder, newName string, flags uint32) syscall.Errno {
	if n.kind != goFuseNodeDir {
		return syscall.ENOTDIR
	}
	dstParent, ok := newParent.(*goFuseNode)
	if !ok {
		return syscall.EIO
	}
	oldPath := filepath.Join(n.path, name)
	newPath := filepath.Join(dstParent.path, newName)

	entry, err := n.gfs.cacheManager.Get(ctx, oldPath)
	if err != nil {
		return syscall.ENOENT
	}
	newEntry := &cache.CacheEntry{
		FilePath:     newPath,
		StoragePath:  newPath,
		Size:         entry.Size,
		LastAccessed: time.Now(),
		Data:         entry.Data,
	}
	if err := n.gfs.cacheManager.Put(ctx, newEntry); err != nil {
		return syscall.EIO
	}
	if err := n.gfs.cacheManager.Delete(ctx, oldPath); err != nil {
		n.gfs.logger.Printf("Rename: failed to delete old path %s: %v", oldPath, err)
	}
	return 0
}

func (n *goFuseNode) Unlink(ctx context.Context, name string) syscall.Errno {
	if n.kind != goFuseNodeDir {
		return syscall.ENOTDIR
	}
	fullPath := filepath.Join(n.path, name)
	if err := n.gfs.cacheManager.Delete(ctx, fullPath); err != nil {
		return syscall.EIO
	}
	return 0
}

func (n *goFuseNode) Open(ctx context.Context, flags uint32) (gfs.FileHandle, uint32, syscall.Errno) {
	if n.kind != goFuseNodeFile {
		return nil, 0, syscall.EISDIR
	}

	if n.gfs.enablePassthrough && (flags&uint32(syscall.O_ACCMODE)) == uint32(syscall.O_RDONLY) {
		if resolver, ok := n.gfs.cacheManager.(localFilePathResolver); ok {
			if localPath, ok := resolver.LocalFilePath(ctx, n.path); ok {
				f, err := os.Open(localPath)
				if err == nil {
					return &goFusePassthroughHandle{file: f}, gfuse.FOPEN_KEEP_CACHE | gfuse.FOPEN_PASSTHROUGH, 0
				}
				n.gfs.logger.Printf("Passthrough open fallback for %s (%s): %v", n.path, localPath, err)
			}
		}
	}

	return n, gfuse.FOPEN_KEEP_CACHE, 0
}

func (h *goFusePassthroughHandle) PassthroughFd() (int, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.file == nil {
		return -1, false
	}
	return int(h.file.Fd()), true
}

func (h *goFusePassthroughHandle) Read(ctx context.Context, dest []byte, off int64) (gfuse.ReadResult, syscall.Errno) {
	h.mu.Lock()
	file := h.file
	h.mu.Unlock()
	if file == nil {
		return nil, syscall.EBADF
	}
	n, err := file.ReadAt(dest, off)
	if err != nil && err != io.EOF {
		return nil, syscall.EIO
	}
	return gfuse.ReadResultData(dest[:n]), 0
}

func (h *goFusePassthroughHandle) Release(ctx context.Context) syscall.Errno {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.file == nil {
		return 0
	}
	if err := h.file.Close(); err != nil {
		return syscall.EIO
	}
	h.file = nil
	return 0
}

func (n *goFuseNode) Read(ctx context.Context, f gfs.FileHandle, dest []byte, off int64) (gfuse.ReadResult, syscall.Errno) {
	if n.kind != goFuseNodeFile {
		return nil, syscall.EISDIR
	}

	n.mu.RLock()
	if n.isNew && n.dirty && n.writeFile != nil {
		size := n.entry.Size
		if off >= size {
			n.mu.RUnlock()
			return gfuse.ReadResultData(nil), 0
		}
		limit := int64(len(dest))
		end := off + limit
		if end > size {
			end = size
		}
		buf := make([]byte, end-off)
		readN, err := n.writeFile.ReadAt(buf, off)
		n.mu.RUnlock()
		if err != nil && err != io.EOF {
			return nil, syscall.EIO
		}
		return gfuse.ReadResultData(buf[:readN]), 0
	}
	n.mu.RUnlock()

	if rr, ok := n.gfs.cacheManager.(rangeReader); ok {
		data, err := rr.ReadRange(ctx, n.path, off, len(dest))
		if err != nil {
			return nil, syscall.EIO
		}
		return gfuse.ReadResultData(data), 0
	}

	entry, err := n.gfs.cacheManager.Get(ctx, n.path)
	if err != nil {
		return nil, syscall.EIO
	}
	if off >= int64(len(entry.Data)) {
		return gfuse.ReadResultData(nil), 0
	}
	end := off + int64(len(dest))
	if end > int64(len(entry.Data)) {
		end = int64(len(entry.Data))
	}
	return gfuse.ReadResultData(entry.Data[off:end]), 0
}

func (n *goFuseNode) Write(ctx context.Context, f gfs.FileHandle, data []byte, off int64) (uint32, syscall.Errno) {
	if n.kind != goFuseNodeFile {
		return 0, syscall.EISDIR
	}
	n.mu.Lock()
	defer n.mu.Unlock()

	if !n.isNew {
		return 0, syscall.EPERM
	}
	if err := n.ensureWriteFileLocked(); err != nil {
		return 0, syscall.EIO
	}
	if _, err := n.writeFile.WriteAt(data, off); err != nil {
		return 0, syscall.EIO
	}

	prevSize := n.entry.Size
	needed := off + int64(len(data))
	if needed > n.entry.Size {
		n.entry.Size = needed
	}
	n.entry.LastAccessed = time.Now()
	n.dirty = true
	if crossedProgressBoundary(prevSize, n.entry.Size) {
		n.gfs.logger.Printf("Write progress %s: %d bytes", n.path, n.entry.Size)
	}
	return uint32(len(data)), 0
}

func (n *goFuseNode) Setattr(ctx context.Context, f gfs.FileHandle, in *gfuse.SetAttrIn, out *gfuse.AttrOut) syscall.Errno {
	if n.kind != goFuseNodeFile {
		return syscall.EISDIR
	}
	n.mu.Lock()
	defer n.mu.Unlock()

	if size, ok := in.GetSize(); ok {
		if !n.isNew {
			return syscall.EPERM
		}
		if err := n.ensureWriteFileLocked(); err != nil {
			return syscall.EIO
		}
		if err := n.writeFile.Truncate(int64(size)); err != nil {
			return syscall.EIO
		}
		n.entry.Size = int64(size)
		n.entry.LastAccessed = time.Now()
		n.dirty = true
	}
	out.SetTimeout(attrCacheTTL)
	fillFileAttr(&out.Attr, n.path, n.entry)
	return 0
}

func (n *goFuseNode) Flush(ctx context.Context, f gfs.FileHandle) syscall.Errno {
	if n.kind != goFuseNodeFile {
		return syscall.EISDIR
	}
	n.mu.Lock()
	defer n.mu.Unlock()

	if !n.dirty {
		return 0
	}
	if n.writeFile != nil {
		reader := io.NewSectionReader(n.writeFile, 0, n.entry.Size)
		if putter, ok := n.gfs.cacheManager.(streamingPutter); ok {
			if err := putter.PutFromReader(ctx, n.path, reader, n.entry.Size, time.Now()); err != nil {
				return syscall.EIO
			}
		} else {
			data := make([]byte, int(n.entry.Size))
			if _, err := io.ReadFull(reader, data); err != nil && err != io.EOF {
				return syscall.EIO
			}
			n.entry.Data = data
			if err := n.gfs.cacheManager.Put(ctx, n.entry); err != nil {
				return syscall.EIO
			}
		}
		n.entry.Data = nil
		n.cleanupWriteFileLocked()
	} else if err := n.gfs.cacheManager.Put(ctx, n.entry); err != nil {
		return syscall.EIO
	}
	n.dirty = false
	return 0
}

func (n *goFuseNode) Fsync(ctx context.Context, f gfs.FileHandle, flags uint32) syscall.Errno {
	return n.Flush(ctx, f)
}

func (n *goFuseNode) Release(ctx context.Context, f gfs.FileHandle) syscall.Errno {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.cleanupWriteFileLocked()
	return 0
}

func (n *goFuseNode) ensureWriteFileLocked() error {
	if n.writeFile != nil {
		return nil
	}
	tmp, err := os.CreateTemp("", "fuse-client-write-*")
	if err != nil {
		return err
	}
	n.writeFile = tmp
	n.writePath = tmp.Name()
	return nil
}

func (n *goFuseNode) cleanupWriteFileLocked() {
	if n.writeFile != nil {
		_ = n.writeFile.Close()
		n.writeFile = nil
	}
	if n.writePath != "" {
		_ = os.Remove(n.writePath)
		n.writePath = ""
	}
}

func (n *goFuseNode) resolveEntry(ctx context.Context) *cache.CacheEntry {
	n.mu.RLock()
	isNew := n.isNew
	if n.entry != nil {
		entryCopy := *n.entry
		n.mu.RUnlock()
		if isNew || entryCopy.Size > 0 {
			return &entryCopy
		}
	} else {
		n.mu.RUnlock()
	}

	// Size=0 for an existing file is often stale metadata while chunk manifests
	// are converging; prefer a fresh coordinator-backed view first.
	if lister, ok := n.gfs.cacheManager.(freshLister); ok {
		freshEntries, err := lister.ListFresh(ctx)
		if err == nil {
			if entry, ok := findEntryByPath(freshEntries, n.path); ok {
				n.mu.Lock()
				n.entry = entry
				n.mu.Unlock()
				entryCopy := *entry
				return &entryCopy
			}
		}
	}

	entries, err := n.gfs.cacheManager.List(ctx)
	if err != nil {
		return nil
	}
	if entry, ok := findEntryByPath(entries, n.path); ok {
		n.mu.Lock()
		n.entry = entry
		n.mu.Unlock()
		entryCopy := *entry
		return &entryCopy
	}
	return nil
}

func (n *goFuseNode) fileExists(ctx context.Context, fullPath string) bool {
	if lister, ok := n.gfs.cacheManager.(freshLister); ok {
		entries, err := lister.ListFresh(ctx)
		if err == nil {
			for _, entry := range entries {
				if entry.FilePath == fullPath {
					return true
				}
			}
			return false
		}
		n.gfs.logger.Printf("Create metadata refresh failed for %s (fallback to local metadata): %v", fullPath, err)
	}

	entries, err := n.gfs.cacheManager.List(ctx)
	if err != nil {
		return false
	}
	for _, entry := range entries {
		if entry.FilePath == fullPath {
			return true
		}
	}
	return false
}

func (n *goFuseNode) lookupNodeFromPath(ctx context.Context, fullPath string) (*goFuseNode, bool) {
	entries, err := n.gfs.cacheManager.List(ctx)
	if err != nil {
		return nil, false
	}
	if child, ok := nodeFromEntries(n.gfs, fullPath, entries); ok {
		if child.kind == goFuseNodeFile && child.entry != nil && child.entry.Size <= 0 {
			// Force a refresh below to avoid exposing stale EOF-sized files.
		} else {
			return child, true
		}
	}
	if lister, ok := n.gfs.cacheManager.(freshLister); ok {
		freshEntries, err := lister.ListFresh(ctx)
		if err == nil {
			if child, ok := nodeFromEntries(n.gfs, fullPath, freshEntries); ok {
				return child, true
			}
		}
	}
	return nil, false
}

func nodeFromEntries(g *GoFuseFileSystem, fullPath string, entries []*cache.CacheEntry) (*goFuseNode, bool) {
	for _, entry := range entries {
		if entry.FilePath != fullPath {
			continue
		}
		entryCopy := *entry
		return &goFuseNode{
			gfs:   g,
			path:  fullPath,
			kind:  goFuseNodeFile,
			entry: &entryCopy,
		}, true
	}
	for _, entry := range entries {
		if strings.HasPrefix(entry.FilePath, fullPath+"/") {
			return &goFuseNode{
				gfs:  g,
				path: fullPath,
				kind: goFuseNodeDir,
			}, true
		}
	}
	return nil, false
}

func findEntryByPath(entries []*cache.CacheEntry, fullPath string) (*cache.CacheEntry, bool) {
	for _, entry := range entries {
		if entry == nil || entry.FilePath != fullPath {
			continue
		}
		entryCopy := *entry
		return &entryCopy, true
	}
	return nil, false
}

func stableAttrForNode(node *goFuseNode) gfs.StableAttr {
	mode := uint32(syscall.S_IFREG)
	if node.kind == goFuseNodeDir {
		mode = uint32(syscall.S_IFDIR)
	}
	return gfs.StableAttr{
		Mode: mode,
		Ino:  pathToInode(node.path),
	}
}

func setEntryTimeouts(out *gfuse.EntryOut) {
	out.SetEntryTimeout(attrCacheTTL)
	out.SetAttrTimeout(attrCacheTTL)
}

func fillDirAttr(attr *gfuse.Attr, path string) {
	now := time.Now()
	attr.Ino = pathToInode(path)
	attr.Mode = goFuseDirMode
	attr.Nlink = 2
	attr.Size = 0
	attr.Blksize = defaultBlockSize
	attr.SetTimes(&now, &now, &now)
}

func fillFileAttr(attr *gfuse.Attr, path string, entry *cache.CacheEntry) {
	now := time.Now()
	mt := entry.LastAccessed
	if mt.IsZero() {
		mt = now
	}
	attr.Ino = pathToInode(path)
	attr.Mode = goFuseFileMode
	attr.Nlink = 1
	attr.Size = uint64(entry.Size)
	attr.Blksize = defaultBlockSize
	attr.SetTimes(&mt, &mt, &mt)
}

func boolFromEnv(name string, defaultValue bool) bool {
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return defaultValue
	}
	switch strings.ToLower(raw) {
	case "1", "true", "yes", "on":
		return true
	case "0", "false", "no", "off":
		return false
	default:
		return defaultValue
	}
}

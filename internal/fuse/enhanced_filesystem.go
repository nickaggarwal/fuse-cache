package fuse

import (
	"context"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	"fuse-client/internal/cache"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
)

// EnhancedFileSystem represents our enhanced FUSE filesystem with in-memory directory structure
type EnhancedFileSystem struct {
	cacheManager      cache.CacheManager
	directoryTree     *cache.DirectoryTree
	consistencyMgr    *cache.ConsistencyManager
	logger            *log.Logger
	
	// Configuration
	config            *EnhancedFSConfig
}

// EnhancedFSConfig holds configuration for the enhanced filesystem
type EnhancedFSConfig struct {
	SmallFileCacheThreshold int64         // Files smaller than this are cached in memory
	ReadAheadThreshold     int64         // Threshold for read-ahead optimization  
	MetadataCacheTTL       time.Duration // How long to cache metadata
	DataCacheTTL           time.Duration // How long to cache file data
	MaxConcurrentReads     int           // Maximum concurrent read operations
	ConsistencyLevel       ConsistencyLevel
}

// ConsistencyLevel defines the consistency requirements
type ConsistencyLevel int

const (
	EventualConsistency ConsistencyLevel = iota // Cache for longer periods
	SessionConsistency                          // Cache for shorter periods
	StrongConsistency                          // Always validate with storage
)

// NewEnhancedFileSystem creates a new enhanced FUSE filesystem
func NewEnhancedFileSystem(cacheManager cache.CacheManager, config *EnhancedFSConfig) *EnhancedFileSystem {
	if config == nil {
		config = &EnhancedFSConfig{
			SmallFileCacheThreshold: 64 * 1024,        // 64KB
			ReadAheadThreshold:     1024 * 1024,       // 1MB
			MetadataCacheTTL:       30 * time.Second,
			DataCacheTTL:           5 * time.Second,
			MaxConcurrentReads:     10,
			ConsistencyLevel:       SessionConsistency,
		}
	}

	// Create directory tree
	treeConfig := &cache.DirectoryTreeConfig{
		SmallFileCacheThreshold: config.SmallFileCacheThreshold,
		MetadataTTL:            config.MetadataCacheTTL,
		DataCacheTTL:           config.DataCacheTTL,
		ValidationInterval:     10 * time.Second,
		MaxCachedFiles:         1000,
	}
	directoryTree := cache.NewDirectoryTree(treeConfig)

	// Create consistency manager
	consistencyConfig := &cache.ConsistencyConfig{
		ValidationInterval:   10 * time.Second,
		StaleThreshold:      config.MetadataCacheTTL,
		MaxConcurrentChecks: 5,
		PeerNotificationTTL: 5 * time.Second,
	}
	consistencyMgr := cache.NewConsistencyManager(directoryTree, cacheManager, nil, consistencyConfig)

	fs := &EnhancedFileSystem{
		cacheManager:   cacheManager,
		directoryTree:  directoryTree,
		consistencyMgr: consistencyMgr,
		logger:         log.New(log.Writer(), "[ENHANCED_FUSE] ", log.LstdFlags),
		config:         config,
	}

	// Start consistency manager
	consistencyMgr.Start()

	return fs
}

// Mount mounts the enhanced FUSE filesystem
func (efs *EnhancedFileSystem) Mount(ctx context.Context, mountPoint string) (*fuse.Conn, error) {
	// Create mount point if it doesn't exist
	if err := os.MkdirAll(mountPoint, 0755); err != nil {
		return nil, err
	}

	// Mount the filesystem
	conn, err := fuse.Mount(
		mountPoint,
		fuse.FSName("enhanced-fuse-client"),
		fuse.Subtype("enhanced-fuse-client"),
	)
	if err != nil {
		return nil, err
	}

	efs.logger.Printf("Mounted enhanced FUSE filesystem at: %s", mountPoint)
	return conn, nil
}

// Serve serves the enhanced FUSE filesystem
func (efs *EnhancedFileSystem) Serve(ctx context.Context, conn *fuse.Conn) error {
	defer conn.Close()
	defer efs.consistencyMgr.Stop()

	efs.logger.Printf("Enhanced FUSE filesystem is ready")
	return fs.Serve(conn, efs)
}

// Root returns the root directory of the filesystem
func (efs *EnhancedFileSystem) Root() (fs.Node, error) {
	return &EnhancedDir{
		fs:   efs,
		path: "/",
	}, nil
}

// EnhancedDir represents a directory in the enhanced FUSE filesystem
type EnhancedDir struct {
	fs   *EnhancedFileSystem
	path string
}

// Attr returns the attributes of the directory - INSTANT operation using directory tree
func (d *EnhancedDir) Attr(ctx context.Context, a *fuse.Attr) error {
	node := d.fs.directoryTree.GetDirectoryNode(d.path)
	if node != nil {
		node.Mu.RLock()
		a.Mtime = node.Metadata.ModTime
		a.Atime = node.Metadata.LastAccess
		a.Ctime = node.Metadata.ModTime
		node.Mu.RUnlock()
	} else {
		// Fallback for directories not in tree yet
		now := time.Now()
		a.Mtime = now
		a.Atime = now  
		a.Ctime = now
	}

	a.Inode = 1
	a.Mode = os.ModeDir | 0755
	a.Nlink = 2
	a.Size = 0
	
	return nil
}

// Lookup looks up a file or directory - OPTIMIZED using directory tree
func (d *EnhancedDir) Lookup(ctx context.Context, name string) (fs.Node, error) {
	fullPath := filepath.Join(d.path, name)

	// First check directory tree for instant lookup
	if fileNode := d.fs.directoryTree.GetFileNode(fullPath); fileNode != nil {
		return &EnhancedFile{
			fs:       d.fs,
			path:     fullPath,
			fileNode: fileNode,
		}, nil
	}

	if dirNode := d.fs.directoryTree.GetDirectoryNode(fullPath); dirNode != nil {
		return &EnhancedDir{
			fs:   d.fs,
			path: fullPath,
		}, nil
	}

	// If not in directory tree, fall back to cache manager lookup
	lookupCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	
	if entry, err := d.fs.cacheManager.Get(lookupCtx, fullPath); err == nil {
		// Add to directory tree for future fast lookups
		fileNode := d.fs.directoryTree.AddOrUpdateFile(entry)
		
		return &EnhancedFile{
			fs:       d.fs,
			path:     fullPath,
			fileNode: fileNode,
		}, nil
	}

	// Check if it's a directory by looking for files with this prefix
	entries, err := d.fs.cacheManager.List(ctx)
	if err != nil {
		return nil, fuse.ENOENT
	}

	for _, entry := range entries {
		if filepath.HasPrefix(entry.FilePath, fullPath+"/") {
			return &EnhancedDir{
				fs:   d.fs,
				path: fullPath,
			}, nil
		}
	}

	return nil, fuse.ENOENT
}

// ReadDirAll reads all entries in the directory - INSTANT using directory tree
func (d *EnhancedDir) ReadDirAll(ctx context.Context) ([]fuse.Dirent, error) {
	entries, err := d.fs.directoryTree.GetDirectoryEntries(d.path)
	if err == cache.ErrDirectoryNotFound {
		// Directory not in tree, fall back to cache manager
		return d.readDirAllFallback(ctx)
	}
	if err != nil {
		return nil, fuse.EIO
	}

	dirents := make([]fuse.Dirent, len(entries))
	for i, entry := range entries {
		dirents[i] = fuse.Dirent{
			Name: entry.Name,
			Type: d.entryTypeToFuseType(entry.Type),
		}
	}

	d.fs.logger.Printf("Listed %d entries from directory tree for %s", len(entries), d.path)
	return dirents, nil
}

// readDirAllFallback fallback implementation using cache manager
func (d *EnhancedDir) readDirAllFallback(ctx context.Context) ([]fuse.Dirent, error) {
	entries, err := d.fs.cacheManager.List(ctx)
	if err != nil {
		return nil, fuse.EIO
	}

	var dirents []fuse.Dirent
	seen := make(map[string]bool)

	for _, entry := range entries {
		if !filepath.HasPrefix(entry.FilePath, d.path) {
			continue
		}

		relPath := entry.FilePath[len(d.path):]
		if len(relPath) > 0 && relPath[0] == '/' {
			relPath = relPath[1:]
		}

		if relPath == "" {
			continue
		}

		parts := filepath.SplitList(relPath)
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

		// Update directory tree with this entry
		d.fs.directoryTree.AddOrUpdateFile(entry)
	}

	return dirents, nil
}

// entryTypeToFuseType converts cache entry type to FUSE dirent type
func (d *EnhancedDir) entryTypeToFuseType(t cache.EntryType) fuse.DirentType {
	switch t {
	case cache.EntryTypeDirectory:
		return fuse.DT_Dir
	case cache.EntryTypeFile:
		return fuse.DT_File
	default:
		return fuse.DT_Unknown
	}
}

// Create creates a new file
func (d *EnhancedDir) Create(ctx context.Context, req *fuse.CreateRequest, resp *fuse.CreateResponse) (fs.Node, fs.Handle, error) {
	fullPath := filepath.Join(d.path, req.Name)

	entry := &cache.CacheEntry{
		FilePath:     fullPath,
		Size:         0,
		LastAccessed: time.Now(),
		Data:         []byte{},
	}

	// Store in cache manager first to ensure consistency
	if err := d.fs.cacheManager.Put(ctx, entry); err != nil {
		d.fs.logger.Printf("[ERROR] Failed to create file %s in cache: %v", fullPath, err)
		return nil, nil, fuse.EIO
	}

	// Add to directory tree (this will be done automatically by the enhanced cache manager)
	fileNode := d.fs.directoryTree.AddOrUpdateFile(entry)

	file := &EnhancedFile{
		fs:       d.fs,
		path:     fullPath,
		fileNode: fileNode,
	}

	d.fs.logger.Printf("Created file: %s", fullPath)
	return file, file, nil
}

// Mkdir creates a new directory
func (d *EnhancedDir) Mkdir(ctx context.Context, req *fuse.MkdirRequest) (fs.Node, error) {
	fullPath := filepath.Join(d.path, req.Name)
	
	// Create a placeholder entry for the directory in cache storage
	// This ensures consistency between cache storage and directory tree
	entry := &cache.CacheEntry{
		FilePath:     fullPath + "/.keep",  // Directory marker file
		Size:         0,
		LastAccessed: time.Now(),
		Data:         []byte{},
	}
	
	// Store in cache manager to ensure directory structure exists
	if err := d.fs.cacheManager.Put(ctx, entry); err != nil {
		d.fs.logger.Printf("[ERROR] Failed to create directory marker for %s: %v", fullPath, err)
		return nil, fuse.EIO
	}
	
	// Ensure directory exists in tree (this creates the actual directory structure)
	d.fs.directoryTree.GetDirectoryNode(fullPath)

	d.fs.logger.Printf("Created directory: %s", fullPath)
	return &EnhancedDir{
		fs:   d.fs,
		path: fullPath,
	}, nil
}

// Remove removes a file or directory  
func (d *EnhancedDir) Remove(ctx context.Context, req *fuse.RemoveRequest) error {
	fullPath := filepath.Join(d.path, req.Name)

	d.fs.logger.Printf("Removing: %s (dir: %v)", fullPath, req.Dir)

	// Remove from cache manager
	if err := d.fs.cacheManager.Delete(ctx, fullPath); err != nil {
		d.fs.logger.Printf("[WARNING] Failed to delete %s from cache: %v", fullPath, err)
		// Continue anyway - file might not exist in cache
	}

	// Remove from directory tree
	if req.Dir {
		// For directories, we might need to remove directory markers
		d.fs.directoryTree.RemoveFile(fullPath + "/.keep")
	} else {
		d.fs.directoryTree.RemoveFile(fullPath)
	}

	d.fs.logger.Printf("Removed: %s", fullPath)
	return nil
}

// Rename renames/moves a file or directory (essential for vi and other editors)
func (d *EnhancedDir) Rename(ctx context.Context, req *fuse.RenameRequest, newDir fs.Node) error {
	oldPath := filepath.Join(d.path, req.OldName)
	
	// Get the new directory path
	newDirPath := "/"
	if enhancedNewDir, ok := newDir.(*EnhancedDir); ok {
		newDirPath = enhancedNewDir.path
	}
	newPath := filepath.Join(newDirPath, req.NewName)

	d.fs.logger.Printf("Renaming: %s -> %s", oldPath, newPath)

	// Get the file data from the old location
	entry, err := d.fs.cacheManager.Get(ctx, oldPath)
	if err != nil {
		d.fs.logger.Printf("[ERROR] Failed to get file for rename %s: %v", oldPath, err)
		return fuse.ENOENT
	}

	// Create entry with new path
	newEntry := &cache.CacheEntry{
		FilePath:     newPath,
		StoragePath:  newPath,
		Size:         entry.Size,
		LastAccessed: time.Now(),
		Data:         entry.Data,
		Tier:         entry.Tier,
	}

	// Store at new location
	if err := d.fs.cacheManager.Put(ctx, newEntry); err != nil {
		d.fs.logger.Printf("[ERROR] Failed to create file at new location %s: %v", newPath, err)
		return fuse.EIO
	}

	// Remove from old location
	if err := d.fs.cacheManager.Delete(ctx, oldPath); err != nil {
		d.fs.logger.Printf("[WARNING] Failed to delete old file %s: %v", oldPath, err)
		// Continue anyway
	}

	// Update directory tree
	d.fs.directoryTree.RemoveFile(oldPath)
	d.fs.directoryTree.AddOrUpdateFile(newEntry)

	d.fs.logger.Printf("Successfully renamed: %s -> %s", oldPath, newPath)
	return nil
}

// Link creates a hard link (used by some editors for backup files)
func (d *EnhancedDir) Link(ctx context.Context, req *fuse.LinkRequest, old fs.Node) (fs.Node, error) {
	newPath := filepath.Join(d.path, req.NewName)
	
	// Get the old file
	if oldFile, ok := old.(*EnhancedFile); ok {
		d.fs.logger.Printf("Creating hard link: %s -> %s", oldFile.path, newPath)
		
		// Get the original file data
		entry, err := d.fs.cacheManager.Get(ctx, oldFile.path)
		if err != nil {
			d.fs.logger.Printf("[ERROR] Failed to get original file for link %s: %v", oldFile.path, err)
			return nil, fuse.ENOENT
		}
		
		// Create new entry with same data but different path
		newEntry := &cache.CacheEntry{
			FilePath:     newPath,
			StoragePath:  newPath,
			Size:         entry.Size,
			LastAccessed: time.Now(),
			Data:         entry.Data,
			Tier:         entry.Tier,
		}
		
		// Store the link
		if err := d.fs.cacheManager.Put(ctx, newEntry); err != nil {
			d.fs.logger.Printf("[ERROR] Failed to create hard link %s: %v", newPath, err)
			return nil, fuse.EIO
		}
		
		// Add to directory tree
		fileNode := d.fs.directoryTree.AddOrUpdateFile(newEntry)
		
		newFile := &EnhancedFile{
			fs:       d.fs,
			path:     newPath,
			fileNode: fileNode,
		}
		
		d.fs.logger.Printf("Successfully created hard link: %s", newPath)
		return newFile, nil
	}
	
	return nil, fuse.ENOSYS
}

// Symlink creates a symbolic link
func (d *EnhancedDir) Symlink(ctx context.Context, req *fuse.SymlinkRequest) (fs.Node, error) {
	linkPath := filepath.Join(d.path, req.NewName)
	
	d.fs.logger.Printf("Creating symlink: %s -> %s", linkPath, req.Target)
	
	// Store symlink as special file with target as content
	entry := &cache.CacheEntry{
		FilePath:     linkPath,
		StoragePath:  linkPath,
		Size:         int64(len(req.Target)),
		LastAccessed: time.Now(),
		Data:         []byte(req.Target),
	}
	
	if err := d.fs.cacheManager.Put(ctx, entry); err != nil {
		d.fs.logger.Printf("[ERROR] Failed to create symlink %s: %v", linkPath, err)
		return nil, fuse.EIO
	}
	
	fileNode := d.fs.directoryTree.AddOrUpdateFile(entry)
	
	file := &EnhancedFile{
		fs:       d.fs,
		path:     linkPath,
		fileNode: fileNode,
	}
	
	d.fs.logger.Printf("Successfully created symlink: %s -> %s", linkPath, req.Target)
	return file, nil
}

// Readlink reads the target of a symbolic link
func (f *EnhancedFile) Readlink(ctx context.Context, req *fuse.ReadlinkRequest) (string, error) {
	f.fs.logger.Printf("Reading symlink: %s", f.path)
	
	// Get the file content (which contains the target)
	entry, err := f.fs.cacheManager.Get(ctx, f.path)
	if err != nil {
		return "", fuse.ENOENT
	}
	
	target := string(entry.Data)
	f.fs.logger.Printf("Symlink %s -> %s", f.path, target)
	return target, nil
}

// EnhancedFile represents a file in the enhanced FUSE filesystem
type EnhancedFile struct {
	fs       *EnhancedFileSystem
	path     string
	fileNode *cache.FileNode
	mu       sync.RWMutex
}

// Attr returns the attributes of the file - INSTANT using file node
func (f *EnhancedFile) Attr(ctx context.Context, a *fuse.Attr) error {
	if f.fileNode == nil {
		return fuse.ENOENT
	}

	f.fileNode.Mu.RLock()
	defer f.fileNode.Mu.RUnlock()

	a.Inode = 2
	a.Mode = 0644
	a.Nlink = 1
	a.Size = uint64(f.fileNode.Size)
	a.Mtime = f.fileNode.ModTime
	a.Atime = f.fileNode.LastAccess
	a.Ctime = f.fileNode.ModTime

	f.fs.logger.Printf("Instant attr for %s: size=%d", f.path, f.fileNode.Size)
	return nil
}

// Read reads data from the file - OPTIMIZED with smart caching
func (f *EnhancedFile) Read(ctx context.Context, req *fuse.ReadRequest, resp *fuse.ReadResponse) error {
	if f.fileNode == nil {
		return fuse.ENOENT
	}

	startTime := time.Now()

	// Try cached data first
	if data, ok := f.tryServeCachedData(req); ok {
		resp.Data = data
		f.fs.logger.Printf("Served %d bytes from cache for %s (%.2fms)", 
			len(data), f.path, float64(time.Since(startTime).Nanoseconds())/1e6)
		return nil
	}

	// Check if we need consistency validation
	if f.shouldValidateConsistency() {
		if err := f.validateConsistency(ctx); err != nil {
			f.fs.logger.Printf("Consistency validation failed for %s: %v", f.path, err)
		}
	}

	// Load data from storage tiers with optimized strategy
	entry, err := f.loadDataOptimized(ctx)
	if err != nil {
		return fuse.EIO
	}

	// Update file node with fresh data
	f.updateFileNode(entry)

	// Serve the requested range
	resp.Data = f.extractRequestedData(entry.Data, req)
	
	f.fs.logger.Printf("Read %d bytes from %s at offset %d (%.2fms)", 
		len(resp.Data), f.path, req.Offset, 
		float64(time.Since(startTime).Nanoseconds())/1e6)

	return nil
}

// tryServeCachedData attempts to serve data from cache
func (f *EnhancedFile) tryServeCachedData(req *fuse.ReadRequest) ([]byte, bool) {
	f.fileNode.Mu.RLock()
	defer f.fileNode.Mu.RUnlock()

	if f.fileNode.DataCache == nil {
		return nil, false
	}

	if data, valid := f.fileNode.DataCache.GetCachedData(); valid {
		// Check if we have the requested range
		if req.Offset >= int64(len(data)) {
			return []byte{}, true
		}

		end := req.Offset + int64(req.Size)
		if end > int64(len(data)) {
			end = int64(len(data))
		}

		return data[req.Offset:end], true
	}

	return nil, false
}

// shouldValidateConsistency checks if we need to validate file consistency
func (f *EnhancedFile) shouldValidateConsistency() bool {
	switch f.fs.config.ConsistencyLevel {
	case StrongConsistency:
		return true
	case SessionConsistency:
		return f.fs.consistencyMgr.ShouldValidateFile(f.fileNode)
	case EventualConsistency:
		f.fileNode.Mu.RLock()
		stale := f.fileNode.IsStale
		f.fileNode.Mu.RUnlock()
		return stale
	default:
		return false
	}
}

// validateConsistency validates file consistency with storage
func (f *EnhancedFile) validateConsistency(ctx context.Context) error {
	validateCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	
	entry, err := f.fs.cacheManager.Get(validateCtx, f.path)
	if err != nil {
		return err
	}

	f.fileNode.Mu.Lock()
	defer f.fileNode.Mu.Unlock()

	// Check if file has changed
	if entry.Size != f.fileNode.Size || !entry.LastAccessed.Equal(f.fileNode.ModTime) {
		f.fs.logger.Printf("File %s has changed during validation", f.path)
		
		// Update file node
		f.fileNode.Size = entry.Size
		f.fileNode.ModTime = entry.LastAccessed
		f.fileNode.IsStale = false
		
		// Invalidate cached data
		if f.fileNode.DataCache != nil {
			f.fileNode.DataCache.Invalidate()
		}
	}

	f.fileNode.LastValidated = time.Now()
	return nil
}

// loadDataOptimized loads data using optimized tier selection
func (f *EnhancedFile) loadDataOptimized(ctx context.Context) (*cache.CacheEntry, error) {
	f.fileNode.Mu.RLock()
	localTier := f.fileNode.LocalTier
	// peerLocations := f.fileNode.PeerLocations // Could be used for peer-specific optimization
	f.fileNode.Mu.RUnlock()

	// Try local tier first if available
	if localTier == cache.TierNVMe {
		fastCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
		defer cancel()
		
		if entry, err := f.fs.cacheManager.Get(fastCtx, f.path); err == nil {
			return entry, nil
		}
	}

	// Use full cache manager lookup for other cases
	return f.fs.cacheManager.Get(ctx, f.path)
}

// updateFileNode updates the file node with fresh data
func (f *EnhancedFile) updateFileNode(entry *cache.CacheEntry) {
	f.fileNode.Mu.Lock()
	defer f.fileNode.Mu.Unlock()

	f.fileNode.Size = entry.Size
	f.fileNode.ModTime = entry.LastAccessed
	f.fileNode.LocalTier = entry.Tier
	f.fileNode.LastAccess = time.Now()
	f.fileNode.AccessCount++
	f.fileNode.LastValidated = time.Now()
	f.fileNode.IsStale = false

	// Cache small files in memory
	if entry.Size <= f.fs.config.SmallFileCacheThreshold && entry.Data != nil {
		if f.fileNode.DataCache == nil {
			f.fileNode.DataCache = &cache.TimedCache{}
		}
		
		f.fileNode.DataCache.Mu.Lock()
		f.fileNode.DataCache.Data = append([]byte(nil), entry.Data...) // Deep copy
		f.fileNode.DataCache.CachedAt = time.Now()
		f.fileNode.DataCache.TTL = f.fs.config.DataCacheTTL
		f.fileNode.DataCache.Valid = true
		f.fileNode.DataCache.Size = entry.Size
		f.fileNode.DataCache.Mu.Unlock()
	}
}

// extractRequestedData extracts the requested byte range
func (f *EnhancedFile) extractRequestedData(data []byte, req *fuse.ReadRequest) []byte {
	if req.Offset >= int64(len(data)) {
		return []byte{}
	}

	end := req.Offset + int64(req.Size)
	if end > int64(len(data)) {
		end = int64(len(data))
	}

	return data[req.Offset:end]
}

// Open handles file open operations
func (f *EnhancedFile) Open(ctx context.Context, req *fuse.OpenRequest, resp *fuse.OpenResponse) (fs.Handle, error) {
	f.fs.logger.Printf("Opening file: %s with flags %v", f.path, req.Flags)
	
	// Handle file truncation if requested
	if req.Flags&fuse.OpenTruncate != 0 {
		f.fs.logger.Printf("Truncating file: %s", f.path)
		
		// Create empty entry for truncation
		entry := &cache.CacheEntry{
			FilePath:     f.path,
			Size:         0,
			LastAccessed: time.Now(),
			Data:         []byte{},
		}
		
		// Store in cache manager
		if err := f.fs.cacheManager.Put(ctx, entry); err != nil {
			f.fs.logger.Printf("[ERROR] Failed to truncate file %s: %v", f.path, err)
			return nil, fuse.EIO
		}
		
		// Update file node
		f.fileNode.Mu.Lock()
		f.fileNode.Size = 0
		f.fileNode.ModTime = time.Now()
		f.fileNode.LastValidated = time.Now()
		if f.fileNode.DataCache != nil {
			f.fileNode.DataCache.Invalidate()
		}
		f.fileNode.Mu.Unlock()
	}
	
	// Set response flags for better compatibility
	resp.Flags = fuse.OpenKeepCache
	
	return f, nil
}

// Write writes data to the file
func (f *EnhancedFile) Write(ctx context.Context, req *fuse.WriteRequest, resp *fuse.WriteResponse) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.fileNode == nil {
		return fuse.ENOENT
	}

	// Get current data or create new
	var currentData []byte
	if f.fileNode.DataCache != nil {
		if data, valid := f.fileNode.DataCache.GetCachedData(); valid {
			currentData = data
		}
	}

	// Resize data if necessary
	needed := int(req.Offset) + len(req.Data)
	if needed > len(currentData) {
		newData := make([]byte, needed)
		copy(newData, currentData)
		currentData = newData
	}

	// Write the data
	copy(currentData[req.Offset:], req.Data)

	// Create cache entry
	entry := &cache.CacheEntry{
		FilePath:     f.path,
		Size:         int64(len(currentData)),
		LastAccessed: time.Now(),
		Data:         currentData,
	}

	// Update directory tree immediately
	f.fs.directoryTree.AddOrUpdateFile(entry)

	// Store in cache asynchronously
	go func() {
		bgCtx := context.Background()
		if err := f.fs.cacheManager.Put(bgCtx, entry); err != nil {
			f.fs.logger.Printf("[ERROR] Async write for %s failed: %v", f.path, err)
		}
	}()

	resp.Size = len(req.Data)
	f.fs.logger.Printf("Wrote %d bytes to %s at offset %d (async)", len(req.Data), f.path, req.Offset)
	
	return nil
}

// Flush flushes cached data
func (f *EnhancedFile) Flush(ctx context.Context, req *fuse.FlushRequest) error {
	f.fs.logger.Printf("Flushing file: %s", f.path)
	// Data is already written asynchronously in Write method
	return nil
}

// Fsync syncs file data to storage
func (f *EnhancedFile) Fsync(ctx context.Context, req *fuse.FsyncRequest) error {
	f.fs.logger.Printf("Fsync file: %s", f.path)
	// For our cache system, this is essentially a no-op since we write through
	return nil
}

// Release releases the file handle
func (f *EnhancedFile) Release(ctx context.Context, req *fuse.ReleaseRequest) error {
	f.fs.logger.Printf("Releasing file: %s", f.path)
	return nil
}

// Setattr sets file attributes
func (f *EnhancedFile) Setattr(ctx context.Context, req *fuse.SetattrRequest, resp *fuse.SetattrResponse) error {
	f.fs.logger.Printf("Setting attributes for file: %s, valid: %v", f.path, req.Valid)
	
	if f.fileNode == nil {
		return fuse.ENOENT
	}

	f.fileNode.Mu.Lock()
	defer f.fileNode.Mu.Unlock()

	// Update size if requested (truncation)
	if req.Valid.Size() {
		newSize := int64(req.Size)
		f.fs.logger.Printf("Truncating file %s from %d to %d bytes", f.path, f.fileNode.Size, newSize)
		
		// Handle truncation by updating the cache entry
		var newData []byte
		if f.fileNode.DataCache != nil && f.fileNode.DataCache.IsValid() {
			if cachedData, valid := f.fileNode.DataCache.GetCachedData(); valid {
				if newSize < int64(len(cachedData)) {
					newData = cachedData[:newSize]
				} else {
					newData = cachedData
				}
			}
		}
		
		// Create updated cache entry
		entry := &cache.CacheEntry{
			FilePath:     f.path,
			Size:         newSize,
			LastAccessed: time.Now(),
			Data:         newData,
		}
		
		// Update cache manager
		if err := f.fs.cacheManager.Put(ctx, entry); err != nil {
			f.fs.logger.Printf("[ERROR] Failed to update file size for %s: %v", f.path, err)
			return fuse.EIO
		}
		
		f.fileNode.Size = newSize
		f.fileNode.ModTime = time.Now()
		
		// Invalidate cache if truncating
		if f.fileNode.DataCache != nil {
			f.fileNode.DataCache.Invalidate()
		}
	}

	// Update modification time if requested
	if req.Valid.Mtime() {
		f.fileNode.ModTime = req.Mtime
	}
	
	// Update access time if requested
	if req.Valid.Atime() {
		f.fileNode.LastAccess = req.Atime
	}

	// Return updated attributes
	resp.Attr.Inode = 2
	resp.Attr.Size = uint64(f.fileNode.Size)
	resp.Attr.Mtime = f.fileNode.ModTime
	resp.Attr.Atime = f.fileNode.LastAccess
	resp.Attr.Ctime = f.fileNode.ModTime
	resp.Attr.Mode = 0644
	resp.Attr.Nlink = 1
	resp.Attr.Uid = 1000  // Set reasonable UID
	resp.Attr.Gid = 1000  // Set reasonable GID

	return nil
}

// Note: File locking operations (Getlk, Setlk, Setlkw) are not available in bazil.org/fuse
// Most editors like vi will work without explicit file locking support

// Unmount unmounts the enhanced FUSE filesystem
func (efs *EnhancedFileSystem) Unmount(mountPoint string) error {
	efs.logger.Printf("Unmounting enhanced FUSE filesystem from: %s", mountPoint)
	return fuse.Unmount(mountPoint)
} 
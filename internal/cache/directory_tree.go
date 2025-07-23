package cache

import (
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// DirectoryTree maintains an in-memory representation of the filesystem
type DirectoryTree struct {
	root          *DirectoryNode
	fileMap       map[string]*FileNode // Fast path lookups: path -> node
	directoryMap  map[string]*DirectoryNode // Fast path lookups: path -> node
	Mu            sync.RWMutex
	
	// Version tracking for consistency
	globalVersion int64
	versionMap    map[string]int64 // file path -> version
	
	// Change detection and invalidation
	changeLog     *ChangeLog
	invalidations chan InvalidationEvent
	
	// Configuration
	config *DirectoryTreeConfig
}

// DirectoryTreeConfig holds configuration for the directory tree
type DirectoryTreeConfig struct {
	SmallFileCacheThreshold int64         // Files smaller than this are cached in memory
	MetadataTTL            time.Duration // How long metadata is valid
	DataCacheTTL           time.Duration // How long data cache is valid
	ValidationInterval     time.Duration // How often to check for stale files
	MaxCachedFiles         int           // Maximum number of files to cache data for
}

// DirectoryNode represents a directory in the tree
type DirectoryNode struct {
	Name      string
	Path      string
	Parent    *DirectoryNode
	Children  map[string]*DirectoryNode
	Files     map[string]*FileNode
	Metadata  *DirectoryMetadata
	Mu        sync.RWMutex
}

// DirectoryMetadata holds directory-specific metadata
type DirectoryMetadata struct {
	ModTime      time.Time
	LastAccess   time.Time
	ChildCount   int
	TotalSize    int64
	LastUpdated  time.Time
}

// FileNode represents a file in the tree with cached metadata
type FileNode struct {
	Name      string
	Path      string
	Parent    *DirectoryNode
	
	// Cached metadata (avoids storage layer calls)
	Size         int64
	ModTime      time.Time
	Version      int64
	
	// Storage location info  
	LocalTier     CacheTier
	RemoteTiers   []CacheTier
	PeerLocations []string
	
	// Performance optimizations
	DataCache     *TimedCache
	LastAccess    time.Time
	AccessCount   int64
	
	// Consistency tracking
	LastValidated  time.Time
	ValidationTTL  time.Duration
	IsStale        bool
	
	Mu sync.RWMutex
}

// TimedCache provides time-based data caching
type TimedCache struct {
	Data      []byte
	CachedAt  time.Time
	TTL       time.Duration
	Valid     bool
	Size      int64
	Mu        sync.RWMutex
}

// ChangeLog tracks filesystem changes for consistency
type ChangeLog struct {
	entries    []ChangeEntry
	maxEntries int
	mu         sync.RWMutex
}

// ChangeEntry represents a single filesystem change
type ChangeEntry struct {
	FilePath  string
	Version   int64
	Timestamp time.Time
	PeerID    string
	Operation string // "create", "update", "delete"
	Size      int64
}

// InvalidationEvent represents a file invalidation event
type InvalidationEvent struct {
	FilePath string
	Version  int64
	PeerID   string
}

// NewDirectoryTree creates a new in-memory directory tree
func NewDirectoryTree(config *DirectoryTreeConfig) *DirectoryTree {
	if config == nil {
		config = &DirectoryTreeConfig{
			SmallFileCacheThreshold: 64 * 1024,        // 64KB
			MetadataTTL:            30 * time.Second,
			DataCacheTTL:           5 * time.Second,
			ValidationInterval:     10 * time.Second,
			MaxCachedFiles:         1000,
		}
	}

	root := &DirectoryNode{
		Name:     "",
		Path:     "/",
		Parent:   nil,
		Children: make(map[string]*DirectoryNode),
		Files:    make(map[string]*FileNode),
		Metadata: &DirectoryMetadata{
			ModTime:     time.Now(),
			LastAccess:  time.Now(),
			LastUpdated: time.Now(),
		},
	}

	dt := &DirectoryTree{
		root:          root,
		fileMap:       make(map[string]*FileNode),
		directoryMap:  make(map[string]*DirectoryNode),
		versionMap:    make(map[string]int64),
		changeLog:     &ChangeLog{maxEntries: 1000},
		invalidations: make(chan InvalidationEvent, 100),
		config:        config,
	}

	dt.directoryMap["/"] = root
	return dt
}

// GetFileNode retrieves a file node by path
func (dt *DirectoryTree) GetFileNode(path string) *FileNode {
	dt.Mu.RLock()
	defer dt.Mu.RUnlock()
	return dt.fileMap[path]
}

// GetDirectoryNode retrieves a directory node by path
func (dt *DirectoryTree) GetDirectoryNode(path string) *DirectoryNode {
	dt.Mu.RLock()
	defer dt.Mu.RUnlock()
	return dt.directoryMap[path]
}

// AddOrUpdateFile adds or updates a file in the directory tree
func (dt *DirectoryTree) AddOrUpdateFile(entry *CacheEntry) *FileNode {
	dt.Mu.Lock()
	defer dt.Mu.Unlock()
	
	// Ensure parent directory exists
	parentDir := dt.ensureDirectoryPath(filepath.Dir(entry.FilePath))
	fileName := filepath.Base(entry.FilePath)
	
	// Get or create file node
	node := dt.fileMap[entry.FilePath]
	if node == nil {
		node = &FileNode{
			Name:          fileName,
			Path:          entry.FilePath,
			Parent:        parentDir,
			ValidationTTL: dt.config.MetadataTTL,
		}
		dt.fileMap[entry.FilePath] = node
		
		// Add to parent directory
		parentDir.Mu.Lock()
		parentDir.Files[fileName] = node
		parentDir.Mu.Unlock()
	}
	
	// Update file metadata
	node.Mu.Lock()
	defer node.Mu.Unlock()
	
	oldSize := node.Size
	node.Size = entry.Size
	node.ModTime = entry.LastAccessed
	node.LocalTier = entry.Tier
	node.LastValidated = time.Now()
	node.IsStale = false
	
	// Update version
	newVersion := atomic.AddInt64(&dt.globalVersion, 1)
	node.Version = newVersion
	dt.versionMap[entry.FilePath] = newVersion
	
	// Cache small files in memory
	if entry.Size <= dt.config.SmallFileCacheThreshold && entry.Data != nil {
		if node.DataCache == nil {
			node.DataCache = &TimedCache{}
		}
		
		node.DataCache.Mu.Lock()
		node.DataCache.Data = append([]byte(nil), entry.Data...) // Deep copy
		node.DataCache.CachedAt = time.Now()
		node.DataCache.TTL = dt.config.DataCacheTTL
		node.DataCache.Valid = true
		node.DataCache.Size = entry.Size
		node.DataCache.Mu.Unlock()
	}
	
	// Update parent directory metadata
	parentDir.Mu.Lock()
	parentDir.Metadata.TotalSize += (entry.Size - oldSize)
	parentDir.Metadata.LastUpdated = time.Now()
	parentDir.Mu.Unlock()
	
	// Log the change
	dt.changeLog.AddEntry(ChangeEntry{
		FilePath:  entry.FilePath,
		Version:   newVersion,
		Timestamp: time.Now(),
		Operation: "update",
		Size:      entry.Size,
	})
	
	return node
}

// RemoveFile removes a file from the directory tree
func (dt *DirectoryTree) RemoveFile(path string) bool {
	dt.Mu.Lock()
	defer dt.Mu.Unlock()
	
	node := dt.fileMap[path]
	if node == nil {
		return false
	}
	
	// Remove from parent directory
	if node.Parent != nil {
		node.Parent.Mu.Lock()
		delete(node.Parent.Files, node.Name)
		node.Parent.Metadata.TotalSize -= node.Size
		node.Parent.Metadata.LastUpdated = time.Now()
		node.Parent.Mu.Unlock()
	}
	
	// Remove from maps
	delete(dt.fileMap, path)
	delete(dt.versionMap, path)
	
	// Log the change
	newVersion := atomic.AddInt64(&dt.globalVersion, 1)
	dt.changeLog.AddEntry(ChangeEntry{
		FilePath:  path,
		Version:   newVersion,
		Timestamp: time.Now(),
		Operation: "delete",
	})
	
	return true
}

// ensureDirectoryPath ensures all directories in the path exist
func (dt *DirectoryTree) ensureDirectoryPath(path string) *DirectoryNode {
	if path == "/" {
		return dt.root
	}
	
	// Check if directory already exists
	if dir := dt.directoryMap[path]; dir != nil {
		return dir
	}
	
	// Create directory and all parent directories
	parts := strings.Split(strings.Trim(path, "/"), "/")
	currentPath := ""
	currentDir := dt.root
	
	for _, part := range parts {
		if part == "" {
			continue
		}
		
		currentPath = filepath.Join(currentPath, part)
		if !strings.HasPrefix(currentPath, "/") {
			currentPath = "/" + currentPath
		}
		
		// Check if this level exists
		if existing := dt.directoryMap[currentPath]; existing != nil {
			currentDir = existing
			continue
		}
		
		// Create new directory node
		newDir := &DirectoryNode{
			Name:     part,
			Path:     currentPath,
			Parent:   currentDir,
			Children: make(map[string]*DirectoryNode),
			Files:    make(map[string]*FileNode),
			Metadata: &DirectoryMetadata{
				ModTime:     time.Now(),
				LastAccess:  time.Now(),
				LastUpdated: time.Now(),
			},
		}
		
		// Add to parent
		currentDir.Mu.Lock()
		currentDir.Children[part] = newDir
		currentDir.Mu.Unlock()
		
		// Add to global map
		dt.directoryMap[currentPath] = newDir
		currentDir = newDir
	}
	
	return currentDir
}

// IsValid checks if a timed cache entry is still valid
func (tc *TimedCache) IsValid() bool {
	tc.Mu.RLock()
	defer tc.Mu.RUnlock()
	return tc.Valid && time.Since(tc.CachedAt) < tc.TTL
}

// GetCachedData retrieves cached data if valid
func (tc *TimedCache) GetCachedData() ([]byte, bool) {
	tc.Mu.RLock()
	defer tc.Mu.RUnlock()
	
	if !tc.Valid || time.Since(tc.CachedAt) >= tc.TTL {
		return nil, false
	}
	
	// Return a copy to prevent modification
	data := make([]byte, len(tc.Data))
	copy(data, tc.Data)
	return data, true
}

// Invalidate marks the cache as invalid
func (tc *TimedCache) Invalidate() {
	tc.Mu.Lock()
	defer tc.Mu.Unlock()
	tc.Valid = false
}

// AddEntry adds a change entry to the log
func (cl *ChangeLog) AddEntry(entry ChangeEntry) {
	cl.mu.Lock()
	defer cl.mu.Unlock()
	
	cl.entries = append(cl.entries, entry)
	
	// Keep only the most recent entries
	if len(cl.entries) > cl.maxEntries {
		cl.entries = cl.entries[len(cl.entries)-cl.maxEntries:]
	}
}

// GetRecentChanges returns recent changes since the given timestamp
func (cl *ChangeLog) GetRecentChanges(since time.Time) []ChangeEntry {
	cl.mu.RLock()
	defer cl.mu.RUnlock()
	
	var recent []ChangeEntry
	for _, entry := range cl.entries {
		if entry.Timestamp.After(since) {
			recent = append(recent, entry)
		}
	}
	
	return recent
}

// UpdateFromSnapshot updates the directory tree from a cache snapshot
func (dt *DirectoryTree) UpdateFromSnapshot(snapshot []*CacheEntry) {
	for _, entry := range snapshot {
		dt.AddOrUpdateFile(entry)
	}
}

// GetDirectoryEntries returns all entries in a directory for FUSE operations
func (dt *DirectoryTree) GetDirectoryEntries(path string) ([]DirectoryEntry, error) {
	node := dt.GetDirectoryNode(path)
	if node == nil {
		return nil, ErrDirectoryNotFound
	}
	
	node.Mu.RLock()
	defer node.Mu.RUnlock()
	
	entries := make([]DirectoryEntry, 0, len(node.Children)+len(node.Files))
	
	// Add subdirectories
	for name, child := range node.Children {
		entries = append(entries, DirectoryEntry{
			Name:  name,
			Type:  EntryTypeDirectory,
			Size:  0,
			Mode:  0755,
			Path:  child.Path,
		})
	}
	
	// Add files
	for name, file := range node.Files {
		file.Mu.RLock()
		entries = append(entries, DirectoryEntry{
			Name:  name,
			Type:  EntryTypeFile,
			Size:  file.Size,
			Mode:  0644,
			Path:  file.Path,
			ModTime: file.ModTime,
		})
		file.Mu.RUnlock()
	}
	
	// Update access time
	node.Metadata.LastAccess = time.Now()
	
	return entries, nil
}

// DirectoryEntry represents an entry in a directory listing
type DirectoryEntry struct {
	Name    string
	Type    EntryType
	Size    int64
	Mode    uint32
	Path    string
	ModTime time.Time
}

// EntryType represents the type of directory entry
type EntryType int

const (
	EntryTypeFile EntryType = iota
	EntryTypeDirectory
)

// Errors
var (
	ErrDirectoryNotFound = fmt.Errorf("directory not found")
	ErrFileNotFound      = fmt.Errorf("file not found")
) 
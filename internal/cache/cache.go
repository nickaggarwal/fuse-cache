package cache

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"fuse-client/internal/coordinator"
)

// Coordinator is an alias for the coordinator.Coordinator interface,
// used in CacheConfig to avoid import cycles for callers.
type Coordinator = coordinator.Coordinator

// CacheTier represents different cache tiers
type CacheTier int

const (
	TierNVMe CacheTier = iota
	TierPeer
	TierCloud
)

var (
	remoteReadOrderPeerFirst = []CacheTier{TierPeer, TierCloud}
)

// CacheEntry represents a cached file entry
type CacheEntry struct {
	FilePath     string
	StoragePath  string
	Size         int64
	LastAccessed time.Time
	Tier         CacheTier
	Data         []byte
	IsChunked    bool
	NumChunks    int64
	Checksum     string
}

// CacheManager manages the 3-tier cache system
type CacheManager interface {
	Get(ctx context.Context, filePath string) (*CacheEntry, error)
	Put(ctx context.Context, entry *CacheEntry) error
	Delete(ctx context.Context, filePath string) error
	List(ctx context.Context) ([]*CacheEntry, error)
	Evict(ctx context.Context, tier CacheTier) error
	Stats() (used, capacity int64)
	// WriteTo streams file data to w one chunk at a time, avoiding full
	// in-memory buffering of large chunked files. Returns total bytes written.
	WriteTo(ctx context.Context, filePath string, w io.Writer) (int64, error)
}

// TierStorage represents storage interface for each tier
type TierStorage interface {
	Read(ctx context.Context, path string) ([]byte, error)
	Write(ctx context.Context, path string, data []byte) error
	Delete(ctx context.Context, path string) error
	Exists(ctx context.Context, path string) bool
	Size(ctx context.Context, path string) (int64, error)
}

// CacheConfig holds configuration for cache tiers
type CacheConfig struct {
	NVMePath        string
	MaxNVMeSize     int64
	MaxPeerSize     int64
	PeerTimeout     time.Duration
	CloudTimeout    time.Duration
	CoordinatorAddr string
	Coordinator     Coordinator
	ChunkSize       int64

	// Cloud provider: "s3", "azure", or "gcp"
	CloudProvider string

	// S3 config
	S3Bucket string
	S3Region string

	// Azure config
	AzureStorageAccount string
	AzureStorageKey     string
	AzureContainerName  string

	// GCP config
	GCPBucket string

	// Cloud retry config
	CloudRetryCount    int
	CloudRetryBaseWait time.Duration

	// Peer replication config
	MinPeerReplicas int

	// MetadataRefreshTTL controls how frequently remote metadata is refreshed.
	// This mirrors metadata-cache TTL behavior in systems like Mountpoint.
	MetadataRefreshTTL time.Duration

	// LocalPeerID identifies this client when publishing metadata updates.
	LocalPeerID string

	// ParallelRangeReads controls worker count when a read range spans multiple
	// chunks. Values <=0 use a default.
	ParallelRangeReads int

	// RangePrefetchChunks controls how many chunks after the current read range
	// are prefetched opportunistically.
	RangePrefetchChunks int

	// RangeChunkCacheSize is the maximum number of chunks cached per file for
	// range reads.
	RangeChunkCacheSize int

	// PeerReadThroughputBytesPerSec is the assumed per-peer read throughput used
	// to decide when to enable hybrid peer+cloud reads for large files.
	PeerReadThroughputBytesPerSec int64
}

// DefaultCacheManager implements CacheManager
type DefaultCacheManager struct {
	config       *CacheConfig
	nvmeStorage  TierStorage
	peerStorage  TierStorage
	cloudStorage TierStorage
	entries      map[string]*CacheEntry
	nvmeUsed     int64
	mu           sync.RWMutex
	logger       *log.Logger
	metrics      *CacheMetrics
	metadataAt   time.Time
	metadataView []*CacheEntry
	rangeChunks  map[string]*chunkFileCache
	hybridHints  map[string]hybridReadHint
}

type chunkFileCache struct {
	chunks map[int64][]byte
	order  []int64
}

type hybridReadHint struct {
	enabled   bool
	expiresAt time.Time
}

const slowChunkReadThreshold = 2 * time.Second

// NewCacheManager creates a new cache manager
func NewCacheManager(config *CacheConfig) (*DefaultCacheManager, error) {
	if config.NVMePath == "" {
		return nil, errors.New("NVME path is required")
	}

	// Set default chunk size if not provided
	if config.ChunkSize <= 0 {
		config.ChunkSize = 4 * 1024 * 1024 // 4MB default
	}
	if config.CloudRetryCount <= 0 {
		config.CloudRetryCount = 5
	}
	if config.CloudRetryBaseWait <= 0 {
		config.CloudRetryBaseWait = 1 * time.Second
	}
	if config.MinPeerReplicas <= 0 {
		config.MinPeerReplicas = 3
	}
	if config.MetadataRefreshTTL <= 0 {
		config.MetadataRefreshTTL = 5 * time.Second
	}
	if config.ParallelRangeReads <= 0 {
		config.ParallelRangeReads = 4
	}
	if config.RangePrefetchChunks < 0 {
		config.RangePrefetchChunks = 0
	}
	if config.RangeChunkCacheSize <= 0 {
		config.RangeChunkCacheSize = 8
	}
	if config.PeerReadThroughputBytesPerSec <= 0 {
		config.PeerReadThroughputBytesPerSec = 200 * 1024 * 1024
	}

	cm := &DefaultCacheManager{
		config:      config,
		entries:     make(map[string]*CacheEntry),
		logger:      log.New(log.Writer(), "[CACHE] ", log.LstdFlags),
		metrics:     NewCacheMetrics(),
		rangeChunks: make(map[string]*chunkFileCache),
		hybridHints: make(map[string]hybridReadHint),
	}

	// Initialize storage tiers
	var err error
	cm.nvmeStorage, err = NewNVMeStorage(config.NVMePath)
	if err != nil {
		return nil, err
	}

	if config.Coordinator != nil {
		cm.peerStorage, err = NewPeerStorage(config.Coordinator, config.PeerTimeout, config.LocalPeerID)
	} else {
		// Fallback: create a no-op peer storage if no coordinator is configured
		cm.peerStorage = &noopStorage{}
		err = nil
	}
	if err != nil {
		return nil, err
	}

	// Initialize cloud storage based on provider
	switch config.CloudProvider {
	case "azure":
		cm.cloudStorage, err = NewAzureStorage(
			config.AzureStorageAccount,
			config.AzureStorageKey,
			config.AzureContainerName,
			config.CloudTimeout,
		)
	case "gcp":
		cm.cloudStorage, err = NewGCPStorage(
			config.GCPBucket,
			config.CloudTimeout,
		)
	default: // "s3" or empty
		cm.cloudStorage, err = NewCloudStorage(config.S3Bucket, config.S3Region, config.CloudTimeout)
	}
	if err != nil {
		return nil, err
	}

	return cm, nil
}

// Get retrieves a file from the cache hierarchy
func (cm *DefaultCacheManager) Get(ctx context.Context, filePath string) (*CacheEntry, error) {
	// Check if this is a chunked file
	cm.mu.RLock()
	if entry, ok := cm.entries[filePath]; ok && entry.IsChunked {
		cm.mu.RUnlock()
		result, err := cm.getChunked(ctx, entry)
		if err == nil {
			cm.metrics.RecordHit(TierNVMe)
		} else {
			cm.metrics.RecordMiss(TierNVMe)
		}
		return result, err
	}
	cm.mu.RUnlock()

	if entry, ok := cm.resolveChunkedEntry(ctx, filePath); ok {
		result, err := cm.getChunked(ctx, entry)
		if err == nil {
			cm.metrics.RecordHit(TierNVMe)
		} else {
			cm.metrics.RecordMiss(TierNVMe)
		}
		return result, err
	}

	// Check Tier 1: NVME
	if entry, err := cm.getFromTier(ctx, filePath, TierNVMe); err == nil {
		cm.metrics.RecordHit(TierNVMe)
		return entry, nil
	}
	cm.metrics.RecordMiss(TierNVMe)

	if fileSize, ok := cm.lookupFileSizeHint(filePath); ok && cm.shouldUseHybridRead(ctx, filePath, fileSize) {
		if entry, err := cm.getFromHybridRemote(ctx, filePath); err == nil {
			go cm.promoteToNVMe(context.Background(), entry)
			return entry, nil
		}
	}

	// Remote read strategy:
	// - always peer first, then cloud fallback.
	for _, tier := range cm.remoteReadOrder(filePath) {
		if entry, err := cm.getFromTier(ctx, filePath, tier); err == nil {
			cm.metrics.RecordHit(tier)
			go cm.promoteToNVMe(context.Background(), entry)
			return entry, nil
		}
		cm.metrics.RecordMiss(tier)
	}

	return nil, errors.New("file not found in any tier")
}

// GetLocal retrieves a file only from local NVMe-backed state.
// It never performs peer/cloud fan-out, which is important for serving peer RPCs
// without recursion.
func (cm *DefaultCacheManager) GetLocal(ctx context.Context, filePath string) (*CacheEntry, error) {
	cm.mu.RLock()
	entry, ok := cm.entries[filePath]
	chunked := ok && entry != nil && entry.IsChunked
	cm.mu.RUnlock()

	if chunked {
		return cm.getChunkedLocal(ctx, entry)
	}
	return cm.getFromTier(ctx, filePath, TierNVMe)
}

func (cm *DefaultCacheManager) remoteReadOrder(filePath string) []CacheTier {
	_ = filePath
	return remoteReadOrderPeerFirst
}

func (cm *DefaultCacheManager) lookupFileSizeHint(filePath string) (int64, bool) {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	if parent, ok := parentFilePathFromChunkPath(filePath); ok {
		// For chunk reads, prefer parent file size so strategy tracks the full
		// object size instead of individual chunk size.
		if size, ok := lookupEntrySizeLocked(cm.entries, parent); ok {
			return size, true
		}
	}
	if size, ok := lookupEntrySizeLocked(cm.entries, filePath); ok {
		return size, true
	}

	if parent, ok := parentFilePathFromChunkPath(filePath); ok {
		if size, ok := lookupSizeInViewLocked(cm.metadataView, parent); ok {
			return size, true
		}
	}
	if size, ok := lookupSizeInViewLocked(cm.metadataView, filePath); ok {
		return size, true
	}

	return 0, false
}

func lookupEntrySizeLocked(entries map[string]*CacheEntry, filePath string) (int64, bool) {
	entry, ok := entries[filePath]
	if !ok || entry == nil {
		return 0, false
	}
	if entry.Size < 0 {
		return 0, false
	}
	return entry.Size, true
}

func lookupSizeInViewLocked(view []*CacheEntry, filePath string) (int64, bool) {
	for _, entry := range view {
		if entry == nil || entry.FilePath != filePath {
			continue
		}
		if entry.Size < 0 {
			return 0, false
		}
		return entry.Size, true
	}
	return 0, false
}

func (cm *DefaultCacheManager) shouldUseHybridRead(ctx context.Context, filePath string, fileSize int64) bool {
	if fileSize <= 0 || cm.config.Coordinator == nil || cm.config.PeerReadThroughputBytesPerSec <= 0 {
		return false
	}

	now := time.Now()
	cm.mu.RLock()
	if hint, ok := cm.hybridHints[filePath]; ok && now.Before(hint.expiresAt) {
		cm.mu.RUnlock()
		return hint.enabled
	}
	cm.mu.RUnlock()

	locations, err := cm.config.Coordinator.GetFileLocation(ctx, filePath)
	if err != nil {
		return false
	}

	peerIDs := make(map[string]struct{})
	hasCloud := false
	for _, loc := range locations {
		if loc == nil {
			continue
		}
		if loc.StorageTier == "cloud" {
			hasCloud = true
			continue
		}
		if loc.PeerID != "" {
			peerIDs[loc.PeerID] = struct{}{}
		}
	}

	peerReplicas := len(peerIDs)
	if !hasCloud {
		// Metadata may lag async cloud persistence; probe cloud directly.
		hasCloud = cm.cloudStorage.Exists(ctx, filePath)
	}

	peerCapacity := int64(peerReplicas) * cm.config.PeerReadThroughputBytesPerSec
	enabled := hasCloud && peerReplicas > 1 && fileSize > peerCapacity

	cm.mu.Lock()
	cm.hybridHints[filePath] = hybridReadHint{
		enabled:   enabled,
		expiresAt: now.Add(cm.config.MetadataRefreshTTL),
	}
	cm.mu.Unlock()

	return enabled
}

func (cm *DefaultCacheManager) getFromHybridRemote(ctx context.Context, filePath string) (*CacheEntry, error) {
	type readResult struct {
		entry *CacheEntry
		tier  CacheTier
		err   error
	}

	readCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	resultCh := make(chan readResult, 2)
	go func() {
		entry, err := cm.getFromTier(readCtx, filePath, TierPeer)
		resultCh <- readResult{entry: entry, tier: TierPeer, err: err}
	}()
	go func() {
		// Start cloud slightly after peer to keep peer as primary source.
		timer := time.NewTimer(100 * time.Millisecond)
		defer timer.Stop()
		select {
		case <-readCtx.Done():
			return
		case <-timer.C:
		}
		entry, err := cm.getFromTier(readCtx, filePath, TierCloud)
		resultCh <- readResult{entry: entry, tier: TierCloud, err: err}
	}()

	var lastErr error
	for i := 0; i < 2; i++ {
		res := <-resultCh
		if res.err == nil {
			cm.metrics.RecordHit(res.tier)
			cancel()
			return res.entry, nil
		}
		cm.metrics.RecordMiss(res.tier)
		lastErr = res.err
	}
	if lastErr == nil {
		lastErr = errors.New("hybrid remote read failed")
	}
	return nil, lastErr
}

// getChunked reassembles a chunked file from its chunks
func (cm *DefaultCacheManager) getChunked(ctx context.Context, entry *CacheEntry) (*CacheEntry, error) {
	var allData []byte
	for i := int64(0); i < entry.NumChunks; i++ {
		chunkPath := fmt.Sprintf("%s_chunk_%d", entry.FilePath, i)
		chunkEntry, err := cm.Get(ctx, chunkPath)
		if err != nil {
			return nil, fmt.Errorf("failed to read chunk %d of %s: %v", i, entry.FilePath, err)
		}
		allData = append(allData, chunkEntry.Data...)
	}

	return &CacheEntry{
		FilePath:     entry.FilePath,
		StoragePath:  entry.StoragePath,
		Size:         int64(len(allData)),
		LastAccessed: time.Now(),
		Tier:         entry.Tier,
		Data:         allData,
		IsChunked:    true,
		NumChunks:    entry.NumChunks,
	}, nil
}

func (cm *DefaultCacheManager) getChunkedLocal(ctx context.Context, entry *CacheEntry) (*CacheEntry, error) {
	var allData []byte
	for i := int64(0); i < entry.NumChunks; i++ {
		chunkPath := fmt.Sprintf("%s_chunk_%d", entry.FilePath, i)
		chunkEntry, err := cm.getFromTier(ctx, chunkPath, TierNVMe)
		if err != nil {
			return nil, fmt.Errorf("failed to read local chunk %d of %s: %v", i, entry.FilePath, err)
		}
		allData = append(allData, chunkEntry.Data...)
	}

	return &CacheEntry{
		FilePath:     entry.FilePath,
		StoragePath:  entry.StoragePath,
		Size:         int64(len(allData)),
		LastAccessed: time.Now(),
		Tier:         TierNVMe,
		Data:         allData,
		IsChunked:    true,
		NumChunks:    entry.NumChunks,
	}, nil
}

// WriteTo streams file content to w without buffering the entire file in memory.
// For chunked files, each chunk is read and written individually (max ~ChunkSize in memory).
// For regular files, the data is written directly.
func (cm *DefaultCacheManager) WriteTo(ctx context.Context, filePath string, w io.Writer) (int64, error) {
	// Check if this is a chunked file
	cm.mu.RLock()
	entry, isChunked := cm.entries[filePath]
	if isChunked {
		isChunked = entry.IsChunked
	}
	cm.mu.RUnlock()

	if isChunked {
		var total int64
		for i := int64(0); i < entry.NumChunks; i++ {
			chunkPath := fmt.Sprintf("%s_chunk_%d", filePath, i)
			chunkEntry, err := cm.Get(ctx, chunkPath)
			if err != nil {
				return total, fmt.Errorf("failed to read chunk %d of %s: %v", i, filePath, err)
			}
			n, err := w.Write(chunkEntry.Data)
			total += int64(n)
			if err != nil {
				return total, fmt.Errorf("failed to write chunk %d: %v", i, err)
			}
		}
		cm.metrics.RecordHit(TierNVMe)
		return total, nil
	}

	// Non-chunked: use regular Get
	got, err := cm.Get(ctx, filePath)
	if err != nil {
		return 0, err
	}
	n, err := w.Write(got.Data)
	return int64(n), err
}

// Put stores a file in the cache
func (cm *DefaultCacheManager) Put(ctx context.Context, entry *CacheEntry) error {
	cm.invalidateRangeCache(entry.FilePath)

	// Compute checksum
	if len(entry.Data) > 0 && entry.Checksum == "" {
		h := sha256.Sum256(entry.Data)
		entry.Checksum = hex.EncodeToString(h[:])
	}

	// Check if file needs chunking
	if int64(len(entry.Data)) > cm.config.ChunkSize {
		return cm.putChunked(ctx, entry)
	}

	// Try to store in NVME first (with eviction if needed)
	if err := cm.putToNVMeWithEviction(ctx, entry); err == nil {
		cm.mu.Lock()
		cm.entries[entry.FilePath] = entry
		cm.mu.Unlock()

		cm.metrics.RecordWrite(int64(len(entry.Data)))

		// Deep-copy data before launching goroutine to avoid race
		dataCopy := make([]byte, len(entry.Data))
		copy(dataCopy, entry.Data)
		go cm.persistToCloud(entry.FilePath, dataCopy)
		go cm.publishFileLocation(context.Background(), entry, TierNVMe)

		return nil
	}

	// Parallel Write Strategy: Race Peer and Cloud
	type writeResult struct {
		tier CacheTier
		err  error
	}

	resultChan := make(chan writeResult, 2)
	writeCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Deep-copy data for goroutines
	dataCopy := make([]byte, len(entry.Data))
	copy(dataCopy, entry.Data)

	go func() {
		entryCopy := &CacheEntry{
			FilePath:     entry.FilePath,
			StoragePath:  entry.StoragePath,
			Size:         entry.Size,
			LastAccessed: entry.LastAccessed,
			Tier:         TierPeer,
			Data:         dataCopy,
			Checksum:     entry.Checksum,
		}
		err := cm.putToTier(writeCtx, entryCopy, TierPeer)
		resultChan <- writeResult{TierPeer, err}
	}()

	go func() {
		entryCopy := &CacheEntry{
			FilePath:     entry.FilePath,
			StoragePath:  entry.StoragePath,
			Size:         entry.Size,
			LastAccessed: entry.LastAccessed,
			Tier:         TierCloud,
			Data:         dataCopy,
			Checksum:     entry.Checksum,
		}
		err := cm.putToTier(writeCtx, entryCopy, TierCloud)
		resultChan <- writeResult{TierCloud, err}
	}()

	// Wait for first success or all failures
	for i := 0; i < 2; i++ {
		res := <-resultChan
		if res.err == nil {
			cm.mu.Lock()
			entry.Tier = res.tier
			cm.entries[entry.FilePath] = entry
			cm.mu.Unlock()
			cm.metrics.RecordWrite(int64(len(entry.Data)))
			go cm.publishFileLocation(context.Background(), entry, res.tier)
			return nil
		}
	}

	return errors.New("failed to store file in Peer or Cloud tier")
}

// PutFromReader ingests file content from r without materializing a large
// in-memory buffer. Data is chunked directly into cache storage.
func (cm *DefaultCacheManager) PutFromReader(ctx context.Context, filePath string, r io.Reader, size int64, lastAccessed time.Time) error {
	if size < 0 {
		return errors.New("invalid negative size")
	}
	if filePath == "" {
		return errors.New("empty file path")
	}
	if lastAccessed.IsZero() {
		lastAccessed = time.Now()
	}

	// Small payloads can still use the regular Put path.
	if size <= cm.config.ChunkSize {
		data := make([]byte, int(size))
		if _, err := io.ReadFull(r, data); err != nil && !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) {
			return fmt.Errorf("failed to read %s payload: %w", filePath, err)
		}
		return cm.Put(ctx, &CacheEntry{
			FilePath:     filePath,
			StoragePath:  filePath,
			Size:         size,
			LastAccessed: lastAccessed,
			Data:         data,
		})
	}

	cm.invalidateRangeCache(filePath)

	numChunks := (size + cm.config.ChunkSize - 1) / cm.config.ChunkSize
	remaining := size
	buf := make([]byte, cm.config.ChunkSize)

	for i := int64(0); i < numChunks; i++ {
		toRead := int64(len(buf))
		if remaining < toRead {
			toRead = remaining
		}

		if _, err := io.ReadFull(r, buf[:int(toRead)]); err != nil {
			return fmt.Errorf("failed to read chunk %d for %s: %w", i, filePath, err)
		}

		chunkData := make([]byte, int(toRead))
		copy(chunkData, buf[:int(toRead)])
		chunkPath := fmt.Sprintf("%s_chunk_%d", filePath, i)
		chunkEntry := &CacheEntry{
			FilePath:     chunkPath,
			StoragePath:  chunkPath,
			Size:         int64(len(chunkData)),
			LastAccessed: lastAccessed,
			Tier:         TierNVMe,
			Data:         chunkData,
		}
		if err := cm.putToNVMeWithEviction(ctx, chunkEntry); err != nil {
			return fmt.Errorf("failed to store chunk %d in NVMe for %s: %w", i, filePath, err)
		}
		chunkMeta := *chunkEntry
		chunkMeta.Data = nil
		cm.mu.Lock()
		cm.entries[chunkPath] = &chunkMeta
		cm.mu.Unlock()
		cm.metrics.RecordWrite(chunkEntry.Size)
		remaining -= toRead
	}

	meta := &CacheEntry{
		FilePath:     filePath,
		StoragePath:  filePath,
		Size:         size,
		LastAccessed: lastAccessed,
		Tier:         TierNVMe,
		IsChunked:    true,
		NumChunks:    numChunks,
	}
	cm.mu.Lock()
	cm.entries[filePath] = meta
	cm.mu.Unlock()
	go cm.publishFileLocation(context.Background(), meta, TierNVMe)
	return nil
}

// Delete removes a file from cache
func (cm *DefaultCacheManager) Delete(ctx context.Context, filePath string) error {
	cm.invalidateRangeCache(filePath)

	cm.mu.RLock()
	entry, exists := cm.entries[filePath]
	cm.mu.RUnlock()
	if !exists {
		return errors.New("file not found in cache")
	}

	storage := cm.getStorageForTier(entry.Tier)
	if err := storage.Delete(ctx, entry.StoragePath); err != nil {
		return err
	}

	cm.mu.Lock()
	if entry.Tier == TierNVMe {
		cm.nvmeUsed -= entry.Size
	}
	delete(cm.entries, filePath)
	cm.mu.Unlock()
	return nil
}

// List returns all cached entries
func (cm *DefaultCacheManager) List(ctx context.Context) ([]*CacheEntry, error) {
	return cm.list(ctx, false, false)
}

// ListFresh forces a coordinator metadata refresh and returns an error if the
// remote refresh fails (when coordinator is configured).
func (cm *DefaultCacheManager) ListFresh(ctx context.Context) ([]*CacheEntry, error) {
	return cm.list(ctx, true, true)
}

func (cm *DefaultCacheManager) list(ctx context.Context, forceRefresh, strictRefresh bool) ([]*CacheEntry, error) {
	cm.mu.RLock()
	entries := make([]*CacheEntry, 0, len(cm.entries))
	for _, entry := range cm.entries {
		entryCopy := *entry
		entries = append(entries, &entryCopy)
	}
	coord := cm.config.Coordinator
	metadataFresh := !forceRefresh && time.Since(cm.metadataAt) < cm.config.MetadataRefreshTTL
	metadataView := cm.metadataView
	cm.mu.RUnlock()

	if coord == nil {
		return entries, nil
	}

	// Reuse a fresh metadata snapshot to avoid coordinator round-trips on every stat/ls.
	if metadataFresh {
		return mergeEntries(entries, metadataView), nil
	}

	locations, err := coord.ListFileLocations(ctx, "")
	if err != nil {
		if strictRefresh {
			return nil, err
		}
		cm.logger.Printf("Metadata list fallback to local cache only: %v", err)
		return entries, nil
	}

	remote := make([]*CacheEntry, 0, len(locations))
	for _, loc := range locations {
		if loc == nil || loc.FilePath == "" {
			continue
		}
		numChunks := int64(0)
		if loc.IsChunked && cm.config.ChunkSize > 0 && loc.FileSize > 0 {
			numChunks = (loc.FileSize + cm.config.ChunkSize - 1) / cm.config.ChunkSize
		}
		remote = append(remote, &CacheEntry{
			FilePath:     loc.FilePath,
			StoragePath:  loc.StoragePath,
			Size:         loc.FileSize,
			LastAccessed: loc.LastAccessed,
			Tier:         tierFromStorageTier(loc.StorageTier),
			IsChunked:    loc.IsChunked,
			NumChunks:    numChunks,
		})
	}

	cm.mu.Lock()
	cm.metadataView = remote
	cm.metadataAt = time.Now()
	cm.mu.Unlock()

	return mergeEntries(entries, remote), nil
}

// Evict removes the least recently accessed entries from a tier to free the given number of bytes.
// If bytesNeeded is 0, it evicts entries until usage is below 90% of MaxNVMeSize.
func (cm *DefaultCacheManager) Evict(ctx context.Context, tier CacheTier) error {
	if tier != TierNVMe {
		return nil
	}

	cm.mu.Lock()
	defer cm.mu.Unlock()

	target := cm.config.MaxNVMeSize * 9 / 10 // evict down to 90%
	if cm.nvmeUsed <= target {
		return nil
	}

	// Collect NVMe entries and sort by LastAccessed (oldest first)
	type entryRef struct {
		key   string
		entry *CacheEntry
	}
	var nvmeEntries []entryRef
	for k, e := range cm.entries {
		if e.Tier == TierNVMe {
			nvmeEntries = append(nvmeEntries, entryRef{k, e})
		}
	}
	sort.Slice(nvmeEntries, func(i, j int) bool {
		return nvmeEntries[i].entry.LastAccessed.Before(nvmeEntries[j].entry.LastAccessed)
	})

	for _, ref := range nvmeEntries {
		if cm.nvmeUsed <= target {
			break
		}
		storage := cm.getStorageForTier(TierNVMe)
		if err := storage.Delete(ctx, ref.entry.FilePath); err != nil {
			cm.logger.Printf("Eviction delete failed for %s: %v", ref.key, err)
			continue
		}
		cm.nvmeUsed -= ref.entry.Size
		delete(cm.entries, ref.key)
		cm.metrics.RecordEviction()
		cm.logger.Printf("Evicted %s (%d bytes)", ref.key, ref.entry.Size)
	}

	return nil
}

// persistToCloud asynchronously writes data to cloud storage for durability with retry.
// Data must be pre-copied by the caller to avoid races.
func (cm *DefaultCacheManager) persistToCloud(filePath string, data []byte) {
	var lastErr error
	for attempt := 0; attempt < cm.config.CloudRetryCount; attempt++ {
		ctx, cancel := context.WithTimeout(context.Background(), cm.config.CloudTimeout)
		err := cm.cloudStorage.Write(ctx, filePath, data)
		cancel()

		if err == nil {
			cm.logger.Printf("Persisted %s (%d bytes) to cloud storage", filePath, len(data))
			return
		}

		lastErr = err
		wait := cm.config.CloudRetryBaseWait * (1 << uint(attempt))
		cm.logger.Printf("Cloud persist attempt %d/%d failed for %s: %v (retrying in %v)",
			attempt+1, cm.config.CloudRetryCount, filePath, err, wait)
		time.Sleep(wait)
	}

	cm.logger.Printf("CRITICAL: All %d cloud persist attempts failed for %s: %v",
		cm.config.CloudRetryCount, filePath, lastErr)
}

// putToNVMeWithEviction tries NVMe write, evicting if necessary
func (cm *DefaultCacheManager) putToNVMeWithEviction(ctx context.Context, entry *CacheEntry) error {
	cm.mu.RLock()
	wouldExceed := cm.nvmeUsed+entry.Size > cm.config.MaxNVMeSize
	cm.mu.RUnlock()

	if wouldExceed {
		cm.Evict(ctx, TierNVMe)
		// Re-check after eviction
		cm.mu.RLock()
		stillExceed := cm.nvmeUsed+entry.Size > cm.config.MaxNVMeSize
		cm.mu.RUnlock()
		if stillExceed {
			return errors.New("NVMe storage full after eviction")
		}
	}

	if err := cm.putToTier(ctx, entry, TierNVMe); err != nil {
		return err
	}

	cm.mu.Lock()
	cm.nvmeUsed += entry.Size
	cm.mu.Unlock()
	return nil
}

func (cm *DefaultCacheManager) publishFileLocation(ctx context.Context, entry *CacheEntry, tier CacheTier) {
	if cm.config.Coordinator == nil || cm.config.LocalPeerID == "" || entry == nil {
		return
	}
	location := &coordinator.FileLocation{
		FilePath:     entry.FilePath,
		PeerID:       cm.config.LocalPeerID,
		StorageTier:  cacheTierToStorageTier(tier),
		StoragePath:  entry.FilePath,
		FileSize:     entry.Size,
		LastAccessed: time.Now(),
		IsChunked:    entry.IsChunked,
	}
	if err := cm.config.Coordinator.UpdateFileLocation(ctx, location); err != nil {
		cm.logger.Printf("Failed to publish metadata for %s: %v", entry.FilePath, err)
	}
}

func cacheTierToStorageTier(tier CacheTier) string {
	switch tier {
	case TierNVMe:
		return "nvme"
	case TierPeer:
		return "peer"
	default:
		return "cloud"
	}
}

// Helper methods
func (cm *DefaultCacheManager) getFromTier(ctx context.Context, filePath string, tier CacheTier) (*CacheEntry, error) {
	storage := cm.getStorageForTier(tier)

	if !storage.Exists(ctx, filePath) {
		return nil, errors.New("file not found in tier")
	}

	data, err := storage.Read(ctx, filePath)
	if err != nil {
		return nil, err
	}

	size, err := storage.Size(ctx, filePath)
	if err != nil {
		return nil, err
	}

	entry := &CacheEntry{
		FilePath:     filePath,
		StoragePath:  filePath,
		Size:         size,
		LastAccessed: time.Now(),
		Tier:         tier,
		Data:         data,
	}

	// Verify checksum on NVMe reads if sidecar exists
	if tier == TierNVMe {
		if checksumData, err := storage.Read(ctx, filePath+".sha256"); err == nil {
			storedChecksum := string(checksumData)
			h := sha256.Sum256(data)
			actualChecksum := hex.EncodeToString(h[:])
			if storedChecksum != actualChecksum {
				cm.logger.Printf("Checksum mismatch for %s: stored=%s actual=%s", filePath, storedChecksum, actualChecksum)
				return nil, fmt.Errorf("checksum mismatch for %s", filePath)
			}
			entry.Checksum = storedChecksum
		}
	}

	return entry, nil
}

func (cm *DefaultCacheManager) putToTier(ctx context.Context, entry *CacheEntry, tier CacheTier) error {
	storage := cm.getStorageForTier(tier)
	if err := storage.Write(ctx, entry.FilePath, entry.Data); err != nil {
		return err
	}

	// Write checksum sidecar for NVMe
	if tier == TierNVMe && entry.Checksum != "" {
		if err := storage.Write(ctx, entry.FilePath+".sha256", []byte(entry.Checksum)); err != nil {
			cm.logger.Printf("Failed to write checksum sidecar for %s: %v", entry.FilePath, err)
		}
	}

	return nil
}

func (cm *DefaultCacheManager) getStorageForTier(tier CacheTier) TierStorage {
	switch tier {
	case TierNVMe:
		return cm.nvmeStorage
	case TierPeer:
		return cm.peerStorage
	case TierCloud:
		return cm.cloudStorage
	default:
		return cm.nvmeStorage
	}
}

func tierFromStorageTier(storageTier string) CacheTier {
	switch storageTier {
	case "nvme":
		return TierNVMe
	case "peer":
		return TierPeer
	default:
		return TierCloud
	}
}

// mergeEntries merges local and remote metadata views by FilePath.
// Local entries take precedence because they include freshest in-memory state.
func mergeEntries(local []*CacheEntry, remote []*CacheEntry) []*CacheEntry {
	byPath := make(map[string]*CacheEntry, len(local)+len(remote))
	for _, entry := range remote {
		if entry == nil || entry.FilePath == "" {
			continue
		}
		byPath[entry.FilePath] = entry
	}
	for _, entry := range local {
		if entry == nil || entry.FilePath == "" {
			continue
		}
		byPath[entry.FilePath] = entry
	}

	merged := make([]*CacheEntry, 0, len(byPath))
	for _, entry := range byPath {
		merged = append(merged, entry)
	}
	return merged
}

func (cm *DefaultCacheManager) promoteToNVMe(ctx context.Context, entry *CacheEntry) {
	newEntry := &CacheEntry{
		FilePath:     entry.FilePath,
		StoragePath:  entry.StoragePath,
		Size:         entry.Size,
		LastAccessed: time.Now(),
		Tier:         TierNVMe,
		Data:         entry.Data,
	}

	if err := cm.putToNVMeWithEviction(ctx, newEntry); err != nil {
		cm.logger.Printf("Promotion to NVMe failed for %s: %v", entry.FilePath, err)
		return
	}

	cm.mu.Lock()
	cm.entries[entry.FilePath] = newEntry
	cm.mu.Unlock()
}

func (cm *DefaultCacheManager) putChunked(ctx context.Context, entry *CacheEntry) error {
	dataLen := int64(len(entry.Data))
	numChunks := (dataLen + cm.config.ChunkSize - 1) / cm.config.ChunkSize

	// Write chunks sequentially to avoid unbounded goroutine/data fan-out
	// that can spike memory on large files (e.g., 1GiB+ writes).
	for i := int64(0); i < numChunks; i++ {
		start := i * cm.config.ChunkSize
		end := start + cm.config.ChunkSize
		if end > dataLen {
			end = dataLen
		}

		chunkData := entry.Data[start:end]
		chunkPath := fmt.Sprintf("%s_chunk_%d", entry.FilePath, i)
		chunkEntry := &CacheEntry{
			FilePath:     chunkPath,
			StoragePath:  chunkPath,
			Size:         int64(len(chunkData)),
			LastAccessed: time.Now(),
			Data:         chunkData,
		}
		if err := cm.Put(ctx, chunkEntry); err != nil {
			return fmt.Errorf("failed to upload chunk: %v", err)
		}
	}

	entry.IsChunked = true
	entry.NumChunks = numChunks
	entry.Size = dataLen
	// Drop full payload after chunking to avoid retaining large buffers in memory.
	entry.Data = nil
	entry.Checksum = ""

	cm.mu.Lock()
	cm.entries[entry.FilePath] = entry
	cm.mu.Unlock()
	go cm.publishFileLocation(context.Background(), entry, TierNVMe)

	return nil
}

// ReadRange reads a byte range from a file without loading an entire chunked file
// into memory.
func (cm *DefaultCacheManager) ReadRange(ctx context.Context, filePath string, offset int64, size int) ([]byte, error) {
	started := time.Now()
	if size <= 0 {
		return []byte{}, nil
	}

	cm.mu.RLock()
	entry, hasMeta := cm.entries[filePath]
	isChunked := hasMeta && entry.IsChunked
	chunkSize := cm.config.ChunkSize
	cm.mu.RUnlock()
	if !isChunked {
		if resolved, ok := cm.resolveChunkedEntry(ctx, filePath); ok {
			entry = resolved
			hasMeta = true
			isChunked = true
		}
	}

	if !isChunked {
		got, err := cm.Get(ctx, filePath)
		if err != nil {
			return nil, err
		}
		if offset >= int64(len(got.Data)) {
			return []byte{}, nil
		}
		end := offset + int64(size)
		if end > int64(len(got.Data)) {
			end = int64(len(got.Data))
		}
		out := make([]byte, end-offset)
		copy(out, got.Data[offset:end])
		dur := time.Since(started)
		if dur >= slowChunkReadThreshold {
			cm.logger.Printf("ReadRange slow non-chunked path=%s offset=%d size=%d dur=%v", filePath, offset, size, dur)
		}
		return out, nil
	}

	totalSize := entry.Size
	if totalSize < 0 {
		totalSize = 0
	}
	if offset >= totalSize {
		return []byte{}, nil
	}
	end := offset + int64(size)
	if end > totalSize {
		end = totalSize
	}

	startChunk := offset / chunkSize
	endChunk := (end - 1) / chunkSize
	chunkCount := int(endChunk-startChunk) + 1
	chunks := make([][]byte, chunkCount)
	remoteOrder := cm.remoteReadOrder(filePath)
	hybridRead := cm.shouldUseHybridRead(ctx, filePath, totalSize)
	if !hybridRead && entry.NumChunks > 0 {
		hybridRead = cm.shouldUseHybridRead(ctx, fmt.Sprintf("%s_chunk_%d", filePath, 0), totalSize)
	}

	workerCount := cm.config.ParallelRangeReads
	if workerCount < 1 {
		workerCount = 1
	}
	if workerCount > chunkCount {
		workerCount = chunkCount
	}

	if chunkCount == 1 {
		data, err := cm.readChunkData(ctx, filePath, startChunk, remoteOrder, hybridRead)
		if err != nil {
			return nil, err
		}
		chunks[0] = data
	} else {
		jobs := make(chan int, chunkCount)
		errCh := make(chan error, 1)
		var wg sync.WaitGroup

		for w := 0; w < workerCount; w++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for pos := range jobs {
					chunkIndex := startChunk + int64(pos)
					data, err := cm.readChunkData(ctx, filePath, chunkIndex, remoteOrder, hybridRead)
					if err != nil {
						select {
						case errCh <- err:
						default:
						}
						return
					}
					chunks[pos] = data
				}
			}()
		}

		for pos := 0; pos < chunkCount; pos++ {
			jobs <- pos
		}
		close(jobs)
		wg.Wait()

		select {
		case err := <-errCh:
			cm.logger.Printf("ReadRange failed path=%s offset=%d size=%d startChunk=%d endChunk=%d err=%v",
				filePath, offset, size, startChunk, endChunk, err)
			return nil, err
		default:
		}
	}

	// Opportunistic prefetch for nearby sequential reads.
	for i := 1; i <= cm.config.RangePrefetchChunks; i++ {
		prefetchChunk := endChunk + int64(i)
		if prefetchChunk >= entry.NumChunks {
			break
		}
		go func(chunkIndex int64) {
			_, _ = cm.readChunkData(context.Background(), filePath, chunkIndex, remoteOrder, hybridRead)
		}(prefetchChunk)
	}

	out := make([]byte, 0, end-offset)
	for pos, chunkData := range chunks {
		chunkIndex := startChunk + int64(pos)
		chunkOffset := chunkIndex * chunkSize
		from := int64(0)
		if offset > chunkOffset {
			from = offset - chunkOffset
		}

		to := int64(len(chunkData))
		chunkEnd := chunkOffset + int64(len(chunkData))
		if end < chunkEnd {
			to = end - chunkOffset
		}

		if from < 0 {
			from = 0
		}
		if to < from {
			to = from
		}
		if to > int64(len(chunkData)) {
			to = int64(len(chunkData))
		}
		out = append(out, chunkData[from:to]...)
	}

	dur := time.Since(started)
	if dur >= slowChunkReadThreshold {
		cm.logger.Printf("ReadRange slow chunked path=%s offset=%d size=%d chunks=%d workers=%d hybrid=%t dur=%v",
			filePath, offset, size, chunkCount, workerCount, hybridRead, dur)
	}
	return out, nil
}

func (cm *DefaultCacheManager) resolveChunkedEntry(ctx context.Context, filePath string) (*CacheEntry, bool) {
	cm.mu.RLock()
	if entry, ok := cm.entries[filePath]; ok && entry != nil && entry.IsChunked {
		cm.mu.RUnlock()
		return entry, true
	}
	cm.mu.RUnlock()

	// First check cached metadata view.
	cm.mu.RLock()
	for _, entry := range cm.metadataView {
		if entry == nil || entry.FilePath != filePath || !entry.IsChunked {
			continue
		}
		numChunks := entry.NumChunks
		if numChunks <= 0 && cm.config.ChunkSize > 0 && entry.Size > 0 {
			numChunks = (entry.Size + cm.config.ChunkSize - 1) / cm.config.ChunkSize
		}
		resolved := &CacheEntry{
			FilePath:     filePath,
			StoragePath:  entry.StoragePath,
			Size:         entry.Size,
			LastAccessed: entry.LastAccessed,
			Tier:         tierFromStorageTier(cacheTierToStorageTier(entry.Tier)),
			IsChunked:    true,
			NumChunks:    numChunks,
		}
		cm.mu.RUnlock()
		cm.mu.Lock()
		cm.entries[filePath] = resolved
		cm.mu.Unlock()
		return resolved, true
	}
	cm.mu.RUnlock()

	if cm.config.Coordinator == nil {
		return nil, false
	}

	callCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	locations, err := cm.config.Coordinator.GetFileLocation(callCtx, filePath)
	if err != nil {
		return nil, false
	}

	for _, loc := range locations {
		if loc == nil || !loc.IsChunked {
			continue
		}
		numChunks := int64(0)
		if cm.config.ChunkSize > 0 && loc.FileSize > 0 {
			numChunks = (loc.FileSize + cm.config.ChunkSize - 1) / cm.config.ChunkSize
		}
		resolved := &CacheEntry{
			FilePath:     filePath,
			StoragePath:  loc.StoragePath,
			Size:         loc.FileSize,
			LastAccessed: loc.LastAccessed,
			Tier:         tierFromStorageTier(loc.StorageTier),
			IsChunked:    true,
			NumChunks:    numChunks,
		}
		cm.mu.Lock()
		cm.entries[filePath] = resolved
		cm.mu.Unlock()
		return resolved, true
	}
	return nil, false
}

func (cm *DefaultCacheManager) readChunkData(ctx context.Context, filePath string, chunkIndex int64, remoteOrder []CacheTier, hybridRead bool) ([]byte, error) {
	chunkStart := time.Now()
	chunkData, ok := cm.getChunkFromRangeCache(filePath, chunkIndex)
	if ok {
		return chunkData, nil
	}

	chunkPath := fmt.Sprintf("%s_chunk_%d", filePath, chunkIndex)

	tierStart := time.Now()
	if chunkEntry, err := cm.getFromTier(ctx, chunkPath, TierNVMe); err == nil {
		cm.metrics.RecordHit(TierNVMe)
		chunkData = chunkEntry.Data
		cm.setChunkInRangeCache(filePath, chunkIndex, chunkData)
		cm.traceChunkAttempt(chunkPath, chunkIndex, TierNVMe, tierStart, nil)
		cm.traceChunkTotal(chunkPath, chunkIndex, chunkStart, nil)
		return chunkData, nil
	} else {
		cm.traceChunkAttempt(chunkPath, chunkIndex, TierNVMe, tierStart, err)
	}
	cm.metrics.RecordMiss(TierNVMe)

	primary := TierPeer
	secondary := TierCloud
	if len(remoteOrder) >= 2 {
		primary, secondary = remoteOrder[0], remoteOrder[1]
	}
	if hybridRead {
		hybridStart := time.Now()
		if chunkEntry, tier, err := cm.readChunkHybridPreferPeer(ctx, chunkPath, primary, secondary); err == nil {
			cm.metrics.RecordHit(tier)
			chunkData = chunkEntry.Data
			cm.setChunkInRangeCache(filePath, chunkIndex, chunkData)
			cm.traceChunkAttempt(chunkPath, chunkIndex, tier, hybridStart, nil)
			cm.traceChunkTotal(chunkPath, chunkIndex, chunkStart, nil)
			return chunkData, nil
		} else {
			cm.traceChunkAttempt(chunkPath, chunkIndex, primary, hybridStart, err)
		}
		// If hybrid path fails, continue with strict ordered fallback below.
	}

	tierStart = time.Now()
	if chunkEntry, err := cm.getFromTier(ctx, chunkPath, primary); err == nil {
		cm.metrics.RecordHit(primary)
		chunkData = chunkEntry.Data
		cm.setChunkInRangeCache(filePath, chunkIndex, chunkData)
		cm.traceChunkAttempt(chunkPath, chunkIndex, primary, tierStart, nil)
		cm.traceChunkTotal(chunkPath, chunkIndex, chunkStart, nil)
		return chunkData, nil
	} else {
		cm.traceChunkAttempt(chunkPath, chunkIndex, primary, tierStart, err)
	}
	cm.metrics.RecordMiss(primary)

	tierStart = time.Now()
	if chunkEntry, err := cm.getFromTier(ctx, chunkPath, secondary); err == nil {
		cm.metrics.RecordHit(secondary)
		chunkData = chunkEntry.Data
		cm.setChunkInRangeCache(filePath, chunkIndex, chunkData)
		cm.traceChunkAttempt(chunkPath, chunkIndex, secondary, tierStart, nil)
		cm.traceChunkTotal(chunkPath, chunkIndex, chunkStart, nil)
		return chunkData, nil
	} else {
		cm.traceChunkAttempt(chunkPath, chunkIndex, secondary, tierStart, err)
	}
	cm.metrics.RecordMiss(secondary)

	err := fmt.Errorf("failed to read chunk %d for %s from remote tiers", chunkIndex, filePath)
	cm.traceChunkTotal(chunkPath, chunkIndex, chunkStart, err)
	return nil, err
}

func (cm *DefaultCacheManager) traceChunkAttempt(chunkPath string, chunkIndex int64, tier CacheTier, started time.Time, err error) {
	dur := time.Since(started)
	if err != nil {
		// Cold-range reads legitimately miss local NVMe for each chunk before remote
		// fallback. Suppress expected miss logs to avoid high-volume logging overhead.
		if isExpectedTierMiss(err) {
			return
		}
		cm.logger.Printf("Chunk read attempt failed chunk=%s idx=%d tier=%s dur=%v err=%v",
			chunkPath, chunkIndex, cacheTierToStorageTier(tier), dur, err)
		return
	}
	if dur >= slowChunkReadThreshold {
		cm.logger.Printf("Chunk read attempt slow chunk=%s idx=%d tier=%s dur=%v",
			chunkPath, chunkIndex, cacheTierToStorageTier(tier), dur)
	}
}

func (cm *DefaultCacheManager) traceChunkTotal(chunkPath string, chunkIndex int64, started time.Time, err error) {
	dur := time.Since(started)
	if err != nil {
		cm.logger.Printf("Chunk read failed chunk=%s idx=%d totalDur=%v err=%v", chunkPath, chunkIndex, dur, err)
		return
	}
	if dur >= slowChunkReadThreshold {
		cm.logger.Printf("Chunk read slow chunk=%s idx=%d totalDur=%v", chunkPath, chunkIndex, dur)
	}
}

func isExpectedTierMiss(err error) bool {
	return err != nil && strings.Contains(err.Error(), "file not found in tier")
}

func (cm *DefaultCacheManager) readChunkHybridPreferPeer(ctx context.Context, chunkPath string, primary, secondary CacheTier) (*CacheEntry, CacheTier, error) {
	type readResult struct {
		entry *CacheEntry
		tier  CacheTier
		err   error
	}

	readCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	resultCh := make(chan readResult, 2)
	go func() {
		entry, err := cm.getFromTier(readCtx, chunkPath, primary)
		resultCh <- readResult{entry: entry, tier: primary, err: err}
	}()
	go func() {
		timer := time.NewTimer(100 * time.Millisecond)
		defer timer.Stop()
		select {
		case <-readCtx.Done():
			return
		case <-timer.C:
		}
		entry, err := cm.getFromTier(readCtx, chunkPath, secondary)
		resultCh <- readResult{entry: entry, tier: secondary, err: err}
	}()

	var lastErr error
	for i := 0; i < 2; i++ {
		res := <-resultCh
		if res.err == nil {
			cancel()
			return res.entry, res.tier, nil
		}
		lastErr = res.err
	}
	if lastErr == nil {
		lastErr = errors.New("hybrid chunk read failed")
	}
	return nil, 0, lastErr
}

func (cm *DefaultCacheManager) getChunkFromRangeCache(filePath string, chunkIndex int64) ([]byte, bool) {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	fileCache, ok := cm.rangeChunks[filePath]
	if !ok || fileCache == nil {
		return nil, false
	}
	data, ok := fileCache.chunks[chunkIndex]
	if !ok {
		return nil, false
	}
	return data, true
}

func (cm *DefaultCacheManager) setChunkInRangeCache(filePath string, chunkIndex int64, data []byte) {
	if len(data) == 0 {
		return
	}

	cm.mu.Lock()
	fileCache, ok := cm.rangeChunks[filePath]
	if !ok || fileCache == nil {
		fileCache = &chunkFileCache{
			chunks: make(map[int64][]byte),
			order:  make([]int64, 0, cm.config.RangeChunkCacheSize),
		}
		cm.rangeChunks[filePath] = fileCache
	}
	if _, exists := fileCache.chunks[chunkIndex]; !exists {
		fileCache.order = append(fileCache.order, chunkIndex)
	}
	fileCache.chunks[chunkIndex] = data

	for len(fileCache.order) > cm.config.RangeChunkCacheSize {
		evictChunk := fileCache.order[0]
		fileCache.order = fileCache.order[1:]
		delete(fileCache.chunks, evictChunk)
	}
	cm.mu.Unlock()
}

func (cm *DefaultCacheManager) invalidateRangeCache(filePath string) {
	cm.mu.Lock()
	delete(cm.rangeChunks, filePath)
	delete(cm.hybridHints, filePath)
	if parent, ok := parentFilePathFromChunkPath(filePath); ok {
		delete(cm.rangeChunks, parent)
		delete(cm.hybridHints, parent)
	}
	cm.mu.Unlock()
}

func parentFilePathFromChunkPath(path string) (string, bool) {
	idx := strings.LastIndex(path, "_chunk_")
	if idx <= 0 {
		return "", false
	}
	if _, err := strconv.ParseInt(path[idx+len("_chunk_"):], 10, 64); err != nil {
		return "", false
	}
	return path[:idx], true
}

// Stats returns current NVMe usage and capacity
func (cm *DefaultCacheManager) Stats() (used, capacity int64) {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	return cm.nvmeUsed, cm.config.MaxNVMeSize
}

// GetMetrics returns the cache metrics for reporting
func (cm *DefaultCacheManager) GetMetrics() map[string]interface{} {
	return cm.metrics.Snapshot()
}

// CacheMetrics tracks cache performance counters using atomic operations
type CacheMetrics struct {
	NVMeHits      atomic.Int64
	NVMeMisses    atomic.Int64
	PeerHits      atomic.Int64
	PeerMisses    atomic.Int64
	CloudHits     atomic.Int64
	CloudMisses   atomic.Int64
	WriteCount    atomic.Int64
	WriteBytes    atomic.Int64
	EvictionCount atomic.Int64
}

// NewCacheMetrics creates a new CacheMetrics
func NewCacheMetrics() *CacheMetrics {
	return &CacheMetrics{}
}

// RecordHit records a cache hit for the given tier
func (m *CacheMetrics) RecordHit(tier CacheTier) {
	switch tier {
	case TierNVMe:
		m.NVMeHits.Add(1)
	case TierPeer:
		m.PeerHits.Add(1)
	case TierCloud:
		m.CloudHits.Add(1)
	}
}

// RecordMiss records a cache miss for the given tier
func (m *CacheMetrics) RecordMiss(tier CacheTier) {
	switch tier {
	case TierNVMe:
		m.NVMeMisses.Add(1)
	case TierPeer:
		m.PeerMisses.Add(1)
	case TierCloud:
		m.CloudMisses.Add(1)
	}
}

// RecordWrite records a write operation
func (m *CacheMetrics) RecordWrite(bytes int64) {
	m.WriteCount.Add(1)
	m.WriteBytes.Add(bytes)
}

// RecordEviction records an eviction
func (m *CacheMetrics) RecordEviction() {
	m.EvictionCount.Add(1)
}

// Snapshot returns a point-in-time snapshot of all metrics
func (m *CacheMetrics) Snapshot() map[string]interface{} {
	return map[string]interface{}{
		"nvme_hits":      m.NVMeHits.Load(),
		"nvme_misses":    m.NVMeMisses.Load(),
		"peer_hits":      m.PeerHits.Load(),
		"peer_misses":    m.PeerMisses.Load(),
		"cloud_hits":     m.CloudHits.Load(),
		"cloud_misses":   m.CloudMisses.Load(),
		"write_count":    m.WriteCount.Load(),
		"write_bytes":    m.WriteBytes.Load(),
		"eviction_count": m.EvictionCount.Load(),
	}
}

// noopStorage is a TierStorage that always reports empty. Used as a fallback
// when no coordinator is configured.
type noopStorage struct{}

func (n *noopStorage) Read(ctx context.Context, path string) ([]byte, error) {
	return nil, errors.New("no peer storage configured")
}
func (n *noopStorage) Write(ctx context.Context, path string, data []byte) error {
	return errors.New("no peer storage configured")
}
func (n *noopStorage) Delete(ctx context.Context, path string) error { return nil }
func (n *noopStorage) Exists(ctx context.Context, path string) bool  { return false }
func (n *noopStorage) Size(ctx context.Context, path string) (int64, error) {
	return 0, errors.New("no peer storage configured")
}

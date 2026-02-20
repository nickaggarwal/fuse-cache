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
}

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

	cm := &DefaultCacheManager{
		config:  config,
		entries: make(map[string]*CacheEntry),
		logger:  log.New(log.Writer(), "[CACHE] ", log.LstdFlags),
		metrics: NewCacheMetrics(),
	}

	// Initialize storage tiers
	var err error
	cm.nvmeStorage, err = NewNVMeStorage(config.NVMePath)
	if err != nil {
		return nil, err
	}

	if config.Coordinator != nil {
		cm.peerStorage, err = NewPeerStorage(config.Coordinator, config.PeerTimeout)
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

	// Check Tier 1: NVME
	if entry, err := cm.getFromTier(ctx, filePath, TierNVMe); err == nil {
		cm.metrics.RecordHit(TierNVMe)
		return entry, nil
	}
	cm.metrics.RecordMiss(TierNVMe)

	// Check Tier 2: Peers
	if entry, err := cm.getFromTier(ctx, filePath, TierPeer); err == nil {
		cm.metrics.RecordHit(TierPeer)
		go cm.promoteToNVMe(context.Background(), entry)
		return entry, nil
	}
	cm.metrics.RecordMiss(TierPeer)

	// Check Tier 3: Cloud
	if entry, err := cm.getFromTier(ctx, filePath, TierCloud); err == nil {
		cm.metrics.RecordHit(TierCloud)
		go cm.promoteToNVMe(context.Background(), entry)
		return entry, nil
	}
	cm.metrics.RecordMiss(TierCloud)

	return nil, errors.New("file not found in any tier")
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

// Delete removes a file from cache
func (cm *DefaultCacheManager) Delete(ctx context.Context, filePath string) error {
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
	cm.mu.RLock()
	entries := make([]*CacheEntry, 0, len(cm.entries))
	for _, entry := range cm.entries {
		entryCopy := *entry
		entries = append(entries, &entryCopy)
	}
	coord := cm.config.Coordinator
	metadataFresh := time.Since(cm.metadataAt) < cm.config.MetadataRefreshTTL
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
		cm.logger.Printf("Metadata list fallback to local cache only: %v", err)
		return entries, nil
	}

	remote := make([]*CacheEntry, 0, len(locations))
	for _, loc := range locations {
		if loc == nil || loc.FilePath == "" {
			continue
		}
		remote = append(remote, &CacheEntry{
			FilePath:     loc.FilePath,
			StoragePath:  loc.StoragePath,
			Size:         loc.FileSize,
			LastAccessed: loc.LastAccessed,
			Tier:         tierFromStorageTier(loc.StorageTier),
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

	var wg sync.WaitGroup
	errChan := make(chan error, numChunks)

	for i := int64(0); i < numChunks; i++ {
		start := i * cm.config.ChunkSize
		end := start + cm.config.ChunkSize
		if end > dataLen {
			end = dataLen
		}

		chunkData := entry.Data[start:end]
		chunkPath := fmt.Sprintf("%s_chunk_%d", entry.FilePath, i)

		wg.Add(1)
		go func(path string, data []byte) {
			defer wg.Done()

			chunkEntry := &CacheEntry{
				FilePath:     path,
				StoragePath:  path,
				Size:         int64(len(data)),
				LastAccessed: time.Now(),
				Data:         data,
			}

			if err := cm.Put(ctx, chunkEntry); err != nil {
				errChan <- err
			}
		}(chunkPath, chunkData)
	}

	wg.Wait()
	close(errChan)

	for err := range errChan {
		if err != nil {
			return fmt.Errorf("failed to upload chunk: %v", err)
		}
	}

	entry.IsChunked = true
	entry.NumChunks = numChunks

	cm.mu.Lock()
	cm.entries[entry.FilePath] = entry
	cm.mu.Unlock()
	go cm.publishFileLocation(context.Background(), entry, TierNVMe)

	return nil
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

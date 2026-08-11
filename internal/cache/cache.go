package cache

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"runtime/metrics"
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
	TierUnknown CacheTier = -1
	TierNVMe    CacheTier = iota
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
	// ChunkSize is the stride the file was actually written with. Readers must
	// use this rather than their own -chunk-size flag: the two differ whenever
	// a file outlives a config change, and computing chunk offsets from the
	// wrong stride yields short reads, truncated whole-file responses, and
	// out-of-range slices. Zero means "unknown" (legacy metadata) — callers
	// fall back to the configured size via effectiveChunkSize.
	ChunkSize int64
	Checksum  string
	// PersistedToCloud is set once the background write-through confirms the
	// object is durable in the cloud tier. Eviction must not delete NVMe
	// bytes whose only copy is local — that turns cache pressure into data
	// loss.
	PersistedToCloud bool
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
	// Shutdown waits for all in-flight background goroutines to complete.
	Shutdown()
}

// ExtendedCacheManager is a superset of CacheManager that exposes streaming
// put/read, staged writes, metrics, and range-cache snapshot. All methods are
// implemented by DefaultCacheManager; callers that need only the basic
// interface should use CacheManager.
type ExtendedCacheManager interface {
	CacheManager
	ReadRange(ctx context.Context, filePath string, offset int64, size int) ([]byte, error)
	PutFromReader(ctx context.Context, filePath string, r io.Reader, size int64, lastAccessed time.Time) error
	CreateWriteFile(ctx context.Context, filePath string) (*os.File, string, error)
	PutFromFile(ctx context.Context, filePath string, stagedPath string, size int64, lastAccessed time.Time) error
	GetLocal(ctx context.Context, filePath string) (*CacheEntry, error)
	GetMetrics() map[string]interface{}
	RangeCacheSnapshot() RangeCacheSnapshot
	// Rename moves a file's metadata and local bytes without reading the
	// contents; the cloud copy is re-pointed server-side.
	Rename(ctx context.Context, oldPath, newPath string) error
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
	// S3 transfer tuning
	S3DownloadConcurrency int
	S3DownloadPartSize    int64
	S3UploadConcurrency   int
	S3UploadPartSize      int64
	S3Endpoint            string
	S3ForcePathStyle      bool

	// Azure config
	AzureStorageAccount string
	AzureStorageKey     string
	AzureContainerName  string
	// Azure parallel download tuning
	AzureDownloadConcurrency      int
	AzureDownloadBlockSizeBytes   int64
	AzureParallelDownloadMinBytes int64

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

	// PeerReadSortByNetwork enables network-speed/latency based ordering of
	// peer candidates before attempting reads.
	PeerReadSortByNetwork bool

	// PeerReadParallelFanout enables parallel multi-peer reads for chunk paths.
	PeerReadParallelFanout bool

	// RangePrefetchChunks controls how many chunks after the current read range
	// are prefetched opportunistically.
	RangePrefetchChunks int

	// RangeChunkCacheSize is the maximum number of chunks cached per file for
	// range reads.
	RangeChunkCacheSize int

	// RangeChunkCacheMaxBytes bounds the per-file range cache by bytes. When
	// unset, the cache is capped to a conservative default to avoid large-read
	// OOMs on chunk-heavy workloads.
	RangeChunkCacheMaxBytes int64

	// RangeCacheGlobalMaxBytes bounds the range cache across ALL files. Per-file
	// budgets alone let N sequential readers pin N full per-file caches long
	// after the reads finish (observed: 5 files x 1GiB retained on a 16GB node).
	// 0 derives min(4 x per-file budget, 25% of system RAM); negative disables.
	RangeCacheGlobalMaxBytes int64

	// RangeCacheIdleExpiry drops a file's whole range cache after this long
	// without a read. 0 uses the default; negative disables idle expiry.
	RangeCacheIdleExpiry time.Duration

	// ChunkCompletionDisabled turns off the post-read completion pass that
	// fills unlanded chunks and assembles whole parent files on NVMe.
	ChunkCompletionDisabled bool

	// WatermarkEvictDisabled turns off the background pressure evictor
	// (reactive write-path eviction still applies).
	WatermarkEvictDisabled bool

	// MaxThroughputBytesPerSec caps the bandwidth the adaptive profile will
	// provision pipelines for (0 = uncapped). On shared GPU nodes the cache
	// must not starve training traffic; this is the ceiling mirror of the
	// configured floors (mountpoint-s3 exposes the same knob as
	// --maximum-throughput-gbps).
	MaxThroughputBytesPerSec int64

	// RangePrefetchMaxBytes bounds bytes reserved for prefetch that are not yet
	// part of the settled range cache.
	RangePrefetchMaxBytes int64

	// RangePrefetchMaxChunks caps the adaptive readahead window. Sequential
	// streams double the window per consecutive read starting from
	// RangePrefetchChunks up to this value; random access resets to the base.
	RangePrefetchMaxChunks int

	// PeerReadThroughputBytesPerSec is the assumed per-peer read throughput used
	// to decide when to enable hybrid peer+cloud reads for large files.
	PeerReadThroughputBytesPerSec int64

	// HybridCloudHedgeDelay controls how long to wait before launching cloud
	// fallback when peer is primary in hybrid reads.
	HybridCloudHedgeDelay time.Duration

	// HybridMaxSecondaryInflight bounds concurrent hedged secondary reads.
	HybridMaxSecondaryInflight int

	// HybridAlwaysMinSize enables peer+cloud hedging for files at or above this
	// size, even if optimistic peer throughput estimates suggest peer-only.
	HybridAlwaysMinSize int64

	// HybridStripeMinSize enables deterministic peer/cloud chunk striping for
	// large files at or above this size.
	HybridStripeMinSize int64

	// AdaptiveRemoteRead enables measured peer-vs-cloud tier selection and
	// hedging based on observed per-tier latency and success rate, instead of a
	// static peer-first order and purely size-based hedging. Enabled by default.
	AdaptiveRemoteRead bool

	// PeerRawTransport prefers a plain-HTTP bulk-read transport (with sendfile
	// on the serving side) over gRPC for peer chunk reads, falling back to gRPC
	// on error. Avoids gRPC/protobuf per-stream framing overhead.
	PeerRawTransport bool

	// APIKey authenticates peer raw-transport HTTP reads (sent as X-API-Key)
	// when the client API is key-protected.
	APIKey string

	// PeerServeMaxInflight bounds concurrent peer serves (gRPC + raw HTTP) on
	// this node. Beyond the cap, requests are rejected with a "busy" signal so
	// requesters fail over or retry with jitter instead of piling up
	// (thundering-herd Phase 1 admission control). <=0 uses the default.
	PeerServeMaxInflight int

	// PeerReplicationStagger is the base delay inserted between successive
	// replica writes of one object (jittered). <=0 uses the default; it can be
	// effectively disabled by setting a tiny value like 1ns.
	PeerReplicationStagger time.Duration

	// FetchLeaseEnabled turns on cross-node single-flight for origin (cloud)
	// pulls: a short-TTL coordinator lease per key collapses N simultaneous
	// cloud fetches of one hot object into ~1 (thundering-herd Phase 2).
	// Advisory — coordinator errors degrade to plain cloud reads.
	FetchLeaseEnabled bool

	// FastChunkAdvertise publishes this node as a holder of each chunk the
	// moment the chunk lands on local NVMe after a remote fetch, instead of
	// only after whole-object operations. Later requesters then pull from the
	// growing swarm rather than origin (thundering-herd Phase 3).
	FastChunkAdvertise bool

	// ReplicaReconcileInterval enables the steady-state replica reconciler
	// when > 0: a background loop that tops up under-replicated hot objects
	// this node holds, staggered and respecting peer busy signals
	// (thundering-herd Phase 3). 0 disables the loop.
	ReplicaReconcileInterval time.Duration

	// ReplicaReconcileTarget is the replica count the reconciler aims for.
	// <=0 uses MinPeerReplicas (default 3).
	ReplicaReconcileTarget int

	// ReplicaReconcileMaxPerRun caps replication operations per reconciler
	// pass so reconciliation itself cannot stampede. <=0 uses a default.
	ReplicaReconcileMaxPerRun int

	// ReplicaReconcileMaxTarget caps the heat-boosted per-file replica target
	// (see reader_heat.go). <=0 uses a default of 8.
	ReplicaReconcileMaxTarget int

	// PinnedPrefixesFn returns file-path prefixes that must not be evicted
	// (e.g., active CSI volume mounts). Nil means nothing is pinned.
	PinnedPrefixesFn func() []string

	// WarmReconcileInterval sets how often declared CSI warmup intents are
	// re-asserted against actual residency (warm_reconciler.go). Applying a
	// declared warmup once at mount time leaves it wrong forever if the first
	// pass failed or eviction later reclaimed the bytes. <=0 uses a default;
	// WarmReconcileDisabled turns the loop off entirely.
	WarmReconcileInterval time.Duration

	// WarmReconcileDisabled stops declarative warmups from being re-asserted.
	// The first pass still runs at mount, matching the old fire-and-forget
	// behavior.
	WarmReconcileDisabled bool
}

// DefaultCacheManager implements CacheManager
type DefaultCacheManager struct {
	config       *CacheConfig
	nvmeStorage  TierStorage
	peerStorage  TierStorage
	cloudStorage TierStorage
	// entries indexes local metadata by path, plus parent->chunks and
	// directory->children so delete/lookup/readdir are point lookups rather
	// than scans of every cached object. Mutate only via put/del.
	entries *entryTable
	// nvmeVerify remembers which local files have had their sidecar checksum
	// checked, so serving a file through many small FUSE reads hashes it once
	// rather than once per read.
	nvmeVerify nvmeVerifier
	// nvmeUsed is maintained as the exact sum of nvmeBytes. Never adjust it
	// directly — go through accountNVMeLocked/unaccountNVMeLocked, which make
	// every update idempotent in the key. Deriving the delta from the entry
	// table instead (read old size, subtract, add new) is not safe: callers
	// install the table row *after* the bytes land, so two concurrent writers
	// to one path both saw "no existing entry", both added a full size, and
	// only one row landed. The surplus was unreclaimable — eviction can only
	// free what an entry claims — so a node drifted toward permanently "full"
	// on a nearly empty disk and then refused every write.
	nvmeBytes map[string]int64
	nvmeUsed  int64
	// nvmeKeyMu serializes whole mutations of one NVMe key. Making the ledger
	// idempotent in the key is not sufficient on its own: touching the disk and
	// updating the ledger are two separate critical sections, so a delete and a
	// write on one key interleave as (delete bytes) → (write bytes, credit) →
	// (unaccount) → (install row), leaving bytes and a row on disk with no
	// credit. That undercount lets the cache overfill. Held across both steps by
	// putToNVMeWithEviction and deleteLocalNVMeFootprint; a fixed array so a
	// zero-value manager built by a test helper is usable without construction.
	nvmeKeyMu    [nvmeKeyStripes]sync.Mutex
	mu           sync.RWMutex
	rangeMu      sync.RWMutex
	fetchMu      sync.Mutex
	logger       *log.Logger
	metrics      *CacheMetrics
	metadataAt   time.Time
	metadataView []*CacheEntry
	rangeChunks  map[string]*chunkFileCache
	// rangeTotalBytes tracks the sum of all per-file cache bytes so the
	// global budget check is O(1) per insert instead of an O(files) scan
	// under rangeMu on every cached chunk.
	rangeTotalBytes int64
	hybridHints     map[string]hybridReadHint
	chunkFetches    map[string]*chunkFetchState
	hedgeLimiter    chan struct{}
	peerServeGate   *peerServeGate
	herdStats       herdControlStats
	deleteStats     deleteStats
	// tombstones suppress this node's own metadata publishes for recently
	// deleted paths, so a background persist or chunk completion that was
	// already in flight cannot re-advertise a file that is gone.
	tombstones *deleteTombstones
	readerHeat readerHeat
	// warmIntents holds declarative CSI warmup intents this node must keep
	// re-asserting (warm_reconciler.go).
	warmIntents      *warmIntentRegistry
	chunkAds         *chunkAdvertiser
	chunkPromoteGate chan struct{}
	tierPerf         *tierPerfTracker
	chunkCompletion  chunkCompletionState
	// evictSignal wakes the watermark evictor between its periodic sweeps.
	// Depth 1, non-blocking send: a pending wake already covers a burst.
	evictSignal    chan struct{}
	shutdownCtx    context.Context
	shutdownCancel context.CancelFunc
	// bgWg tracks one-shot background work (cloud persist, promote-after-read,
	// location publish) — everything that finishes on its own. loopWg tracks
	// the perpetual loops, which finish only once shutdownCtx is cancelled.
	// Keeping them apart is what lets a caller wait for outstanding work to
	// settle without also waiting for a loop that never ends; Shutdown waits
	// on both, after cancelling.
	bgWg   sync.WaitGroup
	loopWg sync.WaitGroup
}

type chunkFileCache struct {
	chunks              map[int64]rangeCachedChunk
	order               []int64
	bytes               int64
	lastPrefetchTrigger int64
	prefetchInFlight    map[int64]struct{}
	prefetchBytes       int64
	// Sequential-stream detection for adaptive readahead.
	lastReadEnd int64
	seqStreak   int
	// lastAccess drives global LRU-by-file eviction and idle expiry: a file
	// nobody is reading must not pin heap (a 5GB sequential read leaves a
	// full per-file cache behind otherwise).
	lastAccess time.Time
}

type RangeCacheSnapshot struct {
	Files            int
	Chunks           int
	Bytes            int64
	PrefetchBytes    int64
	PrefetchInFlight int
}

type rangeCachedChunk struct {
	data []byte
	tier CacheTier
	// prefetched marks a chunk fetched by readahead that has not yet been
	// consumed by a foreground read; used for hit/waste accounting.
	prefetched bool
}

type hybridReadHint struct {
	enabled   bool
	expiresAt time.Time
}

type chunkFetchState struct {
	done chan struct{}
	data []byte
	tier CacheTier
	err  error
}

const slowChunkReadThreshold = 2 * time.Second
const defaultHybridCloudHedgeDelay = 20 * time.Millisecond
const defaultHybridMaxSecondaryInflight = 16
const defaultHybridAlwaysMinSize = 512 * 1024 * 1024  // 512MiB
const defaultHybridStripeMinSize = 1024 * 1024 * 1024 // 1GiB
const chunkFetchWaitTimeout = 15 * time.Second
const defaultHybridMetadataLookupTimeout = 500 * time.Millisecond
const largeReadInstrumentationThreshold = 1024 * 1024 * 1024
const defaultRangeChunkCacheMaxBytes = 512 * 1024 * 1024
const defaultRangePrefetchMaxChunks = 16
const defaultRangePrefetchMaxBytes = 128 * 1024 * 1024

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
	if config.RangeChunkCacheMaxBytes <= 0 {
		config.RangeChunkCacheMaxBytes = int64(config.RangeChunkCacheSize) * config.ChunkSize
		if config.RangeChunkCacheMaxBytes <= 0 || config.RangeChunkCacheMaxBytes > defaultRangeChunkCacheMaxBytes {
			config.RangeChunkCacheMaxBytes = defaultRangeChunkCacheMaxBytes
		}
	}
	if config.RangePrefetchMaxBytes <= 0 {
		config.RangePrefetchMaxBytes = defaultRangePrefetchMaxBytes
		if config.RangePrefetchMaxBytes > config.RangeChunkCacheMaxBytes/2 && config.RangeChunkCacheMaxBytes > 0 {
			config.RangePrefetchMaxBytes = config.RangeChunkCacheMaxBytes / 2
		}
		if config.RangePrefetchMaxBytes < config.ChunkSize {
			config.RangePrefetchMaxBytes = config.ChunkSize
		}
	}
	if config.RangePrefetchMaxChunks <= 0 {
		config.RangePrefetchMaxChunks = defaultRangePrefetchMaxChunks
	}
	if config.RangePrefetchMaxChunks < config.RangePrefetchChunks {
		config.RangePrefetchMaxChunks = config.RangePrefetchChunks
	}
	if config.RangeCacheGlobalMaxBytes == 0 {
		config.RangeCacheGlobalMaxBytes = deriveRangeCacheGlobalMaxBytes(config.RangeChunkCacheMaxBytes)
	}
	if config.RangeCacheIdleExpiry == 0 {
		config.RangeCacheIdleExpiry = defaultRangeCacheIdleExpiry
	}
	if config.PeerReadThroughputBytesPerSec <= 0 {
		config.PeerReadThroughputBytesPerSec = 200 * 1024 * 1024
	}
	if config.S3DownloadConcurrency <= 0 {
		config.S3DownloadConcurrency = defaultS3DownloadConcurrency
	}
	if config.S3DownloadPartSize <= 0 {
		config.S3DownloadPartSize = defaultS3DownloadPartSize
	}
	if config.S3UploadConcurrency <= 0 {
		config.S3UploadConcurrency = defaultS3UploadConcurrency
	}
	if config.S3UploadPartSize <= 0 {
		config.S3UploadPartSize = defaultS3UploadPartSize
	}
	if config.AzureDownloadConcurrency <= 0 {
		config.AzureDownloadConcurrency = 8
	}
	if config.AzureDownloadBlockSizeBytes <= 0 {
		config.AzureDownloadBlockSizeBytes = 4 * 1024 * 1024
	}
	if config.AzureParallelDownloadMinBytes <= 0 {
		config.AzureParallelDownloadMinBytes = 8 * 1024 * 1024
	}
	if config.HybridCloudHedgeDelay <= 0 {
		config.HybridCloudHedgeDelay = defaultHybridCloudHedgeDelay
	}
	if config.HybridMaxSecondaryInflight <= 0 {
		config.HybridMaxSecondaryInflight = defaultHybridMaxSecondaryInflight
	}
	if config.HybridAlwaysMinSize <= 0 {
		config.HybridAlwaysMinSize = defaultHybridAlwaysMinSize
	}
	if config.HybridStripeMinSize <= 0 {
		config.HybridStripeMinSize = defaultHybridStripeMinSize
	}

	shutdownCtx, shutdownCancel := context.WithCancel(context.Background())
	cm := &DefaultCacheManager{
		config:           config,
		entries:          newEntryTable(),
		nvmeBytes:        make(map[string]int64),
		logger:           log.New(log.Writer(), "[CACHE] ", log.LstdFlags),
		metrics:          NewCacheMetrics(),
		rangeChunks:      make(map[string]*chunkFileCache),
		hybridHints:      make(map[string]hybridReadHint),
		chunkFetches:     make(map[string]*chunkFetchState),
		hedgeLimiter:     make(chan struct{}, config.HybridMaxSecondaryInflight),
		peerServeGate:    newPeerServeGate(config.PeerServeMaxInflight),
		chunkAds:         newChunkAdvertiser(),
		tombstones:       newDeleteTombstones(),
		chunkPromoteGate: make(chan struct{}, chunkPromoteMaxConcurrent),
		tierPerf:         newTierPerfTracker(),
		evictSignal:      make(chan struct{}, 1),
		shutdownCtx:      shutdownCtx,
		shutdownCancel:   shutdownCancel,
	}

	// Initialize storage tiers
	var err error
	cm.nvmeStorage, err = NewNVMeStorage(config.NVMePath)
	if err != nil {
		return nil, err
	}

	if config.Coordinator != nil {
		var ps *PeerStorage
		ps, err = NewPeerStorage(config.Coordinator, config.PeerTimeout, config.LocalPeerID, config.PeerReadSortByNetwork, config.PeerReadParallelFanout, config.PeerRawTransport, config.APIKey)
		if err == nil {
			ps.replicationStagger = config.PeerReplicationStagger
			// Stream raw-transport peer reads to NVMe as bytes arrive, so
			// promotion doesn't re-write the object in the background.
			ps.promoteSink = cm
			ps.startGC(shutdownCtx)
			cm.peerStorage = ps
		}
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
			config.AzureDownloadConcurrency,
			config.AzureDownloadBlockSizeBytes,
			config.AzureParallelDownloadMinBytes,
		)
	case "gcp":
		cm.cloudStorage, err = NewGCPStorage(
			config.GCPBucket,
			config.CloudTimeout,
		)
	default: // "s3" or empty
		cm.cloudStorage, err = NewCloudStorageWithTuning(
			config.S3Bucket,
			config.S3Region,
			config.CloudTimeout,
			S3TransferTuning{
				DownloadConcurrency: config.S3DownloadConcurrency,
				DownloadPartSize:    config.S3DownloadPartSize,
				UploadConcurrency:   config.S3UploadConcurrency,
				UploadPartSize:      config.S3UploadPartSize,
				Endpoint:            config.S3Endpoint,
				ForcePathStyle:      config.S3ForcePathStyle,
			},
		)
	}
	if err != nil {
		return nil, err
	}

	// Steady-state replica reconciler (thundering-herd Phase 3).
	if config.ReplicaReconcileInterval > 0 {
		cm.startReplicaReconciler(shutdownCtx)
	}

	// Declarative warmup reconciler: re-asserts CSI-declared warmup intents so
	// a failed first pass or later eviction cannot leave declared and actual
	// residency silently divergent. The registry exists even when the loop is
	// off, so intents are still recorded and the first pass still runs.
	cm.warmIntents = newWarmIntentRegistry()
	if !config.WarmReconcileDisabled {
		cm.startWarmReconciler(shutdownCtx)
	}

	// Idle-expiry sweeper for the range cache: reclaims per-file chunk caches
	// once their reader has moved on (global budget bounds the worst case).
	cm.startRangeCacheSweeper(rangeCacheSweepInterval)

	// Rebuild entries + nvmeUsed from what is actually on disk BEFORE the
	// evictor starts: after a restart the accounting says zero while the
	// dir holds everything the previous process landed, so budgets and LRU
	// silently exclude pre-restart files.
	cm.rehydrateNVMeAccounting()

	// Background watermark evictor: keeps NVMe inside the pressure band so
	// foreground writes (almost) never pay the eviction scan, and reacts to
	// real device fullness (shared host disk), not just our own accounting.
	cm.startWatermarkEvictor()

	// Background GC for hybridHints — prevents unbounded growth.
	gcInterval := config.MetadataRefreshTTL * 10
	if gcInterval < 30*time.Second {
		gcInterval = 30 * time.Second
	}
	go func() {
		ticker := time.NewTicker(gcInterval)
		defer ticker.Stop()
		for {
			select {
			case <-shutdownCtx.Done():
				return
			case <-ticker.C:
				now := time.Now()
				cm.mu.Lock()
				for k, h := range cm.hybridHints {
					if now.After(h.expiresAt) {
						delete(cm.hybridHints, k)
					}
				}
				cm.mu.Unlock()
			}
		}
	}()

	return cm, nil
}

// Shutdown waits for all in-flight background goroutines (cloud persist, NVMe
// promotion, file-location publish) to finish. Call once after the server stops
// accepting requests to avoid losing writes or publishing stale metadata.
func (cm *DefaultCacheManager) Shutdown() {
	cm.shutdownCancel()
	cm.loopWg.Wait()
	cm.bgWg.Wait()
}

// goBackground tracks one-shot fn in bgWg so Shutdown can drain it. Work
// registered here must return on its own; a loop that runs until shutdown
// belongs in goLoop, or a caller waiting for outstanding work to settle waits
// forever.
func (cm *DefaultCacheManager) goBackground(fn func()) {
	cm.bgWg.Add(1)
	go func() {
		defer cm.bgWg.Done()
		fn()
	}()
}

// goLoop tracks a perpetual background loop in loopWg. These exit only when
// shutdownCtx is cancelled, so they are kept out of bgWg — see the field
// comment.
func (cm *DefaultCacheManager) goLoop(fn func()) {
	cm.loopWg.Add(1)
	go func() {
		defer cm.loopWg.Done()
		fn()
	}()
}

// Get retrieves a file from the cache hierarchy
func (cm *DefaultCacheManager) Get(ctx context.Context, filePath string) (*CacheEntry, error) {
	// Check if this is a chunked file
	cm.mu.RLock()
	if entry, ok := cm.entries.get(filePath); ok && entry.IsChunked {
		cm.mu.RUnlock()
		result, err := cm.getChunked(ctx, entry)
		if err == nil {
			cm.touchEntries(filePath)
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
			cm.touchEntries(filePath)
			cm.metrics.RecordHit(TierNVMe)
		} else {
			cm.metrics.RecordMiss(TierNVMe)
		}
		return result, err
	}

	// Check Tier 1: NVME
	nvmeStart := time.Now()
	if entry, err := cm.getFromTier(ctx, filePath, TierNVMe); err == nil {
		cm.touchEntries(filePath)
		cm.metrics.RecordHit(TierNVMe)
		cm.metrics.RecordTierRead(TierNVMe, int64(len(entry.Data)), time.Since(nvmeStart))
		return entry, nil
	}
	cm.metrics.RecordMiss(TierNVMe)

	if fileSize, ok := cm.lookupFileSizeHint(filePath); ok && cm.shouldUseHybridRead(ctx, filePath, fileSize) {
		if entry, err := cm.getFromHybridRemote(ctx, filePath); err == nil {
			e := entry
			cm.goBackground(func() { cm.promoteToNVMe(context.Background(), e) })
			return entry, nil
		}
	}

	// Remote read strategy:
	// - always peer first, then cloud fallback.
	// Cloud reads take the cross-node single-flight lease (Phase 2).
	for _, tier := range cm.remoteReadOrder(filePath) {
		tierStart := time.Now()
		if entry, servedTier, err := cm.getFromRemoteTier(ctx, filePath, tier); err == nil {
			cm.touchEntries(filePath)
			cm.metrics.RecordHit(servedTier)
			cm.metrics.RecordTierRead(servedTier, int64(len(entry.Data)), time.Since(tierStart))
			e := entry
			cm.goBackground(func() { cm.promoteToNVMe(context.Background(), e) })
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
	entry, ok := cm.entries.get(filePath)
	chunked := ok && entry != nil && entry.IsChunked
	cm.mu.RUnlock()

	if tierEntry, err := cm.getFromTierNoVerify(ctx, filePath, TierNVMe); err == nil {
		if entry != nil {
			tierEntry.IsChunked = entry.IsChunked
			tierEntry.NumChunks = entry.NumChunks
			tierEntry.Size = entry.Size
		}
		cm.touchEntries(filePath)
		return tierEntry, nil
	}

	if chunked {
		result, err := cm.getChunkedLocal(ctx, entry)
		if err == nil {
			cm.touchEntries(filePath)
		}
		return result, err
	}

	// Serve a chunk request from a whole parent file held on this node. FUSE
	// writes store the object whole on NVMe (only the parent has an entry and
	// no per-chunk files exist), so a cross-node peer asking for
	// "<parent>_chunk_N" would otherwise miss here and force the reader to fall
	// through to cloud. Synthesize the chunk from the whole parent, mirroring
	// the local read path (readChunkFromWholeLocalFile). This is what lets the
	// peer tier actually serve FUSE-written files.
	if _, isChunk := parentFilePathFromChunkPath(filePath); isChunk {
		if syn, err := cm.readChunkFromWholeLocalFile(ctx, filePath); err == nil {
			cm.touchEntries(filePath)
			return syn, nil
		}
	}

	result, err := cm.getFromTierNoVerify(ctx, filePath, TierNVMe)
	if err == nil {
		cm.touchEntries(filePath)
	}
	return result, err
}

// LocalFilePath returns the local NVMe path for filePath if present.
func (cm *DefaultCacheManager) LocalFilePath(ctx context.Context, filePath string) (string, bool) {
	_ = ctx
	fullPath := filepath.Join(cm.config.NVMePath, strings.TrimPrefix(filePath, "/"))
	info, err := os.Stat(fullPath)
	if err != nil || info.IsDir() {
		return "", false
	}
	return fullPath, true
}

// LocalChunkFile resolves "<parent>_chunk_N" to a byte range within a whole
// parent file held on this node, so a peer can stream that range directly
// instead of synthesizing the chunk into a fresh buffer. It returns ok=false
// when the parent is not a whole chunked file present on local NVMe, or when a
// literal chunk file exists (that case is served by LocalFilePath/streamLocalFile).
func (cm *DefaultCacheManager) LocalChunkFile(ctx context.Context, chunkPath string) (string, int64, int64, bool) {
	_ = ctx
	parent, ok := parentFilePathFromChunkPath(chunkPath)
	if !ok {
		return "", 0, 0, false
	}
	idxStr := chunkPath[strings.LastIndex(chunkPath, "_chunk_")+len("_chunk_"):]
	chunkIndex, err := strconv.ParseInt(idxStr, 10, 64)
	if err != nil || chunkIndex < 0 {
		return "", 0, 0, false
	}
	cm.mu.RLock()
	entry, hasParent := cm.entries.get(parent)
	var size int64
	var chunked bool
	var chunkSize int64
	if hasParent && entry != nil {
		size = entry.Size
		chunked = entry.IsChunked
		// Serve the byte range the file was written with, not the range our
		// own flag implies — otherwise this node hands peers mis-aligned data.
		chunkSize = cm.effectiveChunkSize(entry)
	}
	cm.mu.RUnlock()
	if chunkSize <= 0 {
		return "", 0, 0, false
	}
	if !hasParent || !chunked || size <= 0 {
		return "", 0, 0, false
	}

	offset := chunkIndex * chunkSize
	if offset >= size {
		return "", 0, 0, false
	}
	length := chunkSize
	if remaining := size - offset; remaining < length {
		length = remaining
	}

	parentLocal := filepath.Join(cm.config.NVMePath, strings.TrimPrefix(parent, "/"))
	info, err := os.Stat(parentLocal)
	if err != nil || info.IsDir() {
		return "", 0, 0, false
	}
	return parentLocal, offset, length, true
}

func (cm *DefaultCacheManager) remoteReadOrder(filePath string) []CacheTier {
	_ = filePath
	if cm.config.AdaptiveRemoteRead && cm.tierPerf != nil {
		return cm.tierPerf.order()
	}
	return remoteReadOrderPeerFirst
}

// recordTierPerf folds a remote read outcome into the adaptive tier tracker.
// No-op for non-remote tiers or when adaptive reads are disabled.
func (cm *DefaultCacheManager) recordTierPerf(tier CacheTier, dur time.Duration, ok bool) {
	if cm.tierPerf == nil {
		return
	}
	cm.tierPerf.record(tier, dur, ok)
}

// recordTierOutcome folds a failed remote read into the tier tracker, but only
// when the failure says something about the tier's health.
//
// Peer errors have three classes (see isPeerBusy / isPeerMiss), and the same
// split applies to whole tiers:
//   - busy: admission control. The data is provably there; the tier is fine.
//   - miss: the tier does not hold the object. Routing information, and on a
//     cold read the expected answer — every chunk misses the primary tier at
//     least once by construction.
//   - anything else: a real transport or provider failure, which is the only
//     thing the EWMA should learn from.
//
// Recording the first two drove fuse_tier_peer_success_ratio to 0.0 live,
// silently inverting tier order and pinning hedging on. Note that misses do
// not update lastSampleAt either, which is deliberate: an idle tier should
// age into its recovery floor and get re-probed rather than be held down by
// a stream of misses that carry no health signal.
func (cm *DefaultCacheManager) recordTierOutcome(tier CacheTier, start time.Time, err error) {
	if err == nil {
		cm.recordTierPerf(tier, time.Since(start), true)
		return
	}
	if isPeerBusy(err) {
		return
	}
	if isTierMiss(err) {
		if cm.tierPerf != nil {
			cm.tierPerf.recordMiss(tier)
		}
		return
	}
	cm.recordTierPerf(tier, time.Since(start), false)
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

func lookupEntrySizeLocked(entries *entryTable, filePath string) (int64, bool) {
	entry, ok := entries.get(filePath)
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

	lookupCtx, cancel := context.WithTimeout(ctx, defaultHybridMetadataLookupTimeout)
	defer cancel()
	locations, err := cm.config.Coordinator.GetFileLocation(lookupCtx, filePath)
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
	effectiveReplicas := peerReplicas
	if effectiveReplicas < 1 {
		effectiveReplicas = 1
	}
	peerCapacity := int64(effectiveReplicas) * cm.config.PeerReadThroughputBytesPerSec
	enabled := false

	// For large objects, always enable peer+cloud hedging when at least one peer
	// has data. This prevents optimistic peer-throughput assumptions from
	// suppressing cloud acceleration.
	if peerReplicas > 0 && fileSize >= cm.config.HybridAlwaysMinSize {
		enabled = true
	} else if fileSize > peerCapacity && hasCloud && peerReplicas > 0 {
		// Smaller files keep prior behavior: enable hybrid only when peer
		// throughput estimate is insufficient and cloud is known present.
		enabled = true
	}

	// Adaptive override: once both tiers have enough measured samples, let the
	// observed reliability/latency race decide hedging instead of size alone.
	// A close race or an unreliable primary hedges; a clear reliable winner
	// reads alone. Only strengthens toward hedging when cloud is known present.
	if !enabled && cm.config.AdaptiveRemoteRead && cm.tierPerf != nil && peerReplicas > 0 && hasCloud {
		if hedge, ok := cm.tierPerf.shouldHedge(); ok && hedge {
			enabled = true
		}
	}

	cm.mu.Lock()
	cm.hybridHints[filePath] = hybridReadHint{
		enabled:   enabled,
		expiresAt: now.Add(cm.config.MetadataRefreshTTL),
	}
	cm.mu.Unlock()

	return enabled
}

func (cm *DefaultCacheManager) shouldStripeHybridChunk(filePath string, chunkIndex int64) bool {
	if cm.config.HybridStripeMinSize <= 0 {
		return false
	}
	if chunkIndex < 0 || chunkIndex%2 == 0 {
		return false
	}
	fileSize, ok := cm.lookupFileSizeHint(filePath)
	if !ok {
		return false
	}
	return fileSize >= cm.config.HybridStripeMinSize
}

func (cm *DefaultCacheManager) getFromHybridRemote(ctx context.Context, filePath string) (*CacheEntry, error) {
	type readResult struct {
		entry *CacheEntry
		tier  CacheTier
		dur   time.Duration
		err   error
	}

	readCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	resultCh := make(chan readResult, 2)
	go func() {
		start := time.Now()
		entry, err := cm.getFromTier(readCtx, filePath, TierPeer)
		resultCh <- readResult{entry: entry, tier: TierPeer, dur: time.Since(start), err: err}
	}()
	go func() {
		// Start cloud shortly after peer to keep peer primary while enabling
		// throughput gains from hybrid hedging when peer stalls.
		timer := time.NewTimer(cm.effectiveHybridHedgeDelay(TierPeer, TierCloud))
		defer timer.Stop()
		select {
		case <-readCtx.Done():
			return
		case <-timer.C:
		}
		start := time.Now()
		entry, err := cm.getFromTier(readCtx, filePath, TierCloud)
		resultCh <- readResult{entry: entry, tier: TierCloud, dur: time.Since(start), err: err}
	}()

	var lastErr error
	for i := 0; i < 2; i++ {
		res := <-resultCh
		if res.err == nil {
			cm.metrics.RecordHit(res.tier)
			cm.metrics.RecordTierRead(res.tier, int64(len(res.entry.Data)), res.dur)
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

func (cm *DefaultCacheManager) effectiveHybridHedgeDelay(primary, secondary CacheTier) time.Duration {
	delay := cm.config.HybridCloudHedgeDelay
	if delay <= 0 {
		delay = defaultHybridCloudHedgeDelay
	}
	primaryMBps := cm.metrics.TierReadMBps(primary)
	secondaryMBps := cm.metrics.TierReadMBps(secondary)
	if primaryMBps > 0 && secondaryMBps > 0 {
		switch {
		case primaryMBps >= secondaryMBps*1.25:
			delay *= 2
		case primaryMBps <= secondaryMBps*0.75:
			delay /= 2
		}
	}
	if delay < 2*time.Millisecond {
		return 2 * time.Millisecond
	}
	return delay
}

func (cm *DefaultCacheManager) tryAcquireHedgeSlot() (func(), bool) {
	limiter := cm.hedgeLimiter
	if limiter == nil || cap(limiter) == 0 {
		return func() {}, true
	}
	select {
	case limiter <- struct{}{}:
		return func() { <-limiter }, true
	default:
		return nil, false
	}
}

// getChunked reassembles a chunked file from its chunks
func (cm *DefaultCacheManager) getChunked(ctx context.Context, entry *CacheEntry) (*CacheEntry, error) {
	if localEntry, err := cm.getFromTierNoVerify(ctx, entry.FilePath, TierNVMe); err == nil {
		localEntry.IsChunked = true
		localEntry.NumChunks = entry.NumChunks
		localEntry.ChunkSize = entry.ChunkSize
		localEntry.Size = entry.Size
		return localEntry, nil
	}

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
		ChunkSize:    entry.ChunkSize,
	}, nil
}

func (cm *DefaultCacheManager) getChunkedLocal(ctx context.Context, entry *CacheEntry) (*CacheEntry, error) {
	if localEntry, err := cm.getFromTierNoVerify(ctx, entry.FilePath, TierNVMe); err == nil {
		localEntry.IsChunked = true
		localEntry.NumChunks = entry.NumChunks
		localEntry.ChunkSize = entry.ChunkSize
		localEntry.Size = entry.Size
		return localEntry, nil
	}

	var allData []byte
	for i := int64(0); i < entry.NumChunks; i++ {
		chunkPath := fmt.Sprintf("%s_chunk_%d", entry.FilePath, i)
		chunkEntry, err := cm.getFromTierNoVerify(ctx, chunkPath, TierNVMe)
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
		ChunkSize:    entry.ChunkSize,
	}, nil
}

// WriteTo streams file content to w without buffering the entire file in memory.
// For chunked files, each chunk is read and written individually (max ~ChunkSize in memory).
// For regular files, the data is written directly.
func (cm *DefaultCacheManager) WriteTo(ctx context.Context, filePath string, w io.Writer) (int64, error) {
	// Copy IsChunked and NumChunks under the lock to avoid a data race when the
	// entry is concurrently modified after we release RLock.
	cm.mu.RLock()
	var isChunked bool
	var numChunks int64
	var wantSize int64
	if entry, ok := cm.entries.get(filePath); ok && entry != nil {
		isChunked = entry.IsChunked
		numChunks = entry.NumChunks
		wantSize = entry.Size
	}
	cm.mu.RUnlock()

	if isChunked {
		var total int64
		for i := int64(0); i < numChunks; i++ {
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
		// A wrong chunk count (stale metadata, or a stride mismatch on legacy
		// files) yields a short body with no error, which callers cannot
		// distinguish from a correct one. Fail loudly instead.
		if wantSize > 0 && total != wantSize {
			return total, fmt.Errorf("short read for %s: assembled %d bytes from %d chunks, want %d",
				filePath, total, numChunks, wantSize)
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
	if entry == nil {
		return errors.New("nil cache entry")
	}
	if strings.TrimSpace(entry.FilePath) == "" {
		return errors.New("empty file path")
	}
	// Re-creating a deleted path is legitimate, and its metadata must publish
	// again, so a write lifts the tombstone the delete left behind.
	cm.tombstones.clear(entry.FilePath)
	// Many API callers only set FilePath. Keep StoragePath aligned by default.
	if strings.TrimSpace(entry.StoragePath) == "" {
		entry.StoragePath = entry.FilePath
	}
	if entry.Size == 0 {
		entry.Size = int64(len(entry.Data))
	}

	if _, err := cm.deleteLocalNVMeFootprint(ctx, entry.FilePath, true, true); err != nil {
		return err
	}
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

	// Try to store in NVME first (with eviction if needed). The row is a
	// Data-less copy, never entry itself: callers own their entry and keep it
	// alive across writes (FUSE re-Puts the same *CacheEntry on every flush),
	// so installing that pointer would publish a table row the caller mutates
	// without cm.mu — and would pin the whole payload in the table for the
	// row's lifetime.
	nvmeRow := *entry
	nvmeRow.Data = nil
	nvmeRow.Tier = TierNVMe
	if err := cm.putToNVMeWithEviction(ctx, entry, &nvmeRow); err == nil {
		// Only now is the tier true. Stamping it before the attempt leaves a
		// caller's entry claiming NVMe residency for a file that reached no
		// tier at all when every fallback below also fails.
		entry.Tier = TierNVMe
		cm.metrics.RecordWrite(int64(len(entry.Data)))

		// Deep-copy data before launching goroutine to avoid race
		dataCopy := make([]byte, len(entry.Data))
		copy(dataCopy, entry.Data)
		filePath := entry.FilePath
		cm.goBackground(func() { cm.persistToCloud(cm.shutdownCtx, filePath, dataCopy) })
		cm.goBackground(func() { cm.publishFileLocation(context.Background(), entry, TierNVMe) })

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
			entry.Tier = res.tier
			// Same reason as the NVMe row above: the table gets a copy, so a
			// caller that keeps writing through its own entry cannot mutate a
			// published row out from under cm.mu.
			row := *entry
			row.Data = nil
			cm.mu.Lock()
			cm.entries.put(&row)
			cm.mu.Unlock()
			cm.metrics.RecordWrite(int64(len(entry.Data)))
			tier := res.tier
			cm.goBackground(func() { cm.publishFileLocation(context.Background(), entry, tier) })
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
	cm.tombstones.clear(filePath)
	if lastAccessed.IsZero() {
		lastAccessed = time.Now()
	}
	if _, err := cm.deleteLocalNVMeFootprint(ctx, filePath, true, true); err != nil {
		return err
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
		chunkLastAccessed := time.Now()
		chunkEntry := &CacheEntry{
			FilePath:     chunkPath,
			StoragePath:  chunkPath,
			Size:         int64(len(chunkData)),
			LastAccessed: chunkLastAccessed,
			Tier:         TierNVMe,
			Data:         chunkData,
		}
		chunkMeta := *chunkEntry
		chunkMeta.Data = nil
		if err := cm.putToNVMeWithEviction(ctx, chunkEntry, &chunkMeta); err != nil {
			return fmt.Errorf("failed to store chunk %d in NVMe for %s: %w", i, filePath, err)
		}
		cm.metrics.RecordWrite(chunkEntry.Size)
		remaining -= toRead
	}

	meta := &CacheEntry{
		FilePath:     filePath,
		StoragePath:  filePath,
		Size:         size,
		LastAccessed: time.Now(),
		Tier:         TierNVMe,
		IsChunked:    true,
		NumChunks:    numChunks,
		ChunkSize:    cm.config.ChunkSize,
	}
	cm.mu.Lock()
	cm.entries.put(meta)
	cm.mu.Unlock()
	cm.goBackground(func() { cm.publishFileLocation(context.Background(), meta, TierNVMe) })
	fp, nc := filePath, numChunks
	cm.goBackground(func() { cm.persistChunkedFileToCloud(cm.shutdownCtx, fp, nc) })
	return nil
}

// CreateWriteFile creates a staged local file on the NVMe volume so final
// commit can use a local rename instead of a full reread/copy.
func (cm *DefaultCacheManager) CreateWriteFile(ctx context.Context, filePath string) (*os.File, string, error) {
	_ = ctx
	finalPath := filepath.Join(cm.config.NVMePath, strings.TrimPrefix(filePath, "/"))
	dir := filepath.Dir(finalPath)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, "", err
	}
	tmp, err := os.CreateTemp(dir, filepath.Base(finalPath)+".stage-*")
	if err != nil {
		return nil, "", err
	}
	return tmp, tmp.Name(), nil
}

// PutFromFile commits a staged local file into NVMe-backed cache metadata
// without rereading the full payload into memory. Large files are exposed as
// chunked metadata, but chunk bytes are served from the whole local file when
// requested and are persisted to cloud asynchronously.
func (cm *DefaultCacheManager) PutFromFile(ctx context.Context, filePath string, stagedPath string, size int64, lastAccessed time.Time) error {
	if strings.TrimSpace(filePath) == "" {
		return errors.New("empty file path")
	}
	if strings.TrimSpace(stagedPath) == "" {
		return errors.New("empty staged path")
	}
	cm.tombstones.clear(filePath)
	if lastAccessed.IsZero() {
		lastAccessed = time.Now()
	}
	if _, err := cm.deleteLocalNVMeFootprint(ctx, filePath, true, true); err != nil {
		return err
	}

	finalPath := filepath.Join(cm.config.NVMePath, strings.TrimPrefix(filePath, "/"))
	if err := os.MkdirAll(filepath.Dir(finalPath), 0o755); err != nil {
		return err
	}
	// Landing the bytes, crediting them, and publishing the row is one
	// mutation of this key — same atom putToNVMeWithEviction enforces. A
	// delete or eviction interleaving between the rename and the ledger
	// update below finds no row to collect, so it releases nothing and the
	// renamed bytes stay on disk uncredited and unreclaimable forever
	// (collectLocalDeleteTargets only emits keys that have a TierNVMe row).
	// Taken after the footprint delete above, which locks this key itself and
	// the stripe mutexes are not reentrant.
	unlock := cm.lockNVMeKey(filePath)
	defer unlock()
	if err := os.Rename(stagedPath, finalPath); err != nil {
		return err
	}
	info, err := os.Stat(finalPath)
	if err != nil {
		return err
	}
	if size <= 0 {
		size = info.Size()
	}

	cm.invalidateRangeCache(filePath)

	numChunks := int64(0)
	isChunked := false
	chunkSize := int64(0)
	if size > cm.config.ChunkSize {
		isChunked = true
		numChunks = chunkCountFor(size, cm.config.ChunkSize)
		chunkSize = cm.config.ChunkSize
	}

	entry := &CacheEntry{
		FilePath:     filePath,
		StoragePath:  filePath,
		Size:         size,
		LastAccessed: lastAccessed,
		Tier:         TierNVMe,
		IsChunked:    isChunked,
		NumChunks:    numChunks,
		ChunkSize:    chunkSize,
	}

	cm.mu.Lock()
	cm.removeChunkEntriesLocked(filePath)
	cm.entries.put(entry)
	cm.accountNVMeLocked(filePath, size)
	cm.mu.Unlock()
	cm.metrics.RecordWrite(size)

	cm.goBackground(func() { cm.publishFileLocation(context.Background(), entry, TierNVMe) })
	if isChunked {
		fp, fp2, nc := filePath, finalPath, numChunks
		cm.goBackground(func() { cm.persistWholeFileAsChunksToCloud(cm.shutdownCtx, fp, fp2, nc) })
	}
	return nil
}

func (cm *DefaultCacheManager) touchEntries(paths ...string) {
	now := time.Now()
	cm.mu.Lock()
	defer cm.mu.Unlock()

	for _, path := range paths {
		if strings.TrimSpace(path) == "" {
			continue
		}
		if entry, ok := cm.entries.get(path); ok && entry != nil {
			entry.LastAccessed = now
			// Same critical section as the timestamp: eviction walks the list,
			// so a touch that updated one and not the other would hand it a
			// stale ordering.
			cm.entries.touch(path)
		}
	}
}

type localDeleteTarget struct {
	key        string
	deletePath string
}

func (cm *DefaultCacheManager) collectLocalDeleteTargets(filePath string, includeChildren bool) []localDeleteTarget {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	targets := make([]localDeleteTarget, 0, 4)
	seen := make(map[string]struct{})
	addTarget := func(key string, deletePath string) {
		if strings.TrimSpace(key) == "" {
			return
		}
		if _, ok := seen[key]; ok {
			return
		}
		if strings.TrimSpace(deletePath) == "" {
			deletePath = key
		}
		seen[key] = struct{}{}
		targets = append(targets, localDeleteTarget{
			key:        key,
			deletePath: deletePath,
		})
	}

	if entry, ok := cm.entries.get(filePath); ok && entry != nil && entry.Tier == TierNVMe {
		addTarget(filePath, entry.StoragePath)
		if includeChildren && entry.IsChunked && entry.NumChunks > 0 {
			for i := int64(0); i < entry.NumChunks; i++ {
				chunkPath := fmt.Sprintf("%s_chunk_%d", filePath, i)
				if chunkEntry, ok := cm.entries.get(chunkPath); ok && chunkEntry != nil && chunkEntry.Tier == TierNVMe {
					addTarget(chunkPath, chunkEntry.StoragePath)
					continue
				}
				addTarget(chunkPath, chunkPath)
			}
		}
	}

	// Chunks the parent's own NumChunks did not cover: a partially-fetched
	// file, or one whose parent metadata is gone. The index makes this
	// O(chunks of this file); it used to scan every cached object on every
	// overwrite, which on a node holding one large file means one pass over
	// thousands of chunk entries per write.
	if includeChildren {
		for _, key := range cm.entries.chunkKeys(filePath) {
			entry, ok := cm.entries.get(key)
			if !ok || entry == nil || entry.Tier != TierNVMe {
				continue
			}
			addTarget(key, entry.StoragePath)
		}
	}

	return targets
}

// nvmeKeyStripes is the width of the per-key mutation lock. Sized to be wide
// enough that unrelated paths rarely collide, small enough to stay inline in
// the manager.
const nvmeKeyStripes = 64

// lockNVMeKey serializes one key's disk-plus-ledger mutation against any other
// mutation of the same key. Returns the unlock.
func (cm *DefaultCacheManager) lockNVMeKey(key string) func() {
	m := &cm.nvmeKeyMu[nvmeKeyStripe(key)]
	m.Lock()
	return m.Unlock
}

// lockNVMeKeys locks a set of keys, deduped and in a canonical order so two
// callers holding overlapping sets can never deadlock against each other.
func (cm *DefaultCacheManager) lockNVMeKeys(keys []string) func() {
	stripes := make([]int, 0, len(keys))
	seen := make(map[int]bool, len(keys))
	for _, k := range keys {
		s := nvmeKeyStripe(k)
		if !seen[s] {
			seen[s] = true
			stripes = append(stripes, s)
		}
	}
	sort.Ints(stripes)
	for _, s := range stripes {
		cm.nvmeKeyMu[s].Lock()
	}
	return func() {
		for i := len(stripes) - 1; i >= 0; i-- {
			cm.nvmeKeyMu[stripes[i]].Unlock()
		}
	}
}

func nvmeKeyStripe(key string) int {
	// FNV-1a, inlined to avoid allocating a hash per call on the write path.
	var h uint32 = 2166136261
	for i := 0; i < len(key); i++ {
		h ^= uint32(key[i])
		h *= 16777619
	}
	return int(h % nvmeKeyStripes)
}

// accountNVMeLocked records that key currently occupies size bytes on NVMe,
// replacing whatever it was previously credited with. Idempotent in key: two
// concurrent writers of the same path converge on one contribution instead of
// each adding their own. Caller holds cm.mu for writing.
func (cm *DefaultCacheManager) accountNVMeLocked(key string, size int64) {
	if key == "" {
		return
	}
	if size < 0 {
		size = 0
	}
	if cm.nvmeBytes == nil {
		cm.nvmeBytes = make(map[string]int64)
	}
	cm.nvmeUsed += size - cm.nvmeBytes[key]
	if cm.nvmeUsed < 0 {
		cm.nvmeUsed = 0
	}
	cm.nvmeBytes[key] = size
}

// unaccountNVMeLocked drops key's contribution and reports what it was, so
// callers can log the bytes actually reclaimed. Caller holds cm.mu for writing.
func (cm *DefaultCacheManager) unaccountNVMeLocked(key string) int64 {
	prev, ok := cm.nvmeBytes[key]
	if !ok {
		return 0
	}
	delete(cm.nvmeBytes, key)
	cm.nvmeUsed -= prev
	if cm.nvmeUsed < 0 {
		cm.nvmeUsed = 0
	}
	return prev
}

func (cm *DefaultCacheManager) deleteLocalNVMeFootprint(ctx context.Context, filePath string, includeChildren bool, ignoreMissing bool) (int64, error) {
	targets := cm.collectLocalDeleteTargets(filePath, includeChildren)
	if len(targets) == 0 {
		if ignoreMissing {
			return 0, nil
		}
		return 0, errors.New("file not found in local cache")
	}

	storage := cm.getStorageForTier(TierNVMe)
	// Same-key exclusion as the write path, for the same reason, held across
	// both the removal and the ledger release.
	keys := make([]string, 0, len(targets))
	for _, target := range targets {
		keys = append(keys, target.key)
	}
	defer cm.lockNVMeKeys(keys)()

	counted := make(map[string]bool, len(targets))
	for _, target := range targets {
		// Every removal of a local copy goes through here — delete, overwrite,
		// eviction — so it is the one place the verification record has to be
		// dropped for the ranged read path to never serve a stale approval.
		cm.nvmeVerify.forget(target.key)
		counted[target.key] = storage.Exists(ctx, target.deletePath)
		if err := storage.Delete(ctx, target.deletePath); err != nil && !isDeleteNotFoundError(err) {
			return 0, err
		}
		_ = storage.Delete(ctx, target.deletePath+".sha256")
	}

	var freed int64
	cm.mu.Lock()
	for _, target := range targets {
		// Drop the ledger credit whether or not a table row survives: the
		// bytes are gone from disk either way, and a credit with no way left
		// to reclaim it is exactly the leak this accounting exists to avoid.
		released := cm.unaccountNVMeLocked(target.key)
		if counted[target.key] {
			freed += released
		}
		entry, ok := cm.entries.get(target.key)
		if !ok || entry == nil || entry.Tier != TierNVMe {
			continue
		}
		cm.entries.del(target.key)
	}
	cm.mu.Unlock()

	return freed, nil
}

func isDeleteNotFoundError(err error) bool {
	if err == nil {
		return false
	}
	if os.IsNotExist(err) {
		return true
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "not found") || strings.Contains(msg, "no such file")
}

// WriteCloud writes arbitrary bytes to the configured cloud tier object path.
func (cm *DefaultCacheManager) WriteCloud(ctx context.Context, path string, data []byte) error {
	if cm.cloudStorage == nil {
		return errors.New("cloud storage not configured")
	}
	if strings.TrimSpace(path) == "" {
		return errors.New("empty cloud path")
	}
	return cm.cloudStorage.Write(ctx, path, data)
}

// ReadCloud reads arbitrary bytes from the configured cloud tier object path.
func (cm *DefaultCacheManager) ReadCloud(ctx context.Context, path string) ([]byte, error) {
	if cm.cloudStorage == nil {
		return nil, errors.New("cloud storage not configured")
	}
	if strings.TrimSpace(path) == "" {
		return nil, errors.New("empty cloud path")
	}
	return cm.cloudStorage.Read(ctx, path)
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
	entries := cm.entries.snapshot()
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

	remote := cm.entriesFromLocations(locations)

	cm.mu.Lock()
	cm.metadataView = remote
	cm.metadataAt = time.Now()
	cm.mu.Unlock()

	return mergeEntries(entries, remote), nil
}

// entriesFromLocations converts coordinator file locations into the metadata
// view shape. Shared by list() and the point-lookup refresh path.
func (cm *DefaultCacheManager) entriesFromLocations(locations []*coordinator.FileLocation) []*CacheEntry {
	remote := make([]*CacheEntry, 0, len(locations))
	for _, loc := range locations {
		if loc == nil || loc.FilePath == "" {
			continue
		}
		chunkSize := loc.ChunkSize
		if chunkSize <= 0 {
			chunkSize = cm.config.ChunkSize
		}
		numChunks := int64(0)
		if loc.IsChunked {
			numChunks = chunkCountFor(loc.FileSize, chunkSize)
		} else {
			chunkSize = 0
		}
		remote = append(remote, &CacheEntry{
			FilePath:     loc.FilePath,
			StoragePath:  loc.StoragePath,
			Size:         loc.FileSize,
			LastAccessed: loc.LastAccessed,
			Tier:         tierFromStorageTier(loc.StorageTier),
			IsChunked:    loc.IsChunked,
			NumChunks:    numChunks,
			ChunkSize:    chunkSize,
		})
	}
	return remote
}

// Evict removes the least recently accessed entries from a tier to free the given number of bytes.
// If bytesNeeded is 0, it evicts entries until usage is below 90% of MaxNVMeSize.
func (cm *DefaultCacheManager) Evict(ctx context.Context, tier CacheTier) error {
	if tier != TierNVMe {
		return nil
	}
	cm.evictToTarget(ctx, cm.config.MaxNVMeSize*9/10) // evict down to 90%
	return nil
}

// evictBatchSize is how many candidates one pass takes off the cold end of the
// recency list before re-checking usage. It bounds the work done under the
// lock and the work wasted when the first few candidates free enough: a pass
// that reclaims one file walks one batch, not the whole table.
const evictBatchSize = 64

// evictToTarget deletes least-recently-accessed NVMe entries until usage is
// at or under target bytes. Shared by the reactive write-path Evict and the
// background watermark evictor; both honor pinning and the durability guard.
//
// Candidates come off the cold end of the entry table's recency list rather
// than from a scan-and-sort of every entry. The old shape cost O(N log N) with
// N = every cached object — including one entry per chunk, so a node holding a
// single 100GB file at an 8MB stride sorted 12800 entries — and it ran inline
// in the write that tripped the capacity check, meaning the write paid for it.
func (cm *DefaultCacheManager) evictToTarget(ctx context.Context, target int64) {
	// Collect pinned prefixes once before the eviction loop.
	var pinnedPrefixes []string
	if cm.config.PinnedPrefixesFn != nil {
		pinnedPrefixes = cm.config.PinnedPrefixesFn()
	}

	type entryRef struct {
		key   string
		entry *CacheEntry
	}
	// Paths already considered and rejected this call: pinned, not yet durable,
	// or a failed delete. They stay in the list at their current position, so
	// without this the next batch would start on the same ones and never
	// advance.
	skipped := make(map[string]struct{})

	for {
		cm.mu.RLock()
		if cm.nvmeUsed <= target {
			cm.mu.RUnlock()
			return
		}
		batch := make([]entryRef, 0, evictBatchSize)
		cm.entries.eachColdest(func(k string, e *CacheEntry) bool {
			if e == nil || e.Tier != TierNVMe {
				return true
			}
			if _, seen := skipped[k]; seen {
				return true
			}
			// A chunk of a file that is itself present and chunked is evicted
			// with its parent, not on its own.
			if parentPath, ok := parentFilePathFromChunkPath(k); ok {
				if parent, exists := cm.entries.get(parentPath); exists && parent != nil && parent.Tier == TierNVMe && parent.IsChunked {
					return true
				}
			}
			batch = append(batch, entryRef{k, e})
			return len(batch) < evictBatchSize
		})
		cm.mu.RUnlock()

		if len(batch) == 0 {
			// Nothing left that this call can reclaim; usage stays above
			// target until the blocked entries become durable or unpinned.
			return
		}

		for _, ref := range batch {
			cm.mu.RLock()
			done := cm.nvmeUsed <= target
			cm.mu.RUnlock()
			if done {
				return
			}
			if isPathPinned(ref.key, pinnedPrefixes) {
				skipped[ref.key] = struct{}{}
				continue
			}
			// Durability guard: never evict the only copy. Under sustained
			// pressure with a slow cloud link this can leave usage above target —
			// by design; ENOSPC risk beats data loss.
			if !cm.evictionSafe(ctx, ref.key, ref.entry) {
				cm.metrics.EvictionSkippedUnpersisted.Add(1)
				skipped[ref.key] = struct{}{}
				continue
			}
			freed, err := cm.deleteLocalNVMeFootprint(ctx, ref.key, true, true)
			if err != nil {
				cm.logger.Printf("Eviction delete failed for %s: %v", ref.key, err)
				skipped[ref.key] = struct{}{}
				continue
			}
			// A delete that leaves the entry in place (nothing local to remove)
			// frees nothing and would be picked again on the next batch.
			cm.mu.RLock()
			_, stillThere := cm.entries.get(ref.key)
			cm.mu.RUnlock()
			if stillThere {
				skipped[ref.key] = struct{}{}
				continue
			}
			cm.metrics.RecordEviction()
			cm.logger.Printf("Evicted %s (%d bytes)", ref.key, freed)
		}
	}
}

// markPersistedToCloud flags filePath (and, for a chunk, refreshes the parent
// when every sibling chunk is durable) as safely evictable.
func (cm *DefaultCacheManager) markPersistedToCloud(filePath string) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	if e, ok := cm.entries.get(filePath); ok && e != nil {
		e.PersistedToCloud = true
	}
	parentPath, isChunk := parentFilePathFromChunkPath(filePath)
	if !isChunk {
		return
	}
	parent, ok := cm.entries.get(parentPath)
	if !ok || parent == nil || !parent.IsChunked || parent.PersistedToCloud {
		return
	}
	for i := int64(0); i < parent.NumChunks; i++ {
		ce, ok := cm.entries.get(chunkPathFor(parentPath, i))
		if !ok || ce == nil || !ce.PersistedToCloud {
			return
		}
	}
	parent.PersistedToCloud = true
}

// evictionSafe reports whether an entry's bytes exist beyond this node.
// Unpersisted entries are only evictable if the cloud tier independently has
// the object (covers restarts, where the in-memory bit is lost).
func (cm *DefaultCacheManager) evictionSafe(ctx context.Context, key string, entry *CacheEntry) bool {
	// entry is a pointer the caller collected under RLock and then released
	// it. markPersistedToCloud mutates these same fields under the write lock,
	// so take one locked snapshot and read only from the copy — reading them
	// off the live pointer races the cloud-persist path.
	if entry != nil {
		cm.mu.RLock()
		snapshot := *entry
		cm.mu.RUnlock()
		entry = &snapshot
		if entry.PersistedToCloud {
			return true
		}
	}
	if cm.cloudStorage == nil {
		return false
	}
	// HEAD probes, only on the eviction path (covers restart, where the
	// in-memory bit is lost). Revalidate by SIZE, not just existence — a
	// same-key object with a different length is a different version, and
	// evicting the local copy against it silently loses the local write
	// (mountpoint-s3 revalidates cached content by etag for the same
	// reason; size is the check every backend already exposes).
	if entry != nil && entry.IsChunked && entry.NumChunks > 0 && cm.effectiveChunkSize(entry) > 0 {
		// Chunked parents persist as chunk objects: probe first chunk for
		// existence and the last chunk for exact expected size.
		if !cm.cloudStorage.Exists(ctx, chunkPathFor(key, 0)) {
			return false
		}
		chunkSize := cm.effectiveChunkSize(entry)
		lastIdx := entry.NumChunks - 1
		wantLast := entry.Size - lastIdx*chunkSize
		if wantLast <= 0 || wantLast > chunkSize {
			// Size/chunk-count metadata disagrees; do not trust the probe.
			return false
		}
		gotLast, err := cm.cloudStorage.Size(ctx, chunkPathFor(key, lastIdx))
		return err == nil && gotLast == wantLast
	}
	if entry != nil {
		got, err := cm.cloudStorage.Size(ctx, key)
		if err == nil && got == entry.Size {
			cm.markPersistedToCloud(key)
			return true
		}
		return false
	}
	if cm.cloudStorage.Exists(ctx, key) {
		cm.markPersistedToCloud(key)
		return true
	}
	return false
}

// isPathPinned reports whether path falls under any of the given prefixes.
func isPathPinned(path string, prefixes []string) bool {
	for _, p := range prefixes {
		if p == "" {
			continue
		}
		if path == p || strings.HasPrefix(path, p+"/") {
			return true
		}
	}
	return false
}

// persistToCloud writes data to cloud storage with retry. It is always called
// inside a goBackground goroutine. ctx should be the shutdownCtx so that retry
// sleeps are interruptible during shutdown.
func (cm *DefaultCacheManager) persistToCloud(ctx context.Context, filePath string, data []byte) {
	var lastErr error
	for attempt := 0; attempt < cm.config.CloudRetryCount; attempt++ {
		select {
		case <-ctx.Done():
			cm.logger.Printf("Cloud persist cancelled for %s (shutdown)", filePath)
			return
		default:
		}
		// Re-checked every attempt, not just on entry: a delete can land
		// between retries, and this write-through is the most reliable way to
		// resurrect a deleted blob — the whole-file variant holds an open fd
		// that outlives the unlink, so it would happily upload a file whose
		// local bytes are already gone.
		if cm.tombstoned(filePath) {
			cm.logger.Printf("Cloud persist of %s abandoned: deleted", filePath)
			return
		}
		callCtx, cancel := context.WithTimeout(ctx, cm.config.CloudTimeout)
		err := cm.cloudStorage.Write(callCtx, filePath, data)
		cancel()

		if err == nil {
			cm.logger.Printf("Persisted %s (%d bytes) to cloud storage", filePath, len(data))
			cm.markPersistedToCloud(filePath)
			// Already in a background goroutine; call synchronously to stay tracked.
			cm.publishFileLocation(context.Background(), &CacheEntry{
				FilePath:     filePath,
				StoragePath:  filePath,
				Size:         int64(len(data)),
				LastAccessed: time.Now(),
				Tier:         TierCloud,
			}, TierCloud)
			return
		}

		lastErr = err
		wait := cm.config.CloudRetryBaseWait * (1 << uint(attempt))
		cm.logger.Printf("Cloud persist attempt %d/%d failed for %s: %v (retrying in %v)",
			attempt+1, cm.config.CloudRetryCount, filePath, err, wait)
		select {
		case <-ctx.Done():
			return
		case <-time.After(wait):
		}
	}

	cm.logger.Printf("CRITICAL: All %d cloud persist attempts failed for %s: %v",
		cm.config.CloudRetryCount, filePath, lastErr)
}

// persistChunkedFileToCloud persists chunk objects so hybrid peer+cloud range
// reads can use both tiers for large files. ctx should be shutdownCtx.
func (cm *DefaultCacheManager) persistChunkedFileToCloud(ctx context.Context, filePath string, numChunks int64) {
	if numChunks <= 0 || cm.cloudStorage == nil || cm.nvmeStorage == nil {
		return
	}

	// Persist concurrency scales with backlog: a 640-chunk checkpoint gets
	// the max pool, a 3-chunk file the floor (the old fixed cap of 8
	// throttled multi-GB uploads).
	workers := cm.persistWorkersFor(numChunks)

	jobs := make(chan int64, workers*2)
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for idx := range jobs {
				select {
				case <-ctx.Done():
					return
				default:
				}
				chunkPath := fmt.Sprintf("%s_chunk_%d", filePath, idx)
				data, err := cm.nvmeStorage.Read(ctx, chunkPath)
				if err != nil {
					cm.logger.Printf("Chunk cloud persist skipped for %s: %v", chunkPath, err)
					continue
				}
				cm.persistToCloud(ctx, chunkPath, data)
			}
		}()
	}

	for idx := int64(0); idx < numChunks; idx++ {
		select {
		case <-ctx.Done():
			close(jobs)
			wg.Wait()
			return
		case jobs <- idx:
		}
	}
	close(jobs)
	wg.Wait()
}

// putToNVMeWithEviction lands entry's bytes on NVMe, credits them to the
// ledger, and installs row in the entry table — all three as one mutation of
// this key.
//
// row is the metadata readers and eviction will see, and is required: a
// Data-less copy of entry, never entry itself. Two reasons it must be a copy.
// The table hands the pointer to every reader under cm.mu, so a caller that
// keeps its own entry alive across writes would be mutating a published row
// unsynchronized. And the read path serves bytes from nvmeStorage, never from
// entries[].Data, so a payload-carrying row is pure retention — it pins the
// whole file in the heap for the row's lifetime and GET /api/cache
// base64-encodes it into what callers expect to be a metadata listing.
//
// Passing the row in rather than letting the caller install it afterwards is
// the point. Eviction and overwrite-cleanup can only reclaim what a row
// claims, so between the credit and the row install the bytes are charged to
// a key nothing can release, and anything that runs in that window leaves the
// credit behind for good. Both drift signatures seen under concurrent load —
// credit with bytes but no row, and credit with a row but no bytes — are that
// window.
func (cm *DefaultCacheManager) putToNVMeWithEviction(ctx context.Context, entry, row *CacheEntry) error {
	if row == nil {
		// Crediting bytes without publishing a row is exactly the drift this
		// function exists to prevent; refuse rather than half-commit.
		return errors.New("putToNVMeWithEviction: nil row")
	}
	// What this write replaces is whatever the ledger already credits to this
	// key — not what the entry table says. A concurrent same-path write may
	// have landed bytes whose row is not installed until it reaches the
	// critical section below, so the table under-reports what is on disk.
	cm.mu.RLock()
	wouldExceed := cm.nvmeUsed-cm.nvmeBytes[entry.FilePath]+entry.Size > cm.config.MaxNVMeSize
	cm.mu.RUnlock()

	if wouldExceed {
		cm.Evict(ctx, TierNVMe)
		// Re-check after eviction
		cm.mu.RLock()
		stillExceed := cm.nvmeUsed-cm.nvmeBytes[entry.FilePath]+entry.Size > cm.config.MaxNVMeSize
		cm.mu.RUnlock()
		if stillExceed {
			return errors.New("NVMe storage full after eviction")
		}
	}

	// Landing the bytes, crediting them, and publishing the row must be one
	// step with respect to this key: a concurrent delete that removed the old
	// bytes partway through would drop the credit for the new ones, leaving
	// accounted-for bytes on disk that eviction can never reclaim.
	unlock := cm.lockNVMeKey(entry.FilePath)
	if err := cm.putToTier(ctx, entry, TierNVMe); err != nil {
		unlock()
		return err
	}

	cm.mu.Lock()
	cm.accountNVMeLocked(entry.FilePath, entry.Size)
	cm.entries.put(row)
	cm.mu.Unlock()
	unlock()
	// Hand the next eviction to the background evictor while there is still
	// headroom, so the write after this one is less likely to run one inline.
	cm.maybeSignalEvictionPressure()
	return nil
}

func (cm *DefaultCacheManager) publishFileLocation(ctx context.Context, entry *CacheEntry, tier CacheTier) {
	if cm.config.Coordinator == nil || cm.config.LocalPeerID == "" || entry == nil {
		return
	}
	// A publish that lands after a delete recreates the row the delete just
	// retracted, and the ghost is back. This and advertiseHolder are the only
	// two callers of UpdateFileLocation in the package, so guarding here
	// covers every metadata publish rather than each call site.
	if cm.tombstoned(entry.FilePath) {
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
	// Carry the stride so peers that pull this file address its chunks the way
	// it was written, regardless of their own -chunk-size.
	if entry.IsChunked {
		location.ChunkSize = cm.effectiveChunkSize(entry)
	}
	if err := cm.config.Coordinator.UpdateFileLocation(ctx, location); err != nil {
		cm.logger.Printf("Failed to publish metadata for %s: %v", entry.FilePath, err)
	}
}

// heapStatsMB reads heap alloc/in-use via runtime/metrics, which does not
// stop the world — runtime.ReadMemStats does, and these are called from
// read-path logging where a multi-ms global pause is unacceptable.
func heapStatsMB() (allocMB, heapInuseMB float64) {
	samples := []metrics.Sample{
		{Name: "/memory/classes/heap/objects:bytes"}, // ≈ MemStats.HeapAlloc
		{Name: "/memory/classes/heap/unused:bytes"},
	}
	metrics.Read(samples)
	if samples[0].Value.Kind() == metrics.KindUint64 {
		allocMB = float64(samples[0].Value.Uint64()) / (1024 * 1024)
	}
	heapInuseMB = allocMB
	if samples[1].Value.Kind() == metrics.KindUint64 {
		// HeapInuse ≈ objects + unused span space.
		heapInuseMB += float64(samples[1].Value.Uint64()) / (1024 * 1024)
	}
	return allocMB, heapInuseMB
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
	return cm.getFromTierInternal(ctx, filePath, tier, true)
}

func (cm *DefaultCacheManager) getFromTierNoVerify(ctx context.Context, filePath string, tier CacheTier) (*CacheEntry, error) {
	return cm.getFromTierInternal(ctx, filePath, tier, false)
}

func (cm *DefaultCacheManager) getFromTierInternal(ctx context.Context, filePath string, tier CacheTier, verifyNVMeChecksum bool) (*CacheEntry, error) {
	storage := cm.getStorageForTier(tier)

	data, err := storage.Read(ctx, filePath)
	if err != nil {
		if tier == TierNVMe {
			if chunkEntry, chunkErr := cm.readChunkFromWholeLocalFile(ctx, filePath); chunkErr == nil {
				return chunkEntry, nil
			}
		}
		return nil, err
	}

	entry := &CacheEntry{
		FilePath:     filePath,
		StoragePath:  filePath,
		Size:         int64(len(data)),
		LastAccessed: time.Now(),
		Tier:         tier,
		Data:         data,
	}

	// Verify checksum on NVMe reads if sidecar exists
	if tier == TierNVMe && verifyNVMeChecksum {
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
	if tier == TierNVMe {
		paths := []string{filePath}
		if parentPath, ok := parentFilePathFromChunkPath(filePath); ok {
			paths = append(paths, parentPath)
		}
		cm.touchEntries(paths...)
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

func (cm *DefaultCacheManager) removeChunkEntriesLocked(filePath string) {
	for _, key := range cm.entries.chunkKeys(filePath) {
		// Retiring the row without releasing the credit leaked the chunk's
		// bytes on every overwrite of a chunked file.
		cm.unaccountNVMeLocked(key)
		cm.entries.del(key)
	}
}

func (cm *DefaultCacheManager) readChunkFromWholeLocalFile(ctx context.Context, chunkPath string) (*CacheEntry, error) {
	parentPath, ok := parentFilePathFromChunkPath(chunkPath)
	if !ok {
		return nil, os.ErrNotExist
	}
	cm.mu.RLock()
	parentMeta, hasParent := cm.entries.get(parentPath)
	cm.mu.RUnlock()
	if !hasParent || parentMeta == nil {
		return nil, os.ErrNotExist
	}
	fullPath := filepath.Join(cm.config.NVMePath, strings.TrimPrefix(parentPath, "/"))
	f, err := os.Open(fullPath)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	_, chunkIndexStr, _ := strings.Cut(chunkPath, parentPath+"_chunk_")
	chunkIndex, err := strconv.ParseInt(chunkIndexStr, 10, 64)
	if err != nil {
		return nil, err
	}
	// Slice the whole local file with the parent's own stride so the chunk we
	// synthesize matches what every other holder calls chunk N.
	chunkSize := cm.effectiveChunkSize(parentMeta)
	if chunkSize <= 0 {
		return nil, os.ErrNotExist
	}
	offset := chunkIndex * chunkSize
	if offset >= parentMeta.Size {
		return nil, io.EOF
	}
	if remaining := parentMeta.Size - offset; remaining < chunkSize {
		chunkSize = remaining
	}
	buf := make([]byte, chunkSize)
	n, err := f.ReadAt(buf, offset)
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, err
	}
	buf = buf[:n]
	return &CacheEntry{
		FilePath:     chunkPath,
		StoragePath:  chunkPath,
		Size:         int64(len(buf)),
		LastAccessed: time.Now(),
		Tier:         TierNVMe,
		Data:         buf,
	}, nil
}

func (cm *DefaultCacheManager) persistWholeFileAsChunksToCloud(ctx context.Context, filePath string, localPath string, numChunks int64) {
	f, err := os.Open(localPath)
	if err != nil {
		cm.logger.Printf("Chunk cloud persist open failed for %s: %v", filePath, err)
		return
	}
	defer f.Close()

	buf := make([]byte, cm.config.ChunkSize)
	for i := int64(0); i < numChunks; i++ {
		select {
		case <-ctx.Done():
			return
		default:
		}
		n, readErr := io.ReadFull(f, buf)
		if readErr != nil && !errors.Is(readErr, io.EOF) && !errors.Is(readErr, io.ErrUnexpectedEOF) {
			cm.logger.Printf("Chunk cloud persist read failed for %s chunk=%d: %v", filePath, i, readErr)
			return
		}
		if n <= 0 {
			return
		}
		chunkPath := fmt.Sprintf("%s_chunk_%d", filePath, i)
		chunkData := make([]byte, n)
		copy(chunkData, buf[:n])
		cm.persistToCloud(ctx, chunkPath, chunkData)
		if errors.Is(readErr, io.EOF) || errors.Is(readErr, io.ErrUnexpectedEOF) {
			return
		}
	}
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

// alreadyLocalNVMe reports whether path is NVMe-resident with the given size,
// so promotion passes can skip re-writing bytes a streaming (tee) promotion
// already landed.
func (cm *DefaultCacheManager) alreadyLocalNVMe(ctx context.Context, path string, size int64) bool {
	cm.mu.RLock()
	existing, _ := cm.entries.get(path)
	cm.mu.RUnlock()
	return existing != nil && existing.Tier == TierNVMe && existing.Size == size &&
		cm.nvmeStorage.Exists(ctx, path)
}

func (cm *DefaultCacheManager) promoteToNVMe(ctx context.Context, entry *CacheEntry) {
	if cm.alreadyLocalNVMe(ctx, entry.FilePath, entry.Size) {
		cm.touchEntries(entry.FilePath)
		return
	}
	newEntry := &CacheEntry{
		FilePath:     entry.FilePath,
		StoragePath:  entry.StoragePath,
		Size:         entry.Size,
		LastAccessed: time.Now(),
		Tier:         TierNVMe,
		Data:         entry.Data,
	}

	meta := *newEntry
	meta.Data = nil
	if err := cm.putToNVMeWithEviction(ctx, newEntry, &meta); err != nil {
		cm.logger.Printf("Promotion to NVMe failed for %s: %v", entry.FilePath, err)
		return
	}
	cm.touchEntries(entry.FilePath)
	// These bytes came from a peer or the cloud and are now servable locally:
	// join the swarm so the next requester can pull them from here.
	cm.advertisePromoted(entry.FilePath)
}

func (cm *DefaultCacheManager) putChunked(ctx context.Context, entry *CacheEntry) error {
	dataLen := int64(len(entry.Data))
	numChunks := (dataLen + cm.config.ChunkSize - 1) / cm.config.ChunkSize

	// Write chunks sequentially to NVMe and defer cloud persistence to the
	// bounded background worker pool. This avoids spawning one cloud-upload
	// goroutine per chunk, which can overwhelm S3 and throttle writes.
	for i := int64(0); i < numChunks; i++ {
		start := i * cm.config.ChunkSize
		end := start + cm.config.ChunkSize
		if end > dataLen {
			end = dataLen
		}

		chunkData := make([]byte, end-start)
		copy(chunkData, entry.Data[start:end])
		chunkPath := fmt.Sprintf("%s_chunk_%d", entry.FilePath, i)
		chunkEntry := &CacheEntry{
			FilePath:     chunkPath,
			StoragePath:  chunkPath,
			Size:         int64(len(chunkData)),
			LastAccessed: time.Now(),
			Tier:         TierNVMe,
			Data:         chunkData,
		}
		chunkMeta := *chunkEntry
		chunkMeta.Data = nil
		if err := cm.putToNVMeWithEviction(ctx, chunkEntry, &chunkMeta); err != nil {
			return fmt.Errorf("failed to store chunk %d in NVMe for %s: %w", i, entry.FilePath, err)
		}
		cm.metrics.RecordWrite(chunkEntry.Size)
	}

	entry.IsChunked = true
	entry.NumChunks = numChunks
	entry.ChunkSize = cm.config.ChunkSize
	entry.Size = dataLen
	entry.Tier = TierNVMe
	// Drop full payload after chunking to avoid retaining large buffers in memory.
	entry.Data = nil
	entry.Checksum = ""

	cm.mu.Lock()
	cm.entries.put(entry)
	cm.mu.Unlock()
	cm.goBackground(func() { cm.publishFileLocation(context.Background(), entry, TierNVMe) })
	fp, nc := entry.FilePath, numChunks
	cm.goBackground(func() { cm.persistChunkedFileToCloud(cm.shutdownCtx, fp, nc) })

	return nil
}

// ReadRange reads a byte range from a file without loading an entire chunked file
// into memory.
// effectiveChunkSize returns the stride to use when addressing entry's chunks:
// the size the file was written with when known, else this node's configured
// size. Legacy metadata predates CacheEntry.ChunkSize, so the fallback stays.
func (cm *DefaultCacheManager) effectiveChunkSize(entry *CacheEntry) int64 {
	if entry != nil && entry.ChunkSize > 0 {
		return entry.ChunkSize
	}
	return cm.config.ChunkSize
}

// chunkCountFor derives the chunk count from a file size and stride the same
// way putChunked splits it. Holders publish IsChunked without enumerating
// chunks, so readers reconstruct the count instead of trusting a stale one.
func chunkCountFor(size, chunkSize int64) int64 {
	if size <= 0 || chunkSize <= 0 {
		return 0
	}
	return (size + chunkSize - 1) / chunkSize
}

// clampChunkSlice bounds [from,to) to a valid sub-slice of a chunk of length
// n. Both ends are clamped: a wrong chunk-size stride (see effectiveChunkSize)
// can push from past the end of the chunk, and slicing on that panics in the
// FUSE read goroutine, which unmounts the node rather than failing one read.
func clampChunkSlice(from, to, n int64) (int64, int64) {
	if from < 0 {
		from = 0
	}
	if from > n {
		from = n
	}
	if to > n {
		to = n
	}
	if to < from {
		to = from
	}
	return from, to
}

func (cm *DefaultCacheManager) ReadRange(ctx context.Context, filePath string, offset int64, size int) ([]byte, error) {
	started := time.Now()
	if size <= 0 {
		return []byte{}, nil
	}

	cm.mu.RLock()
	entry, hasMeta := cm.entries.get(filePath)
	isChunked := hasMeta && entry.IsChunked
	cm.mu.RUnlock()
	if !isChunked {
		if resolved, ok := cm.resolveChunkedEntry(ctx, filePath); ok {
			entry = resolved
			hasMeta = true
			isChunked = true
		}
	}
	// Address chunks with the stride the file was written with, not this
	// node's flag — they differ across a -chunk-size change and the wrong
	// stride silently mis-slices every chunk boundary.
	chunkSize := cm.effectiveChunkSize(entry)

	if !isChunked {
		// Serve the window straight off the local file when we hold it.
		// Falling through to Get() reads and hashes the whole object for every
		// ~128KiB FUSE window, so an 8MB file read end to end moved 512MB.
		if out, ok := cm.readWholeFileRange(ctx, filePath, offset, size); ok {
			dur := time.Since(started)
			cm.metrics.RecordHit(TierNVMe)
			cm.metrics.RecordTierRead(TierNVMe, int64(len(out)), dur)
			cm.metrics.RecordReadWall(int64(len(out)), dur, int64(len(out)), 0, 0)
			return out, nil
		}
		got, err := cm.Get(ctx, filePath)
		if err != nil {
			return nil, err
		}
		if got.Tier == TierNVMe {
			// Get() verified the sidecar; remember it so the rest of this
			// file's windows take the ranged path above.
			cm.noteNVMeVerified(filePath)
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
		nvmeWallBytes, peerWallBytes, cloudWallBytes := readWallTierBytes(got.Tier, int64(len(out)))
		cm.metrics.RecordReadWall(int64(len(out)), dur, nvmeWallBytes, peerWallBytes, cloudWallBytes)
		if dur >= slowChunkReadThreshold {
			cm.logger.Printf("ReadRange slow non-chunked path=%s offset=%d size=%d dur=%v", filePath, offset, size, dur)
		}
		cm.touchEntries(filePath)
		return out, nil
	}

	totalSize := entry.Size
	if totalSize < 0 {
		totalSize = 0
	}
	largeRead := totalSize >= largeReadInstrumentationThreshold
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
	chunkTiers := make([]CacheTier, chunkCount)
	remoteOrder := cm.remoteReadOrder(filePath)
	hybridRead := cm.shouldUseHybridRead(ctx, filePath, totalSize)
	if !hybridRead && entry.NumChunks > 0 {
		hybridRead = cm.shouldUseHybridRead(ctx, fmt.Sprintf("%s_chunk_%d", filePath, 0), totalSize)
	}

	// Per-file profile: workers derive from file size and measured link
	// speed (configured value is the floor; small files skip fan-out).
	profile := cm.readProfileFor(totalSize, chunkSize)
	workerCount := profile.workers // >= 1 on every readProfileFor path
	if workerCount > chunkCount {
		workerCount = chunkCount
	}

	if chunkCount == 1 {
		data, tier, err := cm.readChunkData(ctx, filePath, startChunk, remoteOrder, hybridRead)
		if err != nil {
			return nil, err
		}
		chunks[0] = data
		chunkTiers[0] = tier
	} else {
		readCtx, readCancel := context.WithCancel(ctx)
		defer readCancel()
		jobs := make(chan int, chunkCount)
		errCh := make(chan error, 1)
		var wg sync.WaitGroup

		for w := 0; w < workerCount; w++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for pos := range jobs {
					chunkIndex := startChunk + int64(pos)
					data, tier, err := cm.readChunkData(readCtx, filePath, chunkIndex, remoteOrder, hybridRead)
					if err != nil {
						select {
						case errCh <- err:
						default:
						}
						readCancel()
						return
					}
					chunks[pos] = data
					chunkTiers[pos] = tier
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
			cm.logReadRangeSnapshot("error", filePath, offset, size, startChunk, endChunk, workerCount, hybridRead, err)
			return nil, err
		default:
		}
	}

	// Adaptive readahead. Sequential streams grow the prefetch window
	// (doubling up to RangePrefetchMaxChunks); random access falls back to the
	// base window, triggered only on boundary and near-boundary reads so next
	// chunks are already inflight before the kernel crosses into them.
	prefetchTrigger := chunkSize / 4
	if prefetchTrigger < 256*1024 {
		prefetchTrigger = 256 * 1024
	}
	if prefetchTrigger > 2*1024*1024 {
		prefetchTrigger = 2 * 1024 * 1024
	}
	chunkEndBoundary := (endChunk + 1) * chunkSize
	nearBoundary := chunkSize > 0 && chunkEndBoundary-end <= prefetchTrigger
	window, sequential := cm.observeReadPattern(filePath, totalSize, offset, end, endChunk, chunkSize)
	if window > 0 && (sequential || offset%chunkSize == 0 || nearBoundary) {
		prefetchChunks, prefetchBudgetBytes, rangeBytes := cm.reservePrefetchChunks(filePath, endChunk, entry.NumChunks, chunkSize, window)
		if largeRead && len(prefetchChunks) > 0 {
			rangeChunkCount, _ := cm.rangeCacheStats(filePath)
			cm.logger.Printf("ReadRange prefetch path=%s offset=%d size=%d endChunk=%d prefetch_chunks=%d window=%d sequential=%t near_boundary=%t range_cache_chunks=%d range_cache_mb=%.1f",
				filePath, offset, size, endChunk, len(prefetchChunks), window, sequential, nearBoundary, rangeChunkCount, float64(rangeBytes)/(1024*1024))
			cm.logger.Printf("ReadRange prefetch budget path=%s endChunk=%d reserved_chunks=%d prefetch_budget_mb=%.1f range_cache_mb=%.1f",
				filePath, endChunk, len(prefetchChunks), float64(prefetchBudgetBytes)/(1024*1024), float64(rangeBytes)/(1024*1024))
		}
		cm.metrics.PrefetchIssued.Add(int64(len(prefetchChunks)))
		for _, prefetchChunk := range prefetchChunks {
			go func(chunkIndex int64) {
				defer cm.releasePrefetchReservation(filePath, chunkIndex, chunkSize)
				prefetchCtx, cancel := context.WithTimeout(context.Background(), cm.config.PeerTimeout)
				defer cancel()
				if _, _, err := cm.readChunkData(prefetchCtx, filePath, chunkIndex, remoteOrder, hybridRead); err == nil {
					cm.markChunkPrefetched(filePath, chunkIndex)
				}
			}(prefetchChunk)
		}
	}

	if chunkCount == 1 {
		chunkData := chunks[0]
		chunkOffset := startChunk * chunkSize
		from := int64(0)
		if offset > chunkOffset {
			from = offset - chunkOffset
		}
		to := int64(len(chunkData))
		chunkEnd := chunkOffset + int64(len(chunkData))
		if end < chunkEnd {
			to = end - chunkOffset
		}
		// from must be clamped as well as to: when the file was written with a
		// different chunk size than this reader is configured for, chunkOffset
		// is computed from the wrong stride and from can land past the end of
		// the chunk. Slicing on that panics inside the FUSE read goroutine and
		// takes the whole daemon (and the node's mount) down.
		from, to = clampChunkSlice(from, to, int64(len(chunkData)))
		nvmeWallBytes, peerWallBytes, cloudWallBytes := readWallTierBytes(chunkTiers[0], to-from)
		dur := time.Since(started)
		cm.metrics.RecordReadWall(to-from, dur, nvmeWallBytes, peerWallBytes, cloudWallBytes)
		if dur >= slowChunkReadThreshold {
			cm.logger.Printf("ReadRange slow chunked path=%s offset=%d size=%d chunks=%d workers=%d hybrid=%t dur=%v",
				filePath, offset, size, chunkCount, workerCount, hybridRead, dur)
		}
		return chunkData[from:to], nil
	}

	out := make([]byte, 0, end-offset)
	var nvmeWallBytes int64
	var peerWallBytes int64
	var cloudWallBytes int64
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

		from, to = clampChunkSlice(from, to, int64(len(chunkData)))
		sliceLen := to - from
		switch chunkTiers[pos] {
		case TierNVMe:
			nvmeWallBytes += sliceLen
		case TierPeer:
			peerWallBytes += sliceLen
		case TierCloud:
			cloudWallBytes += sliceLen
		}
		out = append(out, chunkData[from:to]...)
	}

	dur := time.Since(started)
	cm.metrics.RecordReadWall(int64(len(out)), dur, nvmeWallBytes, peerWallBytes, cloudWallBytes)
	if dur >= slowChunkReadThreshold {
		cm.logger.Printf("ReadRange slow chunked path=%s offset=%d size=%d chunks=%d workers=%d hybrid=%t dur=%v",
			filePath, offset, size, chunkCount, workerCount, hybridRead, dur)
	}
	if largeRead && (offset == 0 || end >= totalSize || dur >= slowChunkReadThreshold) {
		cm.logReadRangeSnapshot("done", filePath, offset, size, startChunk, endChunk, workerCount, hybridRead, nil)
	}
	cm.touchEntries(filePath)
	return out, nil
}

func (cm *DefaultCacheManager) resolveChunkedEntry(ctx context.Context, filePath string) (*CacheEntry, bool) {
	cm.mu.RLock()
	if entry, ok := cm.entries.get(filePath); ok && entry != nil && entry.IsChunked {
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
		chunkSize := cm.effectiveChunkSize(entry)
		numChunks := entry.NumChunks
		if numChunks <= 0 {
			numChunks = chunkCountFor(entry.Size, chunkSize)
		}
		resolved := &CacheEntry{
			FilePath:     filePath,
			StoragePath:  entry.StoragePath,
			Size:         entry.Size,
			LastAccessed: entry.LastAccessed,
			Tier:         tierFromStorageTier(cacheTierToStorageTier(entry.Tier)),
			IsChunked:    true,
			NumChunks:    numChunks,
			ChunkSize:    chunkSize,
		}
		cm.mu.RUnlock()
		cm.mu.Lock()
		cm.entries.put(resolved)
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
		// Prefer the stride the holder published; fall back to ours only for
		// metadata written before FileLocation carried it.
		chunkSize := loc.ChunkSize
		if chunkSize <= 0 {
			chunkSize = cm.config.ChunkSize
		}
		resolved := &CacheEntry{
			FilePath:     filePath,
			StoragePath:  loc.StoragePath,
			Size:         loc.FileSize,
			LastAccessed: loc.LastAccessed,
			Tier:         tierFromStorageTier(loc.StorageTier),
			IsChunked:    true,
			NumChunks:    chunkCountFor(loc.FileSize, chunkSize),
			ChunkSize:    chunkSize,
		}
		cm.mu.Lock()
		cm.entries.put(resolved)
		cm.mu.Unlock()
		return resolved, true
	}
	return nil, false
}

func (cm *DefaultCacheManager) readChunkData(ctx context.Context, filePath string, chunkIndex int64, remoteOrder []CacheTier, hybridRead bool) ([]byte, CacheTier, error) {
	chunkPath := fmt.Sprintf("%s_chunk_%d", filePath, chunkIndex)
	chunkStart := time.Now()
	chunkData, chunkTier, ok := cm.getChunkFromRangeCache(filePath, chunkIndex)
	if ok {
		cm.touchEntries(filePath, chunkPath)
		return chunkData, chunkTier, nil
	}

	// Collapse duplicate concurrent fetches for the same chunk. Without this,
	// parallel FUSE read requests for one chunk can cause repeated remote fetches.
	for {
		state, leader := cm.startChunkFetch(chunkPath)
		if !leader {
			waitCtx, waitCancel := context.WithTimeout(ctx, chunkFetchWaitTimeout)
			select {
			case <-waitCtx.Done():
				waitErr := waitCtx.Err()
				waitCancel()
				if errors.Is(waitErr, context.DeadlineExceeded) {
					cm.clearStaleChunkFetch(chunkPath, state)
					continue
				}
				return nil, TierUnknown, waitErr
			case <-state.done:
				waitCancel()
			}
			if state.err != nil {
				return nil, state.tier, state.err
			}
			return state.data, state.tier, nil
		}
		return cm.fetchChunkAsLeader(ctx, filePath, chunkPath, chunkIndex, remoteOrder, hybridRead, chunkStart, state)
	}
}

func (cm *DefaultCacheManager) startChunkFetch(chunkPath string) (*chunkFetchState, bool) {
	cm.fetchMu.Lock()
	defer cm.fetchMu.Unlock()

	if cm.chunkFetches == nil {
		cm.chunkFetches = make(map[string]*chunkFetchState)
	}
	if state, ok := cm.chunkFetches[chunkPath]; ok {
		return state, false
	}

	state := &chunkFetchState{done: make(chan struct{})}
	cm.chunkFetches[chunkPath] = state
	return state, true
}

func (cm *DefaultCacheManager) completeChunkFetch(chunkPath string, state *chunkFetchState, data []byte, tier CacheTier, err error) {
	cm.fetchMu.Lock()
	state.data = data
	state.tier = tier
	state.err = err
	close(state.done)
	delete(cm.chunkFetches, chunkPath)
	cm.fetchMu.Unlock()
}

func (cm *DefaultCacheManager) clearStaleChunkFetch(chunkPath string, state *chunkFetchState) {
	cm.fetchMu.Lock()
	defer cm.fetchMu.Unlock()
	current, ok := cm.chunkFetches[chunkPath]
	if !ok || current != state {
		return
	}
	delete(cm.chunkFetches, chunkPath)
}

func (cm *DefaultCacheManager) fetchChunkAsLeader(
	ctx context.Context,
	filePath string,
	chunkPath string,
	chunkIndex int64,
	remoteOrder []CacheTier,
	hybridRead bool,
	chunkStart time.Time,
	state *chunkFetchState,
) ([]byte, CacheTier, error) {
	var (
		chunkData []byte
		chunkTier CacheTier
		err       error
	)
	defer func() {
		cm.completeChunkFetch(chunkPath, state, chunkData, chunkTier, err)
	}()

	fetchCtx := ctx
	cancel := func() {}
	if _, ok := ctx.Deadline(); !ok {
		timeout := cm.chunkReadTimeout(remoteOrder, hybridRead)
		if timeout > 0 {
			fetchCtx, cancel = context.WithTimeout(ctx, timeout)
		}
	}
	defer cancel()

	chunkData, chunkTier, err = cm.readChunkDataFromTiers(fetchCtx, filePath, chunkPath, chunkIndex, remoteOrder, hybridRead, chunkStart)
	return chunkData, chunkTier, err
}

func (cm *DefaultCacheManager) chunkReadTimeout(remoteOrder []CacheTier, hybridRead bool) time.Duration {
	timeout := cm.config.PeerTimeout
	if hybridRead {
		if cm.config.CloudTimeout > timeout {
			timeout = cm.config.CloudTimeout
		}
		return timeout
	}
	for _, tier := range remoteOrder {
		switch tier {
		case TierCloud:
			if cm.config.CloudTimeout > timeout {
				timeout = cm.config.CloudTimeout
			}
		case TierPeer:
			if cm.config.PeerTimeout > timeout {
				timeout = cm.config.PeerTimeout
			}
		}
	}
	return timeout
}

func (cm *DefaultCacheManager) readChunkDataFromTiers(
	ctx context.Context,
	filePath string,
	chunkPath string,
	chunkIndex int64,
	remoteOrder []CacheTier,
	hybridRead bool,
	chunkStart time.Time,
) ([]byte, CacheTier, error) {
	tierStart := time.Now()
	if chunkEntry, err := cm.getFromTierNoVerify(ctx, chunkPath, TierNVMe); err == nil {
		tierDur := time.Since(tierStart)
		cm.metrics.RecordHit(TierNVMe)
		cm.metrics.RecordTierRead(TierNVMe, int64(len(chunkEntry.Data)), tierDur)
		chunkData := chunkEntry.Data
		cm.setChunkInRangeCache(filePath, chunkIndex, chunkData, TierNVMe)
		cm.touchEntries(filePath, chunkPath)
		cm.traceChunkAttempt(chunkPath, chunkIndex, TierNVMe, tierStart, nil)
		cm.traceChunkTotal(chunkPath, chunkIndex, chunkStart, nil)
		return chunkData, TierNVMe, nil
	} else {
		cm.traceChunkAttempt(chunkPath, chunkIndex, TierNVMe, tierStart, err)
	}
	cm.metrics.RecordMiss(TierNVMe)

	primary := TierPeer
	secondary := TierCloud
	if len(remoteOrder) >= 2 {
		primary, secondary = remoteOrder[0], remoteOrder[1]
	}
	if hybridRead && cm.shouldStripeHybridChunk(filePath, chunkIndex) {
		primary, secondary = secondary, primary
	}
	if hybridRead {
		hybridPrimary := primary
		hybridSecondary := secondary
		hybridStart := time.Now()
		if chunkEntry, tier, err := cm.readChunkHybridHedged(ctx, chunkPath, hybridPrimary, hybridSecondary); err == nil {
			tierDur := time.Since(hybridStart)
			cm.metrics.RecordHit(tier)
			cm.metrics.RecordTierRead(tier, int64(len(chunkEntry.Data)), tierDur)
			cm.recordTierPerf(tier, tierDur, true)
			chunkData := chunkEntry.Data
			cm.setChunkInRangeCache(filePath, chunkIndex, chunkData, tier)
			cm.maybeAdvertiseFetchedChunk(filePath, chunkPath, tier, chunkData)
			cm.touchEntries(filePath, chunkPath)
			cm.traceChunkAttempt(chunkPath, chunkIndex, tier, hybridStart, nil)
			cm.traceChunkTotal(chunkPath, chunkIndex, chunkStart, nil)
			return chunkData, tier, nil
		} else {
			cm.traceChunkAttempt(chunkPath, chunkIndex, hybridPrimary, hybridStart, err)
		}
		// If hybrid path fails, continue with strict ordered fallback below.
	}

	// Ordered fallback reads go through getFromRemoteTier so cloud pulls take
	// the cross-node single-flight lease (Phase 2): a herd of nodes missing
	// the same chunk collapses to ~one origin pull; the rest pick it up from
	// the leader via the peer tier. servedTier is where the data actually came
	// from (a lease follower asked for cloud but got peer).
	tierStart = time.Now()
	if chunkEntry, servedTier, err := cm.getFromRemoteTierWithBusyRetry(ctx, chunkPath, primary); err == nil {
		tierDur := time.Since(tierStart)
		cm.metrics.RecordHit(servedTier)
		cm.metrics.RecordTierRead(servedTier, int64(len(chunkEntry.Data)), tierDur)
		cm.recordTierPerf(servedTier, tierDur, true)
		chunkData := chunkEntry.Data
		cm.setChunkInRangeCache(filePath, chunkIndex, chunkData, servedTier)
		cm.maybeAdvertiseFetchedChunk(filePath, chunkPath, servedTier, chunkData)
		cm.touchEntries(filePath, chunkPath)
		cm.traceChunkAttempt(chunkPath, chunkIndex, servedTier, tierStart, nil)
		cm.traceChunkTotal(chunkPath, chunkIndex, chunkStart, nil)
		return chunkData, servedTier, nil
	} else {
		cm.recordTierOutcome(primary, tierStart, err)
		cm.traceChunkAttempt(chunkPath, chunkIndex, primary, tierStart, err)
	}
	cm.metrics.RecordMiss(primary)

	tierStart = time.Now()
	if chunkEntry, servedTier, err := cm.getFromRemoteTierWithBusyRetry(ctx, chunkPath, secondary); err == nil {
		tierDur := time.Since(tierStart)
		cm.metrics.RecordHit(servedTier)
		cm.metrics.RecordTierRead(servedTier, int64(len(chunkEntry.Data)), tierDur)
		cm.recordTierPerf(servedTier, tierDur, true)
		chunkData := chunkEntry.Data
		cm.setChunkInRangeCache(filePath, chunkIndex, chunkData, servedTier)
		cm.maybeAdvertiseFetchedChunk(filePath, chunkPath, servedTier, chunkData)
		cm.touchEntries(filePath, chunkPath)
		cm.traceChunkAttempt(chunkPath, chunkIndex, servedTier, tierStart, nil)
		cm.traceChunkTotal(chunkPath, chunkIndex, chunkStart, nil)
		return chunkData, servedTier, nil
	} else {
		cm.recordTierOutcome(secondary, tierStart, err)
		cm.traceChunkAttempt(chunkPath, chunkIndex, secondary, tierStart, err)
	}
	cm.metrics.RecordMiss(secondary)

	err := fmt.Errorf("failed to read chunk %d for %s from remote tiers", chunkIndex, filePath)
	cm.traceChunkTotal(chunkPath, chunkIndex, chunkStart, err)
	return nil, TierUnknown, err
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
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "file not found in tier") ||
		strings.Contains(msg, "no such file") ||
		strings.Contains(msg, "not found") ||
		strings.Contains(msg, "does not exist") ||
		strings.Contains(msg, "status code 404")
}

func (cm *DefaultCacheManager) readChunkHybridHedged(ctx context.Context, chunkPath string, primary, secondary CacheTier) (*CacheEntry, CacheTier, error) {
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

	expectedResults := 1
	if release, ok := cm.tryAcquireHedgeSlot(); ok {
		expectedResults = 2
		go func() {
			defer release()
			timer := time.NewTimer(cm.effectiveHybridHedgeDelay(primary, secondary))
			defer timer.Stop()
			select {
			case <-readCtx.Done():
				return
			case <-timer.C:
			}
			entry, err := cm.getFromTier(readCtx, chunkPath, secondary)
			resultCh <- readResult{entry: entry, tier: secondary, err: err}
		}()
	}

	var primaryErr error
	var secondaryErr error
	for i := 0; i < expectedResults; i++ {
		res := <-resultCh
		if res.err == nil {
			cancel()
			return res.entry, res.tier, nil
		}
		if res.tier == primary {
			primaryErr = res.err
		} else {
			secondaryErr = res.err
		}
	}

	if primaryErr != nil && secondaryErr != nil {
		return nil, 0, fmt.Errorf("hybrid chunk read failed (primary=%v, secondary=%v)", primaryErr, secondaryErr)
	}
	if primaryErr != nil {
		return nil, 0, primaryErr
	}
	if secondaryErr != nil {
		return nil, 0, secondaryErr
	}
	return nil, 0, errors.New("hybrid chunk read failed")
}

// fileCacheLocked returns the per-file range cache, creating it if needed.
// Callers must hold rangeMu for writing.
func (cm *DefaultCacheManager) fileCacheLocked(filePath string) *chunkFileCache {
	fileCache, ok := cm.rangeChunks[filePath]
	if !ok || fileCache == nil {
		fileCache = &chunkFileCache{
			chunks:              make(map[int64]rangeCachedChunk),
			order:               make([]int64, 0, cm.config.RangeChunkCacheSize),
			lastPrefetchTrigger: -1,
			prefetchInFlight:    make(map[int64]struct{}),
		}
		cm.rangeChunks[filePath] = fileCache
	}
	return fileCache
}

// evictOldestRangeChunkLocked drops the oldest cached chunk, counting chunks
// that were prefetched but never consumed. Callers must hold rangeMu for
// writing.
func (cm *DefaultCacheManager) evictOldestRangeChunkLocked(fileCache *chunkFileCache) {
	if len(fileCache.order) == 0 {
		return
	}
	evictChunk := fileCache.order[0]
	fileCache.order = fileCache.order[1:]
	if cached, ok := fileCache.chunks[evictChunk]; ok {
		fileCache.bytes -= int64(len(cached.data))
		cm.rangeTotalBytes -= int64(len(cached.data))
		if cached.prefetched {
			cm.metrics.PrefetchWasted.Add(1)
		}
	}
	delete(fileCache.chunks, evictChunk)
}

// dropFileCacheLocked removes a file's range cache, counting prefetched chunks
// that were never consumed. Callers must hold rangeMu for writing.
func (cm *DefaultCacheManager) dropFileCacheLocked(filePath string) {
	fileCache, ok := cm.rangeChunks[filePath]
	if ok && fileCache != nil {
		cm.rangeTotalBytes -= fileCache.bytes
		for _, cached := range fileCache.chunks {
			if cached.prefetched {
				cm.metrics.PrefetchWasted.Add(1)
			}
		}
	}
	delete(cm.rangeChunks, filePath)
}

// observeReadPattern updates per-file sequential-stream state and returns the
// readahead window (in chunks) for this read plus whether it continued a
// sequential stream. Consecutive sequential reads double the window from
// RangePrefetchChunks up to RangePrefetchMaxChunks (Linux-readahead style);
// a random access resets it to the base. A sequential stream re-reading below
// the prefetch high-water mark (e.g. a second pass over the file) re-arms
// lastPrefetchTrigger so prefetch works for repeat passes.
func (cm *DefaultCacheManager) observeReadPattern(filePath string, fileSize, offset, end, endChunk, chunkSize int64) (int, bool) {
	base := cm.config.RangePrefetchChunks
	if base <= 0 || chunkSize <= 0 {
		return 0, false
	}
	// Per-file profile: throughput-derived window, gated by size class
	// (small files get no readahead, medium files can't reserve more than
	// their own length). Static config remains the floor for large files.
	profile := cm.readProfileFor(fileSize, chunkSize)
	maxChunks := profile.prefetchMaxChunks
	if maxChunks <= 0 {
		return 0, false
	}
	if maxChunks < base {
		maxChunks = base
	}

	cm.rangeMu.Lock()
	fileCache := cm.fileCacheLocked(filePath)
	fileCache.touchLocked(time.Now())
	sequential := fileCache.lastReadEnd > 0 &&
		offset >= fileCache.lastReadEnd-chunkSize &&
		offset <= fileCache.lastReadEnd+chunkSize
	if sequential {
		fileCache.seqStreak++
		if endChunk <= fileCache.lastPrefetchTrigger {
			fileCache.lastPrefetchTrigger = endChunk - 1
		}
	} else {
		fileCache.seqStreak = 0
	}
	fileCache.lastReadEnd = end
	streak := fileCache.seqStreak
	cm.rangeMu.Unlock()

	// Confirmed long sequential stream (checkpoint restore shape): allow the
	// window past the bandwidth-delay target, still under the byte budget.
	if streak >= longStreakThreshold {
		widened := maxChunks * longStreakMultiplier
		if budget := cm.config.RangePrefetchMaxBytes; budget > 0 {
			if byBudget := int(budget / chunkSize); widened > byBudget {
				widened = byBudget
			}
		}
		if fileChunks := int((fileSize + chunkSize - 1) / chunkSize); widened > fileChunks {
			widened = fileChunks
		}
		if widened > maxChunks {
			maxChunks = widened
		}
	}

	window := base
	for i := 0; i < streak && window < maxChunks; i++ {
		window *= 2
	}
	if window > maxChunks {
		window = maxChunks
	}
	return window, sequential
}

func (cm *DefaultCacheManager) getChunkFromRangeCache(filePath string, chunkIndex int64) ([]byte, CacheTier, bool) {
	cm.rangeMu.RLock()
	fileCache, ok := cm.rangeChunks[filePath]
	if !ok || fileCache == nil {
		cm.rangeMu.RUnlock()
		return nil, TierUnknown, false
	}
	cached, ok := fileCache.chunks[chunkIndex]
	cm.rangeMu.RUnlock()
	if !ok {
		return nil, TierUnknown, false
	}
	if cached.prefetched {
		cm.rangeMu.Lock()
		if fileCache, ok := cm.rangeChunks[filePath]; ok && fileCache != nil {
			if cur, ok := fileCache.chunks[chunkIndex]; ok && cur.prefetched {
				cur.prefetched = false
				fileCache.chunks[chunkIndex] = cur
				cm.metrics.PrefetchHits.Add(1)
			}
		}
		cm.rangeMu.Unlock()
	}
	return cached.data, cached.tier, true
}

// markChunkPrefetched flags a range-cached chunk as populated by readahead so
// consumption (hit) or eviction without consumption (waste) can be counted.
func (cm *DefaultCacheManager) markChunkPrefetched(filePath string, chunkIndex int64) {
	cm.rangeMu.Lock()
	if fileCache, ok := cm.rangeChunks[filePath]; ok && fileCache != nil {
		if cached, ok := fileCache.chunks[chunkIndex]; ok && !cached.prefetched {
			cached.prefetched = true
			fileCache.chunks[chunkIndex] = cached
		}
	}
	cm.rangeMu.Unlock()
}

func (cm *DefaultCacheManager) setChunkInRangeCache(filePath string, chunkIndex int64, data []byte, tier CacheTier) {
	if len(data) == 0 {
		return
	}

	cm.rangeMu.Lock()
	fileCache := cm.fileCacheLocked(filePath)
	fileCache.touchLocked(time.Now())
	if existing, exists := fileCache.chunks[chunkIndex]; exists {
		fileCache.bytes -= int64(len(existing.data))
		cm.rangeTotalBytes -= int64(len(existing.data))
	} else {
		fileCache.order = append(fileCache.order, chunkIndex)
	}
	fileCache.chunks[chunkIndex] = rangeCachedChunk{
		data: data,
		tier: tier,
	}
	fileCache.bytes += int64(len(data))
	cm.rangeTotalBytes += int64(len(data))

	for len(fileCache.order) > cm.config.RangeChunkCacheSize {
		cm.evictOldestRangeChunkLocked(fileCache)
	}
	for fileCache.bytes > cm.config.RangeChunkCacheMaxBytes && len(fileCache.order) > 0 {
		cm.evictOldestRangeChunkLocked(fileCache)
	}
	cm.enforceGlobalRangeBudgetLocked(filePath)
	snapshotChunks := len(fileCache.order)
	snapshotBytes := fileCache.bytes
	shouldLog := false
	if snapshotChunks > 0 && (snapshotChunks == 1 || snapshotChunks%32 == 0 || snapshotChunks == cm.config.RangeChunkCacheSize) {
		shouldLog = true
	}
	cm.rangeMu.Unlock()

	if shouldLog {
		allocMB, heapInuseMB := heapStatsMB()
		cm.logger.Printf("Range cache snapshot path=%s chunks=%d bytes=%d chunk_size_mb=%.1f cache_limit_chunks=%d cache_limit_mb=%.1f alloc_mb=%.1f heap_inuse_mb=%.1f goroutines=%d last_chunk=%d tier=%s",
			filePath,
			snapshotChunks,
			snapshotBytes,
			float64(len(data))/(1024*1024),
			cm.config.RangeChunkCacheSize,
			float64(cm.config.RangeChunkCacheMaxBytes)/(1024*1024),
			allocMB,
			heapInuseMB,
			runtime.NumGoroutine(),
			chunkIndex,
			cacheTierToStorageTier(tier),
		)
	}
}

func (cm *DefaultCacheManager) rangeCacheStats(filePath string) (int, int64) {
	cm.rangeMu.RLock()
	defer cm.rangeMu.RUnlock()

	fileCache, ok := cm.rangeChunks[filePath]
	if !ok || fileCache == nil {
		return 0, 0
	}
	return len(fileCache.order), fileCache.bytes
}

func (cm *DefaultCacheManager) RangeCacheSnapshot() RangeCacheSnapshot {
	cm.rangeMu.RLock()
	defer cm.rangeMu.RUnlock()

	snap := RangeCacheSnapshot{
		Files: len(cm.rangeChunks),
	}
	for _, fileCache := range cm.rangeChunks {
		if fileCache == nil {
			continue
		}
		snap.Chunks += len(fileCache.order)
		snap.Bytes += fileCache.bytes
		snap.PrefetchBytes += fileCache.prefetchBytes
		snap.PrefetchInFlight += len(fileCache.prefetchInFlight)
	}
	return snap
}

// TierPerfSnapshot exposes the adaptive remote-read tracker's per-tier EWMA
// latency (ms), success ratio (0..1), sample count, and the currently chosen
// primary remote tier ("peer", "cloud", or "none" before enough samples).
type TierPerfSnapshot struct {
	PeerLatencyMs  float64
	PeerSuccess    float64
	PeerSamples    int64
	CloudLatencyMs float64
	CloudSuccess   float64
	CloudSamples   int64
	PrimaryTier    string
	// PeerMisses/CloudMisses count reads a tier declined because it did not
	// hold the object. These are deliberately excluded from the EWMAs above,
	// which leaves a zero sample count ambiguous between "no traffic" and
	// "all misses". Counting them separately keeps that distinction visible
	// instead of trading one silent metric for another.
	PeerMisses  int64
	CloudMisses int64
}

func (cm *DefaultCacheManager) TierPerfSnapshot() TierPerfSnapshot {
	if cm.tierPerf == nil {
		return TierPerfSnapshot{PrimaryTier: "none"}
	}
	pl, ps, pn, cl, cs, cn := cm.tierPerf.snapshot()
	peerMisses, cloudMisses := cm.tierPerf.missCounts()
	primary := "none"
	if pn >= tierPerfMinSamples && cn >= tierPerfMinSamples {
		primary = cacheTierToStorageTier(cm.tierPerf.order()[0])
	}
	return TierPerfSnapshot{
		PeerLatencyMs:  pl,
		PeerSuccess:    ps,
		PeerSamples:    pn,
		CloudLatencyMs: cl,
		CloudSuccess:   cs,
		CloudSamples:   cn,
		PrimaryTier:    primary,
		PeerMisses:     peerMisses,
		CloudMisses:    cloudMisses,
	}
}

func (cm *DefaultCacheManager) reservePrefetchChunks(filePath string, endChunk int64, numChunks int64, chunkSize int64, want int) ([]int64, int64, int64) {
	if want <= 0 || chunkSize <= 0 || numChunks <= 0 {
		_, rangeBytes := cm.rangeCacheStats(filePath)
		return nil, 0, rangeBytes
	}

	cm.rangeMu.Lock()
	fileCache := cm.fileCacheLocked(filePath)
	if endChunk <= fileCache.lastPrefetchTrigger {
		rangeBytes := fileCache.bytes
		cm.rangeMu.Unlock()
		return nil, cm.config.RangePrefetchMaxBytes - fileCache.prefetchBytes, rangeBytes
	}
	fileCache.lastPrefetchTrigger = endChunk
	rangeBytes := fileCache.bytes
	remainingBudget := cm.config.RangePrefetchMaxBytes - fileCache.prefetchBytes
	reserved := make([]int64, 0, want)
	for i := 1; i <= want; i++ {
		prefetchChunk := endChunk + int64(i)
		if prefetchChunk >= numChunks {
			break
		}
		if remainingBudget < chunkSize {
			break
		}
		if _, ok := fileCache.chunks[prefetchChunk]; ok {
			continue
		}
		if _, ok := fileCache.prefetchInFlight[prefetchChunk]; ok {
			continue
		}
		fileCache.prefetchInFlight[prefetchChunk] = struct{}{}
		fileCache.prefetchBytes += chunkSize
		remainingBudget -= chunkSize
		reserved = append(reserved, prefetchChunk)
	}
	rangeBytes = fileCache.bytes
	cm.rangeMu.Unlock()
	return reserved, remainingBudget, rangeBytes
}

func (cm *DefaultCacheManager) releasePrefetchReservation(filePath string, chunkIndex int64, chunkSize int64) {
	cm.rangeMu.Lock()
	fileCache, ok := cm.rangeChunks[filePath]
	if ok && fileCache != nil {
		if _, exists := fileCache.prefetchInFlight[chunkIndex]; exists {
			delete(fileCache.prefetchInFlight, chunkIndex)
			fileCache.prefetchBytes -= chunkSize
			if fileCache.prefetchBytes < 0 {
				fileCache.prefetchBytes = 0
			}
		}
	}
	cm.rangeMu.Unlock()
}

func (cm *DefaultCacheManager) logReadRangeSnapshot(
	phase string,
	filePath string,
	offset int64,
	size int,
	startChunk int64,
	endChunk int64,
	workerCount int,
	hybridRead bool,
	err error,
) {
	rangeChunks, rangeBytes := cm.rangeCacheStats(filePath)
	allocMB, heapInuseMB := heapStatsMB()
	if err != nil {
		cm.logger.Printf("ReadRange snapshot phase=%s path=%s offset=%d size=%d start_chunk=%d end_chunk=%d workers=%d hybrid=%t range_cache_chunks=%d range_cache_mb=%.1f alloc_mb=%.1f heap_inuse_mb=%.1f goroutines=%d err=%v",
			phase,
			filePath,
			offset,
			size,
			startChunk,
			endChunk,
			workerCount,
			hybridRead,
			rangeChunks,
			float64(rangeBytes)/(1024*1024),
			allocMB,
			heapInuseMB,
			runtime.NumGoroutine(),
			err,
		)
		return
	}
	cm.logger.Printf("ReadRange snapshot phase=%s path=%s offset=%d size=%d start_chunk=%d end_chunk=%d workers=%d hybrid=%t range_cache_chunks=%d range_cache_mb=%.1f alloc_mb=%.1f heap_inuse_mb=%.1f goroutines=%d",
		phase,
		filePath,
		offset,
		size,
		startChunk,
		endChunk,
		workerCount,
		hybridRead,
		rangeChunks,
		float64(rangeBytes)/(1024*1024),
		allocMB,
		heapInuseMB,
		runtime.NumGoroutine(),
	)
}

func (cm *DefaultCacheManager) invalidateRangeCache(filePath string) {
	// The verification record is keyed on (size, mtime), so replaced content
	// would not match it anyway; dropping it here keeps the two caches
	// invalidated by the same call rather than relying on that.
	cm.nvmeVerify.forget(filePath)
	if parent, ok := parentFilePathFromChunkPath(filePath); ok {
		cm.nvmeVerify.forget(parent)
	}

	cm.rangeMu.Lock()
	cm.dropFileCacheLocked(filePath)
	if parent, ok := parentFilePathFromChunkPath(filePath); ok {
		cm.dropFileCacheLocked(parent)
	}
	cm.rangeMu.Unlock()

	cm.mu.Lock()
	delete(cm.hybridHints, filePath)
	if parent, ok := parentFilePathFromChunkPath(filePath); ok {
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

func readWallTierBytes(tier CacheTier, bytes int64) (int64, int64, int64) {
	if bytes <= 0 {
		return 0, 0, 0
	}
	switch tier {
	case TierNVMe:
		return bytes, 0, 0
	case TierPeer:
		return 0, bytes, 0
	case TierCloud:
		return 0, 0, bytes
	default:
		return 0, 0, 0
	}
}

// Stats returns current NVMe usage and capacity
func (cm *DefaultCacheManager) Stats() (used, capacity int64) {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	return cm.nvmeUsed, cm.config.MaxNVMeSize
}

// GetCacheMetrics exposes the raw counter struct (for Prometheus export of
// counters not in the Snapshot map).
func (cm *DefaultCacheManager) GetCacheMetrics() *CacheMetrics {
	return cm.metrics
}

// GetMetrics returns the cache metrics for reporting.
func (cm *DefaultCacheManager) GetMetrics() map[string]interface{} {
	return cm.metrics.Snapshot()
}

// CacheMetrics tracks cache performance counters using atomic operations
type CacheMetrics struct {
	NVMeHits       atomic.Int64
	NVMeMisses     atomic.Int64
	PeerHits       atomic.Int64
	PeerMisses     atomic.Int64
	CloudHits      atomic.Int64
	CloudMisses    atomic.Int64
	WriteCount     atomic.Int64
	WriteBytes     atomic.Int64
	EvictionCount  atomic.Int64
	NVMeReadBytes  atomic.Int64
	NVMeReadNanos  atomic.Int64
	NVMeReadOps    atomic.Int64
	PeerReadBytes  atomic.Int64
	PeerReadNanos  atomic.Int64
	PeerReadOps    atomic.Int64
	CloudReadBytes atomic.Int64
	CloudReadNanos atomic.Int64
	CloudReadOps   atomic.Int64
	ReadWallBytes  atomic.Int64
	ReadWallNanos  atomic.Int64
	ReadWallOps    atomic.Int64
	NVMeReadWall   atomic.Int64
	PeerReadWall   atomic.Int64
	CloudReadWall  atomic.Int64
	ReadWallOther  atomic.Int64
	// Readahead effectiveness: chunks launched, consumed by a foreground read,
	// and evicted/invalidated without ever being read.
	PrefetchIssued atomic.Int64
	PrefetchHits   atomic.Int64
	PrefetchWasted atomic.Int64
	// Global range-cache budget accounting: whole-file cache evictions from
	// the all-files byte budget, and idle-expiry sweeps.
	RangeCacheFileEvictions atomic.Int64
	RangeCacheIdleExpiries  atomic.Int64
	// Chunk completion: whole files assembled, hole chunks fetched, passes
	// skipped for NVMe headroom.
	ChunkCompletionAssembled atomic.Int64
	ChunkCompletionFetched   atomic.Int64
	ChunkCompletionSkipped   atomic.Int64
	// Eviction durability guard + watermark evictor.
	EvictionSkippedUnpersisted atomic.Int64
	WatermarkEvictorRuns       atomic.Int64
	WatermarkEvictedBytes      atomic.Int64
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

// RecordTierRead records successful read bytes/duration for a tier.
func (m *CacheMetrics) RecordTierRead(tier CacheTier, bytes int64, dur time.Duration) {
	if bytes <= 0 {
		return
	}
	nanos := dur.Nanoseconds()
	if nanos < 0 {
		nanos = 0
	}
	switch tier {
	case TierNVMe:
		m.NVMeReadBytes.Add(bytes)
		m.NVMeReadNanos.Add(nanos)
		m.NVMeReadOps.Add(1)
	case TierPeer:
		m.PeerReadBytes.Add(bytes)
		m.PeerReadNanos.Add(nanos)
		m.PeerReadOps.Add(1)
	case TierCloud:
		m.CloudReadBytes.Add(bytes)
		m.CloudReadNanos.Add(nanos)
		m.CloudReadOps.Add(1)
	}
}

// RecordReadWall records end-to-end read wall time and per-tier byte
// contributions for a single read operation.
func (m *CacheMetrics) RecordReadWall(totalBytes int64, dur time.Duration, nvmeBytes int64, peerBytes int64, cloudBytes int64) {
	if totalBytes <= 0 {
		return
	}
	nanos := dur.Nanoseconds()
	if nanos < 0 {
		nanos = 0
	}
	if nvmeBytes < 0 {
		nvmeBytes = 0
	}
	if peerBytes < 0 {
		peerBytes = 0
	}
	if cloudBytes < 0 {
		cloudBytes = 0
	}
	sum := nvmeBytes + peerBytes + cloudBytes
	if sum > totalBytes {
		scale := float64(totalBytes) / float64(sum)
		nvmeBytes = int64(float64(nvmeBytes) * scale)
		peerBytes = int64(float64(peerBytes) * scale)
		cloudBytes = int64(float64(cloudBytes) * scale)
		sum = nvmeBytes + peerBytes + cloudBytes
	}
	otherBytes := totalBytes - sum

	m.ReadWallBytes.Add(totalBytes)
	m.ReadWallNanos.Add(nanos)
	m.ReadWallOps.Add(1)
	m.NVMeReadWall.Add(nvmeBytes)
	m.PeerReadWall.Add(peerBytes)
	m.CloudReadWall.Add(cloudBytes)
	if otherBytes > 0 {
		m.ReadWallOther.Add(otherBytes)
	}
}

// TierReadMBps returns the observed aggregate throughput for a tier.
func (m *CacheMetrics) TierReadMBps(tier CacheTier) float64 {
	switch tier {
	case TierNVMe:
		return bytesPerSecToMBps(m.NVMeReadBytes.Load(), m.NVMeReadNanos.Load())
	case TierPeer:
		return bytesPerSecToMBps(m.PeerReadBytes.Load(), m.PeerReadNanos.Load())
	case TierCloud:
		return bytesPerSecToMBps(m.CloudReadBytes.Load(), m.CloudReadNanos.Load())
	default:
		return 0
	}
}

// Snapshot returns a point-in-time snapshot of all metrics
func (m *CacheMetrics) Snapshot() map[string]interface{} {
	nvmeReadBytes := m.NVMeReadBytes.Load()
	nvmeReadNanos := m.NVMeReadNanos.Load()
	peerReadBytes := m.PeerReadBytes.Load()
	peerReadNanos := m.PeerReadNanos.Load()
	cloudReadBytes := m.CloudReadBytes.Load()
	cloudReadNanos := m.CloudReadNanos.Load()
	readWallBytes := m.ReadWallBytes.Load()
	readWallNanos := m.ReadWallNanos.Load()
	nvmeReadWall := m.NVMeReadWall.Load()
	peerReadWall := m.PeerReadWall.Load()
	cloudReadWall := m.CloudReadWall.Load()
	readWallOther := m.ReadWallOther.Load()

	return map[string]interface{}{
		"nvme_hits":                    m.NVMeHits.Load(),
		"nvme_misses":                  m.NVMeMisses.Load(),
		"peer_hits":                    m.PeerHits.Load(),
		"peer_misses":                  m.PeerMisses.Load(),
		"cloud_hits":                   m.CloudHits.Load(),
		"cloud_misses":                 m.CloudMisses.Load(),
		"write_count":                  m.WriteCount.Load(),
		"write_bytes":                  m.WriteBytes.Load(),
		"eviction_count":               m.EvictionCount.Load(),
		"nvme_read_bytes":              nvmeReadBytes,
		"nvme_read_nanos":              nvmeReadNanos,
		"nvme_read_ops":                m.NVMeReadOps.Load(),
		"peer_read_bytes":              peerReadBytes,
		"peer_read_nanos":              peerReadNanos,
		"peer_read_ops":                m.PeerReadOps.Load(),
		"cloud_read_bytes":             cloudReadBytes,
		"cloud_read_nanos":             cloudReadNanos,
		"cloud_read_ops":               m.CloudReadOps.Load(),
		"nvme_read_mbps":               bytesPerSecToMBps(nvmeReadBytes, nvmeReadNanos),
		"peer_read_mbps":               bytesPerSecToMBps(peerReadBytes, peerReadNanos),
		"cloud_read_mbps":              bytesPerSecToMBps(cloudReadBytes, cloudReadNanos),
		"read_wall_bytes":              readWallBytes,
		"read_wall_nanos":              readWallNanos,
		"read_wall_ops":                m.ReadWallOps.Load(),
		"read_wall_mbps":               bytesPerSecToMBps(readWallBytes, readWallNanos),
		"nvme_read_wall_bytes":         nvmeReadWall,
		"peer_read_wall_bytes":         peerReadWall,
		"cloud_read_wall_bytes":        cloudReadWall,
		"read_wall_other_bytes":        readWallOther,
		"prefetch_issued":              m.PrefetchIssued.Load(),
		"prefetch_hits":                m.PrefetchHits.Load(),
		"prefetch_wasted":              m.PrefetchWasted.Load(),
		"range_cache_file_evictions":   m.RangeCacheFileEvictions.Load(),
		"range_cache_idle_expiries":    m.RangeCacheIdleExpiries.Load(),
		"chunk_completion_assembled":   m.ChunkCompletionAssembled.Load(),
		"chunk_completion_fetched":     m.ChunkCompletionFetched.Load(),
		"chunk_completion_skipped":     m.ChunkCompletionSkipped.Load(),
		"eviction_skipped_unpersisted": m.EvictionSkippedUnpersisted.Load(),
		"watermark_evictor_runs":       m.WatermarkEvictorRuns.Load(),
		"watermark_evicted_bytes":      m.WatermarkEvictedBytes.Load(),
		"nvme_read_wall_mbps":          bytesPerSecToMBps(nvmeReadWall, readWallNanos),
		"peer_read_wall_mbps":          bytesPerSecToMBps(peerReadWall, readWallNanos),
		"cloud_read_wall_mbps":         bytesPerSecToMBps(cloudReadWall, readWallNanos),
		"read_wall_other_mbps":         bytesPerSecToMBps(readWallOther, readWallNanos),
	}
}

func bytesPerSecToMBps(bytes, nanos int64) float64 {
	if bytes <= 0 || nanos <= 0 {
		return 0
	}
	seconds := float64(nanos) / float64(time.Second)
	if seconds <= 0 {
		return 0
	}
	return (float64(bytes) / (1024.0 * 1024.0)) / seconds
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

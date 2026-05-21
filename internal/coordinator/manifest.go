package coordinator

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sort"
	"sync"
	"time"
)

// CloudReader reads file bytes from durable object storage (S3/Azure/GCS).
// cache.TierStorage satisfies this. The coordinator package defines a narrow
// interface here rather than importing the cache package to avoid an import
// cycle.
type CloudReader interface {
	Read(ctx context.Context, path string) ([]byte, error)
}

// FileManifest is a portable description of the file set that should exist in
// peer caches. It is written by /api/snapshot and consumed by /api/restore to
// rehydrate peer NVMe caches from cloud storage after the coordinator (or a
// peer's local disk) has been wiped.
type FileManifest struct {
	GeneratedAt time.Time       `json:"generated_at"`
	Files       []ManifestEntry `json:"files"`
}

// ManifestEntry captures the minimum information needed to fetch a file back
// from cloud storage and re-seed it onto peer caches. ChunkCount is only set
// when IsChunked is true; restore fetches each `<path>_chunk_<i>` separately.
type ManifestEntry struct {
	Path       string `json:"path"`
	Size       int64  `json:"size"`
	IsChunked  bool   `json:"is_chunked,omitempty"`
	ChunkCount int    `json:"chunk_count,omitempty"`
}

// BuildManifest scans all file locations the coordinator currently knows about
// and produces a deduplicated manifest (one entry per unique path).
func (cs *CoordinatorService) BuildManifest(ctx context.Context) (*FileManifest, error) {
	fileMap, err := cs.store.RangeFileLocations(ctx, "")
	if err != nil {
		return nil, err
	}
	paths := make([]string, 0, len(fileMap))
	for path := range fileMap {
		paths = append(paths, path)
	}
	sort.Strings(paths)

	entries := make([]ManifestEntry, 0, len(paths))
	for _, path := range paths {
		locs := fileMap[path]
		if len(locs) == 0 {
			continue
		}
		entry := ManifestEntry{Path: path}
		for _, loc := range locs {
			if loc == nil {
				continue
			}
			if loc.FileSize > entry.Size {
				entry.Size = loc.FileSize
			}
			if loc.IsChunked {
				entry.IsChunked = true
				if n := len(loc.Chunks); n > entry.ChunkCount {
					entry.ChunkCount = n
				}
			}
		}
		entries = append(entries, entry)
	}
	return &FileManifest{
		GeneratedAt: time.Now(),
		Files:       entries,
	}, nil
}

// RestoreOptions controls RestoreFromManifest.
type RestoreOptions struct {
	// SeedPercentage of active peers to seed each file onto (1..100). Defaults
	// to 100 (warm every active peer).
	SeedPercentage int
	// TimeoutSeconds bounds the whole restore. Defaults to 600.
	TimeoutSeconds int
	// Concurrency caps parallel file restores. Defaults to 4.
	Concurrency int
}

// RestoreResult summarizes a manifest restore.
type RestoreResult struct {
	GeneratedAt time.Time `json:"generated_at"`
	TotalFiles  int       `json:"total_files"`
	SeededFiles int       `json:"seeded_files"`
	FailedFiles []string  `json:"failed_files,omitempty"`
}

// RestoreFromManifest fetches every file in the manifest from cloud storage
// and seeds it onto a percentage of active peers (matching SeedPathToPeers
// semantics). Cloud is treated as the durable source of truth; peers are
// caches being re-warmed.
func (cs *CoordinatorService) RestoreFromManifest(ctx context.Context, manifest *FileManifest, cloud CloudReader, opts RestoreOptions) (*RestoreResult, error) {
	if manifest == nil {
		return nil, errors.New("manifest is required")
	}
	if cloud == nil {
		return nil, errors.New("cloud reader is required")
	}

	pct := opts.SeedPercentage
	if pct <= 0 || pct > 100 {
		pct = 100
	}
	timeout := time.Duration(opts.TimeoutSeconds) * time.Second
	if timeout <= 0 {
		timeout = 10 * time.Minute
	}
	concurrency := opts.Concurrency
	if concurrency <= 0 {
		concurrency = 4
	}

	callCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	result := &RestoreResult{
		GeneratedAt: time.Now(),
		TotalFiles:  len(manifest.Files),
	}

	sem := make(chan struct{}, concurrency)
	var (
		mu sync.Mutex
		wg sync.WaitGroup
	)
	for _, entry := range manifest.Files {
		entry := entry
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			if err := cs.restoreOneFile(callCtx, entry, cloud, pct); err != nil {
				mu.Lock()
				result.FailedFiles = append(result.FailedFiles, fmt.Sprintf("%s: %v", entry.Path, err))
				mu.Unlock()
				cs.logger.Printf("restore %s: %v", entry.Path, err)
				return
			}
			mu.Lock()
			result.SeededFiles++
			mu.Unlock()
		}()
	}
	wg.Wait()
	return result, nil
}

func (cs *CoordinatorService) restoreOneFile(ctx context.Context, entry ManifestEntry, cloud CloudReader, seedPct int) error {
	allPeers, err := cs.store.ListPeers(ctx)
	if err != nil {
		return fmt.Errorf("list peers: %w", err)
	}
	activeIDs := make([]string, 0, len(allPeers))
	peerByID := make(map[string]*PeerInfo, len(allPeers))
	for _, p := range allPeers {
		if p != nil && p.Status == "active" {
			activeIDs = append(activeIDs, p.ID)
			peerByID[p.ID] = p
		}
	}
	sort.Strings(activeIDs)
	if len(activeIDs) == 0 {
		return errors.New("no active peers to seed")
	}
	targetCount := int(math.Ceil(float64(len(activeIDs)) * float64(seedPct) / 100.0))
	if targetCount < 1 {
		targetCount = 1
	}
	if targetCount > len(activeIDs) {
		targetCount = len(activeIDs)
	}
	targetIDs := activeIDs[:targetCount]

	// Chunked files live in cloud at <path>_chunk_<i>; restore each chunk to
	// the same peer set so peers end up with a complete file.
	var cloudPaths []string
	if entry.IsChunked && entry.ChunkCount > 0 {
		cloudPaths = make([]string, 0, entry.ChunkCount)
		for i := 0; i < entry.ChunkCount; i++ {
			cloudPaths = append(cloudPaths, fmt.Sprintf("%s_chunk_%d", entry.Path, i))
		}
	} else {
		cloudPaths = []string{entry.Path}
	}

	for _, cloudPath := range cloudPaths {
		data, err := cloud.Read(ctx, cloudPath)
		if err != nil {
			return fmt.Errorf("read cloud %s: %w", cloudPath, err)
		}
		anySuccess := false
		for _, targetID := range targetIDs {
			target := peerByID[targetID]
			if target == nil {
				continue
			}
			if err := putFileToPeer(ctx, target.Address, cloudPath, data); err != nil {
				cs.logger.Printf("seed %s -> %s: %v", cloudPath, targetID, err)
				continue
			}
			anySuccess = true
		}
		if !anySuccess {
			return fmt.Errorf("no peer accepted %s", cloudPath)
		}
	}
	return nil
}

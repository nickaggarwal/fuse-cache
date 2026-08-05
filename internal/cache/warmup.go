package cache

import (
	"context"
	"fmt"
	"path"
	"sort"
	"strings"
	"sync"
	"time"
)

// Declarative cache warmup (the Fluid/NetEase pattern: prefetch model bytes
// before the workload reads them, instead of paying peer/cloud latency on
// first access).
//
// A CSI session whose volume context sets cachePolicy warmup=full asks this
// node to make every known file under the session's subtree fully local on
// NVMe. Enumeration comes from the coordinator's file-location metadata;
// fetching reuses the tiered read machinery (peer-first with busy failover,
// cloud fallback), so warmup rides the same herd controls as foreground
// reads. warmup=metadata only enumerates — it verifies the manifest is
// reachable and reports what a full warmup would pull, moving no data.
//
// Warmup is advisory, like all coordination here: it never evicts to make
// room (headroom-guarded per file), and failures leave the session serving
// normally through just-in-time pulls.

const (
	// warmupFileConcurrency bounds parallel file warms. Chunked files already
	// fan out internally (chunkCompletionFetchWorkers), so this stays small
	// to keep warmup a background citizen next to foreground reads.
	warmupFileConcurrency = 2
)

// WarmupResult summarizes one WarmPrefix pass.
type WarmupResult struct {
	Mode         string
	Files        int   // distinct files found under the prefix
	Warmed       int   // files newly made local
	AlreadyLocal int   // files already whole on NVMe
	Skipped      int   // headroom guard or already being assembled
	Failed       int   // fetch/assembly errors
	Bytes        int64 // bytes newly landed
}

// warmupTarget is one file to warm, aggregated across its coordinator
// locations (different holders may disagree on size mid-write; take the max,
// matching BuildManifest semantics).
type warmupTarget struct {
	path      string
	size      int64
	isChunked bool
	numChunks int64
}

// WarmPrefix prefetches every file the coordinator knows under prefix into
// local NVMe. mode is the session CachePolicy warmup value: "none"/"" is a
// no-op, "metadata" enumerates only, "full" pulls bytes.
func (cm *DefaultCacheManager) WarmPrefix(ctx context.Context, prefix, mode string) (*WarmupResult, error) {
	mode = strings.ToLower(strings.TrimSpace(mode))
	res := &WarmupResult{Mode: mode}
	if mode == "" || mode == "none" {
		return res, nil
	}
	if mode != "metadata" && mode != "full" {
		return res, fmt.Errorf("unknown warmup mode %q", mode)
	}
	if cm.config.Coordinator == nil {
		return res, fmt.Errorf("warmup needs a coordinator for enumeration")
	}
	prefix = path.Clean("/" + strings.TrimSpace(prefix))

	locations, err := cm.config.Coordinator.ListFileLocations(ctx, prefix)
	if err != nil {
		return res, fmt.Errorf("list files under %s: %w", prefix, err)
	}

	byPath := make(map[string]*warmupTarget)
	for _, loc := range locations {
		if loc == nil || loc.FilePath == "" {
			continue
		}
		// Chunks warm with their parent; parents are what get published.
		if _, isChunk := parentFilePathFromChunkPath(loc.FilePath); isChunk {
			continue
		}
		t := byPath[loc.FilePath]
		if t == nil {
			t = &warmupTarget{path: loc.FilePath}
			byPath[loc.FilePath] = t
		}
		if loc.FileSize > t.size {
			t.size = loc.FileSize
		}
		if loc.IsChunked {
			t.isChunked = true
			if n := int64(len(loc.Chunks)); n > t.numChunks {
				t.numChunks = n
			}
		}
	}
	targets := make([]*warmupTarget, 0, len(byPath))
	for _, t := range byPath {
		targets = append(targets, t)
	}
	sort.Slice(targets, func(i, j int) bool { return targets[i].path < targets[j].path })
	res.Files = len(targets)

	if mode == "metadata" || len(targets) == 0 {
		return res, nil
	}

	var (
		mu sync.Mutex
		wg sync.WaitGroup
	)
	sem := make(chan struct{}, warmupFileConcurrency)
	for _, t := range targets {
		if ctx.Err() != nil {
			break
		}
		t := t
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			outcome := cm.warmOneFile(ctx, t)
			mu.Lock()
			switch outcome {
			case warmOutcomeWarmed:
				res.Warmed++
				res.Bytes += t.size
			case warmOutcomeAlreadyLocal:
				res.AlreadyLocal++
			case warmOutcomeSkipped:
				res.Skipped++
			case warmOutcomeFailed:
				res.Failed++
			}
			mu.Unlock()
		}()
	}
	wg.Wait()
	return res, nil
}

type warmOutcome int

const (
	warmOutcomeWarmed warmOutcome = iota
	warmOutcomeAlreadyLocal
	warmOutcomeSkipped
	warmOutcomeFailed
)

func (cm *DefaultCacheManager) warmOneFile(ctx context.Context, t *warmupTarget) warmOutcome {
	if _, whole := cm.LocalFilePath(ctx, t.path); whole {
		return warmOutcomeAlreadyLocal
	}
	// Headroom guard: warmup is opportunistic and must never force eviction.
	if used, capacity := cm.Stats(); capacity > 0 && t.size > 0 && used+t.size > capacity {
		cm.logger.Printf("Warmup skipped %s: needs %d bytes headroom (used %d / cap %d)",
			t.path, t.size, used, capacity)
		return warmOutcomeSkipped
	}

	if t.isChunked && t.numChunks > 0 && t.size > 0 {
		if !cm.tryStartCompletion(t.path) {
			// A completion pass is already assembling it — that IS the warmup.
			return warmOutcomeSkipped
		}
		defer cm.endCompletion(t.path)
		if err := cm.runChunkCompletion(ctx, t.path, t.numChunks, t.size); err != nil {
			cm.logger.Printf("Warmup of %s failed: %v", t.path, err)
			return warmOutcomeFailed
		}
		return warmOutcomeWarmed
	}

	// Whole (unchunked) file: fetch from the remote tiers and land on NVMe.
	if err := cm.fetchAndLandChunk(ctx, t.path); err != nil {
		cm.logger.Printf("Warmup of %s failed: %v", t.path, err)
		return warmOutcomeFailed
	}
	size := t.size
	if size <= 0 {
		if s, err := cm.nvmeStorage.Size(ctx, t.path); err == nil {
			size = s
		}
	}
	// Advertise this node as a holder so peers can read the warmed copy.
	cm.goBackground(func() {
		cm.publishFileLocation(cm.shutdownCtx, &CacheEntry{
			FilePath: t.path, StoragePath: t.path, Size: size,
			LastAccessed: time.Now(),
		}, TierNVMe)
	})
	return warmOutcomeWarmed
}

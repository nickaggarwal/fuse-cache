package cache

import (
	"context"
	"fmt"
	"path"
	"strings"
	"sync"
)

// Manifest-driven warmup. WarmPrefixOpts enumerates a prefix from the LOCAL
// coordinator's file locations, so a path that exists only in the global
// index (published by another cluster, resolvable by a forced lookup with
// -global-read, invisible to readdir) is simply not enumerated: the warm
// "succeeds" with files=0. A caller that needs to know a release is local --
// sky-control's model-build gate -- must not be able to pass on that.
//
// WarmFiles takes the caller's verified file list instead. Every listed path
// is resolved by a forced single-path lookup (local entry, point lookup,
// subtree, then the global mirror), and the per-file outcome comes back, so
// "expected N, found N, all warmed or already local" is checkable and a path
// nobody can resolve is reported as not_found rather than silently absent.

// WarmFile is one file a caller expects to be warmed. Size is optional; when
// set, a resolved size that disagrees is a failure, because warming a
// different object than the manifest describes is not a warm.
type WarmFile struct {
	Path string `json:"path"`
	Size int64  `json:"size,omitempty"`
}

// Per-file warm outcomes.
const (
	WarmFileWarmed       = "warmed"
	WarmFileAlreadyLocal = "already_local"
	WarmFileSkipped      = "skipped"
	WarmFileFailed       = "failed"
	WarmFileNotFound     = "not_found"
	WarmFileSizeMismatch = "size_mismatch"
)

// WarmFileResult is one file's outcome.
type WarmFileResult struct {
	Path   string `json:"path"`
	Status string `json:"status"`
	// Size is the resolved size (0 when not found).
	Size int64 `json:"size"`
	// Source is where the shape was learned: "local", "coordinator" or
	// "global" ("" when not found).
	Source string `json:"source,omitempty"`
	Error  string `json:"error,omitempty"`
}

// WarmFilesResult is a manifest warm. Complete is true only when every listed
// file is warmed or already local: the property a readiness gate checks.
type WarmFilesResult struct {
	Mode         string           `json:"mode"`
	Files        int              `json:"files"`
	Warmed       int              `json:"warmed"`
	AlreadyLocal int              `json:"already_local"`
	Skipped      int              `json:"skipped"`
	Failed       int              `json:"failed"`
	NotFound     int              `json:"not_found"`
	Bytes        int64            `json:"bytes"`
	Complete     bool             `json:"complete"`
	Results      []WarmFileResult `json:"results"`
}

// FilesWarmer is implemented by cache managers that support manifest warmup;
// the HTTP API asserts against it.
type FilesWarmer interface {
	WarmFiles(ctx context.Context, files []WarmFile, opts WarmupOptions) (*WarmFilesResult, error)
}

// MaxWarmFiles bounds one manifest warm.
const MaxWarmFiles = 10000

// WarmFiles makes every listed file whole on local NVMe. Mode "metadata"
// resolves only (proves every path is visible from this node) and moves no
// data. Per-file problems are outcomes, not errors; the error return is for a
// request that cannot be planned at all.
func (cm *DefaultCacheManager) WarmFiles(ctx context.Context, files []WarmFile, opts WarmupOptions) (*WarmFilesResult, error) {
	mode := strings.ToLower(strings.TrimSpace(opts.Mode))
	if mode == "" {
		mode = "full"
	}
	res := &WarmFilesResult{Mode: mode}
	if mode != "metadata" && mode != "full" {
		return res, fmt.Errorf("unknown warmup mode %q", opts.Mode)
	}
	order, fileConcurrency, chunkWorkers, err := warmupPlan(opts)
	if err != nil {
		return res, err
	}
	if err := ValidateWarmFiles(files); err != nil {
		return res, err
	}

	res.Files = len(files)
	res.Results = make([]WarmFileResult, len(files))
	var (
		mu sync.Mutex
		wg sync.WaitGroup
	)
	snapshotLocked := func() WarmupProgress {
		return WarmupProgress{
			Files: res.Files, Done: res.Warmed + res.AlreadyLocal + res.Skipped + res.Failed + res.NotFound,
			Warmed: res.Warmed, AlreadyLocal: res.AlreadyLocal,
			Skipped: res.Skipped, Failed: res.Failed + res.NotFound, Bytes: res.Bytes,
		}
	}
	record := func(i int, r WarmFileResult) {
		mu.Lock()
		res.Results[i] = r
		switch r.Status {
		case WarmFileWarmed:
			res.Warmed++
			res.Bytes += r.Size
		case WarmFileAlreadyLocal:
			res.AlreadyLocal++
		case WarmFileSkipped:
			res.Skipped++
		case WarmFileNotFound:
			res.NotFound++
		default: // failed, size_mismatch
			res.Failed++
		}
		snap := snapshotLocked()
		mu.Unlock()
		if opts.OnProgress != nil {
			opts.OnProgress(snap)
		}
	}

	sem := make(chan struct{}, fileConcurrency)
	for i, f := range files {
		if ctx.Err() != nil {
			// Abandoned files are failures, never silently absent.
			record(i, WarmFileResult{Path: f.Path, Status: WarmFileFailed, Error: ctx.Err().Error()})
			continue
		}
		wg.Add(1)
		sem <- struct{}{}
		go func(i int, f WarmFile) {
			defer wg.Done()
			defer func() { <-sem }()
			record(i, cm.warmListedFile(ctx, f, mode, order, chunkWorkers))
		}(i, f)
	}
	wg.Wait()
	res.Complete = res.Warmed+res.AlreadyLocal == res.Files
	return res, nil
}

// ValidateWarmFiles checks a manifest without I/O and normalizes each path
// in place to its clean absolute form.
func ValidateWarmFiles(files []WarmFile) error {
	if len(files) == 0 {
		return fmt.Errorf("files is empty")
	}
	if len(files) > MaxWarmFiles {
		return fmt.Errorf("%d files exceeds the limit of %d", len(files), MaxWarmFiles)
	}
	seen := make(map[string]bool, len(files))
	for i, f := range files {
		raw := "/" + strings.TrimPrefix(strings.TrimSpace(f.Path), "/")
		p := path.Clean(raw)
		if p == "/" || p != raw {
			return fmt.Errorf("files[%d]: %q is not a clean path", i, f.Path)
		}
		if _, isChunk := parentFilePathFromChunkPath(p); isChunk {
			return fmt.Errorf("files[%d]: %q is a chunk path; list the file", i, f.Path)
		}
		if f.Size < 0 {
			return fmt.Errorf("files[%d]: negative size", i)
		}
		if seen[p] {
			return fmt.Errorf("files[%d]: %q listed twice", i, f.Path)
		}
		seen[p] = true
		files[i].Path = p
	}
	return nil
}

// warmListedFile resolves one listed path and, in full mode, lands it.
func (cm *DefaultCacheManager) warmListedFile(ctx context.Context, f WarmFile, mode string, order []CacheTier, chunkWorkers int) WarmFileResult {
	r := WarmFileResult{Path: f.Path}
	t, source, ok := cm.resolveWarmTarget(ctx, f.Path)
	if !ok {
		r.Status = WarmFileNotFound
		r.Error = cm.notFoundReason(ctx, f.Path)
		return r
	}
	r.Size, r.Source = t.size, source
	if f.Size > 0 && t.size != f.Size {
		r.Status = WarmFileSizeMismatch
		r.Error = fmt.Sprintf("manifest size %d, resolved size %d", f.Size, t.size)
		return r
	}
	// A short local copy is not "already local": fall through and warm.
	if _, whole := cm.LocalFilePath(ctx, f.Path); whole {
		if size, err := cm.nvmeStorage.Size(ctx, f.Path); err == nil && (t.size <= 0 || size == t.size) {
			r.Status = WarmFileAlreadyLocal
			return r
		}
	}
	if mode == "metadata" {
		// Resolved and visible; nothing moved. Reported as already_local only
		// when it truly is, so a metadata pass never claims residency.
		r.Status = WarmFileSkipped
		r.Error = "metadata mode: resolved, not fetched"
		return r
	}
	switch cm.warmOneFile(ctx, t, order, chunkWorkers, nil) {
	case warmOutcomeWarmed:
		r.Status = WarmFileWarmed
	case warmOutcomeAlreadyLocal:
		r.Status = WarmFileAlreadyLocal
	case warmOutcomeSkipped:
		r.Status = WarmFileSkipped
		r.Error = "skipped: insufficient NVMe headroom or assembly already in progress"
	default:
		r.Status = WarmFileFailed
		r.Error = "fetch or assembly failed (see node log)"
	}
	// Trust the disk, not the outcome: a warm that "succeeded" but left a
	// short file is a failure.
	if r.Status == WarmFileWarmed || r.Status == WarmFileAlreadyLocal {
		if size, err := cm.nvmeStorage.Size(ctx, f.Path); err != nil || (t.size > 0 && size != t.size) {
			r.Status = WarmFileFailed
			r.Error = fmt.Sprintf("after warm, local size %d (err %v), expected %d", size, err, t.size)
		}
	}
	return r
}

// notFoundReason explains a not_found: which resolution steps were consulted
// and what each said, so a cross-cluster gap (no global resolver, bucket
// mismatch, unsynced mirror) is diagnosable from the API, not just a status.
func (cm *DefaultCacheManager) notFoundReason(ctx context.Context, p string) string {
	if cm.config.Coordinator == nil {
		return "no coordinator configured"
	}
	parts := []string{}
	if locs, err := cm.config.Coordinator.GetFileLocation(ctx, p); err != nil {
		parts = append(parts, "coordinator lookup error: "+err.Error())
	} else {
		parts = append(parts, fmt.Sprintf("coordinator rows=%d", len(locs)))
	}
	if !cm.config.GlobalReadEnabled {
		return strings.Join(append(parts, "global read disabled"), "; ")
	}
	resolver := cm.globalResolver()
	if resolver == nil {
		return strings.Join(append(parts, "coordinator client has no global resolver"), "; ")
	}
	reader, ok := cm.cloudStorage.(rawKeyReader)
	if !ok {
		return strings.Join(append(parts, "cloud tier cannot read raw keys"), "; ")
	}
	res, err := resolver.ResolveGlobal(ctx, p)
	switch {
	case err != nil:
		parts = append(parts, "global resolve error: "+err.Error())
	case res == nil:
		parts = append(parts, "global resolve: nil")
	default:
		parts = append(parts, fmt.Sprintf("global ready=%v rows=%d local_bucket=%q", res.Ready, len(res.Locations), reader.BucketName()))
		for _, l := range res.Locations {
			parts = append(parts, fmt.Sprintf("row cluster=%s bucket=%q size=%d", l.ClusterID, l.Bucket, l.FileSize))
		}
	}
	return strings.Join(parts, "; ")
}

// resolveWarmTarget learns a path's shape by forced single-path lookup:
// local entry, coordinator point lookup, then (with -global-read) the global
// mirror. Source says which answered.
func (cm *DefaultCacheManager) resolveWarmTarget(ctx context.Context, p string) (*warmupTarget, string, bool) {
	cm.mu.RLock()
	local, isLocal := cm.entries.get(p)
	cm.mu.RUnlock()
	entry, ok := cm.LookupEntry(ctx, p, true)
	if !ok || entry == nil {
		return nil, "", false
	}
	source := "coordinator"
	if isLocal && local != nil {
		source = "local"
	} else if cm.config.Coordinator != nil {
		// LookupEntry falls through to the global mirror only when this
		// cluster's coordinator has no row, so "no local row" is the tell.
		if locs, err := cm.config.Coordinator.GetFileLocation(ctx, p); err == nil && len(locs) == 0 {
			source = "global"
		}
	}
	t := &warmupTarget{path: p, size: entry.Size, isChunked: entry.IsChunked, numChunks: entry.NumChunks, chunkSize: entry.ChunkSize}
	if t.isChunked {
		if t.chunkSize <= 0 {
			t.chunkSize = cm.config.ChunkSize
		}
		if t.numChunks <= 0 {
			t.numChunks = chunkCountFor(t.size, t.chunkSize)
		}
	}
	return t, source, true
}

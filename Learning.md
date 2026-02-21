# Read Performance Learnings (AKS: 2x A100 + 1x L64s_v3)

## Environment
- Cluster/context: `stargz-test`
- Namespace: `fuse-system-aztest`
- Release: `fuse-cache-aztest`
- Benchmark script: `scripts/ops/test-smart-read.sh`
- Test pattern: write on A100 pod, clear reader NVMe cache, read on L64s pod

## Best Known Good Baseline
- Helm lineage: rollback target equivalent to revision `53` config profile
- Client image: `stargzrepo.azurecr.io/fuse-client:fuse-readtrace-amd64-20260220205844`
- Key settings:
  - `CHUNK_SIZE_MB=8`
  - `FUSE_PARALLEL_RANGE_READS=16`
  - `FUSE_RANGE_PREFETCH_CHUNKS=8`
  - `FUSE_RANGE_CHUNK_CACHE_SIZE=64`

## Benchmarks Observed

### Stable high-performing cycle (best so far)
- 5GB cross-pod read: about **506 MB/s**
- Repeated 1GB reads: about **476-499 MB/s**
- Improvement vs earlier baseline: from roughly **155-187 MB/s** to ~**500 MB/s class**

### Regression cycle (newer image/config set, later reverted)
- Image family: `fuse-readspeed-amd64-*`
- Typical config in that cycle:
  - `CHUNK_SIZE_MB=16`
  - `parallelRangeReads=16`
  - `rangePrefetchChunks=8`
  - `rangeChunkCacheSize=64`
- Observed:
  - 1GB read: **~384-407 MB/s**
  - 5GB read: **~346 MB/s**
- Result: slower than revision `53` profile

### Post-rollback validation
- After reverting to known-good profile/image:
  - 1GB read re-check: **~475 MB/s**
- Confirms rollback restored expected performance band

## Instrumentation Notes
- `/metrics` tier counters showed many runs were peer-dominated (cloud counters often zero in tested runs).
- Example from a strong peer-only 1GB read:
  - `fuse_cache_peer_read_bytes_total=1073741824`
  - `fuse_cache_peer_read_ops_total=128` (or lower with larger chunk size)
  - `fuse_cache_cloud_read_bytes_total=0`

## Changes Tried (Summary)
- Increased range read worker/prefetch/cache tunables.
- Adjusted chunk size (8MB vs 16MB).
- Added gRPC window/message tuning.
- Added additional hybrid-read logic and cloud-path probing in some iterations.
- Tried larger FUSE readahead/background settings in one cycle.

## What to Keep / What to Avoid
- Keep the revision `53` profile as the current performance baseline.
- Avoid promoting `CHUNK_SIZE_MB=16` profile as default without clear throughput win.
- Treat every change as A/B only against the known-good baseline (`~475-506 MB/s` reads).

## Next Benchmark Discipline
- For each change set, always record:
  - image tag
  - helm revision
  - pod pair (writer/reader)
  - 1GB and 5GB read MB/s
  - whether cloud tier was actually used (`/metrics`)

## 2026-02-21 Cloud Read Optimization (Azure)
- Image: `stargzrepo.azurecr.io/fuse-client:fuse-cloudopt-20260221100456`
- Helm revision: `71`
- Profile kept at or below 32MB: `CHUNK_SIZE_MB=24`, `parallelRangeReads=96`, `rangePrefetchChunks=48`, `rangeChunkCacheSize=384`
- New Azure settings:
  - `FUSE_AZURE_DOWNLOAD_CONCURRENCY=16`
  - `FUSE_AZURE_DOWNLOAD_BLOCK_SIZE_MB=8`
  - `FUSE_AZURE_PARALLEL_DOWNLOAD_MIN_SIZE_MB=4`
- Code-path changes:
  - Azure `Read()` now uses `DownloadBuffer` with parallel block fetch for larger blobs.
  - `Exists()` and `Size()` switched to blob `GetProperties()` (avoids stream-body leak on `Exists` and fixes size lookup correctness).

### Observed Benchmarks (A100 writer -> L64 reader)
- 1GB read: `~397 MB/s`
- 5GB read run #1: `~395 MB/s`
  - Peer wall contribution: `~215.6 MB/s`
  - Cloud wall contribution: `~179.8 MB/s`
  - Cloud share by bytes: `~45.5%`
- 5GB read run #2: `~420 MB/s`
  - Peer wall contribution: `~145.8 MB/s`
  - Cloud wall contribution: `~274.5 MB/s`
  - Cloud share by bytes: `~65.3%`

### Takeaway
- Cloud-side contribution increased materially on later runs with the Azure parallel download path.
- We still remain below the 1GB/s target, but this moved cloud from secondary contributor to dominant contributor in the best run.

## 2026-02-21 go-fuse v2 Trial (with passthrough enabled)
- Image lineage:
  - `stargzrepo.azurecr.io/fuse-client:gofuse-bench-20260221-105900` (initial trial)
  - `stargzrepo.azurecr.io/fuse-client:gofuse-benchfix-20260221-110616` (metadata refresh fix)
  - `stargzrepo.azurecr.io/fuse-client:gofuse-benchfix2-20260221-110935` (lookup attr-size fix)
- Helm revision after final fix: `74`
- Backend config:
  - `FUSE_BACKEND=gofuse`
  - `FUSE_GOFUSE_ENABLE_PASSTHROUGH=true`
- New test suite:
  - `scripts/ops/test-gofuse-cached-read-suite.sh`
  - Runs 1GiB and 5GiB with:
    - write
    - cold read (after reader-side local cache clear)
    - cached read (second pass)

### Failure + Fixes
- Initial failure symptom:
  - Reader saw files but read returned `0 bytes` (`dd` EOF immediately).
- Root causes fixed:
  - `Lookup` in go-fuse backend did not populate `EntryOut.Attr`, leaving kernel-visible size at `0`.
  - Additional metadata refresh hardening for zero-sized stale entries.

### Observed Benchmarks (A100 writer -> A100 reader)
- 1GiB:
  - write: `~274 MB/s`
  - cold read: `~547 MB/s`
  - cached read: `~2316 MB/s`
- 5GiB:
  - write: `~271 MB/s`
  - cold read: `~524 MB/s`
  - cached read: `~2518 MB/s`

### Notes
- Cached-read numbers are much higher than cold reads and likely include range/kernel cache effects.
- Reader-side NVMe file presence is not guaranteed for this path because range reads are cached in-memory.

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

## 2026-02-23 go-fuse Read Timeout Clamp + 1GB Offset Trace (EKS)
- Cluster/namespace: `stargz-test-eks` / `fuse-system-awstest`
- Image: `679004966033.dkr.ecr.us-east-1.amazonaws.com/fuse-client:eks-az6fix-amd64-readtrace-timeoutclamp-20260223-141050`
- Pods used (healthy nodes): writer `client-6knlj`, reader `client-8pgbj`
- Runtime trace/timeouts:
  - `FUSE_GOFUSE_READ_TRACE=true`
  - `FUSE_GOFUSE_READ_TRACE_STEP_MB=8`
  - `FUSE_GOFUSE_READ_TRACE_PATH=smart-read-`
  - `FUSE_GOFUSE_READ_TIMEOUT_MS=15000`

### Code change
- File: `internal/fuse/gofuse_backend.go`
- Change: clamp effective `ReadRange` context deadline to `now + FUSE_GOFUSE_READ_TIMEOUT_MS` even when caller context already has a longer deadline.
- Reason: prevent long/hung backend reads from inheriting overly long caller deadlines and stalling FUSE responses.

### Commands run
- `kubectl -n fuse-system-awstest set image daemonset/client client=679004966033.dkr.ecr.us-east-1.amazonaws.com/fuse-client:eks-az6fix-amd64-readtrace-timeoutclamp-20260223-141050`
- `kubectl -n fuse-system-awstest set env daemonset/client FUSE_GOFUSE_READ_TRACE=true FUSE_GOFUSE_READ_TRACE_STEP_MB=8 FUSE_GOFUSE_READ_TRACE_PATH=smart-read- FUSE_GOFUSE_READ_TIMEOUT_MS=15000`
- `HYBRID_SETTLE_SEC=20 timeout 1800 ./scripts/ops/test-smart-read.sh fuse-system-awstest 1024 client-6knlj client-8pgbj`
- `HYBRID_SETTLE_SEC=20 timeout 2400 ./scripts/ops/test-smart-read.sh fuse-system-awstest 5120 client-6knlj client-8pgbj`

### Observed performance (post-patch)
- 1GB:
  - write: `531 MB/s`
  - read: `295 MB/s`
- 5GB:
  - write: `198 MB/s`
  - read: `308 MB/s`

### Trace around 1GB offset (5GB run)
- File: `smart-read-5120mb-1771885827.bin`
- `off=1065353216` -> `dur=21.060648ms`
- `off=1073741824` -> `dur=21.239031ms`
- `off=1082130432` -> `dur=22.656559ms`
- No `ReadRange timeout retry` and no `ReadRange error` lines in this run.

### Operational note
- One 5GB attempt failed due `kubectl exec` stream reset (`read tcp ... connection reset by peer`) while the benchmark process was running.
- Immediate rerun succeeded; treat that failure as API stream transport noise, not a read-path regression.

## 2026-02-24 EKS Hotspot Iteration (Non-config Strategies)
- Cluster/context: `stargz-test-eks`
- Namespace: `fuse-system-awstest`
- Runtime profile kept stable:
  - `CHUNK_SIZE_MB=8`
  - `FUSE_RANGE_PREFETCH_CHUNKS=4`
  - `FUSE_PEER_READ_MBPS=10000`
  - go-fuse v2 backend

### Ideas tested in this iteration
1. gRPC peer-read server buffer pooling (remove large per-request buffer allocations).
2. `ReadRange` single-chunk zero-copy return path (avoid extra copy for 128KiB FUSE reads).
3. Split range-cache and chunk-fetch locking away from the main cache mutex (reduce contention).
4. go-fuse read-window cache (2MiB prefetch per read call) to reduce read syscall overhead.

### A/B results
- Baseline before iteration:
  - 1GB read: `788 MB/s`
  - 5GB read: `887 MB/s`
- Iteration A (all 4 ideas enabled):
  - 1GB read: `634-646 MB/s` (regression)
  - 5GB read: `838 MB/s` (regression)
- Iteration B (Idea 4 disabled, Ideas 1-3 kept):
  - 1GB read: `1201-1244 MB/s`
  - 5GB read: `1368-1395 MB/s`

### Tier contribution (Iteration B)
- Delta read bytes from `/api/cache/stats` over 1GB+5GB run:
  - `peer_read_bytes=6442450944` (100%)
  - `cloud_read_bytes=0` (0%)
- Reads were fully peer-served in this cycle.

### Outcome
- Keep: Ideas 1, 2, 3.
- Rejected: Idea 4 (go-fuse read-window prefetch) due clear throughput regression.

## 2026-03-01 AKS Cached-Read + Large-File Reliability Pass
- Cluster/namespace: `stargz-test` / `fuse-system-aztest`
- Backend: `gofuse` with passthrough enabled
- Helm revisions:
  - `86`: added `-nvme-max-gb` wiring in chart, set `config.nvmeMaxGB=48`
  - `87`: chart compatibility gate for advanced S3 flags (`config.s3AdvancedArgsEnabled=false`) to match AKS client image flag set

### Failure reason found
- 5GB cached suite intermittently failed with:
  - `failed to read chunk 0 ... from remote tiers`
  - FUSE read `Input/output error`
- Root cause pattern:
  - On writer pod, only a subset of chunks for the 5GB object existed in NVMe (for example, 192 chunks present out of expected 320 at 16MiB/chunk).
  - With default `nvme-max-gb=10` and prior benchmark residue, older/newer chunks were evicted under pressure before async cloud persistence completed, leaving gaps.

### Changes applied
1. Increased working-set headroom on AKS via chart-configured `-nvme-max-gb` (set to `48` for benchmark run).
2. Hardened `scripts/ops/test-gofuse-cached-read-suite.sh`:
   - Added configurable read retries:
     - `READ_RETRIES` (default `8`)
     - `READ_RETRY_DELAY_SEC` (default `5`)
   - Cleaned stale NVMe benchmark artifacts before each size iteration.
   - Kept FUSE-mounted file deletion out of cleanup path to avoid noisy I/O cleanup errors.
3. Added chart switch `config.s3AdvancedArgsEnabled` (default `false`) so older AKS images that do not support advanced S3 flags can still roll.

### Observed results (A100 writer -> A100 reader)
- `test-gofuse-cached-read-suite.sh`:
  - 1GB: write `282 MB/s`, cold `436 MB/s`, cached `2003 MB/s`
  - 5GB: write `262 MB/s`, cold `622 MB/s`, cached `2389 MB/s`
- `test-smart-read.sh` after NVMe headroom increase:
  - 1GB: `264 / 545 MB/s` (write/read)
  - 5GB: `270 / 648 MB/s` (write/read)

### Takeaway
- Increasing NVMe headroom removed the worst large-file chunk availability regressions and improved 5GB cold-read throughput versus prior AKS runs.
- Cached read remains far higher than cold read due range/kernel cache effects in the current go-fuse read path.

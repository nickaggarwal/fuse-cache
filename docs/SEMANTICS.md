# fuse-cache Semantics

What the FUSE mount supports, what it refuses, what is slow by design, and
the consistency model. Pattern borrowed from mountpoint-s3's SEMANTICS.md:
state the contract explicitly instead of letting applications discover it.

Unlike mountpoint-s3, fuse-cache is a *write-back cache* with a local NVMe
tier, not a thin S3 shim — so mutation is supported and cheap locally. The
trade is a durability window (see Writes).

## Reads

- Sequential and random reads fully supported; sequential is the fast path
  (adaptive readahead, peer/cloud striping for large files).
- Warm reads of whole local files use kernel passthrough (device speed).
- Remote reads: peer tier first (~ms), cloud fallback (~100ms+). A file
  written on another node is readable everywhere immediately after its
  metadata publish (usually < 1s after write completes there).
- Cold-read throughput scales with file size: small files pay one
  round-trip; multi-GB files fan out across chunks, peers, and cloud.

## Writes

- Writes land on local NVMe and ack at NVMe speed. Files larger than the
  chunk size (default 8MiB) are split into `_chunk_N` objects.
- **Durability is asynchronous**: cloud persist happens in the background
  (backlog-scaled workers). Until a path is `persisted`, the only copy is
  local — a node lost in that window loses the file. `fsync`/`Flush`
  persists buffered writes locally but does NOT wait for cloud persist.
  This is deliberate: the hot path stays at NVMe speed, and consumers that
  need node-loss safety ask for it explicitly (below).
- **Durability receipt** (`POST /api/cache/durable`): every write is tracked
  as `local` → `uploading` → `persisted`, or `failed` when an attempt
  exhausts its retries. Failed uploads are retried with backoff (30 s up to
  10 min) instead of being dropped, and pending uploads are journalled
  under `<nvme>/.fuse-durability/` so a client restart resumes them. The
  receipt (`200`, `"durable": true`) is issued only when every requested
  path, and for a chunked file every chunk plus the parent, is in object
  storage; otherwise the answer is `202` with per-path state. A node that
  did not write a path verifies it by probing every object's size in the
  cloud. Limits: the journal is rename-atomic but not fsynced, so a kernel
  crash or power loss (not a process crash) can drop a just-written record;
  that file stays unevictable but its upload does not auto-resume, and the
  receipt reports it as `local`, never as persisted. The receipt verifies
  sizes, not content digests (see Consistency model).
- Eviction will never delete bytes whose only copy is local.
- In-place partial rewrite of an existing chunked file is supported but
  slow (read-modify-write of affected chunks) — avoid for hot paths.
- Concurrent writers to the same path from different nodes are NOT
  coordinated: last publish wins, no merge.
- Concurrent writers on one node serialize only if they enter through the
  FUSE layer. The HTTP API (`PUT /api/files/{path}`) does not, and neither
  does a warm or a promote. For those entrances the atomicity depends on
  size:
  - **Unchunked** (payload ≤ chunk size): atomic. One object is swapped, so
    a concurrent reader sees some whole version, never a mix. Measured 0
    torn reads in 214,181 under a tight overwrite loop.
  - **Chunked**: NOT atomic. `putChunked` writes chunks in parallel with no
    version barrier, so a reader assembling the file mid-overwrite gets some
    old chunks and some new. This is the common case under contention, not a
    rare race — measured ~93% of reads. The length is always valid; the
    contents may straddle two versions.

  Pinned by `TestConcurrentWrite_{Unchunked,Chunked}OverwriteIsAtomic` /
  `...Tears` in `internal/cache/concurrent_rw_test.go`. If you need an
  atomic replace of a large file, write to a temporary path and rename.

## Deletes and renames

- Delete removes local + peer + cloud copies (best-effort; cloud delete is
  asynchronous). Open handles on other nodes may keep serving cached chunks
  until their range caches expire.
- Rename is a metadata move (`os.Rename` locally, a location-metadata
  rewrite cluster-wide) as of `19e069e`; it is no longer O(size). It is
  still not atomic cluster-wide — do not build lock protocols on it.

## Metadata and permissions

- Modes/ownership are whatever the FUSE mount options say; per-file chmod/
  chown are accepted locally but NOT persisted to cloud or peers.
- Symlinks and hard links are not replicated; treat them as unsupported.
- `stat` sizes for files being written on another node may lag until that
  node's metadata publish.

## Consistency model

- **Read-after-write, same node**: strong (NVMe).
- **Read-after-close, cross node**: strong once the writer's location
  publish lands (typically sub-second). Reading a path on node B while node
  A is mid-write yields not-found or the previous version, never torn data
  at chunk granularity — but multi-chunk files can mix old/new chunks if
  overwritten in place while being read, remotely *or locally* (see Writes).
  Write-once-then-read workloads (checkpoints, models) never see this.
- **Cache staleness**: range caches expire after 30s idle; NVMe entries
  revalidate against cloud (existence + size) on the eviction path after
  restart. There is no per-read etag check against cloud (perf trade;
  chunk content checksums are on the roadmap).

## Cross-cluster (global control plane)

Off by default. Everything above describes one cluster and is unchanged by
this section when `globalCP.enabled` is false.

A cluster may publish a configured subset of its paths to a global control
plane, which other clusters mirror and read. The contract is deliberately
much weaker than the single-cluster one, and the weakening is in the
*discovery* path, not the data path.

- **The global path is object storage only.** The control plane answers
  "which cluster published this path, into which bucket, under which key
  prefix". The reading cluster then fetches from object storage itself.
  There is no cross-cluster peer transfer, no cross-cluster FUSE traffic,
  and no inbound network path into a member cluster.
- **Only explicitly published prefixes are visible.** A path outside
  `publishPrefixes` does not exist to any other cluster. This is not a
  permission check layered on a global namespace; there is no global
  namespace apart from what was published.
- **Cross-cluster visibility is eventually consistent**, bounded by the
  publish coalescing window plus stream delivery. Do NOT read the
  "typically sub-second" figure from *Consistency model* as applying here:
  that number is a cross-node location publish inside one cluster. A
  cross-cluster publish is a different, longer path, and there is no upper
  bound on it when the control plane is unreachable.
- **A path is published only once its bytes are in object storage.** A file
  that exists only on a local NVMe tier is never advertised, because no
  other cluster could fetch it. A chunked file is published as exactly one
  row (its parent, with size and chunk stride) after every chunk has been
  uploaded; `_chunk_N` objects never appear in the global index. Readers
  derive the chunk keys from the parent row and its stride, so a reader with
  a different `-chunk-size` still reads the right bytes. So cross-cluster visibility lags the
  durability window described in *Writes*, and inherits it: a cluster that
  dies inside that window never publishes, and the file is simply absent
  globally rather than advertised-and-missing.
- **A global read is a cloud read.** It is attempted only after this
  cluster's own cloud read has already missed, so it adds latency to misses
  and nothing to hits. With `-global-read` on, a forced FUSE lookup (open,
  create, getattr refresh) of a path this cluster does not know falls back
  to the global mirror, so a published file can be opened and read through
  the mount. `readdir` does not list global-only paths (the mirror is not
  enumerated), and `create` of a name another cluster published returns
  `EEXIST`. With `-global-read` off, global paths are `ENOENT`. A global miss is still a miss, with exactly the
  failure taxonomy described for any other tier.
- **Every single-cluster weakness is inherited, not repaired.** Concurrent
  writers in *different* clusters are no better coordinated than concurrent
  writers on different nodes: last publish wins, no merge. The chunked
  in-place overwrite tearing described in *Writes* is unchanged, and it is
  if anything easier to hit across clusters, because the window is longer.
  Write-once-then-read workloads (checkpoints, models) are the supported
  shape.
- **A stale index row is normal and is not an error.** A row says a cluster
  published the path at some time; it is not a liveness claim about the
  object. A row whose object has since been deleted reads as a miss. The
  `rows_found` vs `hits` counters exist to make that gap visible.
- **The control plane is advisory.** If it is unreachable this cluster
  behaves exactly as a single-cluster deployment: local read, write, peer
  fetch and cloud fetch are untouched, and the mirror keeps serving its
  last-good contents. Only discovery of *new* remote publications stops.
  There is no mode in which a global control plane outage can fail a local
  operation.
- **Two clusters sharing a bucket must set different `cloud.keyPrefix`
  values.** They otherwise overwrite each other's objects silently, with no
  error at any layer. This is enforced by nothing — it is configuration.

Cross-cluster deletes and withdrawals are best-effort and idempotent. A
withdraw that never reaches the control plane leaves a stale row, which
degrades to the miss case above.

## Explicitly out of scope

- POSIX byte-range locks (accepted locally, not distributed).
- mmap shared-writable across nodes.
- Atomic cross-node rename or directory rename semantics.
- Running databases / git repos on the mount across multiple nodes.

## Object size limits

- S3 multipart caps parts at 10,000: with the default 8MiB upload part
  size, a single whole-file upload caps at ~78GiB. The client auto-raises
  the part size for larger files (up to S3's 5GiB/part = 48.8TiB object
  ceiling); chunked persistence (the default for large files) sidesteps
  this entirely.

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
  (backlog-scaled workers). Until `PersistedToCloud` is set, the only copy
  is local — a node lost in that window loses the file. `fsync`/`Flush`
  persists buffered writes locally but does NOT wait for cloud persist.
- Eviction will never delete bytes whose only copy is local.
- In-place partial rewrite of an existing chunked file is supported but
  slow (read-modify-write of affected chunks) — avoid for hot paths.
- Concurrent writers to the same path from different nodes are NOT
  coordinated: last publish wins, no merge. Concurrent writers on one node
  serialize through the FUSE layer.

## Deletes and renames

- Delete removes local + peer + cloud copies (best-effort; cloud delete is
  asynchronous). Open handles on other nodes may keep serving cached chunks
  until their range caches expire.
- Rename is a copy+delete at the cache layer — O(size), not atomic. Do not
  build lock protocols on rename.

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
  overwritten in place while being read remotely. Write-once-then-read
  workloads (checkpoints, models) never see this.
- **Cache staleness**: range caches expire after 30s idle; NVMe entries
  revalidate against cloud (existence + size) on the eviction path after
  restart. There is no per-read etag check against cloud (perf trade;
  chunk content checksums are on the roadmap).

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

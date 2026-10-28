# fusedata — Rust read data path (prototype)

A Rust rebuild of the fuse-client **read hot path**, built to be measured
against the Go implementation on the same cache directory. It is a prototype:
it does not replace the Go client and has never been deployed.

| Piece | Module | Go counterpart |
|---|---|---|
| Cache-layout reader (whole files, `_chunk_N` files, `.sha256` sidecars, journal shape hints) | `fusedata-core::layout` | `nvme_storage.go`, `nvme_range.go`, `ReadRange` in `cache.go` |
| Chunk addressing (`effective_chunk_size`, `chunk_count_for`, `clamp_chunk_slice`) | `fusedata-core::chunk` | same names in `cache.go` |
| Peer `/api/peer/read` client + server (keep-alive, `sendfile`, 503/404 classes) | `fusedata-core::peer` | `peer_storage.go` (raw transport), `internal/api/peer_read.go` |
| Azure blob ranged reader (Shared Key, first-range-then-parallel) | `fusedata-core::azure` | `azure_storage.go` |
| Coordinator lookup (holders of a path, peer addresses) | `fusedata-core::coord` | `coordinator/client.go` (HTTP) |
| Tier fall-through local → peer → Azure, single-flight, chunk promotion in Go layout | `fusedata-core::tiered` | `readChunkDataFromTiers` |
| Read-only FUSE mount (`fuser`) | `fusedata::fuse_fs` | `internal/fuse/gofuse_backend.go` |
| Bench/CLI | `fusedata` binary | `tools/readbench` |

Out of scope (still Go-only): writes, coordinator publishing, warm intents,
durable/KV store, CSI, node-init, snapshot/restore, eviction, hybrid
peer/cloud hedging, fetch leases, latency EWMAs, S3/GCS.

## What the Go profile said

`tools/readbench bench` drives `DefaultCacheManager.ReadRange` in-process
with the client's flag defaults (`-chunk-size 16`, `-parallel-range-reads 8`,
`-range-prefetch-chunks 2`, `-range-chunk-cache-size 16`,
`-range-whole-file-reads=false`), on a 3 GiB per-chunk file and a 1 GiB
whole-layout file written by the real Go write paths. Linux arm64 (Docker
linuxkit VM, 14 vCPU, 8 GiB), `go1.26`.

1. **Every window that misses the range cache reads the whole 16 MiB chunk
   file.** `readChunkDataFromTiers` → `getFromTierNoVerify` →
   `NVMeStorage.Read` → `os.ReadFile`. In the 128 KiB random-read profile this
   is **94.7 % of CPU** (87 % in the `read` syscall copying 16 MiB, 7 % in
   `memclrNoHeapPointers` zeroing the fresh 16 MiB buffer), and the alloc
   profile is **100 % `os.readFileContents`: 127.9 bytes allocated per byte
   returned** (16 MiB / 128 KiB). Random 128 KiB reads therefore run at
   ~80–110 MB/s with ~1 ms p50 even from page cache, and cost 11–14 CPU s/GiB.
2. **The range cache hides this for sequential reads only.** A sequential
   stream hits the per-file chunk cache (`getChunkFromRangeCache`), so after
   the first window of each chunk a read is a slice of a cached buffer
   (p50 0.5 µs). But the slow path is still paid once per chunk on the reading
   goroutine or the prefetcher (p99 1.4–3.7 ms at 1 MiB windows), and every
   chunk's 16 MiB lands in a new heap allocation (1.1–1.4 B allocated per byte
   read, 4–9 GCs per GiB).
3. **The whole-file range path (`-range-whole-file-reads`, off by default)
   avoids the amplification but reopens the file per window.**
   `readWholeChunkedRange` does `open`+`fstat`+`pread`+`close` and allocates a
   fresh buffer for every window, which also resets the kernel's per-fd
   readahead state. On a 2 GiB whole-layout file from page cache it took
   random 128 KiB reads from 65 MB/s to 2.1 GB/s, but sequential 1 MiB reads
   fell from 23.7 GB/s to 6.3 GB/s (no prefetch; the flag help quotes 21.1 s
   vs 8.7 s for 15 GB on an A100 node). Go has to choose per deployment
   between good random and good sequential; the Rust reader gets both from
   one path.
4. **Locks, hashing and metrics were not material.** No mutex, SHA-256 or
   metrics frame appears in the top 60 of the random-read profile (each
   < 0.2 %); sidecar hashing happens once per file version. GC background
   marking was ~3 % and follows from item 1.
5. **Peer transport is already zero-copy on the server** (`sendfile` via
   `io.CopyN`), so the client is where the cost is: `readFromPeerRaw`
   allocates a fresh `Content-Length` buffer per object and copies through
   `io.ReadFull`.

So the Rust layout reader's design follows directly: **`pread` exactly the
window into a caller-owned buffer, keep descriptors open across reads, and do
readahead with the kernel (hints + a few blocking prefetch workers) instead of
by materialising whole chunks in user memory.**

## Results

Full tables, environment and caveats: [`bench/RESULTS.md`](bench/RESULTS.md)
(raw JSONL in `bench/results/`). Docker linuxkit VM, not an AKS node, so
read the ratios, not the MB/s.

| workload (median of 3) | Go | Rust | |
|---|---:|---:|---:|
| FUSE, cold, 1 reader, rand 128 KiB | 88 MB/s, p99 3.7 ms | 982 MB/s, p99 0.27 ms | 11.1x |
| FUSE, cold, 1 reader, seq 1 MiB | 2,230 MB/s | 2,432 MB/s | 1.09x |
| FUSE, cold, 8 readers, seq 1 MiB | 3,652 MB/s | 6,214 MB/s | 1.70x |
| FUSE daemon CPU, cold rand 128 KiB | 12.8 s/GiB | 0.42 s/GiB | 30x less |
| library, warm, 1 thread, rand 128 KiB | 114 MB/s | 17,230 MB/s | 151x |
| library, warm, 1 thread, seq 1 MiB | 33,607 MB/s | 15,729 MB/s | **0.47x** |
| peer client, 4 streams, Go server | 24,535 MB/s | 39,788 MB/s | 1.62x |
| Azure (Azurite), 1 stream | 121 MB/s | 213 MB/s | 1.76x |
| Azure (Azurite), 4 streams | 265 MB/s | 174 MB/s | **0.66x** |

Where Go is faster: single-reader sequential from page cache in-process (its
range cache turns windows into memory slices), FUSE warm 1 MiB sequential
(0.69x, kernel-cache bound), and 4-stream Azure against the emulator.

## Reproduce

```bash
# 0. toolchains: Go from the repo; Rust 1.90.0 via rustup (rust-toolchain.toml)
#    curl -sSf https://sh.rustup.rs | sh -s -- -y --no-modify-path --profile minimal

# 1. build
GOOS=linux GOARCH=arm64 go build -o /tmp/rb/readbench-linux ./tools/readbench
rust/fusedata/scripts/build-linux.sh            # -> rust/fusedata/target-linux/release/fusedata
cp rust/fusedata/target-linux/release/fusedata /tmp/rb/fusedata
#    (never cp over a binary a container is running: through the bind mount
#     that bypasses ETXTBSY and the running process segfaults)

# 2. lab container (needs /dev/fuse)
docker build -t fusedata-lab -f rust/fusedata/bench/Dockerfile.lab rust/fusedata/bench
docker volume create fusedata-cache
docker run -d --name fusedata-lab --privileged --device /dev/fuse \
  -v "$PWD":/src:ro -v fusedata-cache:/cache -v /tmp/rb:/out fusedata-lab sleep infinity

# 3. dataset, written by the Go client's own write paths
docker exec fusedata-lab /out/readbench-linux gen -dir /cache/c -size-mb 3072 -extra-bytes 777 -layout chunked
docker exec fusedata-lab /out/readbench-linux gen -dir /cache/c -path /bench/whole.bin -size-mb 1024 -extra-bytes 333 -layout whole
docker exec fusedata-lab /out/readbench-linux gen -dir /cache/c -path /bench/small.bin -size-mb 6 -extra-bytes 5 -layout small

# 4. servers and mounts
docker exec -d fusedata-lab sh -c '/out/readbench-linux serve-peer -dir /cache/c -listen 127.0.0.1:18081 -publish /bench/model.bin:3221226249:16 -publish /bench/whole.bin:1073742157:16 >/out/gopeer.log 2>&1'
docker exec -d fusedata-lab sh -c '/out/fusedata serve-peer --dir /cache/c --listen 127.0.0.1:18082 --publish /bench/whole.bin:1073742157:16 >/out/rspeer.log 2>&1'
docker exec fusedata-lab mkdir -p /mnt/go /mnt/rs
docker exec -d fusedata-lab sh -c '/out/readbench-linux mount -dir /cache/c -mnt /mnt/go -publish /bench/model.bin:3221226249:16 -publish /bench/whole.bin:1073742157:16 >/out/gomount.log 2>&1'
docker exec -d fusedata-lab sh -c '/out/fusedata mount --dir /cache/c --mnt /mnt/rs --workers 8 --publish /bench/model.bin:3221226249:16 --publish /bench/whole.bin:1073742157:16 >/out/rsmount.log 2>&1'

# 5. correctness, then the matrix
docker exec fusedata-lab sh /src/rust/fusedata/bench/probe-peer.sh      # Go vs Rust server, byte-identical responses
docker exec fusedata-lab sh /src/rust/fusedata/bench/interop-peer.sh    # 2 clients x 2 servers, content verified
docker exec -e REPS=1 -e VERIFY=-verify fusedata-lab sh /src/rust/fusedata/bench/run-fuse.sh
docker exec fusedata-lab sh /src/rust/fusedata/bench/run-all.sh         # -> /tmp/rb/results/*.jsonl (~10 min)
python3 -I rust/fusedata/bench/summarize.py /tmp/rb/results
docker exec fusedata-lab sh /src/rust/fusedata/bench/sweep-readahead.sh # prefetcher knob sweep

# 6. Azure (host side, Azurite)
docker run -d --name fusedata-azurite -p 127.0.0.1:10000:10000 mcr.microsoft.com/azure-storage/azurite:3.35.0 \
  azurite-blob --blobHost 0.0.0.0 --inMemoryPersistence --skipApiVersionCheck --loose
go run ./tools/readbench azure-put -size 268439777
rust/fusedata/scripts/cargo-host.sh build --release -p fusedata
sh rust/fusedata/bench/run-azure.sh > /tmp/rb/results/azure.jsonl

# 7. profiles
docker exec fusedata-lab sh -c 'cd /out && ./readbench-linux bench -dir /cache/c -size 3221226249 -window-kib 128 -pattern rand -total-mb 512 -cpuprofile go-rand128.cpu -memprofile go-rand128.mem'
go tool pprof -top /tmp/rb/readbench-linux /tmp/rb/go-rand128.cpu
```

Tests (`cargo test` builds `tools/readbench` with the repo's Go toolchain and
uses it to write fixtures and run Go peers; without `go` on PATH the
compatibility tests skip):

```bash
rust/fusedata/scripts/cargo-host.sh test --workspace
go test ./internal/cache -run 'TestReadRangeStrideSmallerThanChunks|TestAzureReadBench'
```

## Gaps before this could be fuse-gpu's read path

* **Writes and the rest of the daemon.** The mount is read-only. The Go
  client's FUSE writes, `PutFromFile`, durability journal, publishing to the
  coordinator, eviction/accounting, warm intents, CSI agent and metrics would
  all have to stay in Go (Rust as a read-side sidecar on the same dir) or be
  ported. The most plausible shape is Go keeps the control plane and the
  write path; Rust serves the FUSE reads and `/api/peer/read` off the shared
  cache dir — the on-disk layout is the interface, and it is already shared.
* **Single-reader sequential.** Rust's page-cache prefetcher (4 x 4 MiB
  workers) loses to Go's in-memory chunk cache for one sequential reader from
  page cache in-process (0.5x), and only matches it through FUSE. A
  safetensors load with one reader thread needs a deeper or adaptive
  prefetch window before switching, and a re-measure on real NVMe.
* **Remote tiers are simple.** No hedging/striping across peer and cloud,
  no fetch leases, no latency EWMA ordering, no busy-retry across the whole
  holder set beyond one jittered pass, no checksum verification of remote
  chunks (same as Go). Single-flight is per process. Promotion writes whole
  chunks but never assembles them into a whole file or advertises the node
  as a holder (the Go control plane would have to).
* **Azure at concurrency.** Slower than the Go SDK at 4 concurrent chunk
  fetches against Azurite; untested against a real account (no credentials
  used, by design). Only Shared Key auth; no managed identity / SAS.
* **Coordinator** client is HTTP-only (`/api/files/location`, `/api/peers`);
  the Go client defaults to gRPC.
* **FUSE details.** Inode numbers are process-local and never forgotten
  (`forget` is not implemented), directory listings come from the local dir
  only (no coordinator view of peer-only files beyond a looked-up path),
  `max_read`/`max_pages` are not tuned, no passthrough (`FOPEN_PASSTHROUGH`)
  path, no FUSE request interruption.
* **Staleness.** The layout reader revalidates a cached entry's backing file
  every 500 ms (inode + mtime); a delete-and-replace between checks can serve
  the old inode's bytes for up to that long. Go reopens per window and
  cannot.
* **Not measured:** real NVMe, real network between nodes, real Azure, AKS,
  GPU loaders. Only arm64 builds were run (the cluster nodes are amd64; the
  code has no arch-specific parts).

## Compatibility notes

* **On-disk layout** is read as the Go client writes it; nothing is
  converted. Promotion (a chunk fetched from a peer or Azure) writes
  `<path>_chunk_<n>` via a `.nvme-stream-*` temp file and `rename`, and a test
  has Go's `ReadRange` read those promoted chunks back.
* **Peer protocol**: `probe-peer.sh` shows status, `Content-Length`,
  `Content-Type`, `Retry-After` and body hash identical between the Go and
  Rust servers for whole files, literal chunk files, chunks synthesised from a
  whole parent, the short last chunk, past-the-end chunks (404), traversal
  paths, bad chunk suffixes and a missing `path` (400).
* **Azure**: the signer reproduces the `Authorization` header the Go azblob
  SDK sent for the same request (captured by `readbench azure-vector`). Like
  the SDK, the range goes in `x-ms-range` (a canonicalized header), not
  `Range`.
* **Stride mismatch**: `clamp_chunk_slice` matches Go's table. Porting it
  found that Go's `ReadRange` returns *more* bytes than requested when the
  published stride is smaller than the chunk files on disk (each span clamps
  to the longer real chunk and the pieces are appended);
  `internal/cache/readrange_stride_overrun_test.go` pins that behaviour. The
  Rust reader bounds each span by the space left in the caller's buffer.

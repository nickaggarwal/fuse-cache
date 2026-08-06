# FUSE Client with 3-Tier Cache

A distributed file system that mounts as a local FUSE directory on every
node. Writes land on local NVMe and persist to cloud storage asynchronously;
reads hit the fastest tier that has the bytes and fall through automatically.
Validated as the storage layer for GPU/CPU pod checkpoint-restore
([pod-snapshotter](https://github.com/nickaggarwal/pod-snapshotter)) on both
AKS and EKS.

**New here?** Start with [Quick Start](#quick-start) — it gets a 3-node cluster
running on your laptop in about two minutes. Then [Where Things
Live](#where-things-live) for orientation and [Testing](#testing) before you
open a PR. Everything below [Operating Modes](#operating-modes) is production
reference material you can skip on a first pass.

## Quick Start

### Prerequisites

- Go 1.21+ (`go.mod` says 1.21; the toolchain in use is newer and fine)
- Linux for anything that mounts FUSE. macOS is fine for editing, building
  most packages, and running the test suite — see [Developing on
  macOS](#developing-on-macos).
- No cloud account needed. The coordinator falls back to an in-memory store
  when `-etcd-endpoints` is empty, and the cloud tier is optional for local work.

### Run a 3-node cluster locally (Linux)

```bash
make build        # bin/coordinator, bin/client, bin/csi-driver, bin/node-init
make dev-start    # coordinator + 3 clients, ports and peering wired up
```

That gives you:

| Process | HTTP | gRPC | Mount | NVMe cache dir |
|---|---|---|---|---|
| coordinator | `:8080` | `:9080` | — | — |
| client-1 | `:8081` | `:9081` | `/tmp/fuse-client1` | `/tmp/nvme-cache1` |
| client-2 | `:8082` | `:9082` | `/tmp/fuse-client2` | `/tmp/nvme-cache2` |
| client-3 | `:8083` | `:9083` | `/tmp/fuse-client3` | `/tmp/nvme-cache3` |

Prove the tiers actually work — write on one node, read from another:

```bash
echo "hello from node 1" > /tmp/fuse-client1/greeting.txt   # Tier 1 write
cat /tmp/fuse-client2/greeting.txt                          # Tier 2 peer fetch

curl -s localhost:8081/api/cache/stats | jq      # what node 1 holds
curl -s localhost:8080/api/peers | jq            # who the coordinator sees
curl -s localhost:8081/metrics | grep fuse_tier  # adaptive tier scoring
```

Tear down with `make dev-stop` (kills the processes) and `make dev-cleanup`
(removes the `/tmp` dirs).

### Run a single client against real cloud storage

```bash
export AZURE_STORAGE_ACCOUNT=... AZURE_STORAGE_KEY=... AZURE_CONTAINER_NAME=fuse-cache
./bin/coordinator -port 8080 -grpc-port 9080 &
./bin/client -mount /tmp/fuse-client -nvme /tmp/nvme-cache \
  -coordinator-grpc localhost:9080 -cloud-provider azure
```

Swap `-cloud-provider s3` or `gcp` and the matching env vars (see
[Environment Variables](#environment-variables)). `./bin/client -help` prints
the full flag set; the load-bearing ones are in [Usage](#usage).

### Kubernetes, locally

```bash
make k8s-devbox-install-tools   # helm + kind + kubectl, if you need them
make k8s-devbox-create          # kind cluster
make k8s-devbox-deploy          # Helm chart into it
make k8s-devbox-status
make k8s-devbox-delete
```

## Where Things Live

Orientation for "I want to change X — which file?"

| You want to change… | Start in |
|---|---|
| How a read picks a tier, hedges, or falls back | `internal/cache/cache.go` (`readChunkDataFromTiers`), `tier_perf.go` |
| Peer-to-peer transfer (client side) | `internal/cache/peer_storage.go` |
| Peer-to-peer transfer (serve side) | `internal/cache/grpc_peer_server.go`, `internal/api/handler.go` |
| Cloud backends | `internal/cache/{cloud,azure,gcp}_storage.go` — all implement `TierStorage` |
| Eviction / NVMe budget | `internal/cache/nvme_storage.go`, the watermark evictor in `cache.go` |
| Thundering-herd control | `peer_admission.go`, `origin_lease.go`, `replica_reconciler.go`, `chunk_advertise.go` |
| Peer registry, file locations, etcd | `internal/coordinator/` (`coordinator.go`, `store.go`, `etcd_store.go`) |
| The FUSE syscall layer | `internal/fuse/gofuse_backend.go` (default), `filesystem.go` (bazil) |
| Kubernetes CSI / volume warmup | `internal/csidriver/`, `internal/agentserver/`, `internal/session/`, `internal/cache/warmup.go` |
| An HTTP endpoint | `internal/api/handler.go`, `internal/api/warm.go` |
| A Prometheus metric | `handlePromMetrics` in `internal/api/handler.go` |

Three things that bite people who skip the conventions:

1. **`internal/pb/` is generated.** Edit `proto/*.proto` and run `make proto`
   (needs `protoc` plus `protoc-gen-go` / `protoc-gen-go-grpc` on `PATH`).
2. **Peer errors have three classes, not two:** busy (`RESOURCE_EXHAUSTED` /
   503, admission control), miss (`NOT_FOUND` / 404, stale routing), and real
   failure. Only the third may feed a health EWMA — a scorer fed cache misses
   measures replica placement, not health, and it fails *silently*. Anything
   new on the peer serve path must return a **typed** miss or it becomes
   "failure" by default. See `internal/cache/tier_miss.go`.
3. **All coordination is advisory.** Reads must succeed with the coordinator
   down. Correctness never depends on leases, advertisement, or the reconciler.

`CLAUDE.md` has the full conventions list; `docs/SEMANTICS.md` covers
filesystem semantics.

## Testing

```bash
make test          # go test ./...
make test-race     # go test -race ./...   (the concurrency suites are race-validated)
make vet fmt lint
```

Run one test:

```bash
go test ./internal/session/ -run TestManager_RefCount -v
go test -race ./internal/cache/ -run TestTierPerf -v
```

Coverage spans the cache tiers, coordinator (including gRPC and manifest),
API handlers, and the CSI subsystem. Two rules worth knowing before you add
a test:

- **Don't race a wall-clock timer against real I/O.** A `time.Sleep(5ms)` to
  "release after the first attempt" passes bare and fails roughly 1 run in 3
  under `-race`, where a gRPC dial can outrun the timer. Key the handoff off
  observed state — a counter, a channel — so the ordering is causal. See the
  `rejected`-counter handoff in `peer_herd_test.go`.
- **Peer tests use real in-process gRPC servers** (`startHerdPeer` in
  `peer_herd_test.go`) rather than mocks, so they exercise the actual
  transport and admission paths.

The Python dashboard has its own stdlib-only suite:

```bash
python3 -m unittest discover -s tools/dashboard
```

### Developing on macOS

`bazil.org/fuse` and `hanwen/go-fuse` are Linux-only, so a bare
`go build ./...` fails on macOS with `undefined: mount` and friends. This is
expected, not a broken checkout. What works:

```bash
# Everything except the FUSE layer — the packages you'll usually be editing
go test ./internal/agentserver/ ./internal/api/ ./internal/cache/ \
        ./internal/coordinator/ ./internal/csidriver/ ./internal/nodeinit/ \
        ./internal/nodelabels/ ./internal/session/

# Compile-check the Linux-only parts without running them
GOOS=linux go build ./internal/... ./cmd/...
```

`cmd/coordinator`, `cmd/csi-driver`, and `cmd/node-init` build natively on
macOS; only `cmd/client` (and `internal/fuse`) need Linux.

Two other current warts, both pre-existing:

- Five `tmp_s3probe*.go` files in the repo root each declare `package main`,
  so the root package does not compile and `go test ./...` reports a failure
  for it. They are leftover debugging probes; ignore the error or delete them.
- Agent Unix socket paths must stay under 108 chars on macOS, which is why the
  CSI tests use `/tmp` rather than `t.TempDir()`.

## Debugging

When behavior looks wrong, `GET /metrics` on any client is the fastest way in
— it splits the read path by tier and phase:

```bash
curl -s localhost:8081/metrics | grep -E 'fuse_tier|fuse_cache_(peer|cloud|nvme)_read_mbps'
```

- `fuse_tier_{peer,cloud}_success_ratio` + `_misses_total` — adaptive tier
  scoring. A success ratio near 0 with a high miss count means the *scorer* is
  being poisoned; a low ratio with low misses means the tier is genuinely sick.
- `fuse_cache_{peer,cloud,nvme}_read_mbps` — which tier actually served the
  bytes.
- `fuse_chunk_completion_*`, `fuse_busy_chunk_retry_*`,
  `fuse_cache_evictions_total` — completion pass, herd control, and evictor
  behavior. `GET /api/cache/stats` adds JSON-only fields the Prometheus
  endpoint does not carry, notably `eviction_skipped_unpersisted`.

That split distinguishes disk limits from FUSE-path limits from peer/cloud
bottlenecks from cache-budget regressions in one pass. For a live cluster,
the [Streamlit UI](#streamlit-ui) puts these same endpoints behind a browser.

Log prefixes to grep for: `[COORDINATOR]`, `[CLIENT]`, `[API]`, `[FUSE]`,
`[CACHE]`, `[CSI]`, `[AGENT]`.

## Architecture

1. **Coordinator** (3 replicas, stateless, etcd-backed): peer registry with
   TTL leases, file location metadata, fetch leases (thundering-herd
   control). HTTP `:8080`, gRPC `:9080`.
2. **Client** (DaemonSet, one per node): FUSE mount (`gofuse` backend with
   kernel passthrough), 3-tier cache manager, peer HTTP `:8081` +
   gRPC `:9081`, CSI agent socket.
3. **3-Tier Cache**:
   - **Tier 1 — local NVMe** (`~μs`): discovered per node by `node-init`
     (raw-device format/mount supported); bounded by a byte budget with a
     host-aware watermark evictor (85/75 band, statfs pressure, never evicts
     bytes not yet durable in cloud)
   - **Tier 2 — peers** (`~ms`): raw-HTTP bulk transport with sendfile on
     the serve side and a streaming tee that lands remote chunks on local
     NVMe during the transfer; gRPC fallback; serve-side admission control
   - **Tier 3 — cloud** (`~100ms+`): AWS S3 (incl. **S3 Express One Zone**
     directory buckets), Azure Blob (incl. premium block blob), GCS; chunk
     reads use ranged parallel GETs with no HEAD round-trip
4. **CSI subsystem**: CSI node plugin + agent gRPC + session manager for
   mounting cache subtrees into pods with refcounting, pinning, and
   **declarative warmup**: a volume mounted with `warmup: full` prefetches
   its whole subtree into local NVMe in the background (headroom-guarded,
   peer-first) so first reads run at local speed — the Fluid/NetEase
   pattern for model cold starts. Example inline volume:

   ```yaml
   csi:
     driver: fuse.csi.storage.io
     volumeAttributes:
       rootPath: /models/llama-70b
       cacheMode: readonly
       warmup: full             # none | metadata | full
       warmupBandwidth: max     # background (default) | max
       sourcePolicy: cloud-first  # also steers the warm fetch tier order
       pinned: "true"           # exempt from eviction while mounted
   ```

   Warming can also be driven without pods, targeted by **node, label, or
   percentage**. Every client exposes `POST /api/cache/warm` (exact node +
   model targeting), and the coordinator fans out across the fleet:

   ```bash
   # Warm one node (sync; returns the WarmupResult):
   curl -X POST http://<node>:8081/api/cache/warm \
     -d '{"prefix":"/models/llama-70b","source":"cloud-only","bandwidth":"max"}'

   # Warm 50% of the pool=gpu nodes via the coordinator (async on each node):
   curl -X POST http://coordinator:8080/api/warm \
     -d '{"prefix":"/models/llama-70b","labels":{"pool":"gpu"},"percentage":50,
          "source":"cloud-first","bandwidth":"max"}'
   ```

   Nodes get labels from the client's `-peer-labels pool=gpu,zone=a` flag
   (or `FUSE_PEER_LABELS`), or — better on a multi-nodepool cluster —
   from their own Kubernetes Node object via
   `-peer-labels-from-node pool=agentpool,zone=topology.kubernetes.io/zone`
   (needs RBAC `get` on nodes; the chart creates it). Strategy knobs:
   `source` = `peer-first` (default) | `cloud-first` | `cloud-only` (spare
   the peers when warming many nodes at once); `bandwidth` = `background`
   (default, polite) | `max` (saturate the NIC: 4 files x 16 chunk streams
   per node).

   An async warm returns a `job_id`; poll
   `GET http://<node>:8081/api/cache/warm/<job_id>` for live counters, or
   `GET /api/cache/warm` for the node's recent jobs. The coordinator's
   fan-out response maps each peer to its job ID.

5. **node-init**: per-node disk bootstrap — discovers the best local disk,
   formats/mounts raw NVMe when needed, publishes the cache dir + byte
   budget the client adopts (cloud-agnostic DaemonSet).

## Highlights (validated Aug 2026)

- **Performance** (5GB cross-node cold read): ~1.04 GB/s single reader,
  ~1.34 GB/s aggregate with two concurrent readers (EKS `i7ie` + S3
  Express); warm reads of completion-assembled files run at kernel
  passthrough / device speed; 1GB served entirely from S3 Express in ~2.7s.
- **Streaming tee promotion**: peer reads write NVMe while bytes arrive —
  no second full-object write; a post-read completion pass fetches missed
  chunks and assembles the whole file for passthrough serving.
- **Per-file adaptive I/O profiles**: prefetch window, read fan-out, and
  cloud-persist concurrency derive per call from file size and measured
  link throughput; configured values are floors, not behavior.
- **Bounded memory and disk**: global range-cache byte budget with
  LRU-by-file eviction + idle expiry; host-aware NVMe watermark evictor
  that reacts to real device pressure (statfs) and refuses to evict the
  only unpersisted copy.
- **Thundering-herd control**: serve-side admission gates, busy-peer
  jittered retry before cloud fallback, cross-node fetch leases, fast chunk
  advertisement (mid-transfer swarm growth), replica reconciler.
- **Stale-peer fast fail**: 2s connect timeout on both transports, no
  double-dial of dead addresses (rollout first-read stall: 30s → ~2s).
- **Ops**: Prometheus `/metrics` + Grafana dashboard, benchmark scripts
  that record node class/tier contribution/git SHA, Terraform for AWS
  (EKS + S3 Express directory bucket + VPC endpoints) and Azure (AKS +
  premium blob), Helm chart with per-cloud overlays.

## Operating Modes

Three modes, chosen by what the deployment optimizes for. All three share the
same baseline (chart defaults, validated Aug 2026 on AKS and EKS): `gofuse`
backend with kernel passthrough, 8MiB chunks, writeback cache **off**
(measured: +4-6% on large sequential writes but −14% on small files; the 8MiB
append buffer already coalesces small writes), and per-file adaptive I/O
profiles — prefetch window, read workers, and persist concurrency derive at
call time from file size and measured link throughput, with the configured
values as floors. Memory is bounded by the global range-cache budget + idle
expiry; disk by the watermark evictor (85/75 band, statfs-aware, never evicts
the only unpersisted copy).

### Mode 1: Read-Optimized Remote Serving

For large-file read throughput (checkpoint restore, model loading):

- peer-first remote reads with parallel fan-out; hybrid peer+cloud striping
  engages automatically for files ≥ the hybrid thresholds
- streaming tee promotion lands remote chunks on NVMe during the transfer;
  the post-read completion pass assembles whole files so warm reads take
  kernel passthrough at device speed
- reader nodes need real local NVMe (Azure: `L8s_v3`+; AWS: `i7ie`/`i3en`);
  pin readers and writers to explicit node classes in tests — never rely on
  whichever pod pairing gets scheduled
- measured: 5GB cold cross-node ~1.04 GB/s single reader, ~1.34 GB/s
  aggregate with two concurrent readers (EKS i7ie + S3 Express); warm
  assembled reads at NVMe/page-cache speed

### Mode 2: Write-Optimized Ingest

For writing large files fast with durability trailing asynchronously:

- local NVMe is the primary path; the caller gets ack at NVMe speed and
  cloud persist happens in the background (persist workers scale with chunk
  backlog, 4–16)
- writer node class matters more than anything in the config: `L64s_v3` is
  the best-validated Azure writer; A100 nodes are not cost-effective as
  writers; on AWS, `i7ie` write throughput tracks the device
- keep writeback cache off unless the workload is exclusively large
  sequential writes (`goFuseWritebackCache: "true"` per deployment then)
- eviction is safe under overcommit: the watermark evictor only removes
  cloud-confirmed bytes (validated live: 5GB written through a 3GB budget,
  zero failures, zero loss)

### Mode 3: Cloud-First

For minimum-footprint or burst topologies where the cloud tier is the
authority and local/peer tiers are pure accelerators:

- size the NVMe budget small and let the watermark evictor cycle it; every
  read transparently re-fetches from cloud (chunk reads use ranged parallel
  GETs — no HEAD — on both S3 and Azure)
- put the bucket in the same AZ/zone as the nodes: S3 Express One Zone
  (`--x-s3` directory buckets, zonal endpoint) or Azure premium block blob;
  measured 1GB fully-from-Express in ~2.7s in-cluster
- smallest validated Azure topology: 1x `D4as_v5` system + 1x `D8ads_v5`
  reader + 1x `L64s_v3` writer, GPU pools at zero
- cost note: cloud-first trades read latency for footprint — the peer tier
  still activates automatically between whatever nodes exist

### Measuring (applies to every mode)

Use `scripts/ops/benchmark-fuse-scenario.sh` for numbers that stay comparable
across builds — see [Benchmarking](#benchmarking). When performance moves
unexpectedly, read the metric split in [Debugging](#debugging).

## Project Structure

```
fuse-client/
├── cmd/
│   ├── coordinator/       # Coordinator (HTTP + gRPC, etcd-backed)
│   ├── client/            # FUSE client daemon
│   ├── csi-driver/        # Kubernetes CSI node plugin
│   └── node-init/         # Per-node disk discovery/bootstrap
├── charts/
│   └── fuse-cache/        # Helm chart (+ values-eks-express.yaml overlay)
├── internal/
│   ├── api/               # Client HTTP API (files, peer read, metrics)
│   ├── cache/             # Cache manager, tiers, adaptive profiles,
│   │                      #   tee promotion, completion, eviction
│   ├── coordinator/       # Coordinator service + etcd store + leases
│   ├── fuse/              # bazil + gofuse backends (passthrough)
│   ├── nodeinit/          # Disk discovery/benchmark/mount
│   ├── agentserver/, session/, csidriver/   # CSI subsystem
│   └── pb/                # Generated gRPC stubs
├── k8s/                   # Raw manifests (+ k8s/eks/ overlay)
├── terraform/             # aws/ (EKS + S3 Express) and azure/ (AKS) modules
├── scripts/
│   ├── devbox/            # kind-based local cluster tooling
│   └── ops/               # Deploy, repair, benchmark scripts
├── tools/dashboard/       # Streamlit monitoring/warm-targeting UI (Python)
├── proto/                 # Source of truth for internal/pb (run `make proto`)
└── docs/                  # Test plans, design notes, filesystem semantics
```

See [Where Things Live](#where-things-live) for a task-to-file map.

## Building

`make build` produces all four binaries into `bin/`. Individually:

```bash
go build -o bin/coordinator ./cmd/coordinator
go build -o bin/client      ./cmd/client        # Linux only
go build -o bin/csi-driver  ./cmd/csi-driver
go build -o bin/node-init   ./cmd/node-init
```

Cross-compile the client from macOS with `GOOS=linux go build ./cmd/client`.

## Usage

For a multi-node local setup use `make dev-start` (see [Quick
Start](#quick-start)) rather than launching clients by hand — it wires the
gRPC ports and peering that the examples below omit.

### Coordinator

```bash
./bin/coordinator -port 8080 -grpc-port 9080
```

| Flag | Default | Notes |
|---|---|---|
| `-port` | `8080` | HTTP |
| `-grpc-port` | `9080` | gRPC — the default client transport |
| `-etcd-endpoints` | *(empty)* | Empty ⇒ in-memory store (single-node / local dev) |
| `-etcd-prefix` | `/fuse` | Key prefix |
| `-etcd-peer-lease-ttl` | | Peer lease TTL |

### Client

```bash
./bin/client -mount /tmp/fuse-client -nvme /tmp/nvme-cache \
  -coordinator-grpc localhost:9080 -port 8081 -grpc-port 9081
```

| Flag | Default | Notes |
|---|---|---|
| `-mount` | `/tmp/fuse-client` | FUSE mount point |
| `-nvme` | `/tmp/nvme-cache` | NVMe cache directory |
| `-fuse-backend` | `bazil` | `bazil` or `gofuse`; the chart deploys `gofuse` |
| `-coordinator-grpc` | `localhost:9080` | Default transport |
| `-coordinator` | `localhost:8080` | HTTP fallback |
| `-port` / `-grpc-port` | `8081` / `9081` | Peer API |
| `-peer-id` | auto | |
| `-cloud-provider` | `s3` | `s3`, `azure`, or `gcp` |
| `-nvme-max-gb` | `10` | Cache capacity |
| `-chunk-size` | `8` | MB |
| `-peer-labels` | | `pool=gpu,zone=a`, for warm targeting |
| `-enable-agent-server` | `true` | CSI agent gRPC server |

`./bin/client -help` prints the full set. Other flag families: `-s3-*` /
`-azure-*` / `-gcp-*` (per-provider credentials and transfer concurrency),
`-range-*` / `-hybrid-*` / `-peer-read-*` (large-file read parallelism,
prefetch, peer/cloud hedging), `-mount-retries` / `-mount-retry-delay-sec`
(FUSE mount recovery), `-node-init-config` / `-node-init-host-root` (adopt
node-init's discovered disk).

## API Endpoints

### Coordinator API

- `POST /api/peers/register` - Register a new peer
- `GET /api/peers` - Get list of active peers
- `PUT /api/peers/status` - Update peer status
- `GET /api/files/location` - Get file location
- `PUT /api/files/location` - Update file location
- `GET /api/worldview` - Global metadata view of peers/files/chunks
- `POST /api/cache/seed` - Seed a file path to a percentage of active peers
- `POST /api/snapshot` - Snapshot coordinator metadata state to disk (optional cloud persist via `persist_cloud` + `cloud_path`)
- `POST /api/restore` - Restore coordinator metadata state from disk (`path`) or cloud object (`cloud_path`)
- `GET /api/stats` - Get system statistics
- `GET /api/health` - Health check

### Client API

- `GET /api/files/{path}` - Get file content
- `PUT /api/files/{path}` - Store file content
- `DELETE /api/files/{path}` - Delete file
- `HEAD /api/files/{path}` - Check file existence
- `GET /api/files/{path}/size` - Get file size
- `GET /api/peers` - Get peer list
- `POST /api/peers/{peerID}/heartbeat` - Send heartbeat
- `GET /api/cache` - List cached files
- `GET /api/cache/stats` - Get cache statistics
- `POST /api/fs/snapshot` - Snapshot local filesystem/cache entries (metadata + optional data payload), optionally persist snapshot JSON to cloud (`persist_cloud` + `cloud_path`)
- `POST /api/fs/restore` - Restore local filesystem/cache entries from direct snapshot payload or from cloud snapshot object (`cloud_path`)
- `GET /api/netprobe?bytes=N` - Stream probe payload for peer network throughput sampling
- `GET /api/health` - Health check
- `GET /metrics` - Prometheus metrics

## Known Best Configs

Current chart defaults (measured, see Operating Modes for the numbers):

- `config.fuseBackend: "gofuse"` + `config.goFuseEnablePassthrough: "true"`
- `config.goFuseWritebackCache: "false"` — A/B measured: +4-6% on large
  sequential writes but −14% on small files; the 8MiB append buffer
  (`config.goFuseWriteAppendBufferMB: "8"`) already coalesces small writes.
  Flip to `"true"` per deployment only for exclusively-large-sequential
  writers.
- `config.goFuseMaxWriteKB: "4096"` — 4MB kernel writes, fewer round-trips
- `config.chunkSizeMB: "8"`
- Range/prefetch values (`parallelRangeReads`, `rangePrefetchChunks`,
  `rangeChunkCacheMaxBytesMB`, `rangePrefetchMaxBytesMB`) are floors: the
  per-file adaptive profile widens the pipeline for large files on fast
  links and shrinks it to nothing for single-chunk files.

Per-cloud overlays: `charts/fuse-cache/values-eks-express.yaml` (EKS + S3
Express: gp2 storage class, zonal endpoint, ECR image — see its comments for
the gotchas it encodes).

## Placement And Policy Defaults

Operationally, the best results have come from treating node classes differently:

- `L64s_v3` as the write-preferred / ingest node class on Azure; `i7ie`/`i3en`
  on AWS (instance-store NVMe, 25Gbps)
- any NVMe-class node serves reads well — A100s work but are not
  cost-effective for storage roles (validated repeatedly)
- local NVMe as the primary fast path
- peer first for remote reads, with cloud added to accelerate large reads
- file-size based hybrid behavior using `hybridAlwaysMinSizeMB` and `hybridStripeMinSizeMB`

In practice, that means:

- keep hot local IO on NVMe
- publish cloud/chunk copies asynchronously where semantics allow
- benchmark with explicit writer/reader node classes instead of whichever pods happen to be selected

Azure sizing note from 2026-05-12:

- minimum Azure topology that worked well on the current build: `1x D4as_v5 system + 1x D8ads_v5 user + 1x L64s_v3 writer`
- adding `1x A100` reader did not improve the important `5GB` read path enough to justify its cost on the current build

## Cache Behavior

1. **File Read**:
   - Tier order: NVMe -> peers -> cloud, with promotion on access
   - Chunked objects are served through range reads (no full-file reassembly)
   - Remote chunks stream to local NVMe **during** the transfer (tee
     promotion); after the read session ends, a background completion pass
     fetches any missed chunks and assembles the whole file so subsequent
     reads take kernel passthrough
   - Hybrid mode adds parallel cloud reads for large files; the ordered
     fallback takes a cross-node fetch lease so a herd of misses collapses
     to one origin pull; a busy peer gets one jittered retry before the
     read falls back to cloud
   - Per-file profile picks fan-out and readahead from file size + measured
     link throughput (small files skip the range machinery entirely)

2. **File Write**:
   - NVMe first (caller acks at NVMe speed); files larger than the chunk
     size split into `_chunk_N` objects
   - Cloud persistence runs in the background with backlog-scaled workers
     (4-16); `PersistedToCloud` is tracked per entry and per chunked parent
   - If NVMe is full: evict (durable entries only), then retry; peers/cloud
     as write fallbacks

3. **Cache Management**:
   - Range cache: per-file byte budget + **global** byte budget with
     LRU-by-file eviction (active file spared) + 30s idle expiry
   - NVMe: background watermark evictor (evict at 85%, target 75%) driven
     by max(internal budget usage, real device usage via statfs); pinned
     paths and entries not yet durable in cloud are never evicted —
     validated live: 5GB written through a 3GB budget, zero loss
   - After restart, eviction HEAD-probes cloud before trusting an entry as
     evictable

## Benchmarking

Use the wrapper rather than an ad hoc terminal capture — it produces a
comparable artifact:

```bash
./scripts/ops/benchmark-fuse-scenario.sh <namespace> <size-mb> [writer-class-substr] [reader-class-substr]

# e.g.
./scripts/ops/benchmark-fuse-scenario.sh fuse-system-aztest 5120 Standard_L64s_v3 Standard_NC24ads_A100_v4
```

It appends a CSV row to `/tmp/fuse-benchmark-results.csv` capturing file size,
writer/reader node classes, write and read throughput, per-tier read
contribution, peer and cloud object-path throughput, go-fuse write/sync/flush
timing, range-cache and prefetch state after the read, Go heap and goroutine
snapshots, node and pod CPU from `kubectl top`, coordinator peer network
telemetry, and the git commit + image tags.

Two conventions that keep rows comparable: pin writer and reader to explicit
node classes instead of whichever pods get scheduled, and record the git SHA
(the script does this for you). Peer and cloud speeds come from per-tier
metric deltas on the reader pod (`/api/cache/stats`); object speed is the
script's end-to-end `READ_MBPS_APPROX`.

Headline results are in [Highlights](#highlights-validated-aug-2026) and the
per-mode sections above. Full run history — including the Feb–Mar 2026 AKS/EKS
matrix — lives in `docs/` and the CSV artifacts rather than here.

## Dashboard

Enable the chart dashboard support when Grafana watches dashboard ConfigMaps:

```yaml
monitoring:
  serviceMonitor:
    enabled: true
  grafanaDashboard:
    enabled: true
    labels:
      grafana_dashboard: "1"
```

The `FUSE Cache Overview` dashboard covers hot local read and overall write
throughput, per-tier read contribution and source share, chunk fetch latency
by tier, go-fuse write vs sync vs flush time, runtime memory / range-cache
bytes / prefetch reservation, and goroutines with in-flight prefetch.

### Streamlit UI

`tools/dashboard/` is a read-mostly Python UI over the same coordinator and
client HTTP APIs — cluster overview, per-node cache counters, warm targeting
(fan-out and single-node), replicas and heat. No server-side component.

```bash
pip install -r tools/dashboard/requirements.txt
streamlit run tools/dashboard/app.py
```

Configure via the sidebar or `FUSE_COORDINATOR_URL`, `FUSE_API_KEY`,
`FUSE_NODE_ADDR`. The node-address override exists because peers register as
`POD_IP:8081`, which is unreachable from outside the cluster — port-forward a
client pod and point the override at it. See `tools/dashboard/README.md`.

## Configuration

### Environment Variables

```bash
# AWS S3 configuration
export AWS_ACCESS_KEY_ID=your-access-key
export AWS_SECRET_ACCESS_KEY=your-secret-key
export AWS_REGION=us-east-1
export S3_BUCKET=fuse-client-cache
export S3_REGION=us-east-1
export FUSE_S3_DOWNLOAD_CONCURRENCY=32
export FUSE_S3_DOWNLOAD_PART_SIZE_MB=8
export FUSE_S3_UPLOAD_CONCURRENCY=16
export FUSE_S3_UPLOAD_PART_SIZE_MB=8
export FUSE_S3_FORCE_PATH_STYLE=false
# Optional endpoint override (for S3-compatible object stores)
export S3_ENDPOINT=

# Azure Blob configuration
export AZURE_STORAGE_ACCOUNT=your-account
export AZURE_STORAGE_KEY=your-key
export AZURE_CONTAINER_NAME=fuse-cache

# GCP Cloud Storage configuration (S3 interoperability HMAC keys)
export GCP_ACCESS_KEY_ID=your-gcs-hmac-access-key
export GCP_SECRET_ACCESS_KEY=your-gcs-hmac-secret
export GCP_BUCKET=fuse-client-cache

# Cache Configuration
export FUSE_NVME_SIZE=10737418240  # 10GB
export FUSE_PEER_SIZE=5368709120   # 5GB
export FUSE_PEER_READ_MBPS=10000   # force peer-first for cold-read performance profiling
export FUSE_PARALLEL_RANGE_READS=8
export FUSE_RANGE_PREFETCH_CHUNKS=2
export FUSE_RANGE_CHUNK_CACHE_SIZE=16
export FUSE_RANGE_CHUNK_CACHE_MAX_BYTES_MB=512
export FUSE_RANGE_PREFETCH_MAX_BYTES_MB=128
export FUSE_PEER_TIMEOUT=30s
export FUSE_CLOUD_TIMEOUT=60s
export FUSE_IO_PROGRESS_MB=512   # set 0 to disable read/write progress logs
export FUSE_NETPROBE_ENABLED=true
export FUSE_NETPROBE_BYTES=1048576
export FUSE_NETPROBE_TIMEOUT_MS=2000
export FUSE_NETPROBE_EVERY_HEARTBEATS=2
```

`FUSE_PEER_SIZE` controls the remote read strategy threshold (in bytes).
`FUSE_PEER_READ_MBPS` controls the hybrid-read throughput model.
`FUSE_PARALLEL_RANGE_READS`, `FUSE_RANGE_PREFETCH_CHUNKS`, and
`FUSE_RANGE_CHUNK_CACHE_SIZE` tune range-read throughput behavior.
`FUSE_RANGE_CHUNK_CACHE_MAX_BYTES_MB` and `FUSE_RANGE_PREFETCH_MAX_BYTES_MB`
turn those controls into explicit byte budgets so large reads stay stable.
`FUSE_S3_DOWNLOAD_*` and `FUSE_S3_UPLOAD_*` tune multipart transfer throughput for S3.
`FUSE_IO_PROGRESS_MB` controls periodic FUSE read/write progress logging cadence.
`FUSE_NETPROBE_*` controls optional peer-to-peer network probing and telemetry
published to coordinator peer metadata (`network_speed_mbps`, `network_latency_ms`).

### Cache Sizes

Default cache sizes:
- NVME: 10GB
- Peer: 5GB
- Cloud: Unlimited (depends on object storage bucket/container)

## Dependencies

- `github.com/hanwen/go-fuse/v2` - default FUSE backend (local fork in
  `third_party/`, kernel passthrough support); `bazil.org/fuse` remains as
  the alternate backend
- `github.com/aws/aws-sdk-go` v1.55+ - AWS S3 client (S3 Express One Zone
  directory buckets supported; Content-MD5 auto-disabled for `--x-s3`)
- `github.com/Azure/azure-sdk-for-go/sdk/storage/azblob` - Azure Blob client
- GCP support uses the AWS S3 SDK against `storage.googleapis.com` (S3 interoperability mode)
- `github.com/gorilla/mux` - HTTP router
- `github.com/sirupsen/logrus` - Logging
- `google.golang.org/grpc` + `google.golang.org/protobuf` - the default
  transport for coordinator, peer, and CSI-agent traffic (HTTP remains as
  fallback); stubs generated into `internal/pb/`
- `go.etcd.io/etcd/client/v3` - coordinator state; optional, in-memory
  fallback when `-etcd-endpoints` is empty
- `github.com/container-storage-interface/spec` - CSI API
- `helm.sh/helm/v3` - Kubernetes package manager for deployment templates

## Security

- File access is controlled through FUSE permissions
- Peer-to-peer communication uses HTTP (HTTPS recommended for production)
- Cloud credentials follow provider best practices (AWS IAM, Azure access keys/identity, GCP HMAC interoperability keys)

## Monitoring

Clients expose Prometheus metrics at `GET /metrics` (no auth) and richer
JSON at `GET /api/cache/stats`; the coordinator serves cluster-wide
`GET /api/stats`. Every service has `GET /api/health`, and peers heartbeat
into the coordinator's TTL-leased registry.

Read-path throughput breaks down per tier, each as a bytes/seconds/mbps
triple:

```
fuse_cache_{peer,cloud,nvme}_read_{bytes_total,seconds_total,mbps}
```

See [Debugging](#debugging) for which metrics answer which question, and
[Dashboard](#dashboard) for the Grafana and Streamlit front ends.

## Future Enhancements

- Streaming write-side cloud persist (mirror of the read-path tee — writes
  currently buffer whole chunks before upload)
- Per-chunk eviction granularity for chunked entries under tight budgets
- gRPC transport for fetch leases (currently HTTP fallback)
- Content checksums on remote-tier chunk reads (size-only today)
- Encryption in transit for the peer tier (kTLS-style, keeping sendfile)

## Contributing

1. Fork, branch, and make your change. [Where Things
   Live](#where-things-live) maps subsystems to files.
2. Add tests. `make test-race` must pass — this codebase is concurrent
   throughout and a test that only passes without `-race` is not passing.
3. Run `make fmt vet lint`.
4. If you touched `proto/*.proto`, run `make proto` and commit the regenerated
   `internal/pb/` alongside it.
5. If you added an adaptive scorer, a metric, or anything on the peer serve
   path, re-read the three conventions in [Where Things
   Live](#where-things-live) — those are the ones that have actually caused
   silent production bugs here.
6. Open a PR describing what you measured, not just what you changed.
   Performance claims in this repo are expected to come with numbers; see
   `scripts/ops/benchmark-fuse-scenario.sh`.

## Kubernetes Dev Box & Helm Chart

The repo now includes a Helm chart and a local kind-based dev box workflow so you can test all Kubernetes resources quickly.

### Helm Chart

Chart location:

```bash
charts/fuse-cache
```

Validate and render chart templates:

```bash
helm lint charts/fuse-cache
helm template fuse-cache charts/fuse-cache
```

Key client mount settings:

- `client.fuseMountHostPath`: host path where mount appears (default `/mnt/fuse`)
- `client.fuseMountContainerPath`: in-container bind path (default `/host/mnt/fuse`)
- `config.fuseMountPath`: mount target used by the client process (default `/host/mnt/fuse`)

Enable Prometheus scraping + Grafana dashboard objects:

```bash
helm upgrade --install fuse-cache charts/fuse-cache \
  --namespace fuse-system \
  --create-namespace \
  --set monitoring.serviceMonitor.enabled=true \
  --set monitoring.grafanaDashboard.enabled=true
```

If you need to install Prometheus + Grafana on AKS:

```bash
helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
helm repo update
helm upgrade --install kube-prometheus prometheus-community/kube-prometheus-stack \
  --namespace monitoring \
  --create-namespace
```

Monitoring values:

- `monitoring.serviceMonitor.enabled`
- `monitoring.serviceMonitor.namespace`
- `monitoring.serviceMonitor.interval`
- `monitoring.serviceMonitor.scrapeTimeout`
- `monitoring.serviceMonitor.labels`
- `monitoring.grafanaDashboard.enabled`
- `monitoring.grafanaDashboard.namespace`
- `monitoring.grafanaDashboard.labels`

If you use raw manifests instead of Helm, apply:

```bash
kubectl apply -f k8s/servicemonitor.yaml
kubectl apply -f k8s/grafana-dashboard-configmap.yaml
```

Install to any cluster:

```bash
helm upgrade --install fuse-cache charts/fuse-cache \
  --namespace fuse-system \
  --create-namespace
```

### Local Kubernetes Dev Box (kind)

The `make k8s-devbox-*` targets in [Quick Start](#kubernetes-locally) wrap
`scripts/devbox/devbox.sh`, which you can also drive directly:

```bash
./scripts/devbox/install-tools.sh all   # helm/kind/kubectl, if needed
./scripts/devbox/devbox.sh create       # create local kind cluster
./scripts/devbox/devbox.sh deploy       # install Helm chart
./scripts/devbox/devbox.sh status       # inspect resources
./scripts/devbox/devbox.sh delete       # tear down cluster
```

## Ops Scripts

Reusable operational commands live under:

```bash
scripts/ops/
```

### Bootstrap Azure + AWS CLI cloud env

This validates Azure/AWS CLI auth, optionally updates AKS/EKS kube contexts,
and writes a reusable env file for cloud test runs.

```bash
# Ubuntu: install required CLIs/tools first (azure-cli, awscli, kubectl, helm, jq)
./scripts/ops/bootstrap-cloud-env.sh --install-tools --skip-context-update

# Then bootstrap cloud env + contexts
./scripts/ops/bootstrap-cloud-env.sh \
  --azure-subscription <azure-subscription-id> \
  --aks-rg stargz-test_group \
  --aks-name stargz-test \
  --eks-name stargz-test \
  --aws-region us-east-1
```

Then load and use it:

```bash
source .env.cloud
kubectl config use-context "$CLOUD_AKS_CONTEXT"
./scripts/ops/test-smart-read.sh "$CLOUD_AKS_NAMESPACE" 1024
```

### Deploy client image tag

```bash
./scripts/ops/deploy-client-image.sh <release> <namespace> <image-tag> [chart-path]
```

Example:

```bash
./scripts/ops/deploy-client-image.sh fuse-cache-aztest fuse-system-aztest <tag> charts/fuse-cache
```

### Repair stale FUSE mount on a VMSS instance

```bash
./scripts/ops/repair-fuse-mount.sh <resource-group> <vmss-name> <instance-id> [mount-path]
```

Example:

```bash
./scripts/ops/repair-fuse-mount.sh mc_stargz-test_group_stargz-test_westus aks-memnvme-38123922-vmss 2 /mnt/fuse
```

### Run NVMe vs FUSE benchmark

```bash
./scripts/ops/bench-fuse-io.sh <namespace> [size-mb] [pod-name] [with-read]
```

Examples:

```bash
# 1GiB write-only benchmark
./scripts/ops/bench-fuse-io.sh fuse-system-aztest 1024

# 1GiB write + read benchmark
./scripts/ops/bench-fuse-io.sh fuse-system-aztest 1024 client-abc123 with-read
```

### Run 5GiB smart-read test (peer/cloud by size)

```bash
./scripts/ops/test-smart-read-5gb.sh <namespace> [writer-pod] [reader-pod]
```

Examples:

```bash
# Auto-select writer/reader pods
./scripts/ops/test-smart-read-5gb.sh fuse-system-aztest

# Explicit writer and reader pods
./scripts/ops/test-smart-read-5gb.sh fuse-system-aztest client-a client-b
```

### Run profile-aware smart-read test (standard vs s3express)

```bash
./scripts/ops/test-smart-read-s3-profile.sh <namespace> <standard|s3express> <size-mb> [writer-pod] [reader-pod]
```

Examples:

```bash
# Standard S3 run (1GiB)
./scripts/ops/test-smart-read-s3-profile.sh fuse-system-awstest standard 1024

# S3 Express run (5GiB), with zone alignment check against endpoint zone-id
./scripts/ops/test-smart-read-s3-profile.sh fuse-system-awstest s3express 5120
```

### Run go-fuse cached-read suite (1GiB + 5GiB)

```bash
# Ensure go-fuse backend is enabled in the release
helm upgrade fuse-cache-aztest charts/fuse-cache -n fuse-system-aztest \
  --reuse-values \
  --set config.fuseBackend=gofuse \
  --set config.goFuseEnablePassthrough=true \
  --set config.nvmeMaxGB=48

# Run 1GiB + 5GiB cold-read vs cached-read throughput suite
./scripts/ops/test-gofuse-cached-read-suite.sh fuse-system-aztest

# Optional: increase cold/cached read retry budget for large-file convergence
READ_RETRIES=12 READ_RETRY_DELAY_SEC=5 HYBRID_SETTLE_SEC=30 \
  ./scripts/ops/test-gofuse-cached-read-suite.sh fuse-system-aztest
```

## License

This project is licensed under the MIT License. 

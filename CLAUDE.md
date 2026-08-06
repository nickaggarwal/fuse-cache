# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What This Is

A distributed file system that mounts as a local FUSE directory on every node. Files written to the mount are cached on local NVMe for speed, replicated across peers for availability, and persisted to cloud object storage (Azure Blob / AWS S3 / GCP Cloud Storage) for durability. Reads hit the fastest tier first and fall through automatically. Large files are chunked and transferred in parallel.

The end goal: any node can read any file at NVMe speed, with the guarantee that data survives node failures because every write is durably backed by cloud storage.

## Architecture

```
┌──────────────────────────────────────────────────────────────────┐
│  Application / User                                              │
│       │                                                          │
│       ▼                                                          │
│  ┌──────────┐   FUSE mount at /mnt/fuse                         │
│  │   FUSE   │   backend = bazil.org/fuse OR hanwen/go-fuse      │
│  └────┬─────┘   (-fuse-backend bazil|gofuse)                     │
│       │                                                          │
│       ▼                                                          │
│  ┌─────────────────────────────────────────────────────────────┐ │
│  │              CacheManager (cache.go)                        │ │
│  │                                                             │ │
│  │  ┌─────────────────────────────────────────────────────┐    │ │
│  │  │  WRITE PATH                                         │    │ │
│  │  │                                                     │    │ │
│  │  │  1. Write to NVMe (local disk)                      │    │ │
│  │  │       ↓ success                                     │    │ │
│  │  │  2. Background persist to Cloud (write-through)     │    │ │
│  │  │       → goroutine, doesn't block caller             │    │ │
│  │  │                                                     │    │ │
│  │  │  If NVMe full → LRU eviction → retry                │    │ │
│  │  │  If NVMe fails → race Peer vs Cloud in parallel     │    │ │
│  │  │       → return on first success                     │    │ │
│  │  │                                                     │    │ │
│  │  │  If file > 4MB → split into chunks                  │    │ │
│  │  │       → write all chunks in parallel                │    │ │
│  │  │       → each chunk follows the same write path      │    │ │
│  │  └─────────────────────────────────────────────────────┘    │ │
│  │                                                             │ │
│  │  ┌─────────────────────────────────────────────────────┐    │ │
│  │  │  READ PATH                                          │    │ │
│  │  │                                                     │    │ │
│  │  │  1. Try NVMe (local, ~μs latency)                   │    │ │
│  │  │       ↓ miss                                        │    │ │
│  │  │  2. Try Peers (network, ~ms latency)                │    │ │
│  │  │       ↓ miss                                        │    │ │
│  │  │  3. Try Cloud (Azure/S3, ~100ms latency)            │    │ │
│  │  │       ↓ hit at any tier                             │    │ │
│  │  │  4. Background promote to NVMe (warm the cache)     │    │ │
│  │  └─────────────────────────────────────────────────────┘    │ │
│  │                                                             │ │
│  │  ┌──────────┐  ┌──────────┐  ┌─────────────────────────┐   │ │
│  │  │ Tier 1   │  │ Tier 2   │  │ Tier 3                  │   │ │
│  │  │ NVMe     │  │ Peers    │  │ Cloud (Azure Blob / S3 / GCS) │   │ │
│  │  │ Local    │  │ HTTP     │  │ Durable, persistent     │   │ │
│  │  │ ~μs      │  │ ~ms      │  │ ~100ms                  │   │ │
│  │  │ 10GB cap │  │ 3x repl  │  │ Unlimited               │   │ │
│  │  │ LRU evict│  │ shuffle  │  │ Write-through from T1   │   │ │
│  │  └──────────┘  └──────────┘  └─────────────────────────┘   │ │
│  └─────────────────────────────────────────────────────────────┘ │
│       │                                                          │
│       ▼                                                          │
│  ┌──────────────┐  HTTP API on :8081, peer gRPC on :9081         │
│  │  API Server  │  File CRUD, peer info, cache stats, health     │
│  │  gorilla/mux │  Auth via X-API-Key, 100MB upload limit        │
│  └──────────────┘  Peer-to-peer chunk transfer uses gRPC stream  │
└──────────────────────────────────────────────────────────────────┘
         │
         │ Register, heartbeat, file locations
         │ (gRPC on :9080, HTTP on :8080 as fallback)
         ▼
┌────────────────────────────────────────────────────────────┐
│  Coordinator (:8080, 3 replicas behind ClusterIP Service)  │
│  - Stateless service; all state in etcd                    │
│  - Peer registry with TTL leases (auto-expire on crash)    │
│  - File location metadata, RMW via etcd CAS                │
│  - Coordinator interface: server (CoordinatorService) +    │
│    two client impls — GRPCCoordinatorClient (default,      │
│    :9080) and CoordinatorClient (HTTP, :8080)              │
│  - Falls back to in-memory store when -etcd-endpoints      │
│    is empty (single-node / local-dev mode)                 │
└────────────────────────────────────────────────────────────┘
         │
         ▼
┌──────────────────────────────────────────┐
│  etcd cluster (StatefulSet, 3 replicas)  │
│  - Source of truth for coordinator state │
│  - /fuse/peers/<id>  (with TTL lease)    │
│  - /fuse/files/<path> (JSON []FileLoc.)  │
└──────────────────────────────────────────┘
```

## Data Flow Details

### Write: Small File (< chunk size, default 8MB)

```
PUT /api/files/report.csv  (2MB)
  │
  ├─→ NVMe write (sync, blocks caller)
  │     └─ Success → return 200 to caller immediately
  │                    └─→ goroutine: persist to Azure Blob (background, fire-and-forget)
  │
  └─ If NVMe full:
       ├─→ LRU eviction (delete oldest until 90% capacity)
       ├─→ Retry NVMe write
       └─ If still full:
            ├─→ goroutine: write to Peers (3x replication, shuffled)
            ├─→ goroutine: write to Cloud
            └─ Return 200 on first goroutine success
```

### Write: Large File (> chunk size)

```
PUT /api/files/dataset.parquet  (50MB, chunk size = 8MB)
  │
  ├─ Split into N chunks: dataset.parquet_chunk_0 .. _chunk_(N-1)
  │
  ├─ Launch N goroutines in parallel, each chunk follows the small-file write path:
  │     chunk_0  → NVMe → background cloud persist
  │     chunk_1  → NVMe → background cloud persist
  │     ...
  │
  ├─ sync.WaitGroup: wait for ALL chunks to succeed
  └─ Return 200 when all chunks stored
```

Large reads are correspondingly parallel: range reads fan out across chunks
with prefetch and a per-file chunk cache, and for very large files the reader
can hedge/stripe across peers and cloud simultaneously (see the `hybrid-*`,
`range-*`, and `peer-read-*` client flags).

### Read: Cache Miss → Tier Fallback

```
GET /api/files/model.bin
  │
  ├─ Tier 1 (NVMe): Exists? Read. → Hit → return data
  │
  ├─ Tier 2 (Peers): Ask each active peer via HTTP HEAD/GET
  │     → Hit → return data
  │              └─→ goroutine: promote to NVMe (copy to local for next read)
  │
  ├─ Tier 3 (Cloud): Download from Azure Blob / S3 / GCS
  │     → Hit → return data
  │              └─→ goroutine: promote to NVMe
  │
  └─ All miss → 404
```

### Durability Guarantee

Every file eventually exists in cloud storage. The write-through path is:
1. Caller writes → NVMe stores it → caller gets 200 (fast path, <1ms)
2. Background goroutine copies the same bytes to Azure Blob / S3 (slow path, ~100ms)
3. If background persist fails, it logs the error. The data still exists on NVMe.
4. If the node dies before cloud persist completes, the data is lost. For stronger guarantees, set NVMe as a fallback and write to cloud synchronously.

### Eviction

NVMe has a configurable capacity (default 10GB). When a write would exceed capacity:
1. Collect all NVMe entries, sort by `LastAccessed` (oldest first)
2. Delete oldest entries until usage drops to 90% of max
3. Retry the write
4. Evicted data is NOT lost — it was already persisted to cloud via write-through

## Three Binaries

| Binary | Source | Role |
|--------|--------|------|
| `coordinator` | `cmd/coordinator/main.go` | Central registry. Tracks which peers are alive and where files live. HTTP :8080, gRPC :9080. |
| `client` | `cmd/client/main.go` | Runs on every storage node. Mounts FUSE, manages 3-tier cache, serves API (:8081) + peer gRPC (:9081), and (by default) the CSI agent gRPC server on a Unix socket. |
| `csi-driver` | `cmd/csi-driver/main.go` | Kubernetes CSI node plugin. Implements CSI Identity + Node services; delegates all cache work to the client daemon's agent over a Unix socket. |
| `node-init` | `cmd/node-init/main.go` | Per-node disk bootstrap (`internal/nodeinit`). Init mode discovers the best local disk (media class via sysfs, free space, micro-benchmark), prepares `<mount>/fuse-cache`, and writes `node-init.json`; daemon mode keeps refreshing measured capacity. The client adopts the discovered dir/budget via `-node-init-config`/`-node-init-host-root`, making the DaemonSet cloud-agnostic. |

## CSI / Agent / Session Subsystem

This is the Kubernetes integration layer. It does **not** reimplement caching —
it orchestrates the existing client daemon. Three packages cooperate over a
Unix-socket gRPC link (defined in `proto/agent.proto`):

```
kubelet → csi-driver (NodePublishVolume) → agent gRPC (Unix socket) → session.Manager
                                                                          │
                                              bind-mounts a subtree of the FUSE mount
                                              into the pod's target path
```

| Package | Role |
|---------|------|
| `internal/csidriver/` (`driver.go`) | CSI Identity+Node services. On publish/unpublish it calls the agent's `CreateSession`/`DeleteSession`, then bind-mounts the returned host path. Holds no cache state. |
| `internal/agentserver/` (`server.go`, `client.go`) | gRPC `AgentService` server (runs inside the client daemon, started when `-enable-agent-server`) + client (used by the CSI driver). |
| `internal/session/` (`manager.go`) | `session.Manager`: tracks active volume mount sessions keyed by volume ID, with **reference counting** (multiple pods sharing a volume) and **pinning** (prevents eviction while mounted). Translates CSI volume context → `CachePolicy` (cacheMode, warmup, pinned, sourcePolicy). |

Key invariant: refcount controls teardown — only the *last* unmount of a volume
deletes the session. macOS note: agent Unix socket paths must stay under 108
chars, so tests use `/tmp`, not `t.TempDir()`.

## Key Packages

| Package | Files | Responsibility |
|---------|-------|----------------|
| `internal/cache/` | `cache.go` | CacheManager: orchestrates reads/writes across tiers, chunking, eviction, write-through |
| | `nvme_storage.go` | Tier 1: local filesystem I/O |
| | `peer_storage.go` | Tier 2: HTTP client to other peers, 3x replication, crypto/rand shuffle |
| | `cloud_storage.go` | Tier 3 (S3): AWS SDK, configurable bucket/region |
| | `azure_storage.go` | Tier 3 (Azure): Azure Blob SDK, configurable account/container |
| | `gcp_storage.go` | Tier 3 (GCP): GCS via S3 interoperability endpoint, configurable bucket + HMAC keys |
| | `peer_storage.go` | Tier 2 client (HTTP path) |
| | `grpc_peer_server.go` | Tier 2 server: serves peer reads/writes via streaming gRPC (`PeerService`, 24MiB stream chunks) |
| `internal/coordinator/` | `coordinator.go` | `CoordinatorService` orchestration: peer ops, file metadata, world view, seed-to-peers. Delegates state to `Store`. |
| | `store.go` | `Store` interface + `InMemoryStore` (single-process fallback) |
| | `etcd_store.go` | `EtcdStore`: peer keys with TTL lease, file-location keys with CAS via etcd Txn |
| | `client.go` | `Coordinator` interface + `CoordinatorClient` (HTTP) |
| | `grpc_server.go` / `grpc_client.go` | gRPC transport for the same `Coordinator` interface (default path; `GRPCCoordinatorClient` falls back to HTTP for endpoints not yet on gRPC) |
| | `manifest.go` | Cache-warming manifest: `BuildManifest` enumerates known file locations; `/api/fs/snapshot` writes it, `/api/fs/restore` rehydrates peer NVMe from cloud. Replaces the old snapshot/restore scheme. |
| `internal/api/` | `handler.go` | Client HTTP API: file CRUD, peer ops, cache stats, health, snapshot/restore, Prometheus `/metrics`. Auth middleware, upload limits, path validation |
| | `warm.go` | `POST /api/cache/warm` + the async warm-job registry behind `GET /api/cache/warm[/{id}]` |
| `internal/nodelabels/` | `nodelabels.go` | Reads this pod's Node object to derive peer labels (`-peer-labels-from-node`); advisory, no client-go |
| `internal/fuse/` | `filesystem.go` | FUSE filesystem (bazil backend): Dir/File nodes backed by CacheManager |
| | `gofuse_backend.go` | Alternative FUSE backend on `hanwen/go-fuse/v2` (`-fuse-backend gofuse`); supports passthrough + writeback tuning |
| `internal/fusemetrics/` | `metrics.go` | Atomic counters for the go-fuse write path, surfaced via `/metrics` |
| `internal/agentserver/`, `internal/session/`, `internal/csidriver/` | — | CSI subsystem (see section above) |
| `internal/pb/` | `*.pb.go` | Generated protobuf/gRPC stubs for coordinator, peer, and agent services |

## Build & Run

```bash
make build                    # builds bin/coordinator, bin/client, bin/csi-driver
make run-coordinator          # coordinator on :8080
make run-client-1             # client on :8081, mount at /tmp/fuse-client1
make dev-setup && make dev-start  # full 3-node dev environment (wires gRPC ports)
make dev-stop                 # kill all processes
make proto                    # regenerate internal/pb from proto/*.proto
```

> `bazil.org/fuse` and `hanwen/go-fuse` are Linux-only — `cmd/client` cannot
> build on macOS. Cross-compile with `GOOS=linux go build ./cmd/client`.
> `make proto` needs `protoc` plus `protoc-gen-go`/`protoc-gen-go-grpc` on
> `PATH` (e.g. `PATH=$PATH:$HOME/go/bin`).

### Local Kubernetes (kind)

```bash
make k8s-devbox-install-tools   # install helm/kind/kubectl locally
make k8s-devbox-create          # create local kind cluster
make k8s-devbox-deploy          # deploy the Helm chart
make k8s-devbox-status / -delete
```

### Dashboard (`tools/dashboard/`)

Streamlit UI for monitoring and warm-targeting the cluster. Read-mostly; no
server-side code — it consumes the existing coordinator/client HTTP APIs and
the Prometheus `/metrics` text.

```bash
pip install -r tools/dashboard/requirements.txt
streamlit run tools/dashboard/app.py
```

- `app.py` — 4 views: cluster overview, per-node cache (herd-control counters),
  warm targeting (fan-out + single node), replicas & heat
- `fuse_api.py` — HTTP client (`ApiResult`, never raises) + `select_warm_targets`,
  a client-side mirror of `CoordinatorService.SelectWarmTargets` for dry-run preview
- `metrics.py` — Prometheus text parser (no `prometheus_client` dep)
- `test_dashboard.py` — stdlib `unittest`: `python3 -m unittest discover -s tools/dashboard`

Config via sidebar with env defaults `FUSE_COORDINATOR_URL`, `FUSE_API_KEY`,
`FUSE_NODE_ADDR`. The node-address override exists because peers register with
`POD_IP:8081`, unreachable from outside the cluster — port-forward a client pod
and point the override at it. Replica counts come from `/api/worldview`, not
`/api/files/locations` (the latter returns one location per path). See
`tools/dashboard/README.md`.

## Testing

```bash
make test          # go test ./...
make test-verbose  # go test -v ./...
make test-race     # go test -race ./...   (CSI/agent/session suites are race-validated)
```

Run a single test: `go test ./internal/session/ -run TestManager_RefCount -v`.
Test coverage spans cache tiers, coordinator (incl. gRPC + manifest), API
handlers, and the CSI subsystem (session manager, agent server, CSI driver).

## AKS Deployment

```bash
az acr build --registry stargzrepo --image fuse-client:latest .
kubectl apply -f k8s/
```

Manifests in `k8s/`:
- `namespace.yaml` — `fuse-system`
- `etcd-statefulset.yaml` — 3-replica etcd cluster (headless service `etcd`, ClusterIP `etcd-client`), PodDisruptionBudget minAvailable=2
- `coordinator-deployment.yaml` — 3-replica stateless Deployment, ClusterIP service `coordinator` + headless `coordinator-headless` for gRPC LB, PodDisruptionBudget minAvailable=2
- `client-daemonset.yaml` — privileged, `/dev/fuse`, NVMe hostPath, bidirectional FUSE mount propagation; also hosts the agent socket consumed by the CSI driver
- `csi-driver.yaml`, `csi-node-daemonset.yaml`, `csi-rbac.yaml` — CSI node plugin DaemonSet, CSIDriver object, and RBAC
- `configmap.yaml` — coordinator DNS, paths, chunk size
- `secrets.yaml` — AWS credentials + Azure storage credentials
- `servicemonitor.yaml`, `grafana-dashboard-configmap.yaml` — Prometheus scrape config + dashboard for the `/metrics` endpoint

Currently deployed on `stargz-test` cluster with Azure Blob Storage as Tier 3.

## Client CLI Flags

The client has many tuning flags (`./bin/client -help` for the full set). The
load-bearing ones:

```
-mount             /tmp/fuse-client    FUSE mount point
-fuse-backend      bazil               FUSE backend: "bazil" or "gofuse"
-nvme              /tmp/nvme-cache     NVMe cache directory
-coordinator       localhost:8080      Coordinator HTTP address
-coordinator-grpc  localhost:9080      Coordinator gRPC address (default transport)
-port              8081                Peer HTTP API port
-grpc-port         9081                Peer gRPC port
-chunk-size        8                   Chunk size in MB
-cloud-provider    s3                  "s3", "azure", or "gcp"
-nvme-max-gb       10                  NVMe cache capacity
-agent-socket      /var/run/fuse-client/agent.sock   CSI agent gRPC socket
-enable-agent-server true              Run the CSI agent gRPC server
```

Other flag families: `-s3-*` / `-azure-*` / `-gcp-*` (per-provider creds and
download/upload concurrency), `-range-*` and `-hybrid-*` and `-peer-read-*`
(large-file read parallelism, prefetch, peer/cloud hedging), `-mount-retries` /
`-mount-retry-delay-sec` (FUSE mount recovery).

Coordinator flags: `-port`, `-grpc-port`, `-etcd-endpoints` (empty ⇒ in-memory),
`-etcd-prefix`, `-etcd-peer-lease-ttl`.

Environment variables: `POD_IP` / `NODE_NAME` (Kubernetes downward API),
`AZURE_STORAGE_ACCOUNT`, `AZURE_STORAGE_KEY`, `AZURE_CONTAINER_NAME`,
`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_REGION`.

## API Endpoints

**Coordinator (`:8080`):**
- `POST /api/peers/register` — register a peer
- `GET /api/peers` — list active peers
- `PUT /api/peers/status` — heartbeat / status update
- `GET/PUT /api/files/location` — file location metadata
- `GET /api/stats` — cluster stats
- `GET /api/health` — health check

**Client (`:8081+`):**
- `GET/PUT/DELETE/HEAD /api/files/{path}` — file CRUD (PUT limited to 100MB)
- `GET /api/files/{path}/size` — file size
- `GET /api/peers` — peer list (via coordinator)
- `GET /api/cache` — cache entries
- `GET /api/cache/stats` — cache stats
- `POST /api/cache/warm` — warm a prefix on this node (`async` ⇒ 202 + `job_id`)
- `GET /api/cache/warm` — list async warm jobs; `GET /api/cache/warm/{id}` — one job's live progress
- `POST /api/fs/snapshot` — build a cache-warming manifest of known files
- `POST /api/fs/restore` — rehydrate peer NVMe caches from cloud using a manifest
- `GET /metrics` — Prometheus metrics (incl. go-fuse write-path counters)
- `GET /api/health` — health check (no auth required)

All non-health endpoints require `X-API-Key` header when `-api-key` is set.

## Code Conventions

- Go module: `fuse-client`, Go 1.21
- Logger prefixes: `[COORDINATOR]`, `[CLIENT]`, `[API]`, `[FUSE]`, `[CACHE]`, `[CSI]`, `[AGENT]`
- All tier backends implement the `TierStorage` interface (Read/Write/Delete/Exists/Size)
- Coordinator operations go through the `Coordinator` interface — server uses `CoordinatorService`; clients use `GRPCCoordinatorClient` (default) or `CoordinatorClient` (HTTP)
- `CacheManager` is an interface; `DefaultCacheManager` is the implementation
- The CSI driver never touches the cache directly — it goes through the agent gRPC API and the `session.Manager`
- Generated gRPC stubs live in `internal/pb/`; do not hand-edit. Edit `proto/*.proto` and run `make proto`
- Graceful shutdown via SIGINT/SIGTERM with context cancellation
- Peer registration retries with exponential backoff (5 attempts)
- Peer selection uses `crypto/rand` for shuffle (not `math/rand`)

## Dependencies

- `bazil.org/fuse` — default FUSE implementation
- `github.com/hanwen/go-fuse/v2` — alternative FUSE backend (`-fuse-backend gofuse`). **Replaced with a local fork**: `replace github.com/hanwen/go-fuse/v2 => ./third_party/go-fuse-local` in `go.mod`
- `github.com/container-storage-interface/spec` v1.9.0 — CSI API
- `google.golang.org/grpc` + `google.golang.org/protobuf` — coordinator/peer/agent transports
- `github.com/aws/aws-sdk-go` — S3 client (also GCS via S3-interop)
- `github.com/Azure/azure-sdk-for-go/sdk/storage/azblob` v1.0.0 — Azure Blob client
- `github.com/gorilla/mux` — HTTP router
- `go.etcd.io/etcd/client/v3` v3.5.13 — distributed coordinator state (optional; in-memory fallback when `-etcd-endpoints` is empty)

## Session Context (July 2026): Thundering-Herd Control + node-init

State of the stacked-PR series implementing `docs/peer-coordination-thundering-herd.md`.
Merge order: #4 → #5 → #6 (each PR's base is the previous branch).

| PR | Branch | Contents |
|----|--------|----------|
| #4 | `peer-thundering-herd-phase1` (base: `main`) | Phase 1: serve-side admission control, busy failover + jittered retry, staggered headroom-aware replication, pairwise latency metadata |
| #5 | `peer-thundering-herd-phase2-3` (base: phase1) | Phase 2: cross-node fetch leases. Phase 3: fast chunk advertisement, demand-driven replica reconciler |
| #6 | `node-init` (base: phase2-3) | node-init disk discovery + deploy wiring + `docs/test-plan.md` (live-cluster validation incl. FUSE kill/hang lifecycle) |

### Key components added

- `internal/cache/peer_admission.go` — `peerServeGate` semaphore (default 64,
  `-peer-serve-max-inflight`). gRPC peer server rejects with
  `RESOURCE_EXHAUSTED`, raw HTTP `/api/peer/read` with 503+Retry-After.
  Requesters treat busy as flow control: skip to next holder, one jittered
  retry pass (10–150ms) only when ALL holders are busy; never evict
  connections or record latency samples on busy.
- `internal/cache/peer_latency.go` — per-(node→peer) EWMA latency/success
  from real transfers; traversal reorders on it once a pair has ≥3 samples
  (beats the coordinator's single-target netprobe). Exposed at
  `/api/peers/latency` + `fuse_peer_pair_*` metrics. **Only real transfers
  feed it**: busy (503/`RESOURCE_EXHAUSTED`) and miss (404/`NOT_FOUND`) are
  both excluded. Recording misses drove `success_ratio` to ~0 on every pair
  live — traversal probes holders in order, so most attempts miss by
  construction, and `pairScore` (`success/latencyMs`) collapsed for everyone.
  Misses are counted separately as `fuse_peer_fetch_miss_skips_total`, which
  is a staleness signal for coordinator location metadata.
- `internal/cache/tier_miss.go` — the same three-class split one level up, for
  the *tier* tracker (`tier_perf.go`). `recordTierOutcome` folds a remote read
  into the peer-vs-cloud EWMA only when the failure says something about tier
  health: busy is skipped, and a miss (`isTierMiss` = `isPeerMiss` or
  `isCloudMiss`, the latter covering Azure `bloberror` + S3/GCP `awserr` 404s)
  is counted but kept out of the EWMA. Recording misses here drove
  `fuse_tier_peer_success_ratio` to 0.0 live, which **inverted `order()`** —
  cloud (~121ms) tried ahead of peer (~13ms) — and pinned `shouldHedge()` on,
  since `ewmaSuccess` never climbed back over `tierPerfReliableSuccess`. Both
  silent. Every chunk misses the primary tier at least once on a cold read, so
  this is the common case, not an edge case. Misses are exposed as
  `fuse_tier_{peer,cloud}_misses_total` so a zero sample count can be told
  apart from "all misses"; a miss also leaves `lastSampleAt` untouched, so an
  idle tier still ages into its recovery floor and gets re-probed.
- `internal/coordinator/fetch_lease*.go` — advisory short-TTL leases
  (`/api/fetch-lease`; etcd key `/fuse/inflight/<key>` with lease-backed
  expiry; in-memory map fallback). `internal/cache/origin_lease.go` gates
  ordered-fallback cloud reads: winner pulls origin, losers wait 100–400ms
  then read the peer tier. Coordinator failure ⇒ plain cloud read, always.
  Hedged/striped hybrid reads intentionally bypass leases.
- `internal/cache/chunk_advertise.go` — remotely-fetched chunks are promoted
  to NVMe and the node advertises itself as a parent-file holder on the FIRST
  chunk (coalesced: 1 publish/parent/5s), growing the swarm mid-transfer.
- `internal/cache/replica_reconciler.go` — periodic top-up of hot
  (<10min-accessed, ≤64MiB) under-replicated local objects toward R
  (default 3) via `PeerStorage.ReplicateTo` (excludes existing holders,
  staggered, busy-aware, ≤8 ops/pass, busy cluster aborts the pass).
  Cold-replica decay is deliberately delegated to LRU eviction.
- `internal/cache/reader_heat.go` — heat-proportional replication: peer-serve
  paths (gRPC + HTTP) record distinct remote readers per parent file; the
  reconciler raises its per-file target (base + readers/2, capped by
  `-replica-reconcile-max-target`, default 8) and the boost decays with the
  10-min hot window. `fuse_replica_reconcile_heat_boosts_total` metric.
- `internal/cache/warmup.go` + session warmup hook — declarative warmup
  (Fluid pattern): CSI volumeAttribute `warmup: full` triggers
  `WarmPrefixOpts` on first session create — enumerate the subtree via
  coordinator `ListFileLocations`, then pull each file whole onto NVMe
  (chunked files reuse `runChunkCompletion`, unchunked `fetchAndLandChunk`;
  headroom-guarded, never evicts). `warmup: metadata` enumerates only.
  Warmed nodes publish themselves as holders. **Strategy** via
  `WarmupOptions`: `Source` (peer-first default / cloud-first / cloud-only
  tier order) and `Bandwidth` (background = 2 files x 4 chunk fetches, max
  = 4 x 16). CSI attrs `warmupBandwidth` + `sourcePolicy` map onto it.
- Warm targeting without pods: client `POST /api/cache/warm`
  {prefix,mode,source,bandwidth,async} (`internal/api/warm.go`);
  coordinator `POST /api/warm` fans out to peers selected by
  `nodes`/`labels`/`percentage` (`internal/coordinator/warm.go`,
  intersection semantics; percentage takes an ID-sorted ceil starting at an
  FNV-1a-of-prefix offset so different prefixes rotate across the cluster;
  peers silent for >75s are dropped ahead of the 90s etcd lease; forwards
  `api_key` as X-API-Key and returns each peer's `job_id`). Peer labels:
  `PeerInfo.Labels`, set via client `-peer-labels` / `FUSE_PEER_LABELS`,
  carried in the register proto (`make proto` now includes agent.proto).
- `internal/nodelabels/` — peer labels derived from the pod's own Kubernetes
  Node object (`-peer-labels-from-node pool=agentpool,...`), because the
  downward API can't expose node labels and a hardcoded `pool=` mislabels
  every node in a multi-nodepool cluster. Raw in-cluster REST GET with the
  projected SA token (no client-go); advisory — failures fall back to
  `-peer-labels`. Manifests/chart add a `fuse-client` SA + ClusterRole with
  `get` on nodes only.
- Async warm observability: an async warm mints a job ID; the daemon tracks
  it (`warmJobs` on `api.Handler`, last 32 finished) and
  `WarmupOptions.OnProgress` feeds counters into it. Poll
  `GET /api/cache/warm/{id}`; the dashboard's warm page lists and polls them.
  Progress is **chunk-level**, not just per-file: `chunkProgress{Plan,Landed}`
  threads through `runChunkCompletionOpts` → `fetchMissingChunks`, so warming
  one huge file reports movement instead of sitting at 0/1 until it lands.
  `Plan` counts only chunks the pass must fetch (already-local chunks
  excluded, so a resumed warm still reaches its denominator).
  `WarmupProgress.InFlightBytes` covers files still open and is backed out
  when the file's bytes move into `Bytes`. Per-chunk locking is skipped when
  `OnProgress` is nil.
- `internal/nodeinit/` + `cmd/node-init/` — best-local-disk discovery:
  /proc/mounts + sysfs classification (nvme/ssd/disk/unknown), scoring
  (class dominates > log2 free space > micro-benchmark of top-3 finalists;
  well-known cloud ephemeral mounts are tie-break hints only), prepares
  `<mount>/fuse-cache`, writes `node-init.json` (dir + byte budget).
  `-mode init` (init container) / `-mode daemon` (sidecar refreshing
  capacity, re-discovers if dir vanishes). Client adopts it via
  `-node-init-config` + `-node-init-host-root` with fallback to static
  `-nvme` flags. Helm: `nodeInit.*` values (enabled by default).

### Conventions established in this work

- Busy (`RESOURCE_EXHAUSTED`/503) is admission control, NOT failure: don't
  reconnect-retry the same node, don't record it in perf trackers.
- Miss (`NOT_FOUND`/404) is stale routing, NOT failure: same treatment —
  no reconnect-retry, no transport fallback (both read the same local cache),
  and never in the latency EWMA. Peer errors have three classes, not two;
  `isPeerBusy` / `isPeerMiss` / everything-else. Anything new on the peer
  serve path must return a *typed* miss, or it silently becomes "failure".
  The same three classes apply to whole tiers (`isTierMiss`), and the rule
  generalizes: **every adaptive scorer must be fed only outcomes that carry
  the signal it scores.** A health EWMA fed cache misses measures replica
  placement, not health — and it fails silently, because a collapsed score
  looks exactly like a genuinely bad link. When adding a scorer, ask what
  each error class actually means to *that* metric.
- Tests must not race wall-clock timers against real I/O. `time.Sleep(5ms)`
  to "release after the first attempt" passes bare and fails ~1-in-3 under
  `-race`, where a dial can outrun the timer. Key the handoff off observed
  state (a counter, a channel) so the ordering is causal.
- All coordination is advisory — reads must succeed with the coordinator
  down; correctness never depends on leases/advertisement/reconciler.
- Jitter everything that could synchronize (`sleepWithJitter`, crypto/rand).
- Herd-control counters live on `DefaultCacheManager` (`PeerLoadSnapshot`,
  `HerdControlSnapshot`) and are exported in `handlePromMetrics`.
- Tests for peer behavior use real in-process gRPC servers via
  `startHerdPeer` (`peer_herd_test.go`) and a lease-capable mock coordinator
  (`herd_phase23_test.go`).

### Known gaps / follow-ups

- Remote-tier chunk reads verify size only, not content checksums
  (noted in test plan IR-3).
- Fetch leases ride the coordinator HTTP fallback, not gRPC proto (add to
  `proto/coordinator.proto` if lease QPS matters).
- node-init end-to-end tested against synthetic host roots only — needs one
  real AKS/EKS/GKE smoke rollout (test plan NI-1).
- `docs/test-plan.md` (TH/NI/IR/FL scenarios) is written but not yet executed
  against a live cluster.
- Stray uncommitted files in repo root (`tmp_s3probe*.go`, test binaries,
  `client`/`coordinator`/`csi-driver` dirs) predate this work — left alone.

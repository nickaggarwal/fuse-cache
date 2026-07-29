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

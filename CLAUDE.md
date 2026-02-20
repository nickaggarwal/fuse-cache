# CLAUDE.md - Distributed FUSE Cache System

## What This Is

A distributed file system that mounts as a local FUSE directory on every node. Files written to the mount are cached on local NVMe for speed, replicated across peers for availability, and persisted to cloud object storage (Azure Blob / AWS S3) for durability. Reads hit the fastest tier first and fall through automatically. Large files are chunked and transferred in parallel.

The end goal: any node can read any file at NVMe speed, with the guarantee that data survives node failures because every write is durably backed by cloud storage.

## Architecture

```
┌──────────────────────────────────────────────────────────────────┐
│  Application / User                                              │
│       │                                                          │
│       ▼                                                          │
│  ┌──────────┐   FUSE mount at /mnt/fuse                         │
│  │   FUSE   │   bazil.org/fuse — presents files as a local dir  │
│  └────┬─────┘                                                    │
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
│  │  │ NVMe     │  │ Peers    │  │ Cloud (Azure Blob / S3) │   │ │
│  │  │ Local    │  │ HTTP     │  │ Durable, persistent     │   │ │
│  │  │ ~μs      │  │ ~ms      │  │ ~100ms                  │   │ │
│  │  │ 10GB cap │  │ 3x repl  │  │ Unlimited               │   │ │
│  │  │ LRU evict│  │ shuffle  │  │ Write-through from T1   │   │ │
│  │  └──────────┘  └──────────┘  └─────────────────────────┘   │ │
│  └─────────────────────────────────────────────────────────────┘ │
│       │                                                          │
│       ▼                                                          │
│  ┌──────────────┐  HTTP API on :8081                             │
│  │  API Server  │  File CRUD, peer info, cache stats, health     │
│  │  gorilla/mux │  Auth via X-API-Key, 100MB upload limit        │
│  └──────────────┘  Path traversal protection                     │
└──────────────────────────────────────────────────────────────────┘
         │
         │ Register, heartbeat, file locations (HTTP)
         ▼
┌──────────────────────────────────────────┐
│  Coordinator (:8080)                     │
│  - In-memory peer registry               │
│  - File location metadata                │
│  - Periodic cleanup of stale peers (60s) │
│  - State persisted to disk (JSON)        │
│  - Coordinator interface: both server    │
│    (CoordinatorService) and client       │
│    (CoordinatorClient via HTTP) impls    │
└──────────────────────────────────────────┘
```

## Data Flow Details

### Write: Small File (< 4MB)

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

### Write: Large File (> 4MB)

```
PUT /api/files/dataset.parquet  (50MB, chunk size = 4MB)
  │
  ├─ Split into 13 chunks: dataset.parquet_chunk_0 .. _chunk_12
  │
  ├─ Launch 13 goroutines in parallel, each chunk follows the small-file write path:
  │     chunk_0  → NVMe → background cloud persist
  │     chunk_1  → NVMe → background cloud persist
  │     ...
  │     chunk_12 → NVMe → background cloud persist
  │
  ├─ sync.WaitGroup: wait for ALL chunks to succeed
  └─ Return 200 when all chunks stored
```

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
  ├─ Tier 3 (Cloud): Download from Azure Blob / S3
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

## Two Binaries

| Binary | Source | Role |
|--------|--------|------|
| `coordinator` | `cmd/coordinator/main.go` | Central registry. Tracks which peers are alive and where files live. Runs on port 8080. |
| `client` | `cmd/client/main.go` | Runs on every storage node. Mounts FUSE, manages 3-tier cache, runs API on port 8081. Registers with coordinator via HTTP. |

## Key Packages

| Package | Files | Responsibility |
|---------|-------|----------------|
| `internal/cache/` | `cache.go` | CacheManager: orchestrates reads/writes across tiers, chunking, eviction, write-through |
| | `nvme_storage.go` | Tier 1: local filesystem I/O |
| | `peer_storage.go` | Tier 2: HTTP client to other peers, 3x replication, crypto/rand shuffle |
| | `cloud_storage.go` | Tier 3 (S3): AWS SDK, configurable bucket/region |
| | `azure_storage.go` | Tier 3 (Azure): Azure Blob SDK, configurable account/container |
| `internal/coordinator/` | `coordinator.go` | Server-side: in-memory peer registry, file location map, state persistence |
| | `client.go` | `Coordinator` interface + `CoordinatorClient` (HTTP). Used by client binary to talk to remote coordinator |
| `internal/api/` | `handler.go` | Client HTTP API: file CRUD, peer ops, cache stats, health. Auth middleware, upload limits, path validation |
| `internal/fuse/` | `filesystem.go` | FUSE filesystem: Dir/File nodes backed by CacheManager |

## Build & Run

```bash
make build                    # builds bin/coordinator and bin/client
make run-coordinator          # coordinator on :8080
make run-client-1             # client on :8081, mount at /tmp/fuse-client1
make dev-setup && make dev-start  # full 3-node dev environment
make dev-stop                 # kill all processes
```

## Testing

```bash
make test          # go test ./...     (26 tests across 3 packages)
make test-verbose  # go test -v ./...
make test-race     # go test -race ./...
```

Test files: `cache_test.go`, `nvme_storage_test.go`, `coordinator_test.go`, `handler_test.go`

## AKS Deployment

```bash
az acr build --registry stargzrepo --image fuse-client:latest .
kubectl apply -f k8s/
```

Manifests in `k8s/`:
- `namespace.yaml` — `fuse-system`
- `coordinator-deployment.yaml` — single replica, PVC for state, ClusterIP service
- `client-daemonset.yaml` — privileged, `/dev/fuse`, NVMe hostPath, bidirectional FUSE mount propagation
- `configmap.yaml` — coordinator DNS, paths, chunk size
- `secrets.yaml` — AWS credentials + Azure storage credentials

Currently deployed on `stargz-test` cluster with Azure Blob Storage as Tier 3.

## Client CLI Flags

```
-mount          /tmp/fuse-client    FUSE mount point
-nvme           /tmp/nvme-cache     NVMe cache directory
-coordinator    localhost:8080      Coordinator address
-port           8081                API server port
-peer-id        (auto)              Peer ID
-chunk-size     4                   Chunk size in MB
-api-key        ""                  API key (optional)
-cloud-provider s3                  "s3" or "azure"
-s3-bucket      fuse-client-cache   S3 bucket name
-s3-region      us-east-1           S3 region
-azure-account  ""                  Azure storage account
-azure-key      ""                  Azure storage key
-azure-container fuse-cache         Azure container name
```

Environment variables: `POD_IP` (Kubernetes downward API), `AZURE_STORAGE_ACCOUNT`, `AZURE_STORAGE_KEY`, `AZURE_CONTAINER_NAME`, `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_REGION`

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
- `GET /api/health` — health check (no auth required)

All non-health endpoints require `X-API-Key` header when `-api-key` is set.

## Code Conventions

- Go module: `fuse-client`, Go 1.20
- Logger prefixes: `[COORDINATOR]`, `[CLIENT]`, `[API]`, `[FUSE]`, `[CACHE]`
- All tier backends implement the `TierStorage` interface (Read/Write/Delete/Exists/Size)
- Coordinator operations go through the `Coordinator` interface — server uses `CoordinatorService`, clients use `CoordinatorClient` (HTTP)
- `CacheManager` is an interface; `DefaultCacheManager` is the implementation
- Graceful shutdown via SIGINT/SIGTERM with context cancellation
- Peer registration retries with exponential backoff (5 attempts)
- Peer selection uses `crypto/rand` for shuffle (not `math/rand`)

## Dependencies

- `bazil.org/fuse` — FUSE implementation
- `github.com/aws/aws-sdk-go` — S3 client
- `github.com/Azure/azure-sdk-for-go/sdk/storage/azblob` v1.0.0 — Azure Blob client
- `github.com/gorilla/mux` — HTTP router

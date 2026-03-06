# FUSE Client with 3-Tier Cache

A distributed file system using FUSE (Filesystem in Userspace) with a 3-tier cache architecture, implemented in Go.

## Architecture

The system consists of:

1. **Master Coordinator**: Manages peer registration and metadata
2. **Client Nodes**: Each exposes a FUSE filesystem and serves as a seed client
3. **3-Tier Cache System**:
   - **Tier 1**: NVME local storage (fastest)
   - **Tier 2**: Other seed peers (distributed cache)
   - **Tier 3**: Cloud storage (AWS S3 / Azure Blob / GCP Cloud Storage)

## Recent Features

- **Helm + AKS-ready mount model**:
  - Privileged client DaemonSet with `hostPID: true`
  - Dedicated host-visible mount path (`/host/mnt/fuse` in container -> `/mnt/fuse` on node)
  - Init container and preStop hooks to reduce stale FUSE mount failures
- **Prometheus metrics endpoint**:
  - Client now exposes `GET /metrics` (Prometheus text format)
  - Includes cache hits/misses, write bytes/count, eviction counters, NVMe capacity/usage, coordinator availability
- **Large file stability improvements**:
  - Write path uses buffered growth to avoid O(n^2) realloc/copy behavior
  - Chunked uploads avoid unbounded goroutine fanout
  - Chunked reads use range-based reads with chunk reuse to avoid full-file in-memory reconstruction
- **Ops runbook scripts** in `scripts/ops/` for deploy, node mount repair, and read benchmarking
  - `test-smart-read.sh` supports cross-pod read/write benchmarks for arbitrary sizes (e.g. 100MB/1GB/5GB)
  - `test-smart-read-5gb.sh` remains as a compatibility wrapper
  - `test-gofuse-cached-read-suite.sh` runs 1GB + 5GB cold vs cached read throughput tests under go-fuse
  - `test-smart-read-s3-profile.sh` labels `standard` vs `s3express` runs and checks zone alignment for S3 Express endpoints

## Components

### Master Coordinator
- Manages peer registration and discovery
- Tracks file locations across the distributed system
- Provides REST API for peer communication
- Handles peer heartbeats and status updates

### Client Nodes
- Exposes FUSE filesystem to users
- Implements 3-tier cache hierarchy
- Serves as seed peers for other nodes
- Provides HTTP API for peer-to-peer communication

### Cache Tiers

#### Tier 1: NVME Storage
- Local NVME/SSD storage for fastest access
- Configurable cache size and location
- Automatic promotion of frequently accessed files

#### Tier 2: Peer Storage
- Distributed cache across other seed peers
- Peer discovery through master coordinator
- HTTP-based file transfer between peers

#### Tier 3: Cloud Storage
- Pluggable backend for persistent storage (AWS S3, Azure Blob, or GCP Cloud Storage)
- Configurable provider credentials, timeouts, and bucket/container settings
- Fallback for files not found locally or on peers

## Project Structure

```
fuse-client/
├── cmd/
│   ├── coordinator/        # Master coordinator application
│   └── client/            # FUSE client application
├── charts/
│   └── fuse-cache/        # Helm chart for coordinator/client deployment
├── internal/
│   ├── api/               # HTTP API handlers
│   ├── cache/             # Cache management and tier implementations
│   ├── coordinator/       # Coordinator service
│   ├── fuse/             # FUSE filesystem implementation
│   └── proto/            # Protocol buffer definitions
├── scripts/
│   ├── devbox/            # kind-based local cluster tooling
│   └── ops/               # AKS/Helm operational scripts
├── go.mod
├── go.sum
└── README.md
```

## Building

```bash
# Build coordinator
go build -o bin/coordinator cmd/coordinator/main.go

# Build client
go build -o bin/client cmd/client/main.go
```

## Usage

### Starting the Coordinator

```bash
./bin/coordinator -port 8080
```

Options:
- `-port`: HTTP server port (default: 8080)
- `-help`: Show help

### Starting a Client

```bash
./bin/client -mount /tmp/fuse-client -nvme /tmp/nvme-cache -coordinator localhost:8080 -port 8081
```

Options:
- `-mount`: Mount point for FUSE filesystem (default: /tmp/fuse-client)
- `-nvme`: Path for NVME cache storage (default: /tmp/nvme-cache)
- `-coordinator`: Coordinator address (default: localhost:8080)
- `-port`: Port for peer API server (default: 8081)
- `-peer-id`: Peer ID (auto-generated if not provided)
- `-cloud-provider`: Cloud storage provider: s3, azure, or gcp (default: s3)
- `-s3-bucket`: S3 bucket name (default: fuse-client-cache)
- `-s3-region`: S3 region (default: us-east-1)
- `-azure-account`: Azure storage account name
- `-azure-key`: Azure storage account key
- `-azure-container`: Azure blob container name (default: fuse-cache)
- `-gcp-bucket`: GCP Cloud Storage bucket name (default: fuse-client-cache)
- `-help`: Show help

### Running Multiple Clients

```bash
# Client 1
./bin/client -mount /tmp/fuse-client1 -nvme /tmp/nvme-cache1 -port 8081

# Client 2
./bin/client -mount /tmp/fuse-client2 -nvme /tmp/nvme-cache2 -port 8082

# Client 3
./bin/client -mount /tmp/fuse-client3 -nvme /tmp/nvme-cache3 -port 8083
```

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

## Cache Behavior

1. **File Read**: 
   - First checks Tier 1 (NVME)
   - If not found, always tries Tier 2 (Peers) before Tier 3 (Cloud)
   - For large files, hybrid read mode can be enabled when:
     - multiple peers have the file metadata
     - cloud copy is available
     - file size `>` `(peer replicas * assumed per-peer MB/s)`
   - In hybrid mode, peer remains primary and cloud is added in parallel to accelerate when needed
   - Promotes files to higher tiers on access
   - Chunked objects are served through range reads (no full-file reassembly required)

2. **File Write**:
   - Tries to store in Tier 1 first
   - Falls back to Tier 2 if Tier 1 is full
   - Falls back to Tier 3 if Tier 2 is unavailable
   - Buffered write growth reduces large-file write amplification
   - Flush/Fsync persists accumulated buffered writes

3. **Cache Management**:
   - LRU eviction within each tier
   - Automatic promotion of frequently accessed files
   - Background cleanup of inactive entries
   - Chunked persistence uses bounded memory behavior

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

- `bazil.org/fuse` - FUSE implementation
- `github.com/aws/aws-sdk-go` - AWS S3 client
- `github.com/Azure/azure-sdk-for-go/sdk/storage/azblob` - Azure Blob client
- GCP support uses the AWS S3 SDK against `storage.googleapis.com` (S3 interoperability mode)
- `github.com/gorilla/mux` - HTTP router
- `github.com/sirupsen/logrus` - Logging
- `google.golang.org/grpc` - gRPC (for future use)
- `helm.sh/helm/v3` - Kubernetes package manager for deployment templates

## Security

- File access is controlled through FUSE permissions
- Peer-to-peer communication uses HTTP (HTTPS recommended for production)
- Cloud credentials follow provider best practices (AWS IAM, Azure access keys/identity, GCP HMAC interoperability keys)

## Monitoring

- Health checks on all endpoints
- Peer heartbeat monitoring
- Cache statistics and metrics
- Coordinator provides system-wide statistics
- Prometheus scrape endpoint on clients: `GET /metrics`
- Read-path throughput breakdown metrics:
  - `fuse_cache_peer_read_bytes_total`, `fuse_cache_peer_read_seconds_total`, `fuse_cache_peer_read_mbps`
  - `fuse_cache_cloud_read_bytes_total`, `fuse_cache_cloud_read_seconds_total`, `fuse_cache_cloud_read_mbps`
  - `fuse_cache_nvme_read_bytes_total`, `fuse_cache_nvme_read_seconds_total`, `fuse_cache_nvme_read_mbps`

## Benchmark Matrix

Benchmark fields captured:
- Cloud test type
- Machine types
- Results (write/read)
- Peer speed, cloud speed, object speed
- Net start at test start (writer/reader)
- CPU start at test start (writer/reader/coordinator)
- Git SHA used for run

Notes:
- `Peer speed` / `Cloud speed` are computed from per-tier metric deltas on the reader pod (`/api/cache/stats`).
- `Object speed` is end-to-end read throughput from benchmark script output (`READ_MBPS_APPROX`).
- `Net start` comes from coordinator peer telemetry (`/api/peers` -> `network_speed_mbps`).
- `CPU start` comes from `kubectl top pod` snapshot taken just before each run.
- Historical rows keep `N/A` where older runs did not capture that field.

### Latest E2E (2026-02-28, EKS `fuse-system-awstest`)

| Date | Cloud Test Type | Machine Types | Scenario | Results (Write/Read MB/s) | Peer Speed MB/s | Cloud Speed MB/s | Object Speed MB/s | Net Start MB/s (W/R) | CPU Start (W/R/C) | Git SHA |
|---|---|---|---|---:|---:|---:|---:|---:|---|---|
| 2026-02-28 | `peer-first(default)` | `i3en.6xlarge` writer + `i3en.6xlarge` reader | `test-smart-read.sh` 1GB | `681 / 1211` | `335.5` | `0.0` | `1211` | `1012.6 / 996.0` | `44m / 2m / 1m` | `87bcb18` |
| 2026-02-28 | `peer-first(default)` | `i3en.6xlarge` writer + `i3en.6xlarge` reader | `test-smart-read.sh` 5GB | `196 / 1225` | `351.1` | `0.0` | `1225` | `1004.1 / 921.6` | `87m / 1m / 1m` | `87bcb18` |
| 2026-02-28 | `s3-standard` | `i3en.6xlarge` writer + `i3en.6xlarge` reader | `test-smart-read-s3-profile.sh` 1GB | `664 / 1203` | `336.6` | `0.0` | `1203` | `848.7 / 809.8` | `172m / 1m / 1m` | `87bcb18` |
| 2026-02-28 | `s3express` | `i3en.6xlarge` writer + `i3en.6xlarge` reader | `test-smart-read-s3-profile.sh` 1GB | `670 / 1221` | `340.2` | `0.0` | `1221` | `816.4 / 787.4` | `166m / 1m / 1m` | `87bcb18` |

### Latest E2E (2026-02-28, AKS `fuse-system-aztest`)

| Date | Cloud Test Type | Machine Types | Scenario | Results (Write/Read MB/s) | Peer Speed MB/s | Cloud Speed MB/s | Object Speed MB/s | Net Start MB/s (W/R) | CPU Start (W/R/C) | Git SHA |
|---|---|---|---|---:|---:|---:|---:|---:|---|---|
| 2026-02-28 | `azure-blob(peer-first)` | `Standard_NC24ads_A100_v4` writer + `Standard_L64s_v3` reader | `test-smart-read.sh` 1GB | `277 / 473` | `16.2` | `0.0` | `473` | `0.0 / 0.0` | `1m / 1m / 1m` | `87bcb18` |
| 2026-02-28 | `azure-blob(peer-first)` | `Standard_NC24ads_A100_v4` writer + `Standard_L64s_v3` reader | `test-smart-read.sh` 5GB | `267 / 561` | `21.6` | `0.0` | `561` | `0.0 / 0.0` | `98m / 1m / 1m` | `87bcb18` |
| 2026-02-28 | `azure-blob(peer-first)` | `Standard_NC24ads_A100_v4` writer + `Standard_NC24ads_A100_v4` reader | `test-smart-read.sh` 1GB | `275 / 518` | `17.6` | `0.0` | `518` | `0.0 / 0.0` | `633m / 1m / 1m` | `87bcb18` |
| 2026-03-01 | `azure-blob(peer-first, nvme-max=48GB)` | `Standard_NC24ads_A100_v4` writer + `Standard_NC24ads_A100_v4` reader | `test-smart-read.sh` 1GB | `264 / 545` | `N/A` | `N/A` | `545` | `N/A` | `N/A` | `5fa1f9c+` |
| 2026-03-01 | `azure-blob(peer-first, nvme-max=48GB)` | `Standard_NC24ads_A100_v4` writer + `Standard_NC24ads_A100_v4` reader | `test-smart-read.sh` 5GB | `270 / 648` | `N/A` | `N/A` | `648` | `N/A` | `N/A` | `5fa1f9c+` |
| 2026-03-02 | `azure-blob(hybrid-largefile, hedge=5ms)` | `Standard_NC24ads_A100_v4` writer + `Standard_NC24ads_A100_v4` reader | `test-smart-read.sh` 1GB | `298 / 550` | `N/A` | `N/A` | `550` | `N/A` | `N/A` | `hybridwall2-20260301-213635` |
| 2026-03-02 | `azure-blob(hybrid-largefile, hedge=5ms)` | `Standard_NC24ads_A100_v4` writer + `Standard_NC24ads_A100_v4` reader | `test-smart-read.sh` 5GB | `288 / 671` | `N/A` | `N/A` | `671` | `N/A` | `N/A` | `hybridwall2-20260301-213635` |
| 2026-03-02 | `azure-blob(hybrid-largefile, hedge=5ms)` | `Standard_NC24ads_A100_v4` writer + `Standard_NC24ads_A100_v4` reader | `test-smart-read.sh` 5GB (profiled) | `276 / 551` | `150.2` | `80.1` | `551` | `N/A` | `N/A` | `hybridwall2-20260301-213635` |

### Latest Cached Read (2026-03-01, AKS `fuse-system-aztest`)

| Date | Machine Types | Scenario | Write MB/s | Cold Read MB/s | Cached Read MB/s | Notes | Git SHA |
|---|---|---|---:|---:|---:|---|---|
| 2026-03-01 | `Standard_NC24ads_A100_v4` writer + `Standard_NC24ads_A100_v4` reader | `test-gofuse-cached-read-suite.sh` 1GB | `282` | `436` | `2003` | `gofuse + passthrough; cached path served via range/kernel cache` | `5fa1f9c+` |
| 2026-03-01 | `Standard_NC24ads_A100_v4` writer + `Standard_NC24ads_A100_v4` reader | `test-gofuse-cached-read-suite.sh` 5GB | `262` | `622` | `2389` | `gofuse + passthrough; cached path served via range/kernel cache` | `5fa1f9c+` |
| 2026-03-02 | `Standard_NC24ads_A100_v4` writer + `Standard_NC24ads_A100_v4` reader | `test-gofuse-cached-read-suite.sh` 1GB | `274` | `751` | `2133` | `hybrid-largefile + wall metrics image` | `hybridwall2-20260301-213635` |
| 2026-03-02 | `Standard_NC24ads_A100_v4` writer + `Standard_NC24ads_A100_v4` reader | `test-gofuse-cached-read-suite.sh` 5GB | `291` | `592` | `2530` | `hybrid-largefile + wall metrics image` | `hybridwall2-20260301-213635` |

### Historical Scenarios

| Date | Cloud Test Type | Machine Types | Scenario | Results (Write/Read MB/s) | Peer Speed MB/s | Cloud Speed MB/s | Object Speed MB/s | Net Start MB/s (W/R) | CPU Start (W/R/C) | Git SHA |
|---|---|---|---|---:|---:|---:|---:|---:|---|---|
| 2026-02-24 | `peer-first(default)` | `i3en.6xlarge` writer + `i3en.6xlarge` reader | Baseline before hotspot iteration, 1GB | `699 / 788` | `N/A` | `0.0` | `788` | `N/A` | `N/A` | `25f8711` |
| 2026-02-24 | `peer-first(default)` | `i3en.6xlarge` writer + `i3en.6xlarge` reader | Baseline before hotspot iteration, 5GB | `339 / 887` | `N/A` | `0.0` | `887` | `N/A` | `N/A` | `25f8711` |
| 2026-02-24 | `peer-first(default)` | `i3en.6xlarge` writer + `i3en.6xlarge` reader | Hotspot Iteration A (regression), 1GB | `703 / 634-646` | `N/A` | `0.0` | `634-646` | `N/A` | `N/A` | `WIP pre-5ddcd09` |
| 2026-02-24 | `peer-first(default)` | `i3en.6xlarge` writer + `i3en.6xlarge` reader | Hotspot Iteration A (regression), 5GB | `203 / 838` | `N/A` | `0.0` | `838` | `N/A` | `N/A` | `WIP pre-5ddcd09` |
| 2026-02-24 | `peer-first(default)` | `i3en.6xlarge` writer + `i3en.6xlarge` reader | Hotspot Iteration B (accepted), 1GB | `697 / 1201-1244` | `N/A` | `0.0` | `1201-1244` | `N/A` | `N/A` | `5ddcd09` |
| 2026-02-24 | `peer-first(default)` | `i3en.6xlarge` writer + `i3en.6xlarge` reader | Hotspot Iteration B (accepted), 5GB | `206-219 / 1368-1395` | `N/A` | `0.0` | `1368-1395` | `N/A` | `N/A` | `5ddcd09` |
| 2026-02-21 | `azure hybrid` | `2x A100 + 1x L64s_v3` | Azure cloud optimization, 5GB run #2 | `N/A / 420` | `145.8` | `274.5` | `420` | `N/A` | `N/A` | `rev 71 image fuse-cloudopt-20260221100456` |
| 2026-02-21 | `gofuse cached-read` | `A100 writer + A100 reader` | go-fuse cold/cached suite, 1GB | `274 / cold 547, cached 2316` | `N/A` | `N/A` | `547 (cold)` | `N/A` | `N/A` | `rev 74 image gofuse-benchfix2-20260221-110935` |
| 2026-02-21 | `gofuse cached-read` | `A100 writer + A100 reader` | go-fuse cold/cached suite, 5GB | `271 / cold 524, cached 2518` | `N/A` | `N/A` | `524 (cold)` | `N/A` | `N/A` | `rev 74 image gofuse-benchfix2-20260221-110935` |

## Future Enhancements

- gRPC for faster peer communication
- Encryption for data in transit and at rest
- More sophisticated cache eviction policies
- Web UI for monitoring and management

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make changes
4. Add tests
5. Submit a pull request

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

Install required local tools (helm/kind/kubectl) if needed:

```bash
./scripts/devbox/install-tools.sh all
```

Use the helper script:

```bash
./scripts/devbox/devbox.sh create   # create local kind cluster
./scripts/devbox/devbox.sh deploy   # install Helm chart
./scripts/devbox/devbox.sh status   # inspect resources
./scripts/devbox/devbox.sh delete   # tear down cluster
```

Or use Make targets:

```bash
make k8s-devbox-install-tools
make k8s-devbox-create
make k8s-devbox-deploy
make k8s-devbox-status
make k8s-devbox-delete
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

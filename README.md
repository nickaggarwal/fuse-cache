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
- **Ops runbook scripts** in `scripts/ops/` for deploy, node mount repair, and 1GB benchmarking
  - `test-smart-read.sh` supports cross-pod read/write benchmarks for arbitrary sizes (e.g. 100MB/1GB/5GB)
  - `test-smart-read-5gb.sh` remains as a compatibility wrapper

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
export FUSE_PEER_READ_MBPS=200     # assumed MB/s per peer
export FUSE_PEER_TIMEOUT=30s
export FUSE_CLOUD_TIMEOUT=60s
export FUSE_IO_PROGRESS_MB=512   # set 0 to disable read/write progress logs
```

`FUSE_PEER_SIZE` controls the remote read strategy threshold (in bytes).
`FUSE_PEER_READ_MBPS` controls the hybrid-read throughput model.
`FUSE_IO_PROGRESS_MB` controls periodic FUSE read/write progress logging cadence.

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

## License

This project is licensed under the MIT License. 

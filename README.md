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
├── internal/
│   ├── api/               # HTTP API handlers
│   ├── cache/             # Cache management and tier implementations
│   ├── coordinator/       # Coordinator service
│   ├── fuse/             # FUSE filesystem implementation
│   └── proto/            # Protocol buffer definitions
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

## Cache Behavior

1. **File Read**: 
   - First checks Tier 1 (NVME)
   - If not found, checks Tier 2 (Peers)
   - If not found, checks Tier 3 (Cloud)
   - Promotes files to higher tiers on access

2. **File Write**:
   - Tries to store in Tier 1 first
   - Falls back to Tier 2 if Tier 1 is full
   - Falls back to Tier 3 if Tier 2 is unavailable

3. **Cache Management**:
   - LRU eviction within each tier
   - Automatic promotion of frequently accessed files
   - Background cleanup of inactive entries

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
export FUSE_PEER_TIMEOUT=30s
export FUSE_CLOUD_TIMEOUT=60s
```

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

## Security

- File access is controlled through FUSE permissions
- Peer-to-peer communication uses HTTP (HTTPS recommended for production)
- Cloud credentials follow provider best practices (AWS IAM, Azure access keys/identity, GCP HMAC interoperability keys)

## Monitoring

- Health checks on all endpoints
- Peer heartbeat monitoring
- Cache statistics and metrics
- Coordinator provides system-wide statistics

## Future Enhancements

- gRPC for faster peer communication
- Encryption for data in transit and at rest
- More sophisticated cache eviction policies
- Web UI for monitoring and management
- Metrics integration (Prometheus/Grafana)

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make changes
4. Add tests
5. Submit a pull request

## License

This project is licensed under the MIT License. 
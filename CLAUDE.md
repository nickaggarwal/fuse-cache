# CLAUDE.md - Project Guide

## Project Overview

Distributed file system using FUSE (Filesystem in Userspace) with a 3-tier cache architecture, written in Go 1.20. Clients mount a virtual filesystem backed by a tiered caching layer; a central coordinator handles peer discovery and file location metadata.

## Architecture

**Two binaries:**
- `cmd/coordinator/main.go` — Master coordinator: peer registration, file location tracking, REST API (stdlib `net/http`)
- `cmd/client/main.go` — Client node: mounts FUSE filesystem, runs HTTP API server (gorilla/mux), manages 3-tier cache

**3-Tier Cache (internal/cache/):**
1. **Tier 1 - NVMe** (`nvme_storage.go`): Local disk storage under a configurable path. Fastest tier.
2. **Tier 2 - Peer** (`peer_storage.go`): Distributed cache across other clients. Discovered via coordinator REST API. Replication factor of 3. Uses HTTP for file transfer.
3. **Tier 3 - Cloud** (`cloud_storage.go`): AWS S3 backend (`fuse-client-cache` bucket, `us-east-1`). Slowest tier, persistent.

**Cache behavior:**
- Reads: NVMe -> Peer -> Cloud. Files fetched from lower tiers are promoted to NVMe in the background.
- Writes: Try NVMe first. If NVMe fails, race Peer and Cloud writes in parallel — return on first success. Large files (> chunk size, default 4MB) are split into chunks and written in parallel.
- All cache state is in-memory (`DefaultCacheManager.entries` map, protected by `sync.RWMutex`).

**Key packages:**
- `internal/fuse/` — FUSE filesystem using `bazil.org/fuse`. Dir/File nodes back reads/writes with CacheManager.
- `internal/api/` — Client HTTP API (gorilla/mux). File CRUD, peer info, cache stats, health check.
- `internal/coordinator/` — CoordinatorService: in-memory peer registry + file location map. Periodic cleanup of inactive peers (60s timeout). State persistence to `coordinator_state.json`.

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
make test          # go test ./...
make test-verbose  # go test -v ./...
make test-race     # go test -race ./...
```

## Key Dependencies

- `bazil.org/fuse` — FUSE implementation
- `github.com/aws/aws-sdk-go` — S3 client (Tier 3)
- `github.com/gorilla/mux` — HTTP router (client API)

## Code Conventions

- Go module: `fuse-client`
- Logger prefixes: `[COORDINATOR]`, `[CLIENT]`, `[API]`, `[FUSE]`
- All tier storage backends implement the `TierStorage` interface (Read/Write/Delete/Exists/Size)
- CacheManager is an interface; `DefaultCacheManager` is the concrete implementation
- Coordinator state is periodically saved to `coordinator_state.json` (every 60s + on shutdown)
- HTTP APIs use JSON for request/response bodies
- Graceful shutdown via SIGINT/SIGTERM with context cancellation

## Environment Variables

```bash
AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION  # for S3 (Tier 3)
```

## API Endpoints

**Coordinator (`:8080`):** `/api/peers/register` (POST), `/api/peers` (GET), `/api/peers/status` (PUT), `/api/files/location` (GET/PUT), `/api/stats` (GET), `/api/health` (GET)

**Client (`:8081+`):** `/api/files/{path}` (GET/PUT/DELETE/HEAD), `/api/files/{path}/size` (GET), `/api/peers` (GET), `/api/peers/{peerID}` (GET), `/api/peers/{peerID}/heartbeat` (POST), `/api/cache` (GET), `/api/cache/stats` (GET), `/api/health` (GET)

# CSI Driver Implementation Plan for fuse-client

## Executive Summary

Add a Kubernetes CSI (Container Storage Interface) node plugin that lets pods consume the fuse-client cache via standard `volume` declarations. The CSI plugin is a thin orchestration layer — it talks to the existing client DaemonSet over a local Unix socket to prepare bind-mount paths, manage sessions, and enforce cache policies. No cache logic lives inside the CSI binary.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│  Pod                                                        │
│   └── volumeMount: /data  ──────────────────────┐           │
│                                                  │           │
├──────────────────────────────────────────────────┤           │
│  kubelet                                         │           │
│   └── calls CSI NodePublishVolume ───────────┐   │           │
│                                              │   │           │
├──────────────────────────────────────────────┤   │           │
│  CSI Node Plugin (DaemonSet)                 │   │           │
│   └── gRPC server on /csi/csi.sock           │   │           │
│   └── calls fuse-client agent over           │   │           │
│       /var/run/fuse-client/agent.sock        │   │           │
│                                              ▼   ▼           │
├──────────────────────────────────────────────────────────────┤
│  fuse-client DaemonSet (existing)                            │
│   └── new: session manager gRPC server on agent.sock         │
│   └── existing: FUSE mount at /host/mnt/fuse                │
│   └── existing: NVMe cache, peer, cloud tiers                │
│                                                              │
│   On SessionCreate:                                          │
│     1. Validate rootPath exists or is fetchable              │
│     2. Create session entry (refcount, pin, policy)          │
│     3. Return hostPath = /host/mnt/fuse/<rootPath>           │
│                                                              │
│   CSI node plugin then bind-mounts hostPath → targetPath     │
└──────────────────────────────────────────────────────────────┘
```

### Key Design Decisions

1. **CSI plugin does NOT embed cache logic.** It delegates to the existing client daemon.
2. **Communication via Unix socket gRPC**, not HTTP — avoids auth/network complexity.
3. **Bind-mount strategy**: CSI plugin bind-mounts a subdirectory of the existing FUSE mount into the pod. No new FUSE mounts per volume.
4. **Session abstraction**: The client daemon tracks active mount sessions with refcounts, pinning, and per-session cache policy.

---

## New Components

### 1. Third Binary: `csi-driver`

| Property | Value |
|----------|-------|
| Source | `cmd/csi-driver/main.go` |
| Build output | `bin/csi-driver` |
| CSI driver name | `fuse.csi.storage.io` |
| Node ID | `NODE_NAME` env var (Kubernetes downward API) |
| Socket | `/csi/csi.sock` (hostPath shared with kubelet) |

Implements only:
- **Identity service**: `GetPluginInfo`, `GetPluginCapabilities`, `Probe`
- **Node service**: `NodePublishVolume`, `NodeUnpublishVolume`, `NodeGetCapabilities`, `NodeGetInfo`

No controller service in Phase 1.

### 2. New Package: `internal/session`

Session/volume manager embedded in the client daemon.

```go
// internal/session/manager.go

type CachePolicy struct {
    CacheMode    string // "readonly", "writeback", "writethrough"
    Warmup       string // "none", "metadata", "full"
    Pinned       bool   // prevent eviction while session active
    SourcePolicy string // "peer-first", "cloud-first", "hybrid"
}

type Session struct {
    VolumeID    string
    RootPath    string      // subtree root within FUSE mount
    ReadOnly    bool
    Policy      CachePolicy
    RefCount    int32
    CreatedAt   time.Time
    HostPath    string      // resolved host path for bind mount
}

type Manager interface {
    Create(ctx context.Context, volumeID, rootPath string, readOnly bool, policy CachePolicy) (*Session, error)
    Delete(ctx context.Context, volumeID string) error
    Get(ctx context.Context, volumeID string) (*Session, error)
    List(ctx context.Context) ([]*Session, error)
    AddRef(ctx context.Context, volumeID string) error
    Release(ctx context.Context, volumeID string) error
}
```

### 3. New Package: `internal/agentserver`

gRPC server running inside the client daemon, listening on a Unix socket.

```go
// internal/agentserver/server.go

// Serves on /var/run/fuse-client/agent.sock
// Proto: proto/agent.proto
// RPCs:
//   CreateSession(CreateSessionRequest) → CreateSessionResponse
//   DeleteSession(DeleteSessionRequest) → DeleteSessionResponse
//   GetSession(GetSessionRequest) → GetSessionResponse
//   ListSessions(ListSessionsRequest) → ListSessionsResponse
//   WarmCache(WarmCacheRequest) → WarmCacheResponse  // Phase 2
```

### 4. New Proto: `proto/agent.proto`

```protobuf
syntax = "proto3";
package fuse.agent;
option go_package = "fuse-client/internal/pb";

message CachePolicy {
  string cache_mode = 1;     // readonly | writeback | writethrough
  string warmup = 2;         // none | metadata | full
  bool pinned = 3;
  string source_policy = 4;  // peer-first | cloud-first | hybrid
}

message CreateSessionRequest {
  string volume_id = 1;
  string root_path = 2;
  bool read_only = 3;
  CachePolicy policy = 4;
}

message CreateSessionResponse {
  string host_path = 1;
  string volume_id = 2;
}

message DeleteSessionRequest {
  string volume_id = 1;
}

message DeleteSessionResponse {}

message GetSessionRequest {
  string volume_id = 1;
}

message GetSessionResponse {
  string volume_id = 1;
  string root_path = 2;
  string host_path = 3;
  bool read_only = 4;
  CachePolicy policy = 5;
  int32 ref_count = 6;
}

message ListSessionsRequest {}

message ListSessionsResponse {
  repeated GetSessionResponse sessions = 1;
}

message WarmCacheRequest {
  string root_path = 1;
  string mode = 2;        // metadata | lazy | full
  int64 max_bytes = 3;
  bool pin = 4;
}

message WarmCacheResponse {
  int64 files_warmed = 1;
  int64 bytes_warmed = 2;
}
```

---

## Kubernetes Objects

### CSI Node Plugin DaemonSet

```
k8s/csi-driver-daemonset.yaml
```

```yaml
apiVersion: apps/v1
kind: DaemonSet
metadata:
  name: fuse-csi-node
  namespace: fuse-system
spec:
  selector:
    matchLabels:
      app: fuse-csi-node
  template:
    metadata:
      labels:
        app: fuse-csi-node
    spec:
      nodeSelector:
        agentpool: userpool
      serviceAccountName: fuse-csi-node
      containers:
        # CSI driver
        - name: csi-driver
          image: stargzrepo.azurecr.io/fuse-client:latest
          command: ["csi-driver"]
          args:
            - "--endpoint=unix:///csi/csi.sock"
            - "--node-id=$(NODE_NAME)"
            - "--agent-socket=/var/run/fuse-client/agent.sock"
            - "--fuse-root=/host/mnt/fuse"
          env:
            - name: NODE_NAME
              valueFrom:
                fieldRef:
                  fieldPath: spec.nodeName
          securityContext:
            privileged: true
          volumeMounts:
            - name: socket-dir
              mountPath: /csi
            - name: agent-socket
              mountPath: /var/run/fuse-client
            - name: mountpoint-dir
              mountPath: /var/lib/kubelet/pods
              mountPropagation: Bidirectional
            - name: fuse-mount
              mountPath: /host/mnt
              mountPropagation: HostToContainer

        # CSI node-driver-registrar sidecar
        - name: node-driver-registrar
          image: registry.k8s.io/sig-storage/csi-node-driver-registrar:v2.10.0
          args:
            - "--csi-address=/csi/csi.sock"
            - "--kubelet-registration-path=/var/lib/kubelet/plugins/fuse.csi.storage.io/csi.sock"
          volumeMounts:
            - name: socket-dir
              mountPath: /csi
            - name: registration-dir
              mountPath: /registration

      volumes:
        - name: socket-dir
          hostPath:
            path: /var/lib/kubelet/plugins/fuse.csi.storage.io
            type: DirectoryOrCreate
        - name: registration-dir
          hostPath:
            path: /var/lib/kubelet/plugins_registry
            type: Directory
        - name: mountpoint-dir
          hostPath:
            path: /var/lib/kubelet/pods
            type: Directory
        - name: agent-socket
          hostPath:
            path: /var/run/fuse-client
            type: DirectoryOrCreate
        - name: fuse-mount
          hostPath:
            path: /mnt
            type: Directory
```

### CSIDriver Object

```
k8s/csi-driver.yaml
```

```yaml
apiVersion: storage.k8s.io/v1
kind: CSIDriver
metadata:
  name: fuse.csi.storage.io
spec:
  attachRequired: false          # node-only, no controller attach
  podInfoOnMount: true           # pass pod name/namespace to NodePublish
  fsGroupPolicy: None            # FUSE handles permissions
  volumeLifecycleModes:
    - Ephemeral                  # Phase 1: inline volumes
    - Persistent                 # Phase 3: PV/PVC
```

### ServiceAccount + RBAC

```
k8s/csi-rbac.yaml
```

```yaml
apiVersion: v1
kind: ServiceAccount
metadata:
  name: fuse-csi-node
  namespace: fuse-system
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: fuse-csi-node
rules:
  - apiGroups: [""]
    resources: ["nodes"]
    verbs: ["get"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata:
  name: fuse-csi-node
subjects:
  - kind: ServiceAccount
    name: fuse-csi-node
    namespace: fuse-system
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: ClusterRole
  name: fuse-csi-node
```

---

## Changes to Existing Code

### 1. Client DaemonSet (`k8s/client-daemonset.yaml`)

Add agent socket volume and mount:

```yaml
# Add to volumes:
- name: agent-socket
  hostPath:
    path: /var/run/fuse-client
    type: DirectoryOrCreate

# Add to container volumeMounts:
- name: agent-socket
  mountPath: /var/run/fuse-client
```

### 2. Client Binary (`cmd/client/main.go`)

Add flags and start agent server:

```go
// New flags:
agentSocket = flag.String("agent-socket", "/var/run/fuse-client/agent.sock",
    "Unix socket path for CSI agent gRPC server")
enableAgentServer = flag.Bool("enable-agent-server", true,
    "Enable the agent gRPC server for CSI integration")

// After cache manager init and FUSE mount, before signal wait:
if *enableAgentServer {
    sessMgr := session.NewManager(cacheManager, *mountPoint)
    agentSrv := agentserver.New(sessMgr, *agentSocket)
    go func() {
        if err := agentSrv.Serve(ctx); err != nil {
            logger.Printf("Agent server error: %v", err)
        }
    }()
}
```

### 3. Cache Manager — Eviction Protection

Add to `internal/cache/cache.go`:

```go
// Add to CacheManager interface:
PinPath(ctx context.Context, prefix string) error
UnpinPath(ctx context.Context, prefix string) error

// In DefaultCacheManager eviction loop, skip entries whose
// FilePath has a prefix match against any active pin.
```

### 4. Dockerfile

Add CSI driver binary to the build:

```dockerfile
RUN ... && \
    CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH \
    go build -trimpath -ldflags="-s -w" -o /bin/csi-driver cmd/csi-driver/main.go

COPY --from=builder /bin/csi-driver /usr/local/bin/csi-driver
```

### 5. Makefile

```makefile
CSI_BINARY := $(BINARY_DIR)/csi-driver

build: $(COORDINATOR_BINARY) $(CLIENT_BINARY) $(CSI_BINARY)

$(CSI_BINARY): $(BINARY_DIR) $(GO_FILES)
	go build -o $(CSI_BINARY) cmd/csi-driver/main.go
```

### 6. go.mod — New Dependency

```
github.com/container-storage-interface/spec v1.9.0
```

This is the only new dependency. The CSI gRPC service definitions come from this module.

---

## Consumer Usage

### Phase 1: Ephemeral Inline Volume

```yaml
apiVersion: v1
kind: Pod
metadata:
  name: model-server
spec:
  containers:
    - name: app
      image: my-model-server:latest
      volumeMounts:
        - name: model-cache
          mountPath: /models
          readOnly: true
  volumes:
    - name: model-cache
      csi:
        driver: fuse.csi.storage.io
        readOnly: true
        volumeAttributes:
          rootPath: "/models/llama-70b"
          cacheMode: "readonly"
          warmup: "metadata"
          pinned: "true"
```

### Phase 3: PersistentVolume

```yaml
apiVersion: v1
kind: PersistentVolume
metadata:
  name: dataset-cache
spec:
  capacity:
    storage: 100Gi  # informational, not enforced
  accessModes:
    - ReadOnlyMany
  csi:
    driver: fuse.csi.storage.io
    volumeHandle: "datasets/imagenet"
    volumeAttributes:
      rootPath: "/datasets/imagenet"
      cacheMode: "readonly"
      warmup: "full"
      pinned: "true"
---
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: dataset-cache
spec:
  accessModes:
    - ReadOnlyMany
  resources:
    requests:
      storage: 100Gi
  volumeName: dataset-cache
```

---

## Phased Roadmap

### Phase 1: Ephemeral CSI MVP

**Goal**: Pods can declare an inline CSI volume and get a bind-mounted subtree of the fuse-client cache.

| Step | File(s) | What |
|------|---------|------|
| 1.1 | `proto/agent.proto` | Define agent gRPC service (CreateSession, DeleteSession, GetSession) |
| 1.2 | `make proto` | Generate `internal/pb/agent.pb.go`, `agent_grpc.pb.go` |
| 1.3 | `internal/session/manager.go` | Session manager: in-memory map, Create/Delete/Get/List, refcounting |
| 1.4 | `internal/agentserver/server.go` | gRPC server on Unix socket, delegates to session manager |
| 1.5 | `internal/agentserver/client.go` | gRPC client for CSI driver to call agent |
| 1.6 | `cmd/csi-driver/main.go` | CSI Identity + Node services; NodePublish calls agent→CreateSession, gets hostPath, bind-mounts; NodeUnpublish calls DeleteSession, unmounts |
| 1.7 | `cmd/client/main.go` | Add `--agent-socket` flag, start agent gRPC server alongside existing services |
| 1.8 | `Dockerfile` | Build and copy `csi-driver` binary |
| 1.9 | `Makefile` | Add `$(CSI_BINARY)` target |
| 1.10 | `k8s/csi-driver.yaml` | CSIDriver object |
| 1.11 | `k8s/csi-rbac.yaml` | ServiceAccount, ClusterRole, ClusterRoleBinding |
| 1.12 | `k8s/csi-driver-daemonset.yaml` | CSI node plugin DaemonSet + registrar sidecar |
| 1.13 | `k8s/client-daemonset.yaml` | Add agent-socket volume/mount |
| 1.14 | Tests | `internal/session/manager_test.go`, `cmd/csi-driver/csi_test.go` |

**Deliverable**: `kubectl apply` the manifests, create a pod with an inline CSI volume pointing at a rootPath, pod sees files from the fuse cache at its mountPath.

### Phase 2: Policy-Aware Mounts

**Goal**: Volume attributes control cache behavior per mount.

| Step | File(s) | What |
|------|---------|------|
| 2.1 | `internal/session/manager.go` | Pass CachePolicy from session into cache manager |
| 2.2 | `internal/cache/cache.go` | `PinPath` / `UnpinPath` — skip pinned prefixes during eviction |
| 2.3 | `internal/cache/cache.go` | Warmup API: `WarmPath(ctx, prefix, mode)` — prefetch metadata or full data for a subtree |
| 2.4 | `proto/agent.proto` | Add `WarmCache` RPC |
| 2.5 | `internal/agentserver/server.go` | Implement WarmCache handler |
| 2.6 | `internal/api/handler.go` | `POST /api/cache/warm` — HTTP wrapper for warmup (external tooling) |
| 2.7 | `cmd/csi-driver/main.go` | After NodePublish bind-mount, call WarmCache if warmup != "none" |
| 2.8 | Metrics | Per-session metrics: bytes served, tier breakdown, attach/publish latency |
| 2.9 | Tests | Eviction-with-pin tests, warmup tests |

**Deliverable**: Setting `warmup: "full"` and `pinned: "true"` in volumeAttributes causes the dataset to be pulled to NVMe on mount and protected from eviction while the pod is running.

### Phase 3: Persistent Volumes

**Goal**: Static PV/PVC support. Cluster admins pre-create PVs for known datasets.

| Step | File(s) | What |
|------|---------|------|
| 3.1 | `k8s/csi-driver.yaml` | Already declares `Persistent` lifecycle mode |
| 3.2 | `cmd/csi-driver/main.go` | Handle PV volumeHandle in NodePublish (map to rootPath) |
| 3.3 | `internal/session/manager.go` | Idempotent Create — same volumeID returns existing session, bumps refcount |
| 3.4 | K8s examples | PV + PVC + StorageClass (no provisioner, manual) |
| 3.5 | Tests | Multi-pod-same-PVC, refcount lifecycle |

**Deliverable**: Multiple pods can mount the same PVC and get the same cached subtree. Session cleanup happens when the last pod unmounts.

### Phase 4: Write-Enabled Mounts

**Goal**: Single-writer semantics through CSI.

| Step | File(s) | What |
|------|---------|------|
| 4.1 | `internal/session/manager.go` | Write-lock tracking: only one non-ReadOnly session per rootPath |
| 4.2 | `cmd/csi-driver/main.go` | NodePublish rejects ReadWriteMany if another writer exists |
| 4.3 | `internal/cache/cache.go` | Flush API: `FlushPath(ctx, prefix)` — force write-through to cloud for a subtree |
| 4.4 | `cmd/csi-driver/main.go` | NodeUnpublish calls FlushPath before releasing write session |
| 4.5 | Tests | Concurrent writer rejection, flush-on-unmount |

**Deliverable**: A pod can mount read-write, writes go through the normal NVMe→cloud path, and FlushPath is called on pod termination to ensure durability.

---

## File Tree (new files only)

```
cmd/csi-driver/
  main.go                     # CSI gRPC server: Identity + Node

internal/session/
  manager.go                  # Session lifecycle, refcounting, pin tracking
  manager_test.go

internal/agentserver/
  server.go                   # gRPC server on Unix socket (runs inside client)
  client.go                   # gRPC client (used by CSI driver)

proto/
  agent.proto                 # Agent service definition

internal/pb/
  agent.pb.go                 # (generated)
  agent_grpc.pb.go            # (generated)

k8s/
  csi-driver.yaml             # CSIDriver object
  csi-rbac.yaml               # ServiceAccount + RBAC
  csi-driver-daemonset.yaml   # CSI node plugin DaemonSet
```

## Modified files

```
cmd/client/main.go            # Add agent-socket flag, start agent server
internal/cache/cache.go        # PinPath/UnpinPath, WarmPath (Phase 2)
internal/api/handler.go        # POST /api/cache/warm (Phase 2)
Dockerfile                     # Build csi-driver binary
Makefile                       # Add csi-driver build target
k8s/client-daemonset.yaml     # Add agent-socket volume
go.mod                        # Add container-storage-interface/spec
```

---

## NodePublishVolume Flow (Detail)

```
kubelet calls NodePublishVolume(req):
  volumeID    = req.VolumeId                    // e.g. "csi-abc123"
  targetPath  = req.TargetPath                  // e.g. /var/lib/kubelet/pods/<uid>/volumes/...
  rootPath    = req.VolumeContext["rootPath"]    // e.g. "/models/llama-70b"
  cacheMode   = req.VolumeContext["cacheMode"]  // e.g. "readonly"
  warmup      = req.VolumeContext["warmup"]     // e.g. "metadata"
  pinned      = req.VolumeContext["pinned"]     // e.g. "true"
  readOnly    = req.Readonly

  1. Connect to agent socket (lazy, cached connection)
  2. Call agent.CreateSession(volumeID, rootPath, readOnly, policy)
     → agent validates rootPath
     → agent creates session entry
     → agent pins path if pinned=true
     → returns hostPath = "/host/mnt/fuse" + rootPath
  3. os.MkdirAll(targetPath)
  4. mount.New("").Mount(hostPath, targetPath, "", []string{"bind", "ro"})
  5. If warmup != "none":
       go agent.WarmCache(rootPath, warmup, ...)  // async, don't block publish
  6. Return success
```

## NodeUnpublishVolume Flow

```
kubelet calls NodeUnpublishVolume(req):
  targetPath = req.TargetPath

  1. mount.New("").Unmount(targetPath)
  2. os.Remove(targetPath)
  3. Call agent.DeleteSession(volumeID)
     → agent unpins path
     → agent decrements refcount
     → if refcount == 0, removes session
  4. Return success
```

---

## What We Explicitly Defer

| Feature | Why |
|---------|-----|
| Dynamic provisioning / StorageClass provisioner | Not needed — datasets are pre-existing in cloud storage |
| Controller service | No cross-node attach orchestration needed |
| RWX multi-writer | Requires distributed locking — hard, not worth it yet |
| Snapshot/clone via CSI | Use existing `/api/fs/snapshot` and `/api/fs/restore` instead |
| Volume expansion | Cache capacity is node-level, not per-volume |
| In-CSI cache implementation | Must stay in client daemon |

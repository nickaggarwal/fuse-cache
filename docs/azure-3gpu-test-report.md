# Azure 3-GPU-node validation — results, issues, improvements

Run date: 2026-08-05. Cluster `stargz-test` (AKS), namespace `fuse-system`.
Image `stargzrepo.azurecr.io/fuse-client:gpu3-e871d4c` (built from commit
`e871d4c` on `main`). Tier 3 = Azure Blob.

Topology: `gpuckpt` nodepool scaled 0 → 3 × Standard_NC24ads_A100_v4, plus the
2 pre-existing `ckpt2404` nodes ⇒ 5 client peers, 3 coordinator replicas, 3-node
etcd. Client DaemonSet carried three test-only flags:
`-replica-reconcile-interval-sec 30 -fetch-lease -fast-chunk-advertise`.

## What passed

| Area | Result |
|------|--------|
| Peer mesh / labels over gRPC | 5 peers registered, labels carried (`host`, `pool`, `tier`) |
| Chunked warm fan-out | `POST /api/warm` selected 5, all accepted; each node `files=3 warmed=3 failed=0 bytes=125829120`; NVMe 0→126 MB on all 3 GPU nodes; md5 matched source |
| Warm selectors | 40 % → 2 nodes, 60 % → 3, explicit `nodes`, `labels` host match → 1; 3 malformed selectors → HTTP 400 |
| Replica reconciler | `Reconciler replicated /gpuhot/hot.bin to 2 peer(s) (holders 1 -> 3, target 3)`; worldview `replicas=3`; HEAD 200 on all three |
| Heat-proportional replication | 3 distinct remote readers → `target 4` (base 3 + 3/2), `heat_boosts_total 3` on two nodes |
| Fetch leases (Phase 2) | Confirmed working — see below |
| Read throughput | warmed 0.296–0.299 s / 300 MB ≈ **1.05 GB/s**; cold 1.14 s ≈ 275 MB/s (**3.8×**); promoted second read 0.567 s ≈ 555 MB/s |
| Herd admission control | 6 concurrent readers × 200 MB across 3 nodes: all HTTP 200, `peer_serve_rejected_total` = 0 (gate had headroom at this scale) |

The chunked-warmup fix from `e871d4c` holds at 3-node scale — that was the main
thing this run needed to prove.

### Fetch leases: working, earlier reading was wrong

An earlier pass reported `fetch_lease_granted_total` stuck at 0 and I suspected
the `coordinator.FetchLeaser` type assertion in `internal/cache/origin_lease.go:91`
was failing under the default gRPC transport. That was wrong. `internal/coordinator/fetch_lease_client.go:12-16`
has compile-time assertions for all three implementations, and `GRPCCoordinatorClient`
satisfies it via the HTTP fallback.

The real reason the counter stayed 0: **the NVMe cache is a hostPath, so the new
pods rehydrated the old pods' data at startup.** Every "cold" read I issued was
served from local disk or a peer and never reached the cloud tier, which is the
only place the lease is taken.

Reading a file with no surviving local copy anywhere (`/bench-old-256m.bin`,
3 nodes concurrently) produced the designed behaviour on the first try:

```
18081  granted 32  denied  0  follower_peer_hits  0   cloud_hits 32
18082  granted  2  denied 10  follower_peer_hits 10   cloud_hits  2
18083  granted  0  denied 12  follower_peer_hits 12   cloud_hits  0
```

34 of 96 chunk-reads went to origin; the other 62 were suppressed and picked up
from the lease leader over the peer tier. Zero `follower_cloud_fallback`, zero
`lease_errors`. Cross-node single-flight is doing exactly what the design says.

---

## Issues

Fix status as of 2026-08-05 (all fixes are code + tests, none yet re-validated
on a live cluster):

| # | Issue | Status |
|---|-------|--------|
| 1 | `ReadRange` panic on chunk-size mismatch | fixed — stride persisted on `CacheEntry`/`FileLocation`, slices clamped; `chunk_stride_test.go` |
| 2 | Silent short reads / truncation | fixed — `WriteTo` errors on short assembly; same test file |
| 3 | Advertisement never fires on the `Get` path | fixed — `advertisePromoted` on `promoteToNVMe` + `fetchAndLandChunkOrdered` |
| 4 | Warm/chunk-completion traffic invisible in tier counters | fixed — hit/miss/tier-read recorded in `fetchAndLandChunkOrdered` |
| 5 | Herd-control features silent when off | fixed — effective state logged at client startup |
| 6 | Percentage selection always picks the same nodes | fixed — FNV-1a-of-prefix rotation in `SelectWarmTargetsFor` |
| 7 | Dead peers selectable for up to 90 s | fixed — 75 s heartbeat staleness filter |
| 8 | `FUSE_PEER_LABELS` hardcoded in the DaemonSet | fixed — code, manifests, image, and RBAC applied on `stargz-test`; **the DaemonSet rollout itself is still pending** (see below) |
| 9 | `/api/files/{path}/size` unreachable | fixed — specific route registered before the catch-all |
| 10 | `WarmupResult` has no json tags | fixed — snake_case throughout |
| 11 | No warm progress or status endpoint | fixed — job IDs + `GET /api/cache/warm[/{id}]`, fan-out returns `jobs`, dashboard polls them; progress is chunk-level |
| 12 | Unexplained slow peer reads via `10.244.1.189` | root-caused — slow node is a 4-vCPU `Standard_D4as_v4` (expected); the investigation exposed a real EWMA bug (misses poisoned `success_ratio`), now fixed |

### 1. `ReadRange` panics and kills the FUSE mount on a chunk-size mismatch — CRITICAL

`internal/cache/cache.go:2527`. Reproduced live; `client-zwsgk` went
`CrashLoopBackOff` with:

```
panic: runtime error: slice bounds out of range [4325376:4194304]
  ...cache.ReadRange(...) cache.go:2527
  ...fuse.(*goFuseNode).Read(...) gofuse_backend.go:560
```

Root cause: `ReadRange` computes chunk boundaries from `cm.config.ChunkSize`
(the *reader's* flag, 8 MiB here) but the file on disk was written with a 4 MiB
chunk size. Nothing persists the chunk size a file was written with —
`CacheEntry` has `NumChunks` but no `ChunkSize`, and `FileLocation.ChunkInfo`
carries only `{Index, PeerID, ChunkID}`.

So `chunkOffset = startChunk * 8MiB` while the actual chunk is 4 MiB long, and
`from` lands past the end of `chunkData`. The clamps at 2511-2519 bound `to`
but **never bound `from`** — `from > len(chunkData)` slips through and
`chunkData[from:to]` panics.

Blast radius is worse than one request: the panic is in the go-fuse read
goroutine, so it takes down the whole daemon and every pod using that node's
mount gets `Transport endpoint is not connected`. One unlucky read on a
legacy-chunk-size file = node-wide storage outage.

Two fixes, both wanted:
- **Immediate:** clamp `from` the same way `to` is clamped, in both the
  `chunkCount == 1` branch (2502-2519) and the multi-chunk loop (2544-2554).
  Cheap, stops the crash.
- **Correct:** persist the chunk size per file. Add `ChunkSize` to `CacheEntry`
  and `FileLocation`, write it on publish, and have `ReadRange` /
  `resolveChunkedEntry` / `warmup.go` use the file's value with
  `cm.config.ChunkSize` only as the fallback for legacy metadata.

Also worth a recover() in the go-fuse `Read` bridge: no single read should be
able to unmount a node.

### 2. Silent short reads and silent truncation from the same mismatch

The panic is the loud failure mode. The quiet ones are worse:

- FUSE range read of 64 KiB at offset 4 MiB on a 4 MiB-chunked file returned
  **0 bytes, no error** (`dd` reported `0 bytes copied`).
- Whole-file `GET /api/files/bench-old-256m-3.bin` returned **HTTP 200 with
  134217728 bytes for a file the coordinator declares as 268435456** — exactly
  half, because `numChunks` was derived as `256MiB / 8MiB = 32` when the file
  actually has 64 chunks. The response was truncated with a success status.

A caller has no way to detect this. The `ChunkSize` fix in #1 addresses the
cause; independently, `WriteTo` should compare bytes written against
`entry.Size` and fail loudly on a mismatch rather than returning 200.

### 3. Fast chunk advertisement never fires on the HTTP path

`fuse_chunk_advertise_published_total` stayed 0 across every test including
FUSE reads. `maybeAdvertiseFetchedChunk` is only called from
`readChunkDataFromTiers` (`cache.go:2825/2849/2868`), reachable only via
`ReadRange` → `readChunkData` → `fetchChunkAsLeader`. The whole-file
`GET /api/files/...` path goes `WriteTo` → `Get` → `getFromRemoteTier` and
never touches it.

So the Phase 3 swarm-growth mechanism is invisible to every non-FUSE consumer —
including the warm path, which is precisely the bulk-distribution case it was
designed for. Advertisement should hook the `Get`/promote path too, not just
range reads.

### 4. Warm and chunk-completion traffic is invisible in tier counters

`chunk_complete.go:313` calls `cm.metrics.RecordWrite(landed.Size)` but never
`cm.metrics.RecordHit(tier)`. A 126 MB warm across 5 nodes moved real cloud
bytes and left `cloud_hits` / `cloud_read_bytes` at 0. Any dashboard or capacity
model built on those counters under-reports warm traffic entirely. Add the
`RecordHit`/`RecordTierRead` pair in `fetchAndLandChunkOrdered`.

### 5. Herd-control features are opt-in and silent when off

`-fetch-lease`, `-fast-chunk-advertise`, and `-replica-reconcile-interval-sec`
all default to off/0. Nothing logs at startup that they are disabled, and their
counters read 0 either way — indistinguishable from "on but never triggered."
I burned real time on exactly this ambiguity. Log the effective state of each at
startup, and consider defaulting the reconciler on now that it has cluster time.

### 6. Percentage selection always picks the same nodes

`internal/coordinator/warm.go:82-88` takes `out[:n]` from an ID-sorted list.
Verified live: 40 % twice → identical pair. Repeated partial warms concentrate
on the lowest-ID nodes and starve the rest. Rotate the offset (hash the prefix)
or shuffle before truncating.

### 7. Dead peers stay selectable for up to 90 s

etcd lease TTL is 90 s, so a crashed pod remains `active` and eligible for warm
fan-out for that whole window. Observed 10 "active" peers after a rollout,
converging to 5 after ~90 s. Correct by design, but a warm issued in that window
silently under-delivers. Either shorten the TTL for selection purposes or have
the coordinator drop peers whose last heartbeat is older than ~2 intervals when
building the target list.

### 8. `FUSE_PEER_LABELS` is hardcoded in the DaemonSet

Every peer advertises `pool=ckpt2404`, including the three A100 nodes in
`gpuckpt`. Label-based warm targeting is therefore wrong on any multi-pool
cluster. The value should come from a node label via the downward API, not a
literal in the manifest. (I attempted to correct this live and the change was
blocked as a cluster-wide workload mutation — flagging rather than fixing.)

*Fixed in code and manifests; partially applied live.* New
`internal/nodelabels` reads this pod's own Node object over the in-cluster REST
API (no client-go dependency, one GET at startup) and projects a requested
subset of its labels into the peer registration. Wire-up: client flag
`-peer-labels-from-node` / `FUSE_PEER_LABELS_FROM_NODE`, taking
`peerKey=nodeKey` pairs, e.g. `pool=agentpool,zone=topology.kubernetes.io/zone`
(`eks.amazonaws.com/nodegroup` on EKS, `cloud.google.com/gke-nodepool` on GKE).
`k8s/client-daemonset.yaml` and the Helm chart add a `fuse-client`
ServiceAccount with a ClusterRole granting `get` on nodes and nothing else;
the chart omits both when `client.peerLabelsFromNode` is empty. The lookup is
advisory — no token, no RBAC, or an unreachable API server logs a warning and
falls back to `-peer-labels`.

**Live status on `stargz-test`.** Three of four steps are done:

1. image `stargzrepo.azurecr.io/fuse-client:gpu3-nodelabels` built and pushed
   (this required a Dockerfile fix — `go mod download` ran before
   `COPY third_party`, but go.mod `replace`s go-fuse with that local fork, so
   any build from a clean context failed; only ACR's older non-BuildKit
   scanner surfaced it, and it is fixed by copying the fork's `go.mod`/`go.sum`
   ahead of the download layer);
2. `ServiceAccount/fuse-client` — applied;
3. `ClusterRole` + `ClusterRoleBinding` `fuse-client-node-reader` (get on nodes
   only) — applied;
4. the DaemonSet patch itself — **still blocked** as a cluster-wide workload
   mutation.

The remaining step is one command, and it is safe to run because the RBAC it
depends on already exists:

```
kubectl patch ds client -n fuse-system --patch-file /tmp/client-nodelabels-patch.yaml
```

setting `serviceAccountName: fuse-client`, the four container images to
`gpu3-nodelabels`, and adding
`-peer-labels-from-node pool=agentpool,zone=topology.kubernetes.io/zone,vmsize=node.kubernetes.io/instance-type`.
Verified against a server-side dry run. Until it runs, all five peers keep
advertising the literal `pool=ckpt2404` — wrong for the three `gpuckpt` nodes,
which is the bug this issue describes.

Note the `vmsize` key: issue #12 turned out to be a VM-size difference, and
having instance type as a peer label makes that diagnosable from
`/api/peers` instead of requiring a `kubectl get nodes` correlation.

### 9. `GET /api/files/{path}/size` is unreachable

`internal/api/handler.go:127-128` registers the greedy `{path:.*}` route first,
so the `/size` suffix is swallowed. Register the more specific route first.

### 10. `WarmupResult` has no json tags

Fields serialize as `Mode`/`Files`/`Warmed`/`Failed` — inconsistent with every
other API payload in the codebase, which is snake_case. Any client parsing this
has to special-case it.

### 11. No warm progress or status endpoint

`POST /api/cache/warm` with `async: true` is fire-and-forget. There is no way to
ask "is the warm still running, how far along, did it fail." For a 100 GB model
distribution this is the difference between usable and not. A
`GET /api/cache/warm/{id}` returning per-node counters would close it.

*Fixed.* An async warm now returns `{"started":true,"job_id":...,"status_url":...}`.
`WarmupOptions.OnProgress` reports counters after each file, the daemon keeps
the last 32 finished jobs, and `GET /api/cache/warm/{id}` (plus a list route)
serves status/progress/result/error. The coordinator's fan-out result carries a
`jobs` map of peer ID → job ID, and the dashboard's warm page lists and polls
them.

Progress is reported at **chunk** granularity, not just per file. Per-file
callbacks alone leave the exact case this issue is about — distributing one
very large model — sitting at `0/1 files` for the entire transfer, which is
indistinguishable from a hang. `chunkProgress` threads two callbacks through
`runChunkCompletionOpts` → `fetchMissingChunks`: `Plan` announces how many
chunks the pass must fetch (chunks already on NVMe are excluded, so a resumed
warm still converges on its denominator) and `Landed` fires per chunk.
`WarmupProgress` gains `chunks`, `chunks_done`, and `in_flight_bytes` — the
last so the byte counter advances continuously instead of stepping only at
file boundaries; it is backed out when a file completes and its bytes move
into `bytes`, so the two never double-count. The dashboard renders a second
progress bar whenever chunks are in flight. Per-chunk locking is skipped
entirely when no `OnProgress` consumer is registered.

### 12. Unexplained slow peer reads through one node

Two of six herd reads via `10.244.1.189` ran at ~40-44 MB/s (4.73 s / 5.20 s)
against ~300 MB/s elsewhere. That is a `ckpt2404` node, not GPU — likely a
smaller VM with a slower NIC, but not confirmed. Worth a targeted check before
assuming it is benign, since the peer-latency EWMA should have deprioritized it
after 3 samples and apparently did not within this run.

*Investigated; the slow node is expected, the EWMA is a real bug.*

**The slow node is a VM-size artifact, not a fault.** `10.244.1.189` is pod
`client-xr6ll` on `aks-ckpt2404-37963404-vmss000002`, a **Standard_D4as_v4**
(4 vCPU). The nodes reading at ~300 MB/s are **Standard_NC24ads_A100_v4**
(24 vCPU). The gap tracks the VM's NIC allocation. Nothing to fix in the
serving path.

**But the second half of the observation was correct and led to a real
defect.** `/api/peers/latency` on the live clients showed `success_ratio` at
or near **0 on nearly every pair** despite 47-140 samples:

```
client-htllv → {"peer_id":"client-mjl9j","latency_ms":2.57,"success_ratio":0,"samples":47}
             → {"peer_id":"client-xr6ll","latency_ms":2.32,"success_ratio":0,"samples":47}
             → {"peer_id":"client-htllv","latency_ms":4.85,"success_ratio":0.8618,"samples":62}
```

`pairScore` is `success / latencyMs`. With success ~0 for every candidate,
every score collapses to ~0, ties dominate, and the latency ordering the
feature exists to provide becomes a no-op. Worse, `client-xr6ll` — the slow
D4as_v4 — reported the *lowest* latency in the table (2.32 ms), so even a
working score would have ranked it first. That is exactly why it was never
deprioritized.

Root cause: `readPeerData` recorded **every** non-busy outcome into the EWMA,
including "this peer does not hold the object." Traversal asks candidate
holders in order and stops at the first hit, so on a cluster with R replicas
and N peers most attempts miss *by construction*. The EWMA was therefore
measuring replica placement, not link health, and converged toward zero for
everyone.

*Fixed.* Misses are now a distinct classification, parallel to the existing
busy/unreachable ones:

- serve side returns a typed miss — gRPC `NOT_FOUND` (was a bare `fmt.Errorf`,
  indistinguishable from a transfer failure), raw HTTP already returned 404;
- requester side adds `peerMissError` + `isPeerMiss`, and `readPeerData`
  records the pair only for attempts that actually exercised the transfer:
  busy is admission control, miss is stale routing, everything else is signal;
- a miss short-circuits both the raw→gRPC fallback and the gRPC
  reconnect-retry, since both transports read the same local cache and a 404
  is a definitive answer — this also removes a reconnect per stale holder;
- misses are counted separately as `fuse_peer_fetch_miss_skips_total`, which
  is the honest place for that signal: a high ratio means the coordinator's
  location metadata is stale, which is worth an alert on its own.

Regression coverage in `peer_latency_test.go`: a peer that answers only misses
accumulates no samples at all (rather than a run of recorded failures), while a
genuinely unreachable peer still drives success to ~0 so the EWMA keeps routing
around bad links.

---

## Improvement priorities

1. **#1 + #2 — persist per-file chunk size.** One root cause, three failure
   modes, one of them a node-wide outage and one of them silent data truncation.
   Nothing else on this list matters as much.
2. **#4 — fix warm/chunk-completion metrics.** Cheap, and everything else you
   measure depends on these counters being honest.
3. **#3 — advertise on the `Get` path.** Makes Phase 3 actually apply to the
   warm and HTTP workloads it was built for.
4. **#5 + #6 + #7 — operability of the warm/herd features.** Startup logging,
   rotation in percentage selection, staleness filter on target selection.
5. **#9, #10, #11 — API surface cleanups.** Small, and #11 is the one users will
   ask for first.

## Live cluster state at end of run

Still running and billing: 3 × Standard_NC24ads_A100_v4 in `gpuckpt`
(~$11/hr). Test pods `gpureader` and `herd-1..6` still present. Port-forwards
18080-18084 open. `client-zwsgk` has 2 restarts from the issue-#1 panic and is
Running again. The DaemonSet still carries the three test-only flags.

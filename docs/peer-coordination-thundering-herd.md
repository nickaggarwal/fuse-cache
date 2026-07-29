# Peer Coordination & Thundering-Herd Control (Write / Replication Path)

Status: **Phases 1–3 implemented** (serve-side admission control, busy
failover + jittered retry, staggered capacity-aware replication, pairwise
latency metadata, cross-node fetch leases, fast chunk advertisement,
demand-driven replica reconciler, metrics). Remaining ideas: coordinator
parent-assignment trees / Dragonfly integration. Grounded in the current
codebase and in 3FS / Dragonfly patterns.

**Defaults (perf-validated on 3×A100):** Phase 1 is **on by default** — it
adds no measurable read-throughput cost and TH-1 validated it live
(admission rejects under load, requesters fail over, all reads correct).
Phase 2–3 (`-fetch-lease`, `-fast-chunk-advertise`,
`-replica-reconcile-interval-sec`) are **opt-in / off by default**: they are
thundering-herd optimizations that help many-reader workloads but add
per-read cost on isolated reads (fast-advertise promotes every fetched
chunk to NVMe, competing with the read; the fetch-lease follower-wait adds
cold-read latency). Enable them for herd-prone deployments. Even bounded
(promotion gate + NVMe pressure skip), fast-advertise did not reach
single-read parity, so it ships off. The mechanisms remain fully
implemented and unit-tested.

## Implemented (Phase 1)

| Mechanism | Where |
|---|---|
| Serve-side admission gate (`peerServeGate`, default 64 in-flight; `-peer-serve-max-inflight`) | `internal/cache/peer_admission.go`, wired into `PeerGRPCServer.ReadFile`/`WriteFile` (`RESOURCE_EXHAUSTED`) and the raw HTTP `/api/peer/read` handler (`503` + `Retry-After`) |
| Requester busy-reaction: skip busy holder → next candidate; one jittered retry pass (10–150 ms) over busy holders when all else fails | `PeerStorage.Read` in `internal/cache/peer_storage.go` |
| Busy is not a network failure: no gRPC conn eviction, no raw→gRPC fallback to the same saturated node, no latency sample recorded | `readPeerData` / `readFromPeer` / `readFromPeerRaw` |
| Staggered, capacity-aware replication: headroom-scored targets (space × network score) on top of the crypto shuffle, jittered stagger between replica RPCs (`-peer-replication-stagger-ms`), busy targets skipped | `PeerStorage.Write`, `sortPeersByReplicationScore` |
| **Pairwise latency metadata**: EWMA of observed this-node→peer latency/success from real transfers, used to order traversal once ≥3 samples exist (beats the coordinator's single-target probe); exposed at `/api/peers/latency` and as `fuse_peer_pair_*` metrics | `internal/cache/peer_latency.go` |
| Metrics: `fuse_peer_serve_inflight/capacity/accepted_total/rejected_total`, `fuse_peer_fetch_busy_skips_total`, `fuse_peer_fetch_jitter_retries_total`, `fuse_peer_replication_busy_skips_total/staggers_total`, `fuse_peer_pair_latency_ms/success_ratio/samples_total` | `internal/api/handler.go` `/metrics` |

## Implemented (Phase 2)

| Mechanism | Where |
|---|---|
| **In-flight fetch leases** (cross-node single-flight for origin pulls): `FetchLeaser` capability on the coordinator, `InMemoryStore` TTL map + `EtcdStore` `/fuse/inflight/<key>` create-if-absent Txn with etcd lease expiry; HTTP `/api/fetch-lease` (POST acquire/renew, DELETE release) on both coordinator clients | `internal/coordinator/fetch_lease*.go`, `cmd/coordinator/main.go` |
| Lease-gated cloud reads: ordered (non-hedged) remote fallbacks call `getFromCloudLeased` — the lease winner pulls origin and releases; losers wait 100–400 ms jitter, read the **peer tier** (leader has it by then), and only fall back to cloud if that misses. Advisory: any coordinator error degrades to a plain cloud read (`-fetch-lease`, default on) | `internal/cache/origin_lease.go`, `readChunkDataFromTiers` / `Get` in `cache.go` |
| Staggered capacity-aware write replication (originally slated Phase 2) | shipped in Phase 1 (`PeerStorage.Write`) |

## Implemented (Phase 3)

| Mechanism | Where |
|---|---|
| **Fast holder advertisement**: a chunk fetched from any remote tier is promoted to local NVMe (making it servable via `GetLocal`/`LocalChunkFile`) and this node publishes itself as a holder of the parent the moment the *first* chunk lands — not after the whole object — so later requesters pull from the growing swarm. Publications are coalesced to one per parent per 5 s to protect the coordinator (`-fast-chunk-advertise`, default on) | `internal/cache/chunk_advertise.go` |
| **Demand-driven replica reconciler**: background loop (`-replica-reconcile-interval-sec`, default 60 s) compares `R_desired` (`-replica-reconcile-target`, default = min replicas 3) against the coordinator holder set for hot local objects (accessed within 10 min, ≤64 MiB), tops up the deficit via staggered busy-aware `ReplicateTo`, capped per pass (`-replica-reconcile-max-per-run`, default 8); a busy cluster aborts the whole pass. Cold-object decay is delegated to LRU eviction (every replica is cloud-backed) | `internal/cache/replica_reconciler.go`, `PeerStorage.ReplicateTo` |
| Metrics: `fuse_fetch_lease_granted/denied/errors_total`, `fuse_fetch_lease_follower_peer_hits/cloud_fallback_total`, `fuse_chunk_advertise_published_total`, `fuse_replica_reconcile_runs/replications/skipped_busy_total` | `internal/api/handler.go` `/metrics` |

Not built (deliberately): coordinator parent-assignment trees — the lease +
fast-advertise + existing multi-peer striping already yields swarm-style
spread; revisit only if origin fan-out is still observed at scale. Dragonfly
integration remains an alternative if the in-cluster deployment should own
bulk P2P distribution instead.

## Problem

When an object becomes hot, the naive path stampedes a single source:

- A write lands the object on **one** node's NVMe + async cloud persist + a fixed
  3× peer replication (crypto/rand shuffle). Until replicas exist, every other
  node that wants it misses NVMe and hits the **same origin** (the writer, or
  cloud) at once.
- Chunked files multiply this: `N` readers × `M` chunks = `N·M` concurrent
  requests at the origin. Each reader independently pulls the **whole** object
  (read-promote), so origin egress and cloud GETs scale with `N`, not with the
  object size.
- Synchronized misses + synchronized retries amplify the spike.

The tension the design must resolve: **speed** (warm replicas fast) vs
**thundering herd** (don't melt the source doing it).

## What already exists (reuse, don't reinvent)

| Primitive | Where | Use in this plan |
|-----------|-------|------------------|
| Intra-node single-flight | `cache.go` `startChunkFetch`/`chunkFetches` (leader/follower on `state.done`) | Extend the *idea* cross-node |
| Hedge concurrency cap | `hedgeLimiter chan struct{}` | Template for serve-side admission control |
| Per-tier speed signal (EWMA latency/success) | `tier_perf.go` `tierPerfTracker` | The "am I saturating the source?" signal |
| File-location metadata + TTL leases | `coordinator` (`EtcdStore`) | Holder set, in-flight-fetch leases, replica count |
| Read-promote (organic replication) | read path publishes `FileLocation` after caching | The seed of swarm propagation |
| Network probe (speed/latency per peer) | `/api/netprobe`, `PeerInfo.Network*` | Target selection / load awareness |
| **Dragonfly P2P distribution** | deployed in-cluster (`dragonfly-system`) | Precedent; possible integration target for swarm |

## Goal / definition of steady state

Each hot object converges to **R replicas spread across the cluster**, with:
- origin/cloud load bounded (each chunk served O(fanout) times, not O(N)),
- no redundant refetch of the same chunk,
- convergence in ~**O(log N)** propagation rounds (swarm), not O(N) origin-BW-limited,
- replicas **decayed** for cold objects so capacity is reclaimed.

`R(object)` is demand-driven (a function of read rate), not fixed at 3.

## Signals (the speed-vs-herd control inputs)

- **Demand**: per-object read/miss rate. Nodes report miss events to the
  coordinator (cheap counter), or coordinator infers from `GetFileLocation` hit
  rate.
- **Supply**: current replica count (coordinator metadata).
- **Source pressure**: serve-side in-flight gauge + `tierPerfTracker` latency
  rising / throughput dropping ⇒ the source is saturating ⇒ **back off**.
- **Headroom**: target NVMe free space + network probe score.

Modulate aggressiveness with **EWMA + hysteresis** (mirror `tierPerfTracker`) to
avoid flapping: widen fan-out only when `demand > supply` **and** source has
headroom; hold/allow decay otherwise.

## Mechanisms (phased)

### Phase 1 — Backpressure & jitter (small, highest ROI)
1. **Serve-side admission control.** Bound concurrent peer serves per node with a
   semaphore (like `hedgeLimiter`). Beyond the cap, return `503 Busy` (raw HTTP
   transport) / `RESOURCE_EXHAUSTED` (gRPC) instead of queuing unboundedly.
2. **Requester reaction.** On `503/busy`, the reader picks the next holder (it
   already iterates candidates) and applies **randomized jitter** before retry,
   breaking synchronized stampedes.
3. **Metrics.** `fuse_peer_serve_inflight`, `fuse_peer_serve_rejected_total`,
   `fuse_peer_fetch_jitter_seconds`.

*No coordinator changes. Protects the source from collapse immediately.*

### Phase 2 — Cross-node single-flight + load-aware write replication
4. **In-flight fetch leases.** Before pulling object/chunk X from origin, a node
   takes a short-TTL lease in the coordinator (`/fuse/inflight/X`). Peers that
   want X while the lease is held **wait briefly then read from the lease holder**
   (which is about to have it) instead of re-hitting origin. Collapses the herd
   to ~1 origin pull + swarm.
5. **Staggered, capacity-aware replication.** Replace the fixed 3× shuffle at
   write time with: pick R targets by headroom/network score, **stagger** the
   replication RPCs with jitter, and **skip busy targets** (respect their
   admission signal). Replication itself must not stampede.

### Phase 3 — Swarm / tree propagation + steady-state reconciler
6. **Fast holder advertisement.** A node publishes its `FileLocation` the instant
   it caches a chunk (not after the whole object), so the holder set grows
   mid-transfer and later requesters pull from the **growing swarm**, not origin.
   This is the O(log N) accelerator (BitTorrent / Dragonfly model).
7. **Parent assignment (optional).** Coordinator hands each new requester a
   *parent* holder to pull from, building a balanced distribution tree and
   spreading serve load evenly. Alternatively, **integrate the in-cluster
   Dragonfly** for the bulk P2P swarm rather than building our own.
8. **Demand-driven replica reconciler.** A background loop compares `R_desired`
   (from demand) vs actual replica count and issues **throttled, staggered**
   replication toward `R_desired`; **decays** replicas for cold objects. This is
   the steady-state control loop; gate it on source-pressure signals.

## Problem → solution map

| Failure mode | Mitigation |
|---|---|
| Synchronized misses hammer origin | in-flight leases (P2) + jittered retry (P1) |
| Origin overload / collapse | admission control + `503/busy` backpressure (P1) |
| Redundant refetch of same chunk | intra-node single-flight (exists) + cross-node leases (P2) |
| Slow convergence (star topology) | fast holder advertisement + swarm/tree (P3) |
| Replication itself stampedes | staggered, load-aware replication (P2) |
| Oscillation / flapping | EWMA + hysteresis on demand & pressure signals |
| Cold replicas waste capacity | reconciler decays R for cold objects (P3) |

## 3FS lessons applied

- 3FS spreads data across **many storage targets at write time** (chain /
  CRAQ-style), so reads are naturally distributed — the herd is avoided by
  **topology**, not just runtime throttling. Bias this design toward
  **write-time distribution** (wider, staggered replica placement) + **read-time
  striping across holders** (the existing parallel fanout already stripes chunk
  reads across peers — Phase 3 makes sure there *are* multiple holders to stripe
  across).
- 3FS uses **request-level flow control at targets** — directly motivates the
  Phase 1 admission control.

## Risks / watch-items

- **Coordinator hotspot** under lease churn → keep leases short-TTL, coalesce,
  and consider gossip for holder advertisement to offload the coordinator.
- **Holder-metadata staleness** → TTL leases + verify-on-read fallback (readers
  already try all active peers, so a stale holder just costs one retry).
- **Over-replication churn** → decay + hysteresis; cap replication ops/sec.
- **Interaction with adaptive read** (`tierPerfTracker`): a source under
  admission pressure will show rising latency and get demoted — desirable, but
  ensure the reconciler and the reader don't fight (share the same pressure EWMA).

## Suggested hand-off order

1. Phase 1 (serve admission control + jittered retry + metrics) — self-contained,
   no coordinator changes, immediately protects sources. **Start here.**
2. Phase 2 (in-flight leases + staggered load-aware replication).
3. Phase 3 (fast advertisement + swarm/tree or Dragonfly integration + reconciler).

Validate each phase with a "cold fan-out" benchmark: `N` readers request one
freshly-written object simultaneously; measure origin egress, p99 read latency,
and time-to-steady-state (all N warmed) vs `N`.

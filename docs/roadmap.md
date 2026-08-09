# What's left: implement, test, improve

Status as of 2026-08-09, after the declarative-cache + routing work landed
(`a8f6821`). Everything here was verified against the tree, not recalled — each
item cites the code or the run that establishes the gap.

Ordering within each section is by payoff, not by effort.

---

## 1. Implement

### 1.1 Warm intents do not survive a daemon restart — *highest value*

`warmIntentRegistry` is in-memory (`internal/cache/warm_reconciler.go:70`), and
the comment above it already names the trap: the CSI driver treats an existing
mountpoint as success and returns early
(`internal/csidriver/driver.go:139`), so a surviving bind mount never recreates
the session, and nothing re-registers the intent. Pinning is lost the same way.

This is the one gap that breaks the guarantee the whole phase exists to make.
After a client restart the node keeps *serving* the subtree correctly through
just-in-time pulls — but it has silently stopped defending its residency, and
`/api/residency` will keep reporting a `last_verified` that no longer has a
reconciler behind it. Declared-vs-actual divergence, which is exactly the
failure this work set out to eliminate, comes back through the restart door.

Fix belongs with **session persistence**, not the reconciler: write
`session.Manager` state (volume ID, target path, refcount, `CachePolicy`) to a
small on-disk journal under the agent socket dir, replay it at startup, and let
replay re-register intents through the existing hook. Getting refcount right
across restart is the delicate part — a replayed refcount that overcounts pins
forever, one that undercounts tears a live pod's mount out from under it.
Reconcile the journal against actual mountpoints at startup rather than
trusting it.

### 1.2 Remote chunk reads verify size, not content

`fetchAndLandChunkOrdered` (`internal/cache/chunk_complete.go:336`) lands
whatever a peer or cloud tier returns, records a hit, and advertises the node as
a holder — without ever checking the bytes. NVMe reads *are* checksum-verified
against a `.sha256` sidecar (`internal/cache/nvme_range.go:138`), so the
protection exists; it just stops at the tier boundary. A truncated or corrupted
remote chunk lands, gets a sidecar written from its own contents, and then
verifies cleanly forever — and the node advertises itself as a good source of
it. Corruption propagates through the swarm with a valid checksum attached.

Carry the origin checksum with the transfer (peer gRPC response field; cloud
object metadata) and verify before landing. Where no origin checksum exists,
that must be a distinguishable state — an unverified-landing counter — not a
silent pass. `docs/test-plan.md` IR-3 is the test that was written for this and
never run.

### 1.3 Streaming write path (~77% of device speed)

Measured: FUSE write ~305 MB/s against 566 MB/s raw NVMe on i7ie.xlarge. `Put`
and `PutFromReader` buffer a whole chunk before writing
(`internal/cache/cache.go:1387-1399`). The lever is streaming the
chunk-and-cloud-persist pipeline, **not** FUSE flags — those were already A/B'd
(writeback_cache costs 14% on small files for 4-6% on large sequential).

### 1.4 Phase 5 read-path levers (from the JuiceFS write-up)

Confirmed absent in the tree; each is small and independent:

- **Client-side location-metadata TTL cache.** Every location lookup hits the
  coordinator today — `GetFileLocation` is called from five separate paths
  (`cache.go:964`, `cache.go:2816`, `peer_storage.go:606`,
  `replica_reconciler.go:165`, `delete.go:361`) with no cache in front of any of
  them. A model directory is hundreds of files. Pairs naturally with
  `cacheMode: readonly`, where a long TTL is safe by construction.
- **Request coalescing on the demand path.** There is no `singleflight` anywhere
  in the tree. Chunk *completion* dedupes by file (`chunk_complete.go:58`) and
  *prefetch* dedupes by chunk (`cache.go:3495`), but two concurrent demand reads
  of the same cold chunk still issue two fetches. Under a herd that is exactly
  the traffic admission control is there to shed.
- **Read-ahead ratio.** `adaptive_window.go` sizes the window in whole chunks;
  there is no sub-chunk ratio. Matters for random-access formats
  (TFRecord/HDF5/LMDB) where whole-chunk prefetch is read amplification. Lowest
  confidence of the three — verify against a real random-read workload before
  building it, since our adaptive window may already cover the sequential case
  that motivated theirs.

### 1.5 Fetch leases still ride the HTTP fallback

No `FetchLease` RPC in `proto/coordinator.proto`; the lease path uses the
coordinator's HTTP fallback. Fine today — leases are advisory and reads succeed
with the coordinator down. Worth doing only if lease QPS shows up in a profile.

### 1.6 EPP is not behind a real data path

`scripts/llmd-epp.yaml` runs in standalone mode and is driven by
`cmd/scoreprobe` speaking ext_proc directly. That proves the *scorer*, which was
the point. It does not prove the integration end to end: no Envoy/Gateway sits
in front, so no actual inference request has ever been routed by this decision.
Closing that needs a Gateway with an ext_proc filter pointed at `fuse-epp:9002`
and a load generator measuring per-request TTFT split by which node served it.

---

## 2. Test

### 2.1 `docs/test-plan.md` is written and unexecuted

Four families, none run against a live cluster: **TH-1..6** (admission control,
fetch leases, chunk advertisement, staggered replication, reconciler top-up,
pairwise latency), **NI-1..4** (node-init discovery per cloud, capacity refresh,
cache dir vanishing, fallback), **IR-1..4** (chunk missing everywhere, peer dies
mid-stream, corrupted cloud object, reader cancellation), **FL-1..6** (SIGKILL,
kill mid-write, SIGSTOP hang, daemon kill under CSI consumers, kill during herd,
crash-loop).

**FL-3 (SIGSTOP hang) is the one to run first.** A stopped-but-alive FUSE daemon
is the nastiest failure this system has: mounts don't error, they block
forever, and every consumer of the mount hangs with it. Nothing in the current
e2e coverage touches it.

**NI-1** is also load-bearing and cheap: node-init has only ever been tested
against synthetic host roots. It is the component that decides where the cache
lives on every node in every cloud, and a wrong answer there is silent — it
just picks a slower disk.

### 2.2 `unknown ≠ zero` is not exercised live

Section 5 of `e2e-llmd-routing.sh` no-ops when all peers are reachable, which is
the normal case. The invariant is enforced at three layers (wire `Unknown`,
attribute `Known`, `unknownScore` at score time) and unit-tested, but the live
path from an unreachable node through to a 0.4 score has never run. Deleting a
client pod to force it is permission-blocked and correctly so — it would take
down a mount other workloads depend on. A `NetworkPolicy` that blackholes one
peer's port 8081 for the duration of the assertion gets the same signal without
touching the mount.

### 2.3 Restart semantics have no test at all

Directly downstream of 1.1: no test kills a client daemon with an active warm
intent and asserts anything about what comes back. Whatever the fix, this is the
test that proves it. Follow the project rule — key the handoff off observed
state (intent count, a counter), never a sleep against real I/O.

### 2.4 Cold-vs-warm needs a repeatable harness, not one run

1.48s vs 3.26s is a real number from a real run, but it is n=1 on one model at
one size. A cold node warms itself just by serving one load, so every repeat
must re-establish asymmetry deliberately. Wrap it: evict → measure cold → warm
via `POST /api/cache/warm` → measure warm, N times, report the distribution.
Then extend to a model where the number actually decides something — 7B+, where
the checkpoint is 15GB rather than 3GB and the peer tier has to work for it.

---

## 3. Improve

### 3.1 `k8s/` manifests are a live footgun

`k8s/client-daemonset.yaml:50` still pins `nodeSelector: agentpool: userpool`,
which matches nothing on the current cluster (`userpool` is scaled to 0; the
live DaemonSet uses a nodeAffinity list over userpool/gpuckpt/ckpt2404). A
`kubectl apply -f k8s/` — the command the README suggests — deschedules every
client pod in the cluster. `deploy-ckpt2404.sh` refuses to run for exactly this
reason, but the manifests themselves should not be a trap. Update them to the
nodeAffinity form the cluster actually runs.

### 3.2 Residency-after-warm is confusing to an operator

Reading a file through the mount promotes it as individual `_chunk_N` files that
coalesce over subsequent passes; residency reads ~0 for ~30s while `du` shows
full size. This cost a debugging cycle during the live test and is understood
but recorded nowhere in-repo. Either surface partial-chunk residency in the
report (so it climbs rather than jumping), or document the lag on the endpoint —
a routing signal that reads 0 when the bytes are present is the same
false-negative class the byte-weighting work was meant to close.

### 3.3 Five `tmp_s3probe*.go` files are tracked in the repo root

`tmp_s3probe.go`, `_bucket_ps`, `_endpoint`, `_exists`, `_read`. They break
`go build ./...` on macOS and predate all of this work. Delete them.

### 3.4 Cluster leftovers from the routing test

Still running on `stargz-test`: `curlbox`, `model-seeder` (Completed),
`vllm-image-prepull` DaemonSet, `fuse-csi-node-ckpt2404` DaemonSet,
`vllm-qwen` (2 A100 pods), `fuse-epp`. The two A100s are the expensive part.
Teardown needs an explicit instruction — scaling `gpuckpt` back to 0 is not
covered by the standing test-cluster deploy authorization.

### 3.5 Peer-tier throughput has headroom

Re-warm ran at 761–785 MB/s over the peer tier for 3.1GB, against ~1.04 GB/s
measured cold cross-node on EKS. Not a defect, but the gap is unexplained and
worth one profiling pass before anyone tunes on guesses.

---

## Suggested order

1. **1.1 session persistence** + **2.3 its test** — closes the one hole in the
   declarative guarantee, and everything downstream reports a residency it can
   now actually defend.
2. **1.2 chunk checksums** + **IR-3** — silent corruption that propagates with a
   valid checksum is the worst-consequence bug in the list.
3. **2.1 FL-3 and NI-1** — cheapest real risk reduction available; both are
   already written.
4. **1.4 metadata cache + singleflight** — small, independent, directly cuts
   coordinator load and herd traffic.
5. **1.3 streaming writes** — largest single perf number left, but a real
   refactor; do it when the above are stable.

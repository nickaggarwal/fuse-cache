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

### 1.1b Fixed this session: NVMe accounting drift and a torn-write boundary

Recorded here because two are shipped fixes worth not re-litigating, and the
third is a live constraint on what the cache promises.

**`nvmeUsed` drifted, and the surplus was unreclaimable.** The counter derived
its delta from the entry table (read old size, subtract, add new), but every
caller installs the table row *after* `putToNVMeWithEviction` returns. Two
concurrent writers to one path therefore both read "no existing entry", both
added a full size, and only one row landed. Eviction can only free what an entry
claims, so the surplus survived for the process lifetime: a node drifts toward
apparently "full" and starts refusing writes with `NVMe storage full after
eviction` on a nearly empty disk. Four distinct sites had the same shape
(derived-delta race; `removeChunkEntriesLocked` dropping chunk rows without
releasing bytes; chunk completion double-counting an already-accounted parent;
rename releasing without re-crediting).

Fixed with an explicit per-key ledger, `nvmeBytes map[string]int64`, where
`nvmeUsed` is always exactly its sum and every update is idempotent in the key
(`accountNVMeLocked` / `unaccountNVMeLocked`).

That alone was not sufficient, which is the part worth remembering: idempotence
*in the key* does not compose when the disk mutation and the ledger update are
two separate critical sections. A delete and a write on one key interleave as
(delete bytes) → (write bytes, credit) → (unaccount) → (install row), leaving
bytes and a row with no credit — a silent undercount that lets the cache
overfill. The mutation, not just the ledger write, has to be the atom, so a
striped `nvmeKeyMu` now spans both steps in `putToNVMeWithEviction`,
`deleteLocalNVMeFootprint`, and the chunk-retire loop in `chunk_complete.go`.
Regression coverage in `nvme_accounting_test.go`, including the consequence test
(a 256B write into a 16KB cache must still succeed after concurrent churn).

Widening the atom once was still not enough, for the same reason a third time:
the *row install* was left outside it. Every caller wrote bytes and credited
them under the key lock, then installed the entry table row after releasing it.
Eviction and overwrite-cleanup can only reclaim what a row claims, so in that
window the bytes are charged to a key nothing can release. `putToNVMeWithEviction`
now takes the row as a parameter and publishes it inside the same critical
section, so bytes, credit, and row move together at all seven call sites. The
general lesson, stated once: **an invariant spanning N pieces of state needs all
N inside the atom.** Each partial fix looked complete because it closed the
interleaving that happened to be reproducing at the time.

Two methodology notes that cost the most time here:
- **Vary `GOMAXPROCS`, don't just add load.** The remaining drift would not
  reproduce in 25 consecutive full-package runs, but failed within 8 at
  `GOMAXPROCS=2/3/8` and never at 1 or 4. Repetition explores one scheduler's
  preferred interleavings; changing the processor count explores different ones.
- **Instrument for the causal chain, not the symptom.** Naming the drifted key
  was not enough — two different bugs produced the same key. Recording every
  disk *and* ledger mutation on one timeline with its stack turned a week-shaped
  guessing game into reading off the last event.

**One reported drift was not a bug at all**, and the same instrumentation is
what showed it. `deleteLocalNVMeFootprint` removes bytes *before* taking `cm.mu`
to release their credit, so a snapshot taken under `cm.mu` mid-eviction
legitimately observes credit whose bytes are gone. The ledger invariant is a
*quiescent* one. Joining the test's own workers is insufficient because a read
that misses schedules its promotion on a goroutine that outlives the worker that
issued it — and that promotion evicts. `assertNoDrift` now drains `bgWg` first.
Asserting an invariant in a state where it is not yet required is a test bug
that reads exactly like a product bug.

**A pre-existing data race in `evictionSafe`**, found by the same tests and
confirmed against unmodified source before being attributed: `evictToTarget`
collects `*CacheEntry` pointers under RLock, releases it, then reads
`PersistedToCloud`/`IsChunked`/`NumChunks`/`Size` off the live pointer while
`markPersistedToCloud` writes them under the write lock. Fixed with a locked
snapshot.

**Chunked overwrite is not atomic** — measured, not fixed, and now documented in
`docs/SEMANTICS.md`. `putChunked` writes chunks in parallel with no version
barrier, so a concurrent reader assembles a mix of old and new chunks in ~93% of
reads under a tight overwrite loop; the unchunked path is atomic (0 torn in
214,181). The old semantics doc asserted same-node writers "serialize through
the FUSE layer", which is true of the FUSE entrance only — the HTTP API, warm,
and promote all bypass it. `TestConcurrentWrite_ChunkedOverwriteTears` pins the
length property that does hold and logs the tearing without failing, so a future
fix shows up as the log going quiet. A real fix means versioned chunk names or a
staged-then-renamed directory; worth doing only if a workload overwrites large
files in place, which the checkpoint/model workloads do not.

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

**Two of the three items previously listed here were already implemented.** The
error came from grepping for the literal string `singleflight`, which appears
nowhere in the tree — the mechanism is hand-rolled under a different name. A
name search is not an existence check; corrected against the call sites:

- ~~**Request coalescing on the demand path.**~~ **Already present.**
  `startChunkFetch`/`completeChunkFetch` (`cache.go:3000`) is a singleflight in
  everything but name: first caller for a chunk key becomes leader, the rest
  wait on `state.done` and share the result. It is on the demand path —
  `cache.go:2976`, immediately after the range-cache miss — with a wait timeout
  and a `clearStaleChunkFetch` retry so a wedged leader cannot strand followers.
- ~~**Client-side location-metadata TTL cache.**~~ **Already present**, in two
  layers. `metadata_lookup.go` answers FUSE lookup/readdir/getattr from the
  entry table's indexes and only falls through to the coordinator's cached
  metadata view past `MetadataRefreshTTL` (default 5s). The two hot
  `GetFileLocation` call sites each sit behind their own TTL memo —
  `hybridHints` (`cache.go:977`) and the chunk-stride resolver
  (`cache.go:2905`). The three remaining callers are cold paths: delete,
  replica reconcile, and a peer fallback.

  Left open: the TTL is uniform. JuiceFS's actual prescription was a *longer*
  TTL for read-only mounts, which we could take safely under
  `cacheMode: readonly` — cheap, and the one part of this item still unbuilt.
- **Read-ahead ratio** — genuinely absent, and now the only real item here.
  `adaptive_window.go` sizes the window in whole chunks; there is no sub-chunk
  ratio. Matters for random-access formats (TFRecord/HDF5/LMDB) where
  whole-chunk prefetch is read amplification. Verify against a real random-read
  workload before building it: our adaptive window may already cover the
  sequential case that motivated theirs.

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

### 2.4 Concurrency coverage exists now; the gap moved up a layer

`internal/cache/concurrent_rw_test.go` (12 tests) and
`nvme_accounting_test.go` (7) now cover many-requests-at-once against one
manager: overlapping `ReadRange` windows straddling chunk edges, whole-file read
herds, atomic-vs-torn overwrite, mixed read/write/delete load, the same under
continuous eviction, range boundaries, degenerate sizes across
`Get`/`ReadRange`/`WriteTo`, and all-or-nothing delete visibility. They found
both bugs in 1.1b. Verified with `-race -count=10`, plain `-count=12`, and
across `GOMAXPROCS=1..8` — the last of which is what actually exposed the
remaining drift, and is now the mode to run before believing a concurrency fix.

The project's "never race a wall-clock timer against real I/O" rule needed one
extension. Two of these tests ran their racing workers for a fixed 200ms and
then asserted `reads > 0`, which bounds the test by the clock but bounds its
*evidence* by whatever the scheduler granted: under full-package load at
`GOMAXPROCS=2` the window intermittently yielded zero reads and the test failed
with "proved nothing" — a starved machine reported as a product defect. They now
run until a fixed sample size (`awaitReads`), with the deadline demoted to a
hang backstop. **A timed window is only safe when the assertion doesn't depend
on how much work fit inside it.**

Two things they deliberately do not cover, both now the real gap:
- **Single-manager only.** Every test drives one `DefaultCacheManager` with mock
  tiers. Concurrent *cross-node* writers — the case `docs/SEMANTICS.md` says is
  uncoordinated — are untested; that needs the in-process gRPC peer harness
  (`startHerdPeer`) rather than mocks.
- **Concurrency against the FUSE entrance.** The tests enter at the cache API,
  which is the entrance the HTTP path uses. The FUSE layer's own serialization
  claim is therefore asserted by documentation, not by a test.

### 2.5 Cold-vs-warm needs a repeatable harness, not one run

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
4. **1.4 read-only metadata TTL** — the only part of 1.4 not already built;
   small and independent. The read-ahead ratio waits on a random-read workload
   to justify it.
5. **1.3 streaming writes** — largest single perf number left, but a real
   refactor; do it when the above are stable.

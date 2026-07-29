# Live-Cluster Test Plan: Thundering-Herd Control, node-init, FUSE Lifecycle

Runnable validation plan for a real Kubernetes cluster (AKS `stargz-test` or
any cluster deployed via `charts/fuse-cache`). Covers:

1. Instrumentation setup (what to watch and how)
2. Thundering-herd Phase 1–3 scenarios
3. node-init disk discovery scenarios
4. Incomplete / failed read scenarios (partial data must fail, never return garbage)
5. FUSE process kill / hang / death lifecycle and recovery

Conventions used below:

```bash
NS=fuse-system
CLIENTS=$(kubectl -n $NS get pods -l app=client -o name)
# Exec helper on a specific client pod:
kx() { kubectl -n $NS exec "$1" -c client -- sh -c "$2"; }
# Metrics scrape helper (per pod):
metrics() { kubectl -n $NS exec "$1" -c client -- curl -s localhost:8081/metrics; }
```

The FUSE mount inside client pods is `/host/mnt/fuse`; on the node it is
`/mnt/fuse`. Workload pods consuming via CSI see their own bind-mounted path.

---

## 0. Prerequisites & Instrumentation

### 0.1 Deploy with metrics scraping

- Deploy the chart; confirm `servicemonitor.yaml` / Grafana dashboard configmap
  are applied if Prometheus-operator is present. Without Prometheus, every
  check below still works by curling `/metrics` directly (the `metrics` helper).
- Verify every client exposes the new counter families:

```bash
for p in $CLIENTS; do
  echo "== $p"; metrics $p | grep -E \
  'fuse_peer_serve_|fuse_peer_fetch_|fuse_peer_replication_|fuse_peer_pair_|fuse_fetch_lease_|fuse_chunk_advertise_|fuse_replica_reconcile_'
done
```

**Pass:** all families present on all clients, zero-valued on a fresh deploy.

### 0.2 Baseline snapshot

Record before each scenario (append to a log file; diffing before/after is the
core measurement technique for counters):

```bash
snapshot() { for p in $CLIENTS; do echo "## $p $(date -u +%FT%TZ)"; metrics $p; done; }
snapshot > /tmp/baseline.txt
```

Also capture: `kubectl -n $NS get pods -o wide` (pod→node map), coordinator
logs (`kubectl -n $NS logs deploy/coordinator`), and cloud-side request
metrics if available (Azure Blob transactions / S3 GetObject count) — cloud
GET count is the ground truth for "did we stampede origin".

### 0.3 Load generator

Use a Job with N parallel pods, all reading the same file through the FUSE
mount (hostPath) or a CSI volume. Template:

```yaml
apiVersion: batch/v1
kind: Job
metadata: {name: herd-readers, namespace: fuse-system}
spec:
  completions: 12
  parallelism: 12          # N simultaneous readers = the herd
  template:
    spec:
      restartPolicy: Never
      containers:
        - name: reader
          image: busybox
          command: ["sh","-c","time cat /data/hot.bin > /dev/null && md5sum /data/hot.bin"]
          volumeMounts: [{name: fuse, mountPath: /data}]
      volumes:
        - name: fuse
          hostPath: {path: /mnt/fuse, type: Directory}
```

---

## 1. Thundering-Herd Scenarios

### TH-1: Serve-side admission control (Phase 1)

**Goal:** a saturated serving node sheds load with busy signals instead of queueing.

1. Redeploy one client with a tiny gate to force rejections:
   `-peer-serve-max-inflight=2` (edit DaemonSet env/args for one test node
   pool, or `kubectl -n $NS set args ...` on a copy).
2. Write a ~1 GiB file on that node:
   `kx <pod-on-node-A> "dd if=/dev/urandom of=/host/mnt/fuse/hot.bin bs=1M count=1024"`
3. Immediately run the herd Job (12 readers) before replication warms other nodes.
4. Observe on node A: `fuse_peer_serve_rejected_total` > 0,
   `fuse_peer_serve_inflight` ≤ 2 at all times.
5. Observe on reader-side clients: `fuse_peer_fetch_busy_skips_total` > 0,
   possibly `fuse_peer_fetch_jitter_retries_total` > 0.

**Pass:** all readers still complete with correct md5 (busy is failover, not
failure); rejected counter grows; no OOM/goroutine pileup on node A
(`fuse_runtime_goroutines` stays bounded).

### TH-2: Cross-node single-flight fetch lease (Phase 2)

**Goal:** N simultaneous cold misses of one object ⇒ ~1 cloud pull.

1. Write `hot2.bin` (2 GiB) via any client; wait for cloud persist
   (watch client log for persist completion, or Azure Blob shows the object).
2. Drop every local copy so all nodes are cold:
   `for p in $CLIENTS; do kx $p "rm -rf /mnt/nvme/cache/* 2>/dev/null || true"; done`
   then delete coordinator location state by restarting clients (or wait for
   TTL expiry) — simplest: `kubectl -n $NS rollout restart ds/client`.
3. Run the herd Job (12 readers on 12 nodes) against `hot2.bin`.
4. Sum across clients: `fuse_fetch_lease_granted_total`,
   `fuse_fetch_lease_denied_total`, `fuse_fetch_lease_follower_peer_hits_total`,
   `fuse_fetch_lease_follower_cloud_fallback_total`.
5. Check cloud-side GET count for the object's chunks.

**Pass:** denied > 0 and follower_peer_hits > 0 (the herd waited and got data
from the leader); cloud GETs per chunk ≪ N (ideally 1–2, follower_cloud_fallback
accounts for the excess); every reader md5 matches.

**Degradation check:** scale coordinator to 0 replicas, rerun readers on warm
data — reads must still succeed (leases are advisory);
`fuse_fetch_lease_errors_total` grows. Scale coordinator back.

### TH-3: Fast chunk advertisement / swarm growth (Phase 3)

**Goal:** holder set grows mid-transfer; later readers pull from peers, not origin.

1. Cold-start one large file as in TH-2 steps 1–2.
2. Start ONE reader on node B; 10 s later start the herd Job on the rest.
3. Watch `fuse_chunk_advertise_published_total` on node B rise while its read
   is still in progress (poll every 2 s).
4. On the late readers: peer hit counters (`fuse_cache_peer_hits_total`) should
   dominate over cloud hits for this file.

**Pass:** advertisement counter > 0 before node B finishes; late readers show
peer-tier hits; coordinator `/api/files/location?path=/hot2.bin` (HTTP :8080)
lists node B as a holder before the whole-file read completes.

### TH-4: Staggered, headroom-aware replication (Phase 1/2)

**Goal:** write replication doesn't burst, skips busy targets.

1. Keep the `-peer-serve-max-inflight=2` node from TH-1 saturated with a herd Job.
2. From another node, write 50 files of 32 MiB in a loop.
3. Check the writer's `fuse_peer_replication_staggers_total` (should be ≈
   files × (replicas−1)) and `fuse_peer_replication_busy_skips_total` > 0.
4. Check the saturated node received few/no new replicas (its
   `fuse_cache_nvme_*` write counters), while roomy nodes did.

**Pass:** staggers grow, busy skips recorded, writes all succeed.

### TH-5: Replica reconciler top-up (Phase 3)

**Goal:** hot under-replicated objects converge to R without stampeding.

1. Write a file, then delete its replicas from all but one node (rm from NVMe
   cache dirs as in TH-2 step 2, but keep node A's copy and keep reading the
   file from node A every ~30 s so it stays "hot").
2. Wait 2× `-replica-reconcile-interval-sec` (default 60 s ⇒ wait ~2–3 min).
3. On node A: `fuse_replica_reconcile_runs_total` increments;
   `fuse_replica_reconcile_replications_total` > 0.
4. Coordinator location list for the path shows ≥ `-replica-reconcile-target`
   holders (default 3).

**Pass:** replica count converges; per-pass replication bounded by
`-replica-reconcile-max-per-run` (verify counter delta per interval ≤ 8).

### TH-6: Pairwise latency traversal (Phase 1 add-on)

**Goal:** readers route to measured-closest peers and away from regressed ones.

1. After warm traffic exists, check `GET localhost:8081/api/peers/latency` on a
   reader node: pairs table populated (samples ≥ 3).
2. Inject latency on one serving node (requires `tc` on the node or a netem
   sidecar): `tc qdisc add dev eth0 root netem delay 150ms`.
3. Run reads; within ~10–20 samples the reader's pair table shows the degraded
   peer's `latency_ms` rising, and `fuse_peer_pair_*` for other peers should
   absorb the traffic (their `samples_total` grows faster).
4. Remove netem: `tc qdisc del dev eth0 root netem`; confirm recovery.

**Pass:** traversal reorders away from the degraded peer and returns after
recovery (this is the live version of the latency-regression unit test).

---

## 2. node-init Scenarios

### NI-1: Discovery correctness per cloud

On each node pool (run once per cloud when testing AKS/EKS/GKE):

```bash
kubectl -n $NS logs <client-pod> -c node-init            # init container log
kubectl -n $NS exec <client-pod> -c client -- cat /var/run/fuse-client/node-init.json
```

**Pass:** chosen `cache_dir` sits on the expected local disk (AKS: `/mnt`;
node pools with NVMe: the nvme mount), `disk_class` matches reality,
`cache_bytes` ≈ 80% of free space; client log line
`node-init: using discovered cache dir ...` present; `df` on the node confirms
the directory exists on that filesystem.

### NI-2: Capacity refresh (daemon)

1. Fill the cache disk: `kx <pod> "dd if=/dev/zero of=<host cache dir>/filler bs=1M count=5000"` (via the node or host-root mount).
2. Within 2 refresh intervals, `node-init.json`'s `free_bytes`/`cache_bytes` drop.
3. Delete the filler; values recover.

**Pass:** config tracks reality; `generated_at` updates each interval.

### NI-3: Cache dir vanishes

`rm -rf` the discovered cache dir from the node (simulates node repair wiping
ephemeral disk). **Pass:** node-init-daemon log shows "vanished; re-running
discovery" and rewrites a valid config; client continues after its next restart.

### NI-4: Fallback path

Deploy one pod with `nodeInit.enabled=false` (or point `-node-init-config` at
a missing file). **Pass:** after `-node-init-wait-sec`, client logs the
fallback line and comes up on the static `-nvme` path. No crash loop.

---

## 3. Incomplete / Failed Read Scenarios

The invariant under test: **a read either returns complete, correct bytes or
fails with an error — never truncated/garbage data silently.**

### IR-1: Chunk missing everywhere

1. Write a chunked file (> chunk size), wait for cloud persist.
2. Delete ONE chunk object from cloud storage directly (az/aws CLI) AND wipe
   all NVMe copies of that chunk (TH-2 technique).
3. Read the file from a cold node.

**Pass:** the read fails (FUSE returns EIO to `cat`; API GET returns 4xx/5xx);
md5 of any partial output is NOT reported as success by the reader Job; client
log shows `failed to read chunk N`. No entry for the file is published as
locally cached afterward (`/api/cache` must not list it as complete).

### IR-2: Peer dies mid-stream

1. Start a large read on node B that is being served by node A
   (watch `fuse_peer_serve_inflight` on A).
2. `kubectl -n $NS delete pod <client-A> --grace-period=0 --force` mid-transfer.

**Pass:** node B's read still completes correctly (fails over to another
holder or cloud — `fuse_cache_cloud_hits_total`/peer retries grow), md5
matches; if no other source exists, the read errors rather than hanging past
the peer timeout (30 s default) or returning short data.

### IR-3: Cloud object corrupted/truncated

1. Overwrite one chunk object in cloud storage with a shorter payload.
2. Cold-read the file.

**Pass:** read fails or surfaces a size/checksum error in client logs — output
must not silently contain the truncated chunk. (If this passes only because
sizes mismatch, note it: content-level checksum verification on remote chunks
is a known gap — NVMe sidecar checksums exist, remote tiers rely on size.)

### IR-4: Reader cancellation

Kill the reading process (`cat`) mid-read; confirm the client cleans up:
`fuse_peer_serve_inflight` on serving peers returns to 0, no leaked
`fuse_runtime_goroutines` growth across 10 repetitions.

---

## 4. FUSE Process Kill / Hang / Recovery Lifecycle

The client daemon *is* the FUSE server: if it dies, `/mnt/fuse` on the node
goes ENOTCONN ("Transport endpoint is not connected") until remount. Recovery
relies on: container restart (K8s), `prepare-fuse-mount` init container +
`mountWithRetry`/`cleanupStaleMountpoint` (lazy-unmounts the stale mount, up
to `-mount-retries` attempts), and the `preStop` lazy unmount.

### FL-1: SIGKILL the client process (ungraceful death)

1. On node A, start a reader loop in a workload pod:
   `while true; do md5sum /data/hot.bin || echo "READ ERR $(date)"; sleep 1; done`
2. Kill the FUSE process ungracefully (bypasses preStop):
   `kx <client-A> "kill -9 1"` (or on the node: `pkill -9 -f '^client'`).
3. Observe the sequence:
   - reader loop prints errors (expected: ENOTCONN / EIO) — record how many,
   - kubelet restarts the container (`kubectl get pod -w`: RESTARTS +1),
   - client log shows `Mount cleanup` + `FUSE mount attempt k/8` lines,
   - reader loop recovers without the workload pod being restarted.

**Pass:** mount recovers automatically within ~`retries × delay` (default
budget ≤ ~90 s; typical: < 20 s); reader errors are transient and bounded; no
manual node intervention; data written before the kill is intact (it was on
NVMe + cloud); RESTARTS increments by exactly 1.

### FL-2: SIGKILL during an in-flight write

1. Start writing a large file through FUSE: `dd if=/dev/zero of=/data/w.bin bs=1M count=2048`.
2. `kill -9` the client mid-write.
3. After recovery, check `w.bin`.

**Pass:** the `dd` fails with an I/O error (write is NOT silently accepted);
after remount the file either doesn't exist or is readable-and-consistent —
document which. Cloud storage must not contain a chunk set that reads back as
a "complete" file of the wrong length (cross-check `/api/files/w.bin/size`
vs cloud objects). This is the documented durability boundary: data not yet
persisted at death is lost, but it must be *visibly* lost.

### FL-3: Simulated hang (SIGSTOP) — the nasty case

A stopped FUSE server doesn't error; every filesystem op on the mount blocks
in uninterruptible sleep. This validates detection, not fast-fail.

1. `kx <client-A> "kill -STOP 1"` (or node-level `pkill -STOP -f '^client'`).
2. From a workload pod: `timeout 10 ls /data` → must time out (blocked).
3. Watch the liveness probe: `/api/health` (served by the same process) stops
   answering ⇒ kubelet kills and restarts the container after
   `periodSeconds × failureThreshold` (default config: 10 s × 3 = ~30 s).
4. Confirm restart happens WITHOUT manual `kill -CONT`, then recovery proceeds
   as in FL-1.
5. Repeat with `kill -STOP` on just long enough (< probe window) then
   `kill -CONT` — mount must resume with zero errors (transient stall only).

**Pass:** liveness probe converts a hang into a restart; blocked readers
unblock (with EIO) once the old FUSE connection is torn down by
`cleanupStaleMountpoint`; no permanently-D-state processes accumulate on the
node after recovery (`ps axo stat | grep -c D` returns to baseline).

### FL-4: Node-level daemon kill under CSI consumers

1. Run a workload pod consuming a CSI volume (session pinned via agent).
2. Kill client with `-9`; after restart, verify:
   - CSI bind mount inside the workload pod recovers or the pod's volume goes
     stale in a detectable way (`ls` errors, not hangs),
   - `session.Manager` state: new client process re-serves the agent socket
     (`/var/run/fuse-client/agent.sock` reconnects; csi-driver logs show
     successful re-`CreateSession` on next publish),
   - pinned prefixes survive: previously pinned files still present on NVMe.

**Pass:** no orphaned sessions wedge unmount; kubelet `NodeUnpublishVolume`
succeeds afterward (delete the workload pod and confirm clean teardown).

### FL-5: Kill during herd (combined)

Run TH-2's herd, and 5 s in, `kill -9` the lease-holding client (find it via
`fuse_fetch_lease_granted_total` delta).

**Pass:** the lease expires via TTL (~10 s), a follower's cloud fallback
(`fuse_fetch_lease_follower_cloud_fallback_total`) rescues the read, all
readers finish with correct md5. This proves a crashed leader delays the herd
by at most the lease TTL, never wedges it.

### FL-6: Rapid crash-loop resilience

`for i in 1 2 3 4 5; do kx <client-A> "kill -9 1"; sleep 25; done`

**Pass:** every cycle remounts successfully (no stale-mount accumulation:
`mount | grep -c fuse` on the node stays at 1); `kubectl describe pod` shows
no CrashLoopBackOff beyond expected restarts; peer/coordinator state
re-registers each time (peer list on coordinator returns node A as active).

---

## 5. Reporting Template

For each scenario record: date/cluster/image tag, before/after metric snapshot
diff, pass/fail against the criteria above, logs of anomalies, and cloud GET
counts where relevant. File failures as issues tagged `herd-control`,
`node-init`, or `fuse-lifecycle`.

Suggested order: 0 → NI-1 → TH-1..6 → IR-1..4 → FL-1..6 (FL last; it restarts
pods and perturbs the cluster).

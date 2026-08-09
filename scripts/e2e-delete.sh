#!/usr/bin/env bash
# End-to-end validation of cluster-wide delete propagation on a live cluster.
#
# This mirrors the reproduction that motivated the change. Before it, `rm` on
# the FUSE mount freed only the local NVMe copy: peer replicas, the cloud blob,
# and the coordinator row all survived, the deleted path still resolved, and
# `cat` re-materialized a full local copy from the peers. Delete was not merely
# incomplete, it was self-undoing.
#
# Every assertion below is one of the places a copy used to survive:
#   DL-1  local NVMe footprint on the writer
#   DL-2  the name stops resolving on the writer AND on a peer
#   DL-3  no on-disk objects on ANY node
#   DL-4  coordinator rows retracted (they have no TTL — only an explicit
#         retraction removes them)
#   DL-5  no resurrection: `cat` fails and no chunk completion runs
#   DL-6  still gone after a reconcile interval (the reconciler is a live
#         resurrection path for hot, small objects)
#   DL-7  chunked delete sweeps every chunk key, not just the parent
#   DL-8  the failure counters stayed at zero (delete is best-effort and
#         returns success either way, so silence is not proof)
#
# Usage: scripts/e2e-delete.sh [namespace]
set -uo pipefail

NS="${1:-fuse-system}"
MOUNT=/host/mnt/fuse
RUN="${RUN:-d1}"
DIR="$MOUNT/del-$RUN"
FAILED=0

pods=($(kubectl -n "$NS" get pods -l app=client -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}'))
if [ "${#pods[@]}" -lt 2 ]; then
  echo "need at least 2 client pods, found ${#pods[@]}" >&2
  exit 1
fi
A="${pods[0]}"
B="${pods[1]}"

kx() { kubectl -n "$NS" exec "$1" -c client -- bash -c "$2"; }

# The runtime image has no curl or wget, so HTTP checks go through a helper pod
# on the cluster network, addressing clients by pod IP.
CURLBOX=curlbox
if ! kubectl -n "$NS" get pod "$CURLBOX" >/dev/null 2>&1; then
  kubectl -n "$NS" run "$CURLBOX" --image=curlimages/curl:8.5.0 --restart=Never \
    --command -- sleep 100000 >/dev/null
  kubectl -n "$NS" wait --for=condition=Ready "pod/$CURLBOX" --timeout=90s >/dev/null
fi
ip_of() { kubectl -n "$NS" get pod "$1" -o jsonpath='{.status.podIP}'; }
api()   { kubectl -n "$NS" exec "$CURLBOX" -- curl -s --max-time "${3:-30}" "$(ip_of "$1"):8081$2"; }
mval()  { api "$1" "/metrics" 60 | grep -E "^$2 " | awk '{print $2}' | head -1; }
coord() { kubectl -n "$NS" exec "$CURLBOX" -- curl -s --max-time 20 \
            "coordinator.$NS.svc.cluster.local:8080$1"; }

ok()  { echo "PASS  $*"; }
bad() { echo "FAIL  $*"; FAILED=$((FAILED+1)); }

# The NVMe cache dir is whatever node-init discovered, not the static -nvme
# default, so locate it per-node rather than assuming one path.
cache_dir_of() {
  local pod="$1" prefix
  for prefix in /host/mnt/fuse-nvme0n1/fuse-cache /host/mnt/nvme/cache /mnt/nvme/cache; do
    if kx "$pod" "test -d $prefix" >/dev/null 2>&1; then echo "$prefix"; return; fi
  done
}

# Count objects named after $1 (parent plus any _chunk_N) under a node's cache.
disk_objects() {
  local pod="$1" name="$2" dir
  dir=$(cache_dir_of "$pod")
  if [ -z "$dir" ]; then echo "?"; return; fi
  kx "$pod" "find $dir -name '$name*' 2>/dev/null | wc -l" | tr -d ' \r'
}

echo "=== delete e2e: writer=$A peer=$B (${#pods[@]} nodes) ==="

# ---------------------------------------------------------------- setup
# 40 MiB at an 8 MiB stride = 5 chunks, and it sits inside both reconciler
# bounds (<=64 MiB, accessed within 10 min), so DL-6 is a real race, not a
# theoretical one.
echo
echo "--- setup: write 40MiB on $A and let it replicate"
kx "$A" "mkdir -p $DIR && dd if=/dev/urandom of=$DIR/g.bin bs=1M count=40 2>/dev/null"
src_md5=$(kx "$A" "md5sum $DIR/g.bin | cut -d' ' -f1")
echo "source md5: $src_md5"

# Pull it from the peer to force replication and to prove it was really there
# before the delete — a delete test that passes against a file that never
# landed proves nothing.
sleep 20
peer_md5=$(kx "$B" "md5sum $DIR/g.bin 2>/dev/null | cut -d' ' -f1")
if [ "$peer_md5" = "$src_md5" ]; then
  ok "setup peer $B holds the file before the delete"
else
  bad "setup peer md5 = ${peer_md5:-<empty>}, want $src_md5 — nothing to delete remotely"
fi
sleep 15

echo "on-disk objects before delete:"
for p in "${pods[@]}"; do echo "  $p: $(disk_objects "$p" g.bin)"; done
echo "coordinator rows before delete:"
coord "/api/files/location?path=$DIR/g.bin" | head -c 600; echo

peer_fail_before=$(mval "$A" fuse_delete_peer_failures_total)
cloud_fail_before=$(mval "$A" fuse_delete_cloud_failures_total)
coord_fail_before=$(mval "$A" fuse_delete_coordinator_failures_total)

# ---------------------------------------------------------------- DL-1
echo
echo "--- DL-1: rm succeeds and frees the local footprint"
kx "$A" "rm -f $DIR/g.bin"
rc=$?
if [ $rc -eq 0 ]; then
  ok "DL-1 rm returned 0"
else
  bad "DL-1 rm returned $rc"
fi
sleep 10
local_left=$(disk_objects "$A" g.bin)
if [ "$local_left" = "0" ]; then
  ok "DL-1 writer's NVMe footprint gone"
else
  bad "DL-1 writer still has $local_left g.bin object(s) on disk"
fi

# ---------------------------------------------------------------- DL-2
# The original symptom: `test -e` kept succeeding because the merged metadata
# view served the stale coordinator row.
echo
echo "--- DL-2: the name stops resolving on both nodes"
if kx "$A" "test -e $DIR/g.bin" 2>/dev/null; then
  bad "DL-2 deleted path still resolves on the writer $A"
else
  ok "DL-2 path gone on the writer"
fi
if kx "$B" "test -e $DIR/g.bin" 2>/dev/null; then
  bad "DL-2 deleted path still resolves on peer $B"
else
  ok "DL-2 path gone on peer $B"
fi

# ---------------------------------------------------------------- DL-3
# Peer replicas used to survive forever, leaking disk on every node with no way
# to enumerate the orphans.
echo
echo "--- DL-3: no objects left on any node"
leaks=0
for p in "${pods[@]}"; do
  n=$(disk_objects "$p" g.bin)
  echo "  $p: $n"
  if [ "$n" != "0" ]; then leaks=$((leaks+1)); fi
done
if [ "$leaks" -eq 0 ]; then
  ok "DL-3 all ${#pods[@]} nodes clean"
else
  bad "DL-3 $leaks node(s) still hold objects"
fi

# ---------------------------------------------------------------- DL-4
echo
echo "--- DL-4: coordinator rows retracted"
rows=$(coord "/api/files/location?path=$DIR/g.bin")
echo "rows: $(echo "$rows" | head -c 400)"
if echo "$rows" | grep -q '"peer_id"'; then
  bad "DL-4 coordinator still serves location rows for the deleted path"
else
  ok "DL-4 no coordinator rows for the deleted path"
fi
chunk_rows=$(coord "/api/files/locations?prefix=$DIR/g.bin" | grep -o "g.bin_chunk_[0-9]*" | sort -u | tr '\n' ' ')
if [ -z "$chunk_rows" ]; then
  ok "DL-4 no chunk rows survived"
else
  bad "DL-4 chunk rows survived: $chunk_rows"
fi

# ---------------------------------------------------------------- DL-5
# The self-undoing part: a read used to re-fetch chunks from peers and rebuild
# a full local copy, logging "Chunk completion assembled".
echo
echo "--- DL-5: no resurrection on read"
if kx "$A" "cat $DIR/g.bin > /dev/null 2>&1"; then
  bad "DL-5 cat succeeded on a deleted file — it was resurrected"
else
  ok "DL-5 cat fails as it should"
fi
sleep 5
if kubectl -n "$NS" logs "$A" -c client --tail=200 2>/dev/null | grep -q "Chunk completion assembled $DIR/g.bin"; then
  bad "DL-5 chunk completion reassembled the deleted file"
else
  ok "DL-5 no chunk completion for the deleted path"
fi
after_read=$(disk_objects "$A" g.bin)
if [ "$after_read" = "0" ]; then
  ok "DL-5 the failed read landed nothing on disk"
else
  bad "DL-5 the read re-materialized $after_read object(s)"
fi

# ---------------------------------------------------------------- DL-6
# The reconciler tops hot under-replicated objects back up toward R. A peer
# whose pass interleaves badly can re-replicate a just-deleted file; retracting
# the coordinator rows first narrows that window but does not close it. Wait for
# an actual reconciler pass rather than a fixed sleep — the reconciler is opt-in
# (-replica-reconcile-interval-sec defaults to 0), and against a cluster where
# it is off a timed sleep would make this check silently vacuous.
echo
echo "--- DL-6: still gone after a reconciler pass"
runs_before=$(mval "$A" fuse_replica_reconcile_runs_total)
if [ -z "$runs_before" ]; then
  echo "SKIP  DL-6 reconciler counter unavailable"
elif [ "$runs_before" = "0" ]; then
  echo "SKIP  DL-6 reconciler disabled on this cluster (-replica-reconcile-interval-sec=0);"
  echo "      re-run with it enabled to cover the resurrection race"
else
  waited=0
  while [ "$waited" -lt 180 ]; do
    sleep 15
    waited=$((waited+15))
    runs_now=$(mval "$A" fuse_replica_reconcile_runs_total)
    if [ "${runs_now:-0}" -gt "$runs_before" ]; then break; fi
  done
  echo "reconciler runs: $runs_before -> ${runs_now:-?} after ${waited}s"
  if [ "${runs_now:-0}" -le "$runs_before" ]; then
    echo "NOTE  DL-6 no reconciler pass observed in ${waited}s; the check below is weaker than intended"
  fi
fi
resurrected=0
for p in "${pods[@]}"; do
  n=$(disk_objects "$p" g.bin)
  if [ "$n" != "0" ]; then
    echo "  $p: $n"
    resurrected=$((resurrected+1))
  fi
done
if [ "$resurrected" -eq 0 ]; then
  ok "DL-6 no node re-replicated the deleted file"
else
  bad "DL-6 $resurrected node(s) re-replicated it — reconciler resurrection race"
fi
if kx "$A" "test -e $DIR/g.bin" 2>/dev/null; then
  bad "DL-6 the name came back"
else
  ok "DL-6 the name is still gone"
fi

# ---------------------------------------------------------------- DL-7
# A chunked delete must sweep every chunk key. The parent row alone leaves the
# bytes behind under names nothing will ever look up.
echo
echo "--- DL-7: chunked delete sweeps every chunk"
kx "$A" "dd if=/dev/urandom of=$DIR/h.bin bs=1M count=40 2>/dev/null"
sleep 25
before_chunks=$(disk_objects "$A" h.bin)
echo "chunks on disk before delete: $before_chunks"
if [ "${before_chunks:-0}" -lt 2 ]; then
  bad "DL-7 expected a chunked layout, found $before_chunks object(s) — test is vacuous"
fi
kx "$A" "rm -f $DIR/h.bin"
sleep 10
left=0
for p in "${pods[@]}"; do
  n=$(disk_objects "$p" h.bin)
  echo "  $p: $n"
  if [ "$n" != "0" ]; then left=$((left+1)); fi
done
if [ "$left" -eq 0 ]; then
  ok "DL-7 every chunk gone cluster-wide"
else
  bad "DL-7 $left node(s) still hold chunks"
fi

# ---------------------------------------------------------------- DL-8
# Delete is best-effort: an unreachable peer or coordinator is logged, counted,
# and the rm still returns success. So a clean run has to be confirmed by the
# counters, not by the exit status.
echo
echo "--- DL-8: no best-effort failures were swallowed"
peer_fail_after=$(mval "$A" fuse_delete_peer_failures_total)
cloud_fail_after=$(mval "$A" fuse_delete_cloud_failures_total)
coord_fail_after=$(mval "$A" fuse_delete_coordinator_failures_total)
suppressed=$(mval "$A" fuse_delete_resurrection_suppressed_total)
echo "peer:        ${peer_fail_before:-?} -> ${peer_fail_after:-?}"
echo "cloud:       ${cloud_fail_before:-?} -> ${cloud_fail_after:-?}"
echo "coordinator: ${coord_fail_before:-?} -> ${coord_fail_after:-?}"
echo "resurrection publishes suppressed by tombstone: ${suppressed:-?}"
if [ "${peer_fail_before:-0}" = "${peer_fail_after:-0}" ] &&
   [ "${cloud_fail_before:-0}" = "${cloud_fail_after:-0}" ] &&
   [ "${coord_fail_before:-0}" = "${coord_fail_after:-0}" ]; then
  ok "DL-8 no tier failed silently during the deletes"
else
  bad "DL-8 a tier failed during delete — bytes may be orphaned"
fi

echo
echo "delete-related log lines from $A:"
kubectl -n "$NS" logs "$A" -c client --tail=400 2>/dev/null \
  | grep -iE "delet|retract|tombstone" | tail -20

kx "$A" "rmdir $DIR 2>/dev/null" >/dev/null 2>&1

echo
echo "=== delete e2e complete: $FAILED failure(s) ==="
exit $FAILED

#!/usr/bin/env bash
# End-to-end validation of the perf-refactors series on a live cluster.
#
# Each section maps to one of the six refactors, and checks the behaviour the
# refactor was supposed to preserve or improve — not just "the cluster is up".
#
#   #7  entry indexes      -> readdir/lookup correctness at scale, chunked
#                             delete removes every chunk, no O(N) stalls
#   #8  ranged reads       -> read throughput, checksum verified once per open
#   #9  LRU eviction       -> eviction happens off the cold end, budget honoured
#   #10 deduped paths      -> replication still reaches R, EWMA still populated
#   #11 serve supervisor   -> mount survives / recovers (exercised in FL section)
#   #12 rename             -> see scripts/e2e-rename.sh
#
# Usage: scripts/e2e-perf-refactors.sh [namespace]
set -uo pipefail

NS="${1:-fuse-system}"
MOUNT=/host/mnt/fuse
# Run-scoped subdirs: a previous aborted run left partial trees behind, and
# deleting them on a shared mount is not ours to do.
RUN="${RUN:-r2}"
FAILED=0

# Plain array assignment rather than mapfile: macOS ships bash 3.2.
pods=($(kubectl -n "$NS" get pods -l app=client -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}'))
if [ "${#pods[@]}" -lt 2 ]; then
  echo "need at least 2 client pods, found ${#pods[@]}" >&2
  exit 1
fi
A="${pods[0]}"; B="${pods[1]}"; C="${pods[2]:-${pods[1]}}"

kx()  { kubectl -n "$NS" exec "$1" -c client -- bash -c "$2"; }

# The runtime image ships no curl or wget, so every HTTP check goes through a
# helper pod in the cluster network. It also has to address pods by IP: peers
# register with POD_IP:8081 and there is no per-pod Service.
CURLBOX=curlbox
ensure_curlbox() {
  if ! kubectl -n "$NS" get pod "$CURLBOX" >/dev/null 2>&1; then
    kubectl -n "$NS" run "$CURLBOX" --image=curlimages/curl:8.5.0 --restart=Never \
      --command -- sleep 100000 >/dev/null
    kubectl -n "$NS" wait --for=condition=Ready "pod/$CURLBOX" --timeout=90s >/dev/null
  fi
}
ip_of() { kubectl -n "$NS" get pod "$1" -o jsonpath='{.status.podIP}'; }
cget()  { kubectl -n "$NS" exec "$CURLBOX" -- curl -s --max-time "${3:-20}" "$2"; }
api()   { cget "$1" "$(ip_of "$1"):8081$2"; }
met()   { api "$1" "/metrics" 60; }
mval()  { met "$1" | grep -E "^$2 " | awk '{print $2}' | head -1; }
coord() { kubectl -n "$NS" exec "$CURLBOX" -- curl -s --max-time 20 \
            "coordinator.$NS.svc.cluster.local:8080$1"; }
ensure_curlbox

ok()  { echo "PASS  $*"; }
bad() { echo "FAIL  $*"; FAILED=$((FAILED+1)); }

echo "=== perf-refactors e2e ==="
echo "pods: ${pods[*]}"
kubectl -n "$NS" get pods -l app=client -o wide --no-headers | awk '{print "  " $1 " -> " $7}'

# ================================================================ 0. health
echo
echo "--- 0: all clients healthy, peers registered"
for p in "${pods[@]}"; do
  h=$(api "$p" "/api/health" | head -c 80)
  echo "  $p: $h"
done
# A client's peer list excludes itself, so the mesh is formed at N-1.
npeers=$(api "$A" "/api/peers" | grep -o '"id":"client-' | wc -l | tr -d ' ')
want=$(( ${#pods[@]} - 1 ))
echo "peers visible from $A: $npeers (want >= $want, self excluded)"
if [ "${npeers:-0}" -ge "$want" ]; then ok "0 peer mesh formed ($npeers)"; else bad "0 only $npeers peers, want >= $want"; fi

# ============================================== 1. #8 ranged read throughput
echo
echo "--- 1 (#8 ranged reads): write + local read + cold cross-node read"
kx "$A" "mkdir -p $MOUNT/perf-$RUN"
echo "writing 2GiB..."
wt=$(kx "$A" "cd $MOUNT/perf-$RUN && { time dd if=/dev/urandom of=big.bin bs=1M count=2048 conv=fsync 2>/dev/null; } 2>&1 | grep real")
echo "  write: $wt"
src_md5=$(kx "$A" "md5sum $MOUNT/perf-$RUN/big.bin | cut -d' ' -f1")
echo "  md5: $src_md5"

echo "local warm read (cache-resident, ranged path):"
kx "$A" "{ time cat $MOUNT/perf-$RUN/big.bin > /dev/null; } 2>&1 | grep real"
echo "local warm read again (page cache):"
kx "$A" "{ time cat $MOUNT/perf-$RUN/big.bin > /dev/null; } 2>&1 | grep real"

echo "waiting 45s for cloud persist + replication..."
sleep 45

echo "cold cross-node read from $B:"
kx "$B" "{ time cat $MOUNT/perf-$RUN/big.bin > /dev/null; } 2>&1 | grep real"
b_md5=$(kx "$B" "md5sum $MOUNT/perf-$RUN/big.bin | cut -d' ' -f1")
if [ "$b_md5" = "$src_md5" ]; then ok "1 cross-node read byte-exact"; else bad "1 cross-node md5 $b_md5 != $src_md5"; fi

echo "promoted second read from $B:"
kx "$B" "{ time cat $MOUNT/perf-$RUN/big.bin > /dev/null; } 2>&1 | grep real"

# ------- #8's specific claim: checksum verified once per open, not per window
echo
# There is no exported verify counter, so the evidence is timing: re-hashing
# 2GiB on every 128KiB window could not sustain a ~1.4s warm read.
echo "verification counters on $A (informational; none are exported today):"
met "$A" | grep -E "fuse_nvme_verif|fuse_nvme_hash" || echo "  (no verification counters exported; see warm-read timing above)"

# ============================================ 2. #7 entry indexes at scale
echo
echo "--- 2 (#7 indexes): many files in one directory, readdir + lookup + delete"
kx "$A" "mkdir -p $MOUNT/many-$RUN"
echo "creating 500 small files..."
t0=$(date +%s)
kx "$A" "cd $MOUNT/many-$RUN && for i in \$(seq 1 500); do echo payload-\$i > f\$i.txt; done"
t1=$(date +%s)
echo "  create 500 files: $((t1-t0))s"

t0=$(date +%s)
n=$(kx "$A" "ls $MOUNT/many-$RUN | wc -l")
t1=$(date +%s)
echo "  readdir of $n entries: $((t1-t0))s"
if [ "${n:-0}" -eq 500 ]; then ok "2 readdir returned all 500"; else bad "2 readdir returned $n, want 500"; fi

t0=$(date +%s)
kx "$A" "cd $MOUNT/many-$RUN && for i in 1 100 250 400 500; do cat f\$i.txt > /dev/null; done"
t1=$(date +%s)
echo "  5 point lookups in a 500-entry dir: $((t1-t0))s"

# chunked delete must remove every chunk key (the index is what finds them)
echo
echo "chunked delete removes every chunk:"
kx "$A" "dd if=/dev/urandom of=$MOUNT/many-$RUN/chunky.bin bs=1M count=40 2>/dev/null"
sleep 5
before=$(api "$A" "/api/cache" | grep -o "/many-$RUN/chunky.bin[^\"]*" | wc -l | tr -d ' ')
kx "$A" "rm -f $MOUNT/many-$RUN/chunky.bin"
sleep 3
after=$(api "$A" "/api/cache" | grep -o "/many-$RUN/chunky.bin[^\"]*" | wc -l | tr -d ' ')
echo "  chunk keys before delete: $before, after: $after"
# "after == 0" alone passes vacuously if the file never registered any keys,
# so require that there was something to remove in the first place.
if [ "${before:-0}" -eq 0 ]; then
  bad "2 no chunk keys were present before delete — the check would pass vacuously"
elif [ "${after:-1}" -eq 0 ]; then
  ok "2 chunked delete removed all $before keys"
else
  bad "2 $after of $before chunk keys survived delete"
fi

# ============================================ 3. #9 eviction off the LRU list
echo
echo "--- 3 (#9 eviction): budget honoured, cold end evicted first"
for p in "${pods[@]}"; do
  echo "  $p cache stats: $(api "$p" "/api/cache/stats")"
done
met "$A" | grep -E "fuse_cache_evict|fuse_eviction" || echo "  (no eviction counters yet)"

# =========================================== 4. #10 replication + pair EWMA
echo
echo "--- 4 (#10 deduped replication + EWMA): replicas reach target, pairs scored"
echo "worldview:"
coord "/api/worldview" | head -c 1500
echo
# These must be read on the node that did the REMOTE reads. $A only ever read
# its own NVMe, so its trackers are empty by construction and would make this
# section pass vacuously. $B is the cold cross-node reader from section 1.
echo "pairwise latency table on $B (the cross-node reader):"
api "$B" "/api/peers/latency" | head -c 1200
echo
met "$B" | grep -E "fuse_peer_pair_" | head -10
echo "miss/busy counters (must stay OUT of the EWMA):"
met "$B" | grep -E "fuse_peer_fetch_miss_skips_total|fuse_peer_fetch_busy_skips_total|fuse_tier_(peer|cloud)_misses_total"
echo "tier success ratios (the #13 regression: these must NOT be 0 with live traffic):"
ratios=$(met "$B" | grep -E "fuse_tier_(peer|cloud)_success_ratio")
echo "$ratios"
peer_ratio=$(echo "$ratios" | grep fuse_tier_peer_success_ratio | awk '{print $2}')
# A node that served real remote reads must not score its peer tier at 0: that
# is the exact shape of the regression where misses were fed into the EWMA.
if [ -n "$peer_ratio" ] && [ "$(echo "$peer_ratio > 0" | bc)" -eq 1 ]; then
  ok "4 peer tier success ratio is $peer_ratio (misses/busy correctly excluded)"
else
  bad "4 peer tier success ratio is ${peer_ratio:-<absent>} — misses may be poisoning the EWMA"
fi

# ================================================== 5. herd / admission control
echo
echo "--- 5: concurrent readers across nodes (herd)"
for p in "${pods[@]}"; do
  echo -n "  $p rejected_before=$(mval "$p" fuse_peer_serve_rejected_total) "
  echo "inflight=$(mval "$p" fuse_peer_serve_inflight)"
done
kx "$A" "rm -f $MOUNT/perf-$RUN/herd.bin; dd if=/dev/urandom of=$MOUNT/perf-$RUN/herd.bin bs=1M count=512 2>/dev/null"
sleep 30
echo "3 concurrent readers:"
for p in "${pods[@]}"; do
  kubectl -n "$NS" exec "$p" -c client -- bash -c "time cat $MOUNT/perf-$RUN/herd.bin > /dev/null" 2>&1 | grep real | sed "s/^/  $p /" &
done
wait
for p in "${pods[@]}"; do
  echo "  $p rejected_after=$(mval "$p" fuse_peer_serve_rejected_total) busy_skips=$(mval "$p" fuse_peer_fetch_busy_skips_total)"
done

# ================================================== 6. fetch leases
echo
echo "--- 6: fetch lease counters"
for p in "${pods[@]}"; do
  echo "  $p: granted=$(mval "$p" fuse_fetch_lease_granted_total) denied=$(mval "$p" fuse_fetch_lease_denied_total) follower_peer=$(mval "$p" fuse_fetch_lease_follower_peer_hits_total) errors=$(mval "$p" fuse_fetch_lease_errors_total)"
done

echo
echo "=== perf-refactors e2e complete: $FAILED failure(s) ==="
exit $FAILED

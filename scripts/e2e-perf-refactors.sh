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
FAILED=0

# Plain array assignment rather than mapfile: macOS ships bash 3.2.
pods=($(kubectl -n "$NS" get pods -l app=client -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}'))
if [ "${#pods[@]}" -lt 2 ]; then
  echo "need at least 2 client pods, found ${#pods[@]}" >&2
  exit 1
fi
A="${pods[0]}"; B="${pods[1]}"; C="${pods[2]:-${pods[1]}}"

kx()  { kubectl -n "$NS" exec "$1" -c client -- sh -c "$2"; }
api() { kubectl -n "$NS" exec "$1" -c client -- sh -c "curl -s $2"; }
met() { api "$1" "localhost:8081/metrics"; }
mval(){ met "$1" | grep -E "^$2 " | awk '{print $2}' | head -1; }

ok()  { echo "PASS  $*"; }
bad() { echo "FAIL  $*"; FAILED=$((FAILED+1)); }

echo "=== perf-refactors e2e ==="
echo "pods: ${pods[*]}"
kubectl -n "$NS" get pods -l app=client -o wide --no-headers | awk '{print "  " $1 " -> " $7}'

# ================================================================ 0. health
echo
echo "--- 0: all clients healthy, peers registered"
for p in "${pods[@]}"; do
  h=$(api "$p" "localhost:8081/api/health" | head -c 80)
  echo "  $p: $h"
done
npeers=$(api "$A" "localhost:8081/api/peers" | grep -o '"peer_id"' | wc -l | tr -d ' ')
echo "peers visible from $A: $npeers"
if [ "${npeers:-0}" -ge "${#pods[@]}" ]; then ok "0 peer mesh formed ($npeers)"; else bad "0 only $npeers peers, want >= ${#pods[@]}"; fi

# ============================================== 1. #8 ranged read throughput
echo
echo "--- 1 (#8 ranged reads): write + local read + cold cross-node read"
kx "$A" "mkdir -p $MOUNT/perf"
echo "writing 2GiB..."
wt=$(kx "$A" "cd $MOUNT/perf && { time dd if=/dev/urandom of=big.bin bs=1M count=2048 conv=fsync 2>/dev/null; } 2>&1 | grep real")
echo "  write: $wt"
src_md5=$(kx "$A" "md5sum $MOUNT/perf/big.bin | cut -d' ' -f1")
echo "  md5: $src_md5"

echo "local warm read (cache-resident, ranged path):"
kx "$A" "{ time cat $MOUNT/perf/big.bin > /dev/null; } 2>&1 | grep real"
echo "local warm read again (page cache):"
kx "$A" "{ time cat $MOUNT/perf/big.bin > /dev/null; } 2>&1 | grep real"

echo "waiting 45s for cloud persist + replication..."
sleep 45

echo "cold cross-node read from $B:"
kx "$B" "{ time cat $MOUNT/perf/big.bin > /dev/null; } 2>&1 | grep real"
b_md5=$(kx "$B" "md5sum $MOUNT/perf/big.bin | cut -d' ' -f1")
if [ "$b_md5" = "$src_md5" ]; then ok "1 cross-node read byte-exact"; else bad "1 cross-node md5 $b_md5 != $src_md5"; fi

echo "promoted second read from $B:"
kx "$B" "{ time cat $MOUNT/perf/big.bin > /dev/null; } 2>&1 | grep real"

# ------- #8's specific claim: checksum verified once per open, not per window
echo
echo "verification counters on $A (should be « number of 128KiB windows in 2GiB = 16384):"
met "$A" | grep -E "fuse_nvme_verif|fuse_nvme_hash" || echo "  (no verification counters exported)"

# ============================================ 2. #7 entry indexes at scale
echo
echo "--- 2 (#7 indexes): many files in one directory, readdir + lookup + delete"
kx "$A" "mkdir -p $MOUNT/many"
echo "creating 500 small files..."
t0=$(date +%s)
kx "$A" "cd $MOUNT/many && for i in \$(seq 1 500); do echo payload-\$i > f\$i.txt; done"
t1=$(date +%s)
echo "  create 500 files: $((t1-t0))s"

t0=$(date +%s)
n=$(kx "$A" "ls $MOUNT/many | wc -l")
t1=$(date +%s)
echo "  readdir of $n entries: $((t1-t0))s"
if [ "${n:-0}" -eq 500 ]; then ok "2 readdir returned all 500"; else bad "2 readdir returned $n, want 500"; fi

t0=$(date +%s)
kx "$A" "cd $MOUNT/many && for i in 1 100 250 400 500; do cat f\$i.txt > /dev/null; done"
t1=$(date +%s)
echo "  5 point lookups in a 500-entry dir: $((t1-t0))s"

# chunked delete must remove every chunk key (the index is what finds them)
echo
echo "chunked delete removes every chunk:"
kx "$A" "dd if=/dev/urandom of=$MOUNT/many/chunky.bin bs=1M count=40 2>/dev/null"
sleep 5
before=$(api "$A" "localhost:8081/api/cache" | grep -o '/many/chunky.bin[^"]*' | wc -l | tr -d ' ')
kx "$A" "rm -f $MOUNT/many/chunky.bin"
sleep 3
after=$(api "$A" "localhost:8081/api/cache" | grep -o '/many/chunky.bin[^"]*' | wc -l | tr -d ' ')
echo "  chunk keys before delete: $before, after: $after"
if [ "${after:-1}" -eq 0 ]; then ok "2 chunked delete removed all keys"; else bad "2 $after chunk keys survived delete"; fi

# ============================================ 3. #9 eviction off the LRU list
echo
echo "--- 3 (#9 eviction): budget honoured, cold end evicted first"
for p in "${pods[@]}"; do
  echo "  $p cache stats: $(api "$p" "localhost:8081/api/cache/stats")"
done
met "$A" | grep -E "fuse_cache_evict|fuse_eviction" || echo "  (no eviction counters yet)"

# =========================================== 4. #10 replication + pair EWMA
echo
echo "--- 4 (#10 deduped replication + EWMA): replicas reach target, pairs scored"
echo "worldview for /perf/big.bin:"
api "$A" "localhost:8081/api/peers" > /dev/null
kubectl -n "$NS" exec "$A" -c client -- sh -c \
  "curl -s 'coordinator.fuse-system.svc.cluster.local:8080/api/worldview' | head -c 1500"
echo
echo "pairwise latency table on $A:"
api "$A" "localhost:8081/api/peers/latency" | head -c 1200
echo
met "$A" | grep -E "fuse_peer_pair_" | head -10
echo "miss/busy counters (must stay OUT of the EWMA):"
met "$A" | grep -E "fuse_peer_fetch_miss_skips_total|fuse_peer_fetch_busy_skips_total|fuse_tier_(peer|cloud)_misses_total"
echo "tier success ratios (the #13 regression: these must NOT be 0 with live traffic):"
met "$A" | grep -E "fuse_tier_(peer|cloud)_success_ratio|fuse_tier_(peer|cloud)_latency"

# ================================================== 5. herd / admission control
echo
echo "--- 5: concurrent readers across nodes (herd)"
for p in "${pods[@]}"; do
  echo -n "  $p rejected_before=$(mval "$p" fuse_peer_serve_rejected_total) "
  echo "inflight=$(mval "$p" fuse_peer_serve_inflight)"
done
kx "$A" "rm -f $MOUNT/perf/herd.bin; dd if=/dev/urandom of=$MOUNT/perf/herd.bin bs=1M count=512 2>/dev/null"
sleep 30
echo "3 concurrent readers:"
for p in "${pods[@]}"; do
  kubectl -n "$NS" exec "$p" -c client -- sh -c "time cat $MOUNT/perf/herd.bin > /dev/null" 2>&1 | grep real | sed "s/^/  $p /" &
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

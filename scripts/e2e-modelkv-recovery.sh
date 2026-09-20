#!/usr/bin/env bash
# Live-cluster recovery validation for the durable Model KV store.
#
# The store's claim is narrow and worth stating precisely: a KV block that has
# demonstrated reuse survives the loss of the node that created it. Everything
# here tests that claim or the policy that bounds its cost. A test that only
# restarted a process would pass while the claim was false, because a restart
# leaves the disk — and therefore the journal and the blocks — intact.
#
#   1  write path is local           N puts cause zero cloud writes
#   2  durability is earned          a block promotes only after reuse, and
#                                    many promoted blocks become few objects
#   3  restart recovery              pod killed, same node: index rebuilt from
#                                    the on-disk journal, blocks readable
#   4  NODE-LOSS recovery            cache wiped AND a different node: blocks
#                                    come back from the cloud catalog
#   5  non-durable blocks stay lost  a never-reused block must NOT reappear;
#                                    a store that "recovered" it would be
#                                    lying about what durability it provides
#   6  corruption is loud            a tampered batch fails closed rather than
#                                    returning wrong KV bytes
#   7  TTL reaping                   the sweep runs and does not eat live data
#
# Deploy first: kubectl apply -f scripts/modelkv-testds.yaml
# Usage:        scripts/e2e-modelkv-recovery.sh [namespace]
# Env:          RUN=<suffix>  run-scoped key prefix (default: kvrec1)
#               KEEP=1        leave test blocks and the curlbox behind
#               APP=<label>   client app label (default: client-kvtest, the
#                             side-by-side DaemonSet from modelkv-testds.yaml;
#                             set APP=client to run against production clients)
#               PORT=<n>      client API port (default: 8091)
#               CACHE=<path>  in-pod NVMe cache dir; default reads node-init,
#                             falling back to the -nvme flag's /mnt/nvme-kvtest
set -uo pipefail

NS="${1:-fuse-system}"
RUN="${RUN:-kvrec1}"
KEEP="${KEEP:-0}"
APP="${APP:-client-kvtest}"
PORT="${PORT:-8091}"
FAILED=0

ok()   { echo "PASS  $*"; }
bad()  { echo "FAIL  $*"; FAILED=$((FAILED+1)); }
skip() { echo "SKIP  $*"; }
note() { echo "      $*"; }

# Plain array assignment rather than mapfile: macOS ships bash 3.2.
pods=($(kubectl -n "$NS" get pods -l "app=$APP" --field-selector=status.phase=Running \
        -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}'))
if [ "${#pods[@]}" -lt 2 ]; then
  echo "need at least 2 running pods with app=$APP, found ${#pods[@]}" >&2
  exit 1
fi
A="${pods[0]}"; B="${pods[1]}"

kx() { kubectl -n "$NS" exec "$1" -c client -- bash -c "$2"; }

# The runtime image ships no curl or wget, so HTTP goes through a helper pod on
# the cluster network, addressing clients by pod IP: peers register as
# POD_IP:8081 and there is no per-pod Service.
CURLBOX=kvcurlbox
if ! kubectl -n "$NS" get pod "$CURLBOX" >/dev/null 2>&1; then
  kubectl -n "$NS" run "$CURLBOX" --image=curlimages/curl:8.5.0 --restart=Never \
    --command -- sleep 100000 >/dev/null
fi
kubectl -n "$NS" wait --for=condition=Ready "pod/$CURLBOX" --timeout=180s >/dev/null

ip_of()   { kubectl -n "$NS" get pod "$1" -o jsonpath='{.status.podIP}'; }
node_of() { kubectl -n "$NS" get pod "$1" -o jsonpath='{.spec.nodeName}'; }

api()  { kubectl -n "$NS" exec "$CURLBOX" -- curl -s --max-time "${3:-30}" "$(ip_of "$1"):${PORT}$2"; }
kvput() {
  # $1 pod  $2 key  $3 prefix  $4 body
  kubectl -n "$NS" exec "$CURLBOX" -- curl -s --max-time 60 -X PUT \
    --data-binary "$4" "$(ip_of "$1"):${PORT}/api/kv/blocks/$2?prefix=$3"
}
kvget()  { kubectl -n "$NS" exec "$CURLBOX" -- curl -s --max-time 60 "$(ip_of "$1"):${PORT}/api/kv/blocks/$2"; }
kvpost() { kubectl -n "$NS" exec "$CURLBOX" -- curl -s --max-time 120 -X POST "$(ip_of "$1"):${PORT}/api/kv$2"; }
kvstats() { api "$1" "/api/kv/stats"; }

pyget() {
  python3 -c '
import json, sys
try:
    d = json.load(sys.stdin)
except Exception:
    sys.exit(0)
try:
    v = eval(sys.argv[1])
except Exception:
    sys.exit(0)
print("" if v is None else v)
' "$1"
}

# node-init discovers the cache dir per node, so it must be read rather than
# assumed: a hardcoded path silently no-ops every wipe in this script, and the
# sections would then pass for the wrong reason.
cache_dir_of() {
  if [ -n "${CACHE:-}" ]; then echo "$CACHE"; return; fi
  local hostdir
  hostdir=$(kx "$1" "cat /var/run/fuse-client/node-init.json 2>/dev/null" | pyget 'd["cache_dir"]')
  if [ -n "$hostdir" ]; then echo "/host${hostdir}"; return; fi
  # No node-init config: read the -nvme flag off the running process rather
  # than guessing, since a wrong path makes every wipe a silent no-op and the
  # recovery sections would then pass without proving anything.
  local flagdir
  flagdir=$(kx "$1" "tr '\\0' '\\n' < /proc/1/cmdline 2>/dev/null | grep -A1 -x -- -nvme | tail -1" 2>/dev/null | tr -d '\r')
  if [ -n "$flagdir" ]; then echo "$flagdir"; else echo "/mnt/nvme/cache"; fi
}

# wipe_kv POD DIR — clear only this store's subtree. Scoped to the kv prefix so
# a mistyped variable cannot take out the node's whole cache.
wipe_kv() {
  local pod="$1" dir="$2"
  case "$dir" in
    /host/mnt/*|/mnt/*) ;;
    *) echo "refusing to wipe unexpected cache dir: $dir" >&2; return 1 ;;
  esac
  kx "$pod" "find ${dir}/kv -mindepth 1 -delete 2>/dev/null; true" >/dev/null 2>&1
}

# wait_pod_ready NODE OLDPOD — the DaemonSet reschedules a killed pod onto the
# same node; wait for the replacement and echo its name.
#
# The old pod must be excluded by name and by deletionTimestamp: a terminating
# pod still reports status.phase=Running, so selecting .items[0] returns the
# corpse. Every later call then targets a pod that is about to vanish, and the
# section fails with "pods not found" rather than testing recovery.
wait_pod_ready() {
  local node="$1" old="${2:-}" deadline=$((SECONDS+300)) p
  while [ $SECONDS -lt $deadline ]; do
    p=$(kubectl -n "$NS" get pods -l "app=$APP" --field-selector="spec.nodeName=${node}" \
        -o go-template='{{range .items}}{{if not .metadata.deletionTimestamp}}{{if eq .status.phase "Running"}}{{.metadata.name}}{{"\n"}}{{end}}{{end}}{{end}}' 2>/dev/null \
        | grep -v "^${old}$" | head -1)
    if [ -n "$p" ]; then
      if kubectl -n "$NS" get pod "$p" -o jsonpath='{.status.containerStatuses[*].ready}' 2>/dev/null | grep -qv false; then
        echo "$p"; return 0
      fi
    fi
    sleep 5
  done
  return 1
}

# The KV API must answer before assertions run: the container is Ready as soon
# as the process is up, but RecoverIndex runs before the listener serves.
wait_kv_ready() {
  local pod="$1" deadline=$((SECONDS+180))
  while [ $SECONDS -lt $deadline ]; do
    if [ -n "$(kvstats "$pod" | pyget 'd["blocks"]')" ]; then return 0; fi
    sleep 3
  done
  return 1
}

PREFIX="conv-${RUN}"

echo "=== Model KV store: durability + recovery e2e ==="
echo "namespace: $NS   app: $APP   port: $PORT   prefix: $PREFIX"
echo "A = $A on $(node_of "$A")"
echo "B = $B on $(node_of "$B")"

# Fail fast and clearly if the store is not enabled, rather than reporting
# seven mysterious failures.
probe=$(kvstats "$A")
if [ -z "$probe" ] || [ -z "$(echo "$probe" | pyget 'd["blocks"]')" ]; then
  echo
  echo "Model KV store is not answering on $A." >&2
  echo "Deploy the side-by-side test DaemonSet with:" >&2
  echo "  kubectl apply -f scripts/modelkv-testds.yaml" >&2
  echo "or enable it on the production clients with:" >&2
  echo "  WITH_MODEL_KV=1 scripts/deploy-ckpt2404.sh $NS   (rolls every client pod)" >&2
  exit 1
fi

CACHE_A=$(cache_dir_of "$A")
CACHE_B=$(cache_dir_of "$B")
echo "cache dirs: A=$CACHE_A  B=$CACHE_B"

# A KV block is megabytes in production. A few KB here keeps the run quick;
# the code paths under test do not branch on size.
BLOCK=$(python3 -c 'print("k" * 4096)')

# ===================================================== 1. write path is local
echo
echo "--- 1: puts do not touch cloud storage"
before=$(kvstats "$A" | pyget 'd["cloud_writes"]')
for i in $(seq 0 9); do
  kvput "$A" "${PREFIX}-blk${i}" "$PREFIX" "$BLOCK" >/dev/null
done
after=$(kvstats "$A" | pyget 'd["cloud_writes"]')
if [ "$before" = "$after" ]; then
  ok "10 puts caused 0 cloud writes (cloud_writes stayed at $before)"
  note "the weights path would have issued 10 uploads plus 3x peer replication"
else
  bad "cloud_writes moved $before -> $after during puts; the write path is not local-only"
fi

# ================================================== 2. durability is earned
echo
echo "--- 2: promotion requires reuse, and promoted blocks coalesce"
# One read each: below the default promote-after-reads of 2.
for i in $(seq 0 9); do kvget "$A" "${PREFIX}-blk${i}" >/dev/null; done
mid=$(kvstats "$A" | pyget 'd["durable_blocks"]')
if [ "$mid" = "0" ]; then
  ok "after 1 read each, 0 blocks are durable (promotion threshold not met)"
else
  bad "durable_blocks=$mid after a single read; promotion is firing too early"
fi

# Second read each: crosses the threshold and enqueues for durability.
for i in $(seq 0 9); do kvget "$A" "${PREFIX}-blk${i}" >/dev/null; done
w_before=$(kvstats "$A" | pyget 'd["cloud_writes"]')
kvpost "$A" "/flush?prefix=$PREFIX" >/dev/null
st=$(kvstats "$A")
dur=$(echo "$st" | pyget 'd["durable_blocks"]')
w_after=$(echo "$st" | pyget 'd["cloud_writes"]')
delta=$((w_after - w_before))
if [ "$dur" -ge 10 ] 2>/dev/null; then
  ok "10 reused blocks promoted to durable (durable_blocks=$dur)"
else
  bad "durable_blocks=$dur after reuse + flush, want >=10"
fi
# One batch object + one catalog entry, independent of block count. That ratio
# is the write amplification the design exists to remove.
if [ "$delta" -ge 1 ] && [ "$delta" -le 2 ]; then
  ok "10 blocks became $delta cloud objects (batch + catalog)"
else
  bad "flush issued $delta cloud writes for 10 blocks, want 2"
fi

# ================================================== 3. restart recovery
echo
echo "--- 3: daemon restart recovers the index from the on-disk journal"
NODE_A=$(node_of "$A")
kubectl -n "$NS" delete pod "$A" --wait=false >/dev/null 2>&1
note "deleted $A; waiting for the DaemonSet replacement on $NODE_A"
A2=$(wait_pod_ready "$NODE_A" "$A")
if [ -z "$A2" ]; then
  bad "no ready client pod returned on $NODE_A within 300s"
elif ! wait_kv_ready "$A2"; then
  bad "replacement pod $A2 is Ready but the KV API never answered"
else
  note "replacement pod: $A2"
  # The daemon recovers at startup; no explicit call needed. Poll rather than
  # sleep, so the assertion is on observed state and not on a guessed delay.
  deadline=$((SECONDS+120)); blocks=""
  while [ $SECONDS -lt $deadline ]; do
    blocks=$(kvstats "$A2" | pyget 'd["blocks"]')
    [ -n "$blocks" ] && [ "$blocks" != "0" ] && break
    sleep 5
  done
  if [ -n "$blocks" ] && [ "$blocks" != "0" ]; then
    ok "index rebuilt on restart without an explicit call (blocks=$blocks)"
  else
    bad "index is empty after restart; the journal walk did not run"
  fi
  got=$(kvget "$A2" "${PREFIX}-blk3" | wc -c | tr -d ' ')
  if [ "$got" = "4096" ]; then
    ok "block bytes readable through the recovered index ($got bytes)"
  else
    bad "recovered block read returned $got bytes, want 4096"
  fi
  A="$A2"
fi

# ============================================ 4. node-loss recovery (the claim)
echo
echo "--- 4: NODE LOSS — a different node with no local state serves the blocks"
# This is the section that tests the actual durability claim. Node B has never
# seen these keys and its KV subtree is wiped, so anything it serves came from
# cloud.
note "wiping KV state on B ($(node_of "$B")) so nothing local can satisfy the read"
wipe_kv "$B" "$CACHE_B"
rec=$(kvpost "$B" "/recover?mode=cloud&prefix=$PREFIX")
n=$(echo "$rec" | pyget 'd["recovered"]')
if [ -n "$n" ] && [ "$n" -ge 10 ] 2>/dev/null; then
  ok "B recovered $n durable blocks from the cloud catalog with no local state"
else
  bad "B recovered '${n:-<none>}' blocks from cloud, want >=10"
  note "response: $(echo "$rec" | head -c 300)"
fi
body=$(kvget "$B" "${PREFIX}-blk7")
blen=$(printf '%s' "$body" | wc -c | tr -d ' ')
if [ "$blen" = "4096" ]; then
  ok "B served block bytes it never wrote ($blen bytes) — durability claim holds"
else
  bad "B read returned $blen bytes, want 4096"
fi
if [ "$(printf '%s' "$body" | tr -d 'k' | wc -c | tr -d ' ')" = "0" ]; then
  ok "recovered bytes match what was written (checksum-verified on decode)"
else
  bad "recovered bytes differ from what was written"
fi

# ==================================== 5. non-durable blocks must stay lost
echo
echo "--- 5: a never-reused block does NOT come back (no false durability)"
COLD="${PREFIX}-cold"
kvput "$A" "$COLD" "$PREFIX" "$BLOCK" >/dev/null
# Never read it, so it never earns promotion. Flush to prove the flush does not
# sweep up unearned blocks.
kvpost "$A" "/flush?prefix=$PREFIX" >/dev/null
wipe_kv "$B" "$CACHE_B"
kvpost "$B" "/recover?mode=cloud&prefix=$PREFIX" >/dev/null
cold_body=$(kvget "$B" "$COLD")
cl=$(printf '%s' "$cold_body" | wc -c | tr -d ' ')
if [ "$cl" = "4096" ]; then
  bad "B served a block that was never promoted; the store over-claims durability"
else
  ok "unearned block is absent on B, as its durability level implies"
fi

# ==================================== 6. corruption fails closed
echo
echo "--- 6: a tampered batch fails closed rather than serving wrong KV"
# Wrong KV bytes produce wrong tokens with no error anywhere, so the only safe
# behavior is a loud failure. Batch objects live in cloud storage, and the
# runtime image carries no blob CLI, so tampering with one from inside the
# cluster is not reachable here. Reported as a skip rather than quietly
# omitted: the property is covered by TestBatchDetectsCorruption, which flips a
# bit in an encoded batch and asserts decode fails with a checksum error.
skip "in-cluster batch tampering needs a blob CLI; covered by TestBatchDetectsCorruption"

# ==================================== 7. TTL reaping
echo
echo "--- 7: TTL reaping bounds the tier without eating live data"
# Force a sweep with the TTL as deployed. Blocks just written are inside the
# window, so a correct reaper removes none of them — asserting that it does not
# eat live data matters as much as asserting that it runs.
before_blocks=$(kvstats "$A" | pyget 'd["blocks"]')
reap=$(kvpost "$A" "/reap")
reaped=$(echo "$reap" | pyget 'd["reaped"]')
after_blocks=$(echo "$reap" | pyget 'd["stats"]["blocks"]')
if [ "$reaped" = "0" ] && [ "$before_blocks" = "$after_blocks" ]; then
  ok "sweep ran and removed 0 fresh blocks (blocks=$after_blocks)"
elif [ -n "$reaped" ]; then
  bad "sweep removed $reaped blocks that are inside the TTL window"
else
  bad "reap endpoint did not answer"
fi

# ==================================== cleanup
if [ "$KEEP" != "1" ]; then
  wipe_kv "$A" "$CACHE_A"
  wipe_kv "$B" "$CACHE_B"
  kubectl -n "$NS" delete pod "$CURLBOX" --wait=false >/dev/null 2>&1
fi

echo
echo "=== $FAILED failure(s) ==="
exit "$FAILED"

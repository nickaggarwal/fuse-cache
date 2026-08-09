#!/usr/bin/env bash
# Live-cluster validation of the declarative-cache + routing-signal work
# (reconciled warmup, PeerInfo.NodeName, residency query).
#
# Each section checks the property the code exists to guarantee, not just that
# the endpoint answers:
#
#   1  peer mesh + NodeName        an external router can join a peer to a Node
#   2  measured, not claimed       residency stats the disk; location metadata
#                                  over-claims after eviction and must diverge
#   3  byte-weighted               half the bytes is not half the files
#   4  unknown is not zero         a peer that cannot be asked reports unknown
#   5  TTL cache + coalescing      a polling router does not storm the peers
#   6a reconciled warmup (HTTP)    a deleted file comes back and last_verified
#                                  advances; declared via /api/cache/intents
#   6b reconciled warmup (CSI)     the same, through the production entrance
#                                  (needs a CSI node DaemonSet; WITH_CSI=1)
#   7  no-NODE_NAME compatibility  an older peer still registers and is selectable
#
# Deploy first: scripts/deploy-ckpt2404.sh [namespace]
# Usage:        scripts/e2e-residency.sh [namespace]
# Env:          RUN=<suffix>   run-scoped subdir (default: resid1)
#               KEEP=1         leave the test tree and CSI pod behind
set -uo pipefail

NS="${1:-fuse-system}"
MOUNT=/host/mnt/fuse
# Run-scoped: an aborted previous run leaves partial trees, and deleting
# someone else's data on a shared mount is not ours to do.
RUN="${RUN:-resid1}"
DIR="/e2e-${RUN}"
KEEP="${KEEP:-0}"
FAILED=0

ok()   { echo "PASS  $*"; }
bad()  { echo "FAIL  $*"; FAILED=$((FAILED+1)); }
skip() { echo "SKIP  $*"; }
note() { echo "      $*"; }

# Plain array assignment rather than mapfile: macOS ships bash 3.2.
pods=($(kubectl -n "$NS" get pods -l app=client -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}'))
if [ "${#pods[@]}" -lt 2 ]; then
  echo "need at least 2 client pods, found ${#pods[@]}" >&2
  exit 1
fi
A="${pods[0]}"; B="${pods[1]}"

kx() { kubectl -n "$NS" exec "$1" -c client -- bash -c "$2"; }

# The runtime image ships no curl or wget, so HTTP checks go through a helper
# pod on the cluster network, addressing clients by pod IP: peers register as
# POD_IP:8081 and there is no per-pod Service.
CURLBOX=curlbox
if ! kubectl -n "$NS" get pod "$CURLBOX" >/dev/null 2>&1; then
  kubectl -n "$NS" run "$CURLBOX" --image=curlimages/curl:8.5.0 --restart=Never \
    --command -- sleep 100000 >/dev/null
fi
kubectl -n "$NS" wait --for=condition=Ready "pod/$CURLBOX" --timeout=120s >/dev/null

ip_of() { kubectl -n "$NS" get pod "$1" -o jsonpath='{.status.podIP}'; }
node_of() { kubectl -n "$NS" get pod "$1" -o jsonpath='{.spec.nodeName}'; }
api()   { kubectl -n "$NS" exec "$CURLBOX" -- curl -s --max-time "${3:-20}" "$(ip_of "$1"):8081$2"; }
coord() { kubectl -n "$NS" exec "$CURLBOX" -- curl -s --max-time "${2:-20}" \
            "coordinator.$NS.svc.cluster.local:8080$1"; }
coordpost() { kubectl -n "$NS" exec "$CURLBOX" -- curl -s --max-time "${3:-30}" \
            -X POST -H 'Content-Type: application/json' -d "$2" \
            "coordinator.$NS.svc.cluster.local:8080$1"; }
apipost() { kubectl -n "$NS" exec "$CURLBOX" -- curl -s --max-time "${4:-120}" \
            -X POST -H 'Content-Type: application/json' -d "$3" \
            "$(ip_of "$1"):8081$2"; }
mval() { api "$1" "/metrics" 60 | grep -E "^$2 " | awk '{print $2}' | head -1; }

# pyget EXPR — evaluate a python expression against the JSON on stdin, where
# `d` is the decoded document. Empty output on any failure so callers can test
# for it rather than crashing mid-run.
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

# The NVMe cache dir is discovered per node by node-init, so it must be read
# rather than assumed — hardcoding /mnt/nvme/cache silently no-ops the eviction
# sections on any node where discovery picked something else.
cache_dir_of() {
  local hostdir
  hostdir=$(kx "$1" "cat /var/run/fuse-client/node-init.json 2>/dev/null" | pyget 'd["cache_dir"]')
  if [ -n "$hostdir" ]; then echo "/host${hostdir}"; else echo "/mnt/nvme/cache"; fi
}

echo "=== residency / declarative-cache e2e ==="
echo "namespace: $NS   run dir: $DIR"
echo "A = $A on $(node_of "$A")"
echo "B = $B on $(node_of "$B")"
CACHE_A=$(cache_dir_of "$A"); CACHE_B=$(cache_dir_of "$B")
echo "cache dirs: A=$CACHE_A  B=$CACHE_B"

# ============================================== 1. peer mesh + node identity
echo
echo "--- 1: peer mesh formed, peers carry NodeName (Phase 2)"
peers_json=$(api "$A" "/api/peers")
npeers=$(printf '%s' "$peers_json" | pyget 'len(d)')
want=$(( ${#pods[@]} - 1 ))   # a client's peer list excludes itself
if [ "${npeers:-0}" -ge "$want" ]; then ok "1 peer mesh formed ($npeers peers, self excluded)"
else bad "1 only ${npeers:-0} peers visible from $A, want >= $want"; fi

named=$(printf '%s' "$peers_json" | pyget 'sum(1 for p in d if p.get("node_name"))')
# NodeName is the join key an external scheduler needs; without it the whole
# residency answer is un-actionable from a Kubernetes scheduler's view.
if [ "${named:-0}" -ge 1 ]; then
  ok "1 NodeName populated on ${named}/${npeers} peers"
  printf '%s' "$peers_json" | pyget '"      " + "\n      ".join(p["id"]+" -> "+p.get("node_name","(none)") for p in d)'
else
  bad "1 no peer carries node_name — Phase 2 not in the running image"
fi

# ==================================== 2/3. measured residency, byte-weighted
echo
echo "--- 2: residency measures the disk, not coordinator metadata"
# Two files with a 1:3 byte split so a byte-weighted answer (0.75) is
# distinguishable from a file-counted one (0.5) in section 3.
kx "$A" "mkdir -p $MOUNT$DIR"
kx "$A" "dd if=/dev/urandom of=$MOUNT$DIR/small.bin bs=1M count=4 conv=fsync 2>/dev/null"
kx "$A" "dd if=/dev/urandom of=$MOUNT$DIR/large.bin bs=1M count=12 conv=fsync 2>/dev/null"
sleep 5

rep_a=$(api "$A" "/api/cache/residency?prefix=$DIR")
files=$(printf '%s' "$rep_a" | pyget 'd["files"]')
res_f=$(printf '%s' "$rep_a" | pyget 'd["resident_files"]')
frac=$(printf '%s' "$rep_a" | pyget 'round(d["fraction"],3)')
note "A: files=$files resident=$res_f fraction=$frac"
if [ "${files:-0}" -eq 2 ] && [ "${res_f:-0}" -eq 2 ]; then
  ok "2 writer node reports both files resident"
else
  bad "2 writer node reports files=${files:-?} resident=${res_f:-?}, want 2/2"
fi

# The load-bearing check. Remove one file straight off NVMe, the way LRU
# eviction does — no delete goes through the coordinator, so its location rows
# still claim the node holds it. Measured residency must drop; metadata must
# not. If both move, the endpoint is reading metadata and is worthless as a
# routing signal.
locs_before=$(coord "/api/files/locations?prefix=$DIR" | pyget 'sum(1 for l in d if l["file_path"].startswith("'"$DIR"'/small.bin"))')
kx "$A" "rm -f $CACHE_A$DIR/small.bin"
rep_a2=$(api "$A" "/api/cache/residency?prefix=$DIR")
res_f2=$(printf '%s' "$rep_a2" | pyget 'd["resident_files"]')
frac2=$(printf '%s' "$rep_a2" | pyget 'round(d["fraction"],3)')
locs_after=$(coord "/api/files/locations?prefix=$DIR" | pyget 'sum(1 for l in d if l["file_path"].startswith("'"$DIR"'/small.bin"))')
note "after evicting small.bin: resident=$res_f2 fraction=$frac2 | location rows ${locs_before:-?} -> ${locs_after:-?}"
if [ "${res_f2:-9}" -eq 1 ]; then
  ok "2 measured residency dropped after an off-metadata eviction"
else
  bad "2 measured residency = ${res_f2:-?} after evicting 1 of 2 files, want 1"
fi
if [ "${locs_after:-0}" -ge 1 ] && [ "${locs_after:-0}" -eq "${locs_before:-0}" ]; then
  ok "2 coordinator metadata still claims the evicted file (the over-claim this endpoint corrects)"
else
  note "2 coordinator rows changed ${locs_before:-?} -> ${locs_after:-?}; over-claim not demonstrated (informational)"
fi

echo
echo "--- 3: residency is byte-weighted, not file-counted"
# 12MiB of 16MiB remain: 0.75 byte-weighted vs 0.5 file-counted.
if [ -n "$frac2" ] && python3 -c "import sys; sys.exit(0 if 0.70 <= $frac2 <= 0.80 else 1)"; then
  ok "3 fraction=$frac2 (~0.75 byte-weighted; a file count would read 0.5)"
elif [ -n "$frac2" ] && python3 -c "import sys; sys.exit(0 if 0.45 <= $frac2 <= 0.55 else 1)"; then
  bad "3 fraction=$frac2 looks file-counted (0.5), want ~0.75 byte-weighted"
else
  bad "3 fraction=${frac2:-?}, want ~0.75"
fi

# ====================================== 4. cluster fan-out ranks, unknown!=0
echo
echo "--- 4: coordinator fan-out ranks nodes; a node it cannot ask is unknown"
# Register a synthetic peer at an unroutable address. It is deliberately
# registered WITHOUT node_name, so this doubles as section 7's backward
# -compatibility check. Advisory state only, and the lease expires on its own.
GHOST="e2e-ghost-${RUN}"
coordpost "/api/peers/register" \
  "{\"id\":\"$GHOST\",\"address\":\"127.0.0.1:1\",\"status\":\"active\",\"available_space\":1,\"used_space\":0}" >/dev/null
sleep 1

clu=$(coord "/api/residency?prefix=$DIR" 30)
n_nodes=$(printf '%s' "$clu" | pyget 'len(d["nodes"])')
best=$(printf '%s' "$clu" | pyget 'round(d["summary"]["best_fraction"],3)')
n_unknown=$(printf '%s' "$clu" | pyget 'd["summary"]["unknown"]')
note "nodes=$n_nodes best_fraction=$best unknown=$n_unknown"
printf '%s' "$clu" | pyget '"      " + "\n      ".join("%-28s %-22s frac=%-6s unknown=%s %s" % (n.get("node_name","(none)"), n["peer_id"], round(n["fraction"],3), n.get("unknown",False), n.get("error","")[:40]) for n in d["nodes"])'

ranked=$(printf '%s' "$clu" | pyget 'all(d["nodes"][i]["fraction"] >= d["nodes"][i+1]["fraction"] for i in range(len(d["nodes"])-1))')
if [ "$ranked" = "True" ]; then ok "4 nodes ranked most-resident first"
else bad "4 nodes not ranked by fraction"; fi

ghost_unknown=$(printf '%s' "$clu" | pyget 'next((n.get("unknown",False) for n in d["nodes"] if n["peer_id"]=="'"$GHOST"'"), "absent")')
ghost_zero=$(printf '%s' "$clu" | pyget 'next((n["fraction"]==0 and not n.get("unknown",False) for n in d["nodes"] if n["peer_id"]=="'"$GHOST"'"), False)')
case "$ghost_unknown" in
  True)   ok "4 unreachable peer reported unknown, not zero" ;;
  absent) bad "4 unreachable peer missing from the report entirely (it should be listed as unknown)" ;;
  *)      if [ "$ghost_zero" = "True" ]; then
            bad "4 unreachable peer reported as fraction 0 — indistinguishable from a node that holds nothing"
          else
            bad "4 unreachable peer not marked unknown (unknown=$ghost_unknown)"
          fi ;;
esac
# One live node at high residency must still be reported even with a peer down:
# routing locality exists, and suppressing it would deroute a healthy node.
if [ -n "$best" ] && python3 -c "import sys; sys.exit(0 if $best > 0.5 else 1)"; then
  ok "4 best_fraction=$best still reported alongside an unknown peer"
else
  bad "4 best_fraction=${best:-?} with a resident node present"
fi

# ============================================= 5. TTL cache + coalescing
echo
echo "--- 5: repeat queries are served from the TTL cache"
# The cache is per-process and the coordinator Service load-balances across
# replicas, so the first poll to land on each replica necessarily misses. The
# property under test is "a polling router does not fan out to the peers on
# every request", and the honest bound for that is R misses total, not one.
# Asserting >=4/5 hits silently assumed a single coordinator and failed here for
# the wrong reason.
replicas=$(kubectl -n "$NS" get deploy coordinator -o jsonpath='{.status.readyReplicas}' 2>/dev/null)
replicas=${replicas:-1}
polls=$(( replicas * 4 ))
hits=0; misses=0
for _ in $(seq 1 "$polls"); do
  c=$(coord "/api/residency?prefix=$DIR/ttlprobe" 30 | pyget 'bool(d.get("cached"))')
  if [ "$c" = "True" ]; then hits=$((hits+1)); else misses=$((misses+1)); fi
done
note "coordinator replicas=$replicas, polls=$polls, cached=$hits, uncached=$misses"
# Allow one extra miss for a TTL expiring mid-run: at 5s TTL a slow poll loop can
# legitimately re-populate one replica.
if [ "$misses" -le $(( replicas + 1 )) ]; then
  ok "5 TTL cache absorbing repeat polls ($hits/$polls cached; <=$((replicas+1)) misses expected for $replicas replicas)"
else
  bad "5 $misses/$polls queries missed the cache with $replicas replicas — expected at most $((replicas+1))"
fi

# ============================================ 6. reconciled warmup (Phase 1)
#
# Run twice, against the two entrances to the same intent registry:
#   6a  HTTP  POST /api/cache/intents      — always runs, no CSI needed
#   6b  CSI   volumeAttributes warmup=full — the production path, needs WITH_CSI=1
# 6a is not a substitute for 6b, but it does mean the reconciler itself is
# covered on a cluster that only has the client DaemonSet, and when both run a
# divergence between them is the interesting signal.
#
# Both drive the loop with POST /api/cache/intents/reconcile rather than waiting
# out the jittered tick. That is what makes this causal: remove the file, force
# the pass, assert. Waiting on a timer tests the clock as much as the code, and
# is the exact shape the project convention warns about.

# reconcile_check LABEL POD CACHE_DIR PREFIX EXPECT_PINNED
# Asserts the shared contract: the intent is visible in residency, a clean pass
# set last_verified, a file removed behind the cache's back comes back on the
# next pass, last_verified advances, and the rewarm counter moves.
reconcile_check() {
  local label="$1" pod="$2" cachedir="$3" prefix="$4" want_pinned="$5"
  local r declared verified rf pinned victim
  local rewarms_before rewarms_after before_verified gone now_rf now_v

  # Wait for the first pass to land the subtree. Registration runs it in the
  # background for both entrances, so this is a bounded poll on observed
  # residency, not a fixed sleep.
  declared=""; verified=""; rf=0
  local deadline=$(( $(date +%s) + 240 ))
  while [ "$(date +%s)" -lt "$deadline" ]; do
    r=$(api "$pod" "/api/cache/residency?prefix=$prefix")
    declared=$(printf '%s' "$r" | pyget 'bool(d.get("declared"))')
    verified=$(printf '%s' "$r" | pyget 'd.get("last_verified","")')
    rf=$(printf '%s' "$r" | pyget 'd["resident_files"]')
    [ "$declared" = "True" ] && [ -n "$verified" ] && [ "${rf:-0}" -ge 1 ] && break
    sleep 5
  done
  note "$label after first pass: declared=$declared resident_files=${rf:-?} last_verified=$verified"
  if [ "$declared" = "True" ]; then ok "$label intent registered and visible in residency"
  else bad "$label residency does not report the declared intent"; fi
  if [ -n "$verified" ]; then ok "$label last_verified set by a clean pass"
  else bad "$label last_verified empty — no pass completed cleanly"; fi

  if [ "$want_pinned" = "1" ]; then
    pinned=$(printf '%s' "$r" | pyget 'bool(d.get("pinned"))')
    if [ "$pinned" = "True" ]; then ok "$label mounted prefix reports pinned (eviction-protected)"
    else bad "$label mounted prefix not reported pinned"; fi
  fi

  # The actual reconciler test: take a warmed file off the node's disk behind
  # the cache's back and require the next pass to notice and refetch it.
  victim=$(kx "$pod" "ls $cachedir$prefix 2>/dev/null | head -1" | tr -d '\r')
  if [ -z "$victim" ]; then
    bad "$label nothing warmed onto the cache dir ($cachedir$prefix) — cannot test re-assert"
    return
  fi
  rewarms_before=$(mval "$pod" "fuse_warm_reconcile_rewarms_total")
  before_verified="$verified"
  kx "$pod" "rm -f $cachedir$prefix/$victim"
  gone=$(api "$pod" "/api/cache/residency?prefix=$prefix" | pyget 'd["resident_files"]')
  note "$label removed $victim; resident_files now ${gone:-?}, rewarms_total=${rewarms_before:-0}"

  # Synchronous: returns only after the pass completes, so the assertions below
  # are ordered by causality rather than by hope.
  apipost "$pod" "/api/cache/intents/reconcile" "{}" 300 >/dev/null

  r=$(api "$pod" "/api/cache/residency?prefix=$prefix")
  now_rf=$(printf '%s' "$r" | pyget 'd["resident_files"]')
  now_v=$(printf '%s' "$r" | pyget 'd.get("last_verified","")')
  rewarms_after=$(mval "$pod" "fuse_warm_reconcile_rewarms_total")
  note "$label after forced reconcile: resident_files=${now_rf:-?} last_verified=$now_v rewarms_total=${rewarms_after:-0}"

  if [ "${now_rf:-0}" -gt "${gone:-0}" ]; then ok "$label reconciler refetched the evicted file"
  else bad "$label evicted file never came back (resident_files stuck at ${now_rf:-?})"; fi
  # A fresh timestamp over unverified residency would be worse than a stale
  # honest one, so this advancing is the contract, not cosmetics.
  if [ -n "$now_v" ] && [ "$now_v" != "$before_verified" ]; then
    ok "$label last_verified advanced ($before_verified -> $now_v)"
  else
    bad "$label last_verified did not advance from $before_verified"
  fi
  if [ "${rewarms_after:-0}" -gt "${rewarms_before:-0}" ]; then
    ok "$label fuse_warm_reconcile_rewarms_total advanced (${rewarms_before:-0} -> ${rewarms_after:-0})"
  else
    bad "$label rewarms counter did not move (${rewarms_before:-0} -> ${rewarms_after:-0})"
  fi
}

echo
echo "--- 6a: declared warmup via HTTP intent is re-asserted after an eviction"
HTTP_VOL="e2e-intent-${RUN}"
reg=$(apipost "$A" "/api/cache/intents" \
  "{\"volume_id\":\"$HTTP_VOL\",\"prefix\":\"$DIR\",\"mode\":\"full\",\"bandwidth\":\"max\"}" 60)
if [ "$(printf '%s' "$reg" | pyget 'bool(d.get("registered"))')" = "True" ]; then
  ok "6a intent declared on A without CSI"
  listed=$(api "$A" "/api/cache/intents" | pyget "sum(1 for i in d['intents'] if i['volume_id']=='$HTTP_VOL')")
  if [ "${listed:-0}" -eq 1 ]; then ok "6a intent listed by GET /api/cache/intents"
  else bad "6a declared intent is not listed (found ${listed:-0})"; fi
  reconcile_check "6a" "$A" "$CACHE_A" "$DIR" 0
  # Teardown drops the obligation and must leave the bytes: they are still
  # valid cache, and deleting them here would make the next section's numbers
  # a lie about what the node holds.
  bytes_before=$(api "$A" "/api/cache/residency?prefix=$DIR" | pyget 'd["resident_bytes"]')
  kubectl -n "$NS" exec "$CURLBOX" -- curl -s --max-time 30 -X DELETE \
    "$(ip_of "$A"):8081/api/cache/intents/$HTTP_VOL" >/dev/null
  r=$(api "$A" "/api/cache/residency?prefix=$DIR")
  gone_declared=$(printf '%s' "$r" | pyget 'bool(d.get("declared"))')
  bytes_after=$(printf '%s' "$r" | pyget 'd["resident_bytes"]')
  if [ "$gone_declared" = "False" ]; then ok "6a teardown dropped the intent"
  else bad "6a intent still declared after DELETE"; fi
  if [ "${bytes_after:-0}" -eq "${bytes_before:-0}" ]; then
    ok "6a teardown left the warmed bytes in place (${bytes_after:-0} B)"
  else
    bad "6a teardown changed resident bytes ${bytes_before:-?} -> ${bytes_after:-?}; it should only drop the obligation"
  fi
else
  # 404 here means the running image predates the intent endpoints.
  bad "6a could not declare an HTTP intent on A: $(printf '%s' "$reg" | head -c 200)"
fi

echo
echo "--- 6b: declared warmup via a CSI mount is re-asserted after an eviction"
CSI_POD="e2e-resid-csi-${RUN}"
have_csi=$(kubectl -n "$NS" get ds -l app=fuse-csi-node -o name 2>/dev/null | grep -c . )
if [ "${have_csi:-0}" -eq 0 ]; then
  skip "6b no CSI node DaemonSet in $NS — redeploy with WITH_CSI=1 (6a already covered the loop)"
else
  NODE_B=$(node_of "$B")
  kubectl -n "$NS" delete pod "$CSI_POD" --ignore-not-found --wait=true >/dev/null 2>&1
  # An inline ephemeral CSI volume is the smallest thing that declares intent:
  # it is exactly the path a real workload takes, and warmup=full is what
  # registers the intent with the reconciler.
  kubectl -n "$NS" apply -f - <<YAML >/dev/null
apiVersion: v1
kind: Pod
metadata:
  name: ${CSI_POD}
  labels: {app: e2e-resid}
spec:
  nodeName: ${NODE_B}
  restartPolicy: Never
  containers:
    - name: hold
      image: busybox:1.36
      command: ["sh", "-c", "sleep 3600"]
      volumeMounts:
        - {name: data, mountPath: /data, readOnly: true}
  volumes:
    - name: data
      csi:
        driver: fuse.csi.storage.io
        readOnly: true
        volumeAttributes:
          rootPath: "${DIR}"
          cacheMode: "readonly"
          warmup: "full"
          pinned: "true"
          sourcePolicy: "peer-first"
YAML
  if ! kubectl -n "$NS" wait --for=condition=Ready "pod/$CSI_POD" --timeout=180s >/dev/null 2>&1; then
    bad "6b CSI pod never became Ready"
    kubectl -n "$NS" describe pod "$CSI_POD" 2>/dev/null | tail -15
  else
    ok "6b CSI pod mounted ${DIR} with warmup=full"
    # Same assertions as 6a, on the production entrance. pinned=1 here because
    # a live CSI mount is what makes the prefix eviction-protected; the HTTP
    # intent deliberately carries no mount and so no pin.
    reconcile_check "6b" "$B" "$CACHE_B" "$DIR" 1
  fi
fi

# ============================== 7. a peer without NODE_NAME is still usable
echo
echo "--- 7: a peer registered without NODE_NAME still registers and is selectable"
# Re-register rather than reuse section 4's: the etcd peer lease is 90s and the
# warm selector drops peers silent for >75s, so after section 6's minutes-long
# waits the original ghost has legitimately expired. Testing an expired peer
# would fail for the wrong reason.
coordpost "/api/peers/register" \
  "{\"id\":\"$GHOST\",\"address\":\"127.0.0.1:1\",\"status\":\"active\",\"available_space\":1,\"used_space\":0}" >/dev/null
sleep 1
listed=$(coord "/api/peers" | pyget 'any(p["id"]=="'"$GHOST"'" for p in d)')
if [ "$listed" = "True" ]; then ok "7 no-NodeName peer accepted and listed"
else bad "7 no-NodeName peer was rejected or dropped from the registry"; fi
# Selectable: the warm fan-out's own selector must still consider it. Dry-run
# by percentage so nothing is actually warmed on the ghost beyond a failed call.
sel=$(coordpost "/api/warm" "{\"prefix\":\"$DIR\",\"mode\":\"metadata\",\"nodes\":[\"$GHOST\"],\"async\":true}" 30)
# Selected>0 is the whole claim. The delivery to 127.0.0.1:1 necessarily fails
# and lands the ghost in `failed` — that is still selection, and the coordinator
# rejects an empty selection outright with "no active peers match the selector",
# which is the real failure mode a missing NodeName would cause.
picked=$(printf '%s' "$sel" | pyget 'd.get("selected",0) > 0 and ("'"$GHOST"'" in (d.get("accepted") or []) or "'"$GHOST"'" in (d.get("failed") or {}))')
if [ "$picked" = "True" ]; then
  ok "7 no-NodeName peer is still selectable for warm fan-out"
else
  bad "7 no-NodeName peer was not selected: $(printf '%s' "$sel" | head -c 200)"
fi

# ==================================================================== cleanup
echo
if [ "$KEEP" = "1" ]; then
  echo "KEEP=1: leaving $DIR, pod $CSI_POD and peer $GHOST in place"
else
  kubectl -n "$NS" delete pod "$CSI_POD" --ignore-not-found --wait=false >/dev/null 2>&1
  kx "$A" "rm -rf $MOUNT$DIR" >/dev/null 2>&1
  # The ghost peer has a TTL lease and expires on its own; nothing to undo.
  echo "cleaned up $DIR and pod $CSI_POD (ghost peer expires with its lease)"
fi

echo
echo "=== residency e2e complete: $FAILED failure(s) ==="
exit $FAILED

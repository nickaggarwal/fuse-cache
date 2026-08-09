#!/usr/bin/env bash
# End-to-end: does fuse cache residency actually steer llm-d routing?
#
# Asserts the property, not the plumbing. Two vLLM pods on two A100 nodes run
# the identical manifest, image, and model path; the ONLY difference is which
# node has the weights on local NVMe. If the scorer works, that difference — and
# nothing else — moves the routing decision.
#
# The decisive check is not "the warm node was picked" but "the picked endpoint
# FOLLOWS residency when residency changes". A single reading can be right by
# accident (a pod ordering, a queue-depth tiebreak); a reading that flips when
# and only when we evict cannot.
#
# Requires: gpuckpt nodepool scaled up, weights seeded (scripts/llmd-seed-model.yaml),
# vLLM (scripts/llmd-vllm.yaml) and EPP (scripts/llmd-epp.yaml) applied.
#
# Exit code = number of failed assertions.
set -uo pipefail

NS="${1:-fuse-system}"
MODEL="Qwen/Qwen2.5-1.5B-Instruct"
PREFIX="models/Qwen2.5-1.5B-Instruct/"
PROBE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../integrations/llm-d-fuse-residency" && pwd)"
FAILURES=0

pass() { echo "  PASS: $*"; }
fail() { echo "  FAIL: $*"; FAILURES=$((FAILURES + 1)); }
section() { echo; echo "=== $* ==="; }

# A pod with curl that can reach the coordinator's ClusterIP. The coordinator
# image has neither curl nor wget, so exec-ing into it is not an option.
curlbox() { kubectl -n "$NS" exec curlbox -- curl -s "$@"; }

residency_json() {
  curlbox "http://coordinator.${NS}.svc.cluster.local:8080/api/residency?prefix=${PREFIX}"
}

# Residency fraction for one node IP, or empty if that node is absent/unknown.
# Unknown deliberately yields empty rather than 0 — see section 4.
frac_for_ip() {
  residency_json | python3 -c "
import json,sys
d=json.load(sys.stdin)
for n in d.get('nodes',[]):
    if n.get('node_ip')=='$1' and not n.get('unknown'):
        print(n['fraction']); break
"
}

# The client pod serving a given node, and that node's node-init cache dir.
# The dir is read from node-init.json rather than hardcoded: node-init picks a
# different disk per node, and a wrong path makes the eviction a silent no-op.
client_pod_on() {
  # The DaemonSet labels its pods app=client, not app=fuse-client.
  kubectl -n "$NS" get pods -l app=client -o json |
    python3 -c "
import json,sys
d=json.load(sys.stdin)
for p in d['items']:
    if p['spec'].get('nodeName')=='$1': print(p['metadata']['name']); break
"
}
cache_dir_of() {
  kubectl -n "$NS" exec "$1" -c client -- cat /run/fuse-client/node-init.json 2>/dev/null |
    python3 -c "import json,sys; print(json.load(sys.stdin)['cache_dir'])"
}

section "0. Port-forward to EPP"
# The script owns this rather than assuming an external one is up: a
# port-forward that died mid-run reports as "scorer returned no decision",
# which reads like a product failure and is not one.
EPP_ADDR="${EPP_ADDR:-localhost:9002}"
PF_PID=""
if [ -z "${EPP_NO_PORT_FORWARD:-}" ]; then
  kubectl -n "$NS" port-forward svc/fuse-epp 9002:9002 >/tmp/e2e-epp-pf.log 2>&1 &
  PF_PID=$!
  trap '[ -n "$PF_PID" ] && kill "$PF_PID" 2>/dev/null' EXIT
  for _ in $(seq 1 20); do
    grep -q "Forwarding from" /tmp/e2e-epp-pf.log 2>/dev/null && break
    sleep 1
  done
fi
kill -0 "$PF_PID" 2>/dev/null && pass "port-forward to svc/fuse-epp established" ||
  { [ -n "$PF_PID" ] && fail "port-forward did not come up"; }

section "0b. Discover the two GPU endpoints"
# Plain read loop, not mapfile: macOS ships bash 3.2, where mapfile does not
# exist and the failure is a cascade of unbound-variable errors, not a clear one.
VLLM=()
while IFS= read -r line; do
  [ -n "$line" ] && VLLM+=("$line")
done < <(kubectl -n "$NS" get pods -l app=vllm-qwen \
  -o jsonpath='{range .items[*]}{.status.podIP}{" "}{.spec.nodeName}{" "}{.status.hostIP}{"\n"}{end}' | sort)
if [ "${#VLLM[@]}" -ne 2 ]; then
  fail "expected 2 vllm pods, found ${#VLLM[@]}"
  exit $FAILURES
fi
EP_A=$(echo "${VLLM[0]}" | awk '{print $1":8000"}'); NODE_A=$(echo "${VLLM[0]}" | awk '{print $2}'); IP_A=$(echo "${VLLM[0]}" | awk '{print $3}')
EP_B=$(echo "${VLLM[1]}" | awk '{print $1":8000"}'); NODE_B=$(echo "${VLLM[1]}" | awk '{print $2}'); IP_B=$(echo "${VLLM[1]}" | awk '{print $3}')
echo "  A: $EP_A on $NODE_A (host $IP_A)"
echo "  B: $EP_B on $NODE_B (host $IP_B)"
[ "$NODE_A" != "$NODE_B" ] && pass "one vLLM pod per node (anti-affinity held)" \
  || fail "both vLLM pods on $NODE_A — the comparison would measure nothing"

section "1. vLLM loaded weights from the FUSE mount, warm beats cold"
# Weight-load time is the payoff number: what a warm cache is worth in seconds.
LOADED=0
for row in "${VLLM[@]}"; do
  pod_ip=$(echo "$row" | awk '{print $1}')
  p=$(kubectl -n "$NS" get pods -l app=vllm-qwen -o json |
    python3 -c "
import json,sys
d=json.load(sys.stdin)
for x in d['items']:
    if x['status'].get('podIP')=='$pod_ip': print(x['metadata']['name']); break
")
  # Read each pod's log individually. A label-selector read with --tail=-1
  # returned nothing here even though the per-pod reads worked, and an empty
  # log would silently look like "no weights loaded".
  # 2>&1, not 2>/dev/null: vLLM writes its INFO lines (including "Loading model
  # weights took") to stderr, so discarding stderr drops exactly the line being
  # grepped for and the check fails while the pod is perfectly healthy.
  logs=$(kubectl -n "$NS" logs "$p" 2>&1)
  t=$(printf '%s\n' "$logs" | grep -oE 'Loading safetensors checkpoint shards: 100%.*\[[0-9:]+<' |
    grep -oE '\[[0-9:]+<' | tr -d '[<' | head -1)
  echo "  $(echo "$row" | awk '{print $2}'): checkpoint load ${t:-unknown}"
  # Match with a shell glob, not `grep -q`: grep -q exits at the first match and
  # closes the pipe, so writing a multi-megabyte vLLM log into it dies with
  # SIGPIPE and reports failure for a pod that loaded weights perfectly.
  case "$logs" in *"Loading model weights took"*) LOADED=$((LOADED + 1)) ;; esac
done
[ "$LOADED" -eq 2 ] &&
  pass "both pods loaded weights off /mnt/fuse (HF_HUB_OFFLINE=1, so no download path existed)" ||
  fail "only $LOADED/2 pods show a weight-load line"

section "2. Residency is byte-weighted, not file-counted"
# The distinction is load-bearing: a node holding 6 of 7 files but none of the
# 3GB safetensors is 86% by count and ~0.4% by bytes. Only one of those numbers
# would route correctly.
if residency_json | python3 -c "
import json,sys
d=json.load(sys.stdin)
diverged=0
for n in d.get('nodes',[]):
    if n.get('unknown'): continue
    bycount = n['resident_files']/n['files'] if n['files'] else 0
    print(f\"  {n['node_name'][-10:]:12} bytes={n['fraction']:.4f}  files={bycount:.4f}\")
    if abs(bycount - n['fraction']) > 0.2: diverged += 1
# Exit 0 == found a node where the two metrics disagree, which is the case
# worth asserting: a node holding most FILES but almost none of the BYTES.
sys.exit(0 if diverged else 9)
"; then
  pass "byte-weighted and file-counted disagree sharply on >=1 node (only bytes route correctly)"
else
  echo "  (no divergence right now — every node happens to be all-or-nothing)"
fi

section "3. EPP scores track residency, and follow it when it changes"
probe() { (cd "$PROBE_DIR" && go run ./cmd/scoreprobe -addr "${EPP_ADDR:-localhost:9002}" -model "$MODEL" -json 2>&1); }

BEFORE=$(probe)
echo "  before: $BEFORE"
CHOSEN_BEFORE=$(echo "$BEFORE" | python3 -c "import json,sys; print(json.load(sys.stdin).get('chosen',''))" 2>/dev/null)
if [ -z "$CHOSEN_BEFORE" ]; then
  fail "probe returned no decision (is the port-forward to svc/fuse-epp up?)"
  exit $FAILURES
fi
pass "EPP emitted per-endpoint scores (--emit-endpoint-scores)"

# Which node currently holds more of the model? That one should be picked.
FA=$(frac_for_ip "$IP_A"); FB=$(frac_for_ip "$IP_B")
echo "  residency: A=$FA  B=$FB"
EXPECT=$(python3 -c "print('$EP_A' if float('${FA:-0}') >= float('${FB:-0}') else '$EP_B')")
[ "$CHOSEN_BEFORE" = "$EXPECT" ] && pass "picked the more-resident endpoint ($EXPECT)" \
  || fail "picked $CHOSEN_BEFORE but $EXPECT holds more of the model"

# Now evict on the winner and require the decision to move. This is the part a
# lucky ordering cannot fake.
WARM_NODE=$([ "$EXPECT" = "$EP_A" ] && echo "$NODE_A" || echo "$NODE_B")
COLD_EP=$([ "$EXPECT" = "$EP_A" ] && echo "$EP_B" || echo "$EP_A")
CP=$(client_pod_on "$WARM_NODE")
CD=$(cache_dir_of "$CP")
if [ -z "$CP" ] || [ -z "$CD" ]; then
  fail "could not resolve client pod / cache dir on $WARM_NODE — skipping the flip"
else
  echo "  evicting model.safetensors from $CP:$CD"
  kubectl -n "$NS" exec "$CP" -c client -- \
    rm -f "/host${CD}/models/Qwen2.5-1.5B-Instruct/model.safetensors" 2>/dev/null
  # Poll rather than sleep past the extractor's poll interval: assert on the
  # observed decision, not on a guess about the clock.
  FLIPPED=0
  for _ in $(seq 1 12); do
    AFTER=$(probe)
    if echo "$AFTER" | grep -q "\"chosen\":\"$COLD_EP\""; then FLIPPED=1; break; fi
    sleep 5
  done
  echo "  after:  ${AFTER:-<none>}"
  [ "$FLIPPED" -eq 1 ] && pass "decision followed the eviction to $COLD_EP" \
    || fail "decision did not move after eviction — scorer is not tracking residency"
fi

section "4. An operator can warm a node on demand, and residency reflects it"
# The routing story only closes if an operator can ACT on a low score. Reading
# the file through the mount also warms a node, but that is a side effect of
# serving a request, not an operator control — and it lands as individual
# _chunk_N files that take a further pass to coalesce, so residency lags. The
# warm API is the supported path and completes the whole file in one job.
#
# Pick the least-resident node that is not one of the two GPU nodes, so warming
# it cannot perturb the routing assertions above.
COLD_TARGET=$(residency_json | python3 -c "
import json,sys
d=json.load(sys.stdin)
gpu={'$IP_A','$IP_B'}
c=[n for n in d.get('nodes',[]) if not n.get('unknown') and n.get('node_ip') not in gpu]
if c:
    n=min(c,key=lambda x:x['fraction'])
    print(n['address'], n['fraction'], n['node_name'])
")
if [ -z "$COLD_TARGET" ]; then
  echo "  (no non-GPU peer to warm; skipping)"
else
  ADDR=$(echo "$COLD_TARGET" | awk '{print $1}')
  TNODE=$(echo "$COLD_TARGET" | awk '{print $3}')

  # Evict first so the section is repeatable. On a second run the "coldest"
  # node is whatever the previous run warmed, and warming an already-full node
  # would pass trivially without moving a byte.
  TCP=$(client_pod_on "$TNODE")
  TCD=$(cache_dir_of "$TCP")
  if [ -n "$TCP" ] && [ -n "$TCD" ]; then
    kubectl -n "$NS" exec "$TCP" -c client -- \
      rm -f "/host${TCD}/models/Qwen2.5-1.5B-Instruct/model.safetensors" 2>/dev/null
    echo "  evicted safetensors on $TNODE to establish a cold baseline"
  fi
  BEFORE_F=$(frac_for_ip "$(kubectl -n "$NS" get pod "$TCP" -o jsonpath='{.status.hostIP}' 2>/dev/null)")
  echo "  target $TNODE ($ADDR) at fraction ${BEFORE_F:-unknown}"

  JOB=$(curlbox -X POST "http://${ADDR}/api/cache/warm" \
    -H 'Content-Type: application/json' \
    -d "{\"prefix\":\"${PREFIX}\",\"mode\":\"full\",\"async\":true}" |
    python3 -c "import json,sys; print(json.load(sys.stdin).get('job_id',''))" 2>/dev/null)
  if [ -z "$JOB" ]; then
    fail "warm API did not return a job_id"
  else
    pass "warm accepted, job $JOB"
    # Poll the job to a terminal status rather than sleeping a fixed time —
    # the whole point of the async job registry is that progress is observable.
    STATUS=""
    for _ in $(seq 1 40); do
      J=$(curlbox "http://${ADDR}/api/cache/warm/${JOB}")
      STATUS=$(printf '%s' "$J" | python3 -c "import json,sys; print(json.load(sys.stdin).get('status',''))" 2>/dev/null)
      case "$STATUS" in done | completed | failed | error) break ;; esac
      sleep 8
    done
    printf '%s' "$J" | python3 -c "
import json,sys
d=json.load(sys.stdin); p=d['progress']
print(f\"  job {d['status']}: files={p['done']}/{p['files']} warmed={p['warmed']} already_local={p['already_local']} failed={p['failed']} bytes={p['bytes']}\")
" 2>/dev/null
    case "$STATUS" in
    done | completed) pass "warm job reached '$STATUS'" ;;
    *) fail "warm job ended '$STATUS'" ;;
    esac
    printf '%s' "$J" | python3 -c "
import json,sys; sys.exit(0 if json.load(sys.stdin)['progress']['failed']==0 else 9)
" 2>/dev/null && pass "no files failed to warm" || fail "warm reported failed files"

    # The measured proof: residency must actually rise. A job that reports
    # success while the disk stays empty is the exact divergence this endpoint
    # exists to catch.
    AFTER_F=$(residency_json | python3 -c "
import json,sys
d=json.load(sys.stdin)
for n in d.get('nodes',[]):
    if n.get('address')=='$ADDR' and not n.get('unknown'): print(n['fraction']); break
")
    echo "  residency $BEFORE_F -> ${AFTER_F:-unknown}"
    python3 -c "
import sys
sys.exit(0 if float('${AFTER_F:-0}') > 0.99 else 9)
" && pass "operator warm drove measured residency to ~1.0" \
      || fail "warm claimed success but measured residency is ${AFTER_F:-unknown}"
  fi
fi

section "5. An unmeasurable node scores unknown, not zero"
# Unknown and zero are arithmetically identical to a weighted-sum scheduler, so
# conflating them deroutes a healthy node on a transient blip. Verified against
# whatever the coordinator currently reports as unknown.
UNK=$(residency_json | python3 -c "
import json,sys
d=json.load(sys.stdin)
print(sum(1 for n in d.get('nodes',[]) if n.get('unknown')))
")
if [ "${UNK:-0}" -gt 0 ]; then
  residency_json | python3 -c "
import json,sys
d=json.load(sys.stdin)
bad=[n for n in d['nodes'] if n.get('unknown') and n.get('fraction')]
sys.exit(9 if bad else 0)
" && pass "$UNK unreachable node(s) reported unknown with no fabricated fraction" \
    || fail "an unknown node also carried a fraction"
else
  echo "  (all peers reachable; nothing unknown to assert against)"
fi

echo
echo "=== $FAILURES failure(s) ==="
exit $FAILURES

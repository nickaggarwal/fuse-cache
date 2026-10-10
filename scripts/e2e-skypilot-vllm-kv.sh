#!/usr/bin/env bash
# e2e: SkyPilot orchestrating vLLM, with the REAL vLLM KV cache landing on the
# distributed FUSE filesystem.
#
# Exit code = number of failed sections.
#
# What this proves, and the one thing that makes it a proof rather than a
# coincidence: a KV "hit" observed on the SAME engine that wrote the KV is
# worthless, because vLLM's own GPU prefix cache would produce an identical
# observation. Section 5 therefore restarts the engine and asserts
#     External Cache Hit   AND   vllm:prefix_cache_hits_total == 0
# simultaneously. The GPU cache is provably empty, so the bytes can only have
# come off the mount.
#
# Prereqs: sky CLI on PATH (or SKY=), kubectl context on the cluster, the
# service already up via:  sky serve up skypilot/vllm-qwen-kv.sky.yaml -n vllm-kv
set -uo pipefail

SKY="${SKY:-sky}"
SVC="${SVC:-vllm-kv}"
NS="${NS:-default}"
LB="${LB:-http://localhost:30001}"
KVDIR="${KVDIR:-/mnt/fuse-kv/vllmkv}"
MODEL="${MODEL:-Qwen/Qwen2.5-1.5B-Instruct}"
FAIL=0

say()  { printf '\n=== %s ===\n' "$*"; }
pass() { printf '  PASS: %s\n' "$*"; }
fail() { printf '  FAIL: %s\n' "$*"; FAIL=$((FAIL+1)); }

# Newest non-terminating Running pod for the service. Sorting by creation
# timestamp matters because replica IDs are lexicographic: "vllm-kv-10" sorts
# before "vllm-kv-2", so `tail -1` on names picks the wrong pod past 9 replicas.
pod_of() {
  kubectl -n "$NS" get pods --sort-by=.metadata.creationTimestamp \
    -o go-template='{{range .items}}{{if not .metadata.deletionTimestamp}}{{if eq .status.phase "Running"}}{{.metadata.name}}{{"\n"}}{{end}}{{end}}{{end}}' 2>/dev/null \
  | grep -E "^${SVC}-[0-9]+-" | tail -1
}

kexec() { kubectl -n "$NS" exec "$1" -- sh -c "$2" 2>/dev/null; }

metric() { kexec "$1" "curl -s localhost:8000/metrics | grep -E '^vllm:${2}' | awk '{print \$2}'" | tail -1; }

# A prompt long enough to span whole KV blocks; a short one may not be stored.
long_prompt() {
  python3 -c "print('The distributed FUSE cache stores model weights on local NVMe and replicates them across peers. ' * 24, end='')"
}

infer() {
  python3 - "$1" "$2" <<'PY' > /tmp/_kvreq.json
import json,sys
print(json.dumps({"model":sys.argv[1],"prompt":sys.argv[2],"max_tokens":16,"temperature":0}))
PY
  curl -s -m 120 "$LB/v1/completions" -H 'Content-Type: application/json' \
       -d @/tmp/_kvreq.json -o /tmp/_kvresp.json -w '%{http_code}'
}

POD=$(pod_of)
[ -z "$POD" ] && { echo "no ready replica pod for service $SVC"; exit 99; }

# Wait for the engine to actually serve before asserting anything about it.
echo "waiting for the engine in $POD to answer..."
_d=$((SECONDS+900))
_up=0
while [ $SECONDS -lt $_d ]; do
  if [ "$(kexec "$POD" "curl -s -o /dev/null -w '%{http_code}' localhost:8000/v1/models")" = "200" ]; then _up=1; break; fi
  sleep 10
done
# Falling through here would run every assertion against a dead engine and
# report a pile of failures that all have one cause. Say the cause instead.
[ "$_up" = 1 ] || { echo "engine in $POD never answered on :8000 -- is the service up? (sky serve status $SVC)"; exit 98; }
echo "engine is serving."
echo "replica pod: $POD  node: $(kubectl -n "$NS" get pod "$POD" -o jsonpath='{.spec.nodeName}' 2>/dev/null)"

# ---------------------------------------------------------------------------
say "1. SkyServe reports the service READY"
ST=$($SKY serve status "$SVC" 2>/dev/null | python3 -c '
import re,sys
rows=[l.rstrip("\n") for l in sys.stdin]
try: h=next(l for l in rows if re.match(r"\s*NAME\s+VERSION", l))
except StopIteration: print(""); sys.exit()
c=h.index("STATUS"); e=h.index("REPLICAS")
for l in rows[rows.index(h)+1:]:
    if l.strip().startswith(sys.argv[1]+" "): print(l[c:e].strip()); break
else: print("")
' "$SVC")
[ "$ST" = "READY" ] && pass "service status READY" || fail "service status is '$ST', want READY"

# ---------------------------------------------------------------------------
say "2. Weights are read through the FUSE mount, not baked into the image"
N=$(kexec "$POD" "ls -1 /mnt/fuse/models/Qwen2.5-1.5B-Instruct 2>/dev/null | wc -l" | tr -d ' \r')
[ "${N:-0}" -ge 7 ] && pass "$N model files visible at /mnt/fuse" || fail "only '${N:-0}' model files via the mount"
# mountPropagation is what makes this work: without it the pod captures the
# directory at container start and sees the empty dir under the FUSE mount.
MP=$(kubectl -n "$NS" get pod "$POD" -o json 2>/dev/null \
     | python3 -c 'import json,sys
p=json.load(sys.stdin)
print(next((m.get("mountPropagation","") for m in p["spec"]["containers"][0].get("volumeMounts",[]) if m["mountPath"]=="/mnt/fuse"),""))')
[ "$MP" = "HostToContainer" ] && pass "mountPropagation=HostToContainer survived SkyPilot's pod_config merge" \
                              || fail "mountPropagation is '$MP'"

# ---------------------------------------------------------------------------
say "3. The KV connector is configured against the mount"
# Assert against the engine's own argv, not its log. vLLM is verbose enough to
# blow past kubelet's 10Mi rotation within one run of this script, so the
# "Shared storage path is ..." line it prints at startup is gone by the time
# sections 4 and 5 have driven traffic through it -- which is exactly why an
# earlier log-grep version of this check passed standalone and failed here.
# argv is state, not history: it cannot rotate away.
#
# The scan walks /proc rather than using pgrep -f, because pgrep -f matches the
# `sh -c` wrapper that kubectl exec itself created and returns that instead.
# The `sh -c*) continue` arm is load-bearing for the same reason one level
# down: this command's OWN wrapper contains the search string in its argv, so
# without the guard the scan matches itself on the first iteration.
ARGV=$(kexec "$POD" 'for p in /proc/[0-9]*; do c=$(tr "\0" " " < $p/cmdline 2>/dev/null); case "$c" in sh\ -c*) continue ;; *api_server*) echo "$c"; break ;; esac; done')
if [ -z "$ARGV" ]; then
  fail "no vllm api_server process found in $POD"
elif printf '%s' "$ARGV" | tr -d ' ' | grep -q "\"shared_storage_path\":\"$KVDIR\""; then
  pass "engine argv roots the KV connector at $KVDIR"
else
  fail "engine argv does not root the connector at $KVDIR"
  printf '    argv: %s\n' "$ARGV" | cut -c1-300
fi
# It must also exist on the mount, or every store silently no-ops into a
# directory no other node can see.
if kexec "$POD" "test -d $KVDIR && echo yes" | grep -q yes; then
  pass "connector root exists on the FUSE mount"
else
  fail "$KVDIR is not a directory in the pod"
fi

# ---------------------------------------------------------------------------
say "4. A real inference writes real KV tensors onto the mount"
# Unique per run: the connector keys its directory on sha256 of the prompt
# tokens, so a fixed prompt reuses the same dir and "count grew" would fail on
# every rerun against a warm mount. A fresh prompt must create a fresh dir.
UNIQ="run-$(date +%s)-$$"
BEFORE=$(kexec "$POD" "ls -1 $KVDIR 2>/dev/null | wc -l" | tr -d ' \r')
CODE=$(infer "$MODEL" "$UNIQ $(long_prompt)")
[ "$CODE" = "200" ] && pass "inference through the SkyServe LB returned 200" || fail "inference http=$CODE"
sleep 5
AFTER=$(kexec "$POD" "ls -1 $KVDIR 2>/dev/null | wc -l" | tr -d ' \r')
[ "${AFTER:-0}" -gt "${BEFORE:-0}" ] && pass "a never-seen prompt created a new KV prefix dir (${BEFORE} -> ${AFTER})" \
                                     || fail "no new KV prefix dir (${BEFORE} -> ${AFTER})"
# Assert the payload is genuine KV, not just some file. One safetensors per
# attention layer, each holding a [2, tokens, hidden] tensor -- the 2 is K and V.
kexec "$POD" "python3 -c \"
import glob,json,struct,sys
f=sorted(glob.glob('$KVDIR/*/*.safetensors'))
if not f: sys.exit('no safetensors')
with open(f[0],'rb') as fh:
    n=struct.unpack('<Q',fh.read(8))[0]; h=json.loads(fh.read(n))
t=[v for k,v in h.items() if k!='__metadata__'][0]
assert t['shape'][0]==2, t['shape']
print('layers=%d dtype=%s shape=%s' % (len(f), t['dtype'], t['shape']))
\"" > /tmp/_kvshape 2>&1
if grep -q 'layers=' /tmp/_kvshape; then pass "genuine KV tensors: $(cat /tmp/_kvshape)"
else fail "KV payload not verifiable: $(cat /tmp/_kvshape)"; fi

# ---------------------------------------------------------------------------
say "5. THE PROOF: a cold engine reads that KV back off the mount"
# Kill the engine. SkyServe replaces the replica; the new process has an empty
# GPU prefix cache but the same FUSE mount. If a hit is still observed while
# prefix_cache_hits_total is 0, the bytes came from the distributed cache and
# nowhere else.
OLD="$POD"
kubectl -n "$NS" exec "$OLD" -- pkill -f 'vllm.entrypoints.openai.api_server' >/dev/null 2>&1
echo "  killed engine in $OLD; waiting for SkyServe to bring up a replacement..."
NEW=""; DEADLINE=$((SECONDS+900))
while [ $SECONDS -lt $DEADLINE ]; do
  c=$(pod_of)
  if [ -n "$c" ] && [ "$c" != "$OLD" ]; then
    if [ "$(kexec "$c" "curl -s -o /dev/null -w '%{http_code}' localhost:8000/v1/models")" = "200" ]; then
      # The pod answers, but the LB only routes once SkyServe's probe passes.
      if [ "$(curl -s -o /dev/null -w '%{http_code}' -m 10 "$LB/v1/models")" = "200" ]; then
        NEW="$c"; break
      fi
    fi
  fi
  sleep 15
done
if [ -z "$NEW" ]; then
  fail "no replacement replica became ready within 900s"
else
  pass "SkyServe recovered the replica automatically: $NEW"
  H0=$(metric "$NEW" "prefix_cache_hits_total")
  if [ "${H0%%.*}" = "0" ]; then pass "fresh engine GPU prefix cache is empty (hits=$H0)"
  else fail "fresh engine already has GPU prefix hits ($H0) - test is not conclusive"; fi

  CODE=$(infer "$MODEL" "$(long_prompt)")
  [ "$CODE" = "200" ] && pass "same prompt replayed, http=200" || fail "replay http=$CODE"
  sleep 5

  HITLOG=$(kubectl -n "$NS" logs "$NEW" 2>/dev/null | grep -c 'External Cache Hit')
  H1=$(metric "$NEW" "prefix_cache_hits_total")
  Q1=$(metric "$NEW" "prefix_cache_queries_total")

  if [ "${HITLOG:-0}" -ge 1 ]; then pass "connector reported External Cache Hit ($HITLOG)"
  else fail "no External Cache Hit on the cold engine"; fi

  # The conjunction is the whole point: a hit WITH zero local hits.
  if [ "${HITLOG:-0}" -ge 1 ] && [ "${H1%%.*}" = "0" ] && [ "${Q1%%.*}" != "0" ]; then
    pass "CONCLUSIVE: external hit with GPU prefix_cache_hits=$H1 over $Q1 queries -> KV came off the FUSE mount"
  else
    fail "not conclusive: external_hits=$HITLOG gpu_hits=$H1 gpu_queries=$Q1"
  fi
fi

# ---------------------------------------------------------------------------
say "6. The KV is visible cluster-wide, not just on the writer's local disk"
SEEN=0; TOT=0
for p in $(kubectl -n fuse-system get pods -l app=client --no-headers 2>/dev/null | awk '{print $1}'); do
  n=$(kubectl -n fuse-system get pod "$p" -o jsonpath='{.spec.nodeName}' 2>/dev/null)
  case "$n" in *gpuckpt*) ;; *) continue ;; esac
  TOT=$((TOT+1))
  c=$(kubectl -n fuse-system exec "$p" -c client -- sh -c 'ls -1 /host/mnt/fuse/vllmkv/ 2>/dev/null | wc -l' 2>/dev/null | tr -d ' \r')
  echo "    $n: ${c:-0} prefix dirs"
  [ "${c:-0}" -ge 1 ] && SEEN=$((SEEN+1))
done
[ "$TOT" -gt 0 ] && [ "$SEEN" -eq "$TOT" ] && pass "all $TOT GPU nodes see the KV through their own mount" \
                                           || fail "only $SEEN/$TOT GPU nodes see the KV"

printf '\n===== %d failure(s) =====\n' "$FAIL"
exit "$FAIL"

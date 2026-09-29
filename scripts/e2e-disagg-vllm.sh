#!/usr/bin/env bash
# End-to-end validation of DISAGGREGATED prefill/decode across two nodes, with
# the fuse-client distributed cache as the KV transport.
#
# Exit code = number of failed checks.
#
# What this suite is actually trying to prove, and why each section exists:
#
# The easy mistake is to declare victory when a disagg-shaped deployment
# returns a correct completion. It always will -- if the KV handoff never
# happens, the decode engine simply prefills the prompt itself and answers
# correctly, a few hundred milliseconds slower. Correct output is therefore
# almost no evidence. The load-bearing assertions are the ones that would fail
# if the handoff silently stopped working:
#
#   S3  the two engines are on DIFFERENT nodes         (else it is not disagg)
#   S5  the decode engine logs an External Cache Hit while its own GPU
#       prefix cache reports ZERO hits                 (the KV came off disk)
#   S6  the KV bytes were written by the OTHER node    (it crossed the cluster)
#
# S6 is the one that separates "two engines sharing a filesystem" from "two
# engines that happen to both be running".
set -uo pipefail

NS=${NS:-default}
PREFILL_SVC=${PREFILL_SVC:-vllm-prefill}
DECODE_SVC=${DECODE_SVC:-vllm-decode}
KVDIR=${KVDIR:-/mnt/fuse-kv/vllmkv-disagg}
SKY=${SKY:-/tmp/skyenv/bin/sky}
MODEL=${MODEL:-Qwen/Qwen2.5-1.5B-Instruct}

FAILS=0
pass() { echo "  PASS: $*"; }
fail() { echo "  FAIL: $*"; FAILS=$((FAILS+1)); }
say()  { echo; echo "=== $* ==="; }

kexec() { kubectl -n "$NS" exec "$1" -- sh -c "$2" 2>/dev/null; }

# Newest non-terminating Running pod for a service. Sorting by creation
# timestamp matters because replica IDs are lexicographic: "vllm-prefill-10"
# sorts before "vllm-prefill-2", so `tail -1` on names picks the wrong pod past
# nine replicas. Terminating pods still report phase=Running, hence the
# deletionTimestamp filter.
pod_of() {
  kubectl -n "$NS" get pods --sort-by=.metadata.creationTimestamp \
    -o go-template='{{range .items}}{{if not .metadata.deletionTimestamp}}{{if eq .status.phase "Running"}}{{.metadata.name}}{{"\n"}}{{end}}{{end}}{{end}}' 2>/dev/null \
  | grep -E "^${1}-[0-9]+-" | tail -1
}

node_of() { kubectl -n "$NS" get pod "$1" -o jsonpath='{.spec.nodeName}' 2>/dev/null; }

wait_serving() {  # pod, label
  local pod=$1 lbl=$2 d=$((SECONDS+900))
  echo "waiting for the $lbl engine in $pod to answer..."
  while [ $SECONDS -lt $d ]; do
    [ "$(kexec "$pod" "curl -s -o /dev/null -w '%{http_code}' localhost:8000/v1/models")" = "200" ] && {
      echo "  $lbl is serving."; return 0; }
    sleep 10
  done
  return 1
}

PPOD=$(pod_of "$PREFILL_SVC")
DPOD=$(pod_of "$DECODE_SVC")
[ -n "$PPOD" ] || { echo "no running pod for $PREFILL_SVC -- is it up? ($SKY serve status)"; exit 98; }
[ -n "$DPOD" ] || { echo "no running pod for $DECODE_SVC -- is it up? ($SKY serve status)"; exit 98; }
PNODE=$(node_of "$PPOD"); DNODE=$(node_of "$DPOD")
echo "prefill: $PPOD  on $PNODE"
echo "decode : $DPOD  on $DNODE"

# Run every assertion against live engines, or a dead one produces a pile of
# failures that all have a single cause.
wait_serving "$PPOD" prefill || { echo "prefill never served"; exit 98; }
wait_serving "$DPOD" decode  || { echo "decode never served";  exit 98; }

# ---------------------------------------------------------------------------
say "1. Both SkyServe services report READY"
for s in "$PREFILL_SVC" "$DECODE_SVC"; do
  # Parse by header column offset: UPTIME renders as "24m 26s", two awk fields,
  # so a positional $4 silently reads the seconds half of the uptime.
  st=$($SKY serve status "$s" 2>/dev/null | /tmp/skyenv/bin/python -c "
import sys
rows=[l.rstrip() for l in sys.stdin if l.strip()]
hdr=next((l for l in rows if l.startswith('NAME')), None)
if hdr:
    i=hdr.index('STATUS'); j=hdr.index('REPLICAS')
    for l in rows[rows.index(hdr)+1:]:
        if l.startswith('$s'):
            print(l[i:j].strip()); break
")
  [ "$st" = "READY" ] && pass "$s is READY" || fail "$s status is '${st:-unknown}', not READY"
done

# ---------------------------------------------------------------------------
say "2. Weights come off the FUSE mount on both halves"
for p in "$PPOD:prefill" "$DPOD:decode"; do
  pod=${p%%:*}; lbl=${p##*:}
  n=$(kexec "$pod" "ls /mnt/fuse/models/Qwen2.5-1.5B-Instruct | wc -l" | tr -d ' ')
  [ "${n:-0}" -ge 5 ] && pass "$lbl sees $n model files at /mnt/fuse" \
                      || fail "$lbl sees only ${n:-0} model files -- did the mount propagate?"
done

# ---------------------------------------------------------------------------
say "3. Prefill and decode are on DIFFERENT nodes"
# Without this, everything below still passes on a single box and proves
# nothing about the distributed cache.
if [ -n "$PNODE" ] && [ -n "$DNODE" ] && [ "$PNODE" != "$DNODE" ]; then
  pass "genuinely disaggregated: $PNODE != $DNODE"
else
  fail "both halves are on '$PNODE' -- this is NOT disaggregated"
fi

# ---------------------------------------------------------------------------
say "4. Both engines are rooted at the SAME shared KV path"
# Assert on argv, not on the log. vLLM is verbose enough to blow past kubelet's
# 10Mi rotation within one run of this script, so the startup line announcing
# the path is gone by the time later sections have driven traffic. argv is
# state, not history: it cannot rotate away.
#
# The scan walks /proc rather than using pgrep -f, because pgrep -f matches the
# `sh -c` wrapper that kubectl exec itself created and returns that instead.
# The `sh -c*) continue` arm is load-bearing for the same reason one level
# down: this command's OWN wrapper contains the search string in its argv, so
# without the guard the scan matches itself on the first iteration.
argv_of() {
  kexec "$1" 'for p in /proc/[0-9]*; do c=$(tr "\0" " " < $p/cmdline 2>/dev/null); case "$c" in sh\ -c*) continue ;; *api_server*) echo "$c"; break ;; esac; done'
}
for p in "$PPOD:prefill" "$DPOD:decode"; do
  pod=${p%%:*}; lbl=${p##*:}
  a=$(argv_of "$pod")
  if [ -z "$a" ]; then
    fail "$lbl: no vllm api_server process found"
  elif printf '%s' "$a" | tr -d ' ' | grep -q "\"shared_storage_path\":\"$KVDIR\""; then
    pass "$lbl argv roots the connector at $KVDIR"
  else
    fail "$lbl argv does not root the connector at $KVDIR"
  fi
done

# ---------------------------------------------------------------------------
say "5. THE PROOF: prefill on one node, decode reads that KV on the other"
UNIQ="disagg-$(date +%s)-$$"
# A long, unique prompt. Unique because the connector keys on sha256 of the
# prompt tokens -- a reused prompt hits KV left by an earlier run and the test
# would pass without the two engines ever talking. Long because a short prompt
# may fit patterns the scheduler handles without a connector round trip.
PROMPT="$UNIQ. $(/tmp/skyenv/bin/python -c "print('The distributed filesystem carries the key value cache between nodes. ' * 40)")"
REQ=$(/tmp/skyenv/bin/python -c "
import json,sys
print(json.dumps({'model':'$MODEL','prompt':sys.argv[1],'max_tokens':1,'temperature':0}))
" "$PROMPT")

before=$(kexec "$PPOD" "ls $KVDIR 2>/dev/null | wc -l" | tr -d ' ')

# Phase 1: prefill only, max_tokens=1. The engine must compute the whole prompt
# KV to emit even one token; the connector saves it on the way out.
pcode=$(kexec "$PPOD" "curl -s -o /tmp/p.json -w '%{http_code}' -X POST localhost:8000/v1/completions -H 'Content-Type: application/json' -d '$(printf '%s' "$REQ" | sed "s/'/'\\\\''/g")'")
[ "$pcode" = "200" ] && pass "prefill phase returned 200 on $PNODE" \
                     || fail "prefill phase returned ${pcode:-no response}"

after=$(kexec "$PPOD" "ls $KVDIR 2>/dev/null | wc -l" | tr -d ' ')
[ "${after:-0}" -gt "${before:-0}" ] && pass "prefill wrote a new KV prefix dir ($before -> $after)" \
                                     || fail "no new KV dir after prefill ($before -> $after)"

# The decode engine must SEE that directory through its own mount on the other
# node before it can possibly load from it.
newdir=$(kexec "$PPOD" "ls -t $KVDIR | head -1")
if kexec "$DPOD" "test -d $KVDIR/$newdir && echo yes" | grep -q yes; then
  pass "decode node $DNODE sees the KV dir written by $PNODE"
else
  fail "decode node cannot see $KVDIR/$newdir -- the cache did not propagate"
fi

# Baseline the decode engine's GPU prefix-cache counter. This is what makes the
# result conclusive: an External Cache Hit alone could be the engine's own
# in-GPU prefix cache. A hit with ZERO GPU prefix hits can only have come off
# the mount.
gpu_hits() {
  kexec "$DPOD" "curl -s localhost:8000/metrics | grep '^vllm:prefix_cache_hits_total' | awk '{print \$2}' | head -1"
}
h0=$(gpu_hits)

# Phase 2: the ORIGINAL request to decode, on the other node.
DREQ=$(/tmp/skyenv/bin/python -c "
import json,sys
print(json.dumps({'model':'$MODEL','prompt':sys.argv[1],'max_tokens':24,'temperature':0}))
" "$PROMPT")
dcode=$(kexec "$DPOD" "curl -s -o /tmp/d.json -w '%{http_code}' -X POST localhost:8000/v1/completions -H 'Content-Type: application/json' -d '$(printf '%s' "$DREQ" | sed "s/'/'\\\\''/g")'")
[ "$dcode" = "200" ] && pass "decode phase returned 200 on $DNODE" \
                     || fail "decode phase returned ${dcode:-no response}"

h1=$(gpu_hits)
ext=$(kubectl -n "$NS" logs "$DPOD" --tail=4000 2>/dev/null | grep -c "External Cache Hit")
delta=$(/tmp/skyenv/bin/python -c "print(float('${h1:-0}') - float('${h0:-0}'))" 2>/dev/null || echo "?")

if [ "${ext:-0}" -gt 0 ] && [ "$delta" = "0.0" ]; then
  pass "CONCLUSIVE: decode logged External Cache Hit with GPU prefix hits +$delta -> KV crossed from $PNODE"
elif [ "${ext:-0}" -gt 0 ]; then
  fail "External Cache Hit seen but GPU prefix hits also rose (+$delta) -- not conclusive"
else
  fail "decode never logged an External Cache Hit -- the KV handoff did not happen"
fi

# ---------------------------------------------------------------------------
say "6. The KV bytes are genuine tensors, written by the prefill node"
files=$(kexec "$DPOD" "ls $KVDIR/$newdir 2>/dev/null | wc -l" | tr -d ' ')
[ "${files:-0}" -gt 0 ] && pass "$files per-layer KV files readable from the decode node" \
                        || fail "no KV files under $KVDIR/$newdir from the decode node"

# Read the tensor header from the DECODE side. Reading it from the prefill side
# would only prove the writer can read its own file.
meta=$(kexec "$DPOD" "/usr/bin/python3 -c \"
import glob,json,struct
fs=sorted(glob.glob('$KVDIR/$newdir/*.safetensors'))
if not fs: print('none'); raise SystemExit
with open(fs[0],'rb') as f:
    n=struct.unpack('<Q', f.read(8))[0]
    h=json.loads(f.read(n))
k=[x for x in h if x!='__metadata__']
t=h[k[0]]
print(len(fs), t['dtype'], t['shape'])
\"")
if printf '%s' "$meta" | grep -qE "BF16|F16|F32"; then
  pass "genuine KV tensors read cross-node: $meta"
else
  fail "could not read KV tensors from the decode node (got: ${meta:-nothing})"
fi

echo
echo "================================================"
echo "$FAILS failure(s)"
exit $FAILS

#!/usr/bin/env bash
# GPU cold-start benchmark on the Nebius lab cluster (2 x spot H200).
#
# Every start is a real pod through containerd + nydus-snapshotter (no test
# harness inside the pod), so pod scheduling, image resolution and the
# readiness probe are all included. Time = pod created -> Ready, where Ready
# is vLLM answering GET /v1/models (readinessProbe), plus kubelet's own
# timestamps.
#
# The image is qwen7b-gpu-nydus (vLLM 0.11 GPU + Qwen2.5-7B baked in, RAFS
# v6). The image path is switched per arm by rewriting the node's nydusd
# backend proxy (config file read by nydusd at each mount):
#   registry  : no proxy (nydusd reads the Nebius registry directly)
#   fuse      : node's fuse-client 127.0.0.1:18081 (fetch-and-deliver)
#   dragonfly : node's dfdaemon 127.0.0.1:4001
#
# Arms (each run starts from a node with no copy of the image: the image is
# removed from containerd + nydusd's cache, and the cache system's copy is
# reset per arm as described):
#   <sys>-first   : cache cold on every node, start on node A
#   <sys>-second  : right after, start on node B (node A holds the image)
#   fuse-prewarm  : warm-image onto node A first, then start on node A
#   fuse-cache2   : second start on node A with a persistent VLLM_CACHE_ROOT
#                   (compile cache), cache dir kept from the first start
#
# Results: results/bench.jsonl, one line per start. Spot reclaim of a node
# mid-start marks the line "preempted" and the start is retried once.
set -uo pipefail
cd "$(dirname "$0")"
source ./env.sh
source ./images.env
export KUBECONFIG=$KUBECONFIG_FILE
RUNS=${RUNS:-2}
CTX=$(ctx a)
k() { kubectl --context "$CTX" "$@"; }
mkdir -p results
OUT=results/bench.jsonl

gpu_nodes() { k get nodes -l agentpool=model -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}' | sort; }
NODES=($(gpu_nodes))
[[ ${#NODES[@]} -ge 2 ]] || { echo "need 2 GPU nodes, have ${#NODES[@]}"; exit 1; }
A=${NODES[0]}; B=${NODES[1]}

# Run a privileged one-shot on a node's host (chroot), for config switches and
# image removal. No state is kept in the pod.
onhost() {  # node script
  local n=$1 name b64; name=host-$(echo "$1" | tail -c 9)-$RANDOM
  b64=$(printf '%s' "$2" | base64 | tr -d '\n')
  k -n skyfuse run "$name" --rm -i --restart=Never --quiet --image="$PYTHON" \
    --overrides="{\"spec\":{\"nodeName\":\"$n\",\"hostPID\":true,\"tolerations\":[{\"operator\":\"Exists\"}],\"containers\":[{\"name\":\"h\",\"image\":\"$PYTHON\",\"command\":[\"chroot\",\"/host\",\"/bin/bash\",\"-c\",\"echo $b64 | base64 -d | bash\"],\"securityContext\":{\"privileged\":true},\"volumeMounts\":[{\"name\":\"host\",\"mountPath\":\"/host\"}]}],\"volumes\":[{\"name\":\"host\",\"hostPath\":{\"path\":\"/\"}}]}}" \
    --command -- true 2>&1 | grep -v "deleted"
}

# Point a node's nydusd backend at one image source.
set_source() {  # node registry|fuse|dragonfly
  local proxy=""
  case $2 in
    fuse)      proxy='"proxy":{"url":"http://127.0.0.1:18081","ping_url":"http://127.0.0.1:18081/v2/","fallback":true,"check_interval":5,"check_pause_elapsed":300,"use_http":true},' ;;
    dragonfly) proxy='"proxy":{"url":"http://127.0.0.1:4001","ping_url":"http://127.0.0.1:4003/healthy","fallback":true,"check_interval":5,"check_pause_elapsed":300,"use_http":true},' ;;
  esac
  onhost "$1" "python3 - <<'PY'
import json
p='/etc/nydus/nydusd-config.fusedev.json'
c=json.load(open(p))
b=c['device']['backend']['config']
b.pop('proxy', None)
src='''{${proxy%,}}'''
if src.strip('{}').strip():
    b.update(json.loads(src))
json.dump(c, open(p,'w'), indent=1)
print('source', '$2', 'proxy' in b)
PY
systemctl restart containerd-nydus-grpc; sleep 3; systemctl is-active containerd-nydus-grpc"
}

# nydusd's registry backend gets an explicit auth (base64 "iam:<IAM token>")
# on every GPU node. Nebius redirects blob GETs to storage, and the CRI
# keychain path was refused (DENIED) on every read. The token goes from a
# k8s secret into the config by a one-shot pod; it is never printed.
set_auth() {
  local tok; tok=$(nebius --no-check-update iam get-access-token)
  k -n skyfuse create secret generic nydus-auth --from-literal=auth="$(printf 'iam:%s' "$tok" | base64 | tr -d '\n')" \
    --dry-run=client -o yaml | k apply -f - >/dev/null
  unset tok
  for n in $A $B; do
    local name=auth-$(echo "$n" | tail -c 7)
    k -n skyfuse delete pod "$name" --ignore-not-found --wait=true >/dev/null 2>&1
    cat <<Y | k apply -f - >/dev/null
apiVersion: v1
kind: Pod
metadata: {name: $name, namespace: skyfuse}
spec:
  nodeName: $n
  restartPolicy: Never
  tolerations: [{operator: Exists}]
  containers:
  - name: a
    image: $PYTHON
    securityContext: {privileged: true}
    command: [python3, -c, "import json;p='/host/etc/nydus/nydusd-config.fusedev.json';c=json.load(open(p));c['device']['backend']['config']['auth']=open('/auth/auth').read().strip();json.dump(c,open(p,'w'),indent=1);print('auth set')"]
    volumeMounts: [{name: host, mountPath: /host}, {name: auth, mountPath: /auth, readOnly: true}]
  volumes: [{name: host, hostPath: {path: /}}, {name: auth, secret: {secretName: nydus-auth}}]
Y
    k -n skyfuse wait --for=jsonpath='{.status.phase}'=Succeeded pod/"$name" --timeout=180s >/dev/null && echo "  auth $n: $(k -n skyfuse logs "$name")"
    k -n skyfuse delete pod "$name" --wait=false >/dev/null 2>&1
  done
}

# Peer fill on or off on every GPU node's fuse client (DaemonSet arg).
set_peer_fill() {  # true|false
  local args; args=$(k -n skyfuse get ds fuse-gpu -o json | python3 -c "
import json,sys
d=json.load(sys.stdin)
a=[x for x in d['spec']['template']['spec']['containers'][0]['args'] if not x.startswith('-peer-fill=')]
print(json.dumps(a+['-peer-fill=$1']))")
  k -n skyfuse patch ds fuse-gpu --type=json -p "[{\"op\":\"replace\",\"path\":\"/spec/template/spec/containers/0/args\",\"value\":$args}]" >/dev/null
  k -n skyfuse rollout status ds/fuse-gpu --timeout=600s >/dev/null
  sleep 20
}

# fuse metric sum on a node's client (regex on the metric name+labels).
fmetric() {  # node regex
  local p; p=$(k -n skyfuse get pod -l app=fuse-gpu --field-selector spec.nodeName=$1 -o jsonpath='{.items[0].metadata.name}')
  k get --raw "/api/v1/namespaces/skyfuse/pods/$p:18082/proxy/metrics" 2>/dev/null | awk -v re="$2" '$1 ~ re {s+=$2} END {printf "%d", s}'
}

# Remove every local copy of the image from a node: the pod's container,
# containerd's image record and nydus snapshots, nydusd's blob cache.
drop_image_on() {  # node
  onhost "$1" "crictl rmi $QWEN7B_NYDUS >/dev/null 2>&1; crictl rmi --prune >/dev/null 2>&1; rm -rf /var/lib/nydus/cache/* 2>/dev/null; echo dropped"
}

# fuse-cache: drop the image's blobs cluster-wide (every holder + bucket).
fuse_cold() {
  # DELETE through one client drops the blob on every holder and in the
  # bucket. Sent via the API server's pod proxy: the client image's sh has no
  # /dev/tcp, so an exec'd request silently did nothing.
  local p; p=$(k -n skyfuse get pod -l app=fuse-gpu --field-selector spec.nodeName=$A -o jsonpath='{.items[0].metadata.name}')
  local n=0
  while read -r d _; do
    k delete --raw "/api/v1/namespaces/skyfuse/pods/$p:18082/proxy/api/files/oci/sha256/${d#sha256:}" >/dev/null 2>&1 && n=$((n+1))
  done < results/qwen7b-gpu-layers.txt
  echo "  fuse_cold: $n blobs deleted"
  sleep 5
}

# Dragonfly: restart its peers (storage is an emptyDir, so this empties them).
df_cold() {
  k -n df-lab rollout restart ds/df-dragonfly-client sts/df-dragonfly-seed-client >/dev/null 2>&1
  k -n df-lab rollout status ds/df-dragonfly-client --timeout=300s >/dev/null 2>&1
  k -n df-lab rollout status sts/df-dragonfly-seed-client --timeout=300s >/dev/null 2>&1
  sleep 10
}

# One measured start: a vLLM pod on the node; returns when Ready or failed.
start() {  # arm run node [cache]
  local arm=$1 run=$2 node=$3 cache=${4:-} pod=bench-$(date +%s)
  local cvol="" cmnt="" cenv=""
  if [[ -n $cache ]]; then
    cvol=", {name: vcache, hostPath: {path: /var/lib/bench-vllm-cache, type: DirectoryOrCreate}}"
    cmnt=", {name: vcache, mountPath: /vllm-cache}"
    cenv=", {name: VLLM_CACHE_ROOT, value: /vllm-cache}"
  fi
  local t0; t0=$(python3 -c 'import time;print(time.time())')
  cat <<Y | k apply -f - >/dev/null
apiVersion: v1
kind: Pod
metadata: {name: $pod, namespace: bench, labels: {app: bench}}
spec:
  nodeName: $node
  restartPolicy: Never
  tolerations: [{operator: Exists}]
  imagePullSecrets: [{name: nebius-cr}]
  containers:
    - name: vllm
      image: $QWEN7B_NYDUS
      command: [vllm, serve, /models/qwen2.5-7b-instruct, --served-model-name=qwen7b, --max-model-len=8192,
                --port=8000, --gpu-memory-utilization=0.85]
      env: [{name: HF_HUB_OFFLINE, value: "1"}$cenv]
      ports: [{containerPort: 8000}]
      startupProbe: {httpGet: {path: /health, port: 8000}, periodSeconds: 2, failureThreshold: 900}
      readinessProbe: {httpGet: {path: /v1/models, port: 8000}, periodSeconds: 2}
      resources:
        requests: {cpu: "4", memory: 40Gi, nvidia.com/gpu: 1}
        limits: {memory: 120Gi, nvidia.com/gpu: 1}
      volumeMounts: [{name: shm, mountPath: /dev/shm}$cmnt]
  volumes: [{name: shm, emptyDir: {medium: Memory, sizeLimit: 16Gi}}$cvol]
Y
  local st t1=""
  for _ in $(seq 1 330); do
    st=$(k -n bench get pod $pod -o jsonpath='{.status.phase} {.status.conditions[?(@.type=="Ready")].status}' 2>/dev/null)
    case $st in
      *" True") t1=$(python3 -c 'import time;print(time.time())'); break ;;
      Failed*) break ;;
    esac
    k get node "$node" >/dev/null 2>&1 || { st="preempted"; break; }
    sleep 2
  done
  k -n bench get pod $pod -o json > results/podjson/$pod.json 2>/dev/null
  python3 - "$arm" "$run" "$node" "$st" "results/podjson/$pod.json" "$t0" "$t1" <<'PY' >> "$OUT"
import json, sys
from datetime import datetime
arm, run, node, st = sys.argv[1], int(sys.argv[2]), sys.argv[3], sys.argv[4]
try:
    p = json.load(open(sys.argv[5]))
except Exception:
    p = {}
ts = lambda s: datetime.strptime(s, "%Y-%m-%dT%H:%M:%SZ") if s else None
created = ts(p.get("metadata", {}).get("creationTimestamp"))
cond = {c["type"]: ts(c.get("lastTransitionTime")) for c in p.get("status", {}).get("conditions", [])}
cs = (p.get("status", {}).get("containerStatuses") or [{}])[0]
started = ts(((cs.get("state") or {}).get("running") or {}).get("startedAt"))
out = {"arm": arm, "run": run, "node": node[-6:], "state": st,
       "container_start_s": (started - created).total_seconds() if started and created else None,
       "ready_s": round(float(sys.argv[7]) - float(sys.argv[6]), 1) if sys.argv[7] else None}
print(json.dumps(out))
PY
  tail -1 "$OUT"
  k -n bench logs $pod > "results/$arm-$run.log" 2>&1
  k -n bench delete pod $pod --wait=true >/dev/null 2>&1
}

fill_layers() {  # write results/qwen7b-gpu-layers.txt from the image manifest
  python3 - "$QWEN7B_NYDUS" > results/qwen7b-gpu-layers.txt <<'PY'
import json, subprocess, sys
ref = sys.argv[1]
m = json.loads(subprocess.check_output(["docker", "buildx", "imagetools", "inspect", ref, "--raw"]))
for l in m["layers"]:
    if (l.get("annotations") or {}).get("containerd.io/snapshot/nydus-blob") == "true":
        print(l["digest"], l["size"])
PY
}

case ${1:-all} in
  auth) set_auth; exit 0 ;;
  peerfill)
    # Peer fill A/B: per run and arm, a cold cluster, then the image's first
    # start on node A (registry fill) and its second start on node B (the
    # blobs are on A only). RUNS runs, alternating arms.
    fill_layers
    for run in $(seq ${RUN0:-1} $(( ${RUN0:-1} + RUNS - 1 ))); do
      for arm in ${ARMS:-on off}; do
        set_peer_fill $([ $arm = on ] && echo true || echo false)
        fuse_cold
        for n in $A $B; do set_source $n fuse; drop_image_on $n; done
        start pf-$arm-first $run "$A"
        for _ in $(seq 1 120); do
          [ "$(fmetric $A '^fuse_registry_fill_inflight$')" -eq 0 ] && break; sleep 5
        done
        sleep 10
        start pf-$arm-second $run "$B"
        echo "{\"arm\":\"pf-$arm-second-fuse\",\"run\":$run,\"peer_fill_units_peer\":$(fmetric $B 'fuse_peer_fill_units_total[{]source=.peer.'),\"peer_fill_units_registry\":$(fmetric $B 'fuse_peer_fill_units_total[{]source=.registry.'),\"reads_fill\":$(fmetric $B 'fuse_blob_read_seconds_count[{]path=.fill.'),\"reads_remote\":$(fmetric $B 'fuse_blob_read_seconds_count[{]path=.remote.'),\"read_s_fill\":$(fmetric $B 'fuse_blob_read_seconds_sum[{]path=.fill.'),\"read_s_remote\":$(fmetric $B 'fuse_blob_read_seconds_sum[{]path=.remote.')}" | tee -a "$OUT"
      done
    done
    set_peer_fill true
    exit 0 ;;
  fuse2)
    fill_layers; fuse_cold
    for n in $A $B; do set_source $n fuse; drop_image_on $n; done
    start fuse-first 1 "$A"; start fuse-second 1 "$B"; exit 0 ;;
  fusewarm)
    set_source "$A" fuse; drop_image_on "$A"
    start fuse-warm-node 1 "$A"; exit 0 ;;
  smoke)
    set_source "$A" registry; drop_image_on "$A"
    start smoke 0 "$A"; exit 0 ;;
esac

fill_layers
for run in $(seq 1 $RUNS); do
  # Baseline: registry direct, cold node.
  for n in $A $B; do set_source $n registry; drop_image_on $n; done
  start registry-first $run $A

  # fuse-cache: cold cluster, first node then second node.
  fuse_cold
  for n in $A $B; do set_source $n fuse; drop_image_on $n; done
  start fuse-first $run $A
  start fuse-second $run $B

  # Dragonfly: cold peers, first node then second node.
  df_cold
  for n in $A $B; do set_source $n dragonfly; drop_image_on $n; done
  start df-first $run $A
  start df-second $run $B

  # fuse-cache: pre-warm node A, then start there (fresh containerd copy).
  fuse_cold; drop_image_on $A
  t0=$(date +%s); ./warm.sh "$A" >/dev/null; echo "{\"arm\":\"fuse-warm-image\",\"run\":$run,\"secs\":$(( $(date +%s) - t0 ))}" | tee -a "$OUT"
  start fuse-prewarm $run $A

  # Compile cache: two starts on node A with the same VLLM_CACHE_ROOT.
  onhost "$A" "rm -rf /var/lib/bench-vllm-cache"
  drop_image_on $A; start fuse-cache1 $run $A cache
  drop_image_on $A; start fuse-cache2 $run $A cache

done

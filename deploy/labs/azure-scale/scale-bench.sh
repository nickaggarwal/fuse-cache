#!/usr/bin/env bash
# Scale-lab cold starts: vLLM (CPU build, Qwen2.5-7B baked into the 13.5 GB
# nydus image models/qwen7b-cpu) through nydusd + fuse-cache on many nodes.
# Each start is a privileged nydusd pod on one node: time from "image may be
# read" to vLLM answering /v1/models (ready-start.sh).
#
#   ./scale-bench.sh family    one cold start per family (first node of
#                              each pool), cluster cold before each
#   ./scale-bench.sh fanout    cold cluster; node 1 starts alone (registry
#                              fill), then every other node starts at once
#   ./scale-bench.sh stagger   cold cluster; a new node starts every 30 s
#
# Results: results/<mode>.jsonl, one line per start, with node family and
# fuse per-read profile deltas.
set -u
cd "$(dirname "$0")"
source ./env.sh
K="kubectl --context $CTX -n fc-scale"
# The test image (nydus, vLLM CPU + Qwen2.5-7B) and the nydusd image, both in
# $ACR. q7layers.txt lists the image's blob digests and sizes; q7boot/image
# holds its RAFS bootstrap and blob.meta files (fetch them from the image's
# last layer once).
R=$ACR.azurecr.io; REPO=${REPO:-models/qwen7b-cpu}
NYDUS_IMG=${NYDUS_IMG:?set NYDUS_IMG (nydus-node image with nydusd)}
MODE=${1:?mode}
OUT=results/$MODE.jsonl
mkdir -p results

nodes() { kubectl --context $CTX get nodes -l fc-scale/role=model -o jsonpath='{range .items[*]}{.metadata.name}{" "}{.metadata.labels.fc-scale/family}{"\n"}{end}' | sort; }
fc_pod() { $K get pod -l app=fuse-client --field-selector spec.nodeName=$1 -o jsonpath='{.items[0].metadata.name}'; }
fc_ip()  { $K get pod -l app=fuse-client --field-selector spec.nodeName=$1 -o jsonpath='{.items[0].status.podIP}'; }
metric() { kubectl --context $CTX get --raw "/api/v1/namespaces/fc-scale/pods/$(fc_pod $1):8092/proxy/metrics" 2>/dev/null | awk -v re="$2" '$1 ~ re {s+=$2} END {printf "%.3f", s}'; }

token() {
  local RT AT
  RT=$(az acr login -n $ACR --expose-token --query accessToken -o tsv --subscription $SUB 2>/dev/null)
  AT=$(curl -s "https://$R/oauth2/token" -d "grant_type=refresh_token&service=$R&scope=repository:$REPO:pull&refresh_token=$RT" \
    | python3 -c 'import sys,json; print(json.load(sys.stdin).get("access_token",""))')
  [ -n "$AT" ] || { echo "no token"; exit 2; }
  umask 077; printf %s "$AT" > "$STATE/acr-pull.tok"; umask 022
}

cold_fc() {  # drop the image's blobs on every holder and in blob storage
  local p; p=$(fc_pod "$(nodes | head -1 | cut -d' ' -f1)")
  while read -r d _; do
    kubectl --context $CTX delete --raw "/api/v1/namespaces/fc-scale/pods/$p:8092/proxy/api/files/oci/sha256/${d#sha256:}" >/dev/null 2>&1
  done < q7layers.txt
  sleep 5
}

# start NODE ARM -> one JSON line (runs a nydusd + vLLM pod on NODE).
start() {
  local node=$1 arm=$2 fam=$3 name=rdy-$3-$(echo "$1" | tail -c 3) proxy="http://$(fc_ip $1):8091"
  local AT; AT=$(cat "$STATE/acr-pull.tok")
  local cfg; cfg=$(mktemp)
  cat > "$cfg" <<J
{"device":{"backend":{"type":"registry","config":{"host":"$R","repo":"$REPO","scheme":"https","registry_token":"$AT","timeout":30,"connect_timeout":10,"retry_limit":3,
 "proxy":{"url":"$proxy","ping_url":"$proxy/v2/","fallback":true,"check_interval":5,"check_pause_elapsed":300,"use_http":true}}},
 "cache":{"type":"blobcache","config":{"work_dir":"/nydus/cache"}}},
 "mode":"direct","digest_validate":false,"iostats_files":false,"enable_xattr":true,"amplify_io":1048576,
 "fs_prefetch":{"enable":true,"threads_count":4,"merging_size":1048576,"bandwidth_rate":0,"prefetch_all":false}}
J
  $K delete pod $name --ignore-not-found --wait=true >/dev/null 2>&1
  $K create configmap $name --from-file=nydusd-config.json="$cfg" --from-file=ready-start.sh=ready-start.sh --dry-run=client -o yaml | $K apply -f - >/dev/null
  rm -f "$cfg"
  local b_fill b_rem b_sfill b_srem
  b_fill=$(metric $node 'fuse_blob_read_seconds_count[{]path=.fill.'); b_rem=$(metric $node 'fuse_blob_read_seconds_count[{]path=.remote.')
  b_sfill=$(metric $node 'fuse_blob_read_seconds_sum[{]path=.fill.'); b_srem=$(metric $node 'fuse_blob_read_seconds_sum[{]path=.remote.')
  cat <<Y | $K apply -f - >/dev/null
apiVersion: v1
kind: Pod
metadata: {name: $name, namespace: fc-scale, labels: {app: rdy}}
spec:
  nodeName: $node
  restartPolicy: Never
  containers:
    - name: nydusd
      image: $NYDUS_IMG
      command: ["sh", "-c", "mkdir -p /nydus/cache /mnt/img /boot-in && until [ -f /boot-in/ready ]; do sleep 0.1; done && cp /boot-in/*.blob.meta /nydus/cache/ && exec /opt/nydus/bin/nydusd --config /cfg/nydusd-config.json --mountpoint /mnt/img --bootstrap /boot-in/image.boot --apisock /tmp/api.sock --log-level warn"]
      securityContext: {privileged: true}
      resources: {requests: {cpu: "1", memory: 20Gi}, limits: {memory: 28Gi}}
      volumeMounts: [{name: cfg, mountPath: /cfg}, {name: devfuse, mountPath: /dev/fuse}, {name: ov, mountPath: /ov}]
  volumes:
    - {name: cfg, configMap: {name: $name}}
    - {name: devfuse, hostPath: {path: /dev/fuse}}
    - {name: ov, emptyDir: {}}
Y
  $K wait --for=condition=Ready pod/$name --timeout=300s >/dev/null || { echo "{\"arm\":\"$arm\",\"node\":\"$node\",\"error\":\"pod not ready\"}"; return; }
  for f in q7boot/image/*; do $K cp "$f" $name:/boot-in/$(basename "$f") -c nydusd >/dev/null 2>&1; done
  $K exec $name -c nydusd -- sh -c 'date +%s.%N > /boot-in/t0; touch /boot-in/ready'
  local out; out=$($K exec $name -c nydusd -- sh /cfg/ready-start.sh 2>&1 | tail -1)
  set -- $out
  local ready=${2:-null}; [ "${1:-}" = vllm-died ] && ready=null
  case $ready in ''|*[!0-9.]*) ready=null ;; esac
  python3 - "$arm" "$node" "$fam" "$ready" "$b_fill" "$(metric $node 'fuse_blob_read_seconds_count[{]path=.fill.')" \
    "$b_rem" "$(metric $node 'fuse_blob_read_seconds_count[{]path=.remote.')" "$b_sfill" "$(metric $node 'fuse_blob_read_seconds_sum[{]path=.fill.')" \
    "$b_srem" "$(metric $node 'fuse_blob_read_seconds_sum[{]path=.remote.')" <<'PY'
import json, sys, time
a = sys.argv
f = lambda i: float(a[i])
nf, nr = f(6) - f(5), f(8) - f(7)
print(json.dumps({"arm": a[1], "node": a[3] + "-" + a[2][-2:], "family": a[3], "ready_s": None if a[4] == "null" else float(a[4]),
                  "reads_fill": int(nf), "reads_remote": int(nr),
                  "ms_fill": round(1000 * (f(10) - f(9)) / nf, 2) if nf else None,
                  "ms_remote": round(1000 * (f(12) - f(11)) / nr, 2) if nr else None,
                  "ts": time.strftime("%H:%M:%S")}))
PY
  $K delete pod $name --wait=false >/dev/null 2>&1
}

token
NODES=(); while IFS= read -r l; do [ -n "$l" ] && NODES+=("$l"); done < <(nodes)
[ ${#NODES[@]} -ge 2 ] || { echo "need model nodes"; exit 1; }
case $MODE in
  family)
    seen=""
    for nf in "${NODES[@]}"; do
      read -r n fam <<< "$nf"
      case " $seen " in *" $fam "*) continue ;; esac
      seen="$seen $fam"
      cold_fc; start $n family-cold $fam | tee -a $OUT
      start $n family-warm $fam | tee -a $OUT
    done ;;
  fanout)
    cold_fc
    read -r n1 f1 <<< "${NODES[0]}"
    start $n1 fanout-first $f1 | tee -a $OUT
    sleep 20
    for nf in "${NODES[@]:1}"; do
      read -r n fam <<< "$nf"
      start $n fanout-rest $fam >> $OUT &
    done
    wait; tail -n $(( ${#NODES[@]} - 1 )) $OUT ;;
  stagger)
    cold_fc
    for nf in "${NODES[@]}"; do
      read -r n fam <<< "$nf"
      start $n stagger $fam >> $OUT &
      sleep 30
    done
    wait; tail -n ${#NODES[@]} $OUT ;;
esac
rm -f "$STATE/acr-pull.tok"

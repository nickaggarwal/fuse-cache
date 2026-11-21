#!/usr/bin/env bash
# Create the one-cluster, 2 x spot H200 cache benchmark on Nebius. Idempotent: a
# resource that already exists (by name) is reused.
#
#   ./up.sh            everything: registry, bucket, both clusters, node groups
#   ./up.sh infra      registry + bucket + clusters + CPU node groups (no GPU)
#   ./up.sh gpu        only the GPU node groups (clusters must exist)
#   ./up.sh preflight  only the quota check (creates nothing)
#
# Prints every resource it creates. Teardown: ./down.sh. Leftovers: ./check.sh.
set -euo pipefail
cd "$(dirname "$0")"
source ./env.sh
WHAT=${1:-all}
SSH_PUB=$(cat ~/.ssh/id_ed25519.pub)
CLOUD_INIT="#cloud-config
users:
  - name: ubuntu
    sudo: ALL=(ALL) NOPASSWD:ALL
    shell: /bin/bash
    ssh_authorized_keys:
      - $SSH_PUB
"

idof() { # type parent name -> id or ""
  case $1 in
    cluster)   n mk8s cluster get-by-name --parent-id "$2" --name "$3" --format json 2>/dev/null | python3 -c 'import json,sys; print(json.load(sys.stdin)["metadata"]["id"])' 2>/dev/null || true ;;
    nodegroup) n mk8s node-group get-by-name --parent-id "$2" --name "$3" --format json 2>/dev/null | python3 -c 'import json,sys; print(json.load(sys.stdin)["metadata"]["id"])' 2>/dev/null || true ;;
    registry)  n registry get-by-name --parent-id "$2" --name "$3" --format json 2>/dev/null | python3 -c 'import json,sys; print(json.load(sys.stdin)["metadata"]["id"])' 2>/dev/null || true ;;
    bucket)    n storage bucket get-by-name --parent-id "$2" --name "$3" --format json 2>/dev/null | python3 -c 'import json,sys; print(json.load(sys.stdin)["metadata"]["id"])' 2>/dev/null || true ;;
  esac
}

shared() {
  local id
  id=$(idof registry "$REG_PROJECT" "$REG_NAME")
  if [[ -z $id ]]; then
    echo "+ registry $REG_NAME ($REG_REGION)"
    n registry create --parent-id "$REG_PROJECT" --name "$REG_NAME" --labels "$LABEL" >/dev/null
    id=$(idof registry "$REG_PROJECT" "$REG_NAME")
  fi
  echo "$id" > "$STATE/registry.id"; echo "  registry $id"
  id=$(idof bucket "$REG_PROJECT" "$BUCKET")
  if [[ -z $id ]]; then
    echo "+ bucket $BUCKET ($REG_REGION)"
    n storage bucket create --parent-id "$REG_PROJECT" --name "$BUCKET" --labels "$LABEL" >/dev/null
    id=$(idof bucket "$REG_PROJECT" "$BUCKET")
  fi
  echo "$id" > "$STATE/bucket.id"; echo "  bucket $id"
  store_iam
}

# The fuse store's S3 identity: a service account in a group that holds
# storage.editor on this bucket only, and a static access key for it. The
# secret goes straight to $STATE/s3.env (mode 600), never to stdout.
store_iam() {
  local sa g kid kj
  sa=$(n iam service-account get-by-name --parent-id "$REG_PROJECT" --name "$PFX-store" --format json 2>/dev/null | python3 -c 'import json,sys; print(json.load(sys.stdin)["metadata"]["id"])' 2>/dev/null || true)
  if [[ -z $sa ]]; then
    echo "+ service account $PFX-store"
    sa=$(n iam service-account create --parent-id "$REG_PROJECT" --name "$PFX-store" --labels "$LABEL" --format json | python3 -c 'import json,sys; print(json.load(sys.stdin)["metadata"]["id"])')
  fi
  echo "$sa" > "$STATE/sa.id"
  g=$(n iam group get-by-name --parent-id "$TENANT" --name "$PFX-store" --format json 2>/dev/null | python3 -c 'import json,sys; print(json.load(sys.stdin)["metadata"]["id"])' 2>/dev/null || true)
  if [[ -z $g ]]; then
    echo "+ group $PFX-store (storage.editor on bucket $BUCKET)"
    g=$(n iam group create --parent-id "$TENANT" --name "$PFX-store" --labels "$LABEL" --format json | python3 -c 'import json,sys; print(json.load(sys.stdin)["metadata"]["id"])')
    n iam group-membership create --parent-id "$g" --member-id "$sa" >/dev/null
    n iam access-permit create --parent-id "$g" --resource-id "$(cat "$STATE/bucket.id")" --role storage.editor >/dev/null
  fi
  echo "$g" > "$STATE/group.id"
  if [[ ! -s $STATE/s3.env ]]; then
    echo "+ access key $PFX-store-key"
    kj=$(n iam v2 access-key create --parent-id "$REG_PROJECT" --name "$PFX-store-key" --account-service-account-id "$sa" \
         --secret-delivery-mode inline --description "fuse store, $PFX test" --format json)
    kid=$(echo "$kj" | python3 -c 'import json,sys; print(json.load(sys.stdin)["metadata"]["id"])')
    echo "$kid" > "$STATE/accesskey.id"
    (umask 077
     n iam v2 access-key get --id "$kid" --format json | python3 -c 'import json,sys; d=json.load(sys.stdin); print("AWS_ACCESS_KEY_ID="+d["status"]["aws_access_key_id"])' > "$STATE/s3.env"
     echo "$kj" | python3 -c '
import json,sys
def find(o):
    if isinstance(o,dict):
        for k,v in o.items():
            if "secret" in k.lower() and isinstance(v,str) and len(v)>20: return v
            r=find(v)
            if r: return r
print("AWS_SECRET_ACCESS_KEY="+find(json.load(sys.stdin)))' >> "$STATE/s3.env")
    # A new key can take a few seconds to authorize: wait for one put.
    for _ in $(seq 12); do
      ( set -a; source "$STATE/s3.env"; set +a
        echo ok | aws --endpoint-url "https://storage.$REG_REGION.nebius.cloud" --region "$REG_REGION" s3 cp - "s3://$BUCKET/.probe" --only-show-errors && \
        aws --endpoint-url "https://storage.$REG_REGION.nebius.cloud" --region "$REG_REGION" s3 rm "s3://$BUCKET/.probe" --only-show-errors ) 2>/dev/null && { echo "  s3 key works"; break; }
      sleep 10
    done
  fi
}

cluster() { # name region project subnet
  local c=$1 region=$2 proj=$3 subnet=$4 id
  id=$(idof cluster "$proj" "$PFX-$c")
  if [[ -z $id ]]; then
    echo "+ cluster $PFX-$c ($region, k8s $K8S_VERSION, public endpoint limited to $OPERATOR_IP/32)"
    n mk8s cluster create --parent-id "$proj" --name "$PFX-$c" --labels "$LABEL" \
      --control-plane-version "$K8S_VERSION" --control-plane-subnet-id "$subnet" \
      --control-plane-endpoints-public-endpoint \
      --control-plane-endpoints-public-endpoint-allowed-cidrs "$OPERATOR_IP/32" \
      --control-plane-etcd-cluster-size 1 --timeout 30m >/dev/null
    id=$(idof cluster "$proj" "$PFX-$c")
  fi
  echo "$id" > "$STATE/cluster-$c.id"; echo "  cluster $c $id"
  n mk8s cluster get-credentials --id "$id" --external --context-name "$(ctx "$c")" \
    --kubeconfig "$KUBECONFIG_FILE" --force >/dev/null
}

subnet_of() { echo "$CLUSTERS" | awk -F'|' -v c="$1" '$1==c{print $4}'; }

cpu_group() { # name
  local c=$1 cid id subnet
  cid=$(cat "$STATE/cluster-$c.id"); subnet=$(subnet_of "$c")
  id=$(idof nodegroup "$cid" cpu)
  if [[ -z $id ]]; then
    echo "+ node group $c/cpu: 1 x $CPU_PLATFORM $CPU_PRESET (on demand), $CPU_DISK_GIB GiB $DISK_TYPE"
    n mk8s node-group create --parent-id "$cid" --name cpu --labels "$LABEL" --fixed-node-count 1 \
      --template-resources-platform "$CPU_PLATFORM" --template-resources-preset "$CPU_PRESET" \
      --template-boot-disk-type "$DISK_TYPE" --template-boot-disk-size-gibibytes "$CPU_DISK_GIB" \
      --template-metadata-labels "agentpool=system,skyctl/cache=true" \
      --template-network-interfaces "[{\"subnet_id\":\"$subnet\",\"public_ip_address\":{}}]" \
      --template-cloud-init-user-data "$CLOUD_INIT" --auto-approve --timeout 30m >/dev/null
    id=$(idof nodegroup "$cid" cpu)
  fi
  echo "  node group $c/cpu $id"
}

field_of() { echo "$CLUSTERS" | awk -F'|' -v c="$1" -v f="$2" '$1==c{print $f}'; }

gpu_group() { # name
  local c=$1 cid id subnet plat preset
  cid=$(cat "$STATE/cluster-$c.id"); subnet=$(subnet_of "$c")
  plat=$(field_of "$c" 5); preset=$(field_of "$c" 6)
  id=$(idof nodegroup "$cid" gpu)
  if [[ -z $id ]]; then
    echo "+ node group $c/gpu: ${GPU_NODES:-1} x $plat $preset, SPOT, drivers $GPU_DRIVERS, $GPU_DISK_GIB GiB $DISK_TYPE"
    n mk8s node-group create --parent-id "$cid" --name gpu --labels "$LABEL" --fixed-node-count "${GPU_NODES:-1}" \
      --template-resources-platform "$plat" --template-resources-preset "$preset" \
      --template-gpu-settings-drivers-preset "$GPU_DRIVERS" \
      --template-preemptible --template-follows-spot-price \
      --template-boot-disk-type "$DISK_TYPE" --template-boot-disk-size-gibibytes "$GPU_DISK_GIB" \
      --template-metadata-labels "agentpool=model" \
      --template-network-interfaces "[{\"subnet_id\":\"$subnet\",\"public_ip_address\":{}}]" \
      --template-cloud-init-user-data "$CLOUD_INIT" --auto-approve --timeout 45m >/dev/null
    id=$(idof nodegroup "$cid" gpu)
  fi
  echo "  node group $c/gpu $id"
}

# Quota preflight: every cluster's region must have room, in the TENANT's
# limits (the binding ones), for its CPU node (vCPU), its spot GPU (GPU count
# and preemptible VMs). Nothing is created when any check fails.
quota() { # name region -> "limit used" (limit "" when the quota is absent)
  n quotas quota-allowance get-by-name --parent-id "$TENANT" --name "$1" --region "$2" --format json 2>/dev/null |
    python3 -c 'import json,sys; d=json.load(sys.stdin); print(d.get("spec",{}).get("limit",""), d.get("status",{}).get("usage","0"))' 2>/dev/null || echo " 0"
}
preflight() {
  local ok=1 c region gpuq lim used need cpu_need
  cpu_need=$(n compute platform list --parent-id "$(field_of a 3)" --format json | python3 -c '
import json,sys
for p in json.load(sys.stdin)["items"]:
    if p["metadata"]["name"]==sys.argv[1]:
        for pr in p["spec"]["presets"]:
            if pr["name"]==sys.argv[2]: print(pr["resources"]["vcpu_count"])' "$CPU_PLATFORM" "$CPU_PRESET")
  while IFS='|' read -r c region _ _ plat preset gpuq; do
    [[ -z $c ]] && continue
    # Public IPv4s: one per node (CPU + GPU_NODES), no LoadBalancer.
    local g=${GPU_NODES:-1}
    # Boot disks share one regional size quota per disk type, in bytes (2048 GiB on
    # this tenant): CPU node + every GPU node.
    local disk_q="compute.disk.size.${DISK_TYPE//_/-}"
    for check in "compute.instance.non-gpu.vcpu:${cpu_need:-8}" "$gpuq:$g" "compute.instance.preemptible.count:$g" "vpc.ipv4-address.public.count:$((g+1))" "$disk_q:$(( (${CPU_DISK_GIB:-0} + g * ${GPU_DISK_GIB:-0}) * 1073741824 ))"; do
      local qn=${check%%:*}; need=${check#*:}
      read -r lim used <<< "$(quota "$qn" "$region")"
      if [[ -z $lim || $(( ${lim:-0} - ${used:-0} )) -lt $need ]]; then
        echo "  QUOTA $c ($region): $qn limit=${lim:-none} used=${used:-0} need=$need"; ok=0
      else
        echo "  quota ok $c ($region): $qn $((lim-used)) free, need $need"
      fi
    done
  done <<< "$CLUSTERS"
  [[ $ok == 1 ]] || { echo "quota preflight failed: nothing created"; exit 1; }
}

T0=$(date +%s)
[[ ${SKIP_PREFLIGHT:-0} == 1 ]] || preflight
[[ $WHAT == preflight ]] && exit 0
if [[ $WHAT == all || $WHAT == infra ]]; then
  shared
  while IFS='|' read -r c region proj subnet _; do
    [[ -z $c ]] && continue
    cluster "$c" "$region" "$proj" "$subnet" &
  done <<< "$CLUSTERS"
  wait
  while IFS='|' read -r c _; do [[ -n $c ]] && cpu_group "$c" & done <<< "$CLUSTERS"; wait
fi
if [[ $WHAT == all || $WHAT == gpu ]]; then
  while IFS='|' read -r c _; do [[ -n $c ]] && gpu_group "$c" & done <<< "$CLUSTERS"; wait
fi
echo "up ($WHAT) in $(( $(date +%s) - T0 ))s; kubeconfig $KUBECONFIG_FILE"
KUBECONFIG=$KUBECONFIG_FILE kubectl --context "$(ctx a)" get nodes -o wide 2>/dev/null | sed "s/^/  a: /" || true

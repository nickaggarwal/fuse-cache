#!/usr/bin/env bash
# Deploy the test stack onto both Nebius clusters after up.sh:
#   fuse (coordinator + store on the CPU node, client with passthrough on the
#   H200 node, S3 bucket backend) and the routers (skyctl-aff2: TLS, peer
#   tokens, LB source ranges, controld on cluster a) with vLLM on the H200.
#
#   ./stack.sh fuse       fuse only
#   ./stack.sh routing    routers + controld (+ vLLM with MODEL=...)
#   ./stack.sh            both
set -euo pipefail
cd "$(dirname "$0")"
source ./env.sh
source ./images.env
WHAT=${1:-all}
export KUBECONFIG=$KUBECONFIG_FILE
FLEET3=${FLEET3:-/Users/nilesh/k8s-projects/sky-control/deploy/affinity-e2e/fleet3}
MODEL=${MODEL:-qwen7b}; NAME=${NAME:-qwen7b}
k() { local c=$1; shift; kubectl --context "$(ctx "$c")" "$@"; }

regcreds() { # c ns: pull secret for the Nebius registry (registry is in another project for cluster b)
  local c=$1 ns=$2 tok
  tok=$(nebius --no-check-update iam get-access-token)
  k "$c" create namespace "$ns" --dry-run=client -o yaml | k "$c" apply -f - >/dev/null
  k "$c" -n "$ns" create secret docker-registry nebius-cr --docker-server="cr.$REG_REGION.nebius.cloud" \
    --docker-username=iam --docker-password="$tok" --dry-run=client -o yaml | k "$c" apply -f - >/dev/null
  k "$c" -n "$ns" patch serviceaccount default -p '{"imagePullSecrets":[{"name":"nebius-cr"}]}' >/dev/null
}

CL=${CL:-a}   # clusters to deploy to

fuse() {
  local c
  for c in $CL; do
    regcreds "$c" skyfuse
    k "$c" -n skyfuse create secret generic s3 --from-env-file="$STATE/s3.env" --dry-run=client -o yaml | k "$c" apply -f - >/dev/null
    # The coordinator first, so its ClusterIP can go into the host-network clients.
    sed -e "s#__FUSE__#$FUSE_CLIENT#g" fuse.yaml | python3 -c '
import sys; docs=sys.stdin.read().split("\n---\n")
print("\n---\n".join(d for d in docs if "kind: Namespace" in d or "name: coordinator" in d))' | k "$c" apply -f - >/dev/null
    local coord; coord=$(k "$c" -n skyfuse get svc coordinator -o jsonpath='{.spec.clusterIP}')
    sed -e "s#__FUSE__#$FUSE_CLIENT#g" -e "s#__FRONT__#$STORE_FRONT#g" -e "s#__COORD__#$coord#g" -e "s#__PREFIX__#cluster-$c#g" \
        -e "s#__BUCKET__#$BUCKET#g" -e "s#__REGION__#$REG_REGION#g" \
        -e "s#__S3_ENDPOINT__#https://storage.$REG_REGION.nebius.cloud#g" fuse.yaml | k "$c" apply -f - >/dev/null
    echo "fuse on $c (coordinator $coord)"
  done
  for c in $CL; do
    k "$c" -n skyfuse rollout status deploy/coordinator --timeout=300s >/dev/null
    k "$c" -n skyfuse rollout status ds/fuse-store --timeout=600s >/dev/null
    k "$c" -n skyfuse rollout status ds/fuse-gpu --timeout=900s >/dev/null || true
    k "$c" -n skyfuse get pods -o wide --no-headers | awk -v c=$c '{print "  "c, $1, $2, $3, $7}'
  done
}

routing() {
  local c
  for c in a b; do regcreds "$c" skyctl-aff2; done
  # The merged fleet3 stack, adapted: no StatefulSet from stack.yaml (vllm.yaml
  # replaces it), contexts are the Nebius ones, and the routers' source ranges
  # are the operator plus every node's public IP (peers call each other's LB).
  echo "routing: deploy with fleet3/up.sh adapted for Nebius (see routing.sh)"
}

case $WHAT in
  fuse) fuse ;;
  routing) routing ;;
  all) fuse; routing ;;
esac

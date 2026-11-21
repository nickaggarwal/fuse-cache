#!/usr/bin/env bash
# Install the benchmark stack on the lab cluster after ./up.sh and ./images.sh:
#   1. registry pull secret in every namespace that pulls
#   2. fuse-cache (coordinator + store on the CPU node; client + Rust
#      store-front on every GPU node, nydusd proxy port 18081 on the host)
#   3. nydus-snapshotter on the GPU nodes (containerd restarted once)
#   4. Dragonfly (df-lab): scheduler + seed on the CPU node, dfdaemon on the
#      GPU nodes at 127.0.0.1:4001
#   5. namespace "bench" for the measured vLLM pods
#
#   ./deploy.sh            all of it
#   ./deploy.sh fuse|nydus|dragonfly|bench
set -euo pipefail
cd "$(dirname "$0")"
source ./env.sh
source ./images.env
export KUBECONFIG=$KUBECONFIG_FILE
WHAT=${1:-all}
CTX=$(ctx a)
k() { kubectl --context "$CTX" "$@"; }
RID=$(cat "$STATE/registry.id")
REG=cr.$REG_REGION.nebius.cloud

pullsecret() {  # ns
  local tok; tok=$(nebius --no-check-update iam get-access-token)
  k create namespace "$1" --dry-run=client -o yaml | k apply -f - >/dev/null
  k -n "$1" create secret docker-registry nebius-cr --docker-server="$REG" \
    --docker-username=iam --docker-password="$tok" --dry-run=client -o yaml | k apply -f - >/dev/null
  k -n "$1" patch serviceaccount default -p '{"imagePullSecrets":[{"name":"nebius-cr"}]}' >/dev/null
}

fuse() {
  CL=a ./stack.sh fuse
}

nydus() {
  pullsecret nydus-system
  sed -e "s#__NYDUS_NODE__#$NYDUS_NODE#g" nydus-snapshotter.yaml | k apply -f - >/dev/null
  k -n nydus-system patch serviceaccount default -p '{"imagePullSecrets":[{"name":"nebius-cr"}]}' >/dev/null 2>&1 || true
  k -n nydus-system rollout status ds/nydus-node --timeout=900s
  k -n nydus-system get pods -o wide --no-headers | awk '{print "  nydus", $1, $2, $3, $7}'
}

dragonfly() {
  pullsecret df-lab
  sed -e "s#__REG__#$REG#g" -e "s#__RID__#${RID#registry-}#g" df-values.yaml > "$STATE/df-values.rendered.yaml"
  helm --kube-context "$CTX" upgrade --install df dragonfly/dragonfly --version 1.8.5 -n df-lab \
    -f "$STATE/df-values.rendered.yaml" --wait --timeout 15m >/dev/null
  k -n df-lab get pods -o wide --no-headers | awk '{print "  df", $1, $2, $3, $7}'
}

bench() {
  pullsecret bench
  # The one-shot host helpers in bench.sh run in skyfuse.
  pullsecret skyfuse
  echo "  namespace bench ready"
}

case $WHAT in
  fuse) fuse ;;
  nydus) nydus ;;
  dragonfly) dragonfly ;;
  bench) bench ;;
  all) fuse; nydus; dragonfly; bench ;;
esac

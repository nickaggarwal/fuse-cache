#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
CLUSTER_NAME="${CLUSTER_NAME:-fuse-cache-dev}"
KIND_CONFIG="$ROOT_DIR/scripts/devbox/kind-config.yaml"
CHART_PATH="$ROOT_DIR/charts/fuse-cache"
RELEASE_NAME="${RELEASE_NAME:-fuse-cache}"
NAMESPACE="${NAMESPACE:-fuse-system}"

require_bin() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "Missing dependency: $1" >&2
    exit 1
  }
}

create_cluster() {
  require_bin kind
  require_bin kubectl

  if kind get clusters | rg -x "$CLUSTER_NAME" >/dev/null 2>&1; then
    echo "kind cluster '$CLUSTER_NAME' already exists"
  else
    kind create cluster --name "$CLUSTER_NAME" --config "$KIND_CONFIG"
  fi

  kubectl cluster-info --context "kind-$CLUSTER_NAME"
}

delete_cluster() {
  require_bin kind
  kind delete cluster --name "$CLUSTER_NAME"
}

deploy_chart() {
  require_bin helm
  require_bin kubectl
  helm upgrade --install "$RELEASE_NAME" "$CHART_PATH" --namespace "$NAMESPACE" --create-namespace
  kubectl get pods -n "$NAMESPACE"
}

status() {
  require_bin kubectl
  kubectl get nodes
  kubectl get all -n "$NAMESPACE" || true
}

usage() {
  cat <<USAGE
Usage: $(basename "$0") <create|delete|deploy|status>

Commands:
  create  Create local kind-based dev box for Kubernetes testing
  deploy  Install/upgrade the Fuse Cache Helm chart into the dev box
  status  Show current cluster and workload status
  delete  Delete the local kind cluster
USAGE
}

case "${1:-}" in
  create)
    create_cluster
    ;;
  deploy)
    deploy_chart
    ;;
  status)
    status
    ;;
  delete)
    delete_cluster
    ;;
  *)
    usage
    exit 1
    ;;
esac

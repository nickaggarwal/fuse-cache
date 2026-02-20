#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 3 || $# -gt 4 ]]; then
  echo "Usage: $0 <release> <namespace> <image-tag> [chart-path]"
  echo "Example: $0 fuse-cache-aztest fuse-system-aztest test123 charts/fuse-cache"
  exit 1
fi

RELEASE="$1"
NAMESPACE="$2"
IMAGE_TAG="$3"
CHART_PATH="${4:-charts/fuse-cache}"

helm upgrade "$RELEASE" "$CHART_PATH" \
  -n "$NAMESPACE" \
  --reuse-values \
  --set "image.tag=$IMAGE_TAG"

kubectl -n "$NAMESPACE" rollout status ds/client --timeout=300s
kubectl -n "$NAMESPACE" get pods -l app=client -o wide

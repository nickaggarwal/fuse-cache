#!/usr/bin/env bash
# Install fuse-cache on the scale lab: etcd + coordinator on the system pool,
# the fuse client (Go) + store-front (Rust) on every model node. Blob storage
# is an Azure storage account in the lab resource group (deleted with it).
#   ./deploy.sh [image-digest-ref]
set -euo pipefail
cd "$(dirname "$0")"
source ./env.sh
az() { command az "$@" --subscription "$SUB"; }
K="kubectl --context $CTX"
IMG=${1:?usage: ./deploy.sh <fuse-client image ref>}
FRONT=${FRONT:-$ACR.azurecr.io/fusedata-store-front:pf-220e5b9}
ST=fcscale$(echo "$SUB" | tr -d - | cut -c1-8)

# Blob storage for the cloud tier, in the lab RG; the key goes straight
# into a k8s secret, never to stdout.
az storage account show -g "$RG" -n "$ST" -o none 2>/dev/null || {
  echo "+ storage account $ST"
  az storage account create -g "$RG" -n "$ST" -l "$LOC" --sku Standard_LRS --kind StorageV2 --tags $TAGS -o none
}
KEY=$(az storage account keys list -g "$RG" -n "$ST" --query "[0].value" -o tsv)
az storage container create --account-name "$ST" --account-key "$KEY" -n fuse -o none
$K create namespace fc-scale --dry-run=client -o yaml | $K apply -f - >/dev/null
$K -n fc-scale create secret generic fc-azure --from-literal=AZURE_STORAGE_ACCOUNT="$ST" \
  --from-literal=AZURE_STORAGE_KEY="$KEY" --from-literal=AZURE_CONTAINER_NAME=fuse \
  --dry-run=client -o yaml | $K apply -f - >/dev/null
unset KEY

sed -e "s#__IMG__#$IMG#g" -e "s#__FRONT__#$FRONT#g" fuse.yaml | $K apply -f - >/dev/null
$K -n fc-scale rollout status sts/etcd --timeout=300s >/dev/null
$K -n fc-scale rollout status deploy/coordinator --timeout=300s >/dev/null
$K -n fc-scale rollout status ds/fuse-client --timeout=900s
$K -n fc-scale get pods -o wide --no-headers | awk '{print "  "$1, $2, $3, $7}'

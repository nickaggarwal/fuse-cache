#!/usr/bin/env bash
# Tear down the scale lab: delete the resource group RG (the cluster, its
# node resource group and every pool go with it), then run check.sh.
# Touches nothing outside RG and its AKS-managed node resource group; the
# shared ACR is untouched.
set -uo pipefail
cd "$(dirname "$0")"
source ./env.sh
az() { command az "$@" --subscription "$SUB"; }; AZ=az
T0=$(date +%s)
if $AZ group show -n "$RG" -o none 2>/dev/null; then
  tag=$($AZ group show -n "$RG" --query "tags.purpose" -o tsv)
  [[ $tag == fc-scale ]] || { echo "refusing: $RG is not tagged purpose=fc-scale"; exit 1; }
  echo "- resource group $RG (cluster $CL, all pools)"
  $AZ group delete -n "$RG" --yes -o none
else
  echo "  resource group $RG already gone"
fi
kubectl config delete-context "$CTX" >/dev/null 2>&1 || true
echo "down in $(( $(date +%s) - T0 ))s"
./check.sh

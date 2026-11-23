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
  # An Azure-initiated rolling upgrade (e.g. VM Agent) on a node scale set
  # blocks the cluster delete, and a failed cluster delete rolls the whole
  # resource-group delete back. Cancel any running upgrade, delete the
  # cluster on its own, then the group; retry once.
  MC="MC_${RG}_${CL}_${LOC}"
  for v in $($AZ vmss list -g "$MC" --query "[].name" -o tsv 2>/dev/null); do
    if [[ $($AZ vmss rolling-upgrade get-latest -g "$MC" -n "$v" --query runningStatus.code -o tsv 2>/dev/null) == RollingForward ]]; then
      echo "  cancelling platform rolling upgrade on $v"
      $AZ vmss rolling-upgrade cancel -g "$MC" -n "$v" -o none 2>/dev/null
    fi
  done
  echo "- cluster $CL"
  $AZ aks delete -g "$RG" -n "$CL" --yes -o none 2>/dev/null || $AZ aks delete -g "$RG" -n "$CL" --yes -o none
  echo "- resource group $RG"
  $AZ group delete -n "$RG" --yes -o none || $AZ group delete -n "$RG" --yes -o none
else
  echo "  resource group $RG already gone"
fi
kubectl config delete-context "$CTX" >/dev/null 2>&1 || true
echo "down in $(( $(date +%s) - T0 ))s"
./check.sh

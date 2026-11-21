#!/usr/bin/env bash
# Leftovers check: the lab's resource group, its AKS node resource group
# (MC_*), and anything else in the subscription tagged purpose=fc-scale.
# Exit 0 = nothing left.
set -uo pipefail
cd "$(dirname "$0")"
source ./env.sh
az() { command az "$@" --subscription "$SUB"; }; AZ=az
left=0
for g in "$RG" "MC_${RG}_${CL}_${LOC}"; do
  if $AZ group show -n "$g" -o none 2>/dev/null; then echo "  LEFT resource group $g"; left=1; fi
done
n=$($AZ resource list --tag purpose=fc-scale --query "length(@)" -o tsv 2>/dev/null || echo 0)
if [[ ${n:-0} -gt 0 ]]; then
  echo "  LEFT $n resources tagged purpose=fc-scale:"
  $AZ resource list --tag purpose=fc-scale --query "[].[resourceGroup,type,name]" -o tsv | sed 's/^/    /'
  left=1
fi
[[ $left == 0 ]] && echo "check: nothing left for $RG / purpose=fc-scale"
exit $left

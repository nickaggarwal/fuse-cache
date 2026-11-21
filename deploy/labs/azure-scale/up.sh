#!/usr/bin/env bash
# Create the fuse-cache scale lab on AKS. Idempotent (existing pools are
# reused; a pool's count is set to what POOLS says).
#
#   ./up.sh preflight   quota check per VM family + regional + spot (creates nothing)
#   ./up.sh             resource group, cluster (system pool), every POOLS pool
#   ./up.sh pools       only add/resize the POOLS pools (cluster must exist)
#
# Teardown: ./down.sh (deletes the resource group). Leftovers: ./check.sh.
set -euo pipefail
cd "$(dirname "$0")"
source ./env.sh
WHAT=${1:-all}
az() { command az "$@" --subscription "$SUB"; }; AZ=az

# vCPU need per family + total, against regional, family and spot quota.
preflight() {
  $AZ vm list-usage -l "$LOC" -o json > "$STATE/usage.json"
  $AZ vm list-skus -l "$LOC" --resource-type virtualMachines -o json > "$STATE/skus.json"
  POOLS="$POOLS" SYSTEM_VM="$SYSTEM_VM" python3 - "$STATE/usage.json" "$STATE/skus.json" <<'PY'
import json, os, sys
usage = {u["name"]["value"]: (int(u["currentValue"]), int(u["limit"])) for u in json.load(open(sys.argv[1]))}
skus = {s["name"]: s for s in json.load(open(sys.argv[2]))}
def cpu(vm): return int({c["name"]: c["value"] for c in skus[vm]["capabilities"]}["vCPUs"])
need_fam, need_spot, total, ok = {}, 0, 0, True
pools = [l.split("|") for l in os.environ["POOLS"].split() if l.strip()]
pools.append(["system", os.environ["SYSTEM_VM"], "1", "no"])
for name, vm, count, spot in pools:
    if vm not in skus:
        print(f"  NOT OFFERED {vm} in this region"); ok = False; continue
    if any(r.get("type") == "Location" for r in skus[vm].get("restrictions", [])):
        print(f"  RESTRICTED {vm} for this subscription"); ok = False
    n = cpu(vm) * int(count)
    if spot == "yes":
        need_spot += n
    else:
        fam = skus[vm]["family"]; need_fam[fam] = need_fam.get(fam, 0) + n; total += n
for fam, n in need_fam.items():
    cur, lim = usage.get(fam, (0, 0))
    flag = "ok" if lim - cur >= n else "QUOTA"; ok &= flag == "ok"
    print(f"  {flag:5} {fam:28} need {n:4}  free {lim-cur:4} (limit {lim})")
cur, lim = usage["cores"]
flag = "ok" if lim - cur >= total else "QUOTA"; ok &= flag == "ok"
print(f"  {flag:5} {'regional vCPUs':28} need {total:4}  free {lim-cur:4} (limit {lim})")
if need_spot:
    cur, lim = usage.get("lowPriorityCores", (0, 0))
    flag = "ok" if lim - cur >= need_spot else "QUOTA"; ok &= flag == "ok"
    print(f"  {flag:5} {'spot vCPUs':28} need {need_spot:4}  free {lim-cur:4} (limit {lim})")
sys.exit(0 if ok else 1)
PY
}

cluster() {
  $AZ group show -n "$RG" -o none 2>/dev/null || { echo "+ resource group $RG ($LOC)"; $AZ group create -n "$RG" -l "$LOC" --tags $TAGS -o none; }
  if ! $AZ aks show -g "$RG" -n "$CL" -o none 2>/dev/null; then
    echo "+ cluster $CL (system pool: 1 x $SYSTEM_VM)"
    $AZ aks create -g "$RG" -n "$CL" -l "$LOC" --tags $TAGS \
      --node-count 1 --node-vm-size "$SYSTEM_VM" --nodepool-name system \
      --network-plugin azure --network-plugin-mode overlay \
      --attach-acr "$($AZ acr show -n "$ACR" -g "$ACR_RG" --query id -o tsv)" \
      --generate-ssh-keys --tier free -o none
  fi
  $AZ aks get-credentials -g "$RG" -n "$CL" --context "$CTX" --overwrite-existing -o none
}

pools() {
  while IFS='|' read -r name vm count spot; do
    [[ -z $name ]] && continue
    if $AZ aks nodepool show -g "$RG" --cluster-name "$CL" -n "$name" -o none 2>/dev/null; then
      echo "~ pool $name -> $count"
      $AZ aks nodepool scale -g "$RG" --cluster-name "$CL" -n "$name" --node-count "$count" -o none &
      continue
    fi
    extra=()
    [[ $spot == yes ]] && extra=(--priority Spot --eviction-policy Delete --spot-max-price -1)
    echo "+ pool $name: $count x $vm${spot/yes/ (spot)}"
    # Ephemeral OS disk on the VM's local disk; label model nodes so the
    # fuse client, nydus snapshotter and bench pods select them.
    $AZ aks nodepool add -g "$RG" --cluster-name "$CL" -n "$name" --node-vm-size "$vm" --node-count "$count" \
      --os-disk-type Ephemeral --labels agentpool=$name fc-scale/role=model fc-scale/family=$name \
      --tags $TAGS "${extra[@]}" -o none &
  done <<< "$POOLS"
  wait
}

T0=$(date +%s)
case $WHAT in
  preflight) preflight; exit $? ;;
  pools) pools ;;
  all) preflight || { echo "quota preflight failed: nothing created"; exit 1; }; cluster; pools ;;
esac
echo "up ($WHAT) in $(( $(date +%s) - T0 ))s"
kubectl --context "$CTX" get nodes -L agentpool,node.kubernetes.io/instance-type --no-headers 2>/dev/null | awk '{print "  "$1, $2, $6, $7}'

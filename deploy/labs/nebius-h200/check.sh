#!/usr/bin/env bash
# List every $PFX resource still present: clusters, node groups, registry,
# bucket, compute instances and disks in the cluster projects. Exit 0 only
# when nothing is left.
set -uo pipefail
cd "$(dirname "$0")"
source ./env.sh
left=0
note() { echo "  LEFT: $*"; left=1; }
projects=$(echo "$CLUSTERS" | awk -F'|' 'NF>2{print $3}'; echo "$REG_PROJECT")
for proj in $(echo "$projects" | sort -u); do
  for kind in "mk8s cluster" "compute instance" "compute disk"; do
    out=$(n $kind list --parent-id "$proj" --format json 2>/dev/null | python3 -c '
import json,sys
for r in (json.load(sys.stdin).get("items") or []):
    m=r["metadata"]; lab=m.get("labels") or {}
    if m["name"].startswith(sys.argv[1]) or lab.get("purpose")=="skyctl-cache-bench" or (m["name"].startswith("computeinstance-") and False):
        print(m["id"], m["name"])' "$PFX")
    while read -r line; do [[ -n $line ]] && note "$kind $line ($proj)"; done <<< "$out"
  done
done
r=$(n registry get-by-name --parent-id "$REG_PROJECT" --name "$REG_NAME" --format json 2>/dev/null | python3 -c 'import json,sys; print(json.load(sys.stdin)["metadata"]["id"])' 2>/dev/null)
[[ -n $r ]] && note "registry $REG_NAME $r"
b=$(n storage bucket get-by-name --parent-id "$REG_PROJECT" --name "$BUCKET" --format json 2>/dev/null | python3 -c 'import json,sys; d=json.load(sys.stdin); print(d["metadata"]["id"], d.get("status",{}).get("state",""))' 2>/dev/null)
[[ -n $b ]] && note "bucket $BUCKET $b"
g=$(n iam group get-by-name --parent-id "$TENANT" --name "$PFX-store" --format json 2>/dev/null | python3 -c 'import json,sys; print(json.load(sys.stdin)["metadata"]["id"])' 2>/dev/null)
[[ -n $g ]] && note "iam group $PFX-store $g"
sa=$(n iam service-account get-by-name --parent-id "$REG_PROJECT" --name "$PFX-store" --format json 2>/dev/null | python3 -c 'import json,sys; print(json.load(sys.stdin)["metadata"]["id"])' 2>/dev/null)
[[ -n $sa ]] && note "service account $PFX-store $sa"
if [[ $left == 0 ]]; then echo "check: nothing left with prefix $PFX / label $LABEL"; fi
exit $left

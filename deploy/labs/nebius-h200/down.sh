#!/usr/bin/env bash
# Tear down everything up.sh created, in dependency order, then run check.sh.
#
#   ./down.sh          node groups -> clusters -> registry images + registry -> bucket
#   ./down.sh gpu      only the GPU node groups (stop GPU billing, keep the rest)
#
# Finds resources by name prefix in the two cluster projects and the registry
# project, not only from saved ids, so a half-finished up.sh is cleaned too.
# Never touches anything without the $PFX prefix (e.g. the stopped
# h200-spot-eun1-20261025 VM in eu-north1).
set -uo pipefail
cd "$(dirname "$0")"
source ./env.sh
WHAT=${1:-all}

clusters_in() { # project -> "id name" lines for $PFX-* clusters
  n mk8s cluster list --parent-id "$1" --format json 2>/dev/null | python3 -c '
import json,sys
for c in (json.load(sys.stdin).get("items") or []):
    if c["metadata"]["name"].startswith(sys.argv[1]+"-"): print(c["metadata"]["id"], c["metadata"]["name"])' "$PFX"
}
groups_in() { # cluster id -> "id name"
  n mk8s node-group list --parent-id "$1" --format json 2>/dev/null | python3 -c '
import json,sys
for g in (json.load(sys.stdin).get("items") or []): print(g["metadata"]["id"], g["metadata"]["name"])'
}

T0=$(date +%s)
while IFS='|' read -r c region proj subnet _; do
  [[ -z $c ]] && continue
  clusters_in "$proj" | while read -r cid cname; do
    groups_in "$cid" | while read -r gid gname; do
      if [[ $WHAT == gpu && $gname != gpu ]]; then continue; fi
      echo "- node group $cname/$gname $gid"
      n mk8s node-group delete --id "$gid" --timeout 30m >/dev/null || echo "  ! delete failed: $gid"
    done &
  done
done <<< "$CLUSTERS"
wait
# Clusters only once their node groups are gone: deleting both at once makes
# the node-group deletes fail with "resource is being deleted".
while IFS='|' read -r c region proj subnet _; do
  [[ -z $c ]] && continue
  clusters_in "$proj" | while read -r cid cname; do
    for _ in $(seq 90); do [[ -z $(groups_in "$cid" | grep -v "^$") ]] && break; sleep 10; done
  done
done <<< "$CLUSTERS"
if [[ $WHAT == gpu ]]; then echo "GPU node groups deleted in $(( $(date +%s) - T0 ))s"; exit 0; fi

while IFS='|' read -r c region proj subnet _; do
  [[ -z $c ]] && continue
  clusters_in "$proj" | while read -r cid cname; do
    echo "- cluster $cname $cid"
    n mk8s cluster delete --id "$cid" --timeout 30m >/dev/null || echo "  ! delete failed: $cid"
  done &
done <<< "$CLUSTERS"
wait

rid=$(n registry get-by-name --parent-id "$REG_PROJECT" --name "$REG_NAME" --format json 2>/dev/null | python3 -c 'import json,sys; print(json.load(sys.stdin)["metadata"]["id"])' 2>/dev/null)
if [[ -n $rid ]]; then
  echo "- registry $REG_NAME $rid (with its images)"
  n registry delete --id "$rid" --force >/dev/null || echo "  ! delete failed: $rid"
fi
bid=$(n storage bucket get-by-name --parent-id "$REG_PROJECT" --name "$BUCKET" --format json 2>/dev/null | python3 -c 'import json,sys; print(json.load(sys.stdin)["metadata"]["id"])' 2>/dev/null)
if [[ -n $bid ]]; then
  # Immediate purge (ttl 0): the default keeps a deleted bucket 7 days.
  if [[ -s $STATE/s3.env ]]; then
    ( set -a; source "$STATE/s3.env"; set +a
      aws --endpoint-url "https://storage.$REG_REGION.nebius.cloud" s3 rm "s3://$BUCKET" --recursive --only-show-errors ) \
      && echo "  emptied s3://$BUCKET" || echo "  ! could not empty s3://$BUCKET"
  fi
  echo "- bucket $BUCKET $bid (purged now)"
  n storage bucket delete --id "$bid" --ttl 0s >/dev/null || echo "  ! delete failed: $bid (empty it first: objects block deletion)"
  # A non-empty bucket ends in ScheduledForDeletion: purge removes it now.
  n storage bucket purge --id "$bid" >/dev/null 2>&1 || true
fi
# IAM for the store: access key, service account, group (permits and the
# membership go with the group / account).
kid=$(cat "$STATE/accesskey.id" 2>/dev/null)
[[ -n $kid ]] && { echo "- access key $kid"; n iam v2 access-key delete --id "$kid" >/dev/null || echo "  ! delete failed: $kid"; }
gid=$(n iam group get-by-name --parent-id "$TENANT" --name "$PFX-store" --format json 2>/dev/null | python3 -c 'import json,sys; print(json.load(sys.stdin)["metadata"]["id"])' 2>/dev/null)
[[ -n $gid ]] && { echo "- group $PFX-store $gid"; n iam group delete --id "$gid" >/dev/null || echo "  ! delete failed: $gid"; }
said=$(n iam service-account get-by-name --parent-id "$REG_PROJECT" --name "$PFX-store" --format json 2>/dev/null | python3 -c 'import json,sys; print(json.load(sys.stdin)["metadata"]["id"])' 2>/dev/null)
[[ -n $said ]] && { echo "- service account $PFX-store $said"; n iam service-account delete --id "$said" >/dev/null || echo "  ! delete failed: $said"; }
rm -f "$STATE/s3.env"

for f in "$STATE"/*.id; do [[ -e $f ]] && rm -f "$f"; done
kubectl config --kubeconfig "$KUBECONFIG_FILE" delete-context "$(ctx a)" >/dev/null 2>&1
echo "down in $(( $(date +%s) - T0 ))s"
./check.sh

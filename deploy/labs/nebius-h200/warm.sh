#!/usr/bin/env bash
# Pre-warm the qwen7b-gpu nydus image onto one node's fuse-client with
# POST /api/cache/warm-image (whole image, 16 parallel range requests,
# every blob digest-checked). Prints the client's JSON answer.
#   ./warm.sh <node>
set -uo pipefail
cd "$(dirname "$0")"
source ./env.sh
source ./images.env
export KUBECONFIG=$KUBECONFIG_FILE
NODE=${1:?node}
k() { kubectl --context "$(ctx a)" "$@"; }
RID=$(cat "$STATE/registry.id")
REG=cr.$REG_REGION.nebius.cloud
REPO=${RID#registry-}/qwen7b-nydus
TOKEN=$(nebius --no-check-update iam get-access-token)
BODY=$(python3 -c '
import json, sys
d = [l.split()[0] for l in open("results/qwen7b-gpu-layers.txt")]
print(json.dumps({"registry": sys.argv[1], "repo": sys.argv[2], "digests": d, "token": sys.argv[3]}))' "$REG" "$REPO" "$TOKEN")
P=$(k -n skyfuse get pod -l app=fuse-gpu --field-selector spec.nodeName=$NODE -o jsonpath='{.items[0].metadata.name}')
printf '%s' "$BODY" | k -n skyfuse exec -i "$P" -c client -- sh -c 'cat > /tmp/warm.json; exec 3<>/dev/tcp/127.0.0.1/18081; n=$(wc -c < /tmp/warm.json); { printf "POST /api/cache/warm-image HTTP/1.1\r\nHost: x\r\nContent-Type: application/json\r\nContent-Length: %s\r\nConnection: close\r\n\r\n" "$n"; cat /tmp/warm.json; } >&3; cat <&3 | tail -1; rm -f /tmp/warm.json'

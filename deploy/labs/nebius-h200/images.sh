#!/usr/bin/env bash
# Copy the test images into the Nebius registry, by digest, multi-arch
# manifests preserved (docker buildx imagetools create copies a manifest
# without pulling layers locally). Writes images.env with the Nebius refs.
#
#   ./images.sh
set -euo pipefail
cd "$(dirname "$0")"
source ./env.sh
# Source registry the images are copied from (an ACR the operator can pull).
SRC_ACR=${SRC_ACR:?set SRC_ACR, e.g. myacr.azurecr.io}
RID=$(cat "$STATE/registry.id")
DST=cr.$REG_REGION.nebius.cloud/${RID#registry-}
az acr login -n "${SRC_ACR%%.*}" >/dev/null 2>&1
# name | source ref
SRCS="
fuse-client|${SRC_ACR}/fuse-client@sha256:144a561fc519d8ddb3a7ef472b570a4bb07f64aae42e3906158784d02973d623
store-front|${SRC_ACR}/fusedata-store-front:pf-220e5b9
nydus-node|${SRC_ACR}/skyimage/nydus-node@sha256:8ed51dbef675b5bb6261eaf708bf589999df3ab0e47dfb56d539241c0c91f437
python|${SRC_ACR}/upstream/python@sha256:aa943ce411423c7582288f18707ff66eddcbaeb510502d747a72aeb34f30d159
df-scheduler|${SRC_ACR}/upstream/dragonfly-scheduler:v2.5.2
df-client|${SRC_ACR}/upstream/dragonfly-client:v1.5.5
busybox|${SRC_ACR}/upstream/busybox:1.37.0
"
: > images.env
while IFS='|' read -r name src; do
  [[ -z $name ]] && continue
  t0=$(date +%s)
  docker buildx imagetools create -t "${DST}/${name}:test" "$src" >/dev/null
  dg=$(docker buildx imagetools inspect "${DST}/${name}:test" --format '{{json .Manifest}}' | python3 -c 'import json,sys; print(json.load(sys.stdin)["digest"])')
  echo "$(echo "$name" | tr a-z- A-Z_)=${DST}/${name}@${dg}" >> images.env
  echo "  $name -> ${DST}/${name}@${dg} ($(( $(date +%s) - t0 ))s)"
done <<< "$SRCS"

# The nydus model image. The Nebius registry rejects Docker v2 manifests
# with nydus layer media types (MANIFEST_INVALID: Unknown blob format), so
# the layers are copied as blobs and the manifest is pushed as OCI with tar
# layer types. The containerd.io/snapshot/nydus-* annotations, which the
# snapshotter keys on, are kept.
QWEN_SRC=${SRC_ACR}/models/qwen7b-gpu@sha256:fdf9b6ab1d239564b8c3e70e1f9e5662ecbefc6a88349e2b37be85e1da6d8079
t0=$(date +%s)
docker buildx imagetools create -t "${DST}/qwen7b-nydus:src" "$QWEN_SRC" >/dev/null 2>&1 || true  # blobs land even when the manifest PUT is refused
docker buildx imagetools inspect "$QWEN_SRC" --raw | python3 -c '
import json,sys
m=json.load(sys.stdin)
m["mediaType"]="application/vnd.oci.image.manifest.v1+json"
m["config"]["mediaType"]="application/vnd.oci.image.config.v1+json"
for l in m["layers"]:
    if l["mediaType"].endswith("nydus.blob.v1"): l["mediaType"]="application/vnd.oci.image.layer.v1.tar"
    elif "gzip" in l["mediaType"]: l["mediaType"]="application/vnd.oci.image.layer.v1.tar+gzip"
sys.stdout.write(json.dumps(m,separators=(",",":")))' > "$STATE/qwen7b-oci-manifest.json"
TOK=$(nebius --no-check-update iam get-access-token)
code=$(curl -s -o "$STATE/qwen7b-put.out" -w "%{http_code}" -X PUT -H "Authorization: Bearer $TOK" \
  -H "Content-Type: application/vnd.oci.image.manifest.v1+json" --data-binary @"$STATE/qwen7b-oci-manifest.json" \
  "https://cr.$REG_REGION.nebius.cloud/v2/${RID#registry-}/qwen7b-nydus/manifests/test")
unset TOK
[[ $code == 201 ]] || { echo "qwen7b-nydus manifest PUT: http $code $(head -c 300 "$STATE/qwen7b-put.out")"; exit 1; }
echo "QWEN7B_NYDUS=${DST}/qwen7b-nydus:test" >> images.env
echo "  qwen7b-nydus -> ${DST}/qwen7b-nydus:test ($(( $(date +%s) - t0 ))s)"

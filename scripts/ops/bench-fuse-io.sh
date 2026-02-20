#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 1 || $# -gt 4 ]]; then
  echo "Usage: $0 <namespace> [size-mb] [pod-name] [with-read]"
  echo "Example (write only): $0 fuse-system-aztest 1024"
  echo "Example (write + read): $0 fuse-system-aztest 1024 '' with-read"
  exit 1
fi

NAMESPACE="$1"
SIZE_MB="${2:-1024}"
POD_NAME="${3:-}"
READ_MODE="${4:-write-only}"
RUN_ID="$(date +%s)"
NVME_FILE="/mnt/nvme/cache/bench-nvme-${SIZE_MB}m-${RUN_ID}.bin"
FUSE_FILE="/host/mnt/fuse/bench-fuse-${SIZE_MB}m-${RUN_ID}.bin"

if [[ -z "$POD_NAME" ]]; then
  POD_NAME="$(kubectl -n "$NAMESPACE" get pods -l app=client -o jsonpath='{range .items[?(@.status.phase=="Running")]}{.metadata.name}{"\n"}{end}' | head -n1)"
fi

if [[ -z "$POD_NAME" ]]; then
  echo "No running client pod found in namespace $NAMESPACE"
  exit 1
fi

echo "Using pod: $POD_NAME"
echo "Size: ${SIZE_MB}MiB"

kubectl -n "$NAMESPACE" exec "$POD_NAME" -c client -- sh -lc "
set -e
pkill dd || true
sync

echo NVME_WRITE_START
s=\$(date +%s%N)
dd if=/dev/zero of=${NVME_FILE} bs=1M count=${SIZE_MB} conv=fsync
e=\$(date +%s%N)
dt_ms=\$(((e-s)/1000000))
mbps=\$(( ${SIZE_MB}*1000/dt_ms ))
ls -lh ${NVME_FILE}
echo NVME_WRITE_MS=\$dt_ms
echo NVME_WRITE_MBPS_APPROX=\$mbps

echo FUSE_WRITE_START
s=\$(date +%s%N)
dd if=/dev/zero of=${FUSE_FILE} bs=1M count=${SIZE_MB} conv=fsync
e=\$(date +%s%N)
dt_ms=\$(((e-s)/1000000))
mbps=\$(( ${SIZE_MB}*1000/dt_ms ))
ls -lh ${FUSE_FILE}
echo FUSE_WRITE_MS=\$dt_ms
echo FUSE_WRITE_MBPS_APPROX=\$mbps
"

if [[ "$READ_MODE" == "with-read" ]]; then
  kubectl -n "$NAMESPACE" exec "$POD_NAME" -c client -- sh -lc "
set -e
echo FUSE_READ_START
s=\$(date +%s%N)
dd if=${FUSE_FILE} of=/dev/null bs=1M count=${SIZE_MB}
e=\$(date +%s%N)
dt_ms=\$(((e-s)/1000000))
mbps=\$(( ${SIZE_MB}*1000/dt_ms ))
echo FUSE_READ_MS=\$dt_ms
echo FUSE_READ_MBPS_APPROX=\$mbps
"
fi

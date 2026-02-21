#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 2 || $# -gt 4 ]]; then
  echo "Usage: $0 <namespace> <size-mb> [writer-pod] [reader-pod]"
  echo "Example (100MB): $0 fuse-system-aztest 100"
  echo "Example (1GB):   $0 fuse-system-aztest 1024"
  echo "Example (5GB):   $0 fuse-system-aztest 5120"
  exit 1
fi

NAMESPACE="$1"
SIZE_MB="$2"
WRITER_POD="${3:-}"
READER_POD="${4:-}"
KUBECTL="kubectl --request-timeout=0"

if ! [[ "$SIZE_MB" =~ ^[0-9]+$ ]] || [[ "$SIZE_MB" -le 0 ]]; then
  echo "size-mb must be a positive integer, got: $SIZE_MB"
  exit 1
fi

RUN_ID="$(date +%s)"
FUSE_FILE="/host/mnt/fuse/smart-read-${SIZE_MB}mb-${RUN_ID}.bin"

if [[ -z "$WRITER_POD" ]]; then
  WRITER_POD="$($KUBECTL -n "$NAMESPACE" get pods -l app=client -o jsonpath='{range .items[?(@.status.phase=="Running")]}{.metadata.name}{"\n"}{end}' | head -n1)"
fi
if [[ -z "$READER_POD" ]]; then
  READER_POD="$($KUBECTL -n "$NAMESPACE" get pods -l app=client -o jsonpath='{range .items[?(@.status.phase=="Running")]}{.metadata.name}{"\n"}{end}' | head -n2 | tail -n1)"
fi

if [[ -z "$WRITER_POD" ]]; then
  echo "No running client pod found in namespace $NAMESPACE"
  exit 1
fi
if [[ -z "$READER_POD" ]]; then
  READER_POD="$WRITER_POD"
fi

echo "Namespace: $NAMESPACE"
echo "Writer pod: $WRITER_POD"
echo "Reader pod: $READER_POD"
echo "File size: ${SIZE_MB}MiB"
echo "FUSE file: $FUSE_FILE"

$KUBECTL -n "$NAMESPACE" exec "$WRITER_POD" -c client -- sh -lc "
set -e
echo WRITE_START
s=\$(date +%s%N)
dd if=/dev/zero of=${FUSE_FILE} bs=1M count=${SIZE_MB} conv=fsync status=progress
e=\$(date +%s%N)
dt_ms=\$(((e-s)/1000000))
mbps=\$(( ${SIZE_MB}*1000/dt_ms ))
ls -lh ${FUSE_FILE}
echo WRITE_MS=\$dt_ms
echo WRITE_MBPS_APPROX=\$mbps
"

# Clear local cache on reader to force a remote path on first read.
$KUBECTL -n "$NAMESPACE" exec "$READER_POD" -c client -- sh -lc "
set -e
base=\$(basename ${FUSE_FILE})
rm -f /mnt/nvme/cache/\${base} /mnt/nvme/cache/\${base}.sha256 /mnt/nvme/cache/\${base}_chunk_* /mnt/nvme/cache/\${base}_chunk_*.sha256 || true
sync
echo CACHE_CLEAR_OK
"

$KUBECTL -n "$NAMESPACE" exec "$READER_POD" -c client -- sh -lc "
set -e
echo READ_START
for i in \$(seq 1 30); do
  if [ -f ${FUSE_FILE} ]; then
    break
  fi
  sleep 2
done
if [ ! -f ${FUSE_FILE} ]; then
  echo READ_FILE_NOT_VISIBLE=${FUSE_FILE}
  ls -lah /host/mnt/fuse | tail -n 20 || true
  exit 1
fi
s=\$(date +%s%N)
dd if=${FUSE_FILE} of=/dev/null bs=1M count=${SIZE_MB} status=none
e=\$(date +%s%N)
dt_ms=\$(((e-s)/1000000))
mbps=\$(( ${SIZE_MB}*1000/dt_ms ))
echo READ_MODE=buffered
echo READ_MS=\$dt_ms
echo READ_MBPS_APPROX=\$mbps
"

echo "Completed smart read test for ${SIZE_MB}MiB file."

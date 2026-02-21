#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 1 || $# -gt 3 ]]; then
  echo "Usage: $0 <namespace> [writer-pod] [reader-pod]"
  echo "Example: $0 fuse-system-aztest"
  exit 1
fi

NAMESPACE="$1"
WRITER_POD="${2:-}"
READER_POD="${3:-}"
KUBECTL="kubectl --request-timeout=0"
HYBRID_SETTLE_SEC="${HYBRID_SETTLE_SEC:-20}"
SIZES_MB=(1024 5120)

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
echo "Hybrid settle wait: ${HYBRID_SETTLE_SEC}s"

backend="$($KUBECTL -n "$NAMESPACE" get cm fuse-config -o jsonpath='{.data.FUSE_BACKEND}' 2>/dev/null || true)"
if [[ -z "$backend" ]]; then
  echo "WARNING: FUSE_BACKEND not found in fuse-config ConfigMap."
else
  echo "Configured FUSE_BACKEND: $backend"
fi

echo
echo "CSV_HEADER,size_mb,write_mbps,cold_read_mbps,cached_read_mbps"

for SIZE_MB in "${SIZES_MB[@]}"; do
  RUN_ID="$(date +%s)"
  FUSE_FILE="/host/mnt/fuse/gofuse-suite-${SIZE_MB}mb-${RUN_ID}.bin"
  BASENAME="$(basename "$FUSE_FILE")"
  EXPECTED_BYTES="$((SIZE_MB*1024*1024))"

  echo
  echo "=== SIZE ${SIZE_MB}MiB ==="
  echo "File: $FUSE_FILE"

  set +e
  write_out="$($KUBECTL -n "$NAMESPACE" exec "$WRITER_POD" -c client -- sh -lc "
set -e
s=\$(date +%s%N)
dd if=/dev/zero of=${FUSE_FILE} bs=1M count=${SIZE_MB} conv=fsync status=none
e=\$(date +%s%N)
dt_ms=\$(((e-s)/1000000))
if [ \"\$dt_ms\" -le 0 ]; then dt_ms=1; fi
mbps=\$(( ${SIZE_MB}*1000/dt_ms ))
echo WRITE_MS=\$dt_ms
echo WRITE_MBPS_APPROX=\$mbps
" 2>&1)"
  write_rc=$?
  set -e
  if [[ $write_rc -ne 0 ]]; then
    printf '%s\n' "$write_out"
    exit $write_rc
  fi
  write_mbps="$(printf '%s\n' "$write_out" | awk -F= '/^WRITE_MBPS_APPROX=/{print $2; exit}')"

  if [[ "$HYBRID_SETTLE_SEC" -gt 0 ]]; then
    echo "Waiting ${HYBRID_SETTLE_SEC}s for async persistence..."
    sleep "$HYBRID_SETTLE_SEC"
  fi

  # Remove local NVMe copies on reader so first read is cold (remote path).
  $KUBECTL -n "$NAMESPACE" exec "$READER_POD" -c client -- sh -lc "
set -e
rm -f /mnt/nvme/cache/${BASENAME} /mnt/nvme/cache/${BASENAME}.sha256 /mnt/nvme/cache/${BASENAME}_chunk_* /mnt/nvme/cache/${BASENAME}_chunk_*.sha256 || true
sync
"

  set +e
  cold_out="$($KUBECTL -n "$NAMESPACE" exec "$READER_POD" -c client -- sh -lc "
set -e
for i in \$(seq 1 120); do
  if [ -f ${FUSE_FILE} ]; then break; fi
  sleep 2
done
[ -f ${FUSE_FILE} ] || {
  echo READ_FILE_NOT_VISIBLE=${FUSE_FILE}
  ls -lah /host/mnt/fuse | tail -n 30 || true
  exit 1
}
s=\$(date +%s%N)
dd_out=\$(dd if=${FUSE_FILE} of=/dev/null bs=1M count=${SIZE_MB} 2>&1 >/dev/null)
e=\$(date +%s%N)
dt_ms=\$(((e-s)/1000000))
if [ \"\$dt_ms\" -le 0 ]; then dt_ms=1; fi
bytes=\$(printf '%s\n' \"\$dd_out\" | awk '/bytes/{print \$1; exit}')
if [ -z \"\$bytes\" ]; then
  echo READ_BYTES_PARSE_FAILED
  printf '%s\n' \"\$dd_out\"
  exit 1
fi
if [ \"\$bytes\" -lt ${EXPECTED_BYTES} ]; then
  echo READ_BYTES_SHORT=\$bytes
  printf '%s\n' \"\$dd_out\"
  exit 1
fi
mbps=\$(( bytes*1000/dt_ms/1024/1024 ))
echo COLD_READ_MS=\$dt_ms
echo COLD_READ_BYTES=\$bytes
echo COLD_READ_MBPS_APPROX=\$mbps
" 2>&1)"
  cold_rc=$?
  set -e
  if [[ $cold_rc -ne 0 ]]; then
    printf '%s\n' "$cold_out"
    exit $cold_rc
  fi
  cold_mbps="$(printf '%s\n' "$cold_out" | awk -F= '/^COLD_READ_MBPS_APPROX=/{print $2; exit}')"

  # Drop page cache to avoid the second read being only kernel page-cache speed.
  $KUBECTL -n "$NAMESPACE" exec "$READER_POD" -c client -- sh -lc "
set +e
sync
echo 3 > /proc/sys/vm/drop_caches 2>/dev/null
exit 0
" >/dev/null 2>&1 || true

  set +e
  cache_probe="$($KUBECTL -n "$NAMESPACE" exec "$READER_POD" -c client -- sh -lc "
set -e
if [ -f /mnt/nvme/cache/${BASENAME} ]; then
  echo CACHE_FILE_KIND=single
elif [ -f /mnt/nvme/cache/${BASENAME}_chunk_0 ]; then
  echo CACHE_FILE_KIND=chunked
else
  echo CACHE_FILE_KIND=missing
fi
" 2>&1)"
  cache_probe_rc=$?
  set -e
  if [[ $cache_probe_rc -ne 0 ]]; then
    printf '%s\n' "$cache_probe"
    exit $cache_probe_rc
  fi
  cache_kind="$(printf '%s\n' "$cache_probe" | awk -F= '/^CACHE_FILE_KIND=/{print $2; exit}')"
  if [[ "${cache_kind}" == "missing" ]]; then
    echo "INFO: no NVMe file detected for ${BASENAME}; cached read may be served from range/kernel cache."
  fi

  set +e
  cached_out="$($KUBECTL -n "$NAMESPACE" exec "$READER_POD" -c client -- sh -lc "
set -e
s=\$(date +%s%N)
dd_out=\$(dd if=${FUSE_FILE} of=/dev/null bs=1M count=${SIZE_MB} 2>&1 >/dev/null)
e=\$(date +%s%N)
dt_ms=\$(((e-s)/1000000))
if [ \"\$dt_ms\" -le 0 ]; then dt_ms=1; fi
bytes=\$(printf '%s\n' \"\$dd_out\" | awk '/bytes/{print \$1; exit}')
if [ -z \"\$bytes\" ]; then
  echo READ_BYTES_PARSE_FAILED
  printf '%s\n' \"\$dd_out\"
  exit 1
fi
if [ \"\$bytes\" -lt ${EXPECTED_BYTES} ]; then
  echo READ_BYTES_SHORT=\$bytes
  printf '%s\n' \"\$dd_out\"
  exit 1
fi
mbps=\$(( bytes*1000/dt_ms/1024/1024 ))
echo CACHED_READ_MS=\$dt_ms
echo CACHED_READ_BYTES=\$bytes
echo CACHED_READ_MBPS_APPROX=\$mbps
" 2>&1)"
  cached_rc=$?
  set -e
  if [[ $cached_rc -ne 0 ]]; then
    printf '%s\n' "$cached_out"
    exit $cached_rc
  fi
  cached_mbps="$(printf '%s\n' "$cached_out" | awk -F= '/^CACHED_READ_MBPS_APPROX=/{print $2; exit}')"

  printf 'CSV_ROW,%s,%s,%s,%s\n' "$SIZE_MB" "${write_mbps:-0}" "${cold_mbps:-0}" "${cached_mbps:-0}"
done

echo
echo "Suite complete."

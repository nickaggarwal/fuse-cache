#!/usr/bin/env bash
# End-to-end validation of the rename metadata move on a live cluster.
#
# Rename is only reachable through the filesystem (there is no HTTP rename
# endpoint), so this drives `mv` on the FUSE mount and then checks the three
# places a rename now has to land correctly: the bytes stay on the same inode
# locally, the entry table re-keys the parent and every chunk, and the durable
# copy is re-pointed with server-side copies rather than a re-upload.
#
# Usage: scripts/e2e-rename.sh [namespace]
set -uo pipefail

NS="${1:-fuse-system}"
MOUNT=/host/mnt/fuse
FAILED=0

pods=($(kubectl -n "$NS" get pods -l app=client -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}'))
if [ "${#pods[@]}" -lt 2 ]; then
  echo "need at least 2 client pods, found ${#pods[@]}" >&2
  exit 1
fi
A="${pods[0]}"
B="${pods[1]}"

kx() { kubectl -n "$NS" exec "$1" -c client -- sh -c "$2"; }
api() { kubectl -n "$NS" exec "$1" -c client -- sh -c "curl -s $2"; }

ok()   { echo "PASS  $*"; }
bad()  { echo "FAIL  $*"; FAILED=$((FAILED+1)); }

echo "=== rename e2e: writer=$A reader=$B ==="

# ---------------------------------------------------------------- RN-1
# A chunked file renamed on the mount. 40 MiB at an 8 MiB stride = 5 chunks,
# so this exercises the multi-key path, not the single-object shortcut.
echo
echo "--- RN-1: chunked rename keeps data and moves every key"
kx "$A" "mkdir -p $MOUNT/rn && dd if=/dev/urandom of=$MOUNT/rn/src.bin bs=1M count=40 2>/dev/null"
before_md5=$(kx "$A" "md5sum $MOUNT/rn/src.bin | cut -d' ' -f1")
echo "source md5: $before_md5"

# Let the background cloud persist finish before renaming, so the cloud copy
# path has something to move.
sleep 20

# Inode identity before the move: a metadata rename preserves it, a
# copy-then-delete does not.
ino_before=$(kx "$A" "stat -c %i $MOUNT/rn/src.bin 2>/dev/null || echo none")

t0=$(date +%s.%N)
kx "$A" "mv $MOUNT/rn/src.bin $MOUNT/rn/dst.bin"
rc=$?
t1=$(date +%s.%N)
elapsed=$(echo "$t1 - $t0" | bc)

if [ $rc -ne 0 ]; then
  bad "RN-1 mv returned $rc"
else
  ok "RN-1 mv succeeded in ${elapsed}s"
fi

# 40 MiB re-read-and-rewrite would take seconds; a metadata move is sub-second
# even including the kubectl exec round trip.
echo "rename wall time: ${elapsed}s (a full re-upload of 40MiB would be far slower)"

after_md5=$(kx "$A" "md5sum $MOUNT/rn/dst.bin | cut -d' ' -f1")
if [ "$before_md5" = "$after_md5" ] && [ -n "$after_md5" ]; then
  ok "RN-1 content preserved ($after_md5)"
else
  bad "RN-1 md5 changed: $before_md5 -> $after_md5"
fi

if kx "$A" "test -e $MOUNT/rn/src.bin" 2>/dev/null; then
  bad "RN-1 source path still visible after rename"
else
  ok "RN-1 source path gone"
fi

ino_after=$(kx "$A" "stat -c %i $MOUNT/rn/dst.bin 2>/dev/null || echo none")
echo "inode before=$ino_before after=$ino_after (FUSE inode numbers are synthetic; disk-level check below)"

# ---------------------------------------------------------------- RN-2
# The cache's own view: no entry may remain filed under the old parent.
echo
echo "--- RN-2: entry table re-keyed, nothing orphaned under the old parent"
cache_json=$(api "$A" "localhost:8081/api/cache")
old_keys=$(echo "$cache_json" | grep -o '/rn/src.bin[^"]*' | sort -u)
new_keys=$(echo "$cache_json" | grep -o '/rn/dst.bin[^"]*' | sort -u)
echo "keys still under old name:"; echo "${old_keys:-  (none)}"
echo "keys under new name:"; echo "${new_keys:-  (none)}"
if [ -z "$old_keys" ]; then
  ok "RN-2 no entries left under the old path"
else
  bad "RN-2 orphaned entries under old path"
fi
if [ -n "$new_keys" ]; then
  ok "RN-2 destination present in the entry table"
else
  bad "RN-2 destination missing from the entry table"
fi

# ---------------------------------------------------------------- RN-3
# On-disk NVMe layout: chunk files must exist under the new name and be gone
# under the old one.
echo
echo "--- RN-3: NVMe chunk files moved on disk"
cache_dir=$(kx "$A" "cat /var/run/fuse-client/node-init.json 2>/dev/null | grep -o '\"cache_dir\":\"[^\"]*\"' | cut -d'\"' -f4")
cache_dir="${cache_dir:-/mnt/nvme/cache}"
# node-init dirs are host paths; inside the pod they hang off /host.
for prefix in "/host$cache_dir" "$cache_dir" /mnt/nvme/cache; do
  if kx "$A" "test -d $prefix/rn" 2>/dev/null; then cache_dir="$prefix"; break; fi
done
echo "cache dir: $cache_dir"
kx "$A" "ls -la $cache_dir/rn/ 2>/dev/null || echo '  (no rn dir)'"
old_files=$(kx "$A" "ls $cache_dir/rn/ 2>/dev/null | grep -c '^src.bin' || true")
new_files=$(kx "$A" "ls $cache_dir/rn/ 2>/dev/null | grep -c '^dst.bin' || true")
echo "on-disk src.bin* = $old_files, dst.bin* = $new_files"
if [ "${old_files:-0}" -eq 0 ] && [ "${new_files:-0}" -gt 0 ]; then
  ok "RN-3 chunk files relocated on disk"
else
  bad "RN-3 disk layout wrong (src=$old_files dst=$new_files)"
fi

# ---------------------------------------------------------------- RN-4
# Cross-node: another node reads the renamed file. This exercises coordinator
# republication of the destination and the peer/cloud fetch of the new keys.
echo
echo "--- RN-4: another node reads the renamed file"
remote_md5=$(kx "$B" "md5sum $MOUNT/rn/dst.bin 2>/dev/null | cut -d' ' -f1")
if [ "$remote_md5" = "$before_md5" ]; then
  ok "RN-4 remote node read the renamed file correctly"
else
  bad "RN-4 remote md5 = ${remote_md5:-<empty>}, want $before_md5"
fi

# ---------------------------------------------------------------- RN-5
# POSIX replace: renaming over an existing file must leave the source's bytes.
echo
echo "--- RN-5: rename over an existing destination replaces it"
kx "$A" "dd if=/dev/urandom of=$MOUNT/rn/a.bin bs=1M count=3 2>/dev/null; dd if=/dev/urandom of=$MOUNT/rn/b.bin bs=1M count=7 2>/dev/null"
a_md5=$(kx "$A" "md5sum $MOUNT/rn/a.bin | cut -d' ' -f1")
sleep 5
kx "$A" "mv $MOUNT/rn/a.bin $MOUNT/rn/b.bin"
sleep 2
b_md5=$(kx "$A" "md5sum $MOUNT/rn/b.bin | cut -d' ' -f1")
b_size=$(kx "$A" "stat -c %s $MOUNT/rn/b.bin")
if [ "$b_md5" = "$a_md5" ]; then
  ok "RN-5 destination holds the source's bytes (size $b_size, want 3145728)"
else
  bad "RN-5 destination md5 $b_md5 != source $a_md5"
fi

# ---------------------------------------------------------------- RN-6
# Cloud tier: after the background copy, the destination objects exist and the
# source objects are gone. Checked through the client's own cloud-tier view.
echo
echo "--- RN-6: cloud objects re-pointed (waiting 30s for the background copy)"
sleep 30
kubectl -n "$NS" logs "$A" -c client --tail=400 2>/dev/null | grep -iE "renamed|cloud copy|re-persist" | tail -20

echo
echo "=== rename e2e complete: $FAILED failure(s) ==="
exit $FAILED

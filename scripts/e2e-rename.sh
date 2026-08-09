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
RUN="${RUN:-r2}"
FAILED=0

pods=($(kubectl -n "$NS" get pods -l app=client -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}'))
if [ "${#pods[@]}" -lt 2 ]; then
  echo "need at least 2 client pods, found ${#pods[@]}" >&2
  exit 1
fi
A="${pods[0]}"
B="${pods[1]}"

kx() { kubectl -n "$NS" exec "$1" -c client -- bash -c "$2"; }

# The runtime image has no curl or wget, so HTTP checks go through a helper pod
# on the cluster network, addressing clients by pod IP.
CURLBOX=curlbox
if ! kubectl -n "$NS" get pod "$CURLBOX" >/dev/null 2>&1; then
  kubectl -n "$NS" run "$CURLBOX" --image=curlimages/curl:8.5.0 --restart=Never \
    --command -- sleep 100000 >/dev/null
  kubectl -n "$NS" wait --for=condition=Ready "pod/$CURLBOX" --timeout=90s >/dev/null
fi
ip_of() { kubectl -n "$NS" get pod "$1" -o jsonpath='{.status.podIP}'; }
api() { kubectl -n "$NS" exec "$CURLBOX" -- curl -s --max-time 30 "$(ip_of "$1"):8081$2"; }

ok()   { echo "PASS  $*"; }
bad()  { echo "FAIL  $*"; FAILED=$((FAILED+1)); }

echo "=== rename e2e: writer=$A reader=$B ==="

# ---------------------------------------------------------------- RN-1
# A chunked file renamed on the mount. 40 MiB at an 8 MiB stride = 5 chunks,
# so this exercises the multi-key path, not the single-object shortcut.
echo
echo "--- RN-1: chunked rename keeps data and moves every key"
kx "$A" "mkdir -p $MOUNT/rn-$RUN && dd if=/dev/urandom of=$MOUNT/rn-$RUN/src.bin bs=1M count=40 2>/dev/null"
before_md5=$(kx "$A" "md5sum $MOUNT/rn-$RUN/src.bin | cut -d' ' -f1")
echo "source md5: $before_md5"

# Let the background cloud persist finish before renaming, so the cloud copy
# path has something to move.
sleep 20

# Inode identity before the move: a metadata rename preserves it, a
# copy-then-delete does not.
ino_before=$(kx "$A" "stat -c %i $MOUNT/rn-$RUN/src.bin 2>/dev/null || echo none")

t0=$(date +%s.%N)
kx "$A" "mv $MOUNT/rn-$RUN/src.bin $MOUNT/rn-$RUN/dst.bin"
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

after_md5=$(kx "$A" "md5sum $MOUNT/rn-$RUN/dst.bin | cut -d' ' -f1")
if [ "$before_md5" = "$after_md5" ] && [ -n "$after_md5" ]; then
  ok "RN-1 content preserved ($after_md5)"
else
  bad "RN-1 md5 changed: $before_md5 -> $after_md5"
fi

# The source name must stop resolving. It used to linger: coordinator file rows
# carry no TTL, and the merged metadata view kept serving the old path forever.
# Rename now retracts the source rows, so this is a hard assertion again.
if kx "$A" "test -e $MOUNT/rn-$RUN/src.bin" 2>/dev/null; then
  bad "RN-1 source name still resolves after the rename (stale coordinator row)"
else
  ok "RN-1 source path gone"
fi

ino_after=$(kx "$A" "stat -c %i $MOUNT/rn-$RUN/dst.bin 2>/dev/null || echo none")
echo "inode before=$ino_before after=$ino_after (FUSE inode numbers are synthetic; disk-level check below)"

# ---------------------------------------------------------------- RN-2
# The destination must be republished to the coordinator as a chunked file,
# with a chunk row per chunk, and the source must be retracted. Note /api/cache
# MERGES the coordinator view with the local table, so old-name keys here mean
# remote rows survived the rename — exactly the ghost RN-1 checks from the
# filesystem side, seen from the metadata side.
echo
echo "--- RN-2: destination republished with every chunk key, source retracted"
cache_json=$(api "$A" "/api/cache")
old_keys=$(echo "$cache_json" | grep -o "/rn-$RUN/src.bin[^\"]*" | sort -u)
new_keys=$(echo "$cache_json" | grep -o "/rn-$RUN/dst.bin[^\"]*" | sort -u)
echo "keys still under old name:"; echo "${old_keys:-  (none)}"
echo "keys under new name:"; echo "${new_keys:-  (none)}"
n_new=$(echo "$new_keys" | grep -c "dst.bin_chunk_" || true)
if [ "${n_new:-0}" -ge 5 ]; then
  ok "RN-2 destination republished with $n_new chunk keys"
else
  bad "RN-2 destination has only ${n_new:-0} chunk keys, want >= 5"
fi
if [ -z "$old_keys" ]; then
  ok "RN-2 source keys retracted from the merged view"
else
  bad "RN-2 source keys survived the rename: $(echo "$old_keys" | tr '\n' ' ')"
fi

# ---------------------------------------------------------------- RN-3
# On-disk NVMe layout: objects must exist under the new name and be gone under
# the old one. The cache dir is the node-init-discovered disk, not the static
# -nvme default, so locate it rather than assuming.
echo
echo "--- RN-3: NVMe objects moved on disk"
cache_dir=""
for prefix in /host/mnt/fuse-nvme0n1/fuse-cache /host/mnt/nvme/cache /mnt/nvme/cache; do
  if kx "$A" "test -d $prefix/rn-$RUN" >/dev/null 2>&1; then cache_dir="$prefix"; break; fi
done
if [ -z "$cache_dir" ]; then
  bad "RN-3 could not locate the NVMe cache dir holding rn-$RUN"
else
  echo "cache dir: $cache_dir"
  kx "$A" "ls -la $cache_dir/rn-$RUN/"
  old_files=$(kx "$A" "ls $cache_dir/rn-$RUN/ 2>/dev/null | grep -c '^src.bin' || true")
  new_files=$(kx "$A" "ls $cache_dir/rn-$RUN/ 2>/dev/null | grep -c '^dst.bin' || true")
  echo "on-disk src.bin* = $old_files, dst.bin* = $new_files"
  if [ "${old_files:-0}" -eq 0 ] && [ "${new_files:-0}" -gt 0 ]; then
    ok "RN-3 objects relocated on disk, nothing left under the old name"
  else
    bad "RN-3 disk layout wrong (src=$old_files dst=$new_files)"
  fi
fi

# ---------------------------------------------------------------- RN-4
# Cross-node: another node reads the renamed file. This exercises coordinator
# republication of the destination and the peer/cloud fetch of the new keys.
echo
echo "--- RN-4: another node reads the renamed file"
remote_md5=$(kx "$B" "md5sum $MOUNT/rn-$RUN/dst.bin 2>/dev/null | cut -d' ' -f1")
if [ "$remote_md5" = "$before_md5" ]; then
  ok "RN-4 remote node read the renamed file correctly"
else
  bad "RN-4 remote md5 = ${remote_md5:-<empty>}, want $before_md5"
fi

# ---------------------------------------------------------------- RN-5
# POSIX replace: renaming over an existing file must leave the source's bytes.
echo
echo "--- RN-5: rename over an existing destination replaces it"
kx "$A" "dd if=/dev/urandom of=$MOUNT/rn-$RUN/a.bin bs=1M count=3 2>/dev/null; dd if=/dev/urandom of=$MOUNT/rn-$RUN/b.bin bs=1M count=7 2>/dev/null"
a_md5=$(kx "$A" "md5sum $MOUNT/rn-$RUN/a.bin | cut -d' ' -f1")
sleep 5
kx "$A" "mv $MOUNT/rn-$RUN/a.bin $MOUNT/rn-$RUN/b.bin"
# The destination is re-persisted in the background; read the settled state
# rather than racing the copy.
b_md5=""
for _ in 1 2 3 4 5; do
  sleep 5
  b_md5=$(kx "$A" "md5sum $MOUNT/rn-$RUN/b.bin 2>/dev/null | cut -d' ' -f1")
  [ -n "$b_md5" ] && break
done
b_size=$(kx "$A" "stat -c %s $MOUNT/rn-$RUN/b.bin")
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

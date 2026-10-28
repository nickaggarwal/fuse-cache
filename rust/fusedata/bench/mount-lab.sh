#!/bin/sh
# Mount the lab's FUSE filesystems over /cache/c for the FUSE comparisons:
#   /mnt/go         fixed Go (go-fuse), kernel readahead left at 128 KiB
#   /mnt/go-ra4m    fixed Go with the mount's read_ahead_kb set to 4 MiB
#   /mnt/rs-before  Rust before tuning ($RS_BEFORE)
#   /mnt/rs         Rust after tuning ($RS), which sets 4 MiB itself
RB=${RB:-/out/readbench-linux}
RS=${RS:-/out/fusedata}
RS_BEFORE=${RS_BEFORE:-/out/fusedata-before}
PUB_GO="-publish /bench/model.bin:3221226249:16 -publish /bench/whole.bin:1073742157:16"
PUB_RS="--publish /bench/model.bin:3221226249:16 --publish /bench/whole.bin:1073742157:16"
mkdir -p /mnt/go /mnt/go-ra4m /mnt/rs-before /mnt/rs
# shellcheck disable=SC2086
"$RB" mount -dir /cache/c -mnt /mnt/go $PUB_GO > /out/m-go.log 2>&1 &
# shellcheck disable=SC2086
"$RB" mount -dir /cache/c -mnt /mnt/go-ra4m $PUB_GO > /out/m-go-ra4m.log 2>&1 &
[ -x "$RS_BEFORE" ] && "$RS_BEFORE" mount --dir /cache/c --mnt /mnt/rs-before --workers 8 $PUB_RS > /out/m-rs-before.log 2>&1 &
# shellcheck disable=SC2086
"$RS" mount --dir /cache/c --mnt /mnt/rs --workers 8 $PUB_RS > /out/m-rs.log 2>&1 &
sleep 3
echo 4096 > /sys/class/bdi/$(mountpoint -d /mnt/go-ra4m)/read_ahead_kb
for m in /mnt/go /mnt/go-ra4m /mnt/rs-before /mnt/rs; do
  if mountpoint -q $m; then echo "$m ra_kb=$(cat /sys/class/bdi/$(mountpoint -d $m)/read_ahead_kb)"; else echo "$m NOT MOUNTED"; fi
done

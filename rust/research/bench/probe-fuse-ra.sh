#!/bin/sh
# Effect of the FUSE mount's BDI read_ahead_kb on single-reader sequential
# reads through each mount (cold). Prints read_ahead_kb and MB/s per mount.
RB=${RB:-/out/readbench-linux}
MOUNTS=${MOUNTS:-"/mnt/go /mnt/rs-v3"}
for ra in ${RAS:-128 1024 4096 16384}; do
  for m in $MOUNTS; do
    bdi=/sys/class/bdi/$(mountpoint -d "$m")
    echo "$ra" > "$bdi/read_ahead_kb"
    for win in 128 1024; do
      sync; echo 3 > /proc/sys/vm/drop_caches
      r=$("$RB" bench-file -file "$m/bench/model.bin" -impl x -window-kib $win -pattern seq -total-mb 2048)
      echo "ra_kb=$ra mount=$m win=$win $(echo "$r" | python3 -c 'import json,sys; r=json.load(sys.stdin); print("MBps=%d p99us=%d" % (r["mb_per_s"], r["p99_us"]))')"
    done
  done
done
for m in $MOUNTS; do echo 128 > /sys/class/bdi/$(mountpoint -d "$m")/read_ahead_kb; done

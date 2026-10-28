#!/bin/sh
# Cold-cache sequential sweep over the Rust prefetcher's knobs, to pick
# defaults from data. One JSON row per run.
RS=${RS:-/out/fusedata}
S=${CHUNKED_SIZE:-3221226249}
REPS=${REPS:-3}
for rep in $(seq 1 "$REPS"); do
  for cfg in "0 4096" "4 4096" "8 4096" "8 2048" "16 2048" "16 1024"; do
    set -- $cfg
    for win in 128 1024; do
      sync; echo 3 > /proc/sys/vm/drop_caches
      "$RS" bench --dir /cache/c --publish /bench/model.bin:$S:16 --window-kib $win --pattern seq \
        --total-mb 512 --readahead-mb 128 --readahead-threads $1 --readahead-piece-kib $2 \
        | sed "s|}$|,\"ra_threads\":$1,\"ra_piece_kib\":$2,\"rep\":$rep}|"
    done
  done
done

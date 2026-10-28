#!/bin/sh
# Library-level read path, Go vs Rust, same cache dir, same workloads.
# Runs inside the lab container (see Dockerfile.lab). Each row is one JSON
# object on stdout. Every cell is run REPS times, alternating Go and Rust, and
# the page cache is either dropped before each run (cold) or primed (warm).
#
#   GO=/out/readbench-linux RS=/out/fusedata DIR=/cache/c MODE=warm sh run-library.sh
set -eu
GO=${GO:-/out/readbench-linux}
RS=${RS:-/out/fusedata}
DIR=${DIR:-/cache/c}
MODE=${MODE:-warm}          # warm | cold
REPS=${REPS:-3}
TOTAL=${TOTAL:-1024}        # MiB read per run
THREADS=${THREADS:-1}
CHUNKED=/bench/model.bin
CHUNKED_SIZE=${CHUNKED_SIZE:-3221226249}
WHOLE=/bench/whole.bin
WHOLE_SIZE=${WHOLE_SIZE:-1073742157}

prep() {
  if [ "$MODE" = cold ]; then
    sync; echo 3 > /proc/sys/vm/drop_caches
  else
    cat "$DIR"/bench/model.bin_chunk_* "$DIR$WHOLE" > /dev/null
  fi
}

for rep in $(seq 1 "$REPS"); do
  for layout in chunked whole; do
    if [ $layout = chunked ]; then P=$CHUNKED; S=$CHUNKED_SIZE; else P=$WHOLE; S=$WHOLE_SIZE; fi
    for pat in seq rand; do
      for win in 128 1024; do
        prep
        "$GO" bench -dir "$DIR" -path $P -size $S -stride-mb $([ $layout = chunked ] && echo 16 || echo 16) \
          -window-kib $win -pattern $pat -threads "$THREADS" -total-mb "$TOTAL" 2>/dev/null \
          | sed "s/}\$/,\"layout\":\"$layout\",\"mode\":\"$MODE\",\"rep\":$rep}/"
        prep
        "$RS" bench --dir "$DIR" --path $P --publish $P:$S:16 \
          --window-kib $win --pattern $pat --threads "$THREADS" --total-mb "$TOTAL" \
          | sed "s/}\$/,\"layout\":\"$layout\",\"mode\":\"$MODE\",\"rep\":$rep}/"
      done
    done
  done
done

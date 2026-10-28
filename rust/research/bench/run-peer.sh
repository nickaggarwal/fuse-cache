#!/bin/sh
# Peer transport over loopback: {Go, Rust} client x {Go, Rust} server, whole
# 16 MiB chunk objects, warm page cache (this isolates transport cost; a cold
# cache measures the disk). Each cell REPS times, interleaved.
RB=${RB:-/out/readbench-linux}
RS=${RS:-/out/fusedata}
CS=${CHUNKED_SIZE:-3221226249}
REPS=${REPS:-3}
TOTAL=${TOTAL:-3072}
cat /cache/c/bench/model.bin_chunk_* > /dev/null
for rep in $(seq 1 "$REPS"); do
  for t in 1 4; do
    for srv in go:127.0.0.1:18081 rs:127.0.0.1:18082; do
      name=${srv%%:*}; addr=${srv#*:}
      "$RB" bench-peer -addr "$addr" -size "$CS" -threads $t -total-mb "$TOTAL" -server $name 2>/dev/null \
        | sed "s|}$|,\"rep\":$rep}|"
      "$RS" bench-peer --addr "$addr" --size "$CS" --threads $t --total-mb "$TOTAL" --server $name \
        | sed "s|}$|,\"rep\":$rep}|"
    done
  done
done

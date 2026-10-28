#!/bin/sh
# Library-layer ablation of the Rust single-reader sequential path vs fixed
# Go: each variant changes one knob of the same binary. Cold and warm, 128K
# and 1M windows, REPS interleaved runs. One JSON row per run.
RB=${RB:-/out/readbench-linux}
RS=${RS:-/out/fusedata}
DIR=${DIR:-/cache/c}
P=${P:-/bench/model.bin}
S=${S:-3221226249}
REPS=${REPS:-3}
TOTAL=${TOTAL:-2048}
MODES=${MODES:-"cold warm"}
VARIANTS=${VARIANTS:-"
v1-copy-hint32|--copy --prefetch-mode hint --readahead-min-mb 128 --readahead-mb 128
copy-populate|--copy
view-hint|--prefetch-mode hint
view-pread|--prefetch-mode pread
view-populate|
view-populate-fixed128|--readahead-min-mb 128
view-populate-max512|--readahead-mb 512
view-populate-8thr|--readahead-threads 8
"}

for rep in $(seq 1 "$REPS"); do
  for mode in $MODES; do
    for win in 128 1024; do
      echo "$VARIANTS" | while IFS='|' read -r label args; do
        [ -n "$label" ] || continue
        for impl in go "$label"; do
          sync; echo 3 > /proc/sys/vm/drop_caches; [ "$mode" = cold ] || cat "$DIR$P"_chunk_* > /dev/null
          if [ "$impl" = go ]; then
            [ "$label" = "$(echo "$VARIANTS" | grep . | head -1 | cut -d'|' -f1)" ] || continue
            "$RB" bench -dir "$DIR" -path $P -size $S -window-kib $win -pattern seq -total-mb $TOTAL 2>/dev/null
          else
            # shellcheck disable=SC2086
            "$RS" bench --dir "$DIR" --path $P --publish $P:$S:16 --window-kib $win --pattern seq --total-mb $TOTAL $args
          fi | sed "s|}$|,\"label\":\"$impl\",\"mode\":\"$mode\",\"layer2\":\"library\",\"rep\":$rep}|"
        done
      done
    done
  done
done

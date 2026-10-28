#!/bin/sh
# Single-reader sequential focus loop used to tune the Rust path against
# fixed Go. Labels in $IMPLS (space separated) are label=binary; labels
# starting with "go" are the Go harness. FUSE runs read /mnt/<label>/... so
# each label needs its own mount; library runs skip extra go* labels (their
# library path is identical to "go").
#
# Modes:
#   cold  page cache dropped (backing files and FUSE pages)
#   warm  page cache dropped, then the backing chunk files read once: the
#         daemon reads from page cache, the FUSE-side cache is empty
#   hot   (fuse only) the file was just read through the same mount, so the
#         kernel's FUSE page cache serves it (KEEP_CACHE on both mounts)
RB=${RB:-/out/readbench-linux}
DIR=${DIR:-/cache/c}
P=${P:-/bench/model.bin}
S=${S:-3221226249}
REPS=${REPS:-3}
TOTAL=${TOTAL:-2048}
LAYERS=${LAYERS:-"library fuse"}
MODES=${MODES:-"cold warm hot"}
IMPLS=${IMPLS:-"go=x rs=/out/fusedata"}
EXTRA=${EXTRA:-}

for rep in $(seq 1 "$REPS"); do
  for layer in $LAYERS; do
    for mode in $MODES; do
      [ "$layer" = library ] && [ "$mode" = hot ] && continue
      for win in 128 1024; do
        for spec in $IMPLS; do
          label=${spec%%=*}; bin=${spec#*=}
          case $label in go*) isgo=1 ;; *) isgo= ;; esac
          [ "$layer" = library ] && [ -n "$isgo" ] && [ "$label" != go ] && continue
          sleep ${SETTLE:-2}; sync; echo 3 > /proc/sys/vm/drop_caches
          case $mode in
            warm) cat "$DIR$P"_chunk_* > /dev/null ;;
            hot) "$RB" bench-file -file /mnt/$label$P -impl x -window-kib 1024 -pattern seq -total-mb $TOTAL > /dev/null ;;
          esac
          if [ "$layer" = library ]; then
            if [ -n "$isgo" ]; then
              "$RB" bench -dir "$DIR" -path $P -size $S -window-kib $win -pattern seq -total-mb $TOTAL 2>/dev/null
            else
              # shellcheck disable=SC2086
              "$bin" bench --dir "$DIR" --path $P --publish $P:$S:16 --window-kib $win --pattern seq --total-mb $TOTAL $EXTRA
            fi
          else
            "$RB" bench-file -file /mnt/$label$P -impl "$label" -layer fuse -window-kib $win -pattern seq -total-mb $TOTAL
          fi | sed "s|}$|,\"label\":\"$label\",\"mode\":\"$mode\",\"layer2\":\"$layer\",\"rep\":$rep}|"
        done
      done
    done
  done
done

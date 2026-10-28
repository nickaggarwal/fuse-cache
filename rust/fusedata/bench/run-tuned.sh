#!/bin/sh
# Before/after table for the sequential tuning, against fixed Go (2c047d1):
# single-reader seq 128K/1M (cold, warm), rand 128K (cold), 8-reader seq 1M
# (cold), in-process and through FUSE. Mounts from mount-lab.sh:
#   /mnt/go  /mnt/go-ra4m  /mnt/rs-before  /mnt/rs
# warm = page cache dropped, backing chunk files then read once (the daemon
# reads from page cache, the FUSE-side cache is empty).
RB=${RB:-/out/readbench-linux}
RS=${RS:-/out/fusedata}
RS_BEFORE=${RS_BEFORE:-/out/fusedata-before}
DIR=/cache/c
P=/bench/model.bin
S=3221226249
REPS=${REPS:-5}

cell() { # label layer mode pattern win threads total
  label=$1 layer=$2 mode=$3 pat=$4 win=$5 thr=$6 total=$7
  sleep ${SETTLE:-2}; sync; echo 3 > /proc/sys/vm/drop_caches  # settle: daemons release idle mappings/caches
  CACHED=$(awk "/^Cached:/{print \$2}" /proc/meminfo)
  [ "$mode" = warm ] && cat "$DIR$P"_chunk_* > /dev/null
  if [ "$layer" = library ]; then
    case $label in
      go) "$RB" bench -dir $DIR -path $P -size $S -window-kib $win -pattern $pat -threads $thr -total-mb $total 2>/dev/null ;;
      rs-before) "$RS_BEFORE" bench --dir $DIR --path $P --publish $P:$S:16 --window-kib $win --pattern $pat --threads $thr --total-mb $total ;;
      rs) "$RS" bench --dir $DIR --path $P --publish $P:$S:16 --window-kib $win --pattern $pat --threads $thr --total-mb $total ;;
      *) return ;;
    esac
  else
    "$RB" bench-file -file /mnt/$label$P -impl $label -layer fuse -window-kib $win -pattern $pat -threads $thr -total-mb $total
  fi | sed "s|}$|,\"label\":\"$label\",\"mode\":\"$mode\",\"layer2\":\"$layer\",\"rep\":$rep,\"cached_kib_after_drop\":$CACHED}|"
}

for rep in $(seq 1 "$REPS"); do
  for layer in library fuse; do
    for label in go go-ra4m rs-before rs; do
      cell $label $layer cold seq  128  1 2048
      cell $label $layer cold seq  1024 1 2048
      cell $label $layer warm seq  128  1 2048
      cell $label $layer warm seq  1024 1 2048
      cell $label $layer cold rand 128  1 512
      cell $label $layer cold seq  1024 8 3072
    done
  done
done

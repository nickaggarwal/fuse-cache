#!/bin/sh
# Sanity check that "cold" runs start cold: page-cache size before/after a
# drop, then the Rust view bench with full content verification (touches
# every byte) and the Go bench, each from a dropped cache.
RB=${RB:-/out/readbench-linux}
RS=${RS:-/out/fusedata}
DIR=${DIR:-/cache/c}
P=/bench/model.bin
S=3221226249
cached() { awk '/^Cached:/{print $2/1024 " MiB"}' /proc/meminfo; }
cat "$DIR$P"_chunk_* > /dev/null
echo "cached after priming: $(cached)"
sync; echo 3 > /proc/sys/vm/drop_caches
echo "cached after drop:    $(cached)"
"$RS" bench --dir "$DIR" --path $P --publish $P:$S:16 --window-kib 1024 --pattern seq --total-mb 2048 --verify
echo "cached after rust:    $(cached)"
sync; echo 3 > /proc/sys/vm/drop_caches
"$RS" bench --dir "$DIR" --path $P --publish $P:$S:16 --window-kib 1024 --pattern seq --total-mb 2048
sync; echo 3 > /proc/sys/vm/drop_caches
"$RB" bench -dir "$DIR" -path $P -size $S -window-kib 1024 -pattern seq -total-mb 2048 -verify 2>/dev/null
sync; echo 3 > /proc/sys/vm/drop_caches
"$RB" bench -dir "$DIR" -path $P -size $S -window-kib 1024 -pattern seq -total-mb 2048 2>/dev/null
sync; echo 3 > /proc/sys/vm/drop_caches
s=$(date +%s.%N); cat "$DIR$P"_chunk_* > /dev/null; e=$(date +%s.%N)
python3 -c "print('cat (1 stream, cold): %.0f MB/s' % (3072/($e-$s)))"

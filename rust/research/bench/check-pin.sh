#!/bin/sh
# Does the Rust reader leave cache pages mapped (and so un-droppable)?
# Runs the matrix's access patterns through /mnt/rs and in-process, waits for
# the idle reaper, then drops caches and reports what stayed resident.
RS=${RS:-/out/fusedata}
RB=${RB:-/out/readbench-linux}
F=/bench/model.bin
sync; echo 3 > /proc/sys/vm/drop_caches
"$RB" bench-file -file /mnt/rs$F -impl rs -window-kib 128 -pattern seq -total-mb 2048 -verify > /dev/null
"$RB" bench-file -file /mnt/rs$F -impl rs -window-kib 1024 -pattern seq -total-mb 2048 -verify > /dev/null
"$RB" bench-file -file /mnt/rs$F -impl rs -window-kib 128 -pattern rand -total-mb 512 -verify > /dev/null
"$RB" bench-file -file /mnt/rs$F -impl rs -window-kib 1024 -pattern seq -threads 8 -total-mb 3072 -verify > /dev/null
"$RS" bench --dir /cache/c --publish $F:3221226249:16 --window-kib 128 --pattern seq --total-mb 2048 --verify > /dev/null
echo "daemon rss right after: $(ps -eo rss,args | awk '$2 ~ /^\/out\/fusedata/ && $3=="mount" && $5=="/cache/c" {print $1}' | head -1) KiB"
sleep ${WAIT:-4}
echo "daemon rss after idle:  $(ps -eo rss,args | awk '$2 ~ /^\/out\/fusedata/ && $3=="mount" && $5=="/cache/c" {print $1}' | head -1) KiB"
sync; echo 3 > /proc/sys/vm/drop_caches
grep -E '^(Cached|Mapped):' /proc/meminfo

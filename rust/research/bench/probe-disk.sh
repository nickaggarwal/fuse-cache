#!/bin/sh
# How the lab disk responds to read parallelism (cold page cache): 32 chunk
# files (512 MiB) read with 1, 4 and 8 concurrent readers.
D=${DIR:-/cache/c}/bench
for p in ${PAR:-1 4 8}; do
  sync; echo 3 > /proc/sys/vm/drop_caches
  s=$(date +%s.%N)
  ls "$D"/model.bin_chunk_1[0-3]? 2>/dev/null | head -32 | xargs -P $p -n 1 sh -c 'dd if="$0" of=/dev/null bs=4M 2>/dev/null'
  e=$(date +%s.%N)
  python3 -c "print(\"parallel=$p MBps=%.0f\" % (512 / ($e - $s)))"
done

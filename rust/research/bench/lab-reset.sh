#!/bin/sh
# Unmount every lab FUSE mount, stop the daemons, drop caches, and show that
# the page cache really emptied. A daemon that keeps cache files mmapped
# pins their pages (drop_caches cannot evict mapped pages), which would make
# later "cold" runs warm.
for m in /mnt/*; do
  mountpoint -q "$m" && umount "$m"
done
pkill -f '^/out/fusedata' 2>/dev/null
pkill -f '^/out/readbench-linux mount' 2>/dev/null
pkill -f '^/out/readbench-linux serve-peer' 2>/dev/null
sleep 1
ps -eo pid,rss,args | grep -E '^ *[0-9]+ +[0-9]+ /out/' || true
sync; echo 3 > /proc/sys/vm/drop_caches
grep -E '^(Cached|Mapped):' /proc/meminfo

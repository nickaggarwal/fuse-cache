#!/bin/sh
# FUSE layer, Go (go-fuse backend, production CacheManager) vs Rust (fuser),
# both mounted over the same cache dir and driven by one client
# (`readbench bench-file`, pread). Runs inside the lab container.
#
# Mounts are expected at /mnt/go and /mnt/rs (see README). Each cell runs
# REPS times, alternating implementations. MODE=cold drops the page cache
# before every run; FUSE KEEP_CACHE pages are dropped too.
#
# cpu_s_per_gib in each row is the CLIENT's CPU; server_cpu_s_per_gib is the
# FUSE daemon's (utime+stime from /proc/<pid>/stat around the run), which is
# the number that compares the two implementations.
set -eu
RB=${RB:-/out/readbench-linux}
MODE=${MODE:-cold}
REPS=${REPS:-3}
TOTAL=${TOTAL:-512}
THREADS=${THREADS:-1}
FILE=${FILE:-bench/model.bin}
VERIFY=${VERIFY:-}
TCK=$(getconf CLK_TCK)

daemon_pid() {
  for p in $(pgrep -f "mnt /mnt/$1" || true); do
    case "$(cat /proc/$p/comm)" in sh) ;; *) echo $p; return ;; esac
  done
}
ticks() { awk '{print $14 + $15}' "/proc/$1/stat"; }

for rep in $(seq 1 "$REPS"); do
  for pat in seq rand; do
    for win in 128 1024; do
      for m in go rs; do
        pid=$(daemon_pid $m)
        if [ "$MODE" = cold ]; then sync; echo 3 > /proc/sys/vm/drop_caches; fi
        t0=$(ticks "$pid")
        row=$("$RB" bench-file -file "/mnt/$m/$FILE" -impl "$m" -layer fuse -window-kib $win \
          -pattern $pat -threads "$THREADS" -total-mb "$TOTAL" $VERIFY)
        t1=$(ticks "$pid")
        echo "$row" | python3 -c "
import json,sys
r=json.loads(sys.stdin.read())
r['server_cpu_s_per_gib']=($t1-$t0)/$TCK/(r['bytes']/2**30)
r.update(mode='$MODE',rep=$rep,file='$FILE')
print(json.dumps(r))"
      done
    done
  done
done

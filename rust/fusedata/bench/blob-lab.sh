#!/bin/sh
# Blob endpoint, Go vs Rust, in one host/container:
#   local  : the blob's chunk files are in the server's own cache dir
#   remote : the server's cache dir is empty; a coordinator names a holder
#            (a second server over the dir that has the blob) and every
#            window is fetched from it over /api/peer/read (off/len)
# Go server: readbench serve-peer (the real api.Handler, so the real
# serveOCIBlob + PlanRange + PeerStorage.ReadRange). Rust server: fusedata
# serve-peer (BlobServer + PeerClient.fetch_range).
#
#   DIR_A=<dir holding the blob> DIR_B=<empty dir> DIGEST=<hex> SIZE=<bytes> sh blob-lab.sh
set -eu
RB=${RB:-/out/readbench-linux}
RS=${RS:-/out/fusedata}
A=${DIR_A:?}; B=${DIR_B:?}; D=${DIGEST:?}; SZ=${SIZE:?}
REPS=${REPS:-3}
TOTAL=${TOTAL:-1024}
SETTLE=${SETTLE:-2}
VERIFY=${VERIFY:-}
CHUNK_MB=${CHUNK_MB:-16}
H=${HOST_IP:-127.0.0.1}
P="/oci/sha256/$D"
PIDS=""
cleanup() { for p in $PIDS; do kill "$p" 2>/dev/null || true; done; }
trap cleanup EXIT

# Holders over A (they also serve the "local" cases), coordinator naming the
# holder for each implementation's "remote" server.
"$RB" serve-peer -dir "$A" -listen "$H:19101" -peer-id go-holder -publish "$P:$SZ:$CHUNK_MB" >/tmp/blob-go-a.log 2>&1 & PIDS="$PIDS $!"
"$RS" serve-peer --dir "$A" --listen "$H:19102" --publish "$P:$SZ:$CHUNK_MB" >/tmp/blob-rs-a.log 2>&1 & PIDS="$PIDS $!"
"$RB" coord -listen "$H:19110" -peer "go-holder=$H:19101" -publish "$P:$SZ:$CHUNK_MB:go-holder" >/tmp/blob-coord-go.log 2>&1 & PIDS="$PIDS $!"
"$RB" coord -listen "$H:19111" -peer "rs-holder=$H:19102" -publish "$P:$SZ:$CHUNK_MB:rs-holder" >/tmp/blob-coord-rs.log 2>&1 & PIDS="$PIDS $!"
sleep 2
# Readers over the empty dir B.
"$RB" serve-peer -dir "$B" -listen "$H:19201" -peer-id go-reader -coord "$H:19110" >/tmp/blob-go-b.log 2>&1 & PIDS="$PIDS $!"
"$RS" serve-peer --dir "$B" --listen "$H:19202" --coord "$H:19111" --peer-id rs-reader --no-promote >/tmp/blob-rs-b.log 2>&1 & PIDS="$PIDS $!"
sleep 2

pid_of() { # port -> server pid
  for p in $PIDS; do
    if grep -q ":$1" "/proc/$p/cmdline" 2>/dev/null; then echo "$p"; return; fi
  done
}
ticks() { awk '{print $14 + $15}' "/proc/$1/stat"; }
TCK=$(getconf CLK_TCK)

cell() { # impl case port pattern window streams total
  impl=$1 case=$2 port=$3 pat=$4 win=$5 st=$6 total=$7
  sleep "$SETTLE"; sync; echo 3 > /proc/sys/vm/drop_caches
  pid=$(pid_of "$port"); t0=$(ticks "$pid")
  [ "$case" = remote ] && h0=$(ticks "$(pid_of $([ "$impl" = go ] && echo 19101 || echo 19102))") || h0=0
  row=$("$RB" bench-blob -url "http://$H:$port" -digest "$D" -impl "$impl" -layer "blob-$case" \
        -window-kib "$win" -pattern "$pat" -streams "$st" -total-mb "$total" $VERIFY)
  t1=$(ticks "$pid")
  [ "$case" = remote ] && h1=$(ticks "$(pid_of $([ "$impl" = go ] && echo 19101 || echo 19102))") || h1=0
  echo "$row" | python3 -c "
import json,sys
r=json.loads(sys.stdin.read()); g=r['bytes']/2**30
r.update(case='$case', rep=$REP, server_cpu_s_per_gib=($t1-$t0)/$TCK/g, holder_cpu_s_per_gib=($h1-$h0)/$TCK/g)
print(json.dumps(r))"
}

for REP in $(seq 1 "$REPS"); do
  for case in local remote; do
    for impl in go rust; do
      if [ $case = local ]; then port=$([ $impl = go ] && echo 19101 || echo 19102); else port=$([ $impl = go ] && echo 19201 || echo 19202); fi
      cell $impl $case $port rand 1024 4 "$TOTAL"
      cell $impl $case $port seq  1024 4 "$TOTAL"
      cell $impl $case $port rand 128  4 $((TOTAL / 4))
    done
  done
done

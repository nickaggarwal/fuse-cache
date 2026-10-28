#!/usr/bin/env bash
# Self-contained Go-vs-Rust read-path benchmark for one node.
#
#   node-bench.sh CACHE_DIR SCRATCH_DIR [RESULTS_JSON]
#
# CACHE_DIR    a NEW or EMPTY directory on the disk to measure (the NVMe cache
#              disk). The dataset is generated there by the Go client's own
#              write path (PutFromReader -> <path>_chunk_N files), read by
#              both implementations, and deleted at the end (KEEP=1 keeps it).
#              Needs 2 x FILES x FILE_GB + BLOB_GB + 1 GiB free (per-chunk and
#              whole-layout copies, the blob, slack: default 2x16+4+1 = 37).
#              The script refuses a non-empty dir it did not create, so it
#              cannot be pointed at a live client's cache by mistake.
# SCRATCH_DIR  mount points, logs, raw rows (a few MB).
# RESULTS_JSON default SCRATCH_DIR/results.json:
#              {"env": {...}, "rows": [...], "summary": [...]}
#
# Run as root (drop_caches, FUSE mounts, /sys/class/bdi). Binaries: $READBENCH
# and $FUSEDATA, else `readbench` / `fusedata` on PATH.
#
# Matrix (median of REPS, implementations interleaved):
#   layer library: in-process   go (DefaultCacheManager.ReadRange) | rust
#   layer fuse:    pread client against five mounts of the same dir:
#                  go       go-fuse backend, defaults (128 KiB kernel RA)
#                  go-pt    go-fuse with FUSE_GOFUSE_ENABLE_PASSTHROUGH=1 +
#                           FUSE_GOFUSE_DIRECT_MOUNT=1 (production config)
#                  go-ra4m  go-fuse, mount read_ahead_kb set to 4 MiB
#                  rust     fusedata (4 MiB kernel RA, mapped views)
#                  rust-pt  fusedata --passthrough
#                  Passthrough only applies to files stored whole, so the
#                  fuse layer reads WHOLE-layout files (FUSE writes,
#                  PutFromFile, assembled chunks: what model weights become
#                  on a warmed node). The library layer reads per-chunk files.
#   cases: cold seq 128K, cold seq 1M, warm seq 128K, warm seq 1M,
#          cold rand 128K (1 reader); cold seq 1M with 8 readers
#   cold = page cache dropped; warm = dropped, then the backing files read
#   once (the daemon or kernel reads from page cache, FUSE cache empty).
#   layer blob:    the OCI blob endpoint (GET /v2/<repo>/blobs/sha256:<hex>
#                  with Range), driven like nydusd: 4 connections, 1 MiB
#                  ranges at scattered (rand) or per-stream sequential
#                  offsets, plus 128 KiB rand. go = the api.Handler from
#                  this tree (serveOCIBlob/PlanRange); rust = fusedata
#                  serve-peer (BlobServer). Cases: local (the blob's chunk
#                  files are on this disk) and remote (empty reader dir; each
#                  window is fetched over /api/peer/read from a holder on the
#                  same node via the pod IP, the store-node path minus the
#                  network hop). Daemon CPU is the serving process.
# Each seq cell reads one whole file; cells rotate files so consecutive cells
# never reread the same bytes.
#
# Env: FILES=4 FILE_GB=4 CHUNK_MB=16 REPS=3 RAND_MB=1024
#      LAYERS="library fuse blob"  BLOB_GB=4  BLOB_TOTAL_MB=2048
#      KEEP=1 (keep dataset)  SKIP_GEN=1 (reuse a dataset kept with KEEP=1)
set -euo pipefail

CACHE=${1:?usage: node-bench.sh CACHE_DIR SCRATCH_DIR [RESULTS_JSON]}
SCRATCH=${2:?usage: node-bench.sh CACHE_DIR SCRATCH_DIR [RESULTS_JSON]}
OUT=${3:-$SCRATCH/results.json}
FILES=${FILES:-4}
FILE_GB=${FILE_GB:-4}
CHUNK_MB=${CHUNK_MB:-16}
REPS=${REPS:-3}
RAND_MB=${RAND_MB:-1024}
LAYERS=${LAYERS:-"library fuse blob"}
BLOB_GB=${BLOB_GB:-4}
BLOB_TOTAL_MB=${BLOB_TOTAL_MB:-2048}
HOST_IP=${HOST_IP:-$(hostname -i 2>/dev/null | awk '{print $1}')}
HOST_IP=${HOST_IP:-127.0.0.1}
RB=${READBENCH:-$(command -v readbench || true)}
RS=${FUSEDATA:-$(command -v fusedata || true)}
MARK=.node-bench-dataset

[ "$(id -u)" = 0 ] || { echo "must run as root (drop_caches, FUSE)" >&2; exit 1; }
[ -n "$RB" ] && [ -x "$RB" ] && [ -n "$RS" ] && [ -x "$RS" ] || { echo "readbench/fusedata not found (set READBENCH/FUSEDATA)" >&2; exit 1; }
mkdir -p "$CACHE" "$SCRATCH"
CACHE=$(cd "$CACHE" && pwd); SCRATCH=$(cd "$SCRATCH" && pwd)
if [ ! -e "$CACHE/$MARK" ] && [ -n "$(ls -A "$CACHE")" ]; then
  echo "$CACHE is not empty and was not created by node-bench; use a new directory" >&2; exit 1
fi
LOG=$SCRATCH/node-bench.log
ROWS=$SCRATCH/rows.jsonl
: > "$ROWS"
log() { echo "[$(date +%T)] $*" | tee -a "$LOG" >&2; }

SIZE=$(( FILE_GB * 1024 * 1024 * 1024 + 4321 ))   # odd tail: a short last chunk
path_of() { echo "/bench/f$1.bin"; }       # per-chunk layout (PutFromReader)
wpath_of() { echo "/bench/w$1.bin"; }      # whole layout (PutFromFile)

# ---------------------------------------------------------------- dataset
if [ -z "${SKIP_GEN:-}" ]; then
  need_kb=$(( (2 * FILES * FILE_GB + BLOB_GB + 1) * 1024 * 1024 ))
  avail_kb=$(df -Pk "$CACHE" | awk 'NR==2{print $4}')
  [ "$avail_kb" -ge "$need_kb" ] || { echo "need $((need_kb/1024/1024)) GiB free in $CACHE, have $((avail_kb/1024/1024))" >&2; exit 1; }
  touch "$CACHE/$MARK"
  for i in $(seq 0 $((FILES-1))); do
    if [ -e "$CACHE$(path_of "$i")_chunk_0" ]; then log "dataset f$i exists, reusing"; continue; fi
    log "generating $(path_of "$i") ($FILE_GB GiB, ${CHUNK_MB} MiB chunks)"
    "$RB" gen -dir "$CACHE" -path "$(path_of "$i")" -size-mb $((FILE_GB*1024)) -extra-bytes 4321 \
      -chunk-mb "$CHUNK_MB" -layout chunked >>"$LOG" 2>&1
    if [[ " $LAYERS " == *" fuse "* ]] && [ ! -e "$CACHE$(wpath_of "$i")" ]; then
      log "generating $(wpath_of "$i") ($FILE_GB GiB, whole layout)"
      "$RB" gen -dir "$CACHE" -path "$(wpath_of "$i")" -size-mb $((FILE_GB*1024)) -extra-bytes 4321 \
        -chunk-mb "$CHUNK_MB" -layout whole >>"$LOG" 2>&1
    fi
  done
  if [[ " $LAYERS " == *" blob "* ]] && [ ! -s "$CACHE/.blob-digest" ]; then
    log "generating OCI blob ($BLOB_GB GiB, ${CHUNK_MB} MiB chunks, real sha256 digest)"
    "$RB" gen-blob -dir "$CACHE/blobsrc" -size-mb $((BLOB_GB*1024)) -extra-bytes 777 -chunk-mb "$CHUNK_MB" \
      -digest-out "$CACHE/.blob-digest" >>"$LOG" 2>&1
  fi
fi
BLOB_SIZE=$(( BLOB_GB * 1024 * 1024 * 1024 + 777 ))
BLOB_DIGEST=$(cat "$CACHE/.blob-digest" 2>/dev/null || true)
PUB_GO=(); PUB_RS=()
for i in $(seq 0 $((FILES-1))); do
  PUB_GO+=(-publish "$(path_of "$i"):$SIZE:$CHUNK_MB" -publish "$(wpath_of "$i"):$SIZE:$CHUNK_MB")
  PUB_RS+=(--publish "$(path_of "$i"):$SIZE:$CHUNK_MB" --publish "$(wpath_of "$i"):$SIZE:$CHUNK_MB")
done

# Settle first: the Rust daemon unmaps idle mappings after ~1 s (mapped
# pages cannot be dropped); the Go daemon frees its range cache after idle.
drop() { sleep "${SETTLE:-3}"; sync; echo 3 > /proc/sys/vm/drop_caches; }
NEXT=0
next_file() { FI=$NEXT; NEXT=$(( (NEXT + 1) % FILES )); }

# ---------------------------------------------------------------- mounts
declare -A MNT PID
PIDS=()
cleanup() {
  for m in "${MNT[@]:-}"; do [ -n "$m" ] && { umount "$m" 2>/dev/null || umount -l "$m" 2>/dev/null || true; }; done
  for p in "${PIDS[@]:-}"; do [ -n "$p" ] && kill "$p" 2>/dev/null || true; done
  if [ -z "${KEEP:-}" ] && [ -e "$CACHE/$MARK" ]; then
    log "removing dataset in $CACHE (KEEP=1 keeps it)"
    rm -rf "${CACHE:?}/bench" "${CACHE:?}/blobsrc" "${CACHE:?}/blobdst" "${CACHE:?}/.blob-digest" \
      "${CACHE:?}/.fuse-durability" "${CACHE:?}/.fuse-durability.readbench-hidden" "$CACHE/$MARK"
  fi
}
trap cleanup EXIT
bdi_ra() { cat "/sys/class/bdi/$(mountpoint -d "$1")/read_ahead_kb"; }
FUSE_IMPLS=()
if [[ " $LAYERS " == *" fuse "* ]]; then
  FUSE_IMPLS=(go go-pt go-ra4m rust rust-pt)
  for impl in "${FUSE_IMPLS[@]}"; do
    MNT[$impl]=$SCRATCH/mnt-$impl; mkdir -p "${MNT[$impl]}"
    case $impl in
      rust)    "$RS" mount --dir "$CACHE" --mnt "${MNT[$impl]}" --chunk-mb "$CHUNK_MB" "${PUB_RS[@]}" >>"$SCRATCH/mount-$impl.log" 2>&1 & ;;
      rust-pt) "$RS" mount --dir "$CACHE" --mnt "${MNT[$impl]}" --chunk-mb "$CHUNK_MB" --passthrough "${PUB_RS[@]}" >>"$SCRATCH/mount-$impl.log" 2>&1 & ;;
      go-pt)   FUSE_GOFUSE_ENABLE_PASSTHROUGH=1 FUSE_GOFUSE_DIRECT_MOUNT=1 \
               "$RB" mount -dir "$CACHE" -mnt "${MNT[$impl]}" -chunk-mb "$CHUNK_MB" "${PUB_GO[@]}" >>"$SCRATCH/mount-$impl.log" 2>&1 & ;;
      # Direct mount(2) for every Go mount: on Ubuntu 26.04 hosts AppArmor
      # confines the fusermount3 helper and it fails with EACCES. It changes
      # how the mount is created, not how reads are served.
      *)       FUSE_GOFUSE_DIRECT_MOUNT=1 \
               "$RB" mount -dir "$CACHE" -mnt "${MNT[$impl]}" -chunk-mb "$CHUNK_MB" "${PUB_GO[@]}" >>"$SCRATCH/mount-$impl.log" 2>&1 & ;;
    esac
    PID[$impl]=$!; PIDS+=($!)
  done
  for impl in "${FUSE_IMPLS[@]}"; do
    for _ in $(seq 1 100); do mountpoint -q "${MNT[$impl]}" && break; sleep 0.1; done
    mountpoint -q "${MNT[$impl]}" || { echo "mount $impl did not come up; see $SCRATCH/mount-$impl.log" >&2; exit 1; }
  done
  sleep 1   # rust sets its readahead from a side thread right after INIT
  echo 4096 > "/sys/class/bdi/$(mountpoint -d "${MNT[go-ra4m]}")/read_ahead_kb"
  for impl in "${FUSE_IMPLS[@]}"; do log "mount $impl at ${MNT[$impl]} read_ahead_kb=$(bdi_ra "${MNT[$impl]}")"; done
  grep -h -i "passthrough" "$SCRATCH"/mount-*-pt.log | tail -4 | while read -r l; do log "  $l"; done
fi

# Blob servers: holders over the blob dir, a coordinator per implementation
# naming its holder, readers over an empty dir that fetch every window.
BLOB_IMPLS=()
declare -A BPORT
if [[ " $LAYERS " == *" blob "* ]]; then
  [ -n "$BLOB_DIGEST" ] || { echo "no blob digest (dataset not generated?)" >&2; exit 1; }
  BP="/oci/sha256/$BLOB_DIGEST"
  mkdir -p "$CACHE/blobdst"
  "$RB" serve-peer -dir "$CACHE/blobsrc" -listen "$HOST_IP:19101" -peer-id go-holder -chunk-mb "$CHUNK_MB" -publish "$BP:$BLOB_SIZE:$CHUNK_MB" >>"$SCRATCH/blob-go-holder.log" 2>&1 & PID[go-holder]=$!; PIDS+=($!)
  "$RS" serve-peer --dir "$CACHE/blobsrc" --listen "$HOST_IP:19102" --chunk-mb "$CHUNK_MB" --publish "$BP:$BLOB_SIZE:$CHUNK_MB" >>"$SCRATCH/blob-rs-holder.log" 2>&1 & PID[rust-holder]=$!; PIDS+=($!)
  "$RB" coord -listen "127.0.0.1:19110" -peer "go-holder=$HOST_IP:19101" -publish "$BP:$BLOB_SIZE:$CHUNK_MB:go-holder" >>"$SCRATCH/blob-coord.log" 2>&1 & PIDS+=($!)
  "$RB" coord -listen "127.0.0.1:19111" -peer "rs-holder=$HOST_IP:19102" -publish "$BP:$BLOB_SIZE:$CHUNK_MB:rs-holder" >>"$SCRATCH/blob-coord.log" 2>&1 & PIDS+=($!)
  sleep 2
  "$RB" serve-peer -dir "$CACHE/blobdst" -listen "$HOST_IP:19201" -peer-id go-reader -chunk-mb "$CHUNK_MB" -coord 127.0.0.1:19110 >>"$SCRATCH/blob-go-reader.log" 2>&1 & PID[go-reader]=$!; PIDS+=($!)
  "$RS" serve-peer --dir "$CACHE/blobdst" --listen "$HOST_IP:19202" --chunk-mb "$CHUNK_MB" --coord 127.0.0.1:19111 --peer-id rs-reader --no-promote >>"$SCRATCH/blob-rs-reader.log" 2>&1 & PID[rust-reader]=$!; PIDS+=($!)
  BPORT[go-local]=19101; BPORT[rust-local]=19102; BPORT[go-remote]=19201; BPORT[rust-remote]=19202
  for port in 19101 19102 19201 19202; do
    for _ in $(seq 1 100); do curl -sf -o /dev/null "http://$HOST_IP:$port/v2/" && break; sleep 0.1; done
    curl -sf -o /dev/null "http://$HOST_IP:$port/v2/" || { echo "blob server on :$port did not come up; see $SCRATCH/blob-*.log" >&2; exit 1; }
  done
  log "blob servers up on $HOST_IP (digest ${BLOB_DIGEST:0:12}, $BLOB_SIZE bytes)"
  BLOB_IMPLS=(go rust)
fi
TCK=$(getconf CLK_TCK)
ticks() { awk '{print $14 + $15}' "/proc/$1/stat"; }

# ---------------------------------------------------------------- one cell
# run IMPL LAYER MODE PATTERN WIN_KIB THREADS TOTAL_MB
run() {
  local impl=$1 layer=$2 mode=$3 pat=$4 win=$5 thr=$6 total=$7 row t0=0 t1=0 h0=0 h1=0 p
  next_file
  if [ "$layer" = fuse ]; then p=$(wpath_of "$FI"); else p=$(path_of "$FI"); fi
  drop
  if [ "$mode" = warm ]; then
    case $layer in
      fuse) cat "$CACHE$p" > /dev/null ;;
      blob-*) cat "$CACHE"/blobsrc/oci/sha256/*_chunk_* > /dev/null ;;
      *) cat "$CACHE$p"_chunk_* > /dev/null ;;
    esac
  fi
  if [ "${layer%%-*}" = blob ]; then
    local case=${layer#blob-} srv holder
    srv=${impl}-$([ "$case" = local ] && echo holder || echo reader)
    holder=${impl}-holder
    t0=$(ticks "${PID[$srv]}"); [ "$case" = remote ] && h0=$(ticks "${PID[$holder]}")
    row=$("$RB" bench-blob -url "http://$HOST_IP:${BPORT[$impl-$case]}" -digest "$BLOB_DIGEST" -impl "$impl" -layer "$layer" \
          -window-kib "$win" -pattern "$pat" -streams "$thr" -total-mb "$total" 2>>"$LOG")
    t1=$(ticks "${PID[$srv]}"); [ "$case" = remote ] && h1=$(ticks "${PID[$holder]}")
    p=$BP
  elif [ "$layer" = library ]; then
    if [ "$impl" = go ]; then
      row=$("$RB" bench -dir "$CACHE" -path "$p" -size "$SIZE" -stride-mb "$CHUNK_MB" -chunk-mb "$CHUNK_MB" \
            -window-kib "$win" -pattern "$pat" -threads "$thr" -total-mb "$total" 2>>"$LOG")
    else
      row=$("$RS" bench --dir "$CACHE" --path "$p" --chunk-mb "$CHUNK_MB" --publish "$p:$SIZE:$CHUNK_MB" \
            --window-kib "$win" --pattern "$pat" --threads "$thr" --total-mb "$total" 2>>"$LOG")
    fi
  else
    t0=$(ticks "${PID[$impl]}")
    row=$("$RB" bench-file -file "${MNT[$impl]}$p" -impl "$impl" -layer fuse -window-kib "$win" \
          -pattern "$pat" -threads "$thr" -total-mb "$total" 2>>"$LOG")
    t1=$(ticks "${PID[$impl]}")
  fi
  python3 -c '
import json, sys
r = json.loads(sys.argv[1])
r.update(impl=sys.argv[2], mode=sys.argv[3], rep=int(sys.argv[4]), file=sys.argv[5])
g = r["bytes"] / 2**30
if r["layer"] != "library":
    r["server_cpu_s_per_gib"] = (int(sys.argv[7]) - int(sys.argv[6])) / int(sys.argv[8]) / g
if r["layer"] == "blob-remote":
    r["holder_cpu_s_per_gib"] = (int(sys.argv[10]) - int(sys.argv[9])) / int(sys.argv[8]) / g
print(json.dumps(r))' "$row" "$impl" "$mode" "$REP" "$p" "$t0" "$t1" "$TCK" "$h0" "$h1" >> "$ROWS"
}

FILE_MB=$(( FILE_GB * 1024 ))
log "matrix: layers=[$LAYERS] reps=$REPS files=${FILES}x${FILE_GB}GiB chunk=${CHUNK_MB}MiB"
start=$(date +%s)
for REP in $(seq 1 "$REPS"); do
  for layer in $LAYERS; do
    if [ "$layer" = blob ]; then
      for case in local remote; do
        for impl in "${BLOB_IMPLS[@]}"; do
          run "$impl" "blob-$case" cold rand 1024 4 "$BLOB_TOTAL_MB"
          run "$impl" "blob-$case" cold seq  1024 4 "$BLOB_TOTAL_MB"
          run "$impl" "blob-$case" cold rand 128  4 $(( BLOB_TOTAL_MB / 4 ))
          run "$impl" "blob-$case" warm rand 1024 4 "$BLOB_TOTAL_MB"
        done
      done
      log "rep $REP layer blob done ($(( $(date +%s) - start ))s)"
      continue
    fi
    if [ "$layer" = library ]; then impls=(go rust); else impls=("${FUSE_IMPLS[@]}"); fi
    for impl in "${impls[@]}"; do
      run "$impl" "$layer" cold seq  128  1 "$FILE_MB"
      run "$impl" "$layer" cold seq  1024 1 "$FILE_MB"
      run "$impl" "$layer" warm seq  128  1 "$FILE_MB"
      run "$impl" "$layer" warm seq  1024 1 "$FILE_MB"
      run "$impl" "$layer" cold rand 128  1 "$RAND_MB"
      run "$impl" "$layer" cold seq  1024 8 "$FILE_MB"
    done
    log "rep $REP layer $layer done ($(( $(date +%s) - start ))s)"
  done
done

# ---------------------------------------------------------------- results
ENV_JSON=$(RS_BIN=$RS FILES=$FILES FILE_GB=$FILE_GB CHUNK_MB=$CHUNK_MB REPS=$REPS python3 - "$CACHE" <<'EOF'
import json, os, platform, subprocess, sys
def sh(c):
    try: return subprocess.run(c, shell=True, capture_output=True, text=True, timeout=10).stdout.strip()
    except Exception as e: return f"error: {e}"
cache = sys.argv[1]
src = sh(f"df -P {cache} | awk 'NR==2{{print $1}}'")
print(json.dumps({
    "hostname": platform.node(), "kernel": platform.release(), "arch": platform.machine(),
    "cpu": sh("grep -m1 'model name' /proc/cpuinfo | cut -d: -f2"), "ncpu": os.cpu_count(),
    "mem_kib": sh("awk '/MemTotal/{print $2}' /proc/meminfo"),
    "cache_dir": cache, "cache_fs": sh(f"df -PT {cache} | awk 'NR==2{{print $2}}'"), "cache_dev": src,
    "lsblk": sh("lsblk -d -e 7,43 -o NAME,MODEL,SIZE,ROTA,TYPE 2>/dev/null"),
    "files": int(os.environ.get("FILES", 4)), "file_gb": int(os.environ.get("FILE_GB", 4)),
    "chunk_mb": int(os.environ.get("CHUNK_MB", 16)), "reps": int(os.environ.get("REPS", 3)),
    "fusedata": sh(f"{os.environ.get('RS_BIN','fusedata')} --version"),
}))
EOF
)
python3 - "$ROWS" "$OUT" "$ENV_JSON" <<'EOF' | tee -a "$LOG"
import collections, json, statistics as st, sys
rows = [json.loads(l) for l in open(sys.argv[1]) if l.startswith("{")]
env = json.loads(sys.argv[3])
g = collections.defaultdict(list)
for r in rows:
    g[(r["layer"], r["mode"], r["pattern"], r["window_kib"], r["threads"], r["impl"])].append(r)
summary = []
for k, v in sorted(g.items()):
    med = lambda f: st.median(x[f] for x in v if f in x) if any(f in x for x in v) else None
    mbs = [x["mb_per_s"] for x in v]
    summary.append(dict(layer=k[0], mode=k[1], pattern=k[2], window_kib=k[3], threads=k[4], impl=k[5],
                        n=len(v), mb_per_s=med("mb_per_s"), mb_per_s_min=min(mbs), mb_per_s_max=max(mbs),
                        p50_us=med("p50_us"), p99_us=med("p99_us"),
                        cpu_s_per_gib=med("cpu_s_per_gib"), server_cpu_s_per_gib=med("server_cpu_s_per_gib"),
                        holder_cpu_s_per_gib=med("holder_cpu_s_per_gib")))
json.dump({"env": env, "rows": rows, "summary": summary}, open(sys.argv[2], "w"), indent=1)
by = {(s["layer"], s["mode"], s["pattern"], s["window_kib"], s["threads"], s["impl"]): s for s in summary}
cells = sorted({k[:5] for k in by}, key=lambda c: (c[0], c[1], c[2] != "seq", c[4], c[3]))
impls = [i for i in ("go", "go-pt", "go-ra4m", "rust", "rust-pt") if any(k[5] == i for k in by)]
print(f"{'layer':11} {'mode':5} {'case':14} " + " ".join(f"{i:>16}" for i in impls) + "   (MB/s median, ratio vs go)")
for c in cells:
    go = by.get(c + ("go",))
    out = []
    for i in impls:
        s = by.get(c + (i,))
        if not s: out.append(f"{'-':>16}"); continue
        r = f" {s['mb_per_s']/go['mb_per_s']:5.2f}x" if go and i != "go" else "       "
        out.append(f"{s['mb_per_s']:9.0f}{r}")
    print(f"{c[0]:11} {c[1]:5} {c[2]+' '+str(c[3])+'K t'+str(c[4]):14} " + " ".join(out))
EOF
log "done in $(( $(date +%s) - start ))s; results in $OUT (raw rows: $ROWS)"

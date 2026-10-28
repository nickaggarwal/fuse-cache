#!/usr/bin/env bash
# Self-contained Go-vs-Rust read-path benchmark for one node.
#
#   node-bench.sh CACHE_DIR SCRATCH_DIR [RESULTS_JSON]
#
# CACHE_DIR    a NEW or EMPTY directory on the disk to measure (the NVMe cache
#              disk). The dataset is generated there by the Go client's own
#              write path (PutFromReader -> <path>_chunk_N files), read by
#              both implementations, and deleted at the end (KEEP=1 keeps it).
#              Needs FILES x FILE_GB GiB free + 1 GiB (default 4 x 4 + 1 = 17).
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
#   layer fuse:    pread client go (go-fuse mount, kernel readahead 128 KiB,
#                  the default) | go-ra4m (same Go mount, read_ahead_kb set to
#                  4 MiB: shows how much is the mount setting) | rust (sets
#                  4 MiB itself)
#   cases: cold seq 128K, cold seq 1M, warm seq 128K, warm seq 1M,
#          cold rand 128K (1 reader); cold seq 1M with 8 readers
#   cold = page cache dropped; warm = dropped, then the backing chunk files
#   read once (the daemon reads from page cache, the FUSE cache is empty).
# Single-reader seq reads one whole file; each cell uses the next file in
# turn so consecutive cells never reread the same bytes.
#
# Env: FILES=4 FILE_GB=4 CHUNK_MB=16 REPS=3 RAND_MB=1024 LAYERS="library fuse"
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
LAYERS=${LAYERS:-"library fuse"}
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
path_of() { echo "/bench/f$1.bin"; }

# ---------------------------------------------------------------- dataset
if [ -z "${SKIP_GEN:-}" ]; then
  need_kb=$(( (FILES * FILE_GB + 1) * 1024 * 1024 ))
  avail_kb=$(df -Pk "$CACHE" | awk 'NR==2{print $4}')
  [ "$avail_kb" -ge "$need_kb" ] || { echo "need $((need_kb/1024/1024)) GiB free in $CACHE, have $((avail_kb/1024/1024))" >&2; exit 1; }
  touch "$CACHE/$MARK"
  for i in $(seq 0 $((FILES-1))); do
    if [ -e "$CACHE$(path_of "$i")_chunk_0" ]; then log "dataset f$i exists, reusing"; continue; fi
    log "generating $(path_of "$i") ($FILE_GB GiB, ${CHUNK_MB} MiB chunks)"
    "$RB" gen -dir "$CACHE" -path "$(path_of "$i")" -size-mb $((FILE_GB*1024)) -extra-bytes 4321 \
      -chunk-mb "$CHUNK_MB" -layout chunked >>"$LOG" 2>&1
  done
fi
PUB_GO=(); PUB_RS=()
for i in $(seq 0 $((FILES-1))); do
  PUB_GO+=(-publish "$(path_of "$i"):$SIZE:$CHUNK_MB")
  PUB_RS+=(--publish "$(path_of "$i"):$SIZE:$CHUNK_MB")
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
    rm -rf "${CACHE:?}/bench" "${CACHE:?}/.fuse-durability" "${CACHE:?}/.fuse-durability.readbench-hidden" "$CACHE/$MARK"
  fi
}
trap cleanup EXIT
bdi_ra() { cat "/sys/class/bdi/$(mountpoint -d "$1")/read_ahead_kb"; }
FUSE_IMPLS=()
if [[ " $LAYERS " == *" fuse "* ]]; then
  FUSE_IMPLS=(go go-ra4m rust)
  for impl in "${FUSE_IMPLS[@]}"; do
    MNT[$impl]=$SCRATCH/mnt-$impl; mkdir -p "${MNT[$impl]}"
    if [ "$impl" = rust ]; then
      "$RS" mount --dir "$CACHE" --mnt "${MNT[$impl]}" --chunk-mb "$CHUNK_MB" "${PUB_RS[@]}" >>"$SCRATCH/mount-$impl.log" 2>&1 &
    else
      "$RB" mount -dir "$CACHE" -mnt "${MNT[$impl]}" -chunk-mb "$CHUNK_MB" "${PUB_GO[@]}" >>"$SCRATCH/mount-$impl.log" 2>&1 &
    fi
    PID[$impl]=$!; PIDS+=($!)
  done
  for impl in "${FUSE_IMPLS[@]}"; do
    for _ in $(seq 1 100); do mountpoint -q "${MNT[$impl]}" && break; sleep 0.1; done
    mountpoint -q "${MNT[$impl]}" || { echo "mount $impl did not come up; see $SCRATCH/mount-$impl.log" >&2; exit 1; }
  done
  sleep 1   # rust sets its readahead from a side thread right after INIT
  echo 4096 > "/sys/class/bdi/$(mountpoint -d "${MNT[go-ra4m]}")/read_ahead_kb"
  for impl in "${FUSE_IMPLS[@]}"; do log "mount $impl at ${MNT[$impl]} read_ahead_kb=$(bdi_ra "${MNT[$impl]}")"; done
fi
TCK=$(getconf CLK_TCK)
ticks() { awk '{print $14 + $15}' "/proc/$1/stat"; }

# ---------------------------------------------------------------- one cell
# run IMPL LAYER MODE PATTERN WIN_KIB THREADS TOTAL_MB
run() {
  local impl=$1 layer=$2 mode=$3 pat=$4 win=$5 thr=$6 total=$7 row t0=0 t1=0 p
  next_file; p=$(path_of "$FI")
  drop
  [ "$mode" = warm ] && cat "$CACHE$p"_chunk_* > /dev/null
  if [ "$layer" = library ]; then
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
if r["layer"] == "fuse":
    r["server_cpu_s_per_gib"] = (int(sys.argv[7]) - int(sys.argv[6])) / int(sys.argv[8]) / (r["bytes"] / 2**30)
print(json.dumps(r))' "$row" "$impl" "$mode" "$REP" "$p" "$t0" "$t1" "$TCK" >> "$ROWS"
}

FILE_MB=$(( FILE_GB * 1024 ))
log "matrix: layers=[$LAYERS] reps=$REPS files=${FILES}x${FILE_GB}GiB chunk=${CHUNK_MB}MiB"
start=$(date +%s)
for REP in $(seq 1 "$REPS"); do
  for layer in $LAYERS; do
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
    summary.append(dict(layer=k[0], mode=k[1], pattern=k[2], window_kib=k[3], threads=k[4], impl=k[5],
                        n=len(v), mb_per_s=med("mb_per_s"), p50_us=med("p50_us"), p99_us=med("p99_us"),
                        cpu_s_per_gib=med("cpu_s_per_gib"), server_cpu_s_per_gib=med("server_cpu_s_per_gib")))
json.dump({"env": env, "rows": rows, "summary": summary}, open(sys.argv[2], "w"), indent=1)
by = {(s["layer"], s["mode"], s["pattern"], s["window_kib"], s["threads"], s["impl"]): s for s in summary}
cells = sorted({k[:5] for k in by}, key=lambda c: (c[0], c[1], c[2] != "seq", c[4], c[3]))
impls = [i for i in ("go", "go-ra4m", "rust") if any(k[5] == i for k in by)]
print(f"{'layer':8} {'mode':5} {'case':14} " + " ".join(f"{i:>16}" for i in impls) + "   (MB/s, ratio vs go)")
for c in cells:
    go = by.get(c + ("go",))
    out = []
    for i in impls:
        s = by.get(c + (i,))
        if not s: out.append(f"{'-':>16}"); continue
        r = f" {s['mb_per_s']/go['mb_per_s']:5.2f}x" if go and i != "go" else "       "
        out.append(f"{s['mb_per_s']:9.0f}{r}")
    print(f"{c[0]:8} {c[1]:5} {c[2]+' '+str(c[3])+'K t'+str(c[4]):14} " + " ".join(out))
EOF
log "done in $(( $(date +%s) - start ))s; results in $OUT (raw rows: $ROWS)"

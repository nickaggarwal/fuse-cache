#!/bin/sh
# Full Go-vs-Rust matrix inside the lab container. Expects:
#   /out/readbench-linux, /out/fusedata          (binaries)
#   /cache/c                                      (cache dir from `readbench gen`)
#   /mnt/go, /mnt/rs mounted                      (FUSE layer)
#   Go peer on :18081, Rust peer on :18082        (peer transport)
# Writes JSONL to $OUT (default /out/results).
set -eu
OUT=${OUT:-/out/results}
B=$(dirname "$0")
mkdir -p "$OUT"
MODE=warm REPS=3 TOTAL=1024 sh "$B/run-library.sh" > "$OUT/library-warm.jsonl"
MODE=cold REPS=3 TOTAL=512  sh "$B/run-library.sh" > "$OUT/library-cold.jsonl"
MODE=warm REPS=3 TOTAL=1024 THREADS=8 sh "$B/run-library.sh" > "$OUT/library-warm-t8.jsonl"
OUT="$OUT" sh "$B/run-fuse-all.sh"
REPS=3 sh "$B/run-peer.sh" > "$OUT/peer.jsonl"
echo "results in $OUT"

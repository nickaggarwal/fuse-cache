#!/bin/sh
# The three FUSE matrices from run-all.sh, on their own.
set -eu
OUT=${OUT:-/out/results}
B=$(dirname "$0")
mkdir -p "$OUT"
MODE=cold REPS=3 TOTAL=512  sh "$B/run-fuse.sh" > "$OUT/fuse-cold.jsonl"
MODE=warm REPS=3 TOTAL=1024 sh "$B/run-fuse.sh" > "$OUT/fuse-warm.jsonl"
MODE=cold REPS=3 TOTAL=1024 THREADS=8 sh "$B/run-fuse.sh" > "$OUT/fuse-cold-t8.jsonl"

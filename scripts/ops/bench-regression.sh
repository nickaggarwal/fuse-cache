#!/usr/bin/env bash
# Microbenchmark regression harness: compare HEAD against its parent (or any
# baseline) on this machine, in one run, and report what actually moved.
#
# The design follows from one measurement. On an idle M4 Pro, six consecutive
# runs of the same unchanged BenchmarkGetNVMeHit spanned 777us..864us -- 11%
# apart with nothing changed at all. So:
#
#   - An absolute number from a single run carries no information about a
#     code change. Only A/B on the same machine, close in time, does.
#   - A threshold below the noise floor fires constantly and gets ignored,
#     which is worse than no harness. Hence -count and benchstat's p-value
#     rather than comparing two single numbers.
#
# Both sides are built from git worktrees, so the comparison is of committed
# trees and cannot be polluted by uncommitted edits. Benchmarks are taken
# from the NEW side and run against both trees, so a benchmark added in HEAD
# still produces a baseline column instead of silently measuring nothing.
#
# Exit codes:  0 = no regression   1 = regression   2 = harness error
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$REPO_ROOT"

PKG="${PKG:-./internal/cache}"
BENCH="${BENCH:-.}"
COUNT="${COUNT:-6}"
BENCHTIME="${BENCHTIME:-50x}"
THRESHOLD="${THRESHOLD:-10}"      # percent slower before we call it a regression
BASE="${BASE:-}"
OUT_DIR="${OUT_DIR:-/tmp/bench-regression-$(date +%Y%m%d-%H%M%S)}"
KEEP_WORKTREES="${KEEP_WORKTREES:-0}"

usage() {
  cat <<EOF
Usage: $(basename "$0") [--base <ref>] [--pkg <pkg>] [--bench <re>]
                        [--count N] [--benchtime T] [--threshold PCT]

  --base <ref>       baseline to compare against  (default: HEAD^, or the
                     merge-base with origin/main when run on a PR branch)
  --pkg <pkg>        package(s) to benchmark      (default: ${PKG})
  --bench <re>       benchmark regexp             (default: ${BENCH})
  --count N          runs per benchmark per side  (default: ${COUNT})
  --benchtime T      go test -benchtime           (default: ${BENCHTIME})
  --threshold PCT    regression threshold in %    (default: ${THRESHOLD})
  --keep             keep the worktrees for debugging

Env vars of the same name (PKG, BENCH, COUNT, BENCHTIME, THRESHOLD, BASE) also work.
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --base) BASE="$2"; shift 2 ;;
    --pkg) PKG="$2"; shift 2 ;;
    --bench) BENCH="$2"; shift 2 ;;
    --count) COUNT="$2"; shift 2 ;;
    --benchtime) BENCHTIME="$2"; shift 2 ;;
    --threshold) THRESHOLD="$2"; shift 2 ;;
    --keep) KEEP_WORKTREES=1; shift ;;
    -h|--help) usage; exit 0 ;;
    *) echo "unknown option: $1" >&2; usage; exit 2 ;;
  esac
done

command -v git >/dev/null || { echo "git not found" >&2; exit 2; }
command -v go  >/dev/null || { echo "go not found"  >&2; exit 2; }

BENCHSTAT="$(command -v benchstat || true)"
if [[ -z "$BENCHSTAT" && -x "$(go env GOPATH)/bin/benchstat" ]]; then
  BENCHSTAT="$(go env GOPATH)/bin/benchstat"
fi
if [[ -z "$BENCHSTAT" ]]; then
  echo "benchstat not found; installing..." >&2
  go install golang.org/x/perf/cmd/benchstat@latest >&2 || { echo "install failed" >&2; exit 2; }
  BENCHSTAT="$(go env GOPATH)/bin/benchstat"
fi

NEW_SHA="$(git rev-parse HEAD)"

# Default baseline: the merge-base with origin/main on a branch, HEAD^ otherwise.
# On a PR the interesting question is "what did this branch do", not "what did
# the last commit do" -- comparing against HEAD^ on a 5-commit branch measures
# only the final commit and calls the branch clean.
if [[ -z "$BASE" ]]; then
  if git rev-parse --verify -q origin/main >/dev/null; then
    MB="$(git merge-base HEAD origin/main 2>/dev/null || true)"
    if [[ -n "$MB" && "$MB" != "$NEW_SHA" ]]; then BASE="$MB"; fi
  fi
  [[ -z "$BASE" ]] && BASE="HEAD^"
fi

BASE_SHA="$(git rev-parse --verify "${BASE}^{commit}" 2>/dev/null || true)"
if [[ -z "$BASE_SHA" ]]; then
  echo "cannot resolve baseline ref: ${BASE}" >&2
  exit 2
fi
if [[ "$BASE_SHA" == "$NEW_SHA" ]]; then
  echo "baseline and HEAD are the same commit (${NEW_SHA:0:8}); nothing to compare."
  exit 0
fi

mkdir -p "$OUT_DIR"
WT_BASE="$OUT_DIR/wt-base"
WT_NEW="$OUT_DIR/wt-new"

cleanup() {
  if [[ "$KEEP_WORKTREES" != "1" ]]; then
    git worktree remove --force "$WT_BASE" 2>/dev/null || true
    git worktree remove --force "$WT_NEW"  2>/dev/null || true
  fi
}
trap cleanup EXIT

echo "=== bench-regression ==="
echo "  new:       ${NEW_SHA:0:12}  $(git log -1 --format=%s "$NEW_SHA" | cut -c1-60)"
echo "  base:      ${BASE_SHA:0:12}  $(git log -1 --format=%s "$BASE_SHA" | cut -c1-60)"
echo "  pkg:       ${PKG}"
echo "  bench:     ${BENCH}"
echo "  count:     ${COUNT} x ${BENCHTIME}"
echo "  threshold: ${THRESHOLD}%"
echo "  out:       ${OUT_DIR}"
echo

git worktree add --detach --quiet "$WT_BASE" "$BASE_SHA" || { echo "worktree add (base) failed" >&2; exit 2; }
git worktree add --detach --quiet "$WT_NEW"  "$NEW_SHA"  || { echo "worktree add (new) failed"  >&2; exit 2; }

# Benchmark FUNCTIONS come from the new tree, for both sides.
#
# A benchmark file added in HEAD does not exist in the baseline tree, so
# running each side against its own copy would silently produce no baseline
# row and benchstat would report the new benchmark as uncomparable rather
# than as a result. Copying the new *_test.go files over the baseline tree
# measures OLD code through NEW benchmarks, which is the comparison we want.
#
# This is only valid while the benchmarks compile against both trees; when a
# benchmark uses an API that does not exist in the baseline, the baseline
# build fails loudly below rather than quietly skipping.
copy_bench_files() {
  local pkgdir="$1"
  [[ -d "$WT_NEW/$pkgdir" ]] || return 0
  find "$WT_NEW/$pkgdir" -maxdepth 1 -name "*_test.go" -exec cp {} "$WT_BASE/$pkgdir/" \; 2>/dev/null || true
}

# Resolve ./internal/cache -> internal/cache for the copy step.
for p in $PKG; do
  case "$p" in
    ./...|...) ;;  # too broad to mirror file-by-file; rely on both trees having them
    *) copy_bench_files "${p#./}" ;;
  esac
done

# Interleaving matters more than it looks. Thermal drift and background load
# on a laptop or a shared CI runner move monotonically over minutes, so
# running all of base then all of new attributes that drift to the code
# change. Alternating base/new per repetition spreads it across both sides.
: >"$OUT_DIR/base.txt"
: >"$OUT_DIR/new.txt"
for i in $(seq 1 "$COUNT"); do
  echo "=== repetition $i/$COUNT ==="
  ( cd "$WT_BASE" && go test $PKG -run '^$' -bench "$BENCH" \
      -benchtime "$BENCHTIME" -count 1 -timeout 60m ) >>"$OUT_DIR/base.txt" 2>>"$OUT_DIR/base.err" || {
    echo "baseline benchmark failed; stderr:" >&2; tail -30 "$OUT_DIR/base.err" >&2; exit 2; }
  ( cd "$WT_NEW" && go test $PKG -run '^$' -bench "$BENCH" \
      -benchtime "$BENCHTIME" -count 1 -timeout 60m ) >>"$OUT_DIR/new.txt" 2>>"$OUT_DIR/new.err" || {
    echo "new benchmark failed; stderr:" >&2; tail -30 "$OUT_DIR/new.err" >&2; exit 2; }
done

echo
echo "=== benchstat ==="
"$BENCHSTAT" "$OUT_DIR/base.txt" "$OUT_DIR/new.txt" | tee "$OUT_DIR/benchstat.txt"
echo

# Verdict.
#
# benchstat's own p-value is the gate, not the percentage alone: a 12% delta
# that benchstat marks "~" means the samples overlap too much to distinguish
# from the noise measured above, and treating it as a regression is how a
# harness trains people to ignore it.
python3 - "$OUT_DIR/benchstat.txt" "$THRESHOLD" <<'PY'
import re, sys

path, thr = sys.argv[1], float(sys.argv[2])
text = open(path).read()

regressions, improvements, noisy = [], [], []
# benchstat >=v0.2 rows: name  old  ±%  new  ±%  delta  (p=... n=...) or "~"
row = re.compile(r'^(\S+)\s+.*?([-+]\d+\.\d+)%\s*\((p=([\d.]+)[^)]*)\)')
for line in text.splitlines():
    line = line.rstrip()
    if not line or line.startswith(('goos', 'goarch', 'pkg', 'cpu', '│')):
        continue
    m = row.match(line)
    if not m:
        if '~' in line and line.split() and line.split()[0].startswith('Benchmark'):
            noisy.append(line.split()[0])
        continue
    name, delta = m.group(1), float(m.group(2))
    # ns/op and similar: positive delta = slower = worse. Throughput columns
    # (MB/s) invert, but benchstat reports those in separate sections headed
    # by the unit, so we only gate on the sec/op section.
    if delta >= thr:
        regressions.append((name, delta))
    elif delta <= -thr:
        improvements.append((name, delta))

if improvements:
    print("IMPROVED (statistically significant):")
    for n, d in improvements:
        print(f"  {n}  {d:+.1f}%")
if noisy:
    print(f"\n{len(noisy)} benchmark(s) showed no significant change (benchstat '~').")
if regressions:
    print("\nREGRESSED (statistically significant, beyond threshold):")
    for n, d in regressions:
        print(f"  {n}  {d:+.1f}%")
    print(f"\nFAIL: {len(regressions)} benchmark(s) regressed by >={thr}%")
    sys.exit(1)

print(f"\nPASS: no benchmark regressed by >={thr}% with statistical significance.")
sys.exit(0)
PY
VERDICT=$?

echo
echo "raw results kept in ${OUT_DIR}"
exit $VERDICT

#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
KUBECTL_BIN="${KUBECTL_BIN:-/opt/homebrew/bin/kubectl}"
HELM_BIN="${HELM_BIN:-/opt/homebrew/bin/helm}"
AZ_BIN="${AZ_BIN:-/opt/homebrew/bin/az}"
NAMESPACE="${NAMESPACE:-fuse-system-aztest}"
RELEASE="${RELEASE:-fuse-cache-aztest}"
ACR_NAME="${ACR_NAME:-stargzrepo}"
IMAGE_REPO="${IMAGE_REPO:-stargzrepo.azurecr.io/fuse-client}"
CONTEXT="${CONTEXT:-aks-stargz-test-admin}"
RANGE_READS="${RANGE_READS:-32}"
PREFETCH_CHUNKS="${PREFETCH_CHUNKS:-8}"
RANGE_CACHE="${RANGE_CACHE:-384}"
PEER_MBPS="${PEER_MBPS:-10000}"
AZ_DL_CONCURRENCY="${AZ_DL_CONCURRENCY:-32}"
AZ_DL_BLOCK_MB="${AZ_DL_BLOCK_MB:-8}"
AZ_DL_MIN_MB="${AZ_DL_MIN_MB:-4}"
NVME_MAX_GB="${NVME_MAX_GB:-48}"
HYBRID_ALWAYS_MB="${HYBRID_ALWAYS_MB:-512}"
HYBRID_HEDGE_MS="${HYBRID_HEDGE_MS:-5}"
HYBRID_MAX_SECONDARY="${HYBRID_MAX_SECONDARY:-16}"
SIZE_MB="${SIZE_MB:-1024}"
HYBRID_SETTLE_SEC="${HYBRID_SETTLE_SEC:-20}"
TEST_TIMEOUT_SEC="${TEST_TIMEOUT_SEC:-900}"
COMMIT_RANGE="${COMMIT_RANGE:-5776ea7..HEAD}"
COMMIT_LIST="${COMMIT_LIST:-}"
TIMESTAMP="$(date +%Y%m%d-%H%M%S)"
OUT_CSV="${OUT_CSV:-/tmp/azure-commit-bench-${TIMESTAMP}.csv}"
OUT_LOG="${OUT_LOG:-/tmp/azure-commit-bench-${TIMESTAMP}.log}"

usage() {
  cat <<EOF
Usage: $(basename "$0") [options]

Options:
  --commit-list "<sha sha ...>"   Explicit commits to benchmark
  --commit-range "<range>"        Git commit range (default: ${COMMIT_RANGE})
  --out-csv <path>                CSV output path
  --out-log <path>                Log output path
  --range-reads <n>               FUSE_PARALLEL_RANGE_READS override
  --prefetch <n>                  FUSE_RANGE_PREFETCH_CHUNKS override
  --peer-mbps <n>                 FUSE_PEER_READ_MBPS override
  --size-mb <n>                   Benchmark file size in MiB (default: ${SIZE_MB})
  --settle-sec <n>                Post-write settle wait before read (default: ${HYBRID_SETTLE_SEC})
  --test-timeout-sec <n>          Per-commit benchmark timeout (default: ${TEST_TIMEOUT_SEC})
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --commit-list)
      COMMIT_LIST="${2:?missing value}"
      shift 2
      ;;
    --commit-range)
      COMMIT_RANGE="${2:?missing value}"
      shift 2
      ;;
    --out-csv)
      OUT_CSV="${2:?missing value}"
      shift 2
      ;;
    --out-log)
      OUT_LOG="${2:?missing value}"
      shift 2
      ;;
    --range-reads)
      RANGE_READS="${2:?missing value}"
      shift 2
      ;;
    --prefetch)
      PREFETCH_CHUNKS="${2:?missing value}"
      shift 2
      ;;
    --peer-mbps)
      PEER_MBPS="${2:?missing value}"
      shift 2
      ;;
    --size-mb)
      SIZE_MB="${2:?missing value}"
      shift 2
      ;;
    --settle-sec)
      HYBRID_SETTLE_SEC="${2:?missing value}"
      shift 2
      ;;
    --test-timeout-sec)
      TEST_TIMEOUT_SEC="${2:?missing value}"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      usage
      exit 1
      ;;
  esac
done

for bin in "$KUBECTL_BIN" "$HELM_BIN" "$AZ_BIN" git awk sed tar mktemp curl; do
  if ! command -v "$bin" >/dev/null 2>&1 && [[ ! -x "$bin" ]]; then
    echo "Missing required binary: $bin" >&2
    exit 1
  fi
done

export PATH="/opt/homebrew/bin:/usr/local/bin:$PATH"

"$KUBECTL_BIN" config use-context "$CONTEXT" >/dev/null

if [[ -z "$COMMIT_LIST" ]]; then
  COMMIT_LIST="$(git -C "$REPO_ROOT" log --reverse --no-merges --format='%h' "$COMMIT_RANGE")"
fi

if [[ -z "$COMMIT_LIST" ]]; then
  echo "No commits selected" >&2
  exit 1
fi

echo "commit,date,tag,writer,reader,write_mbps,read_mbps,status" > "$OUT_CSV"
: > "$OUT_LOG"

run_with_timeout() {
  local timeout_sec="$1"
  local outfile="$2"
  shift 2
  local timeout_marker
  timeout_marker="$(mktemp -t azure-commit-timeout)"
  : > "$outfile"
  "$@" >"$outfile" 2>&1 &
  local cmd_pid=$!
  (
    sleep "$timeout_sec"
    echo timeout >"$timeout_marker"
    pkill -TERM -P "$cmd_pid" 2>/dev/null || true
    kill -TERM "$cmd_pid" 2>/dev/null || true
    sleep 5
    pkill -KILL -P "$cmd_pid" 2>/dev/null || true
    kill -KILL "$cmd_pid" 2>/dev/null || true
  ) &
  local watchdog_pid=$!
  local rc=0
  if ! wait "$cmd_pid"; then
    rc=$?
  fi
  kill -TERM "$watchdog_pid" 2>/dev/null || true
  wait "$watchdog_pid" 2>/dev/null || true
  if [[ -s "$timeout_marker" ]]; then
    rm -f "$timeout_marker"
    return 124
  fi
  rm -f "$timeout_marker"
  return "$rc"
}

cleanup_hosts() {
  local pod
  for pod in $("$KUBECTL_BIN" -n "$NAMESPACE" get pods -l app=client -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}'); do
    "$KUBECTL_BIN" -n "$NAMESPACE" exec "$pod" -c client -- sh -lc '
      set -e
      rm -f /host/mnt/fuse/smart-read-* /host/mnt/fuse/gofuse-suite-* 2>/dev/null || true
      rm -f /mnt/nvme/cache/* 2>/dev/null || true
      sync
      echo 3 > /proc/sys/vm/drop_caches 2>/dev/null || true
    ' >/dev/null
  done
}

select_a100_pair() {
  local pods writer reader
  pods="$("$KUBECTL_BIN" -n "$NAMESPACE" get pods -l app=client -o wide --no-headers)"
  writer="$(echo "$pods" | awk '$7 ~ /a100test/ {print $1}' | sed -n '1p')"
  reader="$(echo "$pods" | awk '$7 ~ /a100test/ {print $1}' | sed -n '2p')"
  if [[ -z "$writer" || -z "$reader" ]]; then
    return 1
  fi
  printf '%s %s\n' "$writer" "$reader"
}

build_and_deploy_commit() {
  local commit="$1"
  local tmpdir tag
  tmpdir="$(mktemp -d /tmp/fuse-commit-${commit}-XXXXXX)"
  git -C "$REPO_ROOT" archive "$commit" | tar -x -C "$tmpdir"
  tag="commit-${commit}-${TIMESTAMP}"
  cat > "$tmpdir/Dockerfile.acr" <<'EOF'
FROM golang:1.20 AS builder
WORKDIR /app
COPY . .
RUN go mod download
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -ldflags="-s -w" -o /bin/coordinator cmd/coordinator/main.go && \
    CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -ldflags="-s -w" -o /bin/client cmd/client/main.go
FROM ubuntu:22.04
RUN apt-get update && apt-get install -y --no-install-recommends fuse3 ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=builder /bin/coordinator /usr/local/bin/coordinator
COPY --from=builder /bin/client /usr/local/bin/client
RUN echo "user_allow_other" >> /etc/fuse.conf
ENTRYPOINT []
CMD ["coordinator"]
EOF
  "$AZ_BIN" acr build --registry "$ACR_NAME" --image "fuse-client:${tag}" --file "$tmpdir/Dockerfile.acr" "$tmpdir" >> "$OUT_LOG" 2>&1 || {
    rm -rf "$tmpdir"
    return 1
  }
  "$HELM_BIN" upgrade "$RELEASE" "$REPO_ROOT/charts/fuse-cache" -n "$NAMESPACE" \
    --reuse-values \
    --set "image.tag=${tag}" \
    --set config.fuseBackend=gofuse \
    --set config.goFuseEnablePassthrough=true \
    --set "config.nvmeMaxGB=${NVME_MAX_GB}" \
    --set "config.parallelRangeReads=${RANGE_READS}" \
    --set "config.rangePrefetchChunks=${PREFETCH_CHUNKS}" \
    --set "config.rangeChunkCacheSize=${RANGE_CACHE}" \
    --set "config.peerReadMBps=${PEER_MBPS}" \
    --set "config.azureDownloadConcurrency=${AZ_DL_CONCURRENCY}" \
    --set "config.azureDownloadBlockSizeMB=${AZ_DL_BLOCK_MB}" \
    --set "config.azureParallelDownloadMinSizeMB=${AZ_DL_MIN_MB}" \
    --set "config.hybridAlwaysMinSizeMB=${HYBRID_ALWAYS_MB}" \
    --set "config.hybridHedgeDelayMs=${HYBRID_HEDGE_MS}" \
    --set "config.hybridMaxSecondaryInflight=${HYBRID_MAX_SECONDARY}" >> "$OUT_LOG" 2>&1 || {
    rm -rf "$tmpdir"
    return 1
  }
  "$KUBECTL_BIN" -n "$NAMESPACE" rollout status ds/client --timeout=600s >> "$OUT_LOG" 2>&1 || {
    rm -rf "$tmpdir"
    return 1
  }
  "$KUBECTL_BIN" -n "$NAMESPACE" rollout status deploy/coordinator --timeout=600s >> "$OUT_LOG" 2>&1 || {
    rm -rf "$tmpdir"
    return 1
  }
  rm -rf "$tmpdir"
  printf '%s\n' "$tag"
}

benchmark_commit() {
  local commit="$1"
  local date tag writer reader output rc write_mbps read_mbps tmp_output
  date="$(git -C "$REPO_ROOT" show -s --format=%cs "$commit")"
  echo "=== Benchmarking $commit ($date) ===" | tee -a "$OUT_LOG"
  if ! tag="$(build_and_deploy_commit "$commit")"; then
    echo "$commit,$date,,,-,0,0,build_fail" >> "$OUT_CSV"
    return
  fi
  cleanup_hosts
  if ! read -r writer reader < <(select_a100_pair); then
    echo "$commit,$date,$tag,,,-,0,0,no_a100_pair" >> "$OUT_CSV"
    return
  fi
  tmp_output="$(mktemp -t "azure-commit-test-${commit}")"
  set +e
  run_with_timeout "$TEST_TIMEOUT_SEC" "$tmp_output" env HYBRID_SETTLE_SEC="$HYBRID_SETTLE_SEC" \
    "$REPO_ROOT/scripts/ops/test-smart-read.sh" "$NAMESPACE" "$SIZE_MB" "$writer" "$reader"
  rc=$?
  set -e
  output="$(cat "$tmp_output")"
  rm -f "$tmp_output"
  printf '%s\n' "$output" >> "$OUT_LOG"
  write_mbps="$(printf '%s\n' "$output" | awk -F= '/^WRITE_MBPS_APPROX=/{print $2; exit}')"
  read_mbps="$(printf '%s\n' "$output" | awk -F= '/^READ_MBPS_APPROX=/{print $2; exit}')"
  if [[ $rc -ne 0 || -z "${write_mbps:-}" || -z "${read_mbps:-}" ]]; then
    if [[ $rc -eq 124 || $rc -eq 143 || $rc -eq 137 ]]; then
      echo "$commit,$date,$tag,$writer,$reader,0,0,test_timeout" >> "$OUT_CSV"
    else
      echo "$commit,$date,$tag,$writer,$reader,0,0,test_fail" >> "$OUT_CSV"
    fi
    return
  fi
  echo "$commit,$date,$tag,$writer,$reader,$write_mbps,$read_mbps,ok" >> "$OUT_CSV"
}

for commit in $COMMIT_LIST; do
  benchmark_commit "$commit"
done

echo "CSV: $OUT_CSV"
echo "LOG: $OUT_LOG"
echo "Top by read throughput:"
awk -F, 'NR>1 && $8=="ok" {print $0}' "$OUT_CSV" | sort -t, -k7,7nr | head -n 10

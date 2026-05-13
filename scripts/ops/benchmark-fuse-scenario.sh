#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 2 || $# -gt 4 ]]; then
  echo "Usage: $0 <namespace> <size-mb> [writer-class-substr] [reader-class-substr]"
  echo "Example: $0 fuse-system-aztest 5120 Standard_L64s_v3 Standard_NC24ads_A100_v4"
  exit 1
fi

NAMESPACE="$1"
SIZE_MB="$2"
WRITER_CLASS_FILTER="${3:-}"
READER_CLASS_FILTER="${4:-}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
KUBECTL_BIN="${KUBECTL_BIN:-kubectl}"
KUBECTL="${KUBECTL:-$KUBECTL_BIN --request-timeout=0}"
OUTPUT_CSV="${OUTPUT_CSV:-/tmp/fuse-benchmark-results.csv}"
RUN_TS="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
COMMIT="$(git rev-parse --short HEAD)"
RUN_LOG="$(mktemp /tmp/fuse-bench-run.XXXXXX.log)"
WRITER_METRICS_BEFORE="$(mktemp /tmp/fuse-bench-writer-before.XXXXXX.prom)"
WRITER_METRICS_AFTER="$(mktemp /tmp/fuse-bench-writer-after.XXXXXX.prom)"
READER_METRICS_BEFORE="$(mktemp /tmp/fuse-bench-reader-before.XXXXXX.prom)"
READER_METRICS_AFTER="$(mktemp /tmp/fuse-bench-reader-after.XXXXXX.prom)"
trap 'rm -f "$RUN_LOG" "$WRITER_METRICS_BEFORE" "$WRITER_METRICS_AFTER" "$READER_METRICS_BEFORE" "$READER_METRICS_AFTER"' EXIT

if ! [[ "$SIZE_MB" =~ ^[0-9]+$ ]] || [[ "$SIZE_MB" -le 0 ]]; then
  echo "size-mb must be a positive integer"
  exit 1
fi

metric_value() {
  local file="$1"
  local name="$2"
  awk -v metric="$name" '$1==metric {print $2}' "$file" | tail -n1
}

metric_delta() {
  local before_file="$1"
  local after_file="$2"
  local name="$3"
  local before after
  before="$(metric_value "$before_file" "$name")"
  after="$(metric_value "$after_file" "$name")"
  before="${before:-0}"
  after="${after:-0}"
  awk -v a="$after" -v b="$before" 'BEGIN { printf "%.0f", (a+0)-(b+0) }'
}

metric_delta_float() {
  local before_file="$1"
  local after_file="$2"
  local name="$3"
  local before after
  before="$(metric_value "$before_file" "$name")"
  after="$(metric_value "$after_file" "$name")"
  before="${before:-0}"
  after="${after:-0}"
  awk -v a="$after" -v b="$before" 'BEGIN { printf "%.6f", (a+0)-(b+0) }'
}

safe_div_mibps() {
  local bytes="$1"
  local seconds="$2"
  awk -v b="$bytes" -v s="$seconds" 'BEGIN { if ((s+0) <= 0) { print 0; } else { printf "%.1f", (b/1024/1024)/s } }'
}

safe_pct() {
  local part="$1"
  local total="$2"
  awk -v p="$part" -v t="$total" 'BEGIN { if ((t+0) <= 0) { print 0; } else { printf "%.1f", 100*p/t } }'
}

csv_escape() {
  local value="$1"
  value=${value//\"/\"\"}
  printf '"%s"' "$value"
}

pod_node() {
  local pod="$1"
  $KUBECTL -n "$NAMESPACE" get pod "$pod" -o jsonpath='{.spec.nodeName}'
}

node_instance_type() {
  local node="$1"
  local value
  value="$($KUBECTL get node "$node" -o jsonpath='{.metadata.labels.node\.kubernetes\.io/instance-type}')"
  if [[ -z "$value" ]]; then
    value="$($KUBECTL get node "$node" -o jsonpath='{.metadata.labels.beta\.kubernetes\.io/instance-type}')"
  fi
  printf '%s' "$value"
}

pod_image() {
  local pod="$1"
  $KUBECTL -n "$NAMESPACE" get pod "$pod" -o jsonpath='{.spec.containers[?(@.name=="client")].image}'
}

pick_pod() {
  local filter="$1"
  local exclude_pod="${2:-}"
  local pod node type
  local pod_list
  pod_list="$($KUBECTL -n "$NAMESPACE" get pods -l app=client -o jsonpath='{range .items[?(@.status.phase=="Running")]}{.metadata.name}{"\n"}{end}')"
  while IFS= read -r pod; do
    [[ -z "$pod" ]] && continue
    [[ -n "$exclude_pod" && "$pod" == "$exclude_pod" ]] && continue
    node="$(pod_node "$pod")"
    type="$(node_instance_type "$node")"
    if [[ -z "$filter" || "$type" == *"$filter"* || "$node" == *"$filter"* ]]; then
      echo "$pod"
      return 0
    fi
  done <<EOF
$pod_list
EOF
  return 1
}

scrape_metrics() {
  local pod="$1"
  local out="$2"
  local proxy_path="/api/v1/namespaces/${NAMESPACE}/pods/${pod}:8081/proxy/metrics"
  $KUBECTL get --raw "$proxy_path" > "$out"
}

peer_network_json() {
  local pod="$1"
  local proxy_path="/api/v1/namespaces/${NAMESPACE}/pods/${pod}:8081/proxy/api/peers"
  $KUBECTL get --raw "$proxy_path"
}

best_effort_top() {
  $KUBECTL top "$@" --no-headers 2>/dev/null || true
}

WRITER_POD="$(pick_pod "$WRITER_CLASS_FILTER")"
READER_POD="$(pick_pod "$READER_CLASS_FILTER" "$WRITER_POD" || true)"
if [[ -z "$READER_POD" ]]; then
  READER_POD="$WRITER_POD"
fi

WRITER_NODE="$(pod_node "$WRITER_POD")"
READER_NODE="$(pod_node "$READER_POD")"
WRITER_CLASS="$(node_instance_type "$WRITER_NODE")"
READER_CLASS="$(node_instance_type "$READER_NODE")"
WRITER_IMAGE="$(pod_image "$WRITER_POD")"
READER_IMAGE="$(pod_image "$READER_POD")"

scrape_metrics "$WRITER_POD" "$WRITER_METRICS_BEFORE"
scrape_metrics "$READER_POD" "$READER_METRICS_BEFORE"

"$SCRIPT_DIR/test-smart-read.sh" "$NAMESPACE" "$SIZE_MB" "$WRITER_POD" "$READER_POD" | tee "$RUN_LOG"

scrape_metrics "$WRITER_POD" "$WRITER_METRICS_AFTER"
scrape_metrics "$READER_POD" "$READER_METRICS_AFTER"

WRITE_MBPS="$(awk -F= '/WRITE_MBPS_APPROX=/{print $2}' "$RUN_LOG" | tail -n1)"
READ_MBPS="$(awk -F= '/READ_MBPS_APPROX=/{print $2}' "$RUN_LOG" | tail -n1)"
WRITE_MS="$(awk -F= '/WRITE_MS=/{print $2}' "$RUN_LOG" | tail -n1)"
READ_MS="$(awk -F= '/READ_MS=/{print $2}' "$RUN_LOG" | tail -n1)"

NVME_WALL_BYTES="$(metric_delta "$READER_METRICS_BEFORE" "$READER_METRICS_AFTER" fuse_cache_nvme_read_wall_bytes_total)"
PEER_WALL_BYTES="$(metric_delta "$READER_METRICS_BEFORE" "$READER_METRICS_AFTER" fuse_cache_peer_read_wall_bytes_total)"
CLOUD_WALL_BYTES="$(metric_delta "$READER_METRICS_BEFORE" "$READER_METRICS_AFTER" fuse_cache_cloud_read_wall_bytes_total)"
TOTAL_WALL_BYTES="$(metric_delta "$READER_METRICS_BEFORE" "$READER_METRICS_AFTER" fuse_cache_read_wall_bytes_total)"
PEER_READ_BYTES="$(metric_delta "$READER_METRICS_BEFORE" "$READER_METRICS_AFTER" fuse_cache_peer_read_bytes_total)"
PEER_READ_SECONDS="$(metric_delta_float "$READER_METRICS_BEFORE" "$READER_METRICS_AFTER" fuse_cache_peer_read_seconds_total)"
CLOUD_READ_BYTES="$(metric_delta "$READER_METRICS_BEFORE" "$READER_METRICS_AFTER" fuse_cache_cloud_read_bytes_total)"
CLOUD_READ_SECONDS="$(metric_delta_float "$READER_METRICS_BEFORE" "$READER_METRICS_AFTER" fuse_cache_cloud_read_seconds_total)"

PEER_SHARE_PCT="$(safe_pct "$PEER_WALL_BYTES" "$TOTAL_WALL_BYTES")"
CLOUD_SHARE_PCT="$(safe_pct "$CLOUD_WALL_BYTES" "$TOTAL_WALL_BYTES")"
NVME_SHARE_PCT="$(safe_pct "$NVME_WALL_BYTES" "$TOTAL_WALL_BYTES")"
PEER_OBJECT_MBPS="$(safe_div_mibps "$PEER_READ_BYTES" "$PEER_READ_SECONDS")"
CLOUD_OBJECT_MBPS="$(safe_div_mibps "$CLOUD_READ_BYTES" "$CLOUD_READ_SECONDS")"

WRITE_PHASE_SEC="$(metric_delta_float "$WRITER_METRICS_BEFORE" "$WRITER_METRICS_AFTER" fuse_gofuse_write_phase_seconds_total)"
SYNC_SEC="$(metric_delta_float "$WRITER_METRICS_BEFORE" "$WRITER_METRICS_AFTER" fuse_gofuse_sync_seconds_total)"
FLUSH_SEC="$(metric_delta_float "$WRITER_METRICS_BEFORE" "$WRITER_METRICS_AFTER" fuse_gofuse_flush_seconds_total)"
STAGE_WRITE_CALLS="$(metric_delta "$WRITER_METRICS_BEFORE" "$WRITER_METRICS_AFTER" fuse_gofuse_stage_write_calls_total)"
BUFFER_FLUSH_CALLS="$(metric_delta "$WRITER_METRICS_BEFORE" "$WRITER_METRICS_AFTER" fuse_gofuse_buffer_flush_calls_total)"

RANGE_CACHE_BYTES_BEFORE="$(metric_value "$READER_METRICS_BEFORE" fuse_range_cache_bytes)"
RANGE_CACHE_BYTES_AFTER="$(metric_value "$READER_METRICS_AFTER" fuse_range_cache_bytes)"
PREFETCH_BYTES_AFTER="$(metric_value "$READER_METRICS_AFTER" fuse_range_prefetch_bytes)"
HEAP_INUSE_AFTER="$(metric_value "$READER_METRICS_AFTER" fuse_runtime_heap_inuse_bytes)"
GOROUTINES_AFTER="$(metric_value "$READER_METRICS_AFTER" fuse_runtime_goroutines)"

WRITER_NODE_TOP="$(best_effort_top node "$WRITER_NODE" | tr -s ' ' | tr ' ' ';')"
READER_NODE_TOP="$(best_effort_top node "$READER_NODE" | tr -s ' ' | tr ' ' ';')"
WRITER_POD_TOP="$(best_effort_top pod -n "$NAMESPACE" "$WRITER_POD" | tr -s ' ' | tr ' ' ';')"
READER_POD_TOP="$(best_effort_top pod -n "$NAMESPACE" "$READER_POD" | tr -s ' ' | tr ' ' ';')"

PEERS_JSON="$(peer_network_json "$READER_POD" || echo '[]')"
WRITER_NET_MBPS="$(printf '%s' "$PEERS_JSON" | jq -r --arg id "$WRITER_POD" 'map(select(.id==$id)) | .[0].network_speed_mbps // 0')"
WRITER_NET_LATENCY_MS="$(printf '%s' "$PEERS_JSON" | jq -r --arg id "$WRITER_POD" 'map(select(.id==$id)) | .[0].network_latency_ms // 0')"
READER_NET_MBPS="$(printf '%s' "$PEERS_JSON" | jq -r --arg id "$READER_POD" 'map(select(.id==$id)) | .[0].network_speed_mbps // 0')"
READER_NET_LATENCY_MS="$(printf '%s' "$PEERS_JSON" | jq -r --arg id "$READER_POD" 'map(select(.id==$id)) | .[0].network_latency_ms // 0')"

if [[ ! -f "$OUTPUT_CSV" ]]; then
  cat > "$OUTPUT_CSV" <<'CSV'
run_ts,commit,namespace,size_mb,writer_pod,writer_node,writer_class,writer_image,reader_pod,reader_node,reader_class,reader_image,write_mbps,read_mbps,write_ms,read_ms,nvme_wall_bytes,peer_wall_bytes,cloud_wall_bytes,total_wall_bytes,nvme_share_pct,peer_share_pct,cloud_share_pct,peer_object_mbps,cloud_object_mbps,write_phase_sec,sync_sec,flush_sec,stage_write_calls,buffer_flush_calls,range_cache_bytes_before,range_cache_bytes_after,prefetch_bytes_after,heap_inuse_after,goroutines_after,writer_network_mbps,writer_network_latency_ms,reader_network_mbps,reader_network_latency_ms,writer_node_top,reader_node_top,writer_pod_top,reader_pod_top
CSV
fi

row=(
  "$RUN_TS" "$COMMIT" "$NAMESPACE" "$SIZE_MB"
  "$WRITER_POD" "$WRITER_NODE" "$WRITER_CLASS" "$WRITER_IMAGE"
  "$READER_POD" "$READER_NODE" "$READER_CLASS" "$READER_IMAGE"
  "${WRITE_MBPS:-0}" "${READ_MBPS:-0}" "${WRITE_MS:-0}" "${READ_MS:-0}"
  "$NVME_WALL_BYTES" "$PEER_WALL_BYTES" "$CLOUD_WALL_BYTES" "$TOTAL_WALL_BYTES"
  "$NVME_SHARE_PCT" "$PEER_SHARE_PCT" "$CLOUD_SHARE_PCT"
  "$PEER_OBJECT_MBPS" "$CLOUD_OBJECT_MBPS"
  "$WRITE_PHASE_SEC" "$SYNC_SEC" "$FLUSH_SEC" "$STAGE_WRITE_CALLS" "$BUFFER_FLUSH_CALLS"
  "${RANGE_CACHE_BYTES_BEFORE:-0}" "${RANGE_CACHE_BYTES_AFTER:-0}" "${PREFETCH_BYTES_AFTER:-0}" "${HEAP_INUSE_AFTER:-0}" "${GOROUTINES_AFTER:-0}"
  "$WRITER_NET_MBPS" "$WRITER_NET_LATENCY_MS" "$READER_NET_MBPS" "$READER_NET_LATENCY_MS"
  "$WRITER_NODE_TOP" "$READER_NODE_TOP" "$WRITER_POD_TOP" "$READER_POD_TOP"
)

for i in "${!row[@]}"; do
  csv_escape "${row[$i]}"
  if (( i < ${#row[@]} - 1 )); then
    printf ','
  else
    printf '\n'
  fi
done >> "$OUTPUT_CSV"

echo "Benchmark saved to $OUTPUT_CSV"
echo "Writer: $WRITER_POD ($WRITER_CLASS)"
echo "Reader: $READER_POD ($READER_CLASS)"
echo "Write MB/s: ${WRITE_MBPS:-0}"
echo "Read MB/s: ${READ_MBPS:-0}"
echo "Source share: nvme=${NVME_SHARE_PCT}% peer=${PEER_SHARE_PCT}% cloud=${CLOUD_SHARE_PCT}%"
echo "Object speeds: peer=${PEER_OBJECT_MBPS} MiB/s cloud=${CLOUD_OBJECT_MBPS} MiB/s"

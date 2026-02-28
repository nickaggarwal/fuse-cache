#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 3 || $# -gt 5 ]]; then
  echo "Usage: $0 <namespace> <profile:standard|s3express> <size-mb> [writer-pod] [reader-pod]"
  echo "Example (standard):  $0 fuse-system-awstest standard 1024"
  echo "Example (s3express): $0 fuse-system-awstest s3express 5120"
  exit 1
fi

NAMESPACE="$1"
PROFILE="$(printf '%s' "$2" | tr '[:upper:]' '[:lower:]')"
SIZE_MB="$3"
WRITER_POD="${4:-}"
READER_POD="${5:-}"
KUBECTL="kubectl --request-timeout=0"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

if ! [[ "$SIZE_MB" =~ ^[0-9]+$ ]] || [[ "$SIZE_MB" -le 0 ]]; then
  echo "size-mb must be a positive integer, got: $SIZE_MB"
  exit 1
fi

case "$PROFILE" in
  standard|s3express)
    ;;
  *)
    echo "profile must be one of: standard, s3express (got: $PROFILE)"
    exit 1
    ;;
esac

get_running_client_pods() {
  $KUBECTL -n "$NAMESPACE" get pods -l app=client \
    -o jsonpath='{range .items[?(@.status.phase=="Running")]}{.metadata.name}{"\n"}{end}'
}

get_pod_node() {
  local pod="$1"
  $KUBECTL -n "$NAMESPACE" get pod "$pod" -o jsonpath='{.spec.nodeName}'
}

get_node_zone() {
  local node="$1"
  kubectl get node "$node" -o jsonpath='{.metadata.labels.topology\.kubernetes\.io/zone}'
}

map_zone_name_to_id() {
  local region="$1"
  local zone_name="$2"
  local zone_id=""
  if ! command -v aws >/dev/null 2>&1; then
    printf '\n'
    return 0
  fi
  zone_id="$(aws ec2 describe-availability-zones \
    --region "$region" \
    --zone-names "$zone_name" \
    --query 'AvailabilityZones[0].ZoneId' \
    --output text 2>/dev/null || true)"
  if [[ "$zone_id" == "None" || "$zone_id" == "null" ]]; then
    zone_id=""
  fi
  printf '%s\n' "$zone_id"
}

extract_s3express_zone_id() {
  local endpoint="$1"
  if [[ "$endpoint" =~ s3express-([a-z0-9-]+)\. ]]; then
    printf '%s\n' "${BASH_REMATCH[1]}"
    return 0
  fi
  if [[ "$endpoint" =~ \.([a-z0-9]+-az[0-9]+)\. ]]; then
    printf '%s\n' "${BASH_REMATCH[1]}"
    return 0
  fi
  printf '\n'
}

pick_reader_for_zone_id() {
  local target_zone_id="$1"
  local region="$2"
  local exclude_pod="$3"
  local pod=""
  local node=""
  local zone_name=""
  local zone_id=""

  while IFS= read -r pod; do
    [[ -z "$pod" ]] && continue
    if [[ -n "$exclude_pod" && "$pod" == "$exclude_pod" ]]; then
      continue
    fi
    node="$(get_pod_node "$pod")"
    zone_name="$(get_node_zone "$node")"
    zone_id="$(map_zone_name_to_id "$region" "$zone_name")"
    if [[ "$zone_id" == "$target_zone_id" ]]; then
      printf '%s\n' "$pod"
      return 0
    fi
  done < <(get_running_client_pods)

  return 1
}

if [[ -z "$WRITER_POD" ]]; then
  WRITER_POD="$(get_running_client_pods | head -n1)"
fi
if [[ -z "$READER_POD" ]]; then
  READER_POD="$(get_running_client_pods | head -n2 | tail -n1)"
fi
if [[ -z "$READER_POD" ]]; then
  READER_POD="$WRITER_POD"
fi

if [[ -z "$WRITER_POD" ]]; then
  echo "No running client pod found in namespace $NAMESPACE"
  exit 1
fi

writer_node="$(get_pod_node "$WRITER_POD")"
reader_node="$(get_pod_node "$READER_POD")"
writer_zone="$(get_node_zone "$writer_node")"
reader_zone="$(get_node_zone "$reader_node")"

echo "Namespace: $NAMESPACE"
echo "Profile: $PROFILE"
echo "Writer pod/node/zone: $WRITER_POD / $writer_node / ${writer_zone:-unknown}"
echo "Reader pod/node/zone: $READER_POD / $reader_node / ${reader_zone:-unknown}"

if [[ "$PROFILE" == "s3express" ]]; then
  s3_endpoint="$($KUBECTL -n "$NAMESPACE" exec "$READER_POD" -c client -- sh -lc 'printf %s "${S3_ENDPOINT:-}"')"
  s3_region="$($KUBECTL -n "$NAMESPACE" exec "$READER_POD" -c client -- sh -lc 'printf %s "${S3_REGION:-}"')"
  if [[ -z "$s3_region" ]]; then
    s3_region="us-east-1"
  fi
  endpoint_zone_id="$(extract_s3express_zone_id "$s3_endpoint")"
  reader_zone_id="$(map_zone_name_to_id "$s3_region" "$reader_zone")"

  echo "S3 endpoint: ${s3_endpoint:-<empty>}"
  echo "S3 region: $s3_region"
  echo "Endpoint zone-id: ${endpoint_zone_id:-unknown}"
  echo "Reader zone-id: ${reader_zone_id:-unknown}"

  if [[ -n "$endpoint_zone_id" && -n "$reader_zone_id" && "$endpoint_zone_id" != "$reader_zone_id" ]]; then
    echo "Reader zone does not match s3express endpoint zone-id; searching for zone-matched reader pod..."
    if matched_reader="$(pick_reader_for_zone_id "$endpoint_zone_id" "$s3_region" "$WRITER_POD")"; then
      READER_POD="$matched_reader"
      reader_node="$(get_pod_node "$READER_POD")"
      reader_zone="$(get_node_zone "$reader_node")"
      reader_zone_id="$(map_zone_name_to_id "$s3_region" "$reader_zone")"
      echo "Using zone-matched reader: $READER_POD / $reader_node / ${reader_zone:-unknown} (zone-id: ${reader_zone_id:-unknown})"
    else
      echo "WARNING: no zone-matched reader pod found; benchmark may include cross-zone latency."
    fi
  fi
fi

timestamp="$(date +%Y%m%d-%H%M%S)"
log_file="/tmp/eks-bench-${SIZE_MB}mb-${PROFILE}-${timestamp}.log"

"${SCRIPT_DIR}/test-smart-read.sh" "$NAMESPACE" "$SIZE_MB" "$WRITER_POD" "$READER_POD" | tee "$log_file"

write_mbps="$(awk -F= '/^WRITE_MBPS_APPROX=/{v=$2} END{if (v=="") v="NA"; print v}' "$log_file")"
read_mbps="$(awk -F= '/^READ_MBPS_APPROX=/{v=$2} END{if (v=="") v="NA"; print v}' "$log_file")"

echo "PROFILE_RESULT profile=${PROFILE} size_mb=${SIZE_MB} write_mbps=${write_mbps} read_mbps=${read_mbps} log=${log_file}"

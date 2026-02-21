#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 1 || $# -gt 3 ]]; then
  echo "Usage: $0 <namespace> [writer-pod] [reader-pod]"
  echo "Example: $0 fuse-system-aztest"
  exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
NAMESPACE="$1"
WRITER_POD="${2:-}"
READER_POD="${3:-}"

exec "${SCRIPT_DIR}/test-smart-read.sh" "$NAMESPACE" "5120" "$WRITER_POD" "$READER_POD"

#!/usr/bin/env bash
# Chart conformance for the cross-cluster path.
#
# What this checks that `helm lint` and `helm template` cannot: that the
# flags and environment variables the charts emit are the ones the BINARIES
# actually read. A chart can render perfectly valid YAML naming a flag that
# does not exist, or an env var the binary never looks at; the pod then
# CrashLoopBackOffs, or worse, starts and quietly does nothing. Both are
# invisible to a template test.
#
# So every assertion here extracts the real rendered args/env out of
# `helm template` output and feeds them to the real binary.
#
#   scripts/ops/e2e-global-chart.sh
#
# Exit code is the number of failed checks.

set -u

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"

WORK="$(mktemp -d)"
FAILURES=0
PIDS=()

cleanup() {
  for pid in "${PIDS[@]:-}"; do kill "$pid" 2>/dev/null || true; done
  wait 2>/dev/null || true
  rm -rf "$WORK"
}
trap cleanup EXIT

pass() { echo "  PASS: $1"; }
fail() { echo "  FAIL: $1"; FAILURES=$((FAILURES + 1)); }

need() {
  command -v "$1" >/dev/null 2>&1 || { echo "missing required tool: $1"; exit 127; }
}
need helm
need python3

# Only the binaries this script drives. `make build` includes cmd/client,
# which is Linux-only (bazil/go-fuse), so it cannot be used here.
echo "Building binaries..."
go build -o bin/globalcp ./cmd/globalcp \
  && go build -o bin/coordinator ./cmd/coordinator \
  && go build -o bin/globalcp-certs ./cmd/globalcp-certs \
  || { echo "build failed"; exit 1; }

# render <chart> <out> [helm --set args...]
render() {
  local chart="$1" out="$2"; shift 2
  helm template t "charts/$chart" "$@" > "$out" 2>"$out.err" || {
    echo "    helm template failed: $(head -1 "$out.err")"
    return 1
  }
}

# args_of <rendered> <container> -- prints the container's args, one per line,
# as the kubelet would pass them.
args_of() {
  python3 - "$1" "$2" <<'PY'
import sys, re
doc = open(sys.argv[1]).read()
name = sys.argv[2]
# Deliberately a line scanner, not a YAML parse: this file must run on a
# machine with nothing but python3, and the shape it reads is fixed by the
# templates in this repo.
lines = doc.splitlines()
out, in_c, in_args = [], False, False
for ln in lines:
    s = ln.strip()
    if s == f"- name: {name}":
        in_c = True; continue
    if in_c and s == "args:":
        in_args = True; continue
    if in_args:
        m = re.match(r'^- "?(.*?)"?$', s)
        if m and s.startswith("- "):
            out.append(m.group(1)); continue
        if s.startswith("#"):
            continue
        if s and not s.startswith("- "):
            break
    if in_c and s.startswith("- name: ") and not in_args:
        in_c = False
print("\n".join(out))
PY
}

# env_of <rendered> -- prints the fuse-config ConfigMap as KEY=VALUE lines.
env_of() {
  python3 - "$1" <<'PY'
import sys, re
doc = open(sys.argv[1]).read()
block = doc.split("kind: ConfigMap", 1)[1].split("\n---", 1)[0]
data = block.split("\ndata:", 1)[1]
for ln in data.splitlines():
    m = re.match(r'^  ([A-Z0-9_]+): "(.*)"$', ln)
    if m:
        print(f"{m.group(1)}={m.group(2)}")
PY
}

wait_for() {
  local file="$1" pattern="$2" tries="${3:-60}"
  for _ in $(seq 1 "$tries"); do
    grep -q "$pattern" "$file" 2>/dev/null && return 0
    sleep 0.2
  done
  return 1
}

free_port() { python3 -c 'import socket;s=socket.socket();s.bind(("127.0.0.1",0));print(s.getsockname()[1]);s.close()'; }

echo
echo "=== 1. The globalcp chart emits flags the globalcp binary accepts ==="

CERTDIR="$WORK/tls"
mkdir -p "$CERTDIR"
./bin/globalcp-certs -dir "$CERTDIR" -names globalcp,cluster-a >/dev/null 2>&1 \
  || { echo "cert issue failed"; exit 1; }

render fuse-globalcp "$WORK/gcp.yaml" --set tls.secretName=globalcp-tls || fail "chart renders"
# No mapfile: macOS ships bash 3.2, where it does not exist. This script
# has to run on a developer laptop as well as in CI.
GARGS=()
while IFS= read -r line; do GARGS+=("$line"); done < <(args_of "$WORK/gcp.yaml" globalcp)

if [ "${#GARGS[@]}" -gt 0 ]; then
  pass "extracted ${#GARGS[@]} args from the rendered Deployment"
else
  fail "could not extract globalcp args from the rendered Deployment"
fi

# The chart names paths under the secret's mountPath. Swap that one prefix
# for the local cert dir and keep every flag spelling exactly as rendered --
# the flag NAMES are what is under test, not the paths.
REAL_ARGS=()
GPORT="$(free_port)"; GHTTP="$(free_port)"
for a in "${GARGS[@]}"; do
  case "$a" in
    /etc/fuse-globalcp/tls/tls.crt) REAL_ARGS+=("$CERTDIR/globalcp.crt") ;;
    /etc/fuse-globalcp/tls/tls.key) REAL_ARGS+=("$CERTDIR/globalcp.key") ;;
    /etc/fuse-globalcp/tls/ca.crt)  REAL_ARGS+=("$CERTDIR/ca.crt") ;;
    8090) REAL_ARGS+=("$GHTTP") ;;
    9090) REAL_ARGS+=("$GPORT") ;;
    *) REAL_ARGS+=("$a") ;;
  esac
done

./bin/globalcp "${REAL_ARGS[@]}" >"$WORK/gcp.log" 2>&1 &
PIDS+=($!)
if wait_for "$WORK/gcp.log" "mTLS enabled"; then
  pass "binary starts on the chart's args with mTLS on"
else
  fail "binary did not start with mTLS on the chart's args: $(tail -3 "$WORK/gcp.log" | tr '\n' ' ')"
fi
if grep -q "flag provided but not defined\|Usage of" "$WORK/gcp.log"; then
  fail "the chart emits a flag the binary does not define"
else
  pass "every flag the chart emits is defined by the binary"
fi

echo
echo "=== 2. The dev path renders -allow-insecure-identity, and it works ==="
render fuse-globalcp "$WORK/gcp-dev.yaml" --set allowInsecureIdentity=true || fail "dev chart renders"
if args_of "$WORK/gcp-dev.yaml" globalcp | grep -q -- "-allow-insecure-identity"; then
  pass "dev values render the insecure-identity flag"
else
  fail "dev values did not render -allow-insecure-identity; the pod would refuse to start"
fi
if args_of "$WORK/gcp-dev.yaml" globalcp | grep -q -- "-tls-"; then
  fail "dev values rendered TLS flags with no secret mounted"
else
  pass "dev values render no TLS flags"
fi

# And start it, because "the flag rendered" is not "the pod runs". The dev
# path is the one an operator reaches for first; a chart that renders it and
# then CrashLoopBackOffs is worse than no dev path.
DEV_ARGS=()
DHTTP="$(free_port)"; DGRPC="$(free_port)"
while IFS= read -r line; do
  case "$line" in
    8090) DEV_ARGS+=("$DHTTP") ;;
    9090) DEV_ARGS+=("$DGRPC") ;;
    *) DEV_ARGS+=("$line") ;;
  esac
done < <(args_of "$WORK/gcp-dev.yaml" globalcp)
./bin/globalcp "${DEV_ARGS[@]}" >"$WORK/gcp-dev.log" 2>&1 &
PIDS+=($!)
if wait_for "$WORK/gcp-dev.log" "insecure identity mode"; then
  pass "binary starts on the dev chart's args"
else
  fail "binary did not start on the dev chart's args: $(tail -3 "$WORK/gcp-dev.log" | tr '\n' ' ')"
fi

echo
echo "=== 3. The fuse-cache chart's env actually joins the federation ==="
render fuse-cache "$WORK/fc.yaml" \
  --set globalCP.enabled=true \
  --set globalCP.clusterId=cluster-a \
  --set globalCP.endpoint="127.0.0.1:$GPORT" \
  --set 'globalCP.publishPrefixes={/models/,/checkpoints/}' \
  --set 'globalCP.subscribePrefixes={/models/}' \
  --set globalCP.tls.secretName=cluster-a-globalcp \
  --set globalCP.tls.serverName=globalcp \
  --set cloud.keyPrefix=cluster-a \
  || fail "fuse-cache renders with global on"

env_of "$WORK/fc.yaml" > "$WORK/fc.env"
for key in FUSE_GLOBAL_ENABLED FUSE_GLOBAL_CLUSTER_ID FUSE_GLOBAL_ENDPOINT \
           FUSE_GLOBAL_PUBLISH_PREFIXES FUSE_GLOBAL_SUBSCRIBE_PREFIXES \
           FUSE_GLOBAL_TLS_CERT FUSE_GLOBAL_TLS_KEY FUSE_GLOBAL_TLS_CA \
           FUSE_CLOUD_KEY_PREFIX; do
  grep -q "^$key=" "$WORK/fc.env" || fail "ConfigMap is missing $key"
done
pass "ConfigMap carries every FUSE_GLOBAL_* key the coordinator reads"

# Feed the rendered env to the real coordinator, rewriting only the three
# in-pod cert paths. If a key were misspelled the coordinator would start
# WITHOUT joining -- which is why the assertion is on the control plane
# logging the registration, not on the coordinator starting.
CPORT="$(free_port)"; CGRPC="$(free_port)"
(
  set -a
  # shellcheck disable=SC1090
  while IFS='=' read -r k v; do export "$k=$v"; done < "$WORK/fc.env"
  export FUSE_GLOBAL_TLS_CERT="$CERTDIR/cluster-a.crt"
  export FUSE_GLOBAL_TLS_KEY="$CERTDIR/cluster-a.key"
  export FUSE_GLOBAL_TLS_CA="$CERTDIR/ca.crt"
  set +a
  exec ./bin/coordinator -port "$CPORT" -grpc-port "$CGRPC"
) >"$WORK/coord.log" 2>&1 &
PIDS+=($!)

if wait_for "$WORK/coord.log" 'connected to global control plane'; then
  pass "coordinator joined the federation from the chart's env alone"
else
  fail "coordinator did not join: $(tail -5 "$WORK/coord.log" | tr '\n' ' ')"
fi
if wait_for "$WORK/gcp.log" 'registered cluster=cluster-a'; then
  pass "control plane registered cluster-a (identity from the certificate)"
else
  fail "control plane never registered the cluster: $(tail -5 "$WORK/gcp.log" | tr '\n' ' ')"
fi
if grep -q 'mtls=true' "$WORK/coord.log"; then
  pass "coordinator reports mtls=true"
else
  fail "coordinator did not report mtls=true"
fi

echo
echo "=== 4. A default install is untouched by any of this ==="
render fuse-cache "$WORK/fc-default.yaml" || fail "default chart renders"
# Against the parsed ConfigMap keys, not a grep of the whole document: the
# templates mention FUSE_GLOBAL_READ in a comment, and a check that a
# comment can satisfy is not checking the ConfigMap.
env_of "$WORK/fc-default.yaml" > "$WORK/fc-default.env"
if grep -q '^FUSE_GLOBAL_' "$WORK/fc-default.env"; then
  fail "a default install emits FUSE_GLOBAL_* keys"
else
  pass "no FUSE_GLOBAL_* key in a default install"
fi
if grep -q -- '-global-read' "$WORK/fc-default.yaml"; then
  fail "a default install passes -global-read to the client"
else
  pass "no -global-read in a default install"
fi
if grep -q 'FUSE_CLOUD_KEY_PREFIX: ""' "$WORK/fc-default.yaml"; then
  pass "FUSE_CLOUD_KEY_PREFIX is empty (byte-identical legacy key layout)"
else
  fail "FUSE_CLOUD_KEY_PREFIX is not empty by default"
fi

echo
echo "=== 5. Misconfigurations fail at render, not at runtime ==="
# Each of these produces a pod that starts and then does the wrong thing
# silently, which is exactly the class a template guard is for.
guard() {
  local desc="$1"; shift
  if helm template t "$@" >/dev/null 2>&1; then
    fail "$desc rendered successfully; it should have been rejected"
  else
    pass "$desc rejected at render time"
  fi
}
guard "globalcp with neither TLS nor an explicit insecure opt-in" charts/fuse-globalcp
guard "globalcp with both TLS and insecure identity" charts/fuse-globalcp \
  --set tls.secretName=x --set allowInsecureIdentity=true
guard "globalcp with replicaCount 2 on the in-memory store" charts/fuse-globalcp \
  --set tls.secretName=x --set replicaCount=2
guard "fuse-cache global without a clusterId" charts/fuse-cache \
  --set globalCP.enabled=true
guard "fuse-cache global without an endpoint" charts/fuse-cache \
  --set globalCP.enabled=true --set globalCP.clusterId=a
guard "fuse-cache embedded alongside a TLS secret" charts/fuse-cache \
  --set globalCP.enabled=true --set globalCP.clusterId=a --set globalCP.embedded=true \
  --set globalCP.tls.secretName=s --set 'globalCP.publishPrefixes={/m/}'
guard "fuse-cache joining with no prefixes at all" charts/fuse-cache \
  --set globalCP.enabled=true --set globalCP.clusterId=a --set globalCP.endpoint=cp:9090
guard "fuse-cache global-read without membership" charts/fuse-cache \
  --set globalCP.read=true

echo
echo "=== 6. The image carries the binary the chart runs ==="
if grep -q 'bin/globalcp /usr/local/bin/globalcp' Dockerfile; then
  pass "Dockerfile installs globalcp (the chart's command)"
else
  fail "Dockerfile does not install globalcp; the Deployment would never start"
fi

echo
if [ "$FAILURES" -eq 0 ]; then
  echo "All chart checks passed."
else
  echo "$FAILURES check(s) failed."
fi
exit "$FAILURES"

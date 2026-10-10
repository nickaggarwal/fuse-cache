#!/usr/bin/env bash
#
# Stage 4 gate: prove the global control plane refuses every configuration
# that would let a cluster claim an identity it was not issued.
#
# The unit tests in internal/globalcp cover the authorization logic against
# an in-process TLS listener. This script covers the half they cannot: the
# BINARIES' startup decisions. A service that validates perfectly inside a
# test and then serves cleartext because a flag name was mistyped is the
# exact failure this stage exists to prevent, and only a real process
# invocation can catch it.
#
# No MinIO, no cluster: this is the control path, not the data path. The
# cross-cluster object read -- A writes, B reads A's bytes out of the shared
# bucket -- is internal/globalcpe2e, which runs the same real components
# against a real S3 protocol server in-process (see its doc.go for why that
# is a Go test rather than a third script here).
#
# Exit code = number of failed checks.

set -uo pipefail
cd "$(dirname "$0")/../.."

CERTS=$(mktemp -d)
LOGS=$(mktemp -d)
FAILURES=0
PIDS=()

cleanup() {
  for pid in "${PIDS[@]:-}"; do kill "$pid" 2>/dev/null; done
  wait 2>/dev/null
  rm -rf "$CERTS" "$LOGS"
}
trap cleanup EXIT

pass() { printf '  \033[32mPASS\033[0m %s\n' "$1"; }
fail() { printf '  \033[31mFAIL\033[0m %s\n' "$1"; FAILURES=$((FAILURES + 1)); }

# expect_refusal runs a binary that must exit non-zero, and must say why in
# a way that names the problem. A refusal with an unhelpful message is only
# half the property: an operator who cannot tell which flag is wrong will
# reach for the insecure mode.
expect_refusal() {
  local what=$1 pattern=$2; shift 2
  local out
  out=$("$@" 2>&1)
  if [[ -n "$out" && "$out" == *"$pattern"* ]]; then
    pass "$what"
  else
    fail "$what (expected a message containing '$pattern', got: ${out:-<nothing>})"
  fi
}

wait_for() {
  local file=$1 pattern=$2 secs=${3:-10}
  local deadline=$((SECONDS + secs))
  while ((SECONDS < deadline)); do
    grep -q "$pattern" "$file" 2>/dev/null && return 0
    sleep 0.2
  done
  return 1
}

# Only the three binaries this script drives. Deliberately not `make
# build`: that also builds cmd/client, which is Linux-only (bazil.org/fuse),
# so a full build would make this script undiagnosably fail on a laptop for
# a reason unrelated to what it tests.
echo "Building..."
make bin/globalcp bin/coordinator bin/globalcp-certs >/dev/null 2>&1 || {
  echo "build failed"; make bin/globalcp bin/coordinator bin/globalcp-certs; exit 1; }

echo "Issuing certificates into $CERTS"
./bin/globalcp-certs -dir "$CERTS" -names globalcp,cluster-a,cluster-b >/dev/null || {
  echo "cert generation failed"; exit 1; }

echo
echo "1. Startup refusals (the control plane)"

expect_refusal "no TLS and no explicit opt-in is refused" \
  "refusing to start without mTLS" \
  ./bin/globalcp -grpc-port 19190 -port 19191

expect_refusal "cert+key without a CA is refused, naming the CA" \
  "missing CA bundle" \
  ./bin/globalcp -grpc-port 19190 -port 19191 \
    -tls-cert "$CERTS/globalcp.crt" -tls-key "$CERTS/globalcp.key"

expect_refusal "a CA alone is refused, naming cert and key" \
  "missing certificate" \
  ./bin/globalcp -grpc-port 19190 -port 19191 -tls-ca "$CERTS/ca.crt"

echo
echo "2. Startup refusals (the coordinator)"

expect_refusal "embedded mode cannot be combined with TLS flags" \
  "cannot be combined with -global-tls" \
  ./bin/coordinator -port 19194 -grpc-port 19195 \
    -global-enabled -global-cluster-id cluster-a -global-embedded \
    -global-tls-cert "$CERTS/cluster-a.crt" -global-tls-key "$CERTS/cluster-a.key" \
    -global-tls-ca "$CERTS/ca.crt"

expect_refusal "a partial client TLS config is refused" \
  "missing private key" \
  ./bin/coordinator -port 19194 -grpc-port 19195 \
    -global-enabled -global-cluster-id cluster-a -global-endpoint 127.0.0.1:19192 \
    -global-tls-cert "$CERTS/cluster-a.crt" -global-tls-ca "$CERTS/ca.crt"

echo
echo "3. A real mTLS link"

./bin/globalcp -grpc-port 19192 -port 19193 \
  -tls-cert "$CERTS/globalcp.crt" -tls-key "$CERTS/globalcp.key" -tls-ca "$CERTS/ca.crt" \
  > "$LOGS/cp.log" 2>&1 &
PIDS+=($!)
if wait_for "$LOGS/cp.log" "gRPC server listening"; then
  pass "control plane starts with mTLS"
else
  fail "control plane did not start"; cat "$LOGS/cp.log"; exit $FAILURES
fi
grep -q "mTLS enabled" "$LOGS/cp.log" \
  && pass "control plane reports mTLS on" \
  || fail "control plane did not report mTLS"

./bin/coordinator -port 19196 -grpc-port 19197 \
  -global-enabled -global-cluster-id cluster-a -global-endpoint 127.0.0.1:19192 \
  -global-publish-prefixes /models/ -global-subscribe-prefixes /models/ \
  -global-tls-cert "$CERTS/cluster-a.crt" -global-tls-key "$CERTS/cluster-a.key" \
  -global-tls-ca "$CERTS/ca.crt" -global-tls-server-name globalcp \
  > "$LOGS/co-a.log" 2>&1 &
PIDS+=($!)
if wait_for "$LOGS/co-a.log" "connected to global control plane"; then
  pass "coordinator connects over mTLS"
else
  fail "coordinator never connected"; tail -5 "$LOGS/co-a.log"
fi
# The identity the CP logged must be the one in the certificate, not the
# one the coordinator was told to claim. Here they agree; the unit tests
# cover the case where they do not, which needs a forged request.
if wait_for "$LOGS/cp.log" "registered cluster=cluster-a"; then
  pass "control plane registered the cert's identity"
else
  fail "control plane did not register cluster-a"
fi

echo
echo "4. A plaintext client cannot reach a TLS control plane"

./bin/coordinator -port 19198 -grpc-port 19199 \
  -global-enabled -global-cluster-id cluster-b -global-endpoint 127.0.0.1:19192 \
  -global-publish-prefixes /models/ \
  > "$LOGS/co-b.log" 2>&1 &
PIDS+=($!)
grep -q "without mTLS" <(sleep 2; cat "$LOGS/co-b.log") \
  && pass "plaintext coordinator warns loudly at startup" \
  || fail "plaintext coordinator did not warn"
sleep 3
if grep -q "connected to global control plane" "$LOGS/co-b.log"; then
  fail "a plaintext coordinator connected to a TLS control plane"
else
  pass "plaintext coordinator is refused by the transport"
fi
if grep -q "registered cluster=cluster-b" "$LOGS/cp.log"; then
  fail "a plaintext caller created a cluster record"
else
  pass "no cluster record was created for the plaintext caller"
fi

echo
echo "5. Local autonomy: the coordinator outlives its control plane"

# This is requirement 3. Killing the CP must not take the coordinator with
# it -- the whole design rests on a cluster being complete on its own, and
# a sync agent that panics or exits on disconnect would break that
# silently, since nothing else in the process reports it.
CP_PID=${PIDS[0]}
kill "$CP_PID" 2>/dev/null
sleep 3
if kill -0 "${PIDS[1]}" 2>/dev/null; then
  pass "coordinator still running after the control plane died"
else
  fail "coordinator exited when the control plane died"
fi
if curl -sf --max-time 3 "http://127.0.0.1:19196/api/health" >/dev/null; then
  pass "coordinator still serves local requests"
else
  fail "coordinator stopped serving local requests"
fi

echo
if ((FAILURES == 0)); then
  printf '\033[32mAll checks passed.\033[0m\n'
else
  printf '\033[31m%d check(s) failed.\033[0m\n' "$FAILURES"
fi
exit $FAILURES

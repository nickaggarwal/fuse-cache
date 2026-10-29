#!/usr/bin/env bash
# Run the store workspace's checks on Linux (sendfile/MSG_MORE paths) in a
# container with the pinned Rust toolchain and Go (for the interop tests,
# which build tools/readbench). Same commands as .github/workflows/rust.yml.
set -euo pipefail
root="$(cd "$(dirname "$0")/../../.." && pwd)"
docker run --rm \
  -v "$root":/src -w /src/rust/store \
  -v store-front-cargo:/usr/local/cargo/registry \
  -v store-front-gocache:/root/go \
  -e CARGO_TARGET_DIR=/tmp/target \
  rust:1.90-bookworm \
  bash -c '
    set -e
    if ! command -v go >/dev/null; then
      curl -sSfL https://go.dev/dl/go1.26.5.linux-$(dpkg --print-architecture).tar.gz | tar -C /usr/local -xz
    fi
    export PATH=/usr/local/go/bin:$PATH
    rustup component add rustfmt clippy >/dev/null 2>&1
    cargo fmt --check
    cargo clippy --workspace --all-targets --locked -- -D warnings
    cargo test --workspace --locked
  '

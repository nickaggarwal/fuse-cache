#!/usr/bin/env bash
# Build the fusedata binary for Linux (arch of the Docker host) in a rust
# container. The target dir lives under rust/research/target-linux so the
# Linux and host builds do not clobber each other.
set -euo pipefail
here="$(cd "$(dirname "$0")/.." && pwd)"
image="${RUST_IMAGE:-rust:1.90-slim-bookworm}"
docker run --rm \
  -v "$here":/w -w /w \
  -v fusedata-cargo-registry:/usr/local/cargo/registry \
  -e CARGO_TARGET_DIR=/w/target-linux \
  "$image" \
  cargo build --release --locked -p fusedata "$@"
echo "built: $here/target-linux/release/fusedata"

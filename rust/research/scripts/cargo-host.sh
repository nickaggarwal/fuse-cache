#!/usr/bin/env bash
# Run cargo from the user-local rustup install (rust-toolchain.toml pins the
# version) without relying on the caller's PATH.
set -euo pipefail
export PATH="$HOME/.cargo/bin:$PATH"
cd "$(dirname "$0")/.."
exec cargo "$@"

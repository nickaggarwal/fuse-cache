# Rust in fuse-client

The fuse-client is a Go program. Rust appears in two places, with different
status:

| path | what | status |
|---|---|---|
| [`rust/store`](store/) | **store-front**: a Rust HTTP server that runs as a second container in the store pod, owns the public port, serves `/api/peer/read` and the OCI blob routes from the cache dir with `sendfile`, and forwards everything else to the Go client. Also **burst**, a load generator for store nodes. | merge candidate; built and pushed by CI (`rust.yml`, `images.yml`) |
| [`rust/research`](research/) | FUSE mount, in-process read path, Azure ranged reader, tiered reader, and the Go-vs-Rust benchmark harness (node-bench, A/B write-ups). | research only; not built in CI, not part of any merge |

## Who owns what

**Go owns the system.** Everything that changes state or talks to other
components stays in the Go client: writes and their digest checks, the
cache index, eviction and accounting, coordinator registration and
location publishing, chunk fills and promotion, peer and cloud fallback,
warm intents, CSI, node-init, metrics on `/metrics`.

**store-front owns only the read fast path on store nodes**, and only for
bytes that are already on local disk at a size it can confirm. It holds no
state of its own beyond short caches (published chunk strides, blob sizes
from Go's index). If it cannot answer a request exactly as Go would, it
forwards the request to Go unchanged, so behaviour is Go's by construction
and the differential test (`rust/store/crates/store-front/tests/interop.rs`)
checks it request by request.

Removing store-front means deleting the front container and the client's
`-port=8091 -advertise-port=8081` args; nothing in Go depends on it.

## Working on it

- Toolchain: pinned in `rust/store/rust-toolchain.toml` (rustup picks it
  up). Go is needed for the interop tests (they build `tools/readbench`).
- `cd rust/store && cargo test --workspace` (macOS or Linux);
  `rust/store/scripts/test-linux.sh` runs the CI commands in a Linux
  container.
- Dependencies are pinned exactly (`=`) with a committed `Cargo.lock`;
  `cargo deny --manifest-path rust/store/Cargo.toml check` enforces the
  licence and advisory policy in `rust/store/deny.toml`.
- Deployment, flags, metrics and the burst tool: [`rust/store/STORE.md`](store/STORE.md).

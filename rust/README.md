# Rust in fuse-client

The fuse-client is a Go program. Rust appears in one place:

| path | what | status |
|---|---|---|
| [`rust/store`](store/) | **store-front**: a Rust HTTP server that runs as a second container in the store pod, owns the public port, serves `/api/peer/read` and the OCI blob routes from the cache dir with `sendfile`, stages and hashes PUT bodies for Go to commit, and forwards everything else to the Go client. Also **burst**, a load generator for store nodes. | built, tested and pushed by CI (`rust.yml`, `images.yml`) |

## Who owns what

**Go owns every decision.** Everything that changes state or talks to
other components stays in the Go client: the cache index, eviction and
accounting, whether a write is accepted, coordinator registration and
location publishing, the cloud copy, chunk fills and promotion, peer and
cloud fallback, warm intents, CSI, node-init, metrics on `/metrics`.

**store-front moves bytes on store nodes.** Reads: bytes already on local
disk at a size it can confirm, sent with `sendfile`. Writes: a PUT body
streamed to a staged file and hashed, then handed to Go
(`POST /api/internal/commit`), which re-checks it and adopts it or refuses
it. It holds no state of its own beyond short caches (published chunk
strides, blob sizes from Go's index). If it cannot handle a request exactly
as Go would, it forwards it unchanged; the differential tests
(`rust/store/crates/store-front/tests/`) check reads and writes request by
request.

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

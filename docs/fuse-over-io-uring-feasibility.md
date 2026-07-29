# FUSE-over-io_uring: Feasibility & Plan

Status: **Blocked on library substrate; deprioritized vs. measured bottleneck.**
Author: performance investigation, 2026-07-28.

## TL;DR

FUSE-over-io_uring cannot be "turned on" for this project. It needs three things,
and two of them are missing on our stack:

1. **Kernel ≥ 6.14** on the node — the FUSE-over-io_uring protocol merged in Linux
   6.14 (March 2025). Our AKS nodes ran 5.15; standard AKS images currently top out
   around Ubuntu 24.04 / kernel 6.8, so a 6.14+ pool likely needs a custom or preview
   node image. **Not yet confirmed available on AKS.**
2. **A userspace FUSE daemon that speaks the io_uring FUSE protocol** — registers the
   shared submission/completion rings with the kernel and services requests via
   `uring_cmd` instead of the classic `/dev/fuse` read/write syscall loop.
   **This is the hard blocker (see below).**
3. Mount-time capability negotiation of `CAP_OVER_IO_URING`.

## The hard blocker: the Go FUSE stack has no implementation

This project's FUSE layer is Go:
- `github.com/hanwen/go-fuse/v2` v2.9.0 (vendored at `third_party/go-fuse-local`)
- `bazil.org/fuse`

Neither implements the io_uring FUSE protocol. In go-fuse the *only* trace of it is a
generated constant and its print label:

```
fuse/types.go:  CAP_OVER_IO_URING = (1 << 41)
fuse/print.go:  {CAP_OVER_IO_URING, "IO_URING"}
```

There is no ring setup, no `FUSE_URING` command handling, and no SQE/CQE processing
loop. `bazil.org/fuse` has nothing at all. FUSE-over-io_uring is fundamentally a
**libfuse (C) + kernel** feature; the Go libraries have not implemented the userspace
side.

Consequences:
- A node-pool kernel upgrade alone **cannot** unlock this. The kernel would advertise
  `CAP_OVER_IO_URING`, but our daemon has no code to accept it, so the mount falls back
  to the classic path.
- Enabling it requires one of:
  - **(a)** Implement the io_uring FUSE protocol inside go-fuse (ring registration,
    `uring_cmd`/`FUSE_URING` handling, request dispatch from the ring). Upstream-scale
    effort; no upstream to lean on today.
  - **(b)** Replace the FUSE layer with libfuse via cgo and use its io_uring support.
    A large rewrite of `internal/fuse/*` plus a cgo/C build toolchain.

## Why this is deprioritized (ROI)

FUSE-over-io_uring optimizes the **kernel ↔ FUSE-daemon round-trip** — the per-request
context-switch/copy cost of the classic `/dev/fuse` path. We measured this system's
actual read bottleneck and it is **not** the FUSE round-trip:

| Path | Throughput (measured, A100↔A100, kernel 5.15) |
|------|-----------------------------------------------|
| Raw single-stream network (HTTP netprobe) | **773 MiB/s** |
| Single gRPC peer-read stream | **~94 MiB/s** |
| Local chunk synthesis (in-process) | ~13 GB/s |
| Full in-process gRPC serve path (no network) | ~3.8 GB/s |

The ~8× peer-read gap is the **gRPC/protobuf data plane**, not FUSE. FUSE passthrough
(already enabled, `goFuseEnablePassthrough=true`) already lets cached NVMe reads bypass
the daemon for the data path, which is where the round-trip would have mattered most.

So even a fully working FUSE-over-io_uring would not move the numbers that are currently
limiting this workload.

## Recommended alternative: io_uring on the *data plane* (not the FUSE protocol)

The io_uring win that targets the measured bottleneck does **not** need kernel 6.14 or
FUSE-library changes:

- **Non-gRPC peer bulk transport** (HTTP chunked or raw TCP) with `sendfile`/`splice`
  zero-copy file→socket. Achievable on the current 5.15 kernel; the netprobe already
  proves ~773 MiB/s. Expected ~5–8× peer-read throughput.
- **io_uring `SEND_ZC`** (kernel ≥ 6.0) for the server send path as a second-order
  optimization on top of the raw transport — a modest kernel bump (6.0, widely
  available), *not* 6.14, and no FUSE-library dependency.

This reconciles "use io_uring" with "fix the real bottleneck."

## If we still want true FUSE-over-io_uring: staged plan

1. **Kernel availability spike** (days): determine whether AKS can provide a 6.14+ node
   image (preview channels, Azure Linux, or custom node image via node-image-gallery).
   Hard gate — if no 6.14+ image, stop.
2. **go-fuse protocol spike** (weeks): prototype `CAP_OVER_IO_URING` negotiation + a
   minimal ring read path in a go-fuse fork; benchmark round-trip latency vs. classic.
   Likely upstream-scale; budget accordingly.
3. **Only if 1–2 succeed and a round-trip-bound workload is identified**: productionize
   in the vendored fork, gate behind a flag, and validate on a 6.14+ pool.

Given the ROI analysis, steps 1–2 are research, not committed delivery.

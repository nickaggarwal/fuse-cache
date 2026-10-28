# Rust vs Go read path on a real model node (cpu-e, 2026-10-28)

Node aks-model-26363502-vmss000001: Azure Standard_E8ds_v6 (8 vCPU, 64 GiB),
440 GiB local NVMe, Ubuntu 26.04, kernel 7.0. Image
`fusedata-nodebench:ab-dm1` (Go = fuse-client main abdad90; Rust = this
branch). 3 reps per cell, page cache dropped before every cold cell,
medians. The lab numbers in AB.md came from a VM with a much faster virtual
disk; this is the production disk.

### FUSE (whole-layout files; MB/s median, ratio vs go; [daemon CPU s/GiB])

| case | go | go-pt | go-ra4m | rust | rust-pt |
|---|---:|---:|---:|---:|---:|
| cold seq 128K x1 | 812 [1.08] | 817 (1.01x) [0.00] | 810 (1.00x) [1.10] | 819 (1.01x) [0.88] | 812 (1.00x) [0.00] |
| cold seq 1024K x1 | 812 [1.09] | 798 (0.98x) [0.00] | 808 (1.00x) [1.08] | 816 (1.01x) [0.90] | 817 (1.01x) [0.00] |
| warm seq 128K x1 | 1,720 [1.88] | 9,642 (5.61x) [0.00] | 2,092 (1.22x) [1.67] | 2,864 (1.67x) [0.34] | 9,738 (5.66x) [0.00] |
| warm seq 1024K x1 | 1,710 [1.88] | 9,104 (5.32x) [0.00] | 2,035 (1.19x) [1.65] | 2,792 (1.63x) [0.34] | 9,084 (5.31x) [0.00] |
| cold rand 128K x1 | 30 [34.11] | 569 (19.01x) [0.00] | 30 (1.00x) [34.90] | 400 (13.34x) [0.63] | 568 (18.98x) [0.00] |
| cold seq 1024K x8 | 794 [1.23] | 821 (1.03x) [0.00] | 816 (1.03x) [1.08] | 819 (1.03x) [1.04] | 823 (1.04x) [0.00] |

### Blob endpoint (4 connections; MB/s median [min–max], p99 ms, CPU s/GiB: serving daemon (+ holder for remote))

| case | Go (main) | Rust | Rust/Go | Go p99 | Rust p99 | Go CPU | Rust CPU |
|---|---:|---:|---:|---:|---:|---:|---:|
| local cold rand 1024K | 489 [485–490] | 488 [488–489] | 1.00x | 43.8 | 43.8 | 0.61 | 0.44 |
| local cold seq 1024K | 862 [858–866] | 860 [852–860] | 1.00x | 53.1 | 49.2 | 0.44 | 0.24 |
| local cold rand 128K | 152 [152–154] | 368 [368–375] | 2.42x | 22.3 | 12.2 | 2.52 | 1.06 |
| local warm rand 1024K | 10,537 [10,056–10,585] | 12,866 [12,805–13,243] | 1.22x | 0.7 | 0.5 | 0.22 | 0.12 |
| remote cold rand 1024K | 489 [486–489] | 489 [487–489] | 1.00x | 42.2 | 43.7 | 2.23 + 0.67 | 0.50 + 0.45 |
| remote cold seq 1024K | 850 [843–854] | 859 [858–864] | 1.01x | 43.0 | 48.3 | 2.19 + 0.51 | 0.45 + 0.29 |
| remote cold rand 128K | 152 [152–152] | 366 [362–367] | 2.41x | 22.7 | 12.9 | 4.52 + 2.70 | 0.80 + 1.16 |
| remote warm rand 1024K | 2,253 [2,221–2,304] | 5,496 [5,329–5,568] | 2.44x | 3.2 | 1.2 | 1.75 + 0.27 | 0.56 + 0.14 |

### In-process read path (per-chunk files; MB/s median)

| case | go | rust |
|---|---:|---:|
| cold seq 128K x1 | 813 | 818 (1.01x) |
| cold seq 1024K x1 | 813 | 821 (1.01x) |
| warm seq 128K x1 | 13,224 | 15,042 (1.14x) |
| warm seq 1024K x1 | 13,665 | 24,086 (1.76x) |
| cold rand 128K x1 | 252 | 574 (2.28x) |
| cold seq 1024K x8 | 806 | 813 (1.01x) |

- `go-pt` is the production setting (passthrough + direct mount).
- Every Go mount used direct mount(2): Ubuntu 26.04's AppArmor confines the
  fusermount3 helper (EACCES), which stopped the first attempt.
- "remote" is a holder on the same machine (no network hop), the path a
  store node serves.

## Verdict

- **Cold sequential reads are disk-bound at about 820 MB/s for every
  implementation.** FUSE, in-process, local blob, blob from a peer: Go, Go
  with passthrough and Rust are within 3 %. The lab's 2.3x for cold blob
  reads came from a faster virtual disk; on this NVMe the disk is the limit.
- **Warm FUSE reads: passthrough wins, Go and Rust alike** (9.1–9.7 GB/s vs
  1.7 GB/s for Go without it and 2.8 GB/s for Rust without it). Production
  already runs Go with passthrough; Rust adds nothing there.
- **Cold random 128K FUSE reads: passthrough wins** (569 MB/s, 19x Go
  without it). Rust's own path reaches 400.
- **Rust wins where the disk is not the limit:**
  - small (128K) random blob ranges, local or from a peer: **2.4x**, half
    the p99 (12 vs 22 ms);
  - warm blob ranges from a peer: **2.4x** (5.5 vs 2.3 GB/s), p99 1.2 vs
    3.2 ms;
  - serving CPU on the peer path: about a quarter to a half of Go's.
- **For nydusd's workload** (about 1 MiB ranges, mostly cold on first
  start) Go and Rust are equal on this disk. Rust's advantage shows on
  small ranges and on a store or peer serving many readers from page cache.

## What this means

- Keep Go with passthrough for the weights mount.
- A Rust blob/peer server is worth it for the store node (serving many
  model nodes from page cache, where Go's per-request allocation is the
  cost) and for small random reads, not for a single node's cold start.

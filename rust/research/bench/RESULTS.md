# Go vs Rust read path — results (2026-10-27)

**Environment.** Docker Desktop linuxkit VM on an Apple-silicon Mac: Linux
6.10 arm64, 14 vCPU, 8 GiB RAM, virtio disk (`vda`, mq-deadline; 5.4 GB/s
with one cold reader, 17.5 GB/s with eight, from `probe-disk.sh`). Go 1.26.5
(cross-compiled), Rust 1.90.0 release + thin LTO. This is **not** an A100/AKS
node: absolute numbers are this VM's, and the cold-cache disk is much faster
and lower-latency than a cloud NVMe at queue depth 1. Treat ratios as the
result, not MB/s.

**Data.** `/bench/model.bin` 3 GiB + 777 B as 193 per-chunk files at 16 MiB
(PutFromReader), `/bench/whole.bin` 1 GiB + 333 B as one whole file with
chunked metadata (PutFromFile), both written by the Go client's write paths.

**Method.** Each cell 3 runs, Go and Rust interleaved, median shown. `cold`
drops the page cache before every run; `warm` primes it. Library = in-process
(`DefaultCacheManager.ReadRange` vs `fusedata_core::layout::CacheDir::read_at`).
FUSE = one pread client (`readbench bench-file`) against the Go go-fuse
mount and the Rust fuser mount of the same dir; CPU there is the **FUSE
daemon's** CPU (from /proc/<pid>/stat), at 10 ms tick resolution. Peer =
fetch every 16 MiB chunk object over loopback. Azure = chunk objects from
Azurite on the host. Raw JSONL in `results/`; regenerate the tables with
`python3 -I summarize.py results`.

## Headline

* **Random reads are where Go loses badly and Rust fixes it.** Go's chunk
  path reads the whole 16 MiB chunk to serve a 128 KiB window. Rust is 11-25x
  faster through FUSE (cold/warm), 150x faster in-process from page cache,
  with ~30x lower FUSE-daemon CPU per GiB and p99 cut from 3.7-12 ms to
  0.3-1.1 ms.
* **Multi-reader sequential through FUSE (the model-load shape with
  parallel loaders): Rust 1.7-1.9x**, with 3-4x less daemon CPU.
* **Single-reader sequential is a wash or a Go win.** Go's per-file chunk
  cache turns sequential windows into slices of an in-memory buffer (0.5 µs
  p50); in-process from page cache Go is 1.4-2x faster at one thread. Through
  FUSE, single-reader seq is equal cold (1.07-1.09x) and Go is faster on
  warm 1 MiB windows (0.69x). Rust wins seq only with more than one reader.
* **Peer transport: Rust client 1.3-1.6x** over the Go client against either
  server (half the client CPU per GiB). The Rust server is on par with Go's
  (both use sendfile); it is interoperable but not faster.
* **Azure (emulator): mixed.** Rust 1.8x at one stream, 0.66x at four
  concurrent chunk fetches. Azurite on Docker Desktop is the bottleneck and
  is noisy (±40 % run to run); this needs a real storage account before any
  conclusion.

## Tables

### Library, warm page cache, 1 thread

| case | Go MB/s | Rust MB/s | speedup | Go p50 us | Rust p50 us | Go p99 us | Rust p99 us | Go CPU s/GiB | Rust CPU s/GiB |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| chunked rand 128K | 114 | 17,230 | 150.57x | 983.4 | 5.8 | 2,688 | 21 | 10.801 | 0.063 |
| chunked rand 1024K | 938 | 20,765 | 22.14x | 985.4 | 35.9 | 2,421 | 143 | 1.365 | 0.049 |
| chunked seq 128K | 30,178 | 14,972 | 0.50x | 0.5 | 7.0 | 6 | 20 | 0.178 | 0.100 |
| chunked seq 1024K | 33,607 | 15,729 | 0.47x | 0.5 | 56.1 | 981 | 148 | 0.167 | 0.096 |
| whole rand 128K | 160 | 23,302 | 145.30x | 950.8 | 5.0 | 1,736 | 12 | 8.441 | 0.044 |
| whole rand 1024K | 1,214 | 27,905 | 22.99x | 962.3 | 32.2 | 1,971 | 83 | 1.241 | 0.037 |
| whole seq 128K | 37,146 | 26,599 | 0.72x | 0.5 | 4.2 | 4 | 11 | 0.112 | 0.038 |
| whole seq 1024K | 43,798 | 30,528 | 0.70x | 0.6 | 30.0 | 805 | 77 | 0.128 | 0.033 |

### Library, cold page cache, 1 thread

| case | Go MB/s | Rust MB/s | speedup | Go p50 us | Rust p50 us | Go p99 us | Rust p99 us | Go CPU s/GiB | Rust CPU s/GiB |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| chunked rand 128K | 75 | 888 | 11.79x | 1,115.0 | 86.5 | 11,924 | 496 | 13.500 | 0.262 |
| chunked rand 1024K | 383 | 2,754 | 7.19x | 1,391.1 | 298.0 | 10,726 | 1,005 | 1.821 | 0.135 |
| chunked seq 128K | 6,550 | 8,642 | 1.32x | 0.5 | 11.0 | 11 | 81 | 0.274 | 0.252 |
| chunked seq 1024K | 16,044 | 7,579 | 0.47x | 0.5 | 93.8 | 3,019 | 523 | 0.273 | 0.272 |
| whole rand 128K | 100 | 2,275 | 22.81x | 1,327.4 | 61.2 | 3,878 | 137 | 13.050 | 0.186 |
| whole rand 1024K | 787 | 4,974 | 6.32x | 1,363.5 | 194.1 | 3,452 | 500 | 1.831 | 0.112 |
| whole seq 128K | 4,072 | 3,372 | 0.83x | 0.5 | 31.8 | 8 | 158 | 0.250 | 0.175 |
| whole seq 1024K | 9,463 | 5,846 | 0.62x | 0.5 | 159.8 | 3,321 | 499 | 0.596 | 0.131 |

Go's "cold" sequential is optimistic in-process: its prefetcher reads 2-16
chunks ahead on 8 workers, so the measured reader mostly finds chunks already
in its range cache. That parallelism is the reason it beats Rust's 4 x 4 MiB
page-cache prefetcher at 1 MiB windows here.

### Library, warm page cache, 8 threads

| case | Go MB/s | Rust MB/s | speedup | Go p50 us | Rust p50 us | Go p99 us | Rust p99 us | Go CPU s/GiB | Rust CPU s/GiB |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| chunked rand 128K | 368 | 93,828 | 254.97x | 2,624.3 | 8.2 | 7,846 | 36 | 25.651 | 0.077 |
| chunked rand 1024K | 2,715 | 117,416 | 43.25x | 2,585.9 | 48.5 | 7,920 | 148 | 3.262 | 0.058 |
| chunked seq 128K | 24,343 | 72,279 | 2.97x | 0.6 | 7.2 | 438 | 30 | 0.290 | 0.077 |
| chunked seq 1024K | 22,437 | 68,087 | 3.03x | 1.2 | 54.6 | 6,194 | 261 | 0.294 | 0.074 |
| whole rand 128K | 410 | 71,080 | 173.26x | 2,707.5 | 8.3 | 7,694 | 38 | 22.490 | 0.095 |
| whole rand 1024K | 3,213 | 51,844 | 16.14x | 2,735.6 | 72.3 | 7,891 | 211 | 2.920 | 0.086 |
| whole seq 128K | 29,344 | 84,292 | 2.87x | 0.5 | 7.1 | 319 | 26 | 0.224 | 0.084 |
| whole seq 1024K | 33,302 | 80,567 | 2.42x | 0.8 | 56.9 | 4,933 | 259 | 0.215 | 0.083 |

### FUSE, cold page cache, 1 reader (CPU = FUSE daemon)

| case | Go MB/s | Rust MB/s | speedup | Go p50 us | Rust p50 us | Go p99 us | Rust p99 us | Go CPU s/GiB | Rust CPU s/GiB |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| rand 128K | 88 | 982 | 11.11x | 1,381.0 | 127.5 | 3,683 | 269 | 12.840 | 0.420 |
| rand 1024K | 236 | 1,986 | 8.41x | 3,930.7 | 499.3 | 12,497 | 1,117 | 20.840 | 0.460 |
| seq 128K | 2,474 | 2,644 | 1.07x | 25.8 | 48.8 | 464 | 178 | 1.120 | 0.400 |
| seq 1024K | 2,230 | 2,432 | 1.09x | 269.8 | 391.9 | 4,031 | 761 | 1.200 | 0.400 |

### FUSE, warm page cache, 1 reader (CPU = FUSE daemon)

| case | Go MB/s | Rust MB/s | speedup | Go p50 us | Rust p50 us | Go p99 us | Rust p99 us | Go CPU s/GiB | Rust CPU s/GiB |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| rand 128K | 94 | 2,356 | 24.95x | 1,414.9 | 68.5 | 4,374 | 119 | 12.220 | 0.250 |
| rand 1024K | 257 | 2,981 | 11.61x | 3,840.8 | 376.0 | 10,550 | 659 | 20.460 | 0.270 |
| seq 128K | 2,530 | 3,835 | 1.52x | 35.6 | 15.1 | 501 | 96 | 0.970 | 0.200 |
| seq 1024K | 20,401 | 14,099 | 0.69x | 42.2 | 35.4 | 157 | 401 | 0.000 | 0.020 |

"warm seq 1024K" is served almost entirely from the kernel's FUSE page cache
(both mounts use `FOPEN_KEEP_CACHE`; daemon CPU ~0 for both), so it measures
the kernel, not either daemon. One visible mount difference that may explain
the gap: go-fuse mounts with `max_read=1048576`, the fuser mount has no
`max_read` option (both BDIs show the same 128 KiB `read_ahead_kb`). Not
investigated further.

### FUSE, cold page cache, 8 readers (CPU = FUSE daemon)

| case | Go MB/s | Rust MB/s | speedup | Go p50 us | Rust p50 us | Go p99 us | Rust p99 us | Go CPU s/GiB | Rust CPU s/GiB |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| rand 128K | 279 | 4,401 | 15.77x | 3,629.0 | 242.0 | 9,659 | 468 | 20.570 | 0.570 |
| rand 1024K | 1,172 | 6,595 | 5.63x | 5,746.7 | 1,327.6 | 23,156 | 2,587 | 5.810 | 0.500 |
| seq 128K | 3,367 | 6,315 | 1.88x | 48.5 | 139.6 | 5,014 | 571 | 2.100 | 0.590 |
| seq 1024K | 3,652 | 6,214 | 1.70x | 686.1 | 1,173.3 | 12,251 | 2,456 | 1.820 | 0.580 |

### Peer transport, 16 MiB chunk objects over loopback, warm page cache

Rows are server implementation; columns compare the Go client
(`PeerStorage` raw HTTP) with the Rust client against that server.

| case | Go MB/s | Rust MB/s | speedup | Go p50 us | Rust p50 us | Go p99 us | Rust p99 us | Go CPU s/GiB | Rust CPU s/GiB |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| server=go threads=1 | 9,980 | 13,145 | 1.32x | 1,508.0 | 1,143.9 | 3,214 | 1,939 | 0.148 | 0.072 |
| server=go threads=4 | 24,535 | 39,788 | 1.62x | 2,416.1 | 1,539.2 | 5,806 | 2,506 | 0.144 | 0.091 |
| server=rs threads=1 | 10,979 | 15,193 | 1.38x | 1,405.0 | 1,017.6 | 2,824 | 1,580 | 0.137 | 0.063 |
| server=rs threads=4 | 25,715 | 39,383 | 1.53x | 2,191.7 | 1,557.9 | 6,754 | 2,455 | 0.148 | 0.094 |

Loopback has no wire; on a 25-100 Gb/s pod network the link, not either
client, will set the ceiling. The client CPU halving is what transfers.

### Azure chunk objects, 16 MiB, Azurite (host, macOS)

| case | Go MB/s | Rust MB/s | speedup | Go p50 us | Rust p50 us | Go p99 us | Rust p99 us | Go CPU s/GiB | Rust CPU s/GiB |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| threads=1 | 121 | 213 | 1.76x | 147,511.3 | 63,605.5 | 194,550 | 151,901 | 1.584 | 0.896 |
| threads=4 | 265 | 174 | 0.66x | 235,698.5 | 297,104.5 | 267,845 | 528,455 | 1.320 | 1.000 |

Both clients use first-range-then-parallel with 4 MiB blocks x 8. At 4
threads that is 32 concurrent GETs into a single-process Node emulator; the
Go SDK's connection handling copes better with that than ureq's pool. Not
measured against real Azure.

## Prefetcher sweep (Rust, cold, sequential)

`sweep-readahead.sh` (raw: `results/sweep-readahead.jsonl`): async
`WILLNEED` hints alone gave 5.0/6.2 GB/s at 128K/1M windows; 4 workers
populating 4 MiB pieces gave 10.3/10.3 GB/s; 8 or 16 workers were no better
and cost more CPU. 4 x 4 MiB is the default.

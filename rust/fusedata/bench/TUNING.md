# Single-reader sequential tuning (2026-10-28)

Goal: the model-load case — one reader, mostly sequential over each
safetensors file. The target was to beat **fixed Go** clearly on a cold
single-reader 1 MiB sequential read and at least match it warm. Fixed Go is
commit 2c047d1 ("read only the window for random reads of a local chunk"),
cherry-picked onto this branch as `8dc41e2`.

Same lab as `RESULTS.md`: Docker linuxkit VM, arm64, 14 vCPU, 8 GiB, virtio
disk (5.4 GB/s with one cold reader, 19 GB/s with 16). The dataset is the 3 GiB
per-chunk file at 16 MiB chunks. **Ratios only**: the disk is not an NVMe
behind an AKS node, and the prefetch defaults below were fitted to it.
`node-bench.sh` exists to re-measure on the real node.

## Final table (median of 5, interleaved)

`go` = fixed Go (go-fuse mount, default 128 KiB kernel readahead);
`rs-before` = Rust before this work; `rs` = Rust after; `go-ra4m` = the same Go
mount with the kernel readahead raised to 4 MiB (to separate the mount
setting from the daemon). cold = page cache dropped; warm = dropped, then the
backing chunk files read once (the daemon reads from page cache, the FUSE
page cache is empty). Raw rows: `results/tuning/final-matrix.jsonl`.

| layer | mode | case | go | rs-before | rs | go-ra4m |
|---|---|---|---:|---:|---:|---:|
| fuse | cold | seq 128K t1 | 1,729 | 2,598 (1.50x) | **7,579 (4.38x)** | 3,538 (2.05x) |
| fuse | cold | seq 1024K t1 | 2,140 | 2,429 (1.13x) | **6,806 (3.18x)** | 5,234 (2.45x) |
| fuse | warm | seq 128K t1 | 1,851 | 2,725 (1.47x) | **10,116 (5.46x)** | 5,257 (2.84x) |
| fuse | warm | seq 1024K t1 | 2,136 | 2,544 (1.19x) | **13,311 (6.23x)** | 4,795 (2.24x) |
| fuse | cold | seq 1024K t8 | 2,711 | 5,858 (2.16x) | 6,719 (2.48x) | 7,284 (2.69x) |
| fuse | cold | rand 128K t1 | 764 | 841 (1.10x) | 889 (1.16x) | 503 (0.66x) |
| library | cold | seq 128K t1 | 6,932 | 5,010 (0.72x) | 11,119 (1.60x) | - |
| library | cold | seq 1024K t1 | 15,756 | 7,875 (0.50x) | **17,724 (1.12x)** | - |
| library | warm | seq 128K t1 | 20,023 | 11,527 (0.58x) | 22,436 (1.12x) | - |
| library | warm | seq 1024K t1 | 16,115 | 13,992 (0.87x) | **20,563 (1.28x)** | - |
| library | cold | seq 1024K t8 | 7,476 | 8,859 (1.19x) | 7,000 (0.94x) | - |
| library | cold | rand 128K t1 | 1,281 | 1,218 (0.95x) | 1,599 (1.25x) | - |

MB/s. p99 for the FUSE single-reader seq cells: Go 0.5–5.4 ms, Rust
0.2–1.1 ms.

Against the targets:

* **Through FUSE (what vLLM sees): met clearly.** Cold single-reader seq 1 MiB
  is 3.2x Go, warm is 6.2x. About half of that is the kernel readahead
  setting: Go with the same setting (`go-ra4m`) gets 2.2–2.8x. Rust over
  `go-ra4m` is 1.3x cold and 2.8x warm.
* **In-process: met, narrowly.** Cold 1 MiB 1.12x, warm 1.12–1.28x (was
  0.5–0.87x). Go's in-process numbers vary run to run by up to 4x (its
  prefetch races the reader), so the medians carry real uncertainty; Rust's
  spread is smaller.
* **Not improved:** in-process 8-reader cold (0.94x; the 16 prefetch workers
  compete with 8 readers on a disk that saturates at ~16 streams) and random
  128 KiB (no change was aimed at it).

Noise: across all 210 runs the page cache after a drop was 49–60 MiB in 195
runs. 15 runs started with 200–900 MiB still cached (Go daemons releasing
their range cache, or a Rust mapping not yet reaped). They are spread across
both implementations and kept in the medians.

## What moved the number, and what didn't

Measured one knob at a time on the same binary (`ablate-seq.sh`,
library layer, median of 3–5; raw rows in `results/tuning/`).

| change | effect (single reader, seq) | kept |
|---|---|---|
| **FUSE kernel readahead 128 KiB → 4 MiB** (`/sys/class/bdi/<dev>/read_ahead_kb`, set by the mount) | FUSE cold/warm seq **~3.5 GB/s → ~14 GB/s** on the Rust mount; Go mount 2.2 → 6.5 GB/s with the same setting (`probe-fuse-ra.sh`). The biggest single lever: with 128 KiB the kernel sends 128 KiB READs to either daemon one at a time. 1 MiB was ~11 GB/s; 16 MiB no better than 4. | yes |
| **More, larger prefetch pieces**: 4 x 4 MiB → 16 x 8 MiB workers, window 128 → 512 MiB | in-process cold 1 MiB 6.3 → ~22 GB/s (disk needs ~16 streams to saturate). 24 workers: same. | yes |
| **Prefetch that waits** (`MADV_POPULATE_READ` / `pread` in workers) instead of async `WILLNEED` only | cold 128K 6.2 → 15 GB/s. `WILLNEED` alone did not keep the device busy. | yes |
| **Zero-copy views** (reply/consume straight from a slice of an `mmap`ed cache file instead of `pread` into a buffer) | warm seq 128K/1M **~18 → 30–35 GB/s** in-process, and one fewer copy per FUSE reply; vs `copy` (pread) 0.7–0.9x Go and a 4 MiB `segment` buffer 0.8x Go. | yes |
| **Populate the persistent mapping from the workers** so the reader's slice takes no faults | needed for the view win; without it per-page faults made views slower than `pread` | yes |
| Adaptive window (starts 32 MiB, doubles to 512 MiB per refill; reset on a non-sequential read) | no measurable change on full-file streams vs a fixed window; kept so short bursts and random reads do not trigger a 512 MiB prefetch | yes (behavioural) |
| `max_read=1048576` / `max_write` 1 MiB on the fuser mount (Go mount has it) | no change on its own; kernel readahead was the limit | yes (parity) |
| Multi-threaded FUSE serving | already present (read worker pool behind fuser's single receive loop); not re-measured | — |
| Keeping chunk fds open | already present | — |
| Segment buffer (one 4/16 MiB `pread` per segment, reply from it) | 0.6–0.8x Go; slower than plain views | no (`--view segment` kept for comparison) |

### A bug the tuning found: mapped pages pin the page cache

The first view implementation populated a persistent mapping and never
unmapped it. Pages mapped into a live process cannot be dropped by
`drop_caches` and are not reclaimable cache, so a long-running daemon pinned
every file it had streamed (RSS grew to 3 GiB, the size of the file read).
Every "cold" run after that was silently warm: in-process Rust "cold" showed
75 GB/s on a disk that does 19. `check-cold.sh` caught it.

Fix, in three parts:

1. The reader releases each window's pages (`MADV_DONTNEED`) right after
   consuming the slice. The page stays in the page cache.
2. A prefetch worker that finishes after the reader has passed its piece
   releases what was already consumed (`ChunkFile::consumed`).
3. Every file that may hold mapped pages is registered globally, and
   `layout::reap_mapped` unmaps it after 1 s idle. The FUSE frontend runs it
   every 500 ms. This covers prefetch past the end of a stream and dropped
   entries.

After the fix the daemon RSS settles at ~12 MB within a second of a stream
ending, and drop_caches empties the cache (`check-pin.sh`). The test
`read_view_matches_and_releases_mappings` checks that no cache file is still
mapped after reaping. All benchmark scripts now settle 2–3 s before dropping
caches, and `run-tuned.sh` records the page-cache size after each drop
(`cached_kib_after_drop`) so this is visible in the data.

## Reproduce

```bash
# lab (see README.md "Reproduce" for the container + dataset)
cp rust/fusedata/target-linux/release/fusedata /tmp/rb/fusedata-after   # new name: never overwrite a running binary
docker exec fusedata-lab sh /src/rust/fusedata/bench/lab-reset.sh
docker exec -e RS=/out/fusedata-after fusedata-lab sh /src/rust/fusedata/bench/mount-lab.sh
docker exec -e RS=/out/fusedata-after fusedata-lab sh /src/rust/fusedata/bench/run-tuned.sh > /tmp/rb/tuned.jsonl
python3 -I rust/fusedata/bench/focus-summary.py /tmp/rb/tuned.jsonl --md
# knob ablations
docker exec -e RS=/out/fusedata-after fusedata-lab sh /src/rust/fusedata/bench/ablate-seq.sh
docker exec fusedata-lab sh /src/rust/fusedata/bench/probe-fuse-ra.sh
# pinning / cold-cache sanity
docker exec -e RS=/out/fusedata-after fusedata-lab sh /src/rust/fusedata/bench/check-pin.sh
```

## Node benchmark (AKS model node)

Image (linux/amd64, ubuntu:24.04 + fuse3; builds both binaries from this
branch, Go includes 2c047d1):

```bash
docker buildx build --platform linux/amd64 \
  -f rust/fusedata/bench/Dockerfile.node \
  -t <registry>/fusedata-nodebench:<tag> --push .
```

Run privileged on the node with the NVMe cache disk mounted in, pointing at
a **new** directory (the script refuses a non-empty directory it did not
create, and deletes its dataset at the end unless `KEEP=1`):

```bash
docker run --rm --privileged --device /dev/fuse -v /mnt/nvme:/data \
  <registry>/fusedata-nodebench:<tag> \
  node-bench.sh /data/nodebench-cache /data/nodebench-scratch
# results: /mnt/nvme/nodebench-scratch/results.json (+ node-bench.log, rows.jsonl)
```

(As a pod: `privileged: true`, a hostPath volume for the cache disk, and the
same command.)

* Default dataset 4 x 4 GiB at 16 MiB chunks → needs **17 GiB free** on the
  cache disk. `FILES` / `FILE_GB` / `CHUNK_MB` change it.
* Matrix per rep: in-process {go, rust} and FUSE {go, go-ra4m, rust}, each
  cold seq 128K/1M, warm seq 128K/1M, cold rand 128K, cold 8-reader seq 1M.
  Single-reader seq cells read one whole 4 GiB file.
* Runtime: generation ~1–2 min, then ~105 GiB of reads per rep (30 cells,
  3 s settle each). Cold random 128K on Go is the slowest cell. Expect
  **~10–30 min for REPS=3**, depending on the disk; `REPS=1` for a quick look.
  (A 2 x 1 GiB, REPS=1 smoke run took 55 s under amd64 emulation.)
* `results.json` = `{env, rows, summary}`; `summary` has the medians, `env`
  the kernel, CPU, disk, and binary versions. The script also prints the
  table.

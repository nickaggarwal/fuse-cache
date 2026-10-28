# Go (main) vs Rust A/B — blob endpoint, passthrough, read path

Branch `feat/rust`, rebased on main `abdad90`. Go side is main exactly (only
benchmark harness and test files added: `tools/readbench`, two
`internal/cache/*_test.go`).

## What is compared

| column | what runs |
|---|---|
| `go` | go-fuse backend from main, defaults (128 KiB kernel readahead) |
| `go-pt` | same, `FUSE_GOFUSE_ENABLE_PASSTHROUGH=1 FUSE_GOFUSE_DIRECT_MOUNT=1` — the production fuse-gpu config |
| `go-ra4m` | same as `go`, mount `read_ahead_kb` raised to 4 MiB |
| `rust` | fusedata mount (4 MiB kernel readahead, mapped views, prefetch workers) |
| `rust-pt` | fusedata `--passthrough` (new: fuser abi-7-40 backing files) |
| blob `go` | main's `api.Handler`: `serveOCIBlob` → `PlanRange` (sendfile local chunks, `PeerStorage.ReadRange` on a miss), `-no-blob-fill` |
| blob `rust` | fusedata `serve-peer`: new `blob::BlobServer` (same routes, statuses, 64 MiB cap, plan-then-send, ranged peer window on a miss) |

Passthrough only applies to files stored whole, so the FUSE layer reads
whole-layout files (what model weights become after warm/assembly). The
blob workload is nydusd-shaped: HEAD, then 1 MiB `Range` GETs from 4
connections at scattered offsets (plus per-stream sequential and 128 KiB
scattered). "remote" = the reader's cache dir is empty and every window is
fetched over `/api/peer/read?off=&len=` from a holder.

## Real node (cpu-e, E8ds_v6, Ubuntu 26.04, kernel 7.0, local NVMe)

**Not run.** Creating the privileged benchmark pod on
`aks-model-26363502-vmss000001` (`kubectl apply -f
bench/node-bench-pod.yaml`) was blocked by the session's permission policy;
the image is pushed (`skyctlcpu8cac.azurecr.io/fusedata-nodebench:ab-54bfb4f`)
and the manifest is ready. See "Run it" below.

## Local lab (Docker linuxkit VM, arm64, virtio disk; ratios only)

`node-bench.sh` with FILES=2 FILE_GB=2 BLOB_GB=2 REPS=3 (the same script the
pod runs). Raw: `results/ab/lab-results.json` (median, min, max per cell).
Regenerate: `python3 -I bench/ab-tables.py results/ab/lab-results.json`.

### FUSE (whole-layout files; MB/s median, ratio vs go; [daemon CPU s/GiB])

| case | go | go-pt | go-ra4m | rust | rust-pt |
|---|---:|---:|---:|---:|---:|
| cold seq 128K x1 | 1,404 [2.37] | 1,244 (0.89x) [0.00] | 2,481 (1.77x) [2.11] | 2,686 (1.91x) [1.23] | 1,680 (1.20x) [0.00] |
| cold seq 1024K x1 | 1,241 [2.60] | 2,008 (1.62x) [0.00] | 2,708 (2.18x) [2.10] | 3,525 (2.84x) [1.83] | 2,204 (1.78x) [0.00] |
| warm seq 128K x1 | 1,840 [1.91] | 14,193 (7.71x) [0.00] | 3,164 (1.72x) [2.31] | 7,158 (3.89x) [0.64] | 13,063 (7.10x) [0.00] |
| warm seq 1024K x1 | 1,675 [2.01] | 7,636 (4.56x) [0.00] | 5,408 (3.23x) [1.14] | 7,294 (4.35x) [0.68] | 11,271 (6.73x) [0.00] |
| cold rand 128K x1 | 49 [20.68] | 675 (13.79x) [0.00] | 52 (1.05x) [19.72] | 407 (8.32x) [0.72] | 633 (12.92x) [0.00] |
| cold seq 1024K x8 | 1,736 [5.17] | 11,499 (6.63x) [0.01] | 2,353 (1.36x) [3.79] | 3,189 (1.84x) [1.60] | 5,457 (3.14x) [0.00] |

### Blob endpoint (4 connections; MB/s median [min–max], p99 ms, CPU s/GiB: serving daemon (+ holder for remote))

| case | Go (main) | Rust | Rust/Go | Go p99 | Rust p99 | Go CPU | Rust CPU |
|---|---:|---:|---:|---:|---:|---:|---:|
| local cold rand 1024K | 2,229 [1,521–2,230] | 5,195 [4,454–5,370] | 2.33x | 4.0 | 1.9 | 0.22 | 0.16 |
| local cold seq 1024K | 2,375 [2,308–2,438] | 5,577 [3,414–5,962] | 2.35x | 2.7 | 1.6 | 0.21 | 0.17 |
| local cold rand 128K | 873 [286–877] | 1,923 [1,514–2,086] | 2.20x | 1.5 | 0.7 | 0.76 | 0.44 |
| local warm rand 1024K | 17,096 [14,520–18,357] | 17,387 [13,350–24,228] | 1.02x | 1.4 | 1.0 | 0.09 | 0.06 |
| remote cold rand 1024K | 1,727 [1,623–2,164] | 3,994 [3,287–4,166] | 2.31x | 5.9 | 2.8 | 1.30 + 0.34 | 0.37 + 0.25 |
| remote cold seq 1024K | 2,189 [2,160–2,205] | 4,245 [4,160–4,641] | 1.94x | 4.2 | 1.9 | 0.96 + 0.28 | 0.32 + 0.25 |
| remote cold rand 128K | 742 [710–832] | 1,492 [935–1,506] | 2.01x | 1.9 | 0.6 | 2.12 + 0.96 | 0.64 + 0.60 |
| remote warm rand 1024K | 2,477 [2,421–2,894] | 10,440 [7,503–11,286] | 4.22x | 4.3 | 1.2 | 0.89 + 0.13 | 0.25 + 0.06 |

### In-process read path (per-chunk files; MB/s median)

| case | go | rust |
|---|---:|---:|
| cold seq 128K x1 | 2,755 | 4,092 (1.49x) |
| cold seq 1024K x1 | 2,556 | 5,520 (2.16x) |
| warm seq 128K x1 | 16,635 | 6,244 (0.38x) |
| warm seq 1024K x1 | 12,954 | 22,105 (1.71x) |
| cold rand 128K x1 | 302 | 558 (1.85x) |
| cold seq 1024K x8 | 6,000 | 14,010 (2.34x) |


### Verdict (lab; to be confirmed on cpu-e)

* **Blob endpoint — Rust is better: ~2–2.3x throughput on cold reads, local
  and remote, with about half the p99 and 1/3 the serving CPU per GiB on
  the remote path.** Warm local is a tie (both sendfile from page cache).
  Warm remote 4.2x: Go's miss path builds an HTTP request through
  `PeerStorage` + `net/http` per window and copies the body into a fresh
  buffer; Rust keeps a keep-alive connection per holder and reads the body
  into a reused buffer. This is the one workload where nothing in Go main
  already closes the gap.
* **FUSE warm sequential and random — Go with passthrough already matches
  or beats Rust without passthrough, and Rust with passthrough is the same
  kernel path.** go-pt and rust-pt are equal within noise (the kernel reads
  the backing file; daemon CPU 0). Rust's daemon path gets 50–60% of
  passthrough throughput on warm reads.
* **FUSE cold sequential, single reader — Rust (no passthrough) is ahead of
  every Go variant: 2.2x / 1.75x go-pt at 128K / 1M, 1.1x / 1.3x go-ra4m**
  (spread is wide: rust 1.4–4.4 GB/s at 128K) because its prefetch
  workers keep the disk busy; passthrough cold relies on the kernel's own
  readahead. Rust's passthrough mode gives that up and lands near go-pt.
* **FUSE cold 8 readers: go-pt (and rust-pt) win**; Rust's daemon path is
  0.28x go-pt there. Passthrough is the right default for whole files.
* **Per-chunk files (no passthrough possible):** Rust in-process is
  1.5–2.3x Go cold, but 0.38x on warm 128K, where Go serves 128 KiB windows
  from its 16 MiB in-memory range cache. Unchanged from before the rebase.

## Run it on a cpu-e model node

```bash
kubectl --context skyctl-cpu-e apply -f rust/research/bench/node-bench-pod.yaml
kubectl --context skyctl-cpu-e -n default logs -f fusedata-nodebench      # ~30-45 min; prints the table
kubectl --context skyctl-cpu-e -n default cp fusedata-nodebench:/results/results.json results.json
kubectl --context skyctl-cpu-e -n default delete pod fusedata-nodebench
```

The pod runs on node `aks-model-26363502-vmss000001` (idle at the time,
empty fuse cache), tolerates `skyimage.io/lazy`, mounts hostPath
`/skyfleet-fuse-cache` and writes only under
`/skyfleet-fuse-cache/fusedata-nodebench-{cache,scratch}`; the dataset
(2 x 4 x 4 GiB + a 4 GiB blob = 36 GiB; the disk has ~400 GiB free) is deleted
when the script exits. It starts no fuse-client/nydus component and leaves
the fuse-gpu / fuse-store DaemonSets alone. After the run the pod copies
`results.json` and the log into its own filesystem (`/results`) and removes
both directories from the node, so deleting the pod leaves the node as it
was.

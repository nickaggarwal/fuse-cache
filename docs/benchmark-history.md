# Benchmark History

Run history for the FUSE 3-tier cache, oldest scenarios last. Rows are
produced by `scripts/ops/benchmark-fuse-scenario.sh`, which appends to
`/tmp/fuse-benchmark-results.csv`; see the Benchmarking section of the README
for what each field means and how to keep rows comparable.


Benchmark fields captured:
- Cloud test type
- Machine types
- Results (write/read)
- Peer speed, cloud speed, object speed
- Net start at test start (writer/reader)
- CPU start at test start (writer/reader/coordinator)
- Git SHA used for run

Notes:
- `Peer speed` / `Cloud speed` are computed from per-tier metric deltas on the reader pod (`/api/cache/stats`).
- `Object speed` is end-to-end read throughput from benchmark script output (`READ_MBPS_APPROX`).
- `Net start` comes from coordinator peer telemetry (`/api/peers` -> `network_speed_mbps`).
- `CPU start` comes from `kubectl top pod` snapshot taken just before each run.
- Historical rows keep `N/A` where older runs did not capture that field.

### Latest E2E (2026-02-28, EKS `fuse-system-awstest`)

| Date | Cloud Test Type | Machine Types | Scenario | Results (Write/Read MB/s) | Peer Speed MB/s | Cloud Speed MB/s | Object Speed MB/s | Net Start MB/s (W/R) | CPU Start (W/R/C) | Git SHA |
|---|---|---|---|---:|---:|---:|---:|---:|---|---|
| 2026-02-28 | `peer-first(default)` | `i3en.6xlarge` writer + `i3en.6xlarge` reader | `test-smart-read.sh` 1GB | `681 / 1211` | `335.5` | `0.0` | `1211` | `1012.6 / 996.0` | `44m / 2m / 1m` | `87bcb18` |
| 2026-02-28 | `peer-first(default)` | `i3en.6xlarge` writer + `i3en.6xlarge` reader | `test-smart-read.sh` 5GB | `196 / 1225` | `351.1` | `0.0` | `1225` | `1004.1 / 921.6` | `87m / 1m / 1m` | `87bcb18` |
| 2026-02-28 | `s3-standard` | `i3en.6xlarge` writer + `i3en.6xlarge` reader | `test-smart-read-s3-profile.sh` 1GB | `664 / 1203` | `336.6` | `0.0` | `1203` | `848.7 / 809.8` | `172m / 1m / 1m` | `87bcb18` |
| 2026-02-28 | `s3express` | `i3en.6xlarge` writer + `i3en.6xlarge` reader | `test-smart-read-s3-profile.sh` 1GB | `670 / 1221` | `340.2` | `0.0` | `1221` | `816.4 / 787.4` | `166m / 1m / 1m` | `87bcb18` |

### Latest E2E (2026-02-28, AKS `fuse-system-aztest`)

| Date | Cloud Test Type | Machine Types | Scenario | Results (Write/Read MB/s) | Peer Speed MB/s | Cloud Speed MB/s | Object Speed MB/s | Net Start MB/s (W/R) | CPU Start (W/R/C) | Git SHA |
|---|---|---|---|---:|---:|---:|---:|---:|---|---|
| 2026-02-28 | `azure-blob(peer-first)` | `Standard_NC24ads_A100_v4` writer + `Standard_L64s_v3` reader | `test-smart-read.sh` 1GB | `277 / 473` | `16.2` | `0.0` | `473` | `0.0 / 0.0` | `1m / 1m / 1m` | `87bcb18` |
| 2026-02-28 | `azure-blob(peer-first)` | `Standard_NC24ads_A100_v4` writer + `Standard_L64s_v3` reader | `test-smart-read.sh` 5GB | `267 / 561` | `21.6` | `0.0` | `561` | `0.0 / 0.0` | `98m / 1m / 1m` | `87bcb18` |
| 2026-02-28 | `azure-blob(peer-first)` | `Standard_NC24ads_A100_v4` writer + `Standard_NC24ads_A100_v4` reader | `test-smart-read.sh` 1GB | `275 / 518` | `17.6` | `0.0` | `518` | `0.0 / 0.0` | `633m / 1m / 1m` | `87bcb18` |
| 2026-03-01 | `azure-blob(peer-first, nvme-max=48GB)` | `Standard_NC24ads_A100_v4` writer + `Standard_NC24ads_A100_v4` reader | `test-smart-read.sh` 1GB | `264 / 545` | `N/A` | `N/A` | `545` | `N/A` | `N/A` | `5fa1f9c+` |
| 2026-03-01 | `azure-blob(peer-first, nvme-max=48GB)` | `Standard_NC24ads_A100_v4` writer + `Standard_NC24ads_A100_v4` reader | `test-smart-read.sh` 5GB | `270 / 648` | `N/A` | `N/A` | `648` | `N/A` | `N/A` | `5fa1f9c+` |
| 2026-03-02 | `azure-blob(hybrid-largefile, hedge=5ms)` | `Standard_NC24ads_A100_v4` writer + `Standard_NC24ads_A100_v4` reader | `test-smart-read.sh` 1GB | `298 / 550` | `N/A` | `N/A` | `550` | `N/A` | `N/A` | `hybridwall2-20260301-213635` |
| 2026-03-02 | `azure-blob(hybrid-largefile, hedge=5ms)` | `Standard_NC24ads_A100_v4` writer + `Standard_NC24ads_A100_v4` reader | `test-smart-read.sh` 5GB | `288 / 671` | `N/A` | `N/A` | `671` | `N/A` | `N/A` | `hybridwall2-20260301-213635` |
| 2026-03-02 | `azure-blob(hybrid-largefile, hedge=5ms)` | `Standard_NC24ads_A100_v4` writer + `Standard_NC24ads_A100_v4` reader | `test-smart-read.sh` 5GB (profiled) | `276 / 551` | `150.2` | `80.1` | `551` | `N/A` | `N/A` | `hybridwall2-20260301-213635` |

### Latest Cached Read (2026-03-01, AKS `fuse-system-aztest`)

| Date | Machine Types | Scenario | Write MB/s | Cold Read MB/s | Cached Read MB/s | Notes | Git SHA |
|---|---|---|---:|---:|---:|---|---|
| 2026-03-01 | `Standard_NC24ads_A100_v4` writer + `Standard_NC24ads_A100_v4` reader | `test-gofuse-cached-read-suite.sh` 1GB | `282` | `436` | `2003` | `gofuse + passthrough; cached path served via range/kernel cache` | `5fa1f9c+` |
| 2026-03-01 | `Standard_NC24ads_A100_v4` writer + `Standard_NC24ads_A100_v4` reader | `test-gofuse-cached-read-suite.sh` 5GB | `262` | `622` | `2389` | `gofuse + passthrough; cached path served via range/kernel cache` | `5fa1f9c+` |
| 2026-03-02 | `Standard_NC24ads_A100_v4` writer + `Standard_NC24ads_A100_v4` reader | `test-gofuse-cached-read-suite.sh` 1GB | `274` | `751` | `2133` | `hybrid-largefile + wall metrics image` | `hybridwall2-20260301-213635` |
| 2026-03-02 | `Standard_NC24ads_A100_v4` writer + `Standard_NC24ads_A100_v4` reader | `test-gofuse-cached-read-suite.sh` 5GB | `291` | `592` | `2530` | `hybrid-largefile + wall metrics image` | `hybridwall2-20260301-213635` |

### Historical Scenarios

| Date | Cloud Test Type | Machine Types | Scenario | Results (Write/Read MB/s) | Peer Speed MB/s | Cloud Speed MB/s | Object Speed MB/s | Net Start MB/s (W/R) | CPU Start (W/R/C) | Git SHA |
|---|---|---|---|---:|---:|---:|---:|---:|---|---|
| 2026-02-24 | `peer-first(default)` | `i3en.6xlarge` writer + `i3en.6xlarge` reader | Baseline before hotspot iteration, 1GB | `699 / 788` | `N/A` | `0.0` | `788` | `N/A` | `N/A` | `25f8711` |
| 2026-02-24 | `peer-first(default)` | `i3en.6xlarge` writer + `i3en.6xlarge` reader | Baseline before hotspot iteration, 5GB | `339 / 887` | `N/A` | `0.0` | `887` | `N/A` | `N/A` | `25f8711` |
| 2026-02-24 | `peer-first(default)` | `i3en.6xlarge` writer + `i3en.6xlarge` reader | Hotspot Iteration A (regression), 1GB | `703 / 634-646` | `N/A` | `0.0` | `634-646` | `N/A` | `N/A` | `WIP pre-5ddcd09` |
| 2026-02-24 | `peer-first(default)` | `i3en.6xlarge` writer + `i3en.6xlarge` reader | Hotspot Iteration A (regression), 5GB | `203 / 838` | `N/A` | `0.0` | `838` | `N/A` | `N/A` | `WIP pre-5ddcd09` |
| 2026-02-24 | `peer-first(default)` | `i3en.6xlarge` writer + `i3en.6xlarge` reader | Hotspot Iteration B (accepted), 1GB | `697 / 1201-1244` | `N/A` | `0.0` | `1201-1244` | `N/A` | `N/A` | `5ddcd09` |
| 2026-02-24 | `peer-first(default)` | `i3en.6xlarge` writer + `i3en.6xlarge` reader | Hotspot Iteration B (accepted), 5GB | `206-219 / 1368-1395` | `N/A` | `0.0` | `1368-1395` | `N/A` | `N/A` | `5ddcd09` |
| 2026-02-21 | `azure hybrid` | `2x A100 + 1x L64s_v3` | Azure cloud optimization, 5GB run #2 | `N/A / 420` | `145.8` | `274.5` | `420` | `N/A` | `N/A` | `rev 71 image fuse-cloudopt-20260221100456` |
| 2026-02-21 | `gofuse cached-read` | `A100 writer + A100 reader` | go-fuse cold/cached suite, 1GB | `274 / cold 547, cached 2316` | `N/A` | `N/A` | `547 (cold)` | `N/A` | `N/A` | `rev 74 image gofuse-benchfix2-20260221-110935` |
| 2026-02-21 | `gofuse cached-read` | `A100 writer + A100 reader` | go-fuse cold/cached suite, 5GB | `271 / cold 524, cached 2518` | `N/A` | `N/A` | `524 (cold)` | `N/A` | `N/A` | `rev 74 image gofuse-benchfix2-20260221-110935` |


# Azure scale lab: 6-node results (2026-11-20, eastus)

Setup:
- 4 × D8ds_v6 (32 GiB) + 2 × E8ds_v6 (64 GiB), all with local NVMe, plus 1 system node;
- fuse-cache main (token exchange, holder leases), peer fill off;
- vLLM CPU build with Qwen2.5-7B in a 13.5 GB nydus image;
- time = "image may be read" → vLLM answers /v1/models.

Spot was not possible: spot quota is 3 vCPUs in every region.

## Per family (cluster cold before each cold start)

| Family | Cold | Warm (blobs in nydusd's cache) | Fill read avg |
|---|---|---|---|
| D8ds_v6 (32 GiB) | 201 s | 183 s | 7.2 ms |
| E8ds_v6 (64 GiB) | 234 s | 187 s | 24 ms |

Doubling RAM did not help. The E8ds node's cold start was slower, because its registry-fill reads waited longer (185 s of total wait vs 56 s). That points to registry throughput on that run, not the VM family. One run each, so this is not conclusive.

## Fan-out: one node fills from the registry, then the other 5 start at once

| Node | Ready | Reads (path) | Avg per read |
|---|---|---|---|
| ddsv6-00 (first, registry fill) | 190 s | 10,639 (fill) | 8.4 ms |
| edsv6-01 | 178 s | 13,241 (peer) | 9.2 ms |
| edsv6-00 | 200 s | 13,283 (peer) | 10.7 ms |
| ddsv6-02 | 216 s | 13,268 (peer) | 10.5 ms |
| ddsv6-03 | 221 s | 13,296 (peer) | 10.8 ms |
| ddsv6-01 | 224 s | 13,291 (peer) | 10.8 ms |

- With 5 nodes reading from one holder at the same time, readiness spread from 178 to 224 s, and each peer read went from about 8 to 11 ms.
- The holder served 131,554 peer reads, all from its local copy in the Rust store-front, and none were forwarded to Go.
- The 64 GiB E-series nodes finished first.

## Staggered: a new node every 30 s

| Order | Node | Ready | Reads |
|---|---|---|---|
| 1 | ddsv6-00 | 258 s | 9,860 fill |
| 2 | ddsv6-03 | 215 s | 13,206 peer |
| 3 | edsv6-00 | 201 s | 13,302 peer |
| 4 | edsv6-01 | 172 s | 13,304 peer |
| 5 | ddsv6-01 | 355 s | 1,151 peer |
| 6 | ddsv6-02 | 336 s | 1,177 peer |

- **Nodes 2–4 got faster** as more of the image landed: 215 → 172 s.
- **Nodes 5 and 6 were the slowest** (336–355 s) and made only about 1,150 reads each. These were the same two nodes that had started the image a few minutes earlier in the fan-out run. Fewer fuse reads plus a much slower start points to their local nydusd/overlay state, not to fuse-cache. It is unexplained, and worth a rerun with fresh nodes before reading anything into it.

## What this says for 15–20 nodes

- **Bottleneck:** a single holder serving everyone at once. Per-read latency rose about 35% at 5 concurrent readers.
- **Next tests:**
  - spread reads over several holders (fill a second node first);
  - add the L-series store node;
  - see whether the spread grows linearly with node count.
- **Quota:** 16 nodes needs the eastus increase in PLAN.md.

Torn down 2026-11-20 23:24; check.sh reports nothing left.

# 16-node ring placement A/B (2026-11-21, eastus)

Setup:
- 8 × D8ds_v6 + 8 × E4ds_v6;
- the 70B model's deployment at 1:50 scale, about 3 GB per reader;
- 15 readers start at once from a node-0 seed;
- three waves per arm: cold, warm cluster (readers evicted through `/api/cache/evict`), and 2 nodes lost.

| Wave | Ring off: median / max | Ring on (R=2): median / max | Busiest holder, off → on | Reads served, off → on |
|---|---|---|---|---|
| cold | 16.6 / 28.3 s | 14.7 / 26.8 s | 16% → 19% | 13,126 → 3,955 |
| warm cluster | 15.2 / 26.8 s | 17.3 / 28.1 s | 25% → 13% | 7,295 → 4,592 |
| 2 nodes lost | 12.5 / 27.4 s | 16.0 / 27.3 s | 21% → 37% | 6,533 → 3,452 |

- No bytes came from blob storage in any wave, and no pod crashed or restarted.
- **Start times:** no consistent change. The ring led cold and trailed warm and node-loss, all by 2–4 s.
- **The max is set by CPU:** it is always one of the 4-vCPU nodes, at about 27 s.
- **Fewer reads:** the ring cut reads by 50–70%, because there are fewer failed probes and owners serve whole chunks.
- **Node loss concentrates load:** with the ring, the departed owners' chunks get new owners who don't hold them yet, so reads pile onto the remaining holders (busiest 37%).
- **Why no win:** without the ring, readers serve chunks as soon as they finish them. The single-holder bottleneck the ring targets doesn't arise at this size.
- **Decision:** ring placement stays off by default (`-ring-replicas=0`).

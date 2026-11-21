# fuse-cache scale lab on AKS (15–20 nodes)

Goal: find out what changes when an image starts on many nodes instead of 2–4.
Three effects to measure:
- **fan-out:** how the first node's registry fill feeds everyone else (peer fill);
- **holder records:** whether the coordinator's holder records stay correct with many nodes (holder leases);
- **VM family:** whether the family (local disk type, memory) moves cold-start time.

## Quota (lab subscription, checked 2026-11-20)

| Region | Regional vCPUs free | Ddsv6 | Edsv6 | Lsv4 | Spot vCPUs |
|---|---|---|---|---|---|
| westus3 (cpu-w3 lives here) | 52 of 96 | 32 | 0 (cpu-w3 uses all 32) | 2 | 3 |
| **eastus** | **58 of 86** | **32** | **16** | 10 | 3 |
| eastus2 | 44 of 64 | — | — | — | 3 |
| westus2, centralus | 10 | — | — | — | 3 |

**Today, without a quota increase, the most is 6 model nodes of 8 vCPUs in eastus.** That is the default `POOLS`: 4 × D8ds_v6 + 2 × E8ds_v6 + 1 system node, 52 vCPUs. Spot is useless here (3 vCPUs per region).

For 15–20 nodes, raise these quotas in eastus (portal: Quotas → Compute):

| Plan | Pools | vCPUs | Needs |
|---|---|---|---|
| **A: 16 nodes, 2 families (recommended)** | 8 × D8ds_v6 + 8 × E8ds_v6 + system | 132 | regional 86→160, Ddsv6 32→64, Edsv6 32→64 |
| B: 20 nodes, 3 families | 8 × D8ds_v6 + 8 × E8ds_v6 + 4 × L8s_v4 + system | 164 | as A plus Lsv4 10→32, regional →180 |
| C: 16 nodes, spot | the same pools as spot | 128 spot | lowPriorityCores 3→160 (cheaper; eviction mid-run is possible) |

## The VM families and why they matter here

| Family | vCPU / RAM | Local disk | What it tests |
|---|---|---|---|
| Ddsv6 (D8ds_v6) | 8 / 32 GiB | 440 GiB local NVMe | baseline, cheapest; less page cache for a 13.5 GB image |
| Edsv6 (E8ds_v6) | 8 / 64 GiB | 440 GiB local NVMe | same disk, double RAM: shows the page-cache effect |
| Lsv4 (L8s_v4) | 8 / 64 GiB | 1.75 TiB NVMe | storage-optimised; the store/cache node class |
| DDSv5 / EDSv5 | 8 / 32–64 GiB | 300 GiB temp SSD (not NVMe) | older disk path, if the NVMe/disk question needs a control |

## Test matrix (same 13.5 GB qwen7b-cpu nydus image and harness as cpu-w3)

1. **Fan-out.** Cold cluster: start on node 1, wait for its fill, then start on all other nodes at once. Measure Ready per node, peer fill units per source, the per-read profile, and the holder's serve CPU. Run with peer fill on vs off.
2. **Staggered.** Starts at 0, 30, 60 … s across the fleet. Later nodes should read from earlier ones, not just the first.
3. **Family comparison.** The same cold and warm starts on one node of each family.
4. **Holder churn.** Evict or kill a few nodes mid-run. Check holder rows expire, readers stop being sent to dead nodes, and replication stays rate-limited (no storm).
5. **Store vs no store.** With and without the L-series store node, to see whether peers alone carry a 16-node fan-out.

## Teardown

Everything lives in the resource group `fc-scale-rg`, tagged `purpose=fc-scale`:
- `./down.sh` deletes that group (it refuses if the tag is missing);
- `./check.sh` confirms that the group, its `MC_` node group and every tagged resource are gone.

The shared ACR and the cpu-w3 cluster are never touched.

# cachesim

A discrete-event model of fuse-cache serving many model deployments across a fleet. It does not move real bytes; it applies the cache's policies to a catalogue of images and models.

**Catalogue:**
- 10 base images (vLLM, SGLang, TGI, llama.cpp, TRT-LLM and others) that share OS, CUDA and Python layers;
- 20 models from 0.5B to 70B, with Zipf-like popularity.

**What it models:**
- **Layer sources:** each deployment start reads its image's layers plus the model weights from local NVMe, a live peer, or the registry. Throughputs are the ones measured on cpu-w3 and Nebius, and peer reads slow down as more readers share one holder.
- **Eviction:** LRU per node against its NVMe budget. Layers of running deployments are pinned.
- **Holder records:** one per admission, with a generation and a lease, renewed on an interval. Eviction retracts a node's own record. A dead node's records disappear only when their leases lapse.
- **Replica reconciler:** tops up hot layers below the replica target, within a per-node budget and a cluster-wide slot cap. Like the client, it skips objects larger than `-max-replicate-mb` (64 MiB).
- **Node failure:** a group of nodes fails at a set hour.

**Running it:**

```bash
go run ./tools/cachesim                          # one run, with a timeline
go run ./tools/cachesim -scenario all -seed 1    # comparison table
go run ./tools/cachesim -scenario all -out out/  # plus per-scenario CSVs
```

**Scenarios:**

| Scenario | Changes |
|---|---|
| `baseline` | none |
| `tight-nvme-30gb` | half the NVMe per node |
| `no-leases` | records of dead nodes never expire |
| `no-slot-cap` | no cluster-wide replication cap |
| `no-pin-running` | layers of running deployments can be evicted |
| `burst-4x` | 4 × the start rate |
| `fail-50pct` | half the nodes fail |
| `replicate-all` | the reconciler copies layers of any size |

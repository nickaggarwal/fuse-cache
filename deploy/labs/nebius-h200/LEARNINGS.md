# Nebius H200 cold-start lab: learnings (2026-11-15)

Setup:
- 1 mk8s cluster in eu-west1;
- 1 CPU node plus 2 spot H200 nodes (1 GPU each, 465 GiB disk);
- the qwen7b-gpu nydus image (vLLM 0.11, Qwen2.5-7B, 30.6 GB RAFS v6) in the Nebius registry;
- fuse-cache main-5f712bf with the Rust store-front, nydus-snapshotter v0.16.1, Dragonfly 2.5.2;
- a hard teardown 2 h after start (kill-switch.sh).

Results are in results/bench.jsonl and results/podjson/ (`python3 timing.py results/podjson/*.json`).

## Platform lessons (each of these cost real time)

1. **The disk quota is per region and shared by every node's boot disk.**
   - `compute.disk.size.network-ssd-non-replicated` is 2048 GiB on this tenant, reported in bytes.
   - With 1023 GiB per node, 3 nodes failed at "Start Instance" with QuotaFailure.
   - That error only shows in the node-group events, not in kubectl.
   - up.sh preflight now checks it.
   - Watch node-group ERROR events in any monitor.
2. **The Nebius registry rejects Docker v2 manifests whose layers use `application/vnd.oci.image.layer.nydus.blob.v1`.**
   - The error is `MANIFEST_INVALID: Unknown blob format`.
   - Rewriting to an OCI manifest with `application/vnd.oci.image.layer.v1.tar` layers works.
   - Keep the `containerd.io/snapshot/nydus-*` annotations; the snapshotter keys on them, not on the media type.
   - Next time, run nydusify with `--oci` straight to Nebius.
3. **Registry blob GETs return 307 to `storage.<region>.nebius.cloud`.**
   - Sending the registry Bearer to that redirect gives 403 AccessDenied.
   - nydusd's registry backend with the CRI keychain got `DENIED` on every blob read: the I/O error on `/etc/passwd` at container create.
   - What worked: an explicit `auth` (base64 `iam:<IAM token>`) in nydusd-config.fusedev.json. It is written from a k8s secret by a one-shot pod, never printed.
   - The IAM token expires (about 12 h), so a long-lived setup needs a refresher or a service-account static key.
4. **Dragonfly's dfdaemon proxy got 401/403 against the Nebius registry**, from token passthrough and the redirect. It needs registry auth configured on dfdaemon itself, or the S3 redirect handled. Not solved in this run.
5. **Spot exists for GPU presets only.** The CPU node is on-demand.
6. **zsh `$n:test` expands as `$n:t` + `est`.** Image copies went to `df-schedulerest:latest`. Always use `${n}:test` in commands run from zsh.
7. **Passing a shell script inside `kubectl run --overrides` JSON breaks on quotes and newlines.** Base64 the script and run `echo $b64 | base64 -d | bash`.
8. **nydusd reads its config at mount time.** Restart containerd-nydus-grpc after switching the backend proxy, so the next mount picks it up cleanly.

## Benchmark lessons

See results/ and the summary in the memory file nebius-h200-lab.md.

### Results (1 run each; the 2 h deadline allowed no repeats)

Image: 30.6 GB nydus. Readiness is vLLM answering /v1/models. "Weights" is vLLM's own "Loading weights took". All starts follow a cleared nydusd blob cache and containerd image record on the starting node.

| Start | Node | Path | Weights load | torch.compile | Container start → API up | Created → Ready |
|---|---|---|---|---|---|---|
| registry direct (smoke) | A | nydusd → Nebius registry (307 → storage) | 156.7 s | 21.7 s | 23:16:29 → 23:20:19 = 230 s | ~250 s* |
| fuse-cache first node, cold cluster | A | nydusd → store-front → Go fill from registry | **87.9 s** | 18.8 s | 23:23:56 → 23:27:11 = 195 s | ~314 s* |
| fuse-cache second node | B | nydusd → store-front → Go → peer A | 168.9 s | 22.3 s | 23:28:03 → 23:32:18 = 255 s | ~300 s* |
| fuse-cache warm node | A | nydusd → store-front, blobs on local NVMe | 106.4 s | 18.5 s | 23:37:52 → 23:40:57 = 185 s | **226.7 s** (measured) |
| Dragonfly | – | – | not run: dfdaemon got 401/403 on the Nebius registry (redirect and auth), out of time | | | |

*Created → Ready for the first three came from log timestamps; bench.sh's timing parse was broken for them and fixed afterwards. The fuse-first figure includes about 100 s between container start (07:22:14) and vLLM's first log line (23:23:56). That gap is Python and vLLM imports read lazily through the cold path. The registry run spent about 0 s there because the smoke attempts before it had already pulled the import set into nydusd's cache. So the "container start → API up" column is the fairer comparison.

What the numbers say:
- **Cold first node:** fuse-cache's fill (16 × 8 MiB range GETs per blob, served while filling) loaded weights 1.8x faster than nydusd going straight to the registry: 88 s vs 157 s. That is the fetch-on-miss design working on a real GPU node.
- **Second node (peer path) was the slow one:** 169 s weights.
  - Node B's Go client served about 1,667 peer reads, peaking at 1.5 cores.
  - It neither filled from the registry nor ran faster than registry direct.
  - Same pattern as cpu-w3, where node 2 was slower, but much worse here.
  - Candidate causes: per-read peer round trips across nodes, small peer ranges, and Go peer fetch overhead.
  - **This is the strongest case yet for Rust/peer work (candidate 2: peer fetch on the reading node, bigger prefetch ranges).**
- **Warm local node:** 106 s weights. That is slower than the cold fill (88 s), even with every blob on local disk.
  - store-front CPU peaked at only 0.17 cores and the client sat idle, so neither process was busy.
  - Most likely, nydusd's own blobcache refill plus FUSE per-read cost is the floor.
  - Either way, the cache is not the limit here: vLLM's safetensors read through nydusd's FUSE is.
- **Fuse client CPU, the Rust gate:**
  - cold fill on A peaked at 1.05 cores;
  - peer serving on B peaked at 1.5 cores;
  - Rust store-front peaked at 0.2 cores, using 369 MiB.
  - No CPU saturation on 16-vCPU nodes. So the Rust gate is per-read latency on the peer path, not CPU.
- **torch.compile:** about 19–22 s every time. The compile cache (VLLM_CACHE_ROOT) arm was not run, but it is a reliable ~20 s lever per cpu-w3.

Not run because of the deadline: Dragonfly arms, prewarm (warm-image), compile cache, repeats.

# SkyPilot orchestration of vLLM on the FUSE cache

Two SkyServe specs. Both run vLLM on an A100 in the `gpuckpt` nodepool and
read the model **through the distributed FUSE mount** rather than downloading
it or baking it into an image.

| File | What it adds |
|------|--------------|
| `vllm-qwen.sky.yaml` | Weights off `/mnt/fuse` (read-only). Nothing else. |
| `vllm-qwen-kv.sky.yaml` | The above **plus** vLLM's real KV cache written to and read back from the same filesystem. |

```bash
python3 -m venv /tmp/skyenv && /tmp/skyenv/bin/pip install 'skypilot[kubernetes]'
sky serve up skypilot/vllm-qwen-kv.sky.yaml -n vllm-kv -y
SKY=/tmp/skyenv/bin/sky ./scripts/e2e-skypilot-vllm-kv.sh
sky serve down vllm-kv -y
```

## Four things that are not obvious and cost real time

**1. `mountPropagation: HostToContainer` is mandatory, and SkyPilot never
emits it.** `/mnt/fuse` on the host is a FUSE mount created by the client
DaemonSet *after* the node boots. Without propagation a pod captures the
directory at container start and sees the empty dir *underneath* the mount —
the model silently appears to be missing. The field has zero occurrences in
SkyPilot's source, but it is a valid `V1VolumeMount` field, so the client-side
validator accepts it, and `volumeMounts` patch-merge by `mountPath`, so it
survives the merge intact. Verified live: the field is present on the running
pod and all 7 model files are visible.

**2. The container entry in `pod_config` has no `name:` on purpose.** SkyPilot
merges containers by name; an unnamed entry merges into `containers[0]`, which
is `ray-node` — the container that actually runs the task. Naming it appends a
*second* container that never runs your code.

**3. The serve controller must run in consolidation mode on this cluster.**
The controller pod requests **no** GPU. When the only schedulable capacity is
the two A100 nodes, a GPU-less pod lands there and dies inside the NVIDIA CDI
hook:

```
failed to inject CDI devices: unresolvable CDI devices nvidia.com/gpu=none
```

`sky serve up` then loops "Failed to provision ... Retry after 30s" forever and
never mentions CDI. `~/.sky/config.yaml`:

```yaml
serve:
  controller:
    consolidation_mode: true
```

runs the controller in the API server process, so the only pod SkyPilot creates
is the replica — which *does* request a GPU and resolves CDI normally. Note
this is auto-enabled for jobs under `--deploy` but defaults to **False** for
serve, so it must be set explicitly.

**4. Blue-green updates need a spare GPU.** `sky serve update --mode
blue_green` brings the new replica up *before* retiring the old one. On a
2-GPU cluster a second update while an old replica is still around wants 3
GPUs and wedges in PROVISIONING. Tear down and relaunch instead.

## What the KV spec proves, and what it does not

`SharedStorageConnector` is vLLM's own **debug** connector: its source says
"Simple debug implementation", its path helpers are literally named
`_generate_foldername_debug`, it is not TP-safe (no rank in the path), and it
has no eviction and no size cap. It is enough to prove genuine KV tensors move
through the distributed filesystem; it is not a production KV tier.

Measured on `stargz-test`: one inference wrote 28 safetensors files, one per
attention layer, each a `kv_cache` BF16 tensor of shape `[2, 448, 256]` (the 2
is K and V) — 12.8 MB under a sha256-of-prompt-tokens directory.

The load-bearing assertion is **section 5**, because a KV hit observed on the
same engine that wrote it proves nothing — vLLM's own GPU prefix cache produces
an identical observation. So the script kills the engine, lets SkyServe rebuild
the replica, and asserts a *conjunction*:

```
External Cache Hit!                        (connector served the prefix)
vllm:prefix_cache_hits_total    = 0.0      (GPU cache was provably empty)
vllm:prefix_cache_queries_total = 457.0    (...and it was actually consulted)
```

Zero local hits over 457 queries means those bytes came off the mount and
nowhere else.

## Wiring the Model KV store itself (not done)

This uses vLLM's *filesystem* path, so the Model KV store's earned durability,
cloud catalog, and promotion logic are all bypassed. Making the store a
first-class vLLM tier means an out-of-tree `SecondaryTierManager`, which needs
**vLLM >= 0.26.0** (`TieringOffloadingSpec` + `secondary_tiers`); the image here
is pinned to 0.9.2. Two further constraints from `vllm/v1/kv_offload/tiering/
base.py`: every method "runs in the Scheduler process and must be lightweight
and non-blocking", and secondary tiers "cannot directly access GPU memory".

That version also brings `vllm:kv_offload_tiering_chunk_hits_total{tier="1:fs"}`
— the only counter that can prove a disk-tier hit directly, without the
kill-and-replay dance section 5 has to do today.

Beware: `SharedStorageConnector` was **renamed** to `ExampleConnector` in
v0.13.0 with no alias, so this exact config raises "Unsupported connector type"
on any newer vLLM.

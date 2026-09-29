# Disaggregated prefill/decode over the FUSE distributed cache

Two vLLM engines, on two different A100 nodes, with the fuse-client distributed
cache as the KV transport between them. Prefill computes the KV on one node;
decode consumes it on the other.

```
                    ┌──────────────────────────┐
  request ─────────▶│  disagg-proxy.py :9000   │
                    └────┬────────────────┬────┘
        max_tokens=1 ────┘                └──── original request
                    ▼                          ▼
      ┌─────────────────────┐      ┌─────────────────────┐
      │  vllm-prefill       │      │  vllm-decode        │
      │  node ...vmss0000h  │      │  node ...vmss0000g  │
      │  A100 #1            │      │  A100 #2            │
      └──────────┬──────────┘      └──────────▲──────────┘
         writes KV│                           │reads KV
                  ▼                           │
      ┌───────────────────────────────────────┴──────────┐
      │  /mnt/fuse-kv/vllmkv-disagg   (fuse-client)      │
      │  sha256(prompt_tokens)/<layer>.safetensors       │
      └──────────────────────────────────────────────────┘
```

## Why both halves say `kv_role: kv_both`

This looks wrong on first read. It is what vLLM's own V1 disagg example does
(`examples/offline_inference/disaggregated-prefill-v1/`), and the reason is
that `SharedStorageConnector` is **symmetric**: its `role` parameter is
`KVConnectorRole.SCHEDULER` vs `WORKER`, an internal distinction, not
producer vs consumer. The connector has exactly one behaviour — save KV after
computing it, load KV if a file matching the prompt hash already exists.

So the disaggregation is enforced by **routing**, not by a flag:

- the prefill engine is sent the request with `max_tokens` forced to `1`. It
  must compute the entire prompt KV to emit even one token, and saves it on
  the way out. The token is discarded.
- the decode engine is sent the **original** request. Same prompt ⇒ same
  `sha256(prompt_tokens)` ⇒ the connector finds the KV already on the mount,
  skips prefill, and goes straight to decoding.

`kv_producer`/`kv_consumer` are meaningful for connectors that push KV over a
network link (NixlConnector, P2pNcclConnector). NIXL is not installed in
`vllm/vllm-openai:v0.9.2` (`import nixl` → ModuleNotFoundError), and storage
transport is the interesting case here anyway, since the storage *is* the
thing being tested.

## Why this is real disaggregation, and the honest limit

Real: prefill and decode are separate processes, on separate GPUs, on separate
physical nodes, and KV computed on one is consumed by the other. That is the
property the distributed cache exists to provide.

The limit, stated plainly: this is **storage-mediated** disaggregation. A
production P/D stack (NIXL, LMCache, Mooncake) moves KV over RDMA in the
request's critical path and measures latency in microseconds. Writing
safetensors to a filesystem and reading them back is slower per request. What
it buys instead is that the KV outlives both engines and is visible to every
node in the cluster — which is why an engine restart can still hit it, and why
any node can pick up work for a prompt another node prefilled.

## Node pinning is not optional

Each spec pins its half with `nodeAffinity` on `kubernetes.io/hostname`. With
exactly one allocatable GPU per node, the scheduler would otherwise be free to
place both halves on the same node after a replica restart, and the setup
would silently collapse into the single-engine case it is meant to be
distinguished from — still returning correct answers the whole time.

Check the pinning holds before trusting any result:

```bash
kubectl -n default get pods -o wide | grep vllm
```

## Running it

Both nodes must have a free GPU. `sky serve status` will sit in `NO_REPLICA`
with a pending pod if one is occupied.

```bash
sky serve up skypilot/vllm-qwen-disagg-prefill.sky.yaml -n vllm-prefill -y
sky serve up skypilot/vllm-qwen-disagg-decode.sky.yaml  -n vllm-decode  -y
sky serve status                     # note the two endpoint ports

PREFILL_URL=http://localhost:30002 \
DECODE_URL=http://localhost:30003 \
python3 skypilot/disagg-proxy.py     # serves :9000

curl -s localhost:9000/v1/completions -H 'Content-Type: application/json' \
  -d '{"model":"Qwen/Qwen2.5-1.5B-Instruct","prompt":"...","max_tokens":32}' -D-
# X-Disagg-Prefill-Ms / X-Disagg-Decode-Ms report the split
```

Validation:

```bash
scripts/e2e-disagg-vllm.sh           # exit code = failure count
```

## What the test suite is actually asserting

The easy mistake is to declare victory when a disagg-shaped deployment returns
a correct completion. It always will: if the KV handoff never happens, the
decode engine just prefills the prompt itself and answers correctly, slightly
slower. **Correct output is almost no evidence.** The load-bearing checks are
the ones that fail if the handoff silently stops:

| § | assertion | what it rules out |
|---|-----------|-------------------|
| 3 | the two pods are on different nodes | single-box setup proving nothing |
| 5 | decode logs `External Cache Hit` **while its GPU `prefix_cache_hits_total` stays at 0** | the hit came from the engine's own in-GPU prefix cache |
| 6 | the tensors are read back **from the decode side** | the writer merely reading its own file |

§5 is a conjunction on purpose. `External Cache Hit` alone is not proof — a
same-engine GPU prefix-cache hit looks identical from outside. A hit recorded
while the GPU counter is provably flat can only have come off the mount.

The test prompt is unique per run (`disagg-$(date +%s)-$$`) because the
connector keys on the prompt hash: a fixed prompt would hit KV left behind by
an earlier run, and the suite would pass without the two engines ever
interacting.

## Traps that cost time here

1. **`mountPropagation: HostToContainer` is mandatory.** `/mnt/fuse` is a FUSE
   mount created by the DaemonSet. Without propagation the pod captures the
   directory at container start and sees the empty dir underneath. SkyPilot
   never emits this field (zero occurrences in its source), but it is a valid
   `V1VolumeMount` field, and `volumeMounts` patch-merge by `mountPath`, so it
   survives the merge.
2. **The container entry in `pod_config` has no `name:`.** SkyPilot merges
   containers by name; an unnamed entry merges into `containers[0]`
   (`ray-node`), the one that runs the task. Naming it appends a second
   container instead.
3. **`serve.controller.consolidation_mode: true` in `~/.sky/config.yaml`.** The
   controller is scheduled with no GPU request, and a GPU-less pod landing on a
   GPU node dies in the NVIDIA CDI hook: `unresolvable CDI devices
   nvidia.com/gpu=none`. Consolidation runs it in the API-server process
   instead. It is auto-enabled for *jobs* under `--deploy` but defaults to
   **False** for *serve*.
4. **Finding the engine PID.** `pgrep -f vllm.entrypoints...` matches the
   `sh -c` wrapper `kubectl exec` creates and returns that. Walk `/proc` and
   skip argv beginning `sh -c` — including in the scanner itself, whose own
   command line contains the search string.
5. **Do not grep the engine log for configuration.** vLLM outruns kubelet's
   10Mi rotation within a single test run, so the startup line announcing the
   connector path disappears mid-suite. Assert on `/proc/<pid>/cmdline`: argv
   is state, not history.

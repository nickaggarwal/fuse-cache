# llm-d fuse residency plugins

An llm-d EndpointPicker extractor + scorer that route inference requests toward
model-server pods whose **node already holds the model's weights on local NVMe**.

This is an out-of-tree Go module. It depends on `fuse-client` only over HTTP —
nothing here imports the parent repo — so it builds and tests on macOS even
though `fuse-client` itself does not.

## Why

llm-d already scores KV-cache locality (`prefix-cache-scorer`: "who has this
conversation's blocks?"). Nothing in it scores *weight* locality. Those are
different questions and they compose rather than compete:

| scorer | question | timescale |
|---|---|---|
| `prefix-cache-scorer` | who has this conversation's KV blocks in GPU memory? | per request |
| `fuse-residency-scorer` | who can load this model at NVMe speed instead of from object storage? | per pod lifetime |

The second only decides anything when the first is indifferent — which is
precisely the cold-start case, and cold-start is where the cost lives. JuiceFS
measured 8 IOPS on 4KB cold random reads against object storage versus ~12,000
warmed into local cache. For multi-GB weights that is the difference between a
pod serving in seconds and one serving in minutes.

## How it joins

```
 fuse coordinator                          llm-d EndpointPicker
 GET /api/residency?prefix=models/qwen/
        │                                   ┌──────────────────────────────┐
        │  per node: node_ip, fraction,     │ fuse-residency-extractor     │
        │  pinned, declared, last_verified, │  polls every 10s (one query  │
        │  unknown                          │  for the whole cluster)      │
        └──────────────────────────────────►│           │                  │
                                            │  join: NodeResidency.node_ip │
                                            │      == EndpointMetadata     │
                                            │         .NodeAddress         │
                                            │           ▼                  │
                                            │  stamps FuseResidency attr   │
                                            │           │                  │
                                            │ fuse-residency-scorer        │
                                            │  Category: Affinity          │
                                            │  score = byte-weighted       │
                                            │          resident fraction   │
                                            └──────────────────────────────┘
```

**The join is by node IP, and that is not an arbitrary choice.** An EPP endpoint
carries `pod.Status.HostIP` (`EndpointMetadata.NodeAddress`) and *no node name
at all*. The vLLM pod and the fuse client DaemonSet pod are different pods, so
their pod IPs have nothing in common. Host IP is the only field both sides can
see. This is why `PeerInfo.NodeIP` (`status.hostIP`) was added to fuse-client
alongside the existing `NodeName` — `NodeName` is the join key for anything that
can read the Kubernetes API, `NodeIP` is the one for anything that cannot.

**The extractor polls in the background rather than scraping per endpoint.**
Residency is cluster-wide data; the coordinator answers for every node in one
fan-out and already coalesces callers behind a 5s TTL cache. Scraping it once
per endpoint would multiply one intentionally-shared query by the fleet size to
get N copies of the same answer.

## Unknown is not zero

This is the invariant the whole design turns on, and it is enforced at three
layers (`Unknown bool` on the wire, `Known bool` on the attribute, a neutral
`unknownScore` at score time).

llm-d accumulates `weightedScore[endpoint] += score * weight`. An endpoint a
scorer *omits* from its returned map contributes nothing — arithmetically
identical to scoring it 0. So "I don't know" cannot be said by staying silent,
and saying it with 0 would punish a node for being briefly unreachable exactly
as hard as one verified to hold none of the model. Those two states call for
opposite routing decisions.

Concretely:

- a node the coordinator could not reach → `unknownScore` (default 0.4)
- a node with no `node_ip` (older fuse client, or non-Kubernetes) → `unknownScore`
- a model not in the extractor's `models[]` → `unknownScore`
- a reading older than `staleAfterSec` → `unknownScore`
- a node measured at 0% resident → **0.0**

A failed poll retains the previous reading and lets staleness expire it, rather
than zeroing on a blip. Retention is bounded, though: past `staleAfterSec` the
reading decays to *unknown*, never to zero — routing on a measurement nobody has
been able to refresh for minutes is the failure this mechanism exists to avoid.

This is the same rule that produced the tier-perf fix in fuse-client's
`5ac933d`: **every adaptive scorer must be fed only outcomes that carry the
signal it scores.** A collapsed score looks identical to a genuinely bad node,
so getting it wrong fails silently.

## Build

No fork of `llm-d-router` is required, despite what the sparse out-of-tree docs
imply. llm-d has no dynamic plugin loading — the registry is a package-level map
— but `fwkplugin.Register` is exported and `runner.NewRunner().Run()` is public,
so a ~20-line `main()` that registers before `Run` is enough. `llm-d-router` is
consumed as an ordinary pinned module (see `cmd/epp/main.go`).

```bash
go test ./...                                          # runs on macOS
docker build -t <registry>/fuse-residency-epp:latest .
az acr build --registry stargzrepo --image fuse-residency-epp:latest .
```

> Pinned to `llm-d-router` at main commit `95625abee`, **not** a release tag.
> At `v0.9.0` the API is materially different: `Register` takes no stability
> argument and `EndpointMetadata` has no `NodeAddress` field at all, so the join
> key this plugin needs does not exist there.

## Deploy

1. Build and push the image above.
2. Point your EPP Deployment at it and mount `deploy/epp-config.yaml` as a
   ConfigMap.
3. Add the flags:

```yaml
args:
  - --config-file=/config/epp-config.yaml
  # Both plugins register as Alpha. The gate is deliberate: an operator should
  # have to opt in before routing depends on a new signal.
  - --allow-experimental-plugins
```

4. Set `models[]` in the config to the model names your gateway serves and the
   cache prefixes their weights live under. A model absent from that list scores
   neutral everywhere — the plugin never claims to know something it was not
   asked to measure.

The fuse client DaemonSet must be running a build that populates `NODE_IP`
(`status.hostIP`, added in the same change as these plugins). Verify with:

```bash
curl -s "$COORDINATOR/api/residency?prefix=models/Qwen2.5-7B-Instruct/" | jq '.nodes[] | {node_name, node_ip, fraction}'
```

Any node showing `node_ip: null` predates the change and will score neutral
rather than being routed to.

## Configuration

### `fuse-residency-extractor`

| parameter | default | notes |
|---|---|---|
| `coordinatorURL` | — | required |
| `apiKey` | `""` | sent as `X-API-Key`; only needed if the coordinator runs with `-api-key` |
| `models[]` | — | required, non-empty; `{model, prefix}` pairs |
| `pollIntervalSec` | 10 | residency changes on the timescale of a warm or an eviction |
| `staleAfterSec` | 90 | must exceed `pollIntervalSec`; negative disables expiry |

Two configurations are rejected at startup rather than shipped, because both
fail silently at runtime:

- **empty `models[]`** — scores every endpoint neutral forever, which looks
  identical to a correctly wired plugin on a cold cluster
- **`staleAfterSec <= pollIntervalSec`** — every reading expires before the next
  poll can replace it, so the scorer never once sees a known value

### `fuse-residency-scorer`

| parameter | default | notes |
|---|---|---|
| `extractorName` | extractor default | only needed with multiple extractor instances |
| `unknownScore` | 0.4 | see *Unknown is not zero*; set 0 only if you would rather deroute unmeasured nodes than risk a cold one |
| `pinnedBonus` | 0 | added to a **fully** resident pinned node, then clamped to 1 |

## Metrics

Exported on the EPP metrics endpoint, labelled by model:

| metric | meaning |
|---|---|
| `fuse_residency_polls_total` | polls issued |
| `fuse_residency_poll_errors_total` | polls that failed (previous reading retained) |
| `fuse_residency_nodes_known` | nodes with a measured reading |
| `fuse_residency_nodes_unknown` | nodes the coordinator could not measure |
| `fuse_residency_best_fraction` | highest resident fraction across nodes |

`nodes_unknown` is the load-bearing one. Climbing while `nodes_known` stays flat
means the scorer is quietly contributing nothing and routing has fallen back to
the other scorers' preferences, with no error anywhere to show for it.

## Layout

```
pkg/residency/   HTTP client for GET /api/residency — no llm-d dependency
pkg/attribute/   the shared FuseResidency attribute + plugin type names
pkg/extractor/   background poller; joins reports onto endpoints by node IP
pkg/scorer/      Affinity-category scorer
cmd/epp/         custom EPP binary: register, then call the upstream runner
deploy/          EndpointPickerConfig
```

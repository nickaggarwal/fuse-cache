# fuse-client dashboard

A read-mostly Streamlit UI over the existing coordinator and client-daemon HTTP
APIs. It adds no server-side code and stores no state — every number on screen
comes from `GET /api/peers`, `GET /api/stats`, `GET /api/worldview`, a daemon's
`GET /api/cache/stats`, or its Prometheus `GET /metrics`.

## Views

| View | Source |
|------|--------|
| **Cluster overview** | `/api/peers` + `/api/stats`; NVMe used per node, stale-heartbeat flagging, aggregate tier hit rate summed from each daemon's `/metrics` |
| **Per-node cache** | one daemon's `/api/cache/stats` + `/metrics`: tier hits/misses, NVMe fill, evictions, pairwise peer latency, and the herd-control counters (`fuse_peer_serve_*`, `fuse_fetch_lease_*`, `fuse_chunk_advertise_*`, `fuse_replica_reconcile_*`, `fuse_busy_chunk_*`) |
| **Warm targeting** | fan-out form for coordinator `POST /api/warm` with a client-side dry-run preview, plus single-node `POST /api/cache/warm` (sync/async) |
| **Replicas & heat** | `/api/worldview?prefix=` — replica count vs. target, under-replicated hot files, per-path lookup via `/api/files/location?path=` |

## Run

```bash
python3 -m venv .venv && . .venv/bin/activate
pip install -r tools/dashboard/requirements.txt
streamlit run tools/dashboard/app.py
```

Config comes from the sidebar; the inputs default to environment variables:

| Env var | Sidebar field | Default |
|---------|---------------|---------|
| `FUSE_COORDINATOR_URL` | Coordinator URL | `http://localhost:8080` |
| `FUSE_API_KEY` | API key (sent as `X-API-Key` to daemons) | empty |
| `FUSE_NODE_ADDR` | Node address override | empty |

Responses are cached for 10–20s (`st.cache_data`) so auto-refresh does not hammer
every peer on every rerun. "Refresh now" clears the cache. Auto-refresh is off by
default and is suppressed on the Warm targeting page so form state survives.

The app renders with an unreachable coordinator — it shows a
`Cannot reach coordinator at <url>` banner rather than a traceback.

## Local dev cluster

`make dev-start` brings up a coordinator on `:8080` and clients on `:8081`+, all
on localhost, so the defaults work with no extra setup.

## AKS / port-forwarding

The coordinator:

```bash
kubectl -n fuse-system port-forward svc/coordinator 8080:8080
```

Set the sidebar Coordinator URL to `http://localhost:8080`.

Individual client daemons register with `POD_IP:8081` (see `cmd/client/main.go`),
which is **not routable from a laptop**. To use the Per-node cache view, forward
one client pod and set the sidebar "Node address override":

```bash
kubectl -n fuse-system get pods -l app=fuse-client -o wide
kubectl -n fuse-system port-forward pod/<client-pod> 8081:8081
# sidebar: Node address override = localhost:8081
```

With an override set, every per-node request goes to that one daemon regardless
of which node is selected in the dropdown (the header shows the registered
address alongside the one actually used). To page through nodes, re-point the
port-forward at a different pod. Without an override the app uses the registered
pod IPs, which works when the dashboard runs inside the cluster.

If the daemons were started with `-api-key`, put the key in the sidebar; it is
sent as `X-API-Key` to daemons and forwarded as the `api_key` body field on
coordinator fan-out warms (the coordinator re-sends it as `X-API-Key` when it
calls each peer). The coordinator itself has no auth middleware.

## API notes / limitations

- **Replica counts come from `/api/worldview`, not `/api/files/locations`.**
  `store.ListFileLocations` returns only the *first* location per path, so the
  prefix listing cannot be used to count replicas. `/api/worldview?prefix=` is
  built on `RangeFileLocations` and returns all of them.
- **Some responses use Go field names.** `cache.WarmupResult` and
  `cache.CacheEntry` have no json tags, so they serialize as `Files`, `Warmed`,
  `AlreadyLocal`, `Bytes`, etc. The UI reads both shapes where relevant.
- **Node capacity is derived.** `PeerInfo` carries `available_space` and
  `used_space`; the heartbeat sets available = capacity − used, so the dashboard
  shows capacity as `available_space + used_space`.
- **The dry-run preview is a client-side mirror** of
  `CoordinatorService.SelectWarmTargets` (active-only → node-ID filter → all
  labels must match → ID sort → `ceil(n*pct/100)`, minimum 1). There is no
  server-side dry-run endpoint. `test_dashboard.py` pins the semantics.
- **Timestamps.** Go marshals `time.Time` with up to 9 fractional digits;
  `parse_go_time` trims to 6 for `datetime.fromisoformat`.

## Tests

Stdlib `unittest`, no third-party deps needed (the `requests` import in
`fuse_api` is optional):

```bash
python3 -m unittest discover -s tools/dashboard
```

Covers the Prometheus parser (against verbatim `handlePromMetrics` output) and
the dry-run target selector.

## Files

| File | Role |
|------|------|
| `app.py` | Streamlit UI: sidebar config, 4 views, HTML/CSS chart primitives |
| `fuse_api.py` | HTTP client returning `ApiResult`; `Peer`; `select_warm_targets` |
| `metrics.py` | Prometheus text-exposition parser (`parse`, `MetricSet`) |
| `test_dashboard.py` | `unittest` suite |
| `requirements.txt` | streamlit, requests, pandas |

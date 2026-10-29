# store-front

A Rust server that owns a **store node's** public HTTP port. It moves the
bytes (the two hot read paths out of the shared cache directory, and PUT
bodies into it) and leaves every decision to the Go fuse-client running
beside it.

```
             :8081 (registered with the coordinator)
peers / nydusd ──────────────► store-front ──── native: /api/peer/read,
                                    │                  /v2/<repo>/blobs/<d>,
                                    │                  /oci/blobs/<d>, /v2/,
                                    │                  PUT /api/files/* (stage + hash)
                                    │  commit of each staged PUT, everything
                                    │  else, and every native
                                    │  request that is not an exact local hit
                                    ▼
                       Go client 127.0.0.1:8091 ── writes, coordinator,
                                                   fills, peers, cloud, ...
              both read/write the same cache dir (hostPath)
```

## What it serves, and what it forwards

| request | store-front |
|---|---|
| `GET /api/peer/read?path=P` | `P` is a local file: whole file (sendfile). `P` is `<parent>_chunk_N` and `<parent>` is a local whole file: that chunk's bytes at the stride the coordinator published, when the published size equals the file size. Optional `off`/`len` sub-range, as Go. Over `--max-inflight`: **503 + `Retry-After: 1`**, as Go. |
| `GET/HEAD /v2/<repo>/blobs/<digest>`, `/oci/blobs/<digest>` | Blob held whole at `/oci/sha256/<hex>` with **exactly** the size Go's index reports (asked with a `HEAD` to Go, cached 30 s): HEAD, single `Range` (206, capped at 64 MiB), or whole blob (200). |
| `GET/HEAD /v2/` | 200 with `Docker-Distribution-API-Version: registry/2.0`. |
| anything native that is not an exact local hit (miss, chunk stride not confirmed, short or different-size file, a request Go would refuse: 400, 404, 416) | forwarded unchanged, so the answer is Go's own (`/v2` miss 503, `/oci` miss 404, failed fetch 502, ...). |
| `PUT /api/files/<path>` with a `Content-Length` up to `--max-upload-gb` and a clean path | **Native write**: the body streams to `<path>.stage-front-<pid>-<n>` beside the final file (one 1 MiB buffer, `fsync` before commit) and is hashed (SHA-256) on the way. A blob path whose hash does not match its digest: **400**, as Go. Then `POST /api/internal/commit` to Go (loopback only), which re-checks size, digest and path, renames the file into place, indexes it, publishes it and starts the cloud copy. Go's refusal is relayed; Go down: **502**. Every failure removes the staged file. Chunked bodies, `Expect: 100-continue`, larger uploads and paths that are not already clean go to Go unchanged. `--no-native-put` sends every PUT to Go. |
| everything else (`GET/HEAD/DELETE /api/files/*`, `/api/cache/*`, `/api/health`, `/metrics`, `/api/peers*`, ...) | forwarded unchanged: request head byte for byte (every header, `X-API-Key` included), bodies streamed both ways as framed, `Expect: 100-continue` end to end, one 256 KiB copy buffer per connection. A multi-GB PUT streams; it is never buffered. |
| `/rust/healthz` | liveness: 200 while the process serves. |
| `/rust/readyz` | readiness: 200 when Go's `/api/health` is 200 and the cache dir is readable, else 503 with the reason. |
| `/rust/metrics` | Prometheus, `fuse_front_*` (below). |

Every decision stays in Go: whether a staged write is accepted, the index,
eviction and accounting, coordinator registration and publishing, the
cloud copy, chunk fills, peer and cloud fallback.

**Never a partial file.** A local file is served only when its size is
known to be right: the Go index's size for blobs, the published size for
chunk-of-whole-file reads, and literal files as Go's `LocalFilePath` would
serve them. Otherwise the request goes to Go.

**Auth** is Go's rule: with `--api-key` / `FUSE_API_KEY` set, `X-API-Key`
is required except on `/api/health`, `/metrics`, `/api/netprobe`,
`/rust/*`, `/v2/` and GET/HEAD on blob routes. A request without a valid
key is forwarded, so the 401 is Go's.

## Flags

| flag | env | default | |
|---|---|---|---|
| `--listen` | `FRONT_LISTEN` | `0.0.0.0:8081` | public port (Go registers it via `-advertise-port`) |
| `--backend` | `FRONT_BACKEND` | `127.0.0.1:8091` | the Go client (`-port`) |
| `--cache-dir` | `FRONT_CACHE_DIR` | – | the Go client's `-nvme`; wins over node-init |
| `--node-init-config` | `FRONT_NODE_INIT_CONFIG` | – | `node-init.json`; uses its `cache_dir` under `--host-root`, like Go's `-node-init-config` |
| `--host-root` | `FRONT_HOST_ROOT` | `/` | like Go's `-node-init-host-root` |
| `--node-init-wait-sec` | | `60` | wait for node-init.json to appear |
| `--coordinator` | `FRONT_COORDINATOR` | – | coordinator HTTP `host:port`, for published chunk strides; without it `_chunk_N`-of-whole-file reads go to Go |
| `--api-key` | `FUSE_API_KEY` | – | same value as the Go client's `-api-key` |
| `--max-inflight` | | `64` | concurrent native peer serves (Go `-peer-serve-max-inflight`) |
| `--drain-sec` | | `30` | on SIGTERM/SIGINT: stop accepting, finish in-flight requests up to this long, exit |

Logs go to stderr as `[FRONT] 2026/10/28 15:04:05 ...` (Go's
`log.LstdFlags` format).

## Wiring the store DaemonSet

Two containers in the store pod, sharing the cache dir:

**client** (Go, unchanged image) — add two args; everything else stays:

```yaml
args:
  - -port=8091            # Go listens here (pod-local)
  - -advertise-port=8081  # registers <podIP>:8081 = store-front
  - ...existing args (-node-init-config, -nvme, -chunk-size, coordinator, ...)
```

**front** (`fusedata-store-front` image):

```yaml
- name: front
  image: <acr>.azurecr.io/skyctl-mb/fusedata-store-front:main-<sha>
  args:
    - --listen=0.0.0.0:8081
    - --backend=127.0.0.1:8091
    - --node-init-config=/host-root/run/skyfleet-fuse/node-init.json
    - --host-root=/host-root
    - --coordinator=coordinator.skyctl-mb.svc.cluster.local:8080
  env:
    - name: FUSE_API_KEY          # only if the client has -api-key
      valueFrom: {secretKeyRef: {name: <same secret>, key: <same key>}}
  ports:
    - {name: http, containerPort: 8081}
  volumeMounts:
    - {name: host-root, mountPath: /host-root, readOnly: true}   # same hostPath as the client
  livenessProbe:
    httpGet: {path: /rust/healthz, port: 8081}
    periodSeconds: 10
  readinessProbe:
    httpGet: {path: /rust/readyz, port: 8081}
    periodSeconds: 5
  resources:
    requests: {cpu: 500m, memory: 128Mi}
    limits: {memory: 1Gi}
  lifecycle:
    preStop: {exec: {command: ["sleep", "5"]}}   # let endpoints drain first
terminationGracePeriodSeconds: 45                  # > --drain-sec
```

- The front only reads the cache dir; mount it read-only.
- Point the client's own liveness/readiness probes at `:8091` (its
  `/api/health`), not `:8081`.
- If a Service selects the store pod on 8081, it now reaches the front;
  everything the Service used to reach still works (forwarded).
- Rolling back = drop the front container and the two client args.

## Metrics (`/rust/metrics`)

| series | |
|---|---|
| `fuse_front_requests_total{route,outcome}` | route: `peer_read`, `blob`, `registry_ping`, `put`, `proxy`, `front`; outcome: `local`, `forwarded`, `busy`, `rejected`, `error`, `aborted` (the client hung up mid-response or mid-upload: a requester that raced several holders and cancelled this one; not a server fault) |
| `fuse_front_response_bytes_total{route,outcome}` | response body bytes |
| `fuse_front_request_duration_seconds{route}` | histogram, head-in to last byte out |
| `fuse_front_inflight_requests`, `fuse_front_native_inflight`, `fuse_front_open_connections` | gauges |
| `fuse_front_backend_errors_total` | failures reaching or talking to Go |

`local / (local + forwarded)` on `peer_read` and `blob` is the share of hot
reads Rust served. `busy` should match what peers see as 503s.

## burst (load generator)

In the same image: `/usr/local/bin/burst`. N clients on keep-alive
connections, reading the way model nodes do; one JSON line out; exit 1 on
errors.

```bash
# whole 16 MiB chunks of a model file via /api/peer/read
burst --target <store-ip>:8081 --clients 32 --pattern seq \
  chunk --object /models/qwen/model.safetensors:15400000000:16

# 1 MiB Range windows of a blob via /v2/x/blobs/sha256:<d>, scattered
burst --target <store-ip>:8081 --clients 32 --window-kib 1024 --pattern rand --mib-per-client 2048 \
  blob --blob <64-hex>:<size-bytes>
```

| flag | default | |
|---|---|---|
| `--target` | – | `host:port` of the store (front `:8081`, or Go directly for an A/B) |
| `--clients` | `16` | concurrent clients, one connection each |
| `--mib-per-client` | `0` | bytes each client reads; 0 = one pass over every object |
| `--pattern` | `seq` | `seq`: clients start at different offsets and walk forward; `rand`: scattered window-aligned offsets |
| `--window-kib` | `1024` | Range size (blob mode) |
| `--api-key` | `FUSE_API_KEY` | sent as `X-API-Key` |
| `--seed`, `--timeout-sec`, `--verify` | 42, 120, off | `--verify` checks readbench's pattern (datasets from `readbench gen`/`gen-blob`) |
| `chunk --object PATH:SIZE:STRIDE_MIB` | repeatable | |
| `blob --blob HEX:SIZE` | repeatable | |

Report fields: `mb_per_s`, `requests`, `bytes`, `seconds`, `p50_ms`,
`p90_ms`, `p99_ms`, `max_ms`, `busy_503` (each retried after 10–150 ms
jitter, as Go peers do), `errors`, `first_error`, `reconnects`.

## Build and test

```bash
# image (amd64), from the repo root
docker buildx build --platform linux/amd64 -f rust/store/Dockerfile.store \
  -t <acr>.azurecr.io/skyctl-mb/fusedata-store-front:<tag> --push .

# checks, as CI runs them (rust.yml), on Linux in a container
rust/store/scripts/test-linux.sh
# or on the host (macOS works; sendfile falls back to read/write)
cd rust/store && cargo fmt --check && cargo clippy --workspace --all-targets -- -D warnings && cargo test --workspace
cargo deny --manifest-path rust/store/Cargo.toml check
```

The interop tests build `tools/readbench` with Go and run store-front in
front of the real Go API handlers (`readbench serve-api`: the client's
`api.Handler` over a `DefaultCacheManager`, without FUSE). The differential
test sends the same 36 requests to Go directly and through store-front and
requires identical status, `Content-Length`, `Content-Range`,
`Accept-Ranges`, `Retry-After` and bodies.

## Files

```
rust/store/**                       store-front, burst, store-layout, Dockerfile.store, deny.toml
.github/workflows/rust.yml          fmt, clippy, tests, cargo-deny
.github/workflows/images.yml        builds and pushes the store-front image with fuse-client
tools/readbench/                    Go harness; serve-api runs the real Go handlers for the interop tests
```

## Known limits

- Blob sizes come from Go (`HEAD` per blob, cached 30 s); a blob replaced
  in place within that window would be checked against the old size (the
  exact-size rule then forwards it to Go).
- `_chunk_N` of a whole file is native only with `--coordinator` and a
  published chunked row of the same size; Go's in-memory index is not
  visible to the front, so those reads otherwise go to Go (correct, not
  native).
- One OS thread per client connection (blocking I/O). Fine for a store's
  peer count (tens to low hundreds of connections); not designed for
  thousands of idle keep-alives.
- No TLS (the Go client has none on this port either).

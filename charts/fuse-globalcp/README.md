# fuse-globalcp

The global control plane: one install per *federation*, not per cluster.

It holds an index, never bytes — "cluster A published `/models/x.bin` into
bucket B under prefix `a/`". Clusters dial out to it and hold a stream; it
never dials into a cluster, so member clusters need no inbound rule, no
LoadBalancer, and no public endpoint. Only this one service does.

Install the per-cluster half with the `fuse-cache` chart and its
`globalCP.*` values.

## Identity is the certificate

The control plane reads the **first DNS SAN** of the verified client
certificate as the cluster ID, falling back to the CN. So **issuing a
certificate is granting a cluster its identity.** Two consequences:

- The CA in `tls.secretName`'s `ca.crt` must sign *nothing but* cluster
  certificates. A public CA there is identity forgery, not merely a weaker
  channel — anyone holding a cert for any name could publish as that
  cluster.
- `clusterId` on the member chart is advisory. The control plane ignores
  what a client claims and uses the certificate. Set them to the same value
  so logs agree with reality.

Without mTLS there is no way to tell who is calling, so the binary refuses
to start, and this chart refuses to render. A dev install must say
`allowInsecureIdentity=true` explicitly, and must not be reachable from
outside its own node.

## Install

Production, with cert-manager or any other issuer that produces a
`tls.crt`/`tls.key`/`ca.crt` secret:

```bash
helm install globalcp charts/fuse-globalcp \
  --set tls.secretName=globalcp-server-tls \
  --set service.type=LoadBalancer \
  --set 'service.loadBalancerSourceRanges={203.0.113.0/24}'
```

Dev, on a cluster nothing else can reach:

```bash
helm install globalcp charts/fuse-globalcp --set allowInsecureIdentity=true
```

For a laptop there is a simpler path than either: the coordinator's
`-global-embedded` runs the control plane in-process on loopback, so the
whole cross-cluster path is exercisable with no certificates and no second
cluster (`globalCP.embedded=true` on the `fuse-cache` chart).

Test certificates for a dev federation come from `bin/globalcp-certs`,
which wraps the same trust model the unit tests use:

```bash
make build
./bin/globalcp-certs -dir /tmp/fedcerts -names globalcp,cluster-a,cluster-b
```

Not for production: no passphrase, no rotation, no revocation.

## Scaling

`replicaCount` is 1 and the chart refuses more. The default store is
in-memory and the subscriber fan-out is per-process, so two replicas behind
one Service give each cluster a different view of the index depending on
which pod its stream landed on — a partition that reads as eventual
consistency and never converges. Scaling out needs the etcd-backed store,
which is not wired here. `allowMultiReplica` exists to override the guard
once that is no longer true.

Downtime is survivable by design: clusters reconnect with backoff, resend
their registration, and keep serving their last-good mirror throughout. A
member cluster's local reads, writes, peer fetches and cloud fetches are
unaffected by a control plane outage — only discovery of new remote
publications stops.

## Ingress

`ingress.enabled` routes the **read-only HTTP surface only**
(`/api/global/clusters`, `/api/global/index`, `/api/health`). The gRPC port
is deliberately not routable this way: an ingress that terminates TLS
verifies the client certificate at the proxy and forwards an anonymous
connection, which turns cert-derived identity off with no error anywhere.
Expose gRPC with `service.type`, where the handshake stays end-to-end.

## Verification

`scripts/ops/e2e-global-chart.sh` renders both charts and feeds the actual
rendered args and env to the real binaries — a chart can emit perfectly
valid YAML naming a flag that does not exist, and `helm lint` will not
notice.

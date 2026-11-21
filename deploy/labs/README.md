# Cold-start labs

Scripts that create a short-lived cluster, run fuse-cache cold-start
benchmarks on it, and tear it down. Every lab has the same shape:

| Script | Does |
|---|---|
| `env.sh` | Names, sizes, regions; account IDs come from environment variables |
| `up.sh preflight` | Quota check only; creates nothing |
| `up.sh` | Creates everything (idempotent) |
| `down.sh` | Deletes everything the lab created, then runs `check.sh` |
| `check.sh` | Exit 0 only when nothing is left |

## nebius-h200/

1 CPU node + 2 spot H200s (1 GPU each) in eu-west1, with a Nebius registry and a bucket.
- Needs `NEBIUS_TENANT`, `NEBIUS_PROJECT`, `NEBIUS_SUBNET`, and `SRC_ACR` (where `images.sh` copies the test images from).
- `kill-switch.sh` with `DEADLINE=<epoch>` saves results and runs `down.sh` and `check.sh` at the deadline, whatever state the run is in.
- The benchmark is `bench.sh`. Its modes:
  - `auth`: give nydusd registry credentials;
  - `smoke`;
  - `peerfill`: first-node and second-node starts with `-peer-fill` on and off.
- `LEARNINGS.md` has the platform gotchas, among them:
  - the disk quota;
  - the Nebius registry rejecting nydus media types in Docker v2 manifests;
  - blob redirects and auth.

## azure-scale/

One AKS cluster with one pool per VM family, for 15–20 node tests.
- Needs `AZ_SUBSCRIPTION`, `ACR` and `ACR_RG`.
- Pools are set in `POOLS` in `env.sh`.
- Everything is in one resource group tagged `purpose=fc-scale`. `down.sh` deletes that group and refuses if the tag is missing.
- `PLAN.md` covers:
  - the quota per region and family;
  - which quota increases a 16- or 20-node run needs;
  - the test matrix.

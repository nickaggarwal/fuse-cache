# Sourced by up.sh / down.sh / check.sh. One Nebius mk8s cluster for the
# cold-start cache benchmark: 2 spot H200 nodes (first node / second node),
# 1 on-demand CPU node (coordinator, etcd, Dragonfly scheduler + seed).
# fuse-cache (fetch-and-deliver, store-front) vs Dragonfly vs registry only,
# vLLM Ready with Qwen2.5-7B.
#
# Every resource is created with label purpose=skyctl-h200-test and a name
# starting with $PFX, so down.sh and check.sh can find all of it.

TENANT=${NEBIUS_TENANT:?set NEBIUS_TENANT (tenant-...)}
PFX=skycache
LABEL=purpose=skyctl-cache-bench

# name | region | project | subnet | gpu platform | gpu preset | gpu quota name
# One cluster in eu-west1 (2026-11-15: H200 32 free, preemptible 128, CPU
# vCPU 200, public IPv4 3).
CLUSTERS="
a|eu-west1|${NEBIUS_PROJECT:?set NEBIUS_PROJECT (project-...)}|${NEBIUS_SUBNET:?set NEBIUS_SUBNET (vpcsubnet-...)}|gpu-h200-sxm|1gpu-16vcpu-200gb|compute.instance.gpu.h200
"
# GPU nodes in the one cluster: 2 = first node fills, second node reads it.
GPU_NODES=2

K8S_VERSION=1.35
# GPU node: 1 spot GPU per cluster. Driver preset from the compatibility matrix
# (k8s 1.35: ubuntu24.04 + cuda13.0 for both gpu-h200-sxm and gpu-rtx6000-a). Kernel 6.11 on every
# mk8s worker image: FUSE passthrough needs 6.9+.
GPU_DRIVERS=cuda13.0   # per-cluster platform + preset are in CLUSTERS
# Boot disk = fuse cache. No H200 preset has local NVMe (only the 8-GPU B300
# does), so the cache sits on a network disk: non-replicated SSD, 2 GiB/s at
# full size, about 110 MiB/s per 93 GiB unit. Size must be a multiple of
# 93 GiB: 1023 = 11 units, the nearest to 1,000 GiB (~1.2 GiB/s). No
# redundancy: a failed disk loses the node, acceptable for a test node whose
# cache refills from the bucket.
DISK_TYPE=network_ssd_non_replicated
# Tenant quota (2026-11-16): network_ssd_non_replicated is 2048 GiB per
# region, shared by every node. The CPU node already holds 1023 GiB, so the
# 2 GPU nodes get 465 GiB each (5 x 93): 1023 + 930 = 1953 GiB. The 30.6 GB
# nydus image plus the fuse cache fit easily.
GPU_DISK_GIB=465
# CPU node: router, fuse store + coordinator, controld. Same disk: the store
# serves the cluster's cold reads, so it must not be slower than the GPU
# node's cache.
CPU_PLATFORM=cpu-d3
CPU_PRESET=8vcpu-32gb
CPU_DISK_GIB=1023

# Registry + bucket live in cluster a's region (eu-west1); cluster b reads
# them across regions, like the AKS fleet reads its one ACR.
REG_REGION=eu-west1
REG_PROJECT=$NEBIUS_PROJECT
REG_NAME=${PFX}-images
BUCKET=${PFX}-fuse-store

STATE=${STATE:-$HOME/.sky-control/nebius-cache}   # ids, kubeconfigs, keys (mode 700)
mkdir -p "$STATE" && chmod 700 "$STATE"
KUBECONFIG_FILE=$STATE/kubeconfig
OPERATOR_IP=${OPERATOR_IP:-$(curl -4 -s -m 5 ifconfig.me)}

n() { nebius --no-check-update --no-progress "$@"; }
ctx() { echo "nebius-$PFX-$1"; }

# fuse-cache scale lab on AKS: one cluster, a fleet of 8-vCPU model nodes in
# up to three VM families, to see how first-node, Nth-node and fleet-wide
# cold starts behave at 15-20 nodes (peer fill, holder leases).
#
# Everything is in one resource group, RG, tagged purpose=fc-scale; teardown
# deletes that group (and only it). The ACR it pulls from is shared and
# never touched.
SUB=${AZ_SUBSCRIPTION:?set AZ_SUBSCRIPTION}
LOC=${LOC:-eastus}
RG=fc-scale-rg
CL=fc-scale
TAGS="purpose=fc-scale owner=fuse-cache"
ACR=${ACR:?set ACR (registry the lab pulls from)}
ACR_RG=${ACR_RG:?set ACR_RG}
CTX=fc-scale
STATE=$HOME/.sky-control/fc-scale
mkdir -p "$STATE"
# Pools: name|vm size|count|spot. One pool per VM family, so per-family
# differences (local NVMe vs temp SSD, memory) show up in the same run.
#   Ddsv6 : 8 vCPU / 32 GiB, 440 GiB local NVMe   (fam quota 32 in eastus)
#   Edsv6 : 8 vCPU / 64 GiB, 440 GiB local NVMe   (16 free in eastus)
#   Lsv4  : 8 vCPU / 64 GiB, 1.75 TiB local NVMe  (10 in eastus)
#   DDSv5 / EDSv5 : 300 GiB temp SSD, no NVMe     (10 each)
POOLS=${POOLS:-"
ddsv6|Standard_D8ds_v6|4|no
edsv6|Standard_E8ds_v6|2|no
"}
SYSTEM_VM=Standard_D4as_v7

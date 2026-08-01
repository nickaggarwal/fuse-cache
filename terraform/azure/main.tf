# AKS test cluster for fuse-client + pod-snapshotter (mirrors stargz-test).
#
# Requirements driving the choices:
#   - pod-snapshotter: k8s >= 1.30, containerd >= 2.0. On AKS, Ubuntu 22.04
#     pools ship containerd 1.7 (CheckpointContainer NOT implemented);
#     Ubuntu 24.04 pools ship containerd 2.x. All checkpoint/restore pools
#     therefore use os_sku = "Ubuntu2404".
#       * azurerm provider support: os_sku "Ubuntu2404" is accepted from
#         provider v4.67.0 (pinned in providers.tf) and requires AKS
#         Kubernetes >= 1.32. If you are stuck on an older provider, the
#         az CLI fallback is:
#           az aks nodepool add -g <rg> --cluster-name <cluster> -n nvme2404 \
#             --node-count 0 --node-vm-size Standard_L8s_v3 \
#             --os-sku Ubuntu2404 --mode User --labels pool=nvme2404
#   - fuse-client: local NVMe (Standard_L8s_v3) discovered by node-init;
#     Azure Blob as the cloud tier.
#   - Expensive pools (L8s_v3, A100) default to count 0 — scale up only
#     while testing.

resource "azurerm_resource_group" "this" {
  name     = var.resource_group_name
  location = var.location

  tags = {
    project    = "fuse-snap-test"
    managed_by = "terraform"
  }
}

# ---------------------------------------------------------------------------
# AKS cluster: one small system pool; user pools attached below.
# ---------------------------------------------------------------------------

resource "azurerm_kubernetes_cluster" "this" {
  name                = var.cluster_name
  location            = azurerm_resource_group.this.location
  resource_group_name = azurerm_resource_group.this.name
  dns_prefix          = var.cluster_name
  kubernetes_version  = var.kubernetes_version != "" ? var.kubernetes_version : null

  default_node_pool {
    name       = "system"
    vm_size    = var.system_pool_vm_size
    node_count = var.system_pool_count
    # System pool also runs on Ubuntu 24.04 so every node in the cluster has
    # containerd 2.x (harmless for kube-system, consistent for debugging).
    os_sku = "Ubuntu2404"

    only_critical_addons_enabled = false

    upgrade_settings {
      max_surge = "10%"
    }
  }

  identity {
    type = "SystemAssigned"
  }

  # Workload identity / OIDC — the AKS analogue of EKS IRSA; cheap to enable
  # now, needed if pods later get Azure RBAC instead of account keys.
  oidc_issuer_enabled       = true
  workload_identity_enabled = true

  network_profile {
    network_plugin = "azure"
  }

  tags = {
    project = "fuse-snap-test"
  }
}

# ---------------------------------------------------------------------------
# NVMe user pool — Standard_L8s_v3 (local NVMe for fuse-client node-init).
# Defaults to 0 nodes; scale up only while testing:
#   az aks nodepool scale -g <rg> --cluster-name <cluster> -n nvme --node-count 3
# ---------------------------------------------------------------------------

resource "azurerm_kubernetes_cluster_node_pool" "nvme" {
  name                  = "nvme"
  kubernetes_cluster_id = azurerm_kubernetes_cluster.this.id
  vm_size               = var.nvme_pool_vm_size
  node_count            = var.nvme_pool_count
  mode                  = "User"

  # Ubuntu 24.04 => containerd 2.x (pod-snapshotter hard requirement) and
  # apt/PPA CRIU installs for its nodeSetup DaemonSet.
  os_sku = "Ubuntu2404"

  node_labels = {
    workload = "fuse-snap-test"
    pool     = "nvme"
  }

  tags = {
    project = "fuse-snap-test"
  }

  lifecycle {
    # Allow scaling via az CLI without terraform reverting it.
    ignore_changes = [node_count]
  }
}

# ---------------------------------------------------------------------------
# Optional GPU pool — A100 for GPU checkpoint/restore (cuda-checkpoint).
# Var-gated off by default; count 0 even when enabled.
# ---------------------------------------------------------------------------

resource "azurerm_kubernetes_cluster_node_pool" "gpu" {
  count = var.enable_gpu_pool ? 1 : 0

  name                  = "gpu"
  kubernetes_cluster_id = azurerm_kubernetes_cluster.this.id
  vm_size               = var.gpu_pool_vm_size
  node_count            = var.gpu_pool_count
  mode                  = "User"

  # Ubuntu2404 on GPU pools too: containerd 2.x + PPA CRIU + CDI-mode
  # NVIDIA toolkit are all prerequisites for GPU checkpointing.
  os_sku = "Ubuntu2404"

  node_labels = {
    workload = "fuse-snap-test"
    pool     = "gpu"
  }

  node_taints = [
    "nvidia.com/gpu=present:NoSchedule",
  ]

  tags = {
    project = "fuse-snap-test"
  }

  lifecycle {
    ignore_changes = [node_count]
  }
}

# ---------------------------------------------------------------------------
# Storage account + blob container — fuse-client Tier 3 (cloud).
# ---------------------------------------------------------------------------

resource "random_string" "storage_suffix" {
  length  = 8
  lower   = true
  upper   = false
  numeric = true
  special = false
}

# Performance: premium_blob = true (default) creates a Premium block blob
# account — SSD-backed, single-digit-ms latency, the Azure analogue of
# S3 Express One Zone and the right choice for the fuse-client cloud tier
# in latency tests. Set premium_blob = false for a Standard account
# (~8x cheaper per GB, higher and more variable latency) for comparison runs.
# NOTE: account_kind/tier can't be changed in place — flipping the var
# replaces the account and loses its contents.
resource "azurerm_storage_account" "this" {
  name                = "${var.storage_account_prefix}${random_string.storage_suffix.result}"
  resource_group_name = azurerm_resource_group.this.name
  location            = azurerm_resource_group.this.location

  account_tier             = var.premium_blob ? "Premium" : "Standard"
  account_kind             = var.premium_blob ? "BlockBlobStorage" : "StorageV2"
  account_replication_type = "LRS" # test data; also: premium block blob supports only LRS/ZRS

  min_tls_version = "TLS1_2"

  tags = {
    project = "fuse-snap-test"
  }
}

resource "azurerm_storage_container" "cache" {
  name                  = var.blob_container_name
  storage_account_id    = azurerm_storage_account.this.id
  container_access_type = "private"
}

# ---------------------------------------------------------------------------
# Existing ACR (stargzrepo) — data source only, var-gated. This module does
# NOT create a registry; it just grants the cluster kubelet AcrPull.
# ---------------------------------------------------------------------------

data "azurerm_container_registry" "existing" {
  count               = var.existing_acr_name != "" ? 1 : 0
  name                = var.existing_acr_name
  resource_group_name = var.existing_acr_resource_group
}

resource "azurerm_role_assignment" "acr_pull" {
  count = var.existing_acr_name != "" ? 1 : 0

  scope                            = data.azurerm_container_registry.existing[0].id
  role_definition_name             = "AcrPull"
  principal_id                     = azurerm_kubernetes_cluster.this.kubelet_identity[0].object_id
  skip_service_principal_aad_check = true
}

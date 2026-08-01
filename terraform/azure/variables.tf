# Variables for the fuse-snap-aks module (mirrors the stargz-test setup).

variable "subscription_id" {
  description = "Azure subscription ID. Leave empty to use the ARM_SUBSCRIPTION_ID environment variable (azurerm 4.x requires one of the two; auth itself is az CLI)."
  type        = string
  default     = ""
}

variable "location" {
  description = "Azure region for all resources."
  type        = string
  default     = "eastus"
}

variable "resource_group_name" {
  description = "Resource group to create for the test infrastructure."
  type        = string
  default     = "fuse-snap-aks-rg"
}

variable "cluster_name" {
  description = "AKS cluster name."
  type        = string
  default     = "fuse-snap-aks"
}

variable "kubernetes_version" {
  description = "AKS Kubernetes version. pod-snapshotter needs >= 1.30; Ubuntu2404 os_sku needs >= 1.32. Leave empty to take the AKS default for the region."
  type        = string
  default     = "1.33"
}

variable "system_pool_vm_size" {
  description = "VM size for the system node pool (runs kube-system + coordinator/etcd; no NVMe needed)."
  type        = string
  default     = "Standard_D4as_v5"
}

variable "system_pool_count" {
  description = "Node count for the system pool."
  type        = number
  default     = 1
}

variable "nvme_pool_vm_size" {
  description = "VM size for the NVMe user pool. Standard_L8s_v3 = 8 vCPU / 64 GiB / 1x1.92TB local NVMe, discovered by fuse-client node-init."
  type        = string
  default     = "Standard_L8s_v3"
}

variable "nvme_pool_count" {
  description = "Node count for the NVMe pool. Default 0 = scale-to-zero; scale up only while testing (~$0.62/hr/node)."
  type        = number
  default     = 0
}

variable "nvme_pool_max_count" {
  description = "Upper bound for manual scaling of the NVMe pool."
  type        = number
  default     = 3
}

variable "enable_gpu_pool" {
  description = "Create the optional A100 GPU pool for GPU checkpoint/restore tests. Off by default (~$3.7/hr/node when scaled up)."
  type        = bool
  default     = false
}

variable "gpu_pool_vm_size" {
  description = "VM size for the GPU pool. Standard_NC24ads_A100_v4 = 1x A100 80GB."
  type        = string
  default     = "Standard_NC24ads_A100_v4"
}

variable "gpu_pool_count" {
  description = "Node count for the GPU pool. Keep 0 unless actively testing — GPU-style billing discipline."
  type        = number
  default     = 0
}

variable "storage_account_prefix" {
  description = "Prefix for the storage account name (a random suffix is appended; final name must be globally unique, 3-24 lowercase alphanumerics)."
  type        = string
  default     = "fusesnap"
}

variable "premium_blob" {
  description = "Create a Premium block blob storage account (SSD-backed, single-digit-ms latency — Azure's analogue of S3 Express) instead of Standard. Changing this after apply REPLACES the storage account. Default true for maximum cloud-tier performance."
  type        = bool
  default     = true
}

variable "blob_container_name" {
  description = "Blob container for the fuse-client cloud tier (AZURE_CONTAINER_NAME in k8s/secrets.yaml)."
  type        = string
  default     = "fuse-cache"
}

variable "existing_acr_name" {
  description = "Name of an existing ACR to look up and grant AcrPull on (e.g. stargzrepo). Empty = skip. ACR creation is intentionally NOT part of this module."
  type        = string
  default     = ""
}

variable "existing_acr_resource_group" {
  description = "Resource group of the existing ACR (only used when existing_acr_name is set)."
  type        = string
  default     = ""
}

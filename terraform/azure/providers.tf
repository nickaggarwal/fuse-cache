# Provider configuration for the fuse-snap-aks module.
#
# Authentication: Azure CLI (`az login`) — no credentials in code.
# subscription_id is required by azurerm 4.x; pass it via variable or
# ARM_SUBSCRIPTION_ID env var.

terraform {
  required_version = ">= 1.5"

  required_providers {
    azurerm = {
      source = "hashicorp/azurerm"
      # os_sku = "Ubuntu2404" (containerd 2.x, a pod-snapshotter hard
      # requirement) landed in azurerm v4.67.0 — pin at or above it.
      version = "~> 4.67"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.6"
    }
  }
}

provider "azurerm" {
  features {}

  # Uses az CLI auth by default. Empty string lets ARM_SUBSCRIPTION_ID take
  # over if the variable is not set.
  subscription_id = var.subscription_id != "" ? var.subscription_id : null
}

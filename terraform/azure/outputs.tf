output "cluster_name" {
  description = "AKS cluster name."
  value       = azurerm_kubernetes_cluster.this.name
}

output "resource_group_name" {
  description = "Resource group containing all resources."
  value       = azurerm_resource_group.this.name
}

output "kubeconfig_command" {
  description = "Command to merge this cluster into your kubeconfig."
  value       = "az aks get-credentials --resource-group ${azurerm_resource_group.this.name} --name ${azurerm_kubernetes_cluster.this.name}"
}

output "oidc_issuer_url" {
  description = "OIDC issuer URL (for workload identity federation, if used later)."
  value       = azurerm_kubernetes_cluster.this.oidc_issuer_url
}

output "storage_account_name" {
  description = "Storage account name — wire into k8s/secrets.yaml as AZURE_STORAGE_ACCOUNT."
  value       = azurerm_storage_account.this.name
}

output "storage_account_key" {
  description = "Storage account primary key — wire into k8s/secrets.yaml as AZURE_STORAGE_KEY."
  value       = azurerm_storage_account.this.primary_access_key
  sensitive   = true
}

output "blob_container_name" {
  description = "Blob container name — wire into k8s/secrets.yaml as AZURE_CONTAINER_NAME."
  value       = azurerm_storage_container.cache.name
}

output "acr_login_server" {
  description = "Login server of the referenced existing ACR (empty if existing_acr_name was not set)."
  value       = var.existing_acr_name != "" ? data.azurerm_container_registry.existing[0].login_server : ""
}

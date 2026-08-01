output "cluster_name" {
  description = "EKS cluster name."
  value       = aws_eks_cluster.this.name
}

output "cluster_endpoint" {
  description = "EKS API server endpoint."
  value       = aws_eks_cluster.this.endpoint
}

output "oidc_provider_arn" {
  description = "IAM OIDC provider ARN for IRSA role trust policies."
  value       = aws_iam_openid_connect_provider.irsa.arn
}

output "kubeconfig_command" {
  description = "Command to merge this cluster into your kubeconfig."
  value       = "aws eks update-kubeconfig --name ${aws_eks_cluster.this.name} --region ${var.region} --profile ${var.aws_profile}"
}

output "express_bucket_name" {
  description = "S3 Express One Zone directory bucket name (use as -s3-bucket / AWS_S3_BUCKET for the fuse-client cloud tier)."
  value       = aws_s3_directory_bucket.express.bucket
}

output "express_endpoint_url" {
  description = "Zonal S3 Express endpoint. Point the fuse-client S3 endpoint override here for directory-bucket traffic."
  value       = "https://s3express-${var.az_id}.${var.region}.amazonaws.com"
}

output "standard_bucket_name" {
  description = "Standard S3 bucket name (empty unless create_standard_bucket = true)."
  value       = var.create_standard_bucket ? aws_s3_bucket.standard[0].bucket : ""
}

output "region" {
  description = "AWS region (wire into k8s/secrets.yaml as AWS_REGION)."
  value       = var.region
}

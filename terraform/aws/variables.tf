# Variables for the fuse-snap-test EKS module.
# Mirrors k8s/eks-cluster.yaml + k8s/eks-nodegroup.yaml (eksctl configs this
# module replaces).

variable "aws_profile" {
  description = "Named AWS CLI profile used for authentication (no static credentials in code)."
  type        = string
  default     = "pod-snap-test"
}

variable "region" {
  description = "AWS region for the cluster and buckets."
  type        = string
  default     = "us-east-1"
}

variable "cluster_name" {
  description = "EKS cluster name."
  type        = string
  default     = "fuse-snap-test"
}

variable "kubernetes_version" {
  description = "EKS Kubernetes version. pod-snapshotter requires >= 1.30 (ContainerCheckpoint beta); 1.33 AL2023/Ubuntu 24.04 AMIs ship containerd 2.x."
  type        = string
  default     = "1.33"
}

variable "availability_zone" {
  description = "Single AZ for the node group. Must map to var.az_id — S3 Express One Zone latency guarantees only hold intra-AZ, so nodes and the directory bucket must be co-located."
  type        = string
  default     = "us-east-1d"
}

variable "az_id" {
  description = "AZ *ID* (not name) for the S3 Express One Zone directory bucket, e.g. use1-az6. AZ name→ID mapping is account-specific: check with `aws ec2 describe-availability-zones --region us-east-1`."
  type        = string
  default     = "use1-az6"
}

variable "node_instance_type" {
  description = "Node group instance type. m6id.2xlarge = 8 vCPU / 32 GiB / 474 GB instance-store NVMe (discovered by fuse-client node-init) / 12.5 Gbps."
  type        = string
  default     = "m6id.2xlarge"
}

variable "node_desired_size" {
  description = "Desired node count. >= 3 for fuse-client peer replication / thundering-herd tests."
  type        = number
  default     = 3
}

variable "node_min_size" {
  description = "Minimum node count. 0 allows scale-to-zero when the cluster is idle."
  type        = number
  default     = 0
}

variable "node_max_size" {
  description = "Maximum node count."
  type        = number
  default     = 3
}

variable "node_ami_family" {
  description = "Node AMI family: AL2023_x86_64_STANDARD or UBUNTU (Ubuntu 24.04 via EKS-optimized Ubuntu AMI). Both ship containerd >= 2.0 on EKS 1.33, a pod-snapshotter hard requirement. Ubuntu additionally gives apt/PPA CRIU installs, matching pod-snapshotter's verified AKS environment."
  type        = string
  default     = "AL2023_x86_64_STANDARD"

  validation {
    condition     = contains(["AL2023_x86_64_STANDARD", "UBUNTU"], var.node_ami_family)
    error_message = "node_ami_family must be AL2023_x86_64_STANDARD or UBUNTU."
  }
}

variable "node_volume_size" {
  description = "EBS root volume size in GiB. The cache lives on instance-store NVMe, not this volume."
  type        = number
  default     = 80
}

variable "express_bucket_base" {
  description = "Base name of the S3 Express One Zone directory bucket. Final name is <base>--<az_id>--x-s3 (mandatory directory-bucket naming pattern)."
  type        = string
  default     = "fuse-snap-cache"
}

variable "create_standard_bucket" {
  description = "Also create a standard (regional) S3 bucket for comparing S3 Express vs standard S3 as the fuse-client cloud tier. Off by default."
  type        = bool
  default     = false
}

variable "standard_bucket_name" {
  description = "Name of the optional standard S3 bucket (only used when create_standard_bucket = true)."
  type        = string
  default     = "fuse-snap-cache-standard"
}

variable "enable_transfer_acceleration" {
  description = "Enable S3 Transfer Acceleration on the standard bucket. Only helps for clients far from the bucket region; intra-region EKS traffic gains nothing and pays extra. Off by default."
  type        = bool
  default     = false
}

# EKS test cluster for fuse-client + pod-snapshotter.
# Terraform port of k8s/eks-cluster.yaml + k8s/eks-nodegroup.yaml (eksctl).
#
# Requirements driving the choices:
#   - pod-snapshotter: k8s >= 1.30, containerd >= 2.0 (EKS 1.33 AL2023 or
#     Ubuntu 24.04 AMIs), CRIU installed by its nodeSetup DaemonSet,
#     privileged workloads.
#   - fuse-client: local NVMe per node (m6id.* instance store) discovered by
#     node-init; >= 3 nodes for peer replication / herd tests.
#   - S3 Express One Zone: the node group is pinned to ONE AZ that must be
#     the same zone as the directory bucket — single-digit-ms first byte
#     only holds intra-AZ.

data "aws_availability_zones" "available" {
  state = "available"
}

locals {
  # EKS control planes require subnets in >= 2 AZs even though the node
  # group is pinned to one. Pick any second AZ for the control-plane-only
  # subnet.
  secondary_az = [
    for az in data.aws_availability_zones.available.names :
    az if az != var.availability_zone
  ][0]

  use_ubuntu = var.node_ami_family == "UBUNTU"

  express_bucket_name = "${var.express_bucket_base}--${var.az_id}--x-s3"
}

# ---------------------------------------------------------------------------
# VPC — eksctl creates a dedicated VPC; mirror that with a minimal public
# VPC (privateNetworking: false in the eksctl config → public subnets).
# ---------------------------------------------------------------------------

resource "aws_vpc" "this" {
  cidr_block           = "10.42.0.0/16"
  enable_dns_support   = true
  enable_dns_hostnames = true

  tags = { Name = "${var.cluster_name}-vpc" }
}

resource "aws_internet_gateway" "this" {
  vpc_id = aws_vpc.this.id

  tags = { Name = "${var.cluster_name}-igw" }
}

# Subnet in the pinned AZ — all nodes land here (intra-AZ with the S3
# Express directory bucket).
resource "aws_subnet" "nodes" {
  vpc_id                  = aws_vpc.this.id
  cidr_block              = "10.42.0.0/20"
  availability_zone       = var.availability_zone
  map_public_ip_on_launch = true

  tags = {
    Name                                        = "${var.cluster_name}-nodes"
    "kubernetes.io/cluster/${var.cluster_name}" = "shared"
    "kubernetes.io/role/elb"                    = "1"
  }
}

# Control-plane-only subnet in a second AZ (EKS API requirement).
resource "aws_subnet" "secondary" {
  vpc_id                  = aws_vpc.this.id
  cidr_block              = "10.42.16.0/20"
  availability_zone       = local.secondary_az
  map_public_ip_on_launch = true

  tags = {
    Name                                        = "${var.cluster_name}-secondary"
    "kubernetes.io/cluster/${var.cluster_name}" = "shared"
    "kubernetes.io/role/elb"                    = "1"
  }
}

resource "aws_route_table" "public" {
  vpc_id = aws_vpc.this.id

  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.this.id
  }

  tags = { Name = "${var.cluster_name}-public" }
}

resource "aws_route_table_association" "nodes" {
  subnet_id      = aws_subnet.nodes.id
  route_table_id = aws_route_table.public.id
}

resource "aws_route_table_association" "secondary" {
  subnet_id      = aws_subnet.secondary.id
  route_table_id = aws_route_table.public.id
}

# S3 Gateway VPC endpoint — performance/cost: keeps all S3 (including
# S3 Express One Zone) traffic on the AWS private backbone instead of
# routing via the internet gateway. Free, no bandwidth charge, and lower
# and more consistent latency for the fuse-client cloud tier.
resource "aws_vpc_endpoint" "s3" {
  vpc_id            = aws_vpc.this.id
  service_name      = "com.amazonaws.${var.region}.s3"
  vpc_endpoint_type = "Gateway"
  route_table_ids   = [aws_route_table.public.id]

  tags = { Name = "${var.cluster_name}-s3-gw" }
}

# S3 Express One Zone has its own gateway endpoint service; add it too so
# directory-bucket traffic (s3express-<az-id> zonal endpoint) also stays
# on the backbone.
resource "aws_vpc_endpoint" "s3express" {
  vpc_id            = aws_vpc.this.id
  service_name      = "com.amazonaws.${var.region}.s3express"
  vpc_endpoint_type = "Gateway"
  route_table_ids   = [aws_route_table.public.id]

  tags = { Name = "${var.cluster_name}-s3express-gw" }
}

# ---------------------------------------------------------------------------
# IAM — cluster role, node role
# ---------------------------------------------------------------------------

data "aws_iam_policy_document" "cluster_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["eks.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "cluster" {
  name               = "${var.cluster_name}-cluster-role"
  assume_role_policy = data.aws_iam_policy_document.cluster_assume.json
}

resource "aws_iam_role_policy_attachment" "cluster_policy" {
  role       = aws_iam_role.cluster.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonEKSClusterPolicy"
}

data "aws_iam_policy_document" "node_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["ec2.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "node" {
  name               = "${var.cluster_name}-node-role"
  assume_role_policy = data.aws_iam_policy_document.node_assume.json
}

resource "aws_iam_role_policy_attachment" "node_worker" {
  role       = aws_iam_role.node.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonEKSWorkerNodePolicy"
}

resource "aws_iam_role_policy_attachment" "node_cni" {
  role       = aws_iam_role.node.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonEKS_CNI_Policy"
}

resource "aws_iam_role_policy_attachment" "node_ecr" {
  role       = aws_iam_role.node.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonEC2ContainerRegistryReadOnly"
}

# eksctl config had withAddonPolicies.ebs: true.
resource "aws_iam_role_policy_attachment" "node_ebs_csi" {
  role       = aws_iam_role.node.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonEBSCSIDriverPolicy"
}

# ---------------------------------------------------------------------------
# EKS cluster + IRSA (OIDC provider) — eksctl `iam.withOIDC: true`
# ---------------------------------------------------------------------------

resource "aws_eks_cluster" "this" {
  name     = var.cluster_name
  version  = var.kubernetes_version
  role_arn = aws_iam_role.cluster.arn

  vpc_config {
    subnet_ids              = [aws_subnet.nodes.id, aws_subnet.secondary.id]
    endpoint_public_access  = true
    endpoint_private_access = false
  }

  access_config {
    authentication_mode                         = "API_AND_CONFIG_MAP"
    bootstrap_cluster_creator_admin_permissions = true
  }

  depends_on = [aws_iam_role_policy_attachment.cluster_policy]
}

# Core addons. GOTCHA (hit live, 2026-08-01): creating the control plane and
# node group without these leaves every node NotReady with "cni plugin not
# initialized" and an empty kube-system — eksctl normally installs them, but
# an interrupted run (or plain aws_eks_cluster) does not. vpc-cni/kube-proxy
# must exist before nodes join; coredns needs nodes to schedule on, and the
# EBS CSI driver backs any PVC-based workloads (etcd, etc.).
resource "aws_eks_addon" "core" {
  for_each = toset(["vpc-cni", "kube-proxy", "coredns", "aws-ebs-csi-driver"])

  cluster_name                = aws_eks_cluster.this.name
  addon_name                  = each.key
  resolve_conflicts_on_create = "OVERWRITE"
  resolve_conflicts_on_update = "OVERWRITE"

  # coredns and the EBS CSI controller need schedulable nodes.
  depends_on = [aws_eks_node_group.nvme]
}

data "tls_certificate" "oidc" {
  url = aws_eks_cluster.this.identity[0].oidc[0].issuer
}

resource "aws_iam_openid_connect_provider" "irsa" {
  url             = aws_eks_cluster.this.identity[0].oidc[0].issuer
  client_id_list  = ["sts.amazonaws.com"]
  thumbprint_list = [data.tls_certificate.oidc.certificates[0].sha1_fingerprint]
}

# ---------------------------------------------------------------------------
# Node group — one managed group, pinned to var.availability_zone.
#
# AMI family:
#   - AL2023_x86_64_STANDARD (default): native managed-node-group AMI type,
#     containerd 2.x on EKS 1.33.
#   - UBUNTU: EKS managed node groups have no first-class Ubuntu AMI type,
#     so we switch to ami_type = CUSTOM with Canonical's EKS-optimized
#     Ubuntu 24.04 AMI (via their published SSM parameter) and a launch
#     template that runs /etc/eks/bootstrap.sh. Ubuntu 24.04 ships
#     containerd 2.x and lets pod-snapshotter's nodeSetup DaemonSet install
#     CRIU from ppa:criu/ppa, matching its verified AKS environment.
# ---------------------------------------------------------------------------

data "aws_ssm_parameter" "ubuntu_eks_ami" {
  count = local.use_ubuntu ? 1 : 0
  # Canonical's published parameter for the EKS-optimized Ubuntu 24.04 AMI.
  # If this path 404s for a new k8s version, list available ones with:
  #   aws ssm get-parameters-by-path --path /aws/service/canonical/ubuntu/eks/24.04 --recursive
  name = "/aws/service/canonical/ubuntu/eks/24.04/${var.kubernetes_version}/stable/current/amd64/hvm/ebs-gp3/ami-id"
}

resource "aws_launch_template" "ubuntu" {
  count         = local.use_ubuntu ? 1 : 0
  name_prefix   = "${var.cluster_name}-ubuntu-"
  image_id      = data.aws_ssm_parameter.ubuntu_eks_ami[0].value
  instance_type = var.node_instance_type

  block_device_mappings {
    device_name = "/dev/sda1"
    ebs {
      volume_size           = var.node_volume_size
      volume_type           = "gp3"
      delete_on_termination = true
    }
  }

  metadata_options {
    http_tokens                 = "required"
    http_put_response_hop_limit = 2 # pods on the node need IMDS (one extra hop)
  }

  # With ami_type = CUSTOM, EKS does not inject bootstrap user data — the
  # Ubuntu EKS AMI's bootstrap.sh must be called explicitly.
  user_data = base64encode(<<-EOT
    #!/bin/bash
    set -euo pipefail
    /etc/eks/bootstrap.sh ${var.cluster_name} \
      --apiserver-endpoint ${aws_eks_cluster.this.endpoint} \
      --b64-cluster-ca ${aws_eks_cluster.this.certificate_authority[0].data} \
      --kubelet-extra-args '--node-labels=workload=fuse-snap-test'
  EOT
  )

  tag_specifications {
    resource_type = "instance"
    tags = {
      Name = "${var.cluster_name}-node"
    }
  }
}

resource "aws_eks_node_group" "nvme" {
  cluster_name    = aws_eks_cluster.this.name
  node_group_name = local.use_ubuntu ? "nvme-ubuntu" : "nvme-pool"
  node_role_arn   = aws_iam_role.node.arn

  # Single-AZ on purpose: keep all instance-store NVMe + S3 Express traffic
  # inside var.availability_zone / var.az_id.
  subnet_ids = [aws_subnet.nodes.id]

  scaling_config {
    desired_size = var.node_desired_size
    min_size     = var.node_min_size
    max_size     = var.node_max_size
  }

  ami_type = local.use_ubuntu ? "CUSTOM" : var.node_ami_family

  # With a launch template, instance type / disk / AMI come from the LT.
  instance_types = local.use_ubuntu ? null : [var.node_instance_type]
  disk_size      = local.use_ubuntu ? null : var.node_volume_size

  dynamic "launch_template" {
    for_each = local.use_ubuntu ? [1] : []
    content {
      id      = aws_launch_template.ubuntu[0].id
      version = aws_launch_template.ubuntu[0].latest_version
    }
  }

  labels = {
    workload = "fuse-snap-test"
  }

  update_config {
    max_unavailable = 1
  }

  depends_on = [
    aws_iam_role_policy_attachment.node_worker,
    aws_iam_role_policy_attachment.node_cni,
    aws_iam_role_policy_attachment.node_ecr,
  ]

  lifecycle {
    ignore_changes = [scaling_config[0].desired_size] # allow manual scale-to-zero
  }
}

# ---------------------------------------------------------------------------
# S3 Express One Zone directory bucket (fuse-client Tier 3, low latency).
# Name pattern is mandated by AWS: <base>--<az-id>--x-s3.
# ---------------------------------------------------------------------------

resource "aws_s3_directory_bucket" "express" {
  bucket = local.express_bucket_name

  location {
    name = var.az_id
    type = "AvailabilityZone"
  }

  data_redundancy = "SingleAvailabilityZone"
  type            = "Directory"

  force_destroy = true # test infra: allow destroy with objects present
}

# Optional standard S3 bucket for Express-vs-standard comparison runs.
resource "aws_s3_bucket" "standard" {
  count  = var.create_standard_bucket ? 1 : 0
  bucket = var.standard_bucket_name

  force_destroy = true
}

# Transfer Acceleration for the standard bucket (var-gated): only helps
# when clients are far from the bucket region — for intra-region EKS
# traffic it adds cost for no gain, hence off by default. The real
# standard-S3 performance levers are client-side and already implemented
# in fuse-client: parallel chunk upload/download (-s3-* concurrency
# flags), 8MB chunking, and key-prefix spreading (3,500 PUT / 5,500 GET
# per second *per prefix*).
resource "aws_s3_bucket_accelerate_configuration" "standard" {
  count  = var.create_standard_bucket && var.enable_transfer_acceleration ? 1 : 0
  bucket = aws_s3_bucket.standard[0].id
  status = "Enabled"
}

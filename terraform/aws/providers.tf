# Provider configuration for the fuse-snap-test EKS module.
#
# Authentication: uses a named AWS CLI profile (no credentials in code).
# Configure the profile first:
#   aws configure --profile pod-snap-test

terraform {
  required_version = ">= 1.5"

  required_providers {
    aws = {
      source = "hashicorp/aws"
      # aws_s3_directory_bucket (S3 Express One Zone) needs >= 5.31;
      # pin well above that but stay on the 5.x line.
      version = "~> 5.70"
    }
    tls = {
      source  = "hashicorp/tls"
      version = "~> 4.0"
    }
  }
}

provider "aws" {
  region  = var.region
  profile = var.aws_profile

  default_tags {
    tags = {
      Project   = "fuse-snap-test"
      ManagedBy = "terraform"
    }
  }
}

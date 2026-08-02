# Terraform — fuse-client + pod-snapshotter test infrastructure

Two independent root modules provisioning the test clusters for the
fuse-client distributed cache and pod-snapshotter checkpoint/restore stack:

| Module | What it creates |
|---|---|
| `aws/` | EKS cluster (`fuse-snap-test`), one single-AZ m6id.2xlarge node group (instance-store NVMe), IRSA/OIDC, an S3 Express One Zone directory bucket, optional standard S3 bucket |
| `azure/` | AKS cluster (`fuse-snap-aks`), system pool + scale-to-zero L8s_v3 NVMe pool (Ubuntu 24.04), optional A100 GPU pool, storage account + blob container |

Requirements they encode (see `../CLAUDE.md` and
`../../pod-snapshotter/docs/prerequisites.md`):

- Kubernetes >= 1.30 and **containerd >= 2.0** — hard pod-snapshotter
  requirements. That means EKS 1.33 AL2023/Ubuntu 24.04 AMIs and AKS
  **Ubuntu2404** node pools (Ubuntu 22.04 AKS pools ship containerd 1.7,
  which does not implement `CheckpointContainer`).
- Local NVMe on every worker (m6id instance store / L-series) for the
  fuse-client Tier 1 cache, discovered by `node-init`.
- CRIU is installed by pod-snapshotter's `nodeSetup` DaemonSet at deploy
  time — not Terraform's job.
- No credentials in code: AWS uses a named CLI profile, Azure uses `az` CLI
  auth.

## Prerequisites

- Terraform >= 1.5 (or OpenTofu >= 1.6)
- **AWS module**: AWS CLI with a configured profile
  (`aws configure --profile pod-snap-test`) that can create VPC/EKS/IAM/S3
- **Azure module**: `az login` done; subscription ID exported as
  `ARM_SUBSCRIPTION_ID` (or set the `subscription_id` variable)
- `kubectl` for post-apply verification

## Usage

Each module is a self-contained root — run them independently:

```bash
cd terraform/aws        # or terraform/azure
cp terraform.tfvars.example terraform.tfvars   # edit as needed
terraform init
terraform plan
terraform apply
```

State is local by default (no backend configured). For anything beyond
throwaway testing, add an S3/azurerm backend block.

### AWS specifics

- The node group is **pinned to one AZ** (`availability_zone`, default
  `us-east-1d`) and the S3 Express One Zone directory bucket is created in
  the matching **AZ ID** (`az_id`, default `use1-az6`). The AZ-name → AZ-ID
  mapping is *per-account* — verify yours before applying:

  ```bash
  aws ec2 describe-availability-zones --region us-east-1 \
    --query 'AvailabilityZones[].[ZoneName,ZoneId]' --output table
  ```

  S3 Express's single-digit-ms latency only holds intra-AZ; a mismatch
  silently turns every cloud-tier read into a cross-AZ hop.
- Directory bucket name follows the mandatory pattern
  `<base>--<az-id>--x-s3`, e.g. `fuse-snap-cache--use1-az6--x-s3`.
- The zonal endpoint is emitted as the `express_endpoint_url` output
  (`https://s3express-<az-id>.<region>.amazonaws.com`).
- Set `create_standard_bucket = true` to also get a normal regional bucket
  for Express-vs-standard comparison runs.
- `node_ami_family = "UBUNTU"` switches the node group to Canonical's
  EKS-optimized Ubuntu 24.04 AMI via a custom launch template (managed node
  groups have no first-class Ubuntu AMI type). Default is AL2023, which
  also ships containerd 2.x on EKS 1.33.
- **Storage performance**: Gateway VPC endpoints for both `s3` and
  `s3express` are created automatically (free) so all cloud-tier traffic
  stays on the AWS backbone. Beyond that, S3 Express performance is
  client-side: use the zonal `express_endpoint_url`, keep the fuse-client
  `-s3-*` upload/download concurrency high, and rely on its 8MB parallel
  chunking. `enable_transfer_acceleration` exists for the standard bucket
  but only helps cross-region clients — leave it off for in-cluster tests.

Gotchas validated live on 2026-08-01 (all encoded in this module — listed so
nobody re-discovers them by hand):

- **Core addons are not optional** (`aws_eks_addon.core`): without vpc-cni /
  kube-proxy / coredns every node sits NotReady with "cni plugin not
  initialized" and kube-system is empty. eksctl installs them implicitly;
  raw `aws_eks_cluster` + node group does not.
- **The default StorageClass on a fresh cluster is `gp2`** (in-tree
  provisioner name, served by the EBS CSI addon). Manifests written for AKS
  (`storageClassName: managed-csi`) pend forever — the k8s/eks overlay uses
  `gp2` or emptyDir. The EBS CSI addon also needs the
  `AmazonEBSCSIDriverPolicy` on the node role (this module attaches it).
- **S3 Express + aws-sdk-go v1 rejects Content-MD5** (501 NotImplemented on
  every upload): fixed in fuse-client `f154904` by setting
  `S3DisableContentMD5Validation` for `--x-s3` buckets. Any client image
  older than that cannot write to a directory bucket.
- **S3 Express is not offered in every AZ** — `use1-az1` rejects directory
  bucket creation, `use1-az6` works. Verify with a trial create before
  pinning the AZ.
- **Fresh-account on-demand vCPU quota is 16** (L-1216C47A): 3×i7ie.xlarge
  (12 vCPU) fits; anything bigger needs a service-quota increase first.
- **Chunk objects accumulate in the directory bucket forever** — fuse-cache
  deletes cloud objects only on explicit file delete, so benchmark/test
  churn builds up storage cost (mountpoint-s3's shared-cache docs push the
  same responsibility to bucket lifecycle). S3 Express directory buckets
  support lifecycle expiration: set an expiration rule on test prefixes, or
  purge manually after benchmark campaigns. Also: anyone with write access
  to the bucket can poison cache content — dedicate the bucket to the
  cache, same account, no shared writers (chunk content checksums are on
  the roadmap).

### Azure specifics

- All pools use `os_sku = "Ubuntu2404"` (containerd 2.x). This needs
  **azurerm provider >= 4.67.0** (pinned) and **AKS Kubernetes >= 1.32**.
  If you must use an older provider, create the pool with the CLI instead:

  ```bash
  az aks nodepool add -g fuse-snap-aks-rg --cluster-name fuse-snap-aks \
    -n nvme --node-count 0 --node-vm-size Standard_L8s_v3 \
    --os-sku Ubuntu2404 --mode User
  ```

- No ACR is created — images live in the existing `stargzrepo` registry.
  Set `existing_acr_name`/`existing_acr_resource_group` to have Terraform
  look it up and grant the cluster kubelet `AcrPull` on it.
- The NVMe and GPU pools have `ignore_changes = [node_count]`, so scaling
  with `az aks nodepool scale` won't be reverted on the next apply.
- **Storage performance**: `premium_blob = true` (default) provisions a
  **Premium block blob** storage account — SSD-backed, single-digit-ms
  latency, Azure's closest analogue to S3 Express One Zone and the right
  target for fuse-client cloud-tier latency tests. Set it to `false` for a
  Standard StorageV2 account (~8x cheaper/GB) for comparison runs; note
  flipping the flag **replaces** the account (tier/kind are immutable).
  Remaining throughput levers are client-side: fuse-client's
  `-azure-*` concurrency flags and 8MB parallel chunking.

## Connecting kubectl

Both modules output the exact command:

```bash
terraform output -raw kubeconfig_command
# AWS:   aws eks update-kubeconfig --name fuse-snap-test --region us-east-1 --profile pod-snap-test
# Azure: az aks get-credentials --resource-group fuse-snap-aks-rg --name fuse-snap-aks
```

## Wiring outputs into the k8s manifests

`k8s/secrets.yaml` expects cloud credentials as literal env values. Populate
it from the Terraform outputs (never commit the filled-in file):

**AWS / S3 Express** — the bucket accepts your normal IAM credentials
(profile `pod-snap-test`); the fuse-client pods need:

```bash
cd terraform/aws
terraform output -raw express_bucket_name    # bucket for the client -s3-bucket flag / configmap
terraform output -raw region                 # AWS_REGION
terraform output -raw express_endpoint_url   # S3 endpoint override for directory-bucket traffic
# AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY: create a dedicated IAM user or
# use IRSA (oidc_provider_arn output) — this module intentionally does not
# mint or print static access keys.
```

For a quick test with static keys:

```bash
kubectl -n fuse-system create secret generic fuse-secrets \
  --from-literal=AWS_ACCESS_KEY_ID=<key-id> \
  --from-literal=AWS_SECRET_ACCESS_KEY=<secret> \
  --from-literal=AWS_REGION=$(terraform output -raw region)
```

**Azure / Blob**:

```bash
cd terraform/azure
kubectl -n fuse-system create secret generic fuse-secrets \
  --from-literal=AZURE_STORAGE_ACCOUNT=$(terraform output -raw storage_account_name) \
  --from-literal=AZURE_STORAGE_KEY=$(terraform output -raw storage_account_key) \
  --from-literal=AZURE_CONTAINER_NAME=$(terraform output -raw blob_container_name)
```

(`storage_account_key` is marked sensitive — it only prints with
`terraform output -raw`.)

Then deploy as usual: `kubectl apply -f ../../k8s/`.

## Cost notes

On-demand, us-east-1 / eastus ballpark:

| Resource | Rate | At defaults |
|---|---|---|
| EKS m6id.2xlarge x3 | ~$0.47/hr each | **~$1.42/hr** (+$0.10/hr EKS control plane) |
| AKS Standard_D4as_v5 system x1 | ~$0.17/hr | ~$0.17/hr |
| AKS Standard_L8s_v3 NVMe pool | ~$0.62/hr each | **$0 — defaults to 0 nodes** |
| AKS Standard_NC24ads_A100_v4 GPU pool | ~$3.7/hr each | **$0 — gated off, 0 nodes** |
| S3 Express One Zone | storage + request pricing | negligible for test data |
| Azure Premium block blob (default) | ~$0.15/GB-mo vs ~$0.018 Standard | negligible for test data |

Discipline for the expensive pools:

- **Scale to zero the moment a test run ends.** The L8s_v3 and especially
  the A100 pool are created at `node_count = 0` on purpose:

  ```bash
  az aks nodepool scale -g fuse-snap-aks-rg --cluster-name fuse-snap-aks -n nvme --node-count 3   # start testing
  az aks nodepool scale -g fuse-snap-aks-rg --cluster-name fuse-snap-aks -n nvme --node-count 0   # done
  ```

- EKS: the node group has `min_size = 0`; scale desired down between runs
  (`aws eks update-nodegroup-config ... --scaling-config desiredSize=0`,
  drift-tolerated via `ignore_changes`).
- `terraform destroy` per module when the environment is no longer needed;
  both buckets/containers are destroyable with objects present
  (`force_destroy`) since this is test data.

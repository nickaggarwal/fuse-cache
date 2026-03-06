#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  bootstrap-cloud-env.sh [options]

Options:
  --install-tools               Install required tools on Ubuntu via apt
  --env-file <path>              Output env file path (default: .env.cloud)
  --azure-subscription <name>    Azure subscription name or ID to select
  --aks-rg <name>                AKS resource group
  --aks-name <name>              AKS cluster name
  --eks-name <name>              EKS cluster name
  --aws-region <name>            AWS region (default: us-east-1)
  --aws-profile <name>           AWS profile to use for aws cli calls
  --skip-context-update          Skip az/aws kubecontext setup calls
  -h, --help                     Show this help text

Examples:
  ./scripts/ops/bootstrap-cloud-env.sh \
    --azure-subscription PAY-AS-YOU-GO \
    --aks-rg stargz-test_group \
    --aks-name stargz-test \
    --eks-name stargz-test \
    --aws-region us-east-1

  ./scripts/ops/bootstrap-cloud-env.sh --env-file .env.cloud.local --skip-context-update

  ./scripts/ops/bootstrap-cloud-env.sh --install-tools --skip-context-update
EOF
}

need_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "ERROR: missing required command: $1" >&2
    exit 1
  fi
}

need_sudo() {
  if command -v sudo >/dev/null 2>&1; then
    echo "sudo"
  else
    echo ""
  fi
}

install_tools_ubuntu_apt() {
  if ! command -v apt-get >/dev/null 2>&1; then
    echo "ERROR: --install-tools is only supported on Ubuntu/Debian (apt-get)." >&2
    exit 1
  fi
  if ! command -v curl >/dev/null 2>&1; then
    echo "ERROR: curl is required to install tools. Install curl first." >&2
    exit 1
  fi
  if ! command -v gpg >/dev/null 2>&1; then
    echo "ERROR: gpg is required to install tools. Install gnupg first." >&2
    exit 1
  fi

  SUDO="$(need_sudo)"
  export DEBIAN_FRONTEND=noninteractive

  ${SUDO} apt-get update
  ${SUDO} apt-get install -y ca-certificates curl apt-transport-https gnupg lsb-release software-properties-common jq

  # Azure CLI
  if ! command -v az >/dev/null 2>&1; then
    ${SUDO} mkdir -p /etc/apt/keyrings
    curl -fsSL https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor | ${SUDO} tee /etc/apt/keyrings/microsoft.gpg >/dev/null
    AZ_DIST="$(lsb_release -cs)"
    AZ_REPO="deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/microsoft.gpg] https://packages.microsoft.com/repos/azure-cli/ ${AZ_DIST} main"
    echo "${AZ_REPO}" | ${SUDO} tee /etc/apt/sources.list.d/azure-cli.list >/dev/null
    ${SUDO} apt-get update
    ${SUDO} apt-get install -y azure-cli
  fi

  # AWS CLI
  if ! command -v aws >/dev/null 2>&1; then
    ${SUDO} apt-get install -y awscli
  fi

  # kubectl
  if ! command -v kubectl >/dev/null 2>&1; then
    ${SUDO} mkdir -p /etc/apt/keyrings
    curl -fsSL https://pkgs.k8s.io/core:/stable:/v1.32/deb/Release.key | gpg --dearmor | ${SUDO} tee /etc/apt/keyrings/kubernetes-apt-keyring.gpg >/dev/null
    echo "deb [signed-by=/etc/apt/keyrings/kubernetes-apt-keyring.gpg] https://pkgs.k8s.io/core:/stable:/v1.32/deb/ /" | ${SUDO} tee /etc/apt/sources.list.d/kubernetes.list >/dev/null
    ${SUDO} apt-get update
    ${SUDO} apt-get install -y kubectl
  fi

  # Helm
  if ! command -v helm >/dev/null 2>&1; then
    curl -fsSL https://baltocdn.com/helm/signing.asc | gpg --dearmor | ${SUDO} tee /usr/share/keyrings/helm.gpg >/dev/null
    echo "deb [arch=$(dpkg --print-architecture) signed-by=/usr/share/keyrings/helm.gpg] https://baltocdn.com/helm/stable/debian/ all main" | ${SUDO} tee /etc/apt/sources.list.d/helm-stable-debian.list >/dev/null
    ${SUDO} apt-get update
    ${SUDO} apt-get install -y helm
  fi
}

ENV_FILE=".env.cloud"
AZ_SUBSCRIPTION=""
AKS_RG=""
AKS_NAME=""
EKS_NAME=""
AWS_REGION="us-east-1"
AWS_PROFILE="${AWS_PROFILE:-}"
SKIP_CONTEXT_UPDATE="false"
INSTALL_TOOLS="false"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --install-tools)
      INSTALL_TOOLS="true"
      shift
      ;;
    --env-file)
      ENV_FILE="${2:?missing value for --env-file}"
      shift 2
      ;;
    --azure-subscription)
      AZ_SUBSCRIPTION="${2:?missing value for --azure-subscription}"
      shift 2
      ;;
    --aks-rg)
      AKS_RG="${2:?missing value for --aks-rg}"
      shift 2
      ;;
    --aks-name)
      AKS_NAME="${2:?missing value for --aks-name}"
      shift 2
      ;;
    --eks-name)
      EKS_NAME="${2:?missing value for --eks-name}"
      shift 2
      ;;
    --aws-region)
      AWS_REGION="${2:?missing value for --aws-region}"
      shift 2
      ;;
    --aws-profile)
      AWS_PROFILE="${2:?missing value for --aws-profile}"
      shift 2
      ;;
    --skip-context-update)
      SKIP_CONTEXT_UPDATE="true"
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "ERROR: unknown argument: $1" >&2
      usage
      exit 1
      ;;
  esac
done

if [[ "$INSTALL_TOOLS" == "true" ]]; then
  install_tools_ubuntu_apt
fi

need_cmd az
need_cmd aws
need_cmd kubectl
need_cmd helm
need_cmd jq

if [[ -n "$AZ_SUBSCRIPTION" ]]; then
  az account set --subscription "$AZ_SUBSCRIPTION"
fi

if ! AZ_ACCOUNT_JSON="$(az account show -o json 2>/dev/null)"; then
  echo "ERROR: Azure login not found. Run: az login" >&2
  exit 1
fi
AZ_SUBSCRIPTION_NAME="$(printf '%s' "$AZ_ACCOUNT_JSON" | jq -r '.name')"
AZ_SUBSCRIPTION_ID="$(printf '%s' "$AZ_ACCOUNT_JSON" | jq -r '.id')"
AZ_TENANT_ID="$(printf '%s' "$AZ_ACCOUNT_JSON" | jq -r '.tenantId')"

AWS_CMD=(aws)
if [[ -n "$AWS_PROFILE" ]]; then
  AWS_CMD+=(--profile "$AWS_PROFILE")
fi
AWS_CMD+=(--region "$AWS_REGION")

if ! AWS_ID_JSON="$("${AWS_CMD[@]}" sts get-caller-identity --output json 2>/dev/null)"; then
  echo "ERROR: AWS login not found. Configure credentials for aws cli first." >&2
  exit 1
fi
AWS_ACCOUNT_ID="$(printf '%s' "$AWS_ID_JSON" | jq -r '.Account')"
AWS_ARN="$(printf '%s' "$AWS_ID_JSON" | jq -r '.Arn')"

AKS_CONTEXT=""
EKS_CONTEXT=""

if [[ "$SKIP_CONTEXT_UPDATE" != "true" ]]; then
  if [[ -n "$AKS_RG" && -n "$AKS_NAME" ]]; then
    requested_aks_context="aks-${AKS_NAME}"
    az aks get-credentials \
      --resource-group "$AKS_RG" \
      --name "$AKS_NAME" \
      --admin \
      --context "$requested_aks_context" \
      --overwrite-existing >/dev/null
    if kubectl config get-contexts -o name | grep -Fxq "${requested_aks_context}-admin"; then
      AKS_CONTEXT="${requested_aks_context}-admin"
    elif kubectl config get-contexts -o name | grep -Fxq "$requested_aks_context"; then
      AKS_CONTEXT="$requested_aks_context"
    else
      AKS_CONTEXT="${requested_aks_context}-admin"
    fi
  fi

  if [[ -n "$EKS_NAME" ]]; then
    if [[ -z "$AWS_PROFILE" ]]; then
      aws eks update-kubeconfig \
        --name "$EKS_NAME" \
        --region "$AWS_REGION" \
        --alias "eks-${EKS_NAME}" >/dev/null
    else
      aws eks update-kubeconfig \
        --name "$EKS_NAME" \
        --region "$AWS_REGION" \
        --profile "$AWS_PROFILE" \
        --alias "eks-${EKS_NAME}" >/dev/null
    fi
    EKS_CONTEXT="eks-${EKS_NAME}"
  fi
fi

cat > "$ENV_FILE" <<EOF
# Generated by scripts/ops/bootstrap-cloud-env.sh on $(date -u +"%Y-%m-%dT%H:%M:%SZ")
export CLOUD_AZURE_SUBSCRIPTION_NAME="${AZ_SUBSCRIPTION_NAME}"
export CLOUD_AZURE_SUBSCRIPTION_ID="${AZ_SUBSCRIPTION_ID}"
export CLOUD_AZURE_TENANT_ID="${AZ_TENANT_ID}"

export CLOUD_AWS_ACCOUNT_ID="${AWS_ACCOUNT_ID}"
export CLOUD_AWS_ARN="${AWS_ARN}"
export CLOUD_AWS_REGION="${AWS_REGION}"
export AWS_REGION="${AWS_REGION}"
export AWS_DEFAULT_REGION="${AWS_REGION}"
$(if [[ -n "$AWS_PROFILE" ]]; then printf 'export AWS_PROFILE="%s"\n' "$AWS_PROFILE"; fi)

export CLOUD_AKS_RESOURCE_GROUP="${AKS_RG}"
export CLOUD_AKS_CLUSTER="${AKS_NAME}"
export CLOUD_AKS_CONTEXT="${AKS_CONTEXT}"
export CLOUD_AKS_NAMESPACE="fuse-system-aztest"

export CLOUD_EKS_CLUSTER="${EKS_NAME}"
export CLOUD_EKS_CONTEXT="${EKS_CONTEXT}"
export CLOUD_EKS_NAMESPACE="fuse-system-awstest"

export CLOUD_HELM_CHART="charts/fuse-cache"
export CLOUD_HELM_RELEASE_AKS="fuse-cache-aztest"
export CLOUD_HELM_RELEASE_EKS="fuse-cache-awstest"
export CLOUD_IMAGE_REPO="stargzrepo.azurecr.io/fuse-client"
export CLOUD_ACR_NAME="stargzrepo"
EOF

echo "Cloud environment initialized."
echo
echo "Azure:"
echo "  subscription: ${AZ_SUBSCRIPTION_NAME} (${AZ_SUBSCRIPTION_ID})"
echo "  tenant:       ${AZ_TENANT_ID}"
echo
echo "AWS:"
echo "  account:      ${AWS_ACCOUNT_ID}"
echo "  principal:    ${AWS_ARN}"
echo "  region:       ${AWS_REGION}"
if [[ -n "$AWS_PROFILE" ]]; then
  echo "  profile:      ${AWS_PROFILE}"
fi
echo
echo "Wrote env file: ${ENV_FILE}"
echo
echo "Next:"
echo "  source ${ENV_FILE}"
if [[ -n "$AKS_CONTEXT" ]]; then
  echo "  kubectl config use-context ${AKS_CONTEXT}"
  echo "  ./scripts/ops/test-smart-read.sh \$CLOUD_AKS_NAMESPACE 1024"
fi
if [[ -n "$EKS_CONTEXT" ]]; then
  echo "  kubectl config use-context ${EKS_CONTEXT}"
  echo "  ./scripts/ops/test-smart-read-s3-profile.sh \$CLOUD_EKS_NAMESPACE standard 1024"
fi

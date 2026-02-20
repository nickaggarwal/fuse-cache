#!/usr/bin/env bash
set -euo pipefail

HELM_VERSION="${HELM_VERSION:-v3.16.4}"
KIND_VERSION="${KIND_VERSION:-v0.24.0}"
KUBECTL_VERSION="${KUBECTL_VERSION:-v1.31.2}"
INSTALL_DIR="${INSTALL_DIR:-/usr/local/bin}"

need_cmd() {
  command -v "$1" >/dev/null 2>&1
}

install_helm() {
  if need_cmd helm; then
    echo "helm already installed: $(helm version --short 2>/dev/null || true)"
    return
  fi

  local os arch tmp
  os="$(uname | tr '[:upper:]' '[:lower:]')"
  arch="$(uname -m)"
  case "$arch" in
    x86_64) arch="amd64" ;;
    aarch64|arm64) arch="arm64" ;;
    *) echo "Unsupported architecture for helm: $arch" >&2; exit 1 ;;
  esac

  tmp="$(mktemp -d)"
  trap 'rm -rf "${tmp:-}"' EXIT

  if curl -fsSL "https://get.helm.sh/helm-${HELM_VERSION}-${os}-${arch}.tar.gz" -o "$tmp/helm.tgz"; then
    tar -xzf "$tmp/helm.tgz" -C "$tmp"
    install "$tmp/${os}-${arch}/helm" "$INSTALL_DIR/helm"
    echo "Installed helm to $INSTALL_DIR/helm"
  else
    echo "Direct helm download failed, trying apt package manager..."
    apt-get update
    apt-get install -y helm
  fi
}

install_kind() {
  if need_cmd kind; then
    echo "kind already installed: $(kind --version 2>/dev/null || true)"
    return
  fi

  local os arch
  os="$(uname | tr '[:upper:]' '[:lower:]')"
  arch="$(uname -m)"
  case "$arch" in
    x86_64) arch="amd64" ;;
    aarch64|arm64) arch="arm64" ;;
    *) echo "Unsupported architecture for kind: $arch" >&2; exit 1 ;;
  esac

  if curl -fsSL "https://kind.sigs.k8s.io/dl/${KIND_VERSION}/kind-${os}-${arch}" -o /tmp/kind; then
    install /tmp/kind "$INSTALL_DIR/kind"
    rm -f /tmp/kind
    echo "Installed kind to $INSTALL_DIR/kind"
  else
    echo "Direct kind download failed, trying apt package manager..."
    apt-get update
    apt-get install -y kind
  fi
}

install_kubectl() {
  if need_cmd kubectl; then
    echo "kubectl already installed: $(kubectl version --client --short 2>/dev/null || true)"
    return
  fi

  local os arch
  os="$(uname | tr '[:upper:]' '[:lower:]')"
  arch="$(uname -m)"
  case "$arch" in
    x86_64) arch="amd64" ;;
    aarch64|arm64) arch="arm64" ;;
    *) echo "Unsupported architecture for kubectl: $arch" >&2; exit 1 ;;
  esac

  if curl -fsSL "https://dl.k8s.io/release/${KUBECTL_VERSION}/bin/${os}/${arch}/kubectl" -o /tmp/kubectl; then
    install /tmp/kubectl "$INSTALL_DIR/kubectl"
    rm -f /tmp/kubectl
    echo "Installed kubectl to $INSTALL_DIR/kubectl"
  else
    echo "Direct kubectl download failed, trying apt package manager..."
    apt-get update
    apt-get install -y kubectl
  fi
}

usage() {
  cat <<USAGE
Usage: $(basename "$0") [helm|kind|kubectl|all]

Defaults to 'all' when no argument is provided.
USAGE
}

case "${1:-all}" in
  helm)
    install_helm
    ;;
  kind)
    install_kind
    ;;
  kubectl)
    install_kubectl
    ;;
  all)
    install_helm
    install_kind
    install_kubectl
    ;;
  *)
    usage
    exit 1
    ;;
esac

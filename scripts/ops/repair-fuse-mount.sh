#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 3 || $# -gt 4 ]]; then
  echo "Usage: $0 <resource-group> <vmss-name> <instance-id> [mount-path]"
  echo "Example: $0 mc_stargz-test_group_stargz-test_westus aks-memnvme-38123922-vmss 2 /mnt/fuse"
  exit 1
fi

RESOURCE_GROUP="$1"
VMSS_NAME="$2"
INSTANCE_ID="$3"
MOUNT_PATH="${4:-/mnt/fuse}"

az vmss run-command invoke \
  -g "$RESOURCE_GROUP" \
  -n "$VMSS_NAME" \
  --instance-id "$INSTANCE_ID" \
  --command-id RunShellScript \
  --scripts "mount | grep \" on $MOUNT_PATH \" || true; umount -l $MOUNT_PATH || true; rm -rf $MOUNT_PATH; mkdir -p $MOUNT_PATH; ls -ld $MOUNT_PATH"

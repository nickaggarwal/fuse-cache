#!/bin/sh
# Azure chunk-object reads, Go (AzureStorage.Read via the azblob SDK) vs Rust
# (fusedata AzureReader), against a local Azurite. Runs on the host.
#
#   docker run -d --name fusedata-azurite -p 127.0.0.1:10000:10000 \
#     mcr.microsoft.com/azure-storage/azurite:3.35.0 \
#     azurite-blob --blobHost 0.0.0.0 --inMemoryPersistence --skipApiVersionCheck --loose
#   go run ./tools/readbench azure-put -size 268439777
#   sh rust/fusedata/bench/run-azure.sh
set -eu
ROOT=$(cd "$(dirname "$0")/../../.." && pwd)
RS=${RS:-$ROOT/rust/fusedata/target/release/fusedata}
REPS=${REPS:-3}
SIZE=${SIZE:-268439777}
KEY='Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=='
cd "$ROOT"
go test -c -o /tmp/cache.test ./internal/cache >/dev/null
for rep in $(seq 1 "$REPS"); do
  for t in 1 4; do
    FUSE_AZBENCH_ENDPOINT=http://127.0.0.1:10000/devstoreaccount1/ FUSE_AZBENCH_ACCOUNT=devstoreaccount1 \
      FUSE_AZBENCH_KEY="$KEY" FUSE_AZBENCH_PATH=/bench/az.bin FUSE_AZBENCH_SIZE=$((SIZE - SIZE % 16777216)) \
      FUSE_AZBENCH_THREADS=$t /tmp/cache.test -test.run TestAzureReadBench 2>/dev/null | grep '^{' \
      | sed "s|}$|,\"rep\":$rep}|"
    AZURE_STORAGE_KEY="$KEY" "$RS" bench-azure --azure-account devstoreaccount1 \
      --azure-endpoint http://127.0.0.1:10000/devstoreaccount1 --path /bench/az.bin --size "$SIZE" \
      --threads $t --total-mb 256 | sed "s|}$|,\"rep\":$rep}|"
  done
done

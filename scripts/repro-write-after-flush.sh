#!/bin/bash
# Usage (from the repo root, on any Docker host):
#   docker run --rm --privileged --device /dev/fuse -v "$PWD":/src golang:1.23 bash /src/scripts/repro-write-after-flush.sh
# Before the fix: RESULT=FAIL with 40538 zeros. After: RESULT=PASS.
# Real-mount repro: write, close a dup of the fd (FUSE_FLUSH while still open),
# write more, close, read back. Run inside a privileged Linux container.
set -u
cd /src
apt-get update -qq >/dev/null 2>&1 && apt-get install -y -qq fuse3 >/dev/null 2>&1
CGO_ENABLED=0 go build -o /tmp/coord ./cmd/coordinator && CGO_ENABLED=0 go build -o /tmp/client ./cmd/client || exit 9
/tmp/coord -port 8080 -grpc-port 9080 >/tmp/coord.log 2>&1 &
sleep 2
mkdir -p /mnt/f /tmp/nvme
/tmp/client -mount /mnt/f -nvme /tmp/nvme -coordinator localhost:8080 -coordinator-grpc localhost:9080 \
  -cloud-provider none -fuse-backend gofuse -enable-agent-server=false -port 8081 -grpc-port 9081 >/tmp/client.log 2>&1 &
for i in $(seq 30); do mountpoint -q /mnt/f && break; sleep 1; done
mountpoint -q /mnt/f || { echo "mount failed"; tail -20 /tmp/client.log; exit 8; }
f=/mnt/f/files.img
exec 3>"$f"
head -c 40538 /dev/zero | tr '\0' 'A' >&3
exec 4>&3      # dup the fd ...
exec 4>&-      # ... and close the dup: kernel sends FUSE_FLUSH, file stays open on fd 3
head -c 1053 /dev/zero | tr '\0' 'B' >&3
exec 3>&-
sleep 1
size=$(stat -c %s "$f")
a=$(head -c 40538 "$f" | tr -cd 'A' | wc -c)
b=$(tail -c 1053 "$f" | tr -cd 'B' | wc -c)
z=$(tr -cd '\000' < "$f" | wc -c)
echo "size=$size A=$a/40538 B=$b/1053 zeros=$z"
grep "Write summary path=/files.img" /tmp/client.log | sed 's/.*\(size=[0-9]* write_calls=[0-9]* write_bytes=[0-9]*\).*/  flush: \1/'
[ "$size" = 41591 ] && [ "$a" = 40538 ] && [ "$b" = 1053 ] && echo RESULT=PASS || echo RESULT=FAIL

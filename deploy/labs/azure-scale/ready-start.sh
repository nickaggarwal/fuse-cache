#!/bin/sh
# In a privileged nydusd pod: wait for the RAFS mount, set up a writable
# overlay over it, then exec vLLM inside it via chroot and time readiness.
# Prints one line: mount_s vllm_ready_s (both from /boot-in/ready).
set -u
T0=$(cat /boot-in/t0)
until [ -d /mnt/img/opt ]; do sleep 0.2; done
M=$(awk -v t=$T0 'BEGIN{"date +%s.%N" | getline n; printf "%.1f", n-t}')
mkdir -p /ov/u /ov/w /root
mount -t overlay overlay -o lowerdir=/mnt/img,upperdir=/ov/u,workdir=/ov/w /root
for d in proc sys dev dev/shm tmp; do mkdir -p /root/$d; done
mount -t proc proc /root/proc; mount --rbind /sys /root/sys; mount --rbind /dev /root/dev
mount -t tmpfs -o size=8g tmpfs /root/dev/shm; mount -t tmpfs tmpfs /root/tmp
cp /etc/resolv.conf /root/etc/resolv.conf 2>/dev/null
chroot /root env -i PATH=/opt/venv/bin:/usr/local/bin:/usr/bin:/bin VIRTUAL_ENV=/opt/venv HOME=/root \
  LD_PRELOAD=/usr/lib/x86_64-linux-gnu/libtcmalloc_minimal.so.4:/opt/venv/lib/libiomp5.so \
  HF_HUB_OFFLINE=1 TRANSFORMERS_OFFLINE=1 VLLM_CPU_KVCACHE_SPACE=4 VLLM_CPU_OMP_THREADS_BIND=auto \
  sh -c 'cd /vllm-workspace 2>/dev/null; exec vllm serve /models/qwen2.5-7b-instruct --served-model-name=qwen2.5-7b --max-model-len=4096 --dtype=bfloat16 --port=8000' > /tmp/vllm.log 2>&1 &
until curl -sf -m 2 http://127.0.0.1:8000/v1/models >/dev/null 2>&1; do
  kill -0 $! 2>/dev/null || { echo "vllm-died $M"; tail -20 /tmp/vllm.log; exit 1; }
  sleep 1
done
awk -v t=$T0 -v m=$M 'BEGIN{"date +%s.%N" | getline n; printf "%s %.1f\n", m, n-t}'

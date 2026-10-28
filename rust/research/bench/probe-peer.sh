#!/bin/sh
# Protocol-level comparison of the Go and Rust /api/peer/read servers: for a
# set of request paths, print status, Content-Length, Content-Type,
# Retry-After and the SHA-256 of the body from each. Identical lines mean
# identical observable behaviour for PeerStorage.readFromPeerRaw.
GO=${GO:-127.0.0.1:18081}
RS=${RS:-127.0.0.1:18082}
for p in \
  /bench/model.bin_chunk_0 \
  /bench/model.bin_chunk_191 \
  /bench/model.bin_chunk_192 \
  /bench/model.bin_chunk_193 \
  /bench/whole.bin \
  /bench/whole.bin_chunk_0 \
  /bench/whole.bin_chunk_63 \
  /bench/whole.bin_chunk_64 \
  /bench/small.bin \
  /bench/small.bin.sha256 \
  /bench/../bench/model.bin_chunk_5 \
  /bench/nope \
  /bench/model.bin_chunk_x \
  ""; do
  for side in go rs; do
    if [ $side = go ]; then a=$GO; else a=$RS; fi
    if [ -z "$p" ]; then url="http://$a/api/peer/read"; else url="http://$a/api/peer/read?path=$p"; fi
    hdr=$(curl -s -D - -o /tmp/probe.body "$url" | tr -d '\r')
    code=$(echo "$hdr" | head -1 | cut -d' ' -f2)
    cl=$(echo "$hdr" | grep -i '^content-length:' | cut -d' ' -f2)
    ct=$(echo "$hdr" | grep -i '^content-type:' | cut -d' ' -f2-)
    ra=$(echo "$hdr" | grep -i '^retry-after:' | cut -d' ' -f2)
    sum=$(sha256sum /tmp/probe.body | cut -c1-16)
    printf '%-4s %-36s %s len=%-9s ra=%-2s %-34s %s\n' $side "${p:-<none>}" "$code" "$cl" "$ra" "$ct" "$sum"
  done
done

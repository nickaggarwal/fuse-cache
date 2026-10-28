#!/bin/sh
# Peer protocol interop: every client x server pairing fetches every chunk of
# the chunked file and of the whole-layout file and verifies content.
# Exit code = number of failed pairings. Runs inside the lab container with
# the Go server on :18081 and the Rust server on :18082 (see README).
RB=${RB:-/out/readbench-linux}
RS=${RS:-/out/fusedata}
CS=${CHUNKED_SIZE:-3221226249}
WS=${WHOLE_SIZE:-1073742157}
fail=0
for srv in go:127.0.0.1:18081 rs:127.0.0.1:18082; do
  name=${srv%%:*}; addr=${srv#*:}
  for f in model.bin:$CS whole.bin:$WS; do
    file=${f%%:*}; size=${f#*:}
    total=$(( (size + 1048575) / 1048576 ))
    if "$RB" bench-peer -addr "$addr" -path /bench/$file -size "$size" -threads 4 -total-mb $total -verify -server $name >/dev/null 2>/tmp/interop.err; then
      echo "PASS go-client   -> $name-server  /bench/$file"
    else
      echo "FAIL go-client   -> $name-server  /bench/$file: $(tail -1 /tmp/interop.err)"; fail=$((fail+1))
    fi
    if "$RS" bench-peer --addr "$addr" --path /bench/$file --size "$size" --threads 4 --total-mb $total --verify --server $name >/dev/null 2>/tmp/interop.err; then
      echo "PASS rust-client -> $name-server  /bench/$file"
    else
      echo "FAIL rust-client -> $name-server  /bench/$file: $(tail -1 /tmp/interop.err)"; fail=$((fail+1))
    fi
  done
done
exit $fail

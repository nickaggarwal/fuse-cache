#!/bin/sh
# Library-layer comparison of read_view modes (segment / map / copy, and
# segment sizes) against fixed Go, single reader, cold and warm.
RS=${RS:-/out/fusedata}
VARIANTS=${VARIANTS:-"
seg4m|--view segment
seg16m|--view segment --segment-kib 16384
seg1m|--view segment --segment-kib 1024
map|--view map
copy|--view copy
"}
RS="$RS" VARIANTS="$VARIANTS" exec sh "$(dirname "$0")/ablate-seq.sh"

#!/usr/bin/env bash
# Run the autoscale phases as soon as the pressure workload finishes.
cd "$(dirname "$0")"
until grep -qE "^done|Traceback" results/workload.out; do sleep 20; done
grep -q Traceback results/workload.out && { echo "workload failed; not starting autoscale"; exit 1; }
SCALE=200 RATE=480 python3 -u autoscale.py > results/autoscale.out 2>&1

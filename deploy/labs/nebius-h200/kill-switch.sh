#!/usr/bin/env bash
# Hard 2-hour cutoff for the Nebius lab: at the deadline, save whatever results
# exist, then run down.sh + check.sh no matter what state the benchmark is in.
# DEADLINE (epoch seconds) is set by the caller: 2 h after provisioning starts.
set -u
cd "$(dirname "$0")"
DEADLINE=${DEADLINE:?set DEADLINE to an epoch time}
while [ "$(date +%s)" -lt "$DEADLINE" ]; do sleep 30; done
echo "$(date '+%F %T') kill-switch: 2 h reached, tearing down" | tee -a killswitch.log
pkill -f "bench.sh" 2>/dev/null
mkdir -p results-final && cp -r results/. results-final/ 2>/dev/null
./down.sh >> killswitch.log 2>&1
echo "$(date '+%F %T') down.sh exit=$?" | tee -a killswitch.log
./check.sh >> killswitch.log 2>&1; echo "$(date '+%F %T') check.sh exit=$?" | tee -a killswitch.log

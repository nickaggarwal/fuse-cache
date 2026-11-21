#!/usr/bin/env bash
# Hard cutoff for the Azure scale lab: at DEADLINE (epoch seconds) save
# results and run down.sh + check.sh, whatever state the run is in.
set -u
cd "$(dirname "$0")"
DEADLINE=${DEADLINE:?set DEADLINE to an epoch time}
while [ "$(date +%s)" -lt "$DEADLINE" ]; do sleep 30; done
echo "$(date '+%F %T') kill-switch: deadline reached, tearing down" | tee -a killswitch.log
pkill -f "scale-bench.sh" 2>/dev/null
mkdir -p results-final && cp -r results/. results-final/ 2>/dev/null
./down.sh >> killswitch.log 2>&1
echo "$(date '+%F %T') down.sh exit=$?" | tee -a killswitch.log

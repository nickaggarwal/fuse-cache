#!/usr/bin/env bash
# Live checks of the holder lifecycle on cpu-w3 (fc-imp), against the real
# coordinator and etcd. Only touches files under /holdertest/.
#   1 generation race : a late retraction for an old generation must not
#                       remove a newer admission's row
#   2 vanished file   : a file deleted from disk behind the client is
#                       retracted on the next renewal, not renewed
#   3 pending retract : with the coordinator unreachable a retraction is
#                       kept, and lands once it is back
#   4 replication cap : fetch-lease slots are exclusive across nodes
# Prints PASS/FAIL per check; exit code = failures.
set -u
cd "$(dirname "$0")"
C=${CTX:?set CTX (kube context)}; NS=${NS:-fc-imp}
k() { kubectl --context $C -n $NS "$@"; }
N0=${N0:?set N0 (node 0)}; N1=${N1:?set N1 (node 1)}
pod() { k get pod -l app=fcimp-client --field-selector spec.nodeName=$1 -o jsonpath='{.items[0].metadata.name}'; }
COORD=$(k get pod -l app=coordinator -o jsonpath='{.items[0].metadata.name}')
api()  { kubectl --context $C get --raw "/api/v1/namespaces/$NS/pods/$1:8092/proxy$2" 2>/dev/null; }
capi() { kubectl --context $C get --raw "/api/v1/namespaces/$NS/pods/$COORD:8080/proxy$1" 2>/dev/null; }
post() { kubectl --context $C create --raw "/api/v1/namespaces/$NS/pods/$1:8092/proxy$2" -f - <<< "${3:-{\}}" 2>/dev/null; }
rows() {  # path -> "peer gen" lines for nvme rows
  capi "/api/files/location?path=$1" | python3 -c '
import json,sys
try: d=json.load(sys.stdin)
except Exception: d=[]
for l in (d if isinstance(d,list) else d.get("locations") or []):
    if l.get("storage_tier")=="nvme": print(l["peer_id"], l.get("generation",0))'
}
metric() { api "$(pod $1)" /metrics | awk -v m="$2" '$1==m {print $2}'; }
fail=0
ok()  { echo "PASS $1"; }
bad() { echo "FAIL $1: $2"; fail=$((fail+1)); }
P0=$(pod $N0); P1=$(pod $N1)
writefile() {  # pod path bytes: write a file through the node's fuse mount
  k exec $1 -c client -- sh -c "head -c $3 /dev/urandom > /host/mnt$2 && sync" >/dev/null 2>&1
}

echo "== 1 generation race"
k exec $P0 -c client -- mkdir -p /host/mnt/holdertest >/dev/null 2>&1
writefile $P0 /holdertest/g.bin 4096; sleep 3
post $P0 /api/holders/renew >/dev/null
g1=$(rows /holdertest/g.bin | awk -v p="$N0-fcimp" '$1==p{print $2}')
# evict (delete locally) and re-admit: a new generation
k exec $P0 -c client -- rm -f /host/cache/holdertest/g.bin >/dev/null 2>&1
post $P0 /api/holders/renew >/dev/null
writefile $P0 /holdertest/g.bin 4096; sleep 3
post $P0 /api/holders/renew >/dev/null
g2=$(rows /holdertest/g.bin | awk -v p="$N0-fcimp" '$1==p{print $2}')
# a late retraction for the first generation, straight to the coordinator
kubectl --context $C delete --raw "/api/v1/namespaces/$NS/pods/$COORD:8080/proxy/api/files/location?path=/holdertest/g.bin&peer_id=$N0-fcimp&storage_tier=nvme&generation=$g1" >/dev/null 2>&1
g3=$(rows /holdertest/g.bin | awk -v p="$N0-fcimp" '$1==p{print $2}')
if [ -n "$g1" ] && [ -n "$g2" ] && [ "$g2" -gt "$g1" ] && [ "$g3" = "$g2" ]; then ok "late retraction (gen $g1) kept the newer row (gen $g2)"
else bad "generation race" "g1=$g1 g2=$g2 after-late-retract=$g3"; fi

echo "== 2 vanished file"
writefile $P0 /holdertest/v.bin 4096; sleep 3
post $P0 /api/holders/renew >/dev/null
before=$(rows /holdertest/v.bin | grep -c "$N0-fcimp")
k exec $P0 -c client -- rm -f /host/cache/holdertest/v.bin >/dev/null 2>&1
out=$(post $P0 /api/holders/renew)
after=$(rows /holdertest/v.bin | grep -c "$N0-fcimp")
if [ "$before" -ge 1 ] && [ "$after" -eq 0 ]; then ok "row retracted after the file vanished from disk (renew: $out)"
else bad "vanished file" "rows before=$before after=$after renew=$out"; fi

echo "== 3 pending retraction"
writefile $P1 /holdertest/p.bin 4096; sleep 3
post $P1 /api/holders/renew >/dev/null
# Coordinator unreachable from node 1's client only: a privileged
# host-network pod on node 1 adds REJECT rules from that pod's IP to each
# coordinator pod IP, and removes them afterwards (also on any exit).
CIP=$(k get svc coordinator -o jsonpath='{.spec.clusterIP}')
PIP=$(k get pod $P1 -o jsonpath='{.status.podIP}')
NYDUS_IMG=${NYDUS_IMG:?set NYDUS_IMG (an image with a shell)}
hostnet() {  # rule-command: one-shot privileged pod, run in the host's root and net namespace
  k run hl-net-$RANDOM --rm -i --restart=Never --quiet --image=$NYDUS_IMG \
    --overrides="{\"spec\":{\"nodeName\":\"$N1\",\"hostNetwork\":true,\"hostPID\":true,\"tolerations\":[{\"operator\":\"Exists\"}],\"containers\":[{\"name\":\"n\",\"image\":\"$NYDUS_IMG\",\"command\":[\"chroot\",\"/host\",\"sh\",\"-c\",\"$1\"],\"securityContext\":{\"privileged\":true},\"volumeMounts\":[{\"name\":\"host\",\"mountPath\":\"/host\"}]}],\"volumes\":[{\"name\":\"host\",\"hostPath\":{\"path\":\"/\"}}]}}" \
    --command -- true 2>&1 | grep -vE "deleted|couldn't attach"
}
rules() {  # -I|-D: one rule per coordinator pod IP, built here, run on the node
  local cmd="" ip
  for ip in $CEPS; do cmd="$cmd iptables $1 FORWARD -s $PIP -d $ip -j REJECT 2>/dev/null;"; done
  hostnet "$cmd echo rules$1"
}
unblock() { rules -D; }
CEPS=$(kubectl --context $C -n $NS get endpointslices -l kubernetes.io/service-name=coordinator -o jsonpath='{range .items[*].endpoints[*]}{.addresses[0]} {end}')
trap unblock EXIT
# Service traffic is DNAT-ed to the coordinator pods before it leaves the
# node, so block the pod IPs, not the service's virtual IP.
rules -I
reach=n/a
k exec $P1 -c client -- rm -f /host/cache/holdertest/p.bin >/dev/null 2>&1
# The pass cannot reach the coordinator: its retraction fails every
# attempt (about 1 min of backoff) and must be kept pending, not dropped.
kubectl --context $C --request-timeout=150s create --raw "/api/v1/namespaces/$NS/pods/$P1:8092/proxy/api/holders/renew" -f - <<< '{}' >/dev/null 2>&1
pend=$(metric $N1 fuse_holder_retractions_pending)
still=$(rows /holdertest/p.bin | grep -c "$N1-fcimp")
unblock; trap - EXIT
# The client's connections to the coordinator recover with backoff; the
# renewal timer would retry every 5 min, here up to 5 passes 10 s apart.
for _ in 1 2 3 4 5; do
  post $P1 /api/holders/renew >/dev/null
  gone=$(rows /holdertest/p.bin | grep -c "$N1-fcimp")
  [ "$gone" -eq 0 ] && break
  sleep 10
done
landed=$(metric $N1 fuse_holder_retractions_pending_landed_total)
if [ "${pend:-0}" -ge 1 ] && [ "$still" -ge 1 ] && [ "$gone" -eq 0 ] && [ "${landed:-0}" -ge 1 ]; then ok "retraction kept while unreachable (pending=$pend), landed after (landed=$landed)"
else bad "pending retraction" "probe=$reach pending=$pend row-while-down=$still row-after=$gone landed=$landed"; fi

echo "== 4 replication slots"
lease() { kubectl --context $C create --raw "/api/v1/namespaces/$NS/pods/$COORD:8080/proxy/api/fetch-lease" -f - <<< "{\"key\":\"$1\",\"peer_id\":\"$2\",\"ttl_seconds\":30}" 2>/dev/null; }
a=$(lease replication-slot/live-test nodeA); b=$(lease replication-slot/live-test nodeB)
if echo "$a" | grep -q '"granted":true' && echo "$b" | grep -q '"granted":false'; then ok "slot held by one node, refused to another"
else bad "replication slots" "A=$a B=$b"; fi
kubectl --context $C delete --raw "/api/v1/namespaces/$NS/pods/$COORD:8080/proxy/api/fetch-lease" -f - <<< '{"key":"replication-slot/live-test","peer_id":"nodeA"}' >/dev/null 2>&1

echo "== cleanup"
for f in g.bin v.bin p.bin; do kubectl --context $C delete --raw "/api/v1/namespaces/$NS/pods/$P0:8092/proxy/api/files/holdertest/$f" >/dev/null 2>&1; done
echo "failures: $fail"; exit $fail

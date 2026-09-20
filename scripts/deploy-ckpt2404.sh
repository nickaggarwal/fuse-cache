#!/usr/bin/env bash
# Build the working tree and roll it onto the ckpt2404 nodepool, then confirm
# the residency endpoints this branch adds are actually served.
#
# Why a pool-scoped script rather than `kubectl apply -f k8s/`: the manifests in
# k8s/ pin `nodeSelector: agentpool: userpool`, which no longer matches the
# cluster (the live client DaemonSet was moved to a nodeAffinity list covering
# userpool/gpuckpt/ckpt2404, and userpool is scaled to 0). Applying the repo
# manifests would deschedule every client pod. This script mutates only the
# image and the flags the verification needs, and refuses to run if the shared
# DaemonSet is serving nodes outside the target pool.
#
# Usage:
#   scripts/deploy-ckpt2404.sh [namespace]
# Env:
#   POOL=ckpt2404          node label value for `pool=` / `agentpool=`
#   TAG=<tag>              image tag (default: resid-<short-sha>[-dirty])
#   REGISTRY=stargzrepo    ACR registry name
#   SKIP_BUILD=1           reuse an existing tag instead of running az acr build
#   WITH_CSI=1             also stand up a pool-scoped CSI node DaemonSet, which
#                          section 6 of scripts/e2e-residency.sh needs
#   WARM_RECONCILE_SEC=30  client -warm-reconcile-interval-sec (default 120 is
#                          too slow to observe inside a test run)
#   WITH_MODEL_KV=1        enable the durable Model KV store on the clients,
#                          which scripts/e2e-modelkv-recovery.sh needs
set -uo pipefail

NS="${1:-fuse-system}"
POOL="${POOL:-ckpt2404}"
REGISTRY="${REGISTRY:-stargzrepo}"
WARM_RECONCILE_SEC="${WARM_RECONCILE_SEC:-30}"
WITH_CSI="${WITH_CSI:-0}"
WITH_MODEL_KV="${WITH_MODEL_KV:-0}"
SKIP_BUILD="${SKIP_BUILD:-0}"

if [ -z "${TAG:-}" ]; then
  sha=$(git rev-parse --short HEAD 2>/dev/null || echo nogit)
  [ -n "$(git status --porcelain 2>/dev/null)" ] && sha="${sha}-dirty"
  TAG="resid-${sha}"
fi
IMAGE="${REGISTRY}.azurecr.io/fuse-client:${TAG}"

step() { echo; echo "=== $* ==="; }
die()  { echo "ERROR: $*" >&2; exit 1; }

command -v kubectl >/dev/null || die "kubectl not on PATH"
[ "$SKIP_BUILD" = "1" ] || command -v az >/dev/null || die "az not on PATH (or set SKIP_BUILD=1)"
command -v python3 >/dev/null || die "python3 not on PATH (used to rewrite container args)"

# ------------------------------------------------------------------ 0. guards
step "0. preflight"
nodes=$(kubectl get nodes -l "pool=${POOL}" -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}' | grep -c . )
[ "${nodes:-0}" -ge 2 ] || die "need >=2 nodes labelled pool=${POOL}, found ${nodes:-0}. Scale the pool first:
  az aks nodepool scale --resource-group stargz-test_group --cluster-name stargz-test --name ${POOL} --node-count 2"
echo "nodes in pool ${POOL}: ${nodes}"
kubectl get nodes -l "pool=${POOL}" --no-headers | awk '{print "  " $1 " " $2}'

# The client DaemonSet is shared. Rolling a test image onto it is only
# acceptable while every one of its pods is already on the target pool —
# otherwise this script would be restarting production nodes as a side effect.
outside=$(kubectl -n "$NS" get pods -l app=client -o json 2>/dev/null | python3 -c '
import json,sys
pool=sys.argv[1]
try: pods=json.load(sys.stdin).get("items",[])
except Exception: pods=[]
print(",".join(p["spec"].get("nodeName","?") for p in pods if pool not in p["spec"].get("nodeName","")))
' "$POOL")
if [ -n "$outside" ]; then
  die "DaemonSet client has pods outside pool ${POOL} (${outside}).
Rolling a test image would restart them. Scope the DaemonSet to ${POOL} first, or run this against a pool-scoped copy."
fi

kubectl -n "$NS" get configmap fuse-config >/dev/null 2>&1 || die "configmap fuse-config missing in ${NS}"
kubectl -n "$NS" get secret azure-credentials >/dev/null 2>&1 || die "secret azure-credentials missing in ${NS}"

# ------------------------------------------------------------------- 1. build
step "1. image ${IMAGE}"
if [ "$SKIP_BUILD" = "1" ]; then
  echo "SKIP_BUILD=1, assuming ${TAG} already exists in ${REGISTRY}"
else
  # Local buildx, not `az acr build`: ACR's classic build agent runs a
  # pre-BuildKit dependency scanner that rejects this Dockerfile outright
  # ("unable to understand line FROM --platform=$BUILDPLATFORM ..."), and the
  # Dockerfile needs BUILDPLATFORM for the cross-compile. --platform linux/amd64
  # is explicit because an arm64 workstation would otherwise push an image the
  # AKS nodes cannot run.
  command -v docker >/dev/null || die "docker not on PATH (or set SKIP_BUILD=1)"
  az acr login --name "$REGISTRY" >/dev/null || die "az acr login failed"
  docker buildx build --platform linux/amd64 -t "$IMAGE" --push . || die "image build/push failed"
fi

# ------------------------------------------------------------- 2. client args
# Two flags the verification depends on:
#   -warm-reconcile-interval-sec  the default 120s (±50% jitter) means a single
#                                 re-assert can take 3 minutes to land; 30s
#                                 keeps section 6 inside a sane timeout.
#   -fetch-lease / -fast-chunk-advertise are left exactly as found: they are
#                                 orthogonal to residency and changing them
#                                 would confound the run.
step "2. patch client DaemonSet (image + -warm-reconcile-interval-sec ${WARM_RECONCILE_SEC}$([ "$WITH_MODEL_KV" = 1 ] && echo " + -enable-model-kv"))"
# python3 -c, not `python3 - <<HEREDOC`: a heredoc becomes stdin and would
# swallow the piped DaemonSet JSON, leaving the script reading its own source.
patch=$(kubectl -n "$NS" get ds client -o json | python3 -c '
import json, sys
image, secs, kv = sys.argv[1], sys.argv[2], sys.argv[3]
ds = json.load(sys.stdin)
spec = ds["spec"]["template"]["spec"]
containers = []
for c in spec["containers"]:
    entry = {"name": c["name"], "image": image}
    if c["name"] == "client":
        args = list(c.get("args", []))
        if "-warm-reconcile-interval-sec" in args:
            args[args.index("-warm-reconcile-interval-sec") + 1] = secs
        else:
            args += ["-warm-reconcile-interval-sec", secs]
        if kv == "1" and "-enable-model-kv" not in args:
            # Short batch/reap intervals for the same reason as the warm
            # reconciler: the production defaults (30s batch, 5min reap) make a
            # test run wait on a clock instead of on the code. The e2e script
            # forces both explicitly, so these only bound the background path.
            args += ["-enable-model-kv",
                     "-model-kv-batch-interval-sec", "10",
                     "-model-kv-reap-interval-sec", "30"]
        entry["args"] = args
    containers.append(entry)
inits = [{"name": c["name"], "image": image} for c in spec.get("initContainers", [])]
out = {"spec": {"template": {"spec": {"containers": containers}}}}
if inits:
    out["spec"]["template"]["spec"]["initContainers"] = inits
print(json.dumps(out))
' "$IMAGE" "$WARM_RECONCILE_SEC" "$WITH_MODEL_KV")
[ -n "$patch" ] || die "failed to build the client patch"
kubectl -n "$NS" patch ds client --type=strategic -p "$patch" >/dev/null || die "patch ds/client failed"

step "3. patch coordinator Deployment (image)"
# /api/residency is a coordinator endpoint — a client-only roll leaves the
# cluster-wide half 404ing, which is the confusing half-deploy this catches.
kubectl -n "$NS" set image deploy/coordinator "coordinator=${IMAGE}" >/dev/null || die "set image deploy/coordinator failed"

# --------------------------------------------------------------------- 4. CSI
if [ "$WITH_CSI" = "1" ]; then
  step "4. pool-scoped CSI node DaemonSet"
  # A separate DaemonSet rather than a patch of the shared fuse-csi-node: the
  # shared one is pinned to userpool, the selectors are disjoint, and a node
  # only ever runs one of them, so the driver socket never collides.
  kubectl -n "$NS" get sa fuse-csi-node >/dev/null 2>&1 || die "serviceaccount fuse-csi-node missing; apply k8s/csi-rbac.yaml first"
  kubectl apply -f k8s/csi-driver.yaml >/dev/null || die "apply csi-driver.yaml failed"
  kubectl -n "$NS" apply -f - <<YAML >/dev/null || die "apply pool CSI DaemonSet failed"
apiVersion: apps/v1
kind: DaemonSet
metadata:
  name: fuse-csi-node-${POOL}
  namespace: ${NS}
  labels: {app: fuse-csi-node, pool: "${POOL}"}
spec:
  selector:
    matchLabels: {app: fuse-csi-node, pool: "${POOL}"}
  template:
    metadata:
      labels: {app: fuse-csi-node, pool: "${POOL}"}
    spec:
      nodeSelector: {pool: "${POOL}"}
      serviceAccountName: fuse-csi-node
      containers:
        - name: csi-driver
          image: ${IMAGE}
          command: ["csi-driver"]
          args:
            - "--endpoint=unix:///csi/csi.sock"
            - "--node-id=\$(NODE_NAME)"
            - "--agent-socket=/var/run/fuse-client/agent.sock"
            - "--fuse-root=/host/mnt/fuse"
          env:
            - name: NODE_NAME
              valueFrom: {fieldRef: {fieldPath: spec.nodeName}}
          securityContext: {privileged: true}
          volumeMounts:
            - {name: socket-dir, mountPath: /csi}
            - {name: agent-socket, mountPath: /var/run/fuse-client}
            - {name: mountpoint-dir, mountPath: /var/lib/kubelet/pods, mountPropagation: Bidirectional}
            - {name: fuse-mount, mountPath: /host/mnt, mountPropagation: HostToContainer}
          resources:
            requests: {cpu: 50m, memory: 64Mi}
            limits: {cpu: 200m, memory: 128Mi}
        - name: node-driver-registrar
          image: registry.k8s.io/sig-storage/csi-node-driver-registrar:v2.10.0
          args:
            - "--csi-address=/csi/csi.sock"
            - "--kubelet-registration-path=/var/lib/kubelet/plugins/fuse.csi.storage.io/csi.sock"
          volumeMounts:
            - {name: socket-dir, mountPath: /csi}
            - {name: registration-dir, mountPath: /registration}
          resources:
            requests: {cpu: 10m, memory: 16Mi}
            limits: {cpu: 50m, memory: 32Mi}
      volumes:
        - name: socket-dir
          hostPath: {path: /var/lib/kubelet/plugins/fuse.csi.storage.io, type: DirectoryOrCreate}
        - name: registration-dir
          hostPath: {path: /var/lib/kubelet/plugins_registry, type: Directory}
        - name: mountpoint-dir
          hostPath: {path: /var/lib/kubelet/pods, type: Directory}
        - name: agent-socket
          hostPath: {path: /var/run/fuse-client, type: DirectoryOrCreate}
        - name: fuse-mount
          hostPath: {path: /mnt, type: Directory}
YAML
else
  step "4. CSI node DaemonSet skipped (WITH_CSI=1 to enable)"
  echo "  section 6 of scripts/e2e-residency.sh (declared intent / last_verified) will skip"
fi

# ------------------------------------------------------------------ 5. rollout
step "5. rollout"
# The client teardown unmounts FUSE on the node, so a roll is not instant.
kubectl -n "$NS" rollout status deploy/coordinator --timeout=300s || die "coordinator rollout failed"
kubectl -n "$NS" rollout status ds/client --timeout=600s || die "client rollout failed"
if [ "$WITH_CSI" = "1" ]; then
  kubectl -n "$NS" rollout status "ds/fuse-csi-node-${POOL}" --timeout=300s || die "CSI rollout failed"
fi
kubectl -n "$NS" get pods -l app=client -o wide --no-headers | awk '{print "  " $1 "  " $3 "  " $6 "  " $7}'

# -------------------------------------------------------- 6. endpoints served
step "6. residency endpoints are served by the rolled image"
CURLBOX=curlbox
if ! kubectl -n "$NS" get pod "$CURLBOX" >/dev/null 2>&1; then
  kubectl -n "$NS" run "$CURLBOX" --image=curlimages/curl:8.5.0 --restart=Never \
    --command -- sleep 100000 >/dev/null
fi
kubectl -n "$NS" wait --for=condition=Ready "pod/$CURLBOX" --timeout=120s >/dev/null || die "curlbox not ready"

code() { kubectl -n "$NS" exec "$CURLBOX" -- curl -s -o /dev/null -w '%{http_code}' --max-time 20 "$1"; }
podip=$(kubectl -n "$NS" get pods -l app=client -o jsonpath='{.items[0].status.podIP}')
FAILED=0
# 200 or 503 both prove the route exists; 503 is the honest "could not
# enumerate" answer and is a pass for a reachability check. 404 is the failure
# this step exists to catch — an old image still serving.
c=$(code "${podip}:8081/api/cache/residency?prefix=/")
case "$c" in 200|503) echo "PASS  client /api/cache/residency -> $c" ;;
             *) echo "FAIL  client /api/cache/residency -> $c (old image?)"; FAILED=1 ;; esac
c=$(code "coordinator.${NS}.svc.cluster.local:8080/api/residency?prefix=/")
case "$c" in 200|503) echo "PASS  coordinator /api/residency -> $c" ;;
             *) echo "FAIL  coordinator /api/residency -> $c (old image?)"; FAILED=1 ;; esac

echo
if [ "$FAILED" -eq 0 ]; then
  echo "deployed ${IMAGE} to pool ${POOL}. Next: scripts/e2e-residency.sh ${NS}"
else
  echo "deploy finished with $FAILED endpoint failure(s)"
fi
exit $FAILED

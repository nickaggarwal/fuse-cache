#!/usr/bin/env python3
"""Ring placement A/B on the scale lab: fan-out of one cold model to every
node at once, with -ring-replicas=0 vs 2.

Per arm: drop the catalogue's blobs cluster-wide (cold), seed once through
node 0 (so node 0 is the only holder), then start the same large deployment
on every other node at the same moment. Without the ring, every reader
pulls from node 0; with it, chunks spread to owners as they are read, and
later readers pull from many nodes. Two waves per arm: wave 1 (cold) and
wave 2 on the same nodes after dropping their local copies (warm cluster,
cold node) - wave 2 is where owners serve.

Results: results/ring-ab.jsonl (per start: arm, wave, secs, peer/cloud MB,
per-holder read distribution from /metrics).
"""
import json, os, subprocess, threading, time
src = open("workload.py").read().split("def main():")[0]
g = {"__file__": "workload.py"}; exec(src, g)
kc, proxy, pods, metrics, emit = g["kc"], g["proxy"], g["pods"], g["metrics"], g["emit"]
NS = "fc-scale"
MODEL = os.environ.get("MODEL", "llama-3.3-70b")
m = [i for i, x in enumerate(g["mods"]) if x[0] == MODEL][0]
files = g["deployment_files"](m)

def set_ring(r):
    a = json.loads(kc("-n", NS, "get", "ds", "fuse-client", "-o", "json"))
    args = [x for x in a["spec"]["template"]["spec"]["containers"][0]["args"] if not x.startswith("-ring-replicas=")]
    args.append(f"-ring-replicas={r}")
    kc("-n", NS, "patch", "ds", "fuse-client", "--type=json", "-p", json.dumps([{"op": "replace", "path": "/spec/template/spec/containers/0/args", "value": args}]))
    kc("-n", NS, "rollout", "status", "ds/fuse-client", "--timeout=900s", timeout=960)
    time.sleep(45)

def drop_cluster(p0):
    for f in files:
        kc("delete", "--raw", f"/api/v1/namespaces/{NS}/pods/{p0}:8092/proxy/api/files/wl/{f}")
    time.sleep(10)

def drop_local(pod):
    # Evict through the cache (as LRU would): this node's rows are retracted,
    # nothing else changes. rm -rf left rows pointing at files that were gone.
    for f in files:
        proxy(pod, 8092, f"/api/cache/evict?path=/wl/{f}", "post", {}, timeout=120)

def seed(p0):
    script = "mkdir -p /host/mnt/wl\n" + "\n".join(f"head -c {g['size_bytes'](f)} /dev/urandom > /host/mnt/wl/{f}" for f in files) + "\nsync\n"
    kc("-n", NS, "exec", "-i", p0, "-c", "client", "--", "sh", inp=script, timeout=1800)
    # Readers must find node 0 as the holder: wait until its residency for
    # each file is whole and the coordinator lists it.
    for _ in range(60):
        ok = True
        for f in files:
            try:
                r = json.loads(proxy(p0, 8092, f"/api/cache/residency?prefix=/wl/{f}", timeout=30))
                ok = ok and r.get("fraction", 0) >= 1.0
            except Exception:
                ok = False
        if ok:
            break
        time.sleep(5)
    time.sleep(10)

def wave(arm, w, readers):
    ts = []
    for node, pod in readers.items():
        def go(node=node, pod=pod):
            before = metrics(pod)
            t0 = time.time()
            for f in files:
                proxy(pod, 8092, "/api/cache/warm", "post", {"prefix": f"/wl/{f}", "mode": "full", "timeout_seconds": 600}, timeout=700)
            after = metrics(pod)
            d = lambda k: after.get(k, 0) - before.get(k, 0)
            rec = {"arm": arm, "wave": w, "node": node[-12:], "secs": round(time.time() - t0, 2),
                   "peer_mb": round(d("fuse_cache_peer_read_bytes_total") / 2**20, 1), "cloud_mb": round(d("fuse_cache_cloud_read_bytes_total") / 2**20, 1)}
            emit("results/ring-ab.jsonl", rec); print(json.dumps(rec), flush=True)
        t = threading.Thread(target=go); t.start(); ts.append(t)
    for t in ts:
        t.join()

def holder_served():
    out = {}
    for node, pod in pods().items():
        try:
            mm = metrics(pod)
        except Exception:
            continue
        out[node[-12:]] = int(mm.get("fuse_peer_serve_accepted_total", 0))
    return out

def set_absent(nodes_out):
    if nodes_out:
        kc("-n", NS, "patch", "ds", "fuse-client", "--type=json", "-p", json.dumps([{"op": "add", "path": "/spec/template/spec/affinity", "value": {
            "nodeAffinity": {"requiredDuringSchedulingIgnoredDuringExecution": {"nodeSelectorTerms": [{"matchExpressions": [
                {"key": "kubernetes.io/hostname", "operator": "NotIn", "values": nodes_out}]}]}}}}]))
    else:
        kc("-n", NS, "patch", "ds", "fuse-client", "--type=json", "-p", '[{"op":"remove","path":"/spec/template/spec/affinity"}]')
    kc("-n", NS, "rollout", "status", "ds/fuse-client", "--timeout=900s", timeout=960)
    time.sleep(30)

def run_wave(arm, w, readers):
    before = holder_served()
    wave(arm, w, readers)
    after = holder_served()
    served = {k: after.get(k, 0) - before.get(k, 0) for k in after}
    tot = sum(served.values()) or 1
    top = max(served.values()) if served else 0
    rec = {"arm": arm, "wave": w, "holders_serving": sum(1 for v in served.values() if v > 0), "top_holder_share": round(top / tot, 2),
           "reads_served": tot}
    emit("results/ring-ab.jsonl", rec); print(json.dumps(rec), flush=True)

for arm, r in (("ring-off", 0), ("ring-on", 2)):
    print(f"== {arm}", flush=True)
    set_ring(r)
    ps = pods(); nodes = sorted(ps); p0 = ps[nodes[0]]
    drop_cluster(p0); seed(p0)
    readers = {n: ps[n] for n in nodes[1:]}
    # wave 1: cold cluster, node 0 the only holder, 15 readers at once
    run_wave(arm, 1, readers)
    time.sleep(60)  # deferred owner fills run once reads go quiet
    # wave 2: warm cluster, cold readers (evicted through the cache)
    for pod in readers.values():
        drop_local(pod)
    time.sleep(20)
    run_wave(arm, 2, readers)
    time.sleep(60)
    # wave 3: two nodes gone (no retraction), cold readers on the rest
    gone = nodes[1:3]
    for pod in readers.values():
        drop_local(pod)
    set_absent(gone)
    ps = pods()
    readers3 = {n: ps[n] for n in sorted(ps) if n != nodes[0]}
    run_wave(arm, 3, readers3)
    set_absent([])
print("done", flush=True)

#!/usr/bin/env python3
"""Ring placement size sweep on the scale lab: does the ring matter as the
cluster grows?

At each size N (48, 24, 12 nodes; pools d2 = 2N/3, e2 = N/3) and for each
arm (ring off, ring R=2, switched at runtime through POST /api/ring), REPS
runs with the arm order alternating:

  seed      the deployment's files are dropped cluster-wide and written once
            through one node (the seed is the only holder)
  cold      every other node starts the deployment at the same moment
  warm      readers evict their copies through /api/cache/evict (as LRU
            would) and start again against a warm cluster

At N=48 one extra run per arm removes 4 nodes (no retraction) before the
warm wave ("loss").

Per start: results/sweep.jsonl {n, arm, rep, wave, node, secs, peer_mb,
cloud_mb}. Per wave: {holders_serving, top_holder_share, reads_served}.
"""
import json, os, subprocess, threading, time
from concurrent.futures import ThreadPoolExecutor

src = open(os.path.join(os.path.dirname(os.path.abspath(__file__)), "workload.py")).read().split("def main():")[0]
g = {"__file__": "workload.py"}
exec(src, g)
kc, proxy, pods, metrics, emit = g["kc"], g["proxy"], g["pods"], g["metrics"], g["emit"]
NS = "fc-scale"
SUB = subprocess.run(["bash", "-c", "source ./env.sh && echo $SUB"], capture_output=True, text=True).stdout.strip()
MODEL = os.environ.get("MODEL", "llama-3.3-70b")
SIZES = [int(x) for x in os.environ.get("SIZES", "48,24,12").split(",")]
REPS = int(os.environ.get("REPS", "2"))
OUT = "results/sweep.jsonl"
m = [i for i, x in enumerate(g["mods"]) if x[0] == MODEL][0]
files = g["deployment_files"](m)
pool = ThreadPoolExecutor(max_workers=24)


def sproxy(pod, port, path, method="get", body=None, timeout=120, tries=3):
    """proxy() with retries; never raises (the API-server proxy is the weak
    link at 48 nodes, not the cache)."""
    for attempt in range(tries):
        try:
            return proxy(pod, port, path, method, body, timeout=timeout)
        except Exception:
            time.sleep(2 * (attempt + 1))
    return ""


def safe_metrics(pod):
    """metrics() through the API-server proxy can time out when 47 readers
    are busy; retry, then give up on this sample instead of the run."""
    for attempt in range(3):
        try:
            return metrics(pod)
        except Exception:
            time.sleep(2 * (attempt + 1))
    return None


def log(rec):
    emit(OUT, rec)
    print(json.dumps(rec), flush=True)


def scale_to(n):
    d2, e2 = n * 2 // 3, n - n * 2 // 3
    for name, cnt in (("d2", d2), ("e2", e2)):
        subprocess.run(["az", "aks", "nodepool", "scale", "-g", "fc-scale-rg", "--cluster-name", "fc-scale", "-n", name,
                        "--node-count", str(cnt), "--subscription", SUB, "-o", "none"], capture_output=True, text=True)
    for _ in range(120):
        if len(pods()) == n:
            break
        time.sleep(10)
    time.sleep(30)
    print(f"scaled to {len(pods())} fuse nodes", flush=True)


def set_ring(r, ps):
    list(pool.map(lambda p: sproxy(p, 8092, f"/api/ring?replicas={r}", "post", {}, timeout=60), ps.values()))
    time.sleep(5)


def holder_served(ps):
    def one(item):
        node, pod = item
        m = safe_metrics(pod)
        return node[-12:], int(m.get("fuse_peer_serve_accepted_total", 0)) if m else 0
    return dict(pool.map(one, ps.items()))


def evict(ps):
    def one(pod):
        for f in files:
            sproxy(pod, 8092, f"/api/cache/evict?path=/wl/{f}", "post", {}, timeout=120)
    list(pool.map(one, ps.values()))


def seed(p0):
    for f in files:
        kc("delete", "--raw", f"/api/v1/namespaces/{NS}/pods/{p0}:8092/proxy/api/files/wl/{f}")
    time.sleep(10)
    script = "mkdir -p /host/mnt/wl\n" + "\n".join(
        f"head -c {g['size_bytes'](f)} /dev/urandom > /host/mnt/wl/{f}" for f in files) + "\nsync\n"
    kc("-n", NS, "exec", "-i", p0, "-c", "client", "--", "sh", inp=script, timeout=1800)
    for _ in range(60):
        ok = True
        for f in files:
            try:
                ok = ok and json.loads(sproxy(p0, 8092, f"/api/cache/residency?prefix=/wl/{f}", timeout=30) or "{}").get("fraction", 0) >= 1.0
            except Exception:
                ok = False
        if ok:
            break
        time.sleep(5)
    time.sleep(10)


def wave(n, arm, rep, w, readers, all_pods):
    before = holder_served(all_pods)

    def one(item):
        node, pod = item
        b = safe_metrics(pod)
        t0 = time.time()
        for f in files:
            sproxy(pod, 8092, "/api/cache/warm", "post", {"prefix": f"/wl/{f}", "mode": "full", "timeout_seconds": 600}, timeout=700, tries=1)
        secs = time.time() - t0
        a = safe_metrics(pod)
        d = (lambda k: a.get(k, 0) - b.get(k, 0)) if (a and b) else (lambda k: 0)
        rec = {"n": n, "arm": arm, "rep": rep, "wave": w, "node": node[-12:], "secs": round(secs, 2), "metrics_ok": bool(a and b),
               "peer_mb": round(d("fuse_cache_peer_read_bytes_total") / 2**20, 1),
               "cloud_mb": round(d("fuse_cache_cloud_read_bytes_total") / 2**20, 1)}
        emit(OUT, rec)
        return rec
    recs = list(pool.map(one, readers.items()))
    after = holder_served(all_pods)
    served = {k: after.get(k, 0) - before.get(k, 0) for k in after}
    tot = sum(served.values()) or 1
    secs = sorted(r["secs"] for r in recs)
    log({"n": n, "arm": arm, "rep": rep, "wave": w, "summary": True, "readers": len(recs),
         "median_s": secs[len(secs) // 2], "p90_s": secs[int(0.9 * (len(secs) - 1))], "max_s": secs[-1],
         "cloud_mb": round(sum(r["cloud_mb"] for r in recs)), "holders_serving": sum(1 for v in served.values() if v > 0),
         "top_holder_share": round(max(served.values()) / tot, 2) if served else 0, "reads_served": tot})


def set_absent(nodes_out):
    if nodes_out:
        patch = [{"op": "add", "path": "/spec/template/spec/affinity", "value": {"nodeAffinity": {"requiredDuringSchedulingIgnoredDuringExecution": {
            "nodeSelectorTerms": [{"matchExpressions": [{"key": "kubernetes.io/hostname", "operator": "NotIn", "values": nodes_out}]}]}}}}]
    else:
        patch = [{"op": "remove", "path": "/spec/template/spec/affinity"}]
    kc("-n", NS, "patch", "ds", "fuse-client", "--type=json", "-p", json.dumps(patch))
    kc("-n", NS, "rollout", "status", "ds/fuse-client", "--timeout=900s", timeout=960)
    time.sleep(30)


def run(n, arm, r, rep, loss=False):
    ps = pods()
    nodes = sorted(ps)
    p0 = ps[nodes[0]]
    set_ring(r, ps)
    seed(p0)
    readers = {k: ps[k] for k in nodes[1:]}
    wave(n, arm, rep, "cold", readers, ps)
    time.sleep(60)  # owner fills run once reads go quiet
    evict(readers)
    time.sleep(20)
    if loss:
        gone = nodes[1:5]
        set_absent(gone)
        ps = pods()
        set_ring(r, ps)  # restarted clients would have kept the flag anyway
        readers = {k: ps[k] for k in sorted(ps) if k != nodes[0]}
        wave(n, arm, rep, "loss", readers, ps)
        set_absent([])
    else:
        wave(n, arm, rep, "warm", readers, ps)
        evict(readers)
        time.sleep(10)


def main():
    for n in SIZES:
        print(f"== n={n}", flush=True)
        if len(pods()) != n:
            scale_to(n)
        for rep in range(1, REPS + 1):
            order = (("ring-off", 0), ("ring-on", 2)) if rep % 2 else (("ring-on", 2), ("ring-off", 0))
            for arm, r in order:
                run(n, arm, r, rep)
        if n == max(SIZES):
            for arm, r in (("ring-off", 0), ("ring-on", 2)):
                run(n, arm, r, 1, loss=True)
    print("done", flush=True)


if __name__ == "__main__":
    main()

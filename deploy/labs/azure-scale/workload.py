#!/usr/bin/env python3
"""Real-cluster counterpart of tools/cachesim on the AKS scale lab.

Same catalogue: 10 base images (shared OS/CUDA/Python layers + a runtime layer
each) and 20 models (Zipf popularity), sizes scaled 1:SCALE so a 14-node run
fits in a few hours. Each layer is a file under /wl/; it is seeded once into
fuse-cache (blob storage + one holder) through node 0's API.

A "start" of model m on node n is one POST /api/cache/warm of the files of
m's base image plus its weights on n, timed; the cache fetches each missing
file from a peer or from blob storage (the registry stand-in) and admits it,
evicting under its NVMe budget. Per start it records time, bytes moved and
the node's peer/cloud read deltas, so misses, peer reads and cold fetches can
be told apart. Phases:

  steady    Poisson starts at RATE/h across live nodes for STEADY_MIN
  fail      the fuse client on FAIL_FRAC of the nodes is removed (no
            retraction: their holder rows must age out by lease)
  recover   starts continue on the remaining nodes for RECOVER_MIN

Every 60 s it samples, per node: NVMe used, evictions, holder rows published,
reconcile copies, peer/cloud hits; and from the coordinator: rows per layer,
rows pointing at removed nodes. Output: results/workload.jsonl (starts),
results/samples.jsonl (timeline).
"""
import json, math, os, random, subprocess, sys, threading, time

CTX = os.environ.get("CTX", "fc-scale"); NS = os.environ.get("NS", "fc-scale")
SCALE = int(os.environ.get("SCALE", "100"))
RATE = float(os.environ.get("RATE", "240"))          # starts per hour, fleet-wide
STEADY_MIN = float(os.environ.get("STEADY_MIN", "40"))
RECOVER_MIN = float(os.environ.get("RECOVER_MIN", "30"))
FAIL_FRAC = float(os.environ.get("FAIL_FRAC", "0.25"))
SEED = int(os.environ.get("SEED", "1"))
rng = random.Random(SEED)
os.makedirs("results", exist_ok=True)

def kc(*a, inp=None, timeout=900):
    r = subprocess.run(["kubectl", "--context", CTX] + list(a), input=inp, capture_output=True, text=True, timeout=timeout)
    return r.stdout

def proxy(pod, port, path, method="get", body=None, timeout=900):
    if method == "get":
        return kc("get", "--raw", f"/api/v1/namespaces/{NS}/pods/{pod}:{port}/proxy{path}", timeout=timeout)
    return kc("create", "--raw", f"/api/v1/namespaces/{NS}/pods/{pod}:{port}/proxy{path}", "-f", "-", inp=json.dumps(body or {}), timeout=timeout)

def pods():
    out = kc("-n", NS, "get", "pod", "-l", "app=fuse-client", "-o",
             "jsonpath={range .items[*]}{.spec.nodeName} {.metadata.name} {.status.phase}{'\\n'}{end}")
    res = {}
    for line in out.splitlines():
        p = line.split()
        if len(p) == 3 and p[2] == "Running":
            res[p[0]] = p[1]
    return res

def metrics(pod):
    out = proxy(pod, 8092, "/metrics", timeout=30)
    m = {}
    for line in out.splitlines():
        if line and not line.startswith("#"):
            k, _, v = line.rpartition(" ")
            try:
                m[k] = float(v)
            except ValueError:
                pass
    return m

# ---- catalogue (same shape and sizes as tools/cachesim, MB) ----
shared = {"os-ubuntu-22.04": 80, "cuda-12.4-runtime": 2800, "python-3.11": 120}
runtimes = [("vllm-0.11", 4200), ("vllm-0.10", 4100), ("sglang-0.5", 3900), ("tgi-3.0", 3600), ("llamacpp-b5000", 900),
            ("trtllm-0.18", 6200), ("vllm-cpu-0.11", 1700), ("lmdeploy-0.9", 3300), ("triton-25.01", 5200), ("ollama-0.6", 1100)]
models = [("qwen2.5-0.5b", 1), ("qwen2.5-1.5b", 3.1), ("qwen2.5-7b", 15.2), ("qwen2.5-14b", 29.5), ("qwen2.5-32b", 65),
          ("llama-3.1-8b", 16), ("llama-3.2-3b", 6.4), ("llama-3.3-70b", 141), ("mistral-7b", 14.5), ("mixtral-8x7b", 93),
          ("gemma-2-9b", 18.5), ("gemma-2-27b", 54.6), ("phi-3.5-mini", 7.6), ("phi-4", 29.3), ("deepseek-r1-distill-7b", 15.2),
          ("deepseek-r1-distill-32b", 65.5), ("granite-3.1-8b", 16.3), ("smollm2-1.7b", 3.4), ("olmo-2-7b", 14.6), ("flint-1", 24)]
layers = {}
images = []
for i, (name, mb) in enumerate(runtimes):
    ls = ["os-ubuntu-22.04", "python-3.11"]
    if not any(x in name for x in ("cpu", "llamacpp", "ollama")):
        ls.append("cuda-12.4-runtime")
    layers[f"{name}-runtime"] = mb
    ls.append(f"{name}-runtime")
    images.append(ls)
layers.update(shared)
mods = []
for i, (name, gb) in enumerate(models):
    layers[f"weights-{name}"] = gb * 1024
    base = i % len(images) if gb <= 50 else [0, 2, 5][i % 3]
    mods.append((name, f"weights-{name}", base, 1 / (i + 1) ** 0.9))

def size_bytes(layer):  # scaled, at least 1 MiB
    return max(1 << 20, int(layers[layer] * (1 << 20) / SCALE))

def deployment_files(m):
    name, w, base, _ = mods[m]
    return images[base] + [w]

def pick_model():
    tot = sum(x[3] for x in mods)
    r = rng.random() * tot
    for i, x in enumerate(mods):
        r -= x[3]
        if r <= 0:
            return i
    return len(mods) - 1

# ---- seed: every layer written once through node 0's mount ----
def seed(p0):
    have = kc("-n", NS, "exec", p0, "-c", "client", "--", "sh", "-c", "ls /host/mnt/wl 2>/dev/null | wc -l").strip()
    if have == str(len(layers)):
        print(f"seed: {have} layers already present"); return
    script = "mkdir -p /host/mnt/wl\n" + "\n".join(
        f"[ -s /host/mnt/wl/{l} ] || head -c {size_bytes(l)} /dev/urandom > /host/mnt/wl/{l}" for l in layers) + "\nsync\n"
    t0 = time.time()
    kc("-n", NS, "exec", "-i", p0, "-c", "client", "--", "sh", inp=script, timeout=3600)
    tot = sum(size_bytes(l) for l in layers) / 2**30
    print(f"seed: {len(layers)} layers, {tot:.1f} GiB in {time.time()-t0:.0f}s")
    time.sleep(60)  # let persists and publishes land

# ---- starts ----
out_lock = threading.Lock()
def emit(path, rec):
    with out_lock:
        with open(path, "a") as f:
            f.write(json.dumps(rec) + "\n")

def start(node, pod, m, phase):
    before = metrics(pod)
    t0 = time.time()
    results = []
    for l in deployment_files(m):
        r = proxy(pod, 8092, "/api/cache/warm", "post", {"prefix": f"/wl/{l}", "mode": "full", "timeout_seconds": 600}, timeout=700)
        try:
            results.append(json.loads(r))
        except Exception:
            results.append({"error": r[:200]})
    secs = time.time() - t0
    after = metrics(pod)
    d = lambda k: after.get(k, 0) - before.get(k, 0)
    rec = {"phase": phase, "t": round(t0, 1), "node": node[-12:], "model": mods[m][0], "secs": round(secs, 2),
           "warmed": sum(x.get("warmed", 0) for x in results), "already_local": sum(x.get("already_local", 0) for x in results),
           "failed": sum(x.get("failed", 0) for x in results) + sum(1 for x in results if "error" in x),
           "bytes_mb": round(sum(x.get("bytes", 0) for x in results) / 2**20, 1),
           "peer_mb": round(d("fuse_cache_peer_read_bytes_total") / 2**20, 1),
           "cloud_mb": round(d("fuse_cache_cloud_read_bytes_total") / 2**20, 1),
           "peer_hits": int(d("fuse_cache_peer_hits_total")), "cloud_hits": int(d("fuse_cache_cloud_hits_total"))}
    rec["src"] = "local" if rec["warmed"] == 0 else ("registry" if rec["cloud_mb"] > rec["peer_mb"] else "peer")
    emit("results/workload.jsonl", rec)
    print(json.dumps(rec), flush=True)

# ---- sampler ----
def sampler(stop, removed):
    while not stop.is_set():
        ps = pods()
        rows = {}
        for node, pod in ps.items():
            try:
                m = metrics(pod)
            except Exception:
                continue
            rows[node[-12:]] = {k: m.get(k, 0) for k in (
                "fuse_nvme_used_bytes", "fuse_watermark_evicted_bytes_total", "fuse_cache_evictions_total", "fuse_holder_rows_published",
                "fuse_replica_reconcile_replications_total", "fuse_replica_reconcile_skipped_slots_total",
                "fuse_holder_retractions_pending", "fuse_evict_retracted_total",
                "fuse_peer_fetch_miss_skips_total", "fuse_peer_failure_reports_sent_total",
                "fuse_cache_peer_misses_total", "fuse_cache_cloud_hits_total")}
        coord = kc("-n", NS, "get", "pod", "-l", "app=coordinator", "-o", "jsonpath={.items[0].metadata.name}")
        wv = {}
        try:
            wv = json.loads(proxy(coord, 8080, "/api/worldview?prefix=/wl/", timeout=60))
        except Exception:
            pass
        stale = live = 0
        for f in wv.get("files", []):
            for l in f.get("locations", []):
                if l.get("storage_tier") != "nvme":
                    continue
                live += 1
                if any(l["peer_id"].startswith(r) for r in removed):
                    stale += 1
        emit("results/samples.jsonl", {"t": round(time.time(), 1), "nodes": rows, "nvme_rows": live, "rows_on_removed_nodes": stale})
        stop.wait(60)

def run_phase(name, minutes, nodes_fn):
    end = time.time() + minutes * 60
    lam = RATE / 3600
    threads = []
    while time.time() < end:
        time.sleep(rng.expovariate(lam))
        live = nodes_fn()
        if not live:
            continue
        node = rng.choice(sorted(live))
        t = threading.Thread(target=start, args=(node, live[node], pick_model(), name), daemon=True)
        t.start(); threads.append(t)
        threads = [x for x in threads if x.is_alive()]
    for t in threads:
        t.join(timeout=900)

def main():
    ps = pods()
    if len(ps) < 4:
        sys.exit(f"need fuse-client pods, have {len(ps)}")
    nodes = sorted(ps)
    print(f"{len(nodes)} nodes; catalogue {len(images)} images, {len(mods)} models, {len(layers)} layers, scale 1:{SCALE}")
    seed(ps[nodes[0]])
    removed = []
    stop = threading.Event()
    threading.Thread(target=sampler, args=(stop, removed), daemon=True).start()
    print("phase steady", flush=True)
    run_phase("steady", STEADY_MIN, pods)
    k = max(1, round(len(nodes) * FAIL_FRAC))
    gone = nodes[-k:]
    print(f"phase fail: removing fuse client on {k} nodes", flush=True)
    kc("-n", NS, "patch", "ds", "fuse-client", "--type=json", "-p", json.dumps([{"op": "add", "path": "/spec/template/spec/affinity", "value": {
        "nodeAffinity": {"requiredDuringSchedulingIgnoredDuringExecution": {"nodeSelectorTerms": [{"matchExpressions": [
            {"key": "kubernetes.io/hostname", "operator": "NotIn", "values": gone}]}]}}}}]))
    removed.extend(n for n in gone)
    time.sleep(90)
    print("phase recover", flush=True)
    run_phase("recover", RECOVER_MIN, pods)
    stop.set()
    time.sleep(5)
    # Final per-node counters, read BEFORE any client restart (the last run
    # lost them by restoring the DaemonSet first).
    final = {}
    for node, pod in pods().items():
        try:
            final[node[-12:]] = metrics(pod)
        except Exception:
            pass
    keys = ("fuse_cache_evictions_total", "fuse_evict_retracted_total", "fuse_replica_reconcile_replications_total",
            "fuse_replica_reconcile_skipped_slots_total", "fuse_peer_fetch_miss_skips_total", "fuse_peer_failure_reports_sent_total",
            "fuse_holder_retractions_pending_landed_total", "fuse_holder_vanished_total", "fuse_nvme_used_bytes")
    emit("results/final.json", {k: sum(m.get(k, 0) for m in final.values()) for k in keys} | {"nodes": len(final)})
    print("final", json.dumps({k: sum(m.get(k, 0) for m in final.values()) for k in keys}), flush=True)
    if os.environ.get("RESTORE", "0") == "1":
        kc("-n", NS, "patch", "ds", "fuse-client", "--type=json", "-p", '[{"op":"remove","path":"/spec/template/spec/affinity"}]')
    print("done", flush=True)

if __name__ == "__main__":
    main()

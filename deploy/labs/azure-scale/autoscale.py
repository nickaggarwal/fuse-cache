#!/usr/bin/env python3
"""Autoscaling phases on the scale lab, run after workload.py on the same
cluster and catalogue (reuses its start/sampler code and seeded layers).

  as-pre     RATE/h on the current nodes, 10 min (baseline)
  scale-in   the ddsv6 pool shrinks 8 -> IN_TO while starts continue at
             RATE/h: AKS cordons, drains and deletes the VMs, so those
             nodes leave without retracting their holder rows
  scale-out  the pool grows back to 8 while starts burst at 2 x RATE/h:
             new, empty nodes join; their first starts show whether they
             fill from peers or from blob storage, and how fast they
             become holders others read from

Per start: results/workload.jsonl (phase as-pre / scale-in / scale-out, plus
"new_node": true for nodes that joined during scale-out). Timeline:
results/samples.jsonl. Join/leave times: results/autoscale-events.jsonl.
"""
import json, os, subprocess, threading, time

src = open(os.path.join(os.path.dirname(os.path.abspath(__file__)), "workload.py")).read().split("def main():")[0]
g = {"__file__": "workload.py"}
exec(src, g)
SUB = subprocess.run(["bash", "-c", "source ./env.sh && echo $SUB"], capture_output=True, text=True).stdout.strip()
RG, CL = "fc-scale-rg", "fc-scale"
POOL = os.environ.get("POOL", "ddsv6")
IN_TO = int(os.environ.get("IN_TO", "4"))
OUT_TO = int(os.environ.get("OUT_TO", "8"))
RATE = float(os.environ.get("RATE", "480"))
PRE_MIN = float(os.environ.get("PRE_MIN", "10"))
IN_MIN = float(os.environ.get("IN_MIN", "15"))
OUT_MIN = float(os.environ.get("OUT_MIN", "25"))

def event(rec):
    rec["t"] = round(time.time(), 1)
    g["emit"]("results/autoscale-events.jsonl", rec)
    print(json.dumps(rec), flush=True)

def scale(n):
    t0 = time.time()
    event({"event": "scale-start", "pool": POOL, "to": n})
    r = subprocess.run(["az", "aks", "nodepool", "scale", "-g", RG, "--cluster-name", CL, "-n", POOL,
                        "--node-count", str(n), "--subscription", SUB, "-o", "none"], capture_output=True, text=True)
    event({"event": "scale-done", "pool": POOL, "to": n, "secs": round(time.time() - t0), "rc": r.returncode, "err": r.stderr[-300:]})

def load(phase, minutes, rate, initial):
    end = time.time() + minutes * 60
    lam = rate / 3600
    threads = []
    seen = set(initial)
    while time.time() < end:
        time.sleep(g["rng"].expovariate(lam))
        live = g["pods"]()
        for n in live:
            if n not in seen:
                seen.add(n)
                event({"event": "node-joined", "node": n[-12:]})
        for n in list(seen):
            if n not in live and n in initial:
                pass
        if not live:
            continue
        node = g["rng"].choice(sorted(live))
        def run(node=node, pod=live[node]):
            g["start"](node, pod, g["pick_model"](), phase + ("-new" if node not in initial else ""))
        t = threading.Thread(target=run, daemon=True)
        t.start(); threads.append(t)
        threads = [x for x in threads if x.is_alive()]
    for t in threads:
        t.join(timeout=900)

def main():
    initial = set(g["pods"]())
    print(f"autoscale: {len(initial)} live fuse nodes, pool {POOL}: 8 -> {IN_TO} -> {OUT_TO}", flush=True)
    removed = []
    stop = threading.Event()
    threading.Thread(target=g["sampler"], args=(stop, removed), daemon=True).start()

    print("phase as-pre", flush=True)
    load("as-pre", PRE_MIN, RATE, initial)

    print("phase scale-in", flush=True)
    before = set(g["pods"]())
    th = threading.Thread(target=scale, args=(IN_TO,)); th.start()
    load("scale-in", IN_MIN, RATE, initial)
    th.join()
    after = set(g["pods"]())
    gone = sorted(before - after)
    removed.extend(gone)
    event({"event": "nodes-left", "nodes": [n[-12:] for n in gone]})

    print("phase scale-out", flush=True)
    base = set(g["pods"]())
    th = threading.Thread(target=scale, args=(OUT_TO,)); th.start()
    load("scale-out", OUT_MIN, 2 * RATE, base)
    th.join()
    stop.set()
    time.sleep(5)
    final = {}
    for node, pod in g["pods"]().items():
        try:
            final[node[-12:]] = g["metrics"](pod)
        except Exception:
            pass
    keys = ("fuse_cache_evictions_total", "fuse_evict_retracted_total", "fuse_replica_reconcile_replications_total",
            "fuse_replica_reconcile_skipped_slots_total", "fuse_peer_fetch_miss_skips_total", "fuse_holder_rows_published")
    g["emit"]("results/autoscale-final.json", {k: sum(m.get(k, 0) for m in final.values()) for k in keys} | {"nodes": len(final)})
    print("final", json.dumps({k: sum(m.get(k, 0) for m in final.values()) for k in keys} | {"nodes": len(final)}), flush=True)
    print("done", flush=True)

if __name__ == "__main__":
    main()

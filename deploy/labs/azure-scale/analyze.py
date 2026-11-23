#!/usr/bin/env python3
"""Summarise a workload.py run: starts by phase and source, start time
percentiles, misses and cold fetches over time, eviction, holder rows on
removed nodes, and reconcile copies. Usage: ./analyze.py [results-dir]"""
import json, sys, statistics as st
d = sys.argv[1] if len(sys.argv) > 1 else "results"
starts = [json.loads(l) for l in open(f"{d}/workload.jsonl")]
samples = [json.loads(l) for l in open(f"{d}/samples.jsonl")] if __import__("os").path.exists(f"{d}/samples.jsonl") else []
t0 = min(s["t"] for s in starts)

def pct(xs, p):
    xs = sorted(xs)
    return xs[int(p * (len(xs) - 1))] if xs else 0

print(f"{'phase':13} {'starts':>6} {'local':>6} {'peer':>6} {'reg':>6} {'p50 s':>6} {'p95 s':>6} {'peer GB':>8} {'reg GB':>7} {'failed':>6}")
for ph in ("steady", "recover", "as-pre", "scale-in", "scale-in-new", "scale-out", "scale-out-new"):
    ss = [s for s in starts if s["phase"] == ph]
    if not ss:
        continue
    src = {k: sum(1 for s in ss if s["src"] == k) for k in ("local", "peer", "registry")}
    print(f"{ph:13} {len(ss):6} {src['local']:6} {src['peer']:6} {src['registry']:6} {pct([s['secs'] for s in ss], .5):6.1f} {pct([s['secs'] for s in ss], .95):6.1f} "
          f"{sum(s['peer_mb'] for s in ss)/1024:8.2f} {sum(s['cloud_mb'] for s in ss)/1024:7.2f} {sum(s['failed'] for s in ss):6}")

print("\nper 10 min: starts, % fully local, registry GB (cold fetches), mean start s")
bins = {}
for s in starts:
    b = int((s["t"] - t0) // 600)
    bins.setdefault(b, []).append(s)
for b in sorted(bins):
    ss = bins[b]
    print(f"  {b*10:3d}-{b*10+10:3d} min  {len(ss):4d}  local {100*sum(1 for s in ss if s['src']=='local')/len(ss):5.1f}%  reg {sum(s['cloud_mb'] for s in ss)/1024:6.2f} GB  mean {st.mean(s['secs'] for s in ss):5.2f} s  [{ss[0]['phase']}]")

if samples:
    print("\ntimeline (every ~5 min): nvme GB used (fleet), evictions (fleet), reconcile copies, holder rows, rows on removed nodes, slot-capped passes")
    for i, sm in enumerate(samples):
        if i % 5:
            continue
        n = sm["nodes"].values()
        print(f"  {(sm['t']-t0)/60:5.1f} min  nvme {sum(x['fuse_nvme_used_bytes'] for x in n)/2**30:6.1f}  evictions {int(sum(x.get('fuse_cache_evictions_total',0) for x in n)):5d}"
              f"  copies {int(sum(x['fuse_replica_reconcile_replications_total'] for x in n)):5d}  rows {sm['nvme_rows']:4d}  stale {sm['rows_on_removed_nodes']:3d}"
              f"  slot-capped {int(sum(x['fuse_replica_reconcile_skipped_slots_total'] for x in n)):4d}  nodes {len(sm['nodes'])}")

#!/usr/bin/env python3
"""Summarise ring-sweep.py: per size, wave and arm, the mean over reps of
median / p90 / max start time, holder spread, and blob-storage bytes."""
import json, sys, statistics as st
from collections import defaultdict
rows = [json.loads(l) for l in open(sys.argv[1] if len(sys.argv) > 1 else "results/sweep.jsonl")]
summ = [r for r in rows if r.get("summary")]
grp = defaultdict(list)
for r in summ:
    grp[(r["n"], r["wave"], r["arm"])].append(r)
print(f"{'n':>3} {'wave':5} {'arm':8} {'reps':>4} {'median s':>9} {'p90 s':>7} {'max s':>7} {'holders':>7} {'top':>5} {'reads':>7} {'blob MB':>8}")
for n in sorted({k[0] for k in grp}, reverse=True):
    for w in ("cold", "warm", "loss"):
        for arm in ("ring-off", "ring-on"):
            rs = grp.get((n, w, arm))
            if not rs:
                continue
            m = lambda k: st.mean(r[k] for r in rs)
            spread = "" if len(rs) < 2 else " (" + "/".join(f"{r['median_s']:.1f}" for r in rs) + ")"
            print(f"{n:3} {w:5} {arm:8} {len(rs):4} {m('median_s'):9.1f} {m('p90_s'):7.1f} {m('max_s'):7.1f} {m('holders_serving'):7.0f} {m('top_holder_share'):5.2f} {m('reads_served'):7.0f} {m('cloud_mb'):8.0f}{spread}")

#!/usr/bin/env python3
"""Median table for seq-focus.sh / ablate-seq.sh / run-tuned.sh output.

python3 -I focus-summary.py FILE [--md]
Ratios are against the "go" label in the same layer/mode/case.
"""
import collections, json, statistics as st, sys

rows = [json.loads(l) for l in open(sys.argv[1]) if l.startswith("{")]
md = "--md" in sys.argv
g = collections.defaultdict(list)
p99 = collections.defaultdict(list)
labels = []
for r in rows:
    if r["label"] not in labels:
        labels.append(r["label"])
    k = (r["layer2"], r["mode"], r["pattern"], r["window_kib"], r["threads"], r["label"])
    g[k].append(r["mb_per_s"])
    p99[k].append(r["p99_us"])
cells = sorted({k[:5] for k in g}, key=lambda c: (c[0], c[1], c[2] != "seq", c[4], c[3]))
if md:
    print("| layer | mode | case | " + " | ".join(labels) + " |")
    print("|---|---|---|" + "---:|" * len(labels))
else:
    print(f"{'layer':8} {'mode':5} {'case':14} " + " ".join(f"{l:>15}" for l in labels))
for c in cells:
    go = st.median(g[c + ("go",)]) if g.get(c + ("go",)) else None
    out = []
    for l in labels:
        v = g.get(c + (l,))
        if not v:
            out.append("-" if md else f"{'-':>15}")
            continue
        m = st.median(v)
        ratio = f" ({m/go:.2f}x)" if go and l != "go" else ""
        if md:
            out.append(f"{m:,.0f}{ratio}")
        else:
            out.append(f"{m:8.0f}" + (f" {m/go:5.2f}x" if go and l != "go" else "       "))
    case = f"{c[2]} {c[3]}K t{c[4]}"
    if md:
        print(f"| {c[0]} | {c[1]} | {case} | " + " | ".join(out) + " |")
    else:
        print(f"{c[0]:8} {c[1]:5} {case:14} " + " ".join(out))

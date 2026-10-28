#!/usr/bin/env python3
"""Turn the JSONL from run-all.sh into Markdown tables (median of reps).

usage: python3 -I summarize.py RESULTS_DIR
"""
import collections
import json
import pathlib
import statistics as st
import sys


def load(path):
    p = pathlib.Path(path)
    if not p.exists():
        return []
    return [json.loads(l) for l in p.read_text().splitlines() if l.startswith("{")]


def med(rows, f):
    return st.median(r[f] for r in rows)


def table(rows, keys, label_fn, impls=("go", "rust"), cpu="cpu_s_per_gib"):
    g = collections.defaultdict(list)
    for r in rows:
        g[tuple(r.get(k) for k in keys) + (r["impl"],)].append(r)
    cells = sorted({k[:-1] for k in g})
    out = [
        "| case | Go MB/s | Rust MB/s | speedup | Go p50 us | Rust p50 us | Go p99 us | Rust p99 us | Go CPU s/GiB | Rust CPU s/GiB |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for c in cells:
        a, b = g.get(c + (impls[0],)), g.get(c + (impls[1],))
        if not a or not b:
            continue
        sp = med(b, "mb_per_s") / med(a, "mb_per_s")
        out.append(
            f"| {label_fn(c)} | {med(a,'mb_per_s'):,.0f} | {med(b,'mb_per_s'):,.0f} | {sp:.2f}x | "
            f"{med(a,'p50_us'):,.1f} | {med(b,'p50_us'):,.1f} | {med(a,'p99_us'):,.0f} | {med(b,'p99_us'):,.0f} | "
            f"{med(a,cpu):.3f} | {med(b,cpu):.3f} |"
        )
    return "\n".join(out)


def main(d):
    d = pathlib.Path(d)
    for name in ("library-warm", "library-cold", "library-warm-t8"):
        rows = load(d / f"{name}.jsonl")
        if rows:
            print(f"\n### {name} (threads={rows[0]['threads']}, median of {max(r['rep'] for r in rows)})\n")
            print(table(rows, ("layout", "pattern", "window_kib"), lambda c: f"{c[0]} {c[1]} {c[2]}K"))
    for name in ("fuse-cold", "fuse-warm", "fuse-cold-t8"):
        rows = load(d / f"{name}.jsonl")
        for r in rows:
            r["impl"] = {"go": "go", "rs": "rust"}[r["impl"]]
        if rows:
            print(f"\n### {name} (threads={rows[0]['threads']}, median of {max(r['rep'] for r in rows)})\n")
            cpu = "server_cpu_s_per_gib" if "server_cpu_s_per_gib" in rows[0] else "cpu_s_per_gib"
            print(f"CPU column = {'FUSE daemon' if cpu.startswith('server') else 'client'} CPU per GiB\n")
            print(table(rows, ("pattern", "window_kib"), lambda c: f"{c[0]} {c[1]}K", cpu=cpu))
    rows = load(d / "peer.jsonl")
    if rows:
        for r in rows:
            r["server"] = r["note"].split()[0].split("=")[1]
        print(f"\n### peer transport, 16 MiB chunk objects over loopback (median of {max(r['rep'] for r in rows)})\n")
        print(table(rows, ("server", "threads"), lambda c: f"server={c[0]} threads={c[1]}"))
    rows = load(d / "azure.jsonl")
    if rows:
        print(f"\n### Azure chunk objects (16 MiB, Azurite emulator, median of {max(r['rep'] for r in rows)})\n")
        print(table(rows, ("threads",), lambda c: f"threads={c[0]}"))


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else "/tmp/rb/results")

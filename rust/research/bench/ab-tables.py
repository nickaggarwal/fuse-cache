#!/usr/bin/env python3
"""Markdown A/B tables from a node-bench results.json.

python3 -I ab-tables.py results.json
"""
import json
import sys

d = json.load(open(sys.argv[1]))
by = {(s["layer"], s["mode"], s["pattern"], s["window_kib"], s["threads"], s["impl"]): s for s in d["summary"]}


def cell(s, go):
    if not s:
        return "–"
    r = f" ({s['mb_per_s'] / go['mb_per_s']:.2f}x)" if go and s is not go else ""
    return f"{s['mb_per_s']:,.0f}{r}"


order = [("cold", "seq", 128, 1), ("cold", "seq", 1024, 1), ("warm", "seq", 128, 1), ("warm", "seq", 1024, 1),
         ("cold", "rand", 128, 1), ("cold", "seq", 1024, 8)]
if any(k[0] == "fuse" for k in by):
    print("### FUSE (whole-layout files; MB/s median, ratio vs go; [daemon CPU s/GiB])\n")
    impls = [i for i in ("go", "go-pt", "go-ra4m", "rust", "rust-pt") if any(k[0] == "fuse" and k[5] == i for k in by)]
    print("| case | " + " | ".join(impls) + " |")
    print("|---|" + "---:|" * len(impls))
    for m, p, w, t in order:
        go = by.get(("fuse", m, p, w, t, "go"))
        cols = []
        for i in impls:
            s = by.get(("fuse", m, p, w, t, i))
            cpu = s.get("server_cpu_s_per_gib") if s else None
            cols.append(cell(s, go) + (f" [{cpu:.2f}]" if cpu is not None else ""))
        print(f"| {m} {p} {w}K x{t} | " + " | ".join(cols) + " |")

if any(k[0].startswith("blob") for k in by):
    print("\n### Blob endpoint (4 connections; MB/s median [min–max], p99 ms, CPU s/GiB: serving daemon (+ holder for remote))\n")
    print("| case | Go (main) | Rust | Rust/Go | Go p99 | Rust p99 | Go CPU | Rust CPU |")
    print("|---|---:|---:|---:|---:|---:|---:|---:|")
    for layer in ("blob-local", "blob-remote"):
        for m, p, w in [("cold", "rand", 1024), ("cold", "seq", 1024), ("cold", "rand", 128), ("warm", "rand", 1024)]:
            g, r = by.get((layer, m, p, w, 4, "go")), by.get((layer, m, p, w, 4, "rust"))
            if not g or not r:
                continue

            def cpu(s):
                h = s.get("holder_cpu_s_per_gib")
                return f"{s['server_cpu_s_per_gib']:.2f}" + (f" + {h:.2f}" if h is not None else "")

            print(f"| {layer[5:]} {m} {p} {w}K | {g['mb_per_s']:,.0f} [{g['mb_per_s_min']:,.0f}–{g['mb_per_s_max']:,.0f}] | "
                  f"{r['mb_per_s']:,.0f} [{r['mb_per_s_min']:,.0f}–{r['mb_per_s_max']:,.0f}] | {r['mb_per_s'] / g['mb_per_s']:.2f}x | "
                  f"{g['p99_us'] / 1000:.1f} | {r['p99_us'] / 1000:.1f} | {cpu(g)} | {cpu(r)} |")

if any(k[0] == "library" for k in by):
    print("\n### In-process read path (per-chunk files; MB/s median)\n")
    print("| case | go | rust |")
    print("|---|---:|---:|")
    for m, p, w, t in order:
        go, rs = by.get(("library", m, p, w, t, "go")), by.get(("library", m, p, w, t, "rust"))
        if go and rs:
            print(f"| {m} {p} {w}K x{t} | {go['mb_per_s']:,.0f} | {cell(rs, go)} |")

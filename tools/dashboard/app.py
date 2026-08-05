"""fuse-client cache dashboard.

Monitoring + warm targeting UI for the distributed FUSE cache. Every field,
endpoint and metric name here is taken from the Go source; see fuse_api.py for
the endpoint map.

Run:  streamlit run tools/dashboard/app.py
"""

from __future__ import annotations

import os
import re
import time
from datetime import datetime, timezone
from html import escape
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import streamlit as st

from fuse_api import FuseClient, Peer, select_warm_targets
from metrics import MetricSet

# --------------------------------------------------------------------------
# Palette (dataviz reference instance; validated with scripts/validate_palette.js)
#   categorical light  #2a78d6 / #eb6834 / #1baf7a   -> all-pairs PASS
#   categorical dark   #3987e5 / #d95926 / #199e70   -> all-pairs PASS
#   status             good #0ca30c  critical #d03b3b
# Light-mode aqua sits at 2.74:1 vs the surface -> relief rule: every chart
# below ships visible labels AND a table view, so nothing is color-only.
# --------------------------------------------------------------------------

VIZ_CSS = """
<style>
.viz-root {
  color-scheme: light;
  --surface-1: #fcfcfb;
  --page-plane: #f9f9f7;
  --text-primary: #0b0b0b;
  --text-secondary: #52514e;
  --text-muted: #898781;
  --gridline: #e1e0d9;
  --baseline: #c3c2b7;
  --border: rgba(11,11,11,0.10);
  --series-1: #2a78d6;
  --series-2: #eb6834;
  --series-3: #1baf7a;
  --status-good: #0ca30c;
  --status-warning: #fab219;
  --status-critical: #d03b3b;
  --track: #cde2fb;
  font-family: system-ui, -apple-system, "Segoe UI", sans-serif;
}
@media (prefers-color-scheme: dark) {
  :root:where(:not([data-theme="light"])) .viz-root {
    color-scheme: dark;
    --surface-1: #1a1a19;
    --page-plane: #0d0d0d;
    --text-primary: #ffffff;
    --text-secondary: #c3c2b7;
    --text-muted: #898781;
    --gridline: #2c2c2a;
    --baseline: #383835;
    --border: rgba(255,255,255,0.10);
    --series-1: #3987e5;
    --series-2: #d95926;
    --series-3: #199e70;
    --track: #184f95;
  }
}
:root[data-theme="dark"] .viz-root {
  color-scheme: dark;
  --surface-1: #1a1a19;
  --page-plane: #0d0d0d;
  --text-primary: #ffffff;
  --text-secondary: #c3c2b7;
  --text-muted: #898781;
  --gridline: #2c2c2a;
  --baseline: #383835;
  --border: rgba(255,255,255,0.10);
  --series-1: #3987e5;
  --series-2: #d95926;
  --series-3: #199e70;
  --track: #184f95;
}
.viz-card {
  background: var(--surface-1);
  border: 1px solid var(--border);
  border-radius: 10px;
  padding: 16px 18px 18px;
  margin-bottom: 8px;
}
.viz-title {
  font-size: 13px; font-weight: 600; color: var(--text-primary);
  margin: 0 0 2px;
}
.viz-sub { font-size: 12px; color: var(--text-muted); margin: 0 0 14px; }
.viz-legend { display: flex; flex-wrap: wrap; gap: 14px; margin: 0 0 12px; }
.viz-legend span { font-size: 12px; color: var(--text-secondary);
  display: inline-flex; align-items: center; gap: 6px; }
.viz-swatch { width: 10px; height: 10px; border-radius: 2px; display: inline-block; }
.viz-row { display: grid; grid-template-columns: 230px 1fr 130px;
  align-items: center; gap: 12px; margin-bottom: 10px; }
/* Node IDs share a long VMSS prefix, so truncate from the LEFT (rtl + plaintext
   keeps the glyph order but moves the ellipsis to the front). */
.viz-rowlabel { font-size: 12px; color: var(--text-secondary);
  overflow: hidden; text-overflow: ellipsis; white-space: nowrap;
  direction: rtl; unicode-bidi: plaintext; text-align: left; }
.viz-rowvalue { font-size: 12px; color: var(--text-secondary);
  text-align: right; font-variant-numeric: tabular-nums; }
.viz-track { position: relative; height: 14px; border-radius: 3px;
  background: var(--gridline); overflow: visible; }
.viz-fill { height: 14px; border-radius: 0 4px 4px 0; }
.viz-stack { display: flex; height: 18px; width: 100%; }
.viz-seg:first-child { border-radius: 4px 0 0 4px; }
.viz-seg:last-child { border-radius: 0 4px 4px 0; }
.viz-cols { display: flex; align-items: flex-end; gap: 10px;
  height: 150px; padding-top: 20px; }
.viz-col { flex: 1; display: flex; flex-direction: column;
  justify-content: flex-end; align-items: center; height: 100%; }
.viz-colbar { width: 100%; max-width: 24px; border-radius: 4px 4px 0 0; }
.viz-collabel { font-size: 11px; color: var(--text-muted); margin-top: 6px;
  font-variant-numeric: tabular-nums; }
.viz-colvalue { font-size: 11px; color: var(--text-secondary); margin-bottom: 6px;
  font-variant-numeric: tabular-nums; }
.viz-axis { border-top: 1px solid var(--baseline); margin-top: 0; }
.viz-empty { font-size: 12px; color: var(--text-muted); padding: 8px 0 4px; }
.chip { display: inline-block; font-size: 11px; padding: 1px 8px;
  border-radius: 10px; border: 1px solid var(--border);
  color: var(--text-secondary); margin-right: 4px; }
</style>
"""

S1, S2, S3 = "var(--series-1)", "var(--series-2)", "var(--series-3)"
CRITICAL, GOOD = "var(--status-critical)", "var(--status-good)"

# Herd-control counters, exactly as emitted by handlePromMetrics.
HERD_METRICS: List[Tuple[str, str]] = [
    ("fuse_peer_serve_inflight", "Peer serve in-flight"),
    ("fuse_peer_serve_capacity", "Peer serve capacity"),
    ("fuse_peer_serve_accepted_total", "Peer serves accepted"),
    ("fuse_peer_serve_rejected_total", "Peer serves rejected (busy)"),
    ("fuse_peer_fetch_busy_skips_total", "Fetch busy skips"),
    ("fuse_peer_fetch_jitter_retries_total", "Fetch jitter retries"),
    ("fuse_peer_replication_busy_skips_total", "Replication busy skips"),
    ("fuse_peer_replication_staggers_total", "Replication staggers"),
    ("fuse_fetch_lease_granted_total", "Fetch leases granted (winner)"),
    ("fuse_fetch_lease_denied_total", "Fetch leases denied (follower)"),
    ("fuse_fetch_lease_errors_total", "Fetch lease errors"),
    ("fuse_fetch_lease_follower_peer_hits_total", "Follower peer hits"),
    ("fuse_fetch_lease_follower_cloud_fallback_total", "Follower cloud fallbacks"),
    ("fuse_chunk_advertise_published_total", "Chunk advertisements published"),
    ("fuse_replica_reconcile_runs_total", "Reconciler passes"),
    ("fuse_replica_reconcile_replications_total", "Reconciler replications"),
    ("fuse_replica_reconcile_heat_boosts_total", "Reconciler heat boosts"),
    ("fuse_replica_reconcile_skipped_busy_total", "Reconciler passes skipped (busy)"),
    ("fuse_busy_chunk_retries_total", "Busy chunk retries"),
    ("fuse_busy_chunk_retry_hits_total", "Busy chunk retry hits"),
    ("fuse_chunk_completion_assembled_total", "Chunk completions assembled"),
    ("fuse_chunk_completion_fetched_total", "Chunk completion fetches"),
    ("fuse_chunk_completion_skipped_total", "Chunk completions skipped"),
]

# --------------------------------------------------------------------------
# helpers
# --------------------------------------------------------------------------


def human_bytes(n: Optional[float]) -> str:
    try:
        n = float(n or 0)
    except (TypeError, ValueError):
        return "-"
    sign = "-" if n < 0 else ""
    n = abs(n)
    for unit in ("B", "KiB", "MiB", "GiB", "TiB", "PiB"):
        if n < 1024 or unit == "PiB":
            return f"{sign}{n:,.0f} {unit}" if unit == "B" else f"{sign}{n:,.1f} {unit}"
        n /= 1024.0
    return f"{sign}{n:,.1f} PiB"


def compact(n: Optional[float]) -> str:
    try:
        n = float(n or 0)
    except (TypeError, ValueError):
        return "-"
    for limit, suffix in ((1e12, "T"), (1e9, "B"), (1e6, "M"), (1e3, "K")):
        if abs(n) >= limit:
            return f"{n / limit:,.1f}{suffix}"
    return f"{n:,.0f}"


def parse_go_time(raw: str) -> Optional[datetime]:
    """Go marshals time.Time as RFC3339 with up to 9 fractional digits."""
    if not raw:
        return None
    text = raw.strip().replace("Z", "+00:00")
    text = re.sub(r"\.(\d{6})\d+", r".\1", text)
    try:
        dt = datetime.fromisoformat(text)
    except ValueError:
        return None
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def heartbeat_age_seconds(peer: Peer) -> Optional[float]:
    dt = parse_go_time(peer.last_heartbeat)
    if dt is None:
        return None
    return (datetime.now(timezone.utc) - dt).total_seconds()


def as_int(value: Any) -> int:
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return 0


def as_float(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def peer_capacity(peer: Peer) -> int:
    # cmd/client/main.go reports available = capacity - used, used = used.
    return peer.available_space + peer.used_space


def render_warm_counters(counters: Dict[str, Any]) -> None:
    """Six-metric grid shared by WarmupResult and WarmupProgress (both
    snake_case since internal/cache/warmup.go grew json tags)."""
    r1, r2, r3 = st.columns(3)
    r1.metric("Files found", as_int(counters.get("files")))
    r2.metric("Warmed", as_int(counters.get("warmed")))
    r3.metric("Bytes landed", human_bytes(counters.get("bytes")))
    r4, r5, r6 = st.columns(3)
    r4.metric("Already local", as_int(counters.get("already_local")))
    r5.metric("Skipped", as_int(counters.get("skipped")))
    r6.metric("Failed", as_int(counters.get("failed")))


def warm_job_row(job: Dict[str, Any]) -> Dict[str, Any]:
    """One row of the async warm-job table."""
    prog = job.get("progress") or {}
    files = as_int(prog.get("files"))
    done = as_int(prog.get("done"))
    chunks = as_int(prog.get("chunks"))
    chunks_done = as_int(prog.get("chunks_done"))
    started = parse_go_time(job.get("started_at") or "")
    updated = parse_go_time(job.get("updated_at") or "")
    elapsed = (updated - started).total_seconds() if started and updated else 0.0
    return {
        "job": job.get("id", ""),
        "prefix": job.get("prefix", ""),
        "mode": job.get("mode", ""),
        "status": job.get("status", ""),
        "progress": f"{done}/{files}" if files else str(done),
        # Chunk counters move during a single large file, where files stays 0/1.
        "chunks": f"{chunks_done}/{chunks}" if chunks else "-",
        "warmed": as_int(prog.get("warmed")),
        "failed": as_int(prog.get("failed")),
        # Landed bytes = completed files plus partial progress on open ones.
        "bytes": human_bytes(as_int(prog.get("bytes")) + as_int(prog.get("in_flight_bytes"))),
        "elapsed": f"{elapsed:.0f}s",
        "error": (job.get("error") or "")[:60],
    }


# --------------------------------------------------------------------------
# chart primitives (HTML/CSS marks; thin, gapped, direct-labeled)
# --------------------------------------------------------------------------


def card(title: str, subtitle: str, body: str) -> str:
    sub = f'<p class="viz-sub">{escape(subtitle)}</p>' if subtitle else ""
    return (
        f'<div class="viz-root"><div class="viz-card">'
        f'<p class="viz-title">{escape(title)}</p>{sub}{body}</div></div>'
    )


def legend(items: List[Tuple[str, str]]) -> str:
    parts = "".join(
        f'<span><i class="viz-swatch" style="background:{color}"></i>{escape(label)}</span>'
        for label, color in items
    )
    return f'<div class="viz-legend">{parts}</div>'


def hbar_meter_chart(
    rows: List[Tuple[str, float, float, str]], color: str = S1
) -> str:
    """One meter per row: (label, value, limit, value_text).

    Single series -> one hue, no legend box (the title names it). The track is
    a lighter step of the same ramp; fills carry a 4px rounded data-end.
    """
    if not rows:
        return '<p class="viz-empty">No nodes reporting capacity.</p>'
    out = []
    for label, value, limit, text in rows:
        pct = 0.0 if limit <= 0 else max(0.0, min(1.0, value / limit)) * 100.0
        fill_color = color
        if pct >= 90:
            fill_color = CRITICAL
        elif pct >= 75:
            fill_color = "var(--status-warning)"
        out.append(
            f'<div class="viz-row" title="{escape(label)}: {escape(text)}">'
            f'<div class="viz-rowlabel">{escape(label)}</div>'
            f'<div class="viz-track" style="background:var(--track)">'
            f'<div class="viz-fill" style="width:{pct:.2f}%;background:{fill_color}"></div>'
            f"</div>"
            f'<div class="viz-rowvalue">{escape(text)}</div></div>'
        )
    return "".join(out)


def stacked_share_bar(segments: List[Tuple[str, float, str]]) -> str:
    """One horizontal stacked bar: share of a whole across ≤3 categories.

    2px surface gaps separate segments (never a stroke). Values are labeled
    beneath the bar, so identity is never color-alone.
    """
    total = sum(max(0.0, v) for _, v, _ in segments)
    if total <= 0:
        return '<p class="viz-empty">No reads recorded yet.</p>'
    segs, labels = [], []
    for idx, (name, value, color) in enumerate(segments):
        pct = max(0.0, value) / total * 100.0
        gap = "margin-left:2px;" if idx else ""
        segs.append(
            f'<div class="viz-seg" style="width:{pct:.3f}%;background:{color};{gap}" '
            f'title="{escape(name)}: {compact(value)} hits ({pct:.1f}%)"></div>'
        )
        labels.append(
            f'<span><i class="viz-swatch" style="background:{color}"></i>'
            f"{escape(name)} &nbsp;{compact(value)} ({pct:.1f}%)</span>"
        )
    return (
        f'<div class="viz-stack">{"".join(segs)}</div>'
        f'<div class="viz-legend" style="margin-top:12px">{"".join(labels)}</div>'
    )


def column_histogram(
    bins: List[Tuple[str, float]], colors: List[str], value_fmt=compact
) -> str:
    """Columns for an ordered distribution. Value on the cap; solid hairline
    baseline; 24px cap on bar thickness."""
    if not bins:
        return '<p class="viz-empty">No files matched.</p>'
    peak = max((v for _, v in bins), default=0) or 1
    cols = []
    for (label, value), color in zip(bins, colors):
        height = 0 if value <= 0 else max(3.0, value / peak * 100.0)
        cols.append(
            f'<div class="viz-col" title="{escape(label)}: {value_fmt(value)}">'
            f'<div class="viz-colvalue">{value_fmt(value) if value else ""}</div>'
            f'<div class="viz-colbar" style="height:{height:.1f}%;background:{color}"></div>'
            f"</div>"
        )
    ticks = "".join(
        f'<div style="flex:1;text-align:center" class="viz-collabel">{escape(l)}</div>'
        for l, _ in bins
    )
    return (
        f'<div class="viz-cols">{"".join(cols)}</div>'
        f'<div class="viz-axis"></div>'
        f'<div style="display:flex;gap:10px">{ticks}</div>'
    )


# --------------------------------------------------------------------------
# cached fetchers (keyed on plain strings so st.cache_data can hash them)
# --------------------------------------------------------------------------


def client_for(url: str, key: str) -> FuseClient:
    return FuseClient(url, key)


@st.cache_data(ttl=10, show_spinner=False)
def fetch_peers(url: str, key: str, _bust: int = 0):
    res = client_for(url, key).list_peers()
    return (res.ok, res.data, res.error)


@st.cache_data(ttl=10, show_spinner=False)
def fetch_cluster_stats(url: str, key: str, _bust: int = 0):
    res = client_for(url, key).cluster_stats()
    return (res.ok, res.data, res.error)


@st.cache_data(ttl=10, show_spinner=False)
def fetch_peer_cache_stats(url: str, key: str, address: str, _bust: int = 0):
    res = client_for(url, key).peer_cache_stats(address)
    return (res.ok, res.data, res.error)


@st.cache_data(ttl=10, show_spinner=False)
def fetch_peer_metrics(url: str, key: str, address: str, _bust: int = 0):
    res = client_for(url, key).peer_metrics_text(address)
    return (res.ok, res.data, res.error)


@st.cache_data(ttl=20, show_spinner=False)
def fetch_worldview(url: str, key: str, prefix: str, _bust: int = 0):
    res = client_for(url, key).worldview(prefix)
    return (res.ok, res.data, res.error)


@st.cache_data(ttl=20, show_spinner=False)
def fetch_file_location(url: str, key: str, path: str, _bust: int = 0):
    res = client_for(url, key).file_location(path)
    return (res.ok, res.data, res.error)


# --------------------------------------------------------------------------
# sidebar
# --------------------------------------------------------------------------

st.set_page_config(page_title="fuse-client cache", page_icon=None, layout="wide")
st.markdown(VIZ_CSS, unsafe_allow_html=True)

REFRESH_CHOICES = {"Off": 0, "5s": 5, "10s": 10, "30s": 30, "60s": 60}

with st.sidebar:
    st.markdown("### fuse-client")
    coordinator_url = st.text_input(
        "Coordinator URL",
        value=os.environ.get("FUSE_COORDINATOR_URL", "http://localhost:8080"),
        help="Coordinator HTTP API. Env default: FUSE_COORDINATOR_URL",
    ).strip()
    api_key = st.text_input(
        "API key",
        value=os.environ.get("FUSE_API_KEY", ""),
        type="password",
        help=(
            "Sent as X-API-Key to client daemons (the coordinator has no auth "
            "middleware). Env default: FUSE_API_KEY"
        ),
    ).strip()
    node_override = st.text_input(
        "Node address override",
        value=os.environ.get("FUSE_NODE_ADDR", ""),
        help=(
            "PeerInfo.address is the pod IP (POD_IP:8081), which is not routable "
            "from outside the cluster. Set e.g. localhost:8081 to send every "
            "per-node request to a port-forwarded client pod. "
            "Env default: FUSE_NODE_ADDR"
        ),
    ).strip()
    page = st.radio(
        "View",
        ["Cluster overview", "Per-node cache", "Warm targeting", "Replicas & heat"],
        label_visibility="collapsed",
    )
    refresh_label = st.select_slider(
        "Auto-refresh", options=list(REFRESH_CHOICES), value="Off"
    )
    refresh_secs = REFRESH_CHOICES[refresh_label]
    if st.button("Refresh now", width="stretch"):
        st.cache_data.clear()
        st.rerun()
    st.caption("Responses are cached 10-20s; 'Refresh now' clears the cache.")

fc = client_for(coordinator_url, api_key)


def node_addr(peer: Peer) -> str:
    """Address to talk to a client daemon on. The override exists because the
    registered address is a pod IP (see cmd/client/main.go: POD_IP:port)."""
    return node_override or peer.address

# --------------------------------------------------------------------------
# shared load: peers + cluster stats
# --------------------------------------------------------------------------

peers_ok, peers_data, peers_err = fetch_peers(coordinator_url, api_key)
peers: List[Peer] = list(peers_data or []) if peers_ok else []

if not peers_ok:
    st.error(f"Cannot reach coordinator at {coordinator_url or '(unset)'} — {peers_err}")
    st.caption(
        "Check the URL in the sidebar, or port-forward: "
        "`kubectl -n fuse-system port-forward svc/coordinator 8080:8080`"
    )

# --------------------------------------------------------------------------
# 1. Cluster overview
# --------------------------------------------------------------------------

if page == "Cluster overview":
    st.subheader("Cluster overview")

    stats_ok, stats, stats_err = fetch_cluster_stats(coordinator_url, api_key)
    stats = stats if (stats_ok and isinstance(stats, dict)) else {}
    if not stats_ok:
        st.warning(f"/api/stats unavailable — {stats_err}")

    total_capacity = sum(peer_capacity(p) for p in peers)
    total_used = sum(p.used_space for p in peers)

    # Aggregate hit rate across peers: sum tier hits / (hits + misses) from
    # each daemon's /metrics (fuse_cache_*_hits_total / *_misses_total).
    agg_hits = agg_misses = 0.0
    reachable = 0
    per_node_metrics: Dict[str, MetricSet] = {}
    # With an address override every peer resolves to the same daemon, so poll
    # only the first one instead of counting it N times.
    metric_peers = peers[:1] if node_override else peers
    for p in metric_peers:
        ok, text, _ = fetch_peer_metrics(coordinator_url, api_key, node_addr(p))
        if not ok or not isinstance(text, str):
            continue
        ms = MetricSet.from_text(text)
        per_node_metrics[p.id] = ms
        reachable += 1
        for tier in ("nvme", "peer", "cloud"):
            agg_hits += ms.get(f"fuse_cache_{tier}_hits_total")
            agg_misses += ms.get(f"fuse_cache_{tier}_misses_total")
    hit_rate = (agg_hits / (agg_hits + agg_misses) * 100.0) if (agg_hits + agg_misses) else None

    stale = [p for p in peers if (heartbeat_age_seconds(p) or 0) > 60]

    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Active peers", as_int(stats.get("active_peers", len(peers))))
    k2.metric("NVMe capacity", human_bytes(total_capacity))
    k3.metric("NVMe used", human_bytes(total_used),
              delta=f"{(total_used / total_capacity * 100):.0f}% of capacity" if total_capacity else None,
              delta_color="off")
    k4.metric("Aggregate hit rate", f"{hit_rate:.1f}%" if hit_rate is not None else "n/a")
    st.caption(
        f"Hit rate aggregated over {reachable}/{len(metric_peers)} reachable daemons "
        "(fuse_cache_{nvme,peer,cloud}_{hits,misses}_total). "
        f"Coordinator reports {as_int(stats.get('file_count'))} tracked files."
    )

    if stale:
        st.warning(
            "Stale heartbeat (>60s): " + ", ".join(f"`{p.id}`" for p in stale)
        )

    if peers:
        rows = [
            (
                p.id,
                float(p.used_space),
                float(peer_capacity(p)),
                f"{human_bytes(p.used_space)} / {human_bytes(peer_capacity(p))}",
            )
            for p in sorted(peers, key=lambda x: x.id)
        ]
        st.markdown(
            card(
                "NVMe used per node",
                "Fill is used bytes against the node's reported capacity "
                "(available_space + used_space). Amber ≥75%, red ≥90%.",
                hbar_meter_chart(rows),
            ),
            unsafe_allow_html=True,
        )

    st.markdown("**Peers**")
    if peers:
        table = []
        for p in sorted(peers, key=lambda x: x.id):
            age = heartbeat_age_seconds(p)
            cap = peer_capacity(p)
            table.append(
                {
                    "id": p.id,
                    "address": p.address,
                    "grpc": p.grpc_address,
                    "labels": p.label_str or "-",
                    "status": p.status,
                    "heartbeat age (s)": round(age, 1) if age is not None else None,
                    "stale": (age is not None and age > 60),
                    "used": human_bytes(p.used_space),
                    "capacity": human_bytes(cap),
                    "used %": round(p.used_space / cap * 100, 1) if cap else None,
                    "nvme path": p.nvme_path,
                    "net MB/s": round(p.network_speed_mbps, 1) or None,
                    "net ms": round(p.network_latency_ms, 2) or None,
                }
            )
        st.dataframe(pd.DataFrame(table), width="stretch", hide_index=True)
    elif peers_ok:
        st.info("Coordinator reachable, but no active peers are registered.")

# --------------------------------------------------------------------------
# 2. Per-node cache detail
# --------------------------------------------------------------------------

elif page == "Per-node cache":
    st.subheader("Per-node cache")

    if not peers:
        st.info("No peers to inspect.")
    else:
        ids = [p.id for p in sorted(peers, key=lambda x: x.id)]
        selected_id = st.selectbox("Node", ids)
        peer = next(p for p in peers if p.id == selected_id)
        addr = node_addr(peer)
        suffix = f" (registered `{peer.address}`)" if addr != peer.address else ""
        st.caption(f"`{addr}`{suffix} · labels: {peer.label_str or 'none'}")

        cs_ok, cache_stats, cs_err = fetch_peer_cache_stats(
            coordinator_url, api_key, addr
        )
        m_ok, metrics_text, m_err = fetch_peer_metrics(
            coordinator_url, api_key, addr
        )
        cache_stats = cache_stats if (cs_ok and isinstance(cache_stats, dict)) else {}
        ms = MetricSet.from_text(metrics_text) if (m_ok and isinstance(metrics_text, str)) else MetricSet([])

        if not cs_ok:
            st.warning(f"/api/cache/stats on {addr} — {cs_err}")
        if not m_ok:
            st.warning(f"/metrics on {addr} — {m_err}")

        used = as_int(cache_stats.get("nvme_used", ms.get("fuse_nvme_used_bytes")))
        capacity = as_int(cache_stats.get("nvme_capacity", ms.get("fuse_nvme_capacity_bytes")))

        hits = {t: ms.get(f"fuse_cache_{t}_hits_total") for t in ("nvme", "peer", "cloud")}
        misses = {t: ms.get(f"fuse_cache_{t}_misses_total") for t in ("nvme", "peer", "cloud")}
        total_hits = sum(hits.values())
        total_reads = total_hits + sum(misses.values())

        c1, c2, c3, c4 = st.columns(4)
        c1.metric("NVMe used", human_bytes(used))
        c2.metric("NVMe capacity", human_bytes(capacity))
        c3.metric(
            "Local hit rate",
            f"{total_hits / total_reads * 100:.1f}%" if total_reads else "n/a",
        )
        c4.metric("Evictions", compact(ms.get("fuse_cache_evictions_total")))

        pct = (used / capacity) if capacity else 0.0
        st.progress(min(1.0, max(0.0, pct)), text=f"NVMe {pct * 100:.1f}% full")

        left, right = st.columns(2)
        with left:
            st.markdown(
                card(
                    "Reads served by tier",
                    "Share of cache hits per tier on this node.",
                    stacked_share_bar(
                        [
                            ("NVMe", hits["nvme"], S1),
                            ("Peer", hits["peer"], S2),
                            ("Cloud", hits["cloud"], S3),
                        ]
                    ),
                ),
                unsafe_allow_html=True,
            )
        with right:
            tier_rows = [
                {
                    "tier": t.upper(),
                    "hits": int(hits[t]),
                    "misses": int(misses[t]),
                    "hit rate %": round(hits[t] / (hits[t] + misses[t]) * 100, 1)
                    if (hits[t] + misses[t])
                    else None,
                    "read MB/s": round(ms.get(f"fuse_cache_{t}_read_mbps"), 1),
                    "bytes read": human_bytes(ms.get(f"fuse_cache_{t}_read_bytes_total")),
                }
                for t in ("nvme", "peer", "cloud")
            ]
            st.markdown("**Tier detail** (table view of the chart)")
            st.dataframe(pd.DataFrame(tier_rows), width="stretch", hide_index=True)

        st.markdown("**Herd control & warm counters**")
        herd_rows = [
            {"metric": label, "name": name, "value": ms.get(name)}
            for name, label in HERD_METRICS
            if ms.has(name)
        ]
        if herd_rows:
            st.dataframe(pd.DataFrame(herd_rows), width="stretch", hide_index=True)
        else:
            st.info(
                "No fuse_peer_*/fuse_fetch_lease_*/fuse_replica_reconcile_* metrics "
                "in /metrics — the daemon's cache manager is not a DefaultCacheManager, "
                "or the endpoint is unreachable."
            )

        pair_lat = ms.by_label("fuse_peer_pair_latency_ms", "peer")
        if pair_lat:
            pair_succ = ms.by_label("fuse_peer_pair_success_ratio", "peer")
            pair_n = ms.by_label("fuse_peer_pair_samples_total", "peer")
            st.markdown(f"**Observed latency from `{peer.id}` to each peer**")
            st.dataframe(
                pd.DataFrame(
                    [
                        {
                            "peer": pid,
                            "latency ms": round(v, 2),
                            "success ratio": round(pair_succ.get(pid, 0.0), 4),
                            "samples": int(pair_n.get(pid, 0)),
                        }
                        for pid, v in sorted(pair_lat.items(), key=lambda kv: kv[1])
                    ]
                ),
                width="stretch",
                hide_index=True,
            )

        with st.expander("Other counters from /api/cache/stats"):
            # Some CacheMetrics keys are exposed by /api/cache/stats but NOT by
            # /metrics (eviction_skipped_unpersisted, watermark_*).
            if cache_stats:
                st.dataframe(
                    pd.DataFrame(
                        sorted(
                            ({"key": k, "value": v} for k, v in cache_stats.items()),
                            key=lambda r: r["key"],
                        )
                    ),
                    width="stretch",
                    hide_index=True,
                )
            else:
                st.caption("Unavailable.")

        with st.expander("Raw /metrics"):
            st.code(metrics_text if (m_ok and metrics_text) else (m_err or "unavailable"))

# --------------------------------------------------------------------------
# 3. Warm targeting
# --------------------------------------------------------------------------

elif page == "Warm targeting":
    st.subheader("Warm targeting")
    st.caption(
        "Coordinator POST /api/warm fans out to selected peers, each of which "
        "runs POST /api/cache/warm asynchronously."
    )

    label_keys = sorted({k for p in peers for k in p.labels})
    label_pairs = sorted({f"{k}={v}" for p in peers for k, v in p.labels.items()})

    fan_col, single_col = st.tabs(["Cluster fan-out", "Single node"])

    with fan_col:
        c1, c2 = st.columns([2, 1])
        with c1:
            prefix = st.text_input("Prefix", value="/", key="fan_prefix")
        with c2:
            mode = st.selectbox("Mode", ["full", "metadata"], key="fan_mode")
        c3, c4 = st.columns(2)
        with c3:
            source = st.selectbox(
                "Source",
                ["peer-first", "cloud-first", "cloud-only"],
                key="fan_source",
                help="Tier order for fetches (cache.warmupPlan).",
            )
        with c4:
            bandwidth = st.selectbox(
                "Bandwidth",
                ["background", "max"],
                key="fan_bw",
                help="background = 2 files x 4 chunk fetches; max = 4 x 16.",
            )

        st.markdown("**Selector** — nodes ∩ labels, then percentage of the remainder")
        s1, s2 = st.columns(2)
        with s1:
            sel_nodes = st.multiselect(
                "Node IDs", sorted(p.id for p in peers), key="fan_nodes"
            )
        with s2:
            picked_labels = st.multiselect(
                "Labels (key=value)", label_pairs, key="fan_labels",
                help="Built from labels actually registered on peers.",
            )
        extra_labels = st.text_input(
            "Extra labels", value="", key="fan_extra",
            placeholder="pool=gpu,zone=a",
            help="Comma-separated key=value; merged with the picks above.",
        )
        percentage = st.slider("Percentage", 0, 100, 100, key="fan_pct",
                               help="0 or 100 = every matching peer.")

        labels: Dict[str, str] = {}
        bad_labels: List[str] = []
        for token in list(picked_labels) + [
            t for t in extra_labels.split(",") if t.strip()
        ]:
            token = token.strip()
            if "=" not in token:
                bad_labels.append(token)
                continue
            k, v = token.split("=", 1)
            if not k.strip():
                bad_labels.append(token)
                continue
            labels[k.strip()] = v.strip()
        if bad_labels:
            st.warning("Ignoring malformed label(s): " + ", ".join(f"`{b}`" for b in bad_labels))

        # The prefix is the coordinator's rotation key, so the dry run only
        # matches the real fan-out when it is passed through.
        targets = select_warm_targets(
            peers, sel_nodes, labels, percentage, rotation_key=prefix.strip()
        )

        st.markdown("**Dry run** — peers this selector resolves to right now")
        if not peers:
            st.info("No peers known; the coordinator would reject with 'no active peers match the selector'.")
        elif targets:
            st.success(f"{len(targets)} of {len([p for p in peers if p.status == 'active'])} active peers selected")
            st.dataframe(
                pd.DataFrame(
                    [
                        {
                            "id": t.id,
                            "address": t.address,
                            "labels": t.label_str or "-",
                            "used %": round(t.used_space / peer_capacity(t) * 100, 1)
                            if peer_capacity(t)
                            else None,
                            "free": human_bytes(t.available_space),
                        }
                        for t in targets
                    ]
                ),
                width="stretch",
                hide_index=True,
            )
        else:
            st.error("Selector matches no active peers — the coordinator would return 400.")

        st.caption(
            "Selection mirrors CoordinatorService.SelectWarmTargets: active and "
            "recently-heartbeating only, ID filter, all labels must match, "
            "ID-sorted, then ceil(n x pct/100) (min 1) starting at a "
            "prefix-derived offset — so different prefixes rotate across the "
            "cluster instead of always hitting the lowest IDs."
        )

        fire = st.button(
            "Fire warm fan-out",
            type="primary",
            disabled=not targets or not prefix.strip(),
        )
        if fire:
            with st.spinner(f"POST {coordinator_url}/api/warm"):
                res = fc.warm_cluster(
                    prefix=prefix.strip(),
                    mode=mode,
                    source=source,
                    bandwidth=bandwidth,
                    nodes=sel_nodes,
                    labels=labels,
                    percentage=percentage,
                )
            if not res.ok:
                st.error(f"Warm fan-out failed — {res.error}")
            else:
                body = res.data or {}
                accepted = body.get("accepted") or []
                failed = body.get("failed") or {}
                m1, m2, m3 = st.columns(3)
                m1.metric("Selected", as_int(body.get("selected")))
                m2.metric("Accepted", len(accepted))
                m3.metric("Failed", len(failed))
                if accepted:
                    st.success("Accepted: " + ", ".join(f"`{a}`" for a in accepted))
                jobs = body.get("jobs") or {}
                if jobs:
                    st.dataframe(
                        pd.DataFrame(
                            [{"peer": k, "job": v} for k, v in sorted(jobs.items())]
                        ),
                        width="stretch",
                        hide_index=True,
                    )
                    st.caption(
                        "Poll a job on the single-node panel, or directly: "
                        "GET http://<peer>/api/cache/warm/<job>."
                    )
                if failed:
                    st.error("Failed peers")
                    st.dataframe(
                        pd.DataFrame(
                            [{"peer": k, "error": v} for k, v in sorted(failed.items())]
                        ),
                        width="stretch",
                        hide_index=True,
                    )
                st.caption(
                    "Peer-side warms run async — poll the job IDs above, or "
                    "watch fuse_cache_* and fuse_nvme_used_bytes on the "
                    "Per-node cache page."
                )

    with single_col:
        if not peers:
            st.info("No peers to target.")
        else:
            node_id = st.selectbox(
                "Node", sorted(p.id for p in peers), key="one_node"
            )
            node = next(p for p in peers if p.id == node_id)
            o1, o2 = st.columns([2, 1])
            with o1:
                one_prefix = st.text_input("Prefix", value="/", key="one_prefix")
            with o2:
                one_mode = st.selectbox("Mode", ["full", "metadata"], key="one_mode")
            o3, o4, o5 = st.columns(3)
            with o3:
                one_source = st.selectbox(
                    "Source", ["peer-first", "cloud-first", "cloud-only"], key="one_source"
                )
            with o4:
                one_bw = st.selectbox("Bandwidth", ["background", "max"], key="one_bw")
            with o5:
                one_async = st.toggle(
                    "Async", value=True, key="one_async",
                    help="Async returns 202 immediately; sync waits and returns WarmupResult.",
                )
            one_timeout = st.number_input(
                "Timeout (s)", min_value=0, max_value=3600, value=0, step=30,
                key="one_timeout", help="0 uses the daemon default of 600s.",
            )

            if st.button(
                f"Warm {node_id}", type="primary", disabled=not one_prefix.strip()
            ):
                with st.spinner(f"POST http://{node_addr(node)}/api/cache/warm"):
                    res = fc.warm_node(
                        node_addr(node),
                        prefix=one_prefix.strip(),
                        mode=one_mode,
                        source=one_source,
                        bandwidth=one_bw,
                        async_=bool(one_async),
                        timeout_seconds=int(one_timeout),
                    )
                if not res.ok:
                    st.error(f"Warm failed — {res.error}")
                else:
                    body = res.data or {}
                    if body.get("started"):
                        st.success(
                            f"Accepted (202): warming `{body.get('prefix')}` "
                            f"mode={body.get('mode')} source={body.get('source') or 'peer-first'} "
                            f"bandwidth={body.get('bandwidth') or 'background'}"
                        )
                        if body.get("job_id"):
                            st.session_state["warm_job_id"] = body["job_id"]
                            st.caption(
                                f"Job `{body['job_id']}` — poll it below."
                            )
                    else:
                        render_warm_counters(body)
                    st.json(body)

            st.divider()
            st.markdown("**Async warm jobs on this node**")
            st.caption(
                "GET /api/cache/warm on the daemon. Progress updates once per "
                "file completed, so a single huge file moves only at the end."
            )
            if st.button("Refresh jobs", key="warm_jobs_refresh"):
                pass  # Streamlit reruns the script; the fetch below is live.
            jobs_res = fc.warm_jobs(node_addr(node))
            if not jobs_res.ok:
                st.error(f"Could not list warm jobs — {jobs_res.error}")
            else:
                jobs = (jobs_res.data or {}).get("jobs") or []
                if not jobs:
                    st.info("No warm jobs on this node yet.")
                else:
                    st.dataframe(
                        pd.DataFrame([warm_job_row(j) for j in jobs]),
                        width="stretch",
                        hide_index=True,
                    )
                    ids = [j.get("id", "") for j in jobs]
                    default = st.session_state.get("warm_job_id")
                    idx = ids.index(default) if default in ids else 0
                    chosen = st.selectbox(
                        "Job detail", ids, index=idx, key="warm_job_pick"
                    )
                    job = next((j for j in jobs if j.get("id") == chosen), None)
                    if job:
                        prog = job.get("progress") or {}
                        files = as_int(prog.get("files"))
                        done = as_int(prog.get("done"))
                        if files:
                            st.progress(min(done / files, 1.0), text=f"{done}/{files} files")
                        # A warm of one huge file sits at 0/1 files for its
                        # whole duration; the chunk bar is the only thing that
                        # moves, so show it whenever chunks are being fetched.
                        chunks = as_int(prog.get("chunks"))
                        if chunks:
                            chunks_done = as_int(prog.get("chunks_done"))
                            st.progress(
                                min(chunks_done / chunks, 1.0),
                                text=f"{chunks_done}/{chunks} chunks",
                            )
                        render_warm_counters(prog)
                        if job.get("error"):
                            st.error(job["error"])
                        st.json(job)

# --------------------------------------------------------------------------
# 4. Replicas & heat
# --------------------------------------------------------------------------

elif page == "Replicas & heat":
    st.subheader("Replicas & heat")
    st.caption(
        "Replica counts come from the coordinator's file-location metadata "
        "(GET /api/worldview?prefix=, which returns every location per file). "
        "GET /api/files/locations returns only ONE location per path, so it "
        "cannot be used to count replicas."
    )

    r1, r2 = st.columns([2, 1])
    with r1:
        prefix = st.text_input("Path prefix", value="/", key="rep_prefix")
    with r2:
        target = st.number_input(
            "Replica target", min_value=1, max_value=32, value=3,
            help="Match -replica-reconcile-target (0 -> MinPeerReplicas default 3).",
        )
    hide_chunks = st.checkbox(
        "Hide `_chunk_N` entries", value=True,
        help="Chunk objects are published individually; the parent file is the unit warmup uses.",
    )

    wv_ok, world, wv_err = fetch_worldview(coordinator_url, api_key, prefix.strip())
    if not wv_ok:
        st.error(f"/api/worldview unavailable — {wv_err}")
        st.caption(
            "Fallback: query individual paths below via GET /api/files/location?path=."
        )
        world = None

    files: List[Dict[str, Any]] = []
    if isinstance(world, dict):
        files = [f for f in (world.get("files") or []) if isinstance(f, dict)]
    chunk_re = re.compile(r"_chunk_\d+$")
    if hide_chunks:
        files = [f for f in files if not chunk_re.search(str(f.get("file_path", "")))]

    if world is not None and not files:
        st.info(f"No files tracked under `{prefix}`.")

    if files:
        rows = []
        for f in files:
            path = str(f.get("file_path", ""))
            locs = [l for l in (f.get("locations") or []) if isinstance(l, dict)]
            tier_replicas = f.get("tier_replicas") or {}
            nvme_holders = sorted(
                {str(l.get("peer_id", "")) for l in locs
                 if str(l.get("storage_tier", "")).lower() == "nvme"}
            )
            last = max(
                (parse_go_time(str(l.get("last_accessed", ""))) for l in locs),
                default=None,
                key=lambda d: d or datetime.min.replace(tzinfo=timezone.utc),
            )
            age_min = (
                (datetime.now(timezone.utc) - last).total_seconds() / 60.0
                if last
                else None
            )
            replicas = as_int(f.get("replica_count"))
            nvme_count = as_int(tier_replicas.get("nvme"))
            rows.append(
                {
                    "path": path,
                    "size": human_bytes(f.get("file_size")),
                    "size_bytes": as_int(f.get("file_size")),
                    "replicas": replicas,
                    "nvme replicas": nvme_count,
                    "under target": nvme_count < int(target),
                    "hot (<10m)": (age_min is not None and age_min < 10),
                    "last access (min)": round(age_min, 1) if age_min is not None else None,
                    "chunked": bool(f.get("is_chunked")),
                    "holders": ", ".join(nvme_holders) or "-",
                    "tiers": ", ".join(f"{k}:{v}" for k, v in sorted(tier_replicas.items())),
                }
            )

        under = [r for r in rows if r["under target"]]
        hot_under = [
            r
            for r in under
            if r["hot (<10m)"] and 0 < r["size_bytes"] <= 64 * 1024 * 1024
        ]

        k1, k2, k3, k4 = st.columns(4)
        k1.metric("Files", len(rows))
        k2.metric("Under target", len(under))
        k3.metric("Hot & under-replicated", len(hot_under))
        k4.metric(
            "Median NVMe replicas",
            int(pd.Series([r["nvme replicas"] for r in rows]).median()),
        )
        st.caption(
            "'Hot & under-replicated' applies the reconciler's own filter: "
            "accessed within 10 min and ≤64 MiB (reconcileHotWindow / "
            "reconcileMaxObjectBytes)."
        )

        # Distribution of NVMe replica counts, 0..max. Bins below the target
        # are the story -> status critical; at/above -> series slot 1.
        counts = [r["nvme replicas"] for r in rows]
        top = max(max(counts, default=0), int(target))
        bins = [(str(i), float(sum(1 for c in counts if c == i))) for i in range(0, top + 1)]
        colors = [CRITICAL if i < int(target) else S1 for i in range(0, top + 1)]
        st.markdown(
            card(
                "NVMe replica-count distribution",
                f"Files per replica count under `{prefix}`. Target = {int(target)}.",
                legend([(f"Below target (<{int(target)})", CRITICAL),
                        (f"At or above target", S1)])
                + column_histogram(bins, colors),
            ),
            unsafe_allow_html=True,
        )

        if hot_under:
            st.warning(
                f"{len(hot_under)} hot file(s) below the replica target — the "
                "reconciler should top these up if it is enabled "
                "(-replica-reconcile-interval-sec > 0)."
            )
            st.dataframe(
                pd.DataFrame(hot_under).drop(columns=["size_bytes"]),
                width="stretch",
                hide_index=True,
            )

        st.markdown("**All files** (table view of the chart)")
        st.dataframe(
            pd.DataFrame(sorted(rows, key=lambda r: (r["nvme replicas"], r["path"]))).drop(
                columns=["size_bytes"]
            ),
            width="stretch",
            hide_index=True,
        )

    st.divider()
    st.markdown("**Look up specific paths**")
    st.caption("One path per line -> GET /api/files/location?path= for each.")
    raw_paths = st.text_area("Paths", value="", height=100, key="rep_paths",
                             placeholder="/models/llama/weights.bin")
    if st.button("Look up", disabled=not raw_paths.strip()):
        lookup_rows = []
        for line in raw_paths.splitlines():
            path = line.strip()
            if not path:
                continue
            ok, locs, err = fetch_file_location(coordinator_url, api_key, path)
            if not ok:
                lookup_rows.append({"path": path, "replicas": None, "error": err})
                continue
            locs = locs or []
            lookup_rows.append(
                {
                    "path": path,
                    "replicas": len(locs),
                    "under target": len(locs) < int(target),
                    "holders": ", ".join(
                        sorted({str(l.get("peer_id", "")) for l in locs})
                    )
                    or "-",
                    "tiers": ", ".join(
                        sorted({str(l.get("storage_tier", "")) for l in locs})
                    )
                    or "-",
                    "size": human_bytes(max((as_int(l.get("file_size")) for l in locs), default=0)),
                    "error": "",
                }
            )
        if lookup_rows:
            st.dataframe(pd.DataFrame(lookup_rows), width="stretch", hide_index=True)

# --------------------------------------------------------------------------
# auto-refresh (never on the warm page — a rerun mid-form loses the selector)
# --------------------------------------------------------------------------

if refresh_secs and page != "Warm targeting":
    st.caption(f"Auto-refreshing every {refresh_secs}s.")
    time.sleep(refresh_secs)
    st.rerun()
elif refresh_secs:
    st.caption("Auto-refresh is paused on this page so form state is not lost.")

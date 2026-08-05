"""Thin HTTP client over the fuse-client coordinator + client APIs.

Every call returns an `ApiResult` instead of raising, so one dead peer degrades
a single card rather than blanking the page.

Endpoints used (all verified against the Go source):

Coordinator (cmd/coordinator/main.go setupRoutes, default :8080)
  GET  /api/peers[?requester_id=]  -> []coordinator.PeerInfo
  GET  /api/stats                  -> CoordinatorService.GetPeerStats()
  GET  /api/health
  GET  /api/files/location?path=   -> []coordinator.FileLocation  (one path)
  GET  /api/files/locations?prefix -> []FileLocation, ONE per path (store.ListFileLocations)
  GET  /api/worldview?prefix=      -> coordinator.WorldView (all replicas per file)
  POST /api/warm                   -> coordinator.WarmFanoutResult
  POST /api/cache/seed             -> coordinator.SeedCacheResult

Client (internal/api/handler.go SetupRoutes, default :8081)
  GET  /api/health                 (no auth)
  GET  /metrics                    (no auth) Prometheus text
  GET  /api/cache/stats            -> flat map: coordinator peer stats + nvme_used/
                                      nvme_capacity + CacheMetrics.Snapshot()
  GET  /api/cache                  -> []cache.CacheEntry (Go field names, no json tags)
  GET  /api/peers/latency          -> {peer_id, pairs: []PeerPairLatency}
  POST /api/cache/warm             -> cache.WarmupResult | 202 {started,job_id,status_url}
  GET  /api/cache/warm             -> {peer_id, jobs: []warmJob} (newest first)
  GET  /api/cache/warm/{id}        -> warmJob (status + live progress counters)

The coordinator has NO auth middleware; the client's authMiddleware exempts
/api/health, /metrics and /api/netprobe and requires X-API-Key elsewhere when
the daemon was started with -api-key.
"""

from __future__ import annotations

import json
import math
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

# Mirrors warmPeerStaleAfter in internal/coordinator/warm.go: peers past this
# many seconds without a heartbeat are dropped from warm selection, well
# before the 90s etcd lease flips their status to inactive.
WARM_PEER_STALE_AFTER_SECONDS = 75.0

try:
    import requests
except ImportError:  # pragma: no cover
    # Peer parsing and select_warm_targets are pure Python; keep them usable
    # (and unit-testable) in an interpreter without requests installed.
    requests = None  # type: ignore[assignment]

DEFAULT_TIMEOUT = (3.0, 10.0)  # (connect, read)


@dataclass
class ApiResult:
    """Outcome of one HTTP call. `ok` gates `data`; `error` is display-ready."""

    ok: bool
    data: Any = None
    error: Optional[str] = None
    status: Optional[int] = None
    url: str = ""

    def value(self, default: Any = None) -> Any:
        return self.data if self.ok else default


@dataclass
class Peer:
    """coordinator.PeerInfo as returned by GET /api/peers."""

    id: str
    address: str = ""
    grpc_address: str = ""
    nvme_path: str = ""
    available_space: int = 0
    used_space: int = 0
    status: str = ""
    last_heartbeat: str = ""
    network_speed_mbps: float = 0.0
    network_latency_ms: float = 0.0
    labels: Dict[str, str] = field(default_factory=dict)
    raw: Dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_json(cls, obj: Dict[str, Any]) -> "Peer":
        labels = obj.get("labels") or {}
        if not isinstance(labels, dict):
            labels = {}
        return cls(
            id=str(obj.get("id", "")),
            address=str(obj.get("address", "") or ""),
            grpc_address=str(obj.get("grpc_address", "") or ""),
            nvme_path=str(obj.get("nvme_path", "") or ""),
            available_space=int(obj.get("available_space") or 0),
            used_space=int(obj.get("used_space") or 0),
            status=str(obj.get("status", "") or ""),
            last_heartbeat=str(obj.get("last_heartbeat", "") or ""),
            network_speed_mbps=float(obj.get("network_speed_mbps") or 0.0),
            network_latency_ms=float(obj.get("network_latency_ms") or 0.0),
            labels={str(k): str(v) for k, v in labels.items()},
            raw=obj,
        )

    @property
    def label_str(self) -> str:
        return ", ".join(f"{k}={v}" for k, v in sorted(self.labels.items()))

    def heartbeat_age_seconds(self) -> float:
        """Seconds since last_heartbeat; 0.0 when it is missing or unparseable.

        0.0 rather than infinity on purpose: an unreadable timestamp must not
        make a live peer look dead in the dry run.
        """
        raw = (self.last_heartbeat or "").strip()
        if not raw:
            return 0.0
        # Go emits RFC3339 with a "Z"; fromisoformat wants an explicit offset,
        # and only handles up to 6 fractional digits.
        text = raw[:-1] + "+00:00" if raw.endswith("Z") else raw
        if "." in text:
            head, _, tail = text.partition(".")
            digits = "".join(c for c in tail if c.isdigit())[:6]
            rest = tail[len(digits):].lstrip("0123456789")
            text = f"{head}.{digits}{rest}" if digits else head + rest
        try:
            stamp = datetime.fromisoformat(text)
        except ValueError:
            return 0.0
        if stamp.tzinfo is None:
            stamp = stamp.replace(tzinfo=timezone.utc)
        return max(0.0, (datetime.now(timezone.utc) - stamp).total_seconds())


def _err_text(resp: Any) -> str:
    body = (resp.text or "").strip()
    if len(body) > 300:
        body = body[:300] + "…"
    return f"HTTP {resp.status_code}{': ' + body if body else ''}"


class FuseClient:
    """Requests wrapper. Never raises for network/HTTP problems."""

    def __init__(
        self,
        coordinator_url: str,
        api_key: str = "",
        timeout: Any = DEFAULT_TIMEOUT,
    ):
        if requests is None:  # pragma: no cover
            raise RuntimeError(
                "the `requests` package is required for HTTP calls; "
                "pip install -r tools/dashboard/requirements.txt"
            )
        self.coordinator_url = (coordinator_url or "").rstrip("/")
        self.api_key = api_key or ""
        self.timeout = timeout
        self._session = requests.Session()

    # ---- plumbing -------------------------------------------------------

    def _headers(self, json_body: bool = False) -> Dict[str, str]:
        h = {"Accept": "application/json"}
        if self.api_key:
            h["X-API-Key"] = self.api_key
        if json_body:
            h["Content-Type"] = "application/json"
        return h

    def _request(
        self,
        method: str,
        url: str,
        *,
        params: Optional[Dict[str, Any]] = None,
        body: Optional[Dict[str, Any]] = None,
        timeout: Any = None,
        parse_json: bool = True,
    ) -> ApiResult:
        if not url.startswith("http"):
            return ApiResult(False, error=f"invalid URL: {url!r}", url=url)
        try:
            resp = self._session.request(
                method,
                url,
                params=params,
                data=json.dumps(body) if body is not None else None,
                headers=self._headers(json_body=body is not None),
                timeout=timeout or self.timeout,
            )
        except requests.exceptions.ConnectTimeout:
            return ApiResult(False, error=f"connect timeout to {url}", url=url)
        except requests.exceptions.ReadTimeout:
            return ApiResult(False, error=f"read timeout from {url}", url=url)
        except requests.exceptions.ConnectionError as exc:
            return ApiResult(False, error=f"cannot reach {url} ({_short(exc)})", url=url)
        except requests.exceptions.RequestException as exc:
            return ApiResult(False, error=f"request failed: {_short(exc)}", url=url)

        if resp.status_code >= 400:
            return ApiResult(False, error=_err_text(resp), status=resp.status_code, url=url)
        if not parse_json:
            return ApiResult(True, data=resp.text, status=resp.status_code, url=url)
        if not (resp.text or "").strip():
            return ApiResult(True, data=None, status=resp.status_code, url=url)
        try:
            return ApiResult(True, data=resp.json(), status=resp.status_code, url=url)
        except ValueError:
            return ApiResult(
                False,
                error=f"non-JSON response from {url}",
                status=resp.status_code,
                url=url,
            )

    @staticmethod
    def peer_base_url(address: str) -> str:
        """PeerInfo.address is `host:port` for the client HTTP API."""
        address = (address or "").strip()
        if not address:
            return ""
        if address.startswith(("http://", "https://")):
            return address.rstrip("/")
        return f"http://{address}"

    # ---- coordinator ----------------------------------------------------

    def coordinator_health(self) -> ApiResult:
        return self._request("GET", f"{self.coordinator_url}/api/health")

    def list_peers(self) -> ApiResult:
        """GET /api/peers. No requester_id -> every ACTIVE peer (GetPeers
        filters on Status == "active")."""
        res = self._request("GET", f"{self.coordinator_url}/api/peers")
        if not res.ok:
            return res
        raw = res.data or []
        if not isinstance(raw, list):
            return ApiResult(False, error="unexpected /api/peers payload", url=res.url)
        return ApiResult(True, data=[Peer.from_json(p) for p in raw if isinstance(p, dict)], url=res.url)

    def cluster_stats(self) -> ApiResult:
        return self._request("GET", f"{self.coordinator_url}/api/stats")

    def file_location(self, path: str) -> ApiResult:
        """GET /api/files/location?path= — ALL locations (replicas) of one path."""
        return self._request(
            "GET", f"{self.coordinator_url}/api/files/location", params={"path": path}
        )

    def list_file_locations(self, prefix: str = "") -> ApiResult:
        """GET /api/files/locations?prefix= — ONE location per path (the store
        returns locs[0]); use it to enumerate paths, not to count replicas."""
        return self._request(
            "GET",
            f"{self.coordinator_url}/api/files/locations",
            params={"prefix": prefix} if prefix else None,
        )

    def worldview(self, prefix: str = "", timeout: Any = None) -> ApiResult:
        """GET /api/worldview?prefix= — per-file replica counts + every location."""
        return self._request(
            "GET",
            f"{self.coordinator_url}/api/worldview",
            params={"prefix": prefix} if prefix else None,
            timeout=timeout or (3.0, 30.0),
        )

    def warm_cluster(
        self,
        prefix: str,
        mode: str = "full",
        source: str = "",
        bandwidth: str = "",
        nodes: Optional[List[str]] = None,
        labels: Optional[Dict[str, str]] = None,
        percentage: int = 0,
    ) -> ApiResult:
        """POST /api/warm — coordinator.WarmRequest fan-out."""
        body: Dict[str, Any] = {"prefix": prefix, "mode": mode}
        if source:
            body["source"] = source
        if bandwidth:
            body["bandwidth"] = bandwidth
        if nodes:
            body["nodes"] = list(nodes)
        if labels:
            body["labels"] = dict(labels)
        if percentage:
            body["percentage"] = int(percentage)
        if self.api_key:
            # Forwarded by the coordinator as X-API-Key to each peer.
            body["api_key"] = self.api_key
        return self._request(
            "POST", f"{self.coordinator_url}/api/warm", body=body, timeout=(3.0, 60.0)
        )

    # ---- individual client daemon --------------------------------------

    def peer_health(self, address: str) -> ApiResult:
        return self._request("GET", f"{self.peer_base_url(address)}/api/health", timeout=(2.0, 5.0))

    def peer_cache_stats(self, address: str) -> ApiResult:
        return self._request("GET", f"{self.peer_base_url(address)}/api/cache/stats")

    def peer_metrics_text(self, address: str) -> ApiResult:
        return self._request(
            "GET", f"{self.peer_base_url(address)}/metrics", parse_json=False
        )

    def peer_cache_entries(self, address: str) -> ApiResult:
        """GET /api/cache -> []cache.CacheEntry. NOTE: CacheEntry has no json
        tags, so keys are Go field names (FilePath, Size, LastAccessed, Tier,
        IsChunked, NumChunks, ...) and Tier is the int enum
        (0=nvme, 1=peer, 2=cloud, -1=unknown)."""
        return self._request(
            "GET", f"{self.peer_base_url(address)}/api/cache", timeout=(3.0, 30.0)
        )

    def peer_latency(self, address: str) -> ApiResult:
        return self._request("GET", f"{self.peer_base_url(address)}/api/peers/latency")

    def warm_node(
        self,
        address: str,
        prefix: str,
        mode: str = "full",
        source: str = "",
        bandwidth: str = "",
        async_: bool = True,
        timeout_seconds: int = 0,
    ) -> ApiResult:
        """POST /api/cache/warm on one client daemon.

        async=True -> 202 {"started":true,"job_id":...,"status_url":...};
        poll it with warm_job(). sync -> cache.WarmupResult, snake_case:
        {"mode","files","warmed","already_local","skipped","failed","bytes"}.
        """
        body: Dict[str, Any] = {"prefix": prefix, "mode": mode, "async": bool(async_)}
        if source:
            body["source"] = source
        if bandwidth:
            body["bandwidth"] = bandwidth
        if timeout_seconds:
            body["timeout_seconds"] = int(timeout_seconds)
        read_timeout = 15.0 if async_ else max(30, (timeout_seconds or 600)) + 5
        return self._request(
            "POST",
            f"{self.peer_base_url(address)}/api/cache/warm",
            body=body,
            timeout=(3.0, read_timeout),
        )

    def warm_job(self, address: str, job_id: str) -> ApiResult:
        """GET /api/cache/warm/{id} — one async warm's live progress.

        -> {"id","prefix","mode","status":running|done|failed,"started_at",
            "updated_at","ended_at","progress":{files,done,warmed,
            already_local,skipped,failed,bytes},"result","error"}.
        """
        return self._request(
            "GET", f"{self.peer_base_url(address)}/api/cache/warm/{job_id}"
        )

    def warm_jobs(self, address: str) -> ApiResult:
        """GET /api/cache/warm -> {"peer_id","jobs":[...]}, newest first."""
        return self._request("GET", f"{self.peer_base_url(address)}/api/cache/warm")


def _short(exc: Exception) -> str:
    text = str(exc)
    return text if len(text) <= 160 else text[:160] + "…"


# ---- selector preview (mirrors CoordinatorService.SelectWarmTargets) ------


def warm_rotation_offset(key: str, n: int) -> int:
    """Mirror of warmRotationOffset (FNV-1a 32-bit mod n)."""
    if n <= 0 or not key:
        return 0
    h = 2166136261
    for byte in key.encode("utf-8"):
        h ^= byte
        h = (h * 16777619) & 0xFFFFFFFF
    return h % n


def select_warm_targets(
    peers: List[Peer],
    nodes: Optional[List[str]] = None,
    labels: Optional[Dict[str, str]] = None,
    percentage: int = 0,
    rotation_key: str = "",
) -> List[Peer]:
    """Client-side dry run of internal/coordinator/warm.go SelectWarmTargets.

    Same semantics, in order:
      1. active peers only (Status == "active")
      2. drop peers whose last heartbeat is older than WARM_PEER_STALE_AFTER
      3. ID filter, if any node IDs were given
      4. every requested label must match exactly (labelsMatch)
      5. sort by ID
      6. if 0 < pct < 100: take ceil(n*pct/100), min 1, starting at the
         rotation_key-derived offset in the sorted ring (empty key = head)

    rotation_key is the warm prefix on the Go side, so pass the prefix to
    preview the nodes an actual fan-out would pick.
    """
    want_ids = set(nodes or [])
    want_labels = {k: v for k, v in (labels or {}).items()}

    out: List[Peer] = []
    for p in peers:
        if p is None or p.status != "active":
            continue
        if p.heartbeat_age_seconds() > WARM_PEER_STALE_AFTER_SECONDS:
            continue
        if want_ids and p.id not in want_ids:
            continue
        if any(p.labels.get(k) != v for k, v in want_labels.items()):
            continue
        out.append(p)

    out.sort(key=lambda p: p.id)

    pct = int(percentage or 0)
    if 0 < pct < 100 and out:
        n = max(1, math.ceil(len(out) * pct / 100.0))
        start = warm_rotation_offset(rotation_key, len(out))
        picked = [out[(start + i) % len(out)] for i in range(n)]
        picked.sort(key=lambda p: p.id)
        out = picked
    return out

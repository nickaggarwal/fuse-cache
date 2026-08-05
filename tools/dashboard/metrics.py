"""Minimal Prometheus text-exposition parser.

The fuse-client `/metrics` endpoint (internal/api/handler.go handlePromMetrics)
emits a flat, un-HELP'd set of gauges/counters, some with a single `peer="..."`
label. That is a tiny subset of the exposition format, so a tiny parser is
enough — no prometheus_client dependency.
"""

from __future__ import annotations

from typing import Dict, Iterable, List, NamedTuple, Optional


class Sample(NamedTuple):
    name: str
    labels: Dict[str, str]
    value: float


def _parse_labels(raw: str) -> Dict[str, str]:
    """Parse the inside of `{...}`: `k="v",k2="v2"`.

    Handles backslash escapes for `\\`, `\"` and `\n` per the exposition spec.
    """
    labels: Dict[str, str] = {}
    i, n = 0, len(raw)
    while i < n:
        while i < n and raw[i] in ", \t":
            i += 1
        eq = raw.find("=", i)
        if eq == -1:
            break
        key = raw[i:eq].strip()
        j = raw.find('"', eq)
        if j == -1:
            break
        j += 1
        buf: List[str] = []
        while j < n and raw[j] != '"':
            if raw[j] == "\\" and j + 1 < n:
                nxt = raw[j + 1]
                buf.append({"n": "\n", '"': '"', "\\": "\\"}.get(nxt, nxt))
                j += 2
                continue
            buf.append(raw[j])
            j += 1
        labels[key] = "".join(buf)
        i = j + 1
    return labels


def parse(text: str) -> List[Sample]:
    """Parse exposition text into samples. Malformed lines are skipped."""
    out: List[Sample] = []
    for line in (text or "").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):  # comments incl. HELP/TYPE
            continue
        if line.startswith("{"):  # nameless sample — not emitted by us
            continue

        brace = line.find("{")
        if brace != -1:
            close = line.rfind("}")
            if close < brace:
                continue
            name = line[:brace].strip()
            labels = _parse_labels(line[brace + 1 : close])
            rest = line[close + 1 :].strip()
        else:
            parts = line.split(None, 1)
            if len(parts) != 2:
                continue
            name, rest = parts[0], parts[1]
            labels = {}

        # `value [timestamp]` — the timestamp is optional and ignored.
        fields = rest.split()
        if not fields:
            continue
        raw = fields[0]
        try:
            if raw in ("NaN", "nan"):
                value = float("nan")
            elif raw in ("+Inf", "Inf"):
                value = float("inf")
            elif raw == "-Inf":
                value = float("-inf")
            else:
                value = float(raw)
        except ValueError:
            continue
        out.append(Sample(name=name, labels=labels, value=value))
    return out


class MetricSet:
    """Query helper over a parsed sample list."""

    def __init__(self, samples: Iterable[Sample]):
        self.samples: List[Sample] = list(samples)
        self._unlabeled: Dict[str, float] = {}
        for s in self.samples:
            if not s.labels and s.name not in self._unlabeled:
                self._unlabeled[s.name] = s.value

    @classmethod
    def from_text(cls, text: str) -> "MetricSet":
        return cls(parse(text))

    def get(self, name: str, default: float = 0.0) -> float:
        """Value of an unlabeled metric, or `default` if absent."""
        return self._unlabeled.get(name, default)

    def has(self, name: str) -> bool:
        return name in self._unlabeled

    def series(self, name: str) -> List[Sample]:
        """All samples (labeled or not) carrying `name`."""
        return [s for s in self.samples if s.name == name]

    def by_label(self, name: str, label: str) -> Dict[str, float]:
        """Map label-value -> metric value for a labeled metric family."""
        out: Dict[str, float] = {}
        for s in self.series(name):
            key: Optional[str] = s.labels.get(label)
            if key is not None:
                out[key] = s.value
        return out

    def names(self) -> List[str]:
        seen, out = set(), []
        for s in self.samples:
            if s.name not in seen:
                seen.add(s.name)
                out.append(s.name)
        return out

    def __len__(self) -> int:
        return len(self.samples)

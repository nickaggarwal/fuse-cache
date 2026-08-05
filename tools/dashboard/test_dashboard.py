"""Unit tests for the dashboard's parser and selector logic.

Stdlib unittest only — no pytest dependency. The selector tests mirror
internal/coordinator/warm_test.go semantics.

    python3 -m unittest discover -s tools/dashboard
"""

import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from fuse_api import Peer, select_warm_targets  # noqa: E402
from metrics import MetricSet, parse  # noqa: E402


# A verbatim slice of handlePromMetrics output (internal/api/handler.go).
SAMPLE = """\
fuse_coordinator_available 1
fuse_nvme_used_bytes 1073741824
fuse_nvme_capacity_bytes 10737418240
fuse_tier_peer_read_latency_ms 4.250
fuse_tier_peer_success_ratio 0.9871
fuse_peer_serve_inflight 3
fuse_peer_serve_capacity 64
fuse_peer_serve_rejected_total 12
fuse_replica_reconcile_heat_boosts_total 7
fuse_peer_pair_latency_ms{peer="node-a"} 1.500
fuse_peer_pair_latency_ms{peer="node-b"} 9.250
fuse_peer_pair_success_ratio{peer="node-a"} 1.0000
fuse_peer_pair_samples_total{peer="node-a"} 42
fuse_gofuse_write_phase_seconds_total 0.123456
fuse_cache_nvme_hits_total 900
fuse_cache_nvme_misses_total 100
"""


class TestMetricsParser(unittest.TestCase):
    def setUp(self):
        self.ms = MetricSet.from_text(SAMPLE)

    def test_parses_every_line(self):
        self.assertEqual(len(self.ms), 16)

    def test_unlabeled_lookup(self):
        self.assertEqual(self.ms.get("fuse_nvme_used_bytes"), 1073741824.0)
        self.assertEqual(self.ms.get("fuse_peer_serve_capacity"), 64.0)

    def test_float_values(self):
        self.assertAlmostEqual(self.ms.get("fuse_tier_peer_success_ratio"), 0.9871)
        self.assertAlmostEqual(self.ms.get("fuse_gofuse_write_phase_seconds_total"), 0.123456)

    def test_missing_metric_returns_default(self):
        self.assertEqual(self.ms.get("fuse_not_emitted"), 0.0)
        self.assertEqual(self.ms.get("fuse_not_emitted", -1.0), -1.0)
        self.assertFalse(self.ms.has("fuse_not_emitted"))
        self.assertTrue(self.ms.has("fuse_nvme_used_bytes"))

    def test_labeled_family_grouped_by_label(self):
        lat = self.ms.by_label("fuse_peer_pair_latency_ms", "peer")
        self.assertEqual(lat, {"node-a": 1.5, "node-b": 9.25})
        self.assertEqual(len(self.ms.series("fuse_peer_pair_latency_ms")), 2)

    def test_labeled_metric_not_visible_as_unlabeled(self):
        self.assertEqual(self.ms.get("fuse_peer_pair_latency_ms"), 0.0)

    def test_comments_and_help_type_are_skipped(self):
        ms = MetricSet.from_text(
            "# HELP fuse_x a counter\n# TYPE fuse_x counter\nfuse_x 5\n"
        )
        self.assertEqual(len(ms), 1)
        self.assertEqual(ms.get("fuse_x"), 5.0)

    def test_blank_and_malformed_lines_are_skipped(self):
        ms = MetricSet.from_text("\n\nbad_line_no_value\nfuse_ok 1\nfuse_bad notanumber\n")
        self.assertEqual(ms.names(), ["fuse_ok"])

    def test_optional_timestamp_is_ignored(self):
        ms = MetricSet.from_text("fuse_x 5 1700000000000\n")
        self.assertEqual(ms.get("fuse_x"), 5.0)

    def test_multiple_labels_and_escapes(self):
        samples = parse('m{a="1",b="x=y",c="q\\"z"} 2.5')
        self.assertEqual(len(samples), 1)
        self.assertEqual(samples[0].labels, {"a": "1", "b": "x=y", "c": 'q"z'})
        self.assertEqual(samples[0].value, 2.5)

    def test_special_float_values(self):
        ms = MetricSet.from_text("a NaN\nb +Inf\nc -Inf\n")
        self.assertNotEqual(ms.get("a"), ms.get("a"))  # NaN != NaN
        self.assertEqual(ms.get("b"), float("inf"))
        self.assertEqual(ms.get("c"), float("-inf"))

    def test_empty_input(self):
        self.assertEqual(len(MetricSet.from_text("")), 0)
        self.assertEqual(len(MetricSet.from_text(None)), 0)


def peer(pid, status="active", **labels):
    return Peer(id=pid, address=f"{pid}:8081", status=status, labels=labels)


class TestSelectWarmTargets(unittest.TestCase):
    """Mirrors CoordinatorService.SelectWarmTargets (internal/coordinator/warm.go)."""

    def setUp(self):
        self.peers = [
            peer("node-c", pool="gpu", zone="a"),
            peer("node-a", pool="gpu", zone="b"),
            peer("node-b", pool="cpu", zone="a"),
            peer("node-d", status="inactive", pool="gpu", zone="a"),
        ]

    def ids(self, **kwargs):
        return [p.id for p in select_warm_targets(self.peers, **kwargs)]

    def test_empty_selector_returns_all_active_id_sorted(self):
        self.assertEqual(self.ids(), ["node-a", "node-b", "node-c"])

    def test_inactive_peers_are_never_selected(self):
        self.assertEqual(self.ids(nodes=["node-d"]), [])

    def test_node_id_filter(self):
        self.assertEqual(self.ids(nodes=["node-c", "node-a"]), ["node-a", "node-c"])

    def test_unknown_node_id_is_ignored(self):
        self.assertEqual(self.ids(nodes=["node-a", "ghost"]), ["node-a"])

    def test_label_filter_requires_all_labels_to_match(self):
        self.assertEqual(self.ids(labels={"pool": "gpu"}), ["node-a", "node-c"])
        self.assertEqual(self.ids(labels={"pool": "gpu", "zone": "a"}), ["node-c"])
        self.assertEqual(self.ids(labels={"pool": "gpu", "zone": "z"}), [])

    def test_missing_label_key_does_not_match(self):
        self.assertEqual(self.ids(labels={"absent": "x"}), [])

    def test_nodes_and_labels_intersect(self):
        self.assertEqual(
            self.ids(nodes=["node-a", "node-b"], labels={"pool": "gpu"}), ["node-a"]
        )

    def test_percentage_ceil_over_id_sorted_head(self):
        # 3 active, 50% -> ceil(1.5) = 2, taken off the sorted head.
        self.assertEqual(self.ids(percentage=50), ["node-a", "node-b"])
        self.assertEqual(self.ids(percentage=34), ["node-a", "node-b"])
        self.assertEqual(self.ids(percentage=33), ["node-a"])

    def test_percentage_minimum_one(self):
        self.assertEqual(self.ids(percentage=1), ["node-a"])

    def test_percentage_zero_and_hundred_mean_everything(self):
        self.assertEqual(self.ids(percentage=0), ["node-a", "node-b", "node-c"])
        self.assertEqual(self.ids(percentage=100), ["node-a", "node-b", "node-c"])

    def test_percentage_applies_after_filters(self):
        # pool=gpu narrows to [node-a, node-c]; 50% -> ceil(1.0) = 1.
        self.assertEqual(self.ids(labels={"pool": "gpu"}, percentage=50), ["node-a"])

    def test_percentage_on_empty_set_stays_empty(self):
        self.assertEqual(self.ids(labels={"pool": "none"}, percentage=50), [])

    def test_selection_is_deterministic(self):
        first = self.ids(percentage=50)
        for _ in range(5):
            self.assertEqual(self.ids(percentage=50), first)


class TestPeerParsing(unittest.TestCase):
    def test_from_json_uses_coordinator_field_names(self):
        p = Peer.from_json(
            {
                "id": "node-1",
                "address": "10.0.0.5:8081",
                "grpc_address": "10.0.0.5:9081",
                "nvme_path": "/mnt/nvme/fuse-cache",
                "available_space": 9,
                "used_space": 1,
                "status": "active",
                "last_heartbeat": "2026-08-05T10:00:00.123456789Z",
                "labels": {"pool": "gpu"},
            }
        )
        self.assertEqual(p.id, "node-1")
        self.assertEqual(p.available_space + p.used_space, 10)
        self.assertEqual(p.label_str, "pool=gpu")

    def test_null_labels_become_empty_dict(self):
        # Labels has `omitempty`, so it is absent/null for unlabeled peers.
        p = Peer.from_json({"id": "n", "labels": None})
        self.assertEqual(p.labels, {})
        self.assertEqual(p.label_str, "")


if __name__ == "__main__":
    unittest.main()

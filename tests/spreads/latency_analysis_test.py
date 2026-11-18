"""Reject incomplete or incorrectly correlated latency measurements."""
import csv
import json
from pathlib import Path
import tempfile
import unittest

from latency import analyze


class LatencyAnalysisTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        (self.root / "probes").mkdir()

    def fixture(self, scenario="group-implied", memory=300, mask=7, duplicate=False, missing=False):
        ordinary = scenario.startswith("ordinary-")
        shard = 1 if ordinary else 5
        (self.root / "run.json").write_text(json.dumps({
            "scenario": scenario, "count": 1, "window": 1, "warmup": 1, "seconds": 0.001,
        }))
        with (self.root / "client.csv").open("w", newline="") as stream:
            writer = csv.writer(stream)
            writer.writerow(["request", "order", "shard", "send_ns", "done_ns"])
            writer.writerow([100000, (shard << 48) | 1, shard, 100, 400])
        with (self.root / "probes" / "probe-1.csv").open("w", newline="") as stream:
            writer = csv.writer(stream)
            writer.writerow(["timestamp_ns", "sequence", "request", "kind", "shard", "participants"])
            start = [200, 10, 100000, 0 if ordinary else 2, shard, 0]
            writer.writerow(start)
            if duplicate:
                writer.writerow(start)
            if not missing:
                writer.writerow([memory, 10, 100000 if ordinary else 0,
                                 1 if ordinary else 3, shard if ordinary else 0, mask])

    def test_valid_clock_boundaries(self):
        for scenario in ("ordinary-hot", "group-direct", "group-implied"):
            with self.subTest(scenario=scenario):
                self.fixture(scenario, mask=1 if scenario == "group-direct" else 7)
                run, samples = analyze(self.root)
                self.assertEqual(run["validated_orders"], 1)
                self.assertEqual(samples[0]["memory_us"], 0.2)
                self.assertEqual(samples[0]["client_us"], 0.3)
                self.assertEqual(samples[0]["start_to_memory_us"], 0.1)

    def test_missing_memory(self):
        self.fixture(missing=True)
        with self.assertRaisesRegex(RuntimeError, "Missing group memory"):
            analyze(self.root)

    def test_duplicate_start(self):
        self.fixture(duplicate=True)
        with self.assertRaisesRegex(RuntimeError, "Duplicate request"):
            analyze(self.root)

    def test_clock_order(self):
        for memory in (150, 450):
            with self.subTest(memory=memory):
                self.fixture(memory=memory)
                with self.assertRaisesRegex(RuntimeError, "timestamps are out of order"):
                    analyze(self.root)

    def test_unexpected_participants(self):
        self.fixture(mask=1)
        with self.assertRaisesRegex(RuntimeError, "participant mask"):
            analyze(self.root)


if __name__ == "__main__":
    unittest.main()

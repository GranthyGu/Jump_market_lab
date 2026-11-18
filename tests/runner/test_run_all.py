"""Exercise runner failure propagation, timeouts and complete task discovery."""
import argparse
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import time
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import run_all as runner


class RunnerTests(unittest.TestCase):
    def test_failure_continues_and_missing_is_not_pass(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            marker = root / "later-ran"
            tasks = [runner.Task("failure", [sys.executable, "-c", "raise SystemExit(7)"], 5, "test"),
                     runner.Task("missing", ["/nonexistent/test-program"], 5, "test"),
                     runner.Task("later", [sys.executable, "-c",
                         "from pathlib import Path; import sys; Path(sys.argv[1]).write_text('ok')", str(marker)], 5, "test")]
            report = {"scope": "all", "quick": False, "selected_tests": [t.name for t in tasks],
                      "excluded": [], "results": []}
            self.assertEqual(runner.run_tasks(tasks, root, report, os.environ), 1)
            self.assertTrue(marker.exists())
            saved = json.loads((root / "summary.json").read_text())
            self.assertFalse(saved["passed"])
            self.assertEqual([r["status"] for r in saved["results"]], ["FAIL", "FAIL", "PASS"])
            self.assertEqual(saved["results"][0]["exit_code"], 7)

    def test_deadline_reports_failure(self):
        with tempfile.TemporaryDirectory() as tmp:
            task = runner.Task("slow", [sys.executable, "-c",
                "import subprocess, sys, time; subprocess.Popen([sys.executable, '-c', "
                "'import time; time.sleep(30)']); time.sleep(30)"], 0.1, "test")
            result = runner.execute(task, Path(tmp), os.environ)
            self.assertEqual(result["status"], "FAIL")
            self.assertTrue(result["timed_out"])
            self.assertLess(result["wall_seconds"], 5)

    def test_preflight_failure_is_recorded(self):
        with tempfile.TemporaryDirectory() as tmp:
            result = runner.execute(runner.Task("not_built", [], 1, "test", "missing build"), Path(tmp), os.environ)
            self.assertEqual(result["status"], "FAIL")
            self.assertEqual(result["error"], "missing build")

    def test_interrupt_stops_remaining_tasks_even_if_sigint_was_ignored(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            driver = r'''
import os, signal, sys
from pathlib import Path
from unittest.mock import patch
sys.path.insert(0, sys.argv[1])
import run_all as runner
root = Path(sys.argv[2])
signal.signal(signal.SIGINT, signal.SIG_IGN)
tasks = [runner.Task('slow', [sys.executable, '-c',
    "from pathlib import Path; import sys,time; Path(sys.argv[1]).touch(); time.sleep(30)",
    str(root/'ready')], 10, 'test'),
    runner.Task('later', [sys.executable, '-c',
    "from pathlib import Path; import sys; Path(sys.argv[1]).touch()", str(root/'later')], 5, 'test')]
sys.argv = ['run_all', '--build-dir', str(root), '--output', str(root/'results'), '--regression-only']
with patch.object(runner, 'inventory', return_value=[{}]), patch.object(runner, 'plan', return_value=tasks):
    sys.exit(runner.main())
'''
            proc = subprocess.Popen([sys.executable, "-c", driver, str(Path(runner.__file__).parent), tmp],
                                    stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
            try:
                deadline = time.monotonic() + 5
                while not (root / "ready").exists() and proc.poll() is None and time.monotonic() < deadline:
                    time.sleep(0.01)
                self.assertTrue((root / "ready").exists(), "test child did not start")
                proc.send_signal(signal.SIGINT)
                output, _ = proc.communicate(timeout=5)
                self.assertEqual(proc.returncode, 1)
                report = json.loads(next((root / "results").glob("*/summary.json")).read_text())
                self.assertFalse((root / "later").exists(), output + json.dumps(report))
                self.assertTrue(report["interrupted"])
                self.assertFalse(report["passed"])
            finally:
                if proc.poll() is None:
                    proc.terminate()
                    proc.communicate(timeout=5)
                if proc.stdout: proc.stdout.close()

    def test_plan_includes_new_ctest_and_every_standalone_group(self):
        with tempfile.TemporaryDirectory() as tmp:
            build = Path(tmp)
            (build / "CMakeCache.txt").write_text("CMAKE_CXX_COMPILER:FILEPATH=/usr/bin/c++\nMARKETLAB_BUILD_SQLITE_BENCHMARK:BOOL=OFF\n")
            args = argparse.Namespace(timeout=None, regression_only=False, skip_cmu=False, quick=True, jobs=2, cmu_source=build / "source")
            tests = [{"name": "new_future_test", "command": ["some-test"],
                      "properties": [{"name": "TIMEOUT", "value": 40}]}]
            tasks = runner.plan(args, build, build / "report", tests)
            self.assertEqual(tasks[0].name, "new_future_test")
            self.assertEqual(tasks[0].timeout, 70)
            self.assertEqual({t.name for t in tasks[1:]}, {
                "persistence_queue_recovery_spsc_park", "persistence_queue_recovery_spsc_yield",
                "persistence_queue_comparison", "snapshot_contention_benchmark", "trade_history_benchmark",
                "bpt_cache_concurrency_comparison", "bpt_sqlite_comparison", "cmu_public",
                "spread_process_benchmark", "spread_latency_comparison"})
            self.assertIn("SQLite tests are disabled", next(t.missing for t in tasks if t.name == "bpt_sqlite_comparison"))
            self.assertTrue(all(t.timeout > 0 for t in tasks))
            latency = next(t for t in tasks if t.name == "spread_latency_comparison")
            self.assertIn("--prepare-build", latency.command)
            self.assertIn("200", latency.command)
            self.assertFalse(latency.missing)
            args.skip_cmu = True
            without_cmu = runner.plan(args, build, build / "report", tests)
            self.assertEqual([t.name for t in without_cmu], [t.name for t in tasks if t.name != "cmu_public"])
            args.regression_only = True
            self.assertEqual([t.name for t in runner.plan(args, build, build / "report", tests)], ["new_future_test"])


if __name__ == "__main__":
    unittest.main()

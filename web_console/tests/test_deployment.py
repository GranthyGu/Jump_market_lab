from pathlib import Path
from types import SimpleNamespace
import json
import os
import subprocess
import sys
import tempfile
import time
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from bridge import Journal
from deployment import cleanup_orphans, data_directory, owned_orphans


class DeploymentTests(unittest.TestCase):
    def setUp(self):
        (ROOT / "runtime/tests").mkdir(parents=True, exist_ok=True)
        self.temp = tempfile.TemporaryDirectory(prefix="deployment-", dir=ROOT / "runtime/tests")
        self.root = Path(self.temp.name)
        self.exchange = self.root / "exchange.conf"
        self.exchange.write_text("spreads=enabled\nsymbol=ITEM0001,100,1\nsymbol=A,120,6\n")
        self.config = SimpleNamespace(exchange=self.exchange, publisher=self.root / "publisher.conf")
        self.log = Journal()

    def tearDown(self):
        self.temp.cleanup()

    def test_changed_symbol_ids_get_a_separate_persistent_directory(self):
        first = data_directory(self.config, self.root, None, self.log)
        (first / "retained-order").write_text("existing state")
        self.exchange.write_text("spreads=enabled\nsymbol=ITEM0001,100,1\nsymbol=ITEM0002,100,2\nsymbol=A,120,6\n")
        second = data_directory(self.config, self.root, None, self.log)
        self.assertNotEqual(first, second)
        self.assertEqual(second, data_directory(self.config, self.root, None, self.log))
        self.assertEqual((first / "retained-order").read_text(), "existing state")
        with self.assertRaisesRegex(ValueError, "different symbol"):
            data_directory(self.config, self.root, first, self.log)

    def test_legacy_identification_does_not_clear_recovery_flags(self):
        base = self.root / "data"
        base.mkdir()
        state = base / "cluster-state.json"
        original = '{"group_clean_shutdown":false,"transition":{"pending":true}}'
        state.write_text(original)
        logs = self.root / "logs/engines/old-run"
        logs.mkdir(parents=True)
        (logs / "router.conf").write_text(self.exchange.read_text())
        self.exchange.write_text("spreads=enabled\nsymbol=ITEM0002,100,1\nsymbol=A,120,6\n")
        selected = data_directory(self.config, self.root, None, self.log)
        self.assertNotEqual(selected, base)
        self.assertEqual(state.read_text(), original)

    def test_cleanup_excludes_live_parents_and_foreign_binaries(self):
        executable = str(self.root / "build/bin/market_engine_router")
        command = "Wed Nov 18 20:00:00 2026 " + executable + " --config " + str(self.root / "logs/engines/run/router.conf")
        rows = {
            10:dict(pid=10,parent=1,group=9,executable=executable),
            11:dict(pid=11,parent=99,group=9,executable=executable),
            12:dict(pid=12,parent=1,group=9,executable="/other/bin/market_engine_router"),
        }
        with patch("deployment.process_rows", return_value=rows), patch("deployment.command_for", return_value=command):
            self.assertEqual([p["pid"] for p in owned_orphans(self.config, self.root)], [10])

    def test_cleanup_does_not_signal_a_reused_pid(self):
        row = dict(pid=10,parent=1,group=9,executable="old",role="router",identity="old process")
        with patch("deployment.owned_orphans", return_value=[row]), \
             patch("deployment.process_rows", return_value={10:dict(pid=10,parent=1,group=9,executable="different")}), \
             patch("deployment.os.kill") as kill:
            cleanup_orphans(self.config, self.root, self.log)
            kill.assert_not_called()

    def test_guardian_stops_service_when_console_lifeline_closes(self):
        ready, record = self.root / "ready", self.root / "host.json"
        program = ("import os,signal,sys,time; from pathlib import Path; "
                   "signal.signal(signal.SIGTERM,lambda *_:sys.exit(0)); "
                   f"Path({str(ready)!r}).write_text(str(os.getpid())); time.sleep(60)")
        with (self.root / "host.log").open("w") as log:
            host = subprocess.Popen([sys.executable,"-B",str(ROOT / "service_host.py"),
                "--name","test","--record",str(record),"--",sys.executable,"-B","-c",program],
                stdin=subprocess.PIPE,stdout=log,stderr=subprocess.STDOUT,start_new_session=True)
            try:
                deadline = time.monotonic() + 5
                while not ready.exists() and time.monotonic() < deadline:
                    time.sleep(.02)
                self.assertTrue(ready.exists())
                pid = int(ready.read_text())
                host.stdin.close()
                self.assertEqual(host.wait(8), 0)
                with self.assertRaises(ProcessLookupError):
                    os.kill(pid, 0)
                self.assertEqual(json.loads(record.read_text())["status"], "exited")
            finally:
                if host.poll() is None:
                    host.terminate()
                    host.wait(8)
                if not host.stdin.closed:
                    host.stdin.close()


if __name__ == "__main__":
    unittest.main()

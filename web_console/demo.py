"""A disposable workspace and a paced, real-Gateway demonstration scenario."""
import json
from itertools import groupby
import shutil
import threading
import time
import uuid

from deployment import atomic_json
from demo_scenario import scenario


class DemoWorkspace:
    def __init__(self, runtime):
        self.parent = runtime / "demo"
        self.identity = uuid.uuid4().hex
        self.root = self.parent / ("session-" + self.identity)
        self.root.mkdir(parents=True)
        self.data = self.root / "data"
        self.marker = dict(kind="marketlab-disposable-demo", version=1, identity=self.identity)
        atomic_json(self.root / "session.json", self.marker)

    def cleanup(self):
        # Only the fresh directory created by this object may be removed.
        if (self.root.is_symlink() or self.root.parent.resolve() != self.parent.resolve()
                or self.root.name != "session-" + self.identity
                or json.loads((self.root / "session.json").read_text()) != self.marker):
            raise RuntimeError("Demo directory identity changed; data retained")
        shutil.rmtree(self.root)


class PriceTape:
    """Deduplicate both counterparties and history queries into actual executions."""
    def __init__(self, products):
        self.lock = threading.Lock()
        self.points = {p["symbol"]: {} for p in products}
        self.oldest = {}
        self.revision = 0

    def record(self, trade):
        with self.lock:
            points = self.points.get(trade["symbol"])
            if points is None:
                return
            key = (int(trade["timestamp_ns"]), int(trade["id"]))
            if key in points:
                return
            if len(points)>=400 and key < self.oldest[trade["symbol"]]:
                return
            points[key] = {k: trade[k] for k in ("id", "timestamp", "timestamp_ns", "price", "quantity")}
            if len(points) > 400:
                del points[min(points)]
            if len(points)>=400:
                self.oldest[trade["symbol"]]=min(points)
            self.revision += 1

    def snapshot(self, symbol):
        with self.lock:
            if symbol not in self.points:
                raise ValueError("Unknown chart symbol")
            points = sorted(self.points[symbol].values(), key=lambda p: (int(p["timestamp_ns"]), int(p["id"])))
            return dict(symbol=symbol, revision=self.revision, points=[p.copy() for p in points])


class DemoReplay:
    def __init__(self, console, enabled):
        self.app, self.enabled = console, enabled
        self.lock = threading.RLock()
        self.cancel = threading.Event()
        self.thread = None
        self.updates, self.accepted_ids = {}, set()
        self.state = dict(enabled=enabled, status="Empty" if enabled else "Disabled", running=False,
                          phase="Ready to load", accepted=0, total=0, duration=12, elapsed=0,
                          error="", active_orders=0, history_records=0, filled_orders=0)

    def observe(self, order):
        with self.lock:
            if self.state["running"]:
                self.updates[order["id"]] = (order["status"], order["remaining"])

    def completed_orders(self):
        with self.lock:
            return sum(self.updates.get(oid) == ("Filled", 0) for oid in self.accepted_ids)

    def snapshot(self):
        with self.lock:
            state = self.state.copy()
        if state["running"] and state.get("started"):
            state["elapsed"] = round(time.time() - state["started"], 1)
        return state

    def start(self, duration=12):
        if type(duration) is not int or not 10 <= duration <= 15:
            raise ValueError("Choose a replay duration from 10 to 15 seconds")
        with self.lock:
            if not self.enabled:
                raise RuntimeError("Demo loading is available only in a disposable demo workspace")
            if self.state["status"] != "Empty":
                raise RuntimeError("This demo has already been loaded. Stop and start for a fresh empty session")
            services = self.app.services.snapshot()
            if services["cluster"] != "Live" or not services["owned"] or services["operation"]:
                raise RuntimeError("Wait for all managed engines to be ready")
            for name, trader in self.app.traders.items():
                if name not in self.app.config.accounts:
                    raise RuntimeError("Demo replay requires the configured Alice and Bob accounts")
                state = trader.snapshot()
                if state["status"] not in ("Logged out", "Ready") or state["username"] != name:
                    raise RuntimeError("Use the Alice and Bob accounts for this demo")
                if state["orders"] or state["fills"] or state["pending"] or state["history_busy"]:
                    raise RuntimeError("Load the demo before manual trading; stop and start for an empty session")
            seed = int(uuid.uuid4().hex[:16],16)
            self.plan = scenario(self.app.config, seed=seed)
            self.state.update(status="Loading", running=True, phase="Connecting accounts", duration=duration,
                              total=len(self.plan), started=time.time(), seed=str(seed))
            self.thread = threading.Thread(target=self._run, daemon=True, name="demo-replay")
            self.thread.start()

    def _wait(self, predicate, timeout=10):
        deadline = time.monotonic() + timeout
        while not predicate():
            if self.cancel.wait(.002):
                raise InterruptedError("Demo loading stopped; accepted orders remain available")
            if time.monotonic() > deadline:
                raise TimeoutError("Demo preparation timed out; inspect the activity log")

    def _history(self, all_pages=True):
        for trader in self.app.traders.values():
            trader.query_history(all_pages=all_pages)
        while True:
            self._wait(lambda: all(not t.progress()["history_busy"] for t in self.app.traders.values()))
            snapshots = [t.progress() for t in self.app.traders.values()]
            if any(t["history_error"] for t in snapshots):
                raise RuntimeError("Demo history query failed; use Query trades to retry")
            if not all_pages or not any(t["history_more"] for t in snapshots):
                break
            for trader in self.app.traders.values():
                if trader.progress()["history_more"]:
                    trader.query_history(more=True)

    def _run(self):
        started = time.monotonic()
        try:
            for name, trader in self.app.traders.items():
                if trader.progress()["status"] != "Ready":
                    trader.login(name, self.app.config.accounts[name])
            self._wait(lambda: all(t.progress()["status"] == "Ready" for t in self.app.traders.values()))
            steps = self.plan
            began, phase, index = time.monotonic(), None, 0
            settling = set()
            self.app.log.add("demo", f"DEMO START {len(steps)} real orders over {self.state['duration']} seconds; random seed={self.state['seed']}")
            for _, chunk in groupby(steps, key=lambda step:step["batch"]):
                chunk = list(chunk)
                if chunk[0]["phase"] != phase:
                    phase = chunk[0]["phase"]
                    self.app.log.add("demo", "DEMO PHASE " + phase)
                pending = []
                for step in chunk:
                    due = began + self.state["duration"] * step["at"]
                    if self.cancel.wait(max(0, due - time.monotonic())):
                        raise InterruptedError("Demo loading stopped; accepted orders remain available")
                    trader = self.app.traders[step["slot"]]
                    request = trader.submit(step["symbol"], step["side"], step["price"], step["quantity"])
                    pending.append((trader,request,step["resting"]))
                    index += 1
                for trader,request,resting in pending:
                    receipt = trader.wait_receipt(request)
                    with self.lock:
                        self.accepted_ids.add(receipt["id"])
                    if not resting:
                        settling.add(receipt["id"])
                if chunk[-1]["settle"]:
                    def settled():
                        with self.lock:
                            return all(self.updates.get(oid)==("Filled",0) for oid in settling)
                    self._wait(settled)
                    settling.clear()
                with self.lock:
                    self.state.update(phase=phase, accepted=index,
                                      active_orders=sum(t.progress()["orders"] for t in self.app.traders.values()))
            retained = 50 * len(self.app.config.products)
            expected_fills = len(steps) - retained
            self._wait(lambda:self.completed_orders() == expected_fills)
            if sum(t.progress()["orders"] for t in self.app.traders.values()) != retained:
                raise RuntimeError("Demo order reconciliation failed; inspect active orders")
            with self.lock:
                self.state.update(phase="Loading trade history", filled_orders=expected_fills)
            self._history()
            self.app.market.request_refresh()
            with self.lock:
                self.state.update(status="Ready", phase="Ready for manual trading")
            self.app.log.add("demo", f"DEMO COMPLETE accepted={len(steps)} fully_filled={expected_fills} resting={retained}. Manual trading is ready", "success")
        except InterruptedError as exc:
            with self.lock:
                self.state.update(status="Stopped", phase="Manual trading available")
            self.app.log.add("demo", str(exc), "warning")
        except Exception as exc:
            with self.lock:
                self.state.update(status="Failed", phase="Replay stopped", error=str(exc))
            self.app.log.add("demo", "DEMO FAILED " + str(exc), "error")
        finally:
            snapshots = [t.progress() for t in self.app.traders.values()]
            with self.lock:
                self.state.update(running=False, elapsed=round(time.monotonic()-started, 1),
                                  active_orders=sum(t["orders"] for t in snapshots),
                                  history_records=sum(t["history"] for t in snapshots))

    def close(self):
        self.cancel.set()
        if self.thread:
            self.thread.join(12)
            if self.thread.is_alive():
                raise RuntimeError("Demo replay is still stopping; data retained")

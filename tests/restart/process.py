#!/usr/bin/env python3
"""Isolated TCP restart regression and workload measurement. No production paths."""
import argparse
from collections import Counter
import hashlib
import importlib.util
import json
import select
from pathlib import Path
import socket
import statistics
import struct
import subprocess
import sys
import tempfile
import threading
import time

ROOT = Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location("history_process", ROOT / "tests/bpt/process.py")
history = importlib.util.module_from_spec(spec)
spec.loader.exec_module(history)
Trader, check, fixed = history.Trader, history.check, history.fixed


def exact(connection, count):
    result = bytearray()
    while len(result) < count:
        part = connection.recv(count - len(result))
        if not part:
            raise EOFError("market peer closed")
        result.extend(part)
    return result


def shard_market(port):
    with socket.create_connection(("127.0.0.1", port), timeout=3) as connection:
        connection.sendall(b"\x01")
        length, = struct.unpack("<I", exact(connection, 4))
        data = exact(connection, length)
    count, = struct.unpack_from("<H", data, 16)
    offset, books = 18, {}
    for _ in range(count):
        symbol, levels = struct.unpack_from("<HB", data, offset)
        offset += 3
        books[symbol] = [struct.unpack_from("<BiQQ", data, offset + i * 21) for i in range(levels)]
        offset += levels * 21
    check(offset == len(data), "market frame trailing bytes")
    return books


def publisher_market(port, symbol, metadata=False):
    with socket.create_connection(("127.0.0.1", port), timeout=3) as connection:
        connection.sendall(struct.pack("<H", symbol))
        length, = struct.unpack("<H", exact(connection, 2))
        data = exact(connection, length)
    check(data[0] == 2 and struct.unpack_from("<H", data, 17)[0] == symbol, "publisher snapshot header")
    if metadata:
        return bytes(data[1:17]), struct.unpack_from("<Q", data, 19)[0]
    return [struct.unpack_from("<BiQQ", data, i) for i in range(35, len(data), 21)]


def command(port, text):
    with socket.create_connection(("127.0.0.1", port), timeout=300) as connection:
        connection.sendall(text.encode() + b"\n")
        with connection.makefile("rb") as stream:
            return json.loads(stream.readline())


class Deployment:
    def __init__(self, binaries, root, copy_rate):
        self.root, self.bin = root, binaries
        self.copy_rate = copy_rate
        held = [socket.socket() for _ in range(33)]
        for connection in held:
            connection.bind(("127.0.0.1", 0))
        ports = iter(connection.getsockname()[1] for connection in held)
        self.gateway, self.router, self.admin, self.control, self.query = [next(ports) for _ in range(5)]
        self.topology = {"router_admin_port": self.admin, "supervisor_port": self.control, "shards": {}}
        lines = ["routing=router", f"gateway_listen=127.0.0.1,{self.gateway}",
                 f"router_listen=127.0.0.1,{self.router}", f"engine_endpoint=127.0.0.1,{self.router}"]
        lines += [f"user={user},demo" for user in ("Alice", "Bob", "Carol", "Load")]
        sources = []
        for shard in range(1, 5):
            config = {"market": next(ports)}
            for slot in ("A", "B"):
                config[slot] = dict(zip(("order", "control", "market"), [next(ports) for _ in range(3)]))
            self.topology["shards"][str(shard)] = config
            lines.append(f"shard={shard},127.0.0.1,{config['A']['order']}")
            sources.append(f"shard=s{shard},tcp,127.0.0.1,{config['market']},{root}/unused-{shard}.mmap,{shard},{shard + 4}")
        lines += [f"symbol=ITEM{i},100,{i}" for i in range(1, 5)]
        lines += [f"symbol=LOAD{i},100,{i}" for i in range(1, 5)]
        self.config = root / "exchange.conf"
        self.config.write_text("\n".join(lines) + "\n")
        self.market = root / "publisher.conf"
        self.udp = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.udp.bind(("127.0.0.1", 0))
        symbols = ",".join([f"{i}:ITEM{i}" for i in range(1, 5)] + [f"{i+4}:LOAD{i}" for i in range(1, 5)])
        self.market.write_text(f"symbols={symbols}\n" + "\n".join(sources) +
                               f"\nudp_receiver=127.0.0.1,{self.udp.getsockname()[1]}\nquery_host=127.0.0.1\nquery_port={self.query}\n")
        topology_path = root / "cluster.json"
        topology_path.write_text(json.dumps(self.topology))
        for connection in held:
            connection.close()
        self.children, self.streams = [], []
        self.cluster_args = [sys.executable, ROOT / "src/matching_engine/cluster/run.py", "--config", self.config,
                     "--market-config", self.market, "--cluster-config", topology_path,
                     "--build-dir", binaries, "--data-dir", root / "data", "--log-dir", root / "logs",
                     "--checkpoint-ms", "50", "--copy-mib-per-second", str(copy_rate)]
        self.cluster_process = self.launch(self.cluster_args, "cluster.log")
        self.launch([binaries / "market_gateway", "gateway", "--config", self.config], "gateway.log")
        self.publisher = self.launch([binaries / "market_broadcast", "publish", "--config", self.market], "publisher.log")
        deadline = time.monotonic() + 20
        while time.monotonic() < deadline:
            try:
                if command(self.control, "status")["ok"]:
                    trader = Trader(self.gateway, "Carol")
                    trader.close()
                    shard_market(self.topology["shards"]["1"]["market"])
                    return
            except (OSError, EOFError, AssertionError):
                pass
            time.sleep(0.05)
        self.close()
        raise RuntimeError(f"startup timed out, see {root}")

    def launch(self, args, name):
        stream = (self.root / name).open("w")
        self.streams.append(stream)
        process = subprocess.Popen(list(map(str, args)), stdin=subprocess.DEVNULL,
                                   stdout=stream, stderr=subprocess.STDOUT)
        self.children.append(process)
        return process

    def close(self):
        self.udp.close()
        for process in reversed(self.children):
            if process.poll() is None:
                process.terminate()
        for process in self.children:
            try:
                process.wait(timeout=40)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()
        for stream in self.streams:
            stream.close()


class Load:
    """Bounded pipelined self-trades on separate symbols; verify every response."""
    def __init__(self, port, rate):
        self.trader = Trader(port, "Load")
        self.trader.sock.settimeout(35)
        self.rate = rate
        self.stop = threading.Event()
        self.finished = threading.Event()
        self.lock = threading.Lock()
        self.pending = {}
        self.sent = self.accepted = 0
        self.trades = Counter()
        self.latencies = []
        self.errors = []
        self.started = time.monotonic()
        self.writer = threading.Thread(target=self.send)
        self.reader = threading.Thread(target=self.receive)
        self.reader.start()
        self.writer.start()

    def send(self):
        try:
            while not self.stop.is_set():
                # Eight orders: one crossing pair per shard. Bounded admission
                # prevents overload limits from obscuring restart correctness.
                for index in range(8):
                    while True:
                        with self.lock:
                            available = len(self.pending) < 128
                        if available:
                            break
                        if self.errors:
                            return
                        time.sleep(0.001)
                    shard, side = index // 2 + 1, index % 2 + 1
                    with self.lock:
                        request = self.trader.request
                        self.pending[request] = time.monotonic()
                        self.trader.send(3, struct.pack("<32sBii", fixed(f"LOAD{shard}"), side, 100, 1))
                        self.sent += 1
                    expected = self.sent / self.rate
                    delay = expected - (time.monotonic() - self.started)
                    if delay > 0:
                        time.sleep(delay)
        except Exception as error:
            self.errors.append(repr(error))
        finally:
            self.finished.set()

    def receive(self):
        try:
            while True:
                with self.lock:
                    done = self.finished.is_set() and not self.pending and sum(self.trades.values()) == self.sent
                if done:
                    return
                if not select.select([self.trader.sock], [], [], .1)[0]:
                    continue
                kind, request, payload = self.trader.receive()
                if kind == 5 and request:
                    with self.lock:
                        check(request in self.pending, "duplicate/unknown acceptance")
                        self.latencies.append((time.monotonic() - self.pending.pop(request)) * 1000)
                        self.accepted += 1
                elif kind == 6:
                    trade_id = struct.unpack_from("<Q", payload, 8)[0]
                    self.trades[trade_id] += 1
                    check(self.trades[trade_id] <= 2, "duplicate live trade notification")
                else:
                    check(kind == 5 and request == 0, f"load rejected/disconnected: kind={kind}")
        except Exception as error:
            self.errors.append(repr(error))

    def close(self):
        self.stop.set()
        self.writer.join(timeout=40)
        self.reader.join(timeout=40)
        self.trader.close()
        check(not self.writer.is_alive() and not self.reader.is_alive(), "load threads timed out")
        check(not self.errors, f"load errors: {self.errors}")
        check(self.sent == self.accepted, "missing accepted orders")
        check(all(count == 2 for count in self.trades.values()), "missing participant notifications")
        samples = sorted(self.latencies)
        return {"sent": self.sent, "accepted": self.accepted, "trades": len(self.trades),
                "elapsed_s": time.monotonic() - self.started,
                "target_orders_per_second": self.rate,
                "latency_p50_ms": statistics.median(samples),
                "latency_p99_ms": samples[min(len(samples) - 1, int(len(samples) * .99))],
                "latency_max_ms": max(samples)}


def checkpoint_digest(root, shard):
    files = sorted((Path(root) / f"shard-{shard}" / "checkpoints").glob("*.bin"))
    return {str(path): (path.stat().st_mtime_ns, hashlib.sha256(path.read_bytes()).hexdigest()) for path in files}


def restart_checked(ex, shard, expected_from, target, binary=None):
    result = {}
    def operation():
        result.update(command(ex.control, f"restart {shard}" + (f" {binary}" if binary else "")))
    thread = threading.Thread(target=operation)
    thread.start()
    frozen = None
    source_root = None
    while thread.is_alive():
        state = command(ex.control, "status")["result"]["shards"][str(shard)]
        if state["phase"] == "copying":
            source_root = state["root"]
            current = checkpoint_digest(source_root, shard)
            if frozen is None:
                frozen = current
            check(current == frozen, "checkpoint refreshed during baseline copy")
        time.sleep(.01)
    thread.join()
    if binary:
        check(not result["ok"], "broken replacement unexpectedly promoted")
        state = command(ex.control, "status")["result"]["shards"][str(shard)]
        check(state["active"] == expected_from and state["phase"] == "active", "source not resumed after abort")
        return {"abort_verified": True, "error": result["error"]}
    check(result.get("ok"), f"restart failed: {result}")
    if ex.copy_rate:
        check(frozen is not None, "test did not observe baseline copy")
    if frozen:
        check(checkpoint_digest(source_root, shard) == frozen, "frozen checkpoint changed before source retired")
    metric = result["result"]
    check(metric["from"] == expected_from and metric["to"] == target, "wrong direction")
    check(metric["source_after_copy"] > metric["baseline"], "no transactions accumulated during copy")
    check(metric["final_seq"] >= metric["source_after_copy"], "standby failed to catch copying interval")
    return metric


def exercise(binaries, root, resting, rate, copy_rate, warmup, switch_gap):
    ex = Deployment(binaries, root, copy_rate=copy_rate)
    load = None
    try:
        alice, bob = Trader(ex.gateway, "Alice"), Trader(ex.gateway, "Bob")
        expected = []
        fifo = {}
        for shard in range(1, 5):
            for _ in range(3):
                expected.append(history.add_trade(alice, bob, f"ITEM{shard}"))
            fifo[shard] = [bob.order(f"ITEM{shard}", 2, 5) for _ in range(3)]
            buy = alice.order(f"ITEM{shard}", 1, 2)
            a, b = alice.fills(1)[0], bob.fills(1)[0]
            expected.append((a[0], a[1], a[3], a[5], a[6], buy, fifo[shard][0], fixed("Alice"), fixed("Bob")))
        # Populate a nontrivial file on every shard using bounded batches.
        extra = []
        for start in range(0, resting, 128):
            requests = [bob.send(3, struct.pack("<32sBii", fixed(f"ITEM{i % 4 + 1}"), 2, 110, 1))
                        for i in range(start, min(resting, start + 128))]
            responses = set()
            for _ in requests:
                kind, request, payload = bob.receive()
                check(kind == 5 and request in requests and request not in responses, "seed acceptance")
                responses.add(request)
                extra.append(struct.unpack_from("<Q", payload, 8)[0])
        time.sleep(.15)
        deadline = time.monotonic() + 5
        while True:
            try:
                before_market = publisher_market(ex.query, 1, metadata=True)
                break
            except (OSError, EOFError):
                if time.monotonic() > deadline:
                    raise
                time.sleep(.05)
        first, pages = alice.page(limit=1)
        load = Load(ex.gateway, rate)
        time.sleep(warmup)
        results = []
        for round_index, (origin, target) in enumerate((("A", "B"), ("B", "A"))):
            for shard in range(1, 5):
                results.append(restart_checked(ex, shard, origin, target))
                public_view = shard_market(ex.topology["shards"][str(shard)]["market"])
                check((2, 100, 13 - round_index, 3) in public_view[shard], "market proxy did not follow active engine")
                # Old public sessions remain usable. Partial maker + same-price
                # FIFO survive both directions, including changes after a cut.
                buy = alice.order(f"ITEM{shard}", 1, 1)
                a, b = alice.fills(1)[0], bob.fills(1)[0]
                check(b[2] == fifo[shard][0], "partial maker lost FIFO priority")
                expected.append((a[0], a[1], a[3], a[5], a[6], buy, fifo[shard][0], fixed("Alice"), fixed("Bob")))
                time.sleep(switch_gap)
        abort = restart_checked(ex, 1, "A", "B", binary="/usr/bin/false")
        load_result = load.close()
        load = None
        # Cursor captured before any switch must still name the identical prefix.
        while pages:
            shard, flags, cursor, through = pages.pop(0)
            if flags & 1:
                records, ends = alice.page(shard, cursor, through, 1)
                first.extend(records)
                pages.extend(ends)
        check(sorted(first, key=lambda record: record[1]) == sorted(expected[:16], key=lambda record: record[1]),
              "history continuation changed across migration")
        check(alice.history() == sorted(expected, key=lambda record: record[1]), "history differs from live trades")
        for shard in range(1, 5):
            # Consume the remaining maker (1), then the second (5), then one of
            # the third. Verify the exact order of all resulting trades.
            alice.order(f"ITEM{shard}", 1, 7)
            alice.fills(3)
            sellers = bob.fills(3)
            check([trade[2] for trade in sellers] == fifo[shard], "FIFO reordered after round trip")
        for order_id in extra[:8]:
            request = bob.send(4, struct.pack("<Q", order_id))
            kind, response, payload = bob.receive()
            check(kind == 5 and response == request and payload[-1] == 4, "cancel after restart")
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            if all((2, 100, 4, 1) in publisher_market(ex.query, shard) for shard in range(1, 5)):
                break
            time.sleep(.1)
        else:
            raise AssertionError("live publisher did not converge after switches")
        check(ex.publisher.poll() is None, "publisher stopped during restart")
        after_market = publisher_market(ex.query, 1, metadata=True)
        check(after_market[0] == before_market[0] and after_market[1] > before_market[1],
              "planned cut reset publisher epoch or stalled its sequence")
        ex.udp.settimeout(2)
        packet = ex.udp.recv(65536)
        check(len(packet) >= 35 and packet[0] in (1, 2), "publisher UDP delivery failed")
        alice.close()
        bob.close()
        snapshot = Trader(ex.gateway)
        request = snapshot.send(1, fixed("Bob") + fixed("demo"))
        kind, response, _ = snapshot.receive()
        check(kind == 2 and response == request, "snapshot re-login failed")
        active = {}
        while True:
            kind, response, payload = snapshot.receive()
            check(kind == 7 and response == request, "snapshot stream correlation")
            if payload[8]:
                break
            order = struct.unpack_from("<Q32sBiiiiB", payload, 9)
            check(order[0] not in active, "duplicate snapshot order")
            active[order[0]] = order[6]
        expected_active = {order_id: 1 for order_id in extra[8:]}
        expected_active.update({orders[2]: 4 for orders in fifo.values()})
        check(active == expected_active, "login snapshot lost order state across handoffs")
        snapshot.close()
        # Trade API still exposes every self-trade exactly once in history.
        trader = Trader(ex.gateway, "Load")
        check(len(trader.history()) == load_result["trades"], "replicated trade history lost/duplicated records")
        trader.close()
        cli = subprocess.run([str(binaries / "gateway_client"), "--host", "127.0.0.1", "--port", str(ex.gateway),
                              "--user", "Alice", "--password", "demo"], input="trades\nquit\n", text=True,
                             stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=35)
        (root / "client.log").write_text(cli.stdout)
        check(cli.returncode == 0 and f"HISTORY_COMPLETE total={len(expected) + 12}" in cli.stdout,
              "stock client API failed after restart")
        # Persist a mixed A/B topology, stop the whole managed group, and prove
        # ordinary startup uses the latest roots rather than stale slot A data.
        check(command(ex.control, "restart 1")["ok"], "prepare mixed topology")
        ex.cluster_process.terminate()
        ex.cluster_process.wait(timeout=20)
        time.sleep(1)
        ex.cluster_process = ex.launch(ex.cluster_args, "cluster-reopened.log")
        deadline = time.monotonic() + 20
        while time.monotonic() < deadline:
            try:
                state = command(ex.control, "status")["result"]
                if state["shards"]["1"]["active"] == "B":
                    trader = Trader(ex.gateway, "Load")
                    check(len(trader.history()) == load_result["trades"], "cluster reopen chose stale data")
                    trader.close()
                    break
            except (OSError, EOFError, AssertionError):
                pass
            time.sleep(.05)
        else:
            raise AssertionError("cluster failed to reopen its persisted A/B topology")
        result = {"restarts": results, "workload": load_result, "abort": abort,
                  "checks": ["four shards A->B->A", "live TCP sessions", "checkpoint freeze",
                             "copy-interval catch-up", "partial fills/FIFO", "history cursor continuity",
                             "no lost/duplicate acknowledgments/trades", "cancel API", "failed upgrade rollback",
                             "stable market ports", "live publisher", "stock client", "login snapshot",
                             "mixed topology reopen"]}
        (root / "results.json").write_text(json.dumps(result, indent=2) + "\n")
        return result
    finally:
        if load:
            load.stop.set()
            load.trader.close()
        ex.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("binaries", type=Path)
    parser.add_argument("output", type=Path)
    parser.add_argument("--resting", type=int, default=4000)
    parser.add_argument("--rate", type=int, default=2000)
    parser.add_argument("--copy-rate", type=float, default=.25)
    parser.add_argument("--warmup", type=float, default=.1)
    parser.add_argument("--switch-gap", type=float, default=0)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    root = Path(tempfile.mkdtemp(prefix="restart-", dir=args.output.resolve()))
    print(f"ARTIFACTS={root}", flush=True)
    result = exercise(args.binaries.resolve(), root, args.resting, args.rate, args.copy_rate,
                      args.warmup, args.switch_gap)
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()

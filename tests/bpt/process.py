#!/usr/bin/env python3
"""Real TCP/API/CLI and SIGKILL tests. Creates an isolated deployment per run."""
import argparse
import json
import os
from pathlib import Path
import signal
import socket
import statistics
import struct
import subprocess
import tempfile
import time

HEADER = struct.Struct("<HIQ")
HISTORY = struct.Struct("<QQ32siiQQ32s32s")
END = struct.Struct("<QHHIQQ")


def check(condition, message):
    if not condition:
        raise AssertionError(message)


def fixed(value):
    return value.encode().ljust(32, b"\0")


class Trader:
    def __init__(self, port, user=None):
        self.sock = socket.create_connection(("127.0.0.1", port), timeout=10)
        self.request = 1
        if user:
            req = self.send(1, fixed(user) + fixed("demo"))
            kind, response, _ = self.receive()
            check(kind == 2 and response == req, "login accepted")
            while True:
                kind, response, payload = self.receive()
                check(kind == 7 and response == req, "login snapshot")
                if payload[8]:
                    break

    def close(self):
        self.sock.close()

    def send(self, kind, payload):
        req = self.request
        self.request += 1
        self.sock.sendall(HEADER.pack(kind, HEADER.size + len(payload), req) + payload)
        return req

    def exact(self, count):
        result = bytearray()
        while len(result) < count:
            chunk = self.sock.recv(count - len(result))
            if not chunk:
                raise EOFError("connection closed")
            result.extend(chunk)
        return bytes(result)

    def receive(self):
        kind, length, request = HEADER.unpack(self.exact(HEADER.size))
        check(HEADER.size <= length <= 150, "wire length")
        return kind, request, self.exact(length - HEADER.size)

    def order(self, symbol, side, quantity=1):
        req = self.send(3, struct.pack("<32sBii", fixed(symbol), side, 100, quantity))
        kind, response, payload = self.receive()
        check(kind == 5 and response == req, "order acceptance")
        return struct.unpack_from("<Q", payload, 8)[0]

    def fills(self, count):
        values = []
        for _ in range(count):
            kind, req, payload = self.receive()
            check(kind == 6 and req == 0, "live trade notification")
            values.append(struct.unpack("<QQQ32sBii", payload))
            kind, req, _ = self.receive()
            check(kind == 5 and req == 0, "live filled order update")
        return values

    def page(self, shard=0, after=0, through=0, limit=256):
        req = self.send(9, struct.pack("<HHQQ", shard, limit, after, through))
        records, ends = [], []
        counts = {}
        while True:
            kind, response, payload = self.receive()
            check(response == req, "history request correlation")
            if kind == 10:
                record = HISTORY.unpack(payload)
                records.append(record)
                sid = record[1] >> 48
                counts[sid] = counts.get(sid, 0) + 1
            elif kind == 11:
                _, sid, flags, count, cursor, bound = END.unpack(payload)
                if sid == 0:
                    check(shard == 0 and flags == 2, "all-shards terminal")
                    return records, ends
                check(count == counts.get(sid, 0), "page record count")
                ends.append((sid, flags, cursor, bound))
                if shard:
                    check(shard == sid, "continuation shard")
                    return records, ends
            else:
                raise AssertionError(f"unexpected history response {kind}, {payload.hex()}")

    def history(self):
        records, pages = self.page()
        while pages:
            sid, flags, cursor, bound = pages.pop(0)
            if flags & 1:
                more, ends = self.page(sid, cursor, bound)
                records.extend(more)
                pages.extend(ends)
        return sorted(records, key=lambda r: r[1])


class Exchange:
    def __init__(self, binaries, root, routing):
        self.bin, self.root, self.routing = binaries, root, routing
        held = [socket.socket() for _ in range(6)]
        for sock in held:
            sock.bind(("127.0.0.1", 0))
        self.gateway, router, *ports = [sock.getsockname()[1] for sock in held]
        lines = [f"routing={routing}", f"gateway_listen=127.0.0.1,{self.gateway}",
                 f"router_listen=127.0.0.1,{router}", f"engine_endpoint=127.0.0.1,{router}",
                 "request_timeout_ms=10000"]
        lines += [f"user={user},demo" for user in ("Alice", "Bob", "Carol")]
        lines += [f"shard={i+1},127.0.0.1,{port}" for i, port in enumerate(ports)]
        lines += [f"symbol=ITEM{i+1},100,{i+1}" for i in range(4)]
        self.config = root / "exchange.conf"
        self.config.write_text("\n".join(lines) + "\n")
        self.children, self.engines, self.logs, self.generation = [], [], [], 0
        for sock in held:
            sock.close()
        self.start_engines()
        if routing == "router":
            self.launch("market_gateway", ["router", "--config", str(self.config)], "router.log")
        self.launch("market_gateway", ["gateway", "--config", str(self.config)], "gateway.log")
        self.ready()

    def launch(self, binary, args, log):
        stream = (self.root / log).open("w")
        self.logs.append(stream)
        proc = subprocess.Popen([str(self.bin / binary)] + args, stdout=stream, stderr=subprocess.STDOUT)
        self.children.append(proc)
        return proc

    def start_engines(self):
        self.engines = []
        self.generation += 1
        for shard in range(1, 5):
            self.engines.append(self.launch("market_matching_engine", ["--config", str(self.config), "--shard", str(shard),
                "--data-dir", str(self.root / "data")], f"engine-{shard}-{self.generation}.log"))

    def ready(self):
        deadline = time.monotonic() + 15
        while True:
            try:
                trader = Trader(self.gateway, "Carol")
                trader.close()
                return
            except (OSError, EOFError, AssertionError):
                if time.monotonic() >= deadline:
                    raise RuntimeError(f"exchange did not become ready: {self.root}")
                time.sleep(.025)

    def restart(self, crash):
        for proc in self.engines:
            proc.send_signal(signal.SIGKILL if crash else signal.SIGTERM)
        for proc in self.engines:
            proc.wait(timeout=15)
        if crash:
            # The working index is disposable: mimic torn in-place page writes.
            for shard in range(1, 5):
                path = self.root / "data" / f"shard-{shard}" / "history/index.pages"
                with path.open("r+b") as stream:
                    stream.write(b"TORN INDEX PAGE!!")
        begin = time.perf_counter()
        self.start_engines()
        self.ready()
        return begin

    def close(self):
        for proc in reversed(self.children):
            if proc.poll() is None:
                proc.terminate()
        for proc in self.children:
            try:
                proc.wait(timeout=15)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait()
        for stream in self.logs:
            stream.close()


def add_trade(alice, bob, symbol):
    sell = bob.order(symbol, 2)
    buy = alice.order(symbol, 1)
    a, b = alice.fills(1)[0], bob.fills(1)[0]
    check(a[:2] == b[:2], "participant notifications differ")
    return (a[0], a[1], a[3], a[5], a[6], buy, sell, fixed("Alice"), fixed("Bob"))


def exercise(binaries, root, routing):
    ex = Exchange(binaries, root, routing)
    try:
        expected = []
        alice, bob, carol = [Trader(ex.gateway, user) for user in ("Alice", "Bob", "Carol")]
        for shard in range(1, 5):
            symbol = f"ITEM{shard}"
            sells = [bob.order(symbol, 2) for _ in range(270)]
            buy = alice.order(symbol, 1, 270)
            buys, sellers = alice.fills(270), bob.fills(270)
            for a, b, sell in zip(buys, sellers, sells):
                check(a[:2] == b[:2], "buyer/seller fill identity")
                expected.append((a[0], a[1], a[3], a[5], a[6], buy, sell, fixed("Alice"), fixed("Bob")))
        expected.sort(key=lambda r: r[1])
        check(alice.history() == expected and bob.history() == expected, "API differs from original notifications")
        check(carol.history() == [], "unauthorized participant history leak")

        # Capture four per-shard prefixes, then match another trade before continuing.
        frozen, pages = alice.page(limit=17)
        new_trade = add_trade(alice, bob, "ITEM1")
        while pages:
            sid, flags, cursor, bound = pages.pop(0)
            if flags & 1:
                more, ends = alice.page(sid, cursor, bound, 17)
                frozen.extend(more)
                pages.extend(ends)
        check(sorted(frozen, key=lambda r: r[1]) == expected, "pagination changed its committed prefix")
        expected.append(new_trade)
        expected.sort(key=lambda r: r[1])
        check(alice.history() == expected, "fresh query missed newly committed trade")
        for shard, limit in [(0, 0), (0, 257), (65000, 256)]:
            req = alice.send(9, struct.pack("<HHQQ", shard, limit, 0, 0))
            kind, rid, data = alice.receive()
            check(kind == 8 and rid == req and struct.unpack_from("<H", data, 8)[0] == 4, "invalid history request not rejected")
        anonymous = Trader(ex.gateway)
        anonymous.send(9, struct.pack("<HHQQ", 0, 256, 0, 0))
        try:
            anonymous.receive()
            raise AssertionError("unauthenticated history accepted")
        except EOFError:
            pass
        anonymous.close()

        times = []
        for _ in range(100):
            begin = time.perf_counter_ns()
            records, _ = alice.page(1)
            check(len(records) == 256, "latency benchmark page count")
            times.append((time.perf_counter_ns() - begin) / 1000)
        begin = time.perf_counter_ns()
        check(alice.history() == expected, "full-history benchmark correctness")
        full_us = (time.perf_counter_ns() - begin) / 1000
        for trader in (alice, bob, carol):
            trader.close()

        start = ex.restart(False)
        alice = Trader(ex.gateway, "Alice")
        check(alice.history() == expected, "graceful checkpoint reopen")
        warm_ms = (time.perf_counter() - start) * 1000
        bob = Trader(ex.gateway, "Bob")
        expected.append(add_trade(alice, bob, "ITEM1"))
        expected.sort(key=lambda r: r[1])
        alice.close()
        bob.close()
        start = ex.restart(True)
        alice = Trader(ex.gateway, "Alice")
        check(alice.history() == expected, "SIGKILL recovery/torn index/suffix replay lost original trades")
        crash_ms = (time.perf_counter() - start) * 1000
        alice.close()

        # stdin pipe also exercises multiple buffered commands and automatic paging.
        cli = subprocess.run([str(binaries / "gateway_client"), "--host", "127.0.0.1", "--port", str(ex.gateway),
                              "--user", "Alice", "--password", "demo"], input="help\ntrades\nquit\n", text=True,
                             stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=30)
        (root / "client.log").write_text(cli.stdout)
        check(cli.returncode == 0, "client process failed")
        check(sum(line.startswith("HISTORY_TRADE ") for line in cli.stdout.splitlines()) == len(expected), "client did not print every trade")
        check(f"HISTORY_COMPLETE total={len(expected)}" in cli.stdout, "client did not finish automatic pagination")
        check("cancel-last" in cli.stdout and "relogin" in cli.stdout and "one command per line" in cli.stdout, "client help incomplete")
        ordered = sorted(times)
        result = {"routing": routing, "trades": len(expected), "query_page_size": 256, "latency_samples": len(times),
                  "page_p50_us": statistics.median(times), "page_p95_us": ordered[94], "page_p99_us": ordered[98],
                  "full_history_us": full_us, "warm_restart_through_query_ms": warm_ms,
                  "kill9_restart_through_query_ms": crash_ms}
        (root / "metrics.json").write_text(json.dumps(result, indent=2) + "\n")
        print("PASS TCP/history/authentication/pagination/SIGKILL/CLI", json.dumps(result), flush=True)
        return result
    finally:
        ex.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("bin", type=Path)
    parser.add_argument("output", type=Path)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    root = Path(tempfile.mkdtemp(prefix="history-process-", dir=args.output.resolve()))
    results = []
    for routing in ("direct", "router"):
        folder = root / routing
        folder.mkdir()
        results.append(exercise(args.bin.resolve(), folder, routing))
    (root / "results.json").write_text(json.dumps(results, indent=2) + "\n")
    print("RESULTS", root, flush=True)


if __name__ == "__main__":
    main()

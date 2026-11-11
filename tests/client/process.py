#!/usr/bin/env python3
"""Reconnect deadlines and real engine outages, using isolated ports/data."""
import argparse
import json
import os
from pathlib import Path
import pty
import re
import socket
import struct
import subprocess
import tempfile
import threading
import time

ROOT = Path(__file__).resolve().parents[2]
HEADER = struct.Struct("<HIQ")
STATE = struct.Struct("<Q32sBiiiiB")


def check(value, message):
    if not value:
        raise AssertionError(message)


def eventually(predicate, message, seconds=8):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(.03)
    raise AssertionError(message)


def text(path):
    return path.read_text(errors="replace") if path.exists() else ""


def plain(path):
    return re.sub(r"\x1b\[[0-9;]*m", "", text(path))


def acceptances(path):
    return re.findall(r"^\[\d\d:\d\d:\d\d\.\d{3}\] ACCEPTED  ", plain(path), re.MULTILINE)


def frame(kind, request, payload):
    return HEADER.pack(kind, HEADER.size + len(payload), request) + payload


class Processes:
    def __init__(self, root):
        self.root, self.children, self.files = root, [], []

    def start(self, arguments, name, stdin=subprocess.PIPE):
        log = (self.root / name).open("w")
        self.files.append(log)
        child = subprocess.Popen(list(map(str, arguments)), stdin=stdin, stdout=log, stderr=subprocess.STDOUT)
        self.children.append(child)
        return child

    def close(self):
        for child in reversed(self.children):
            if child.poll() is None:
                child.terminate()
            try:
                child.wait(timeout=8)
            except subprocess.TimeoutExpired:
                child.kill()
                child.wait()
            if child.stdin:
                child.stdin.close()
        for stream in self.files:
            stream.close()


class FakeGateway:
    """Can accept TCP and login while withholding the snapshot end forever."""
    def __init__(self):
        self.listener = socket.socket()
        self.listener.bind(("127.0.0.1", 0))
        self.listener.listen()
        self.listener.settimeout(.1)
        self.port = self.listener.getsockname()[1]
        self.stop = threading.Event()
        self.mode = "available"
        self.connection = None
        self.drop_order = False
        self.orders = self.logins = 0
        self.error = None
        self.thread = threading.Thread(target=self.run)
        self.thread.start()

    def exact(self, connection, count):
        data = bytearray()
        while len(data) < count and not self.stop.is_set():
            try:
                chunk = connection.recv(count - len(data))
            except socket.timeout:
                continue
            if not chunk:
                raise EOFError()
            data.extend(chunk)
        if len(data) != count:
            raise EOFError()
        return bytes(data)

    def disconnect(self, mode):
        self.mode = mode
        if self.connection:
            try:
                self.connection.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass

    def run(self):
        try:
            while not self.stop.is_set():
                try:
                    connection, _ = self.listener.accept()
                except socket.timeout:
                    continue
                self.connection = connection
                connection.settimeout(.1)
                with connection:
                    try:
                        while not self.stop.is_set():
                            kind, length, request = HEADER.unpack(self.exact(connection, HEADER.size))
                            payload = self.exact(connection, length - HEADER.size)
                            stamp = struct.pack("<Q", time.time_ns())
                            if kind == 1:
                                self.logins += 1
                                if self.mode == "closed":
                                    break
                                if self.mode == "invalid":
                                    connection.sendall(frame(8, request, stamp + struct.pack("<H", 1)))
                                    continue
                                connection.sendall(frame(2, request, stamp))
                                if self.mode != "half_login":
                                    connection.sendall(frame(7, request, stamp + b"\x01" + bytes(STATE.size)))
                            elif kind == 3:
                                self.orders += 1
                                if self.drop_order:
                                    self.drop_order = False
                                    self.mode = "closed"
                                    break
                                symbol, side, price, quantity = struct.unpack("<32sBii", payload)
                                order = STATE.pack(self.orders, symbol, side, price, quantity, 0, quantity, 1)
                                connection.sendall(frame(5, request, stamp + order))
                            else:
                                raise AssertionError(f"Unexpected or replayed request: {kind}")
                    except (OSError, EOFError):
                        pass
                self.connection = None
        except Exception as error:
            self.error = error

    def close(self):
        self.stop.set()
        self.disconnect("closed")
        self.thread.join(timeout=3)
        self.listener.close()
        check(not self.thread.is_alive(), "fake server thread leaked")
        if self.error:
            raise self.error


def deadline_and_replay(binary, root):
    gateway = FakeGateway()
    processes = Processes(root)
    log = root / "deadline.log"
    args = [binary / "gateway_client", "--port", gateway.port, "--user", "Alice", "--password", "demo"]
    try:
        client = processes.start(args, log.name)
        eventually(lambda: "READY (" in text(log), "initial login failed")
        gateway.drop_order = True
        client.stdin.write(b"buy ITEM1 100 1\n")
        client.stdin.flush()
        eventually(lambda: "DISCONNECTED:" in text(log), "disconnect not reported")
        time.sleep(2)
        check(client.poll() is None, "client exited during the tolerance window")
        check(gateway.logins >= 5, "client did not retry frequently")
        check(text(log).count("DISCONNECTED:") == 1, "retries flooded disconnect logs")
        check("outcome unknown, not replayed" in text(log), "unknown order outcome hidden")
        gateway.mode = "available"
        eventually(lambda: "RECONNECTED:" in text(log), "client did not recover")
        check(gateway.orders == 1, "unacknowledged order was replayed")
        client.stdin.write(b"buy ITEM1 100 2\n")
        client.stdin.flush()
        eventually(lambda: len(acceptances(log)) == 1, "recovered client cannot trade")
        check(gateway.orders == 2, "unexpected order replay")
        check("\x1b[1;31mDISCONNECTED:" in text(log), "client disconnect warning is not red")
        check("\x1b[1;32mACCEPTED\x1b[0m" in text(log), "order acceptance keyword is not green")

        # Repeated TCP connects and LoginAccepted replies must NOT renew the
        # original 20-second outage deadline without a full snapshot.
        before = gateway.logins
        started = time.monotonic()
        gateway.disconnect("half_login")
        eventually(lambda: text(log).count("DISCONNECTED:") == 2, "second outage missing")
        time.sleep(1)
        client.stdin.write(b"buy ITEM1 100 3\n")
        client.stdin.flush()
        client.wait(timeout=23)
        elapsed = time.monotonic() - started
        timeout_attempts = gateway.logins - before
        check(19.5 <= elapsed <= 22.5, f"wrong outage deadline: {elapsed:.3f}s")
        check(client.returncode != 0 and "RECONNECT_TIMEOUT" in text(log), "timeout was not a failure")
        check(timeout_attempts >= 4, "stalled login was not retried")
        check(gateway.orders == 2, "offline command was sent or replayed")
        check(text(log).count("DISCONNECTED:") == 2 and text(log).count("RECONNECTED:") == 1,
              "unstable login reset the outage or spammed logs")

        gateway.mode = "invalid"
        rejected = processes.start(args, "invalid-login.log")
        check(rejected.wait(timeout=3) != 0, "invalid credentials should fail immediately")
        check("Login rejected" in text(root / "invalid-login.log"), "wrong login rejection")

        gateway.mode = "closed"
        quitter = processes.start(args, "offline-quit.log")
        eventually(lambda: "DISCONNECTED:" in text(root / "offline-quit.log"), "initial outage missing")
        quitter.stdin.write(b"quit\n")
        quitter.stdin.flush()
        check(quitter.wait(timeout=2) == 0, "quit must work during recovery")
        return {"timeout_seconds": elapsed, "unknown_order_replays": 0,
                "login_attempts_during_timeout": timeout_attempts,
                "checks": ["rapid retry", "single outage log", "snapshot-gated recovery", "20s deadline",
                           "no order replay", "offline command blocked", "invalid credentials", "offline quit"]}
    finally:
        processes.close()
        gateway.close()


def engine_outages(binary, root):
    processes = Processes(root)
    held = [socket.socket() for _ in range(4)]
    for item in held:
        item.bind(("127.0.0.1", 0))
    gateway, order, market, query = [item.getsockname()[1] for item in held]
    udp = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    udp.bind(("127.0.0.1", 0))
    udp_port = udp.getsockname()[1]
    config = root / "exchange.conf"
    config.write_text(f"gateway_listen=127.0.0.1,{gateway}\nuser=Alice,demo\nshard=1,127.0.0.1,{order}\n" +
                      "".join(f"symbol=ITEM{i},100,1\n" for i in range(1, 33)))
    publisher_config = root / "publisher.conf"
    publisher_config.write_text("symbols=" + ",".join(f"{i}:ITEM{i}" for i in range(1, 33)) +
        f"\nshard=s1,tcp,127.0.0.1,{market},{root}/unused," + ",".join(map(str, range(1, 33))) +
        f"\nudp_receiver=127.0.0.1,{udp_port}\nquery_host=127.0.0.1\nquery_port={query}\n")
    for item in held:
        item.close()
    udp.close()
    engine_args = [binary / "market_matching_engine", "--config", config, "--shard", "1",
                   "--market-port", market, "--data-dir", root / "data", "--checkpoint-ms", "50"]
    master, slave = pty.openpty()
    try:
        engine = processes.start(engine_args, "engine-0.log")
        processes.start([binary / "market_gateway", "gateway", "--config", config], "gateway.log")
        processes.start([binary / "market_broadcast", "observe", "--config", publisher_config], "market.log")
        processes.start([binary / "market_broadcast", "publish", "--config", publisher_config], "publisher.log")
        client = processes.start([ROOT / "src/scripts/run_client.sh", "--build-dir", binary,
                                  "--port", gateway, "--user", "Alice"], "client.log", stdin=slave)
        os.close(slave)
        slave = -1
        eventually(lambda: "Password:" in text(root / "client.log"), "launcher login prompt missing")
        os.write(master, b"demo\n")
        eventually(lambda: "READY (" in text(root / "client.log"), "launcher login failed")
        eventually(lambda: "BOOK symbol=32 " in text(root / "market.log"), "market baseline missing")
        time.sleep(1)
        measurements = []
        for cycle in range(1, 3):
            base = {name: text(root / name) for name in ("publisher.log", "market.log", "client.log", "gateway.log")}
            started = time.monotonic()
            engine.kill()
            engine.wait(timeout=3)
            eventually(lambda: "PUBLISHER_UNAVAILABLE" in text(root / "publisher.log")[len(base["publisher.log"]):], "publisher warning missing")
            eventually(lambda: "MARKET_DATA_UNAVAILABLE" in text(root / "market.log")[len(base["market.log"]):], "market warning missing")
            eventually(lambda: "GATEWAY_UNAVAILABLE" in text(root / "gateway.log")[len(base["gateway.log"]):], "gateway warning missing")
            time.sleep(3)
            check(client.poll() is None, "launcher exited during engine outage")
            for name, marker in (("publisher.log", "PUBLISHER_UNAVAILABLE"), ("market.log", "MARKET_DATA_UNAVAILABLE"), ("gateway.log", "GATEWAY_UNAVAILABLE")):
                outage = text(root / name)[len(base[name]):]
                check(outage.count(marker) == 1, f"{name} repeated outage warnings")
                check("\x1b[1;31m!!! " + marker in outage, f"{name} warning is not bold red")
                after_warning = outage.split(marker, 1)[1]
                check(after_warning.count("\n") == 1, f"{name} kept scrolling during outage: {after_warning}")
            engine = processes.start(engine_args, f"engine-{cycle}.log")
            eventually(lambda: "RECONNECTED:" in text(root / "client.log")[len(base["client.log"]):], "client did not reconnect", 10)
            eventually(lambda: "PUBLISHER_RESTORED" in text(root / "publisher.log")[len(base["publisher.log"]):], "publisher did not recover", 10)
            eventually(lambda: "MARKET_DATA_RESTORED" in text(root / "market.log")[len(base["market.log"]):], "market observer did not recover", 10)
            eventually(lambda: "GATEWAY_RESTORED" in text(root / "gateway.log")[len(base["gateway.log"]):], "gateway did not report recovery", 10)
            check(text(root / "client.log")[len(base["client.log"]):].count("DISCONNECTED:") == 1, "client retry log spam")
            before = len(acceptances(root / "client.log"))
            os.write(master, b"buy ITEM1 90 1\n")
            eventually(lambda: len(acceptances(root / "client.log")) > before, "post-recovery trade failed")
            measurements.append(time.monotonic() - started)
        os.write(master, b"quit\n")
        check(client.wait(timeout=3) == 0, "launcher quit failed")
        return {"engine_outage_cycles": 2, "recovery_seconds": measurements,
                "checks": ["run_client.sh stays alive", "automatic login after real engine restart",
                           "red warning once per outage", "quiet retries across 32 symbols", "recovery notices", "trading resumes"]}
    finally:
        processes.close()
        os.close(master)
        if slave >= 0:
            os.close(slave)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("binaries", type=Path)
    parser.add_argument("output", type=Path)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    root = Path(tempfile.mkdtemp(prefix="reconnect-", dir=args.output.resolve()))
    print(f"ARTIFACTS={root}", flush=True)
    results = {"protocol": deadline_and_replay(args.binaries.resolve(), root),
               "engine": engine_outages(args.binaries.resolve(), root)}
    (root / "results.json").write_text(json.dumps(results, indent=2) + "\n")
    print(json.dumps(results, indent=2), flush=True)


if __name__ == "__main__":
    main()

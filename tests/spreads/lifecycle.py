#!/usr/bin/env python3
"""Real group replacement, rollback and clean recovery; all state is isolated."""
import argparse
import importlib.util
import json
import os
import re
from pathlib import Path
import socket
import signal
import struct
import subprocess
import sys
import tempfile
import time

ROOT = Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location("restart_process", ROOT / "tests/restart/process.py")
base = importlib.util.module_from_spec(spec)
spec.loader.exec_module(base)
check, fixed, command, exact = base.check, base.fixed, base.command, base.exact
sys.path.insert(0, str(ROOT / "src/matching_engine/cluster"))
from rpc import Rpc


class Trader(base.Trader):
    def __init__(self, port, user=None):
        self.port = port
        super().__init__(port, user)

    def receive(self):
        kind, length, request = base.history.HEADER.unpack(self.exact(14))
        check(14 <= length <= 211, "invalid typed client frame")
        return kind, request, self.exact(length - 14)

    def order(self, symbol, side, price, quantity=1):
        request = self.send(3, struct.pack("<32sBii", fixed(symbol), side, price, quantity))
        kind, response, payload = self.receive()
        check(kind == 5 and response == request, f"order rejected: {kind} {payload!r}")
        return struct.unpack_from("<Q", payload, 8)[0]

    def snapshot(self):
        self.close()
        base.Trader.__init__(self, self.port)
        request = self.send(1, fixed("Alice") + fixed("demo"))
        kind, response, _ = self.receive()
        check(kind == 2 and response == request, "snapshot login acceptance")
        orders = []
        while True:
            kind, response, payload = self.receive()
            check(kind == 7 and response == request, "snapshot correlation")
            if payload[8]:
                return sorted(orders)
            orders.append(payload[9:])

    def history(self):
        request = self.send(9, struct.pack("<HHQQ", 0, 256, 0, 0))
        records, ends = [], 0
        while True:
            kind, response, payload = self.receive()
            check(response == request, "history correlation")
            if kind == 11:
                _, shard, flags, _, _, _ = base.history.END.unpack(payload)
                if shard == 0:
                    check(flags == 2 and ends == 4, "all-shard history termination")
                    return sorted(records)
                check(not flags & 1, "unexpected pagination")
                ends += 1
            else:
                check(kind in (10, 12, 13), "invalid history type")
                # Every execution byte, including original timestamps, stays exact.
                records.append((kind, payload))


def read_market(port):
    with socket.create_connection(("127.0.0.1", port), timeout=3) as connection:
        connection.sendall(b"\x01")
        data = exact(connection, struct.unpack("<I", exact(connection, 4))[0])
    check(struct.unpack_from("<HB", data, 16) == (65535, 2), "expected V2 market")
    count, = struct.unpack_from("<H", data, 19)
    offset, books = 21, {}
    for _ in range(count):
        symbol, levels = struct.unpack_from("<HB", data, offset)
        offset += 3
        books[symbol] = [struct.unpack_from("<BiQQQ", data, offset + i * 29) for i in range(levels)]
        offset += levels * 29
    check(offset == len(data), "market trailing bytes")
    return books


def market(port):
    deadline = time.monotonic() + 5
    while True:
        try:
            return read_market(port)
        except (OSError, EOFError):
            if time.monotonic() >= deadline:
                raise
            time.sleep(.01)


def published(port, symbol):
    with socket.create_connection(("127.0.0.1", port), timeout=3) as connection:
        connection.sendall(struct.pack("<H", symbol))
        data = exact(connection, struct.unpack("<H", exact(connection, 2))[0])
    check(data[0] == 4 and struct.unpack_from("<H", data, 17)[0] == symbol, "Publisher V2 snapshot")
    check((len(data) - 35) % 29 == 0, "Publisher V2 level width")
    return [struct.unpack_from("<BiQQQ", data, i) for i in range(35, len(data), 29)]


class Deployment:
    def __init__(self, binary, root):
        self.root, self.bin = root, binary
        held = [socket.socket() for _ in range(43)]
        for sock in held:
            sock.bind(("127.0.0.1", 0))
        ports = iter(sock.getsockname()[1] for sock in held)
        self.gateway, router, admin, self.control, coordinator, query, self.bad_peer = [next(ports) for _ in range(7)]
        self.query = query
        self.topology = {"router_admin_port": admin, "supervisor_port": self.control,
                         "coordinator_port": coordinator, "shards": {}}
        lines = ["routing=router", "spreads=enabled", "request_timeout_ms=30000",
                 f"gateway_listen=127.0.0.1,{self.gateway}", f"router_listen=127.0.0.1,{router}",
                 f"engine_endpoint=127.0.0.1,{router}", f"coordinator=127.0.0.1,{coordinator}",
                 "user=Alice,demo", "user=Bob,demo", "user=Carol,demo"]
        products = [("A", 120, 6), ("B", 110, 7), ("C", 105, 8),
                    ("SP_AB", 10, 5), ("SP_AC", 15, 5), ("BF_ABC", 5, 5)]
        sources = []
        for shard in range(5, 9):
            config = {"market": next(ports)}
            for slot in ("A", "B"):
                config[slot] = dict(zip(("order", "control", "market", "peer"), [next(ports) for _ in range(4)]))
            self.topology["shards"][str(shard)] = config
            lines += [f"shard={shard},127.0.0.1,{config['A']['order']}",
                      f"spread_peer={shard},127.0.0.1,{config['A']['peer']}"]
            symbols = ",".join(str(i + 1) for i, p in enumerate(products) if p[2] == shard)
            sources.append(f"shard=s{shard},tcp,127.0.0.1,{config['market']},{root}/unused-{shard}.mmap,{symbols}")
        lines += [f"symbol={name},{previous},{shard}" for name, previous, shard in products]
        self.config = root / "exchange.conf"
        self.config.write_text("\n".join(lines) + "\n")
        publisher = root / "publisher.conf"
        self.publisher_config = publisher
        self.udp = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.udp.bind(("127.0.0.1", 0))
        names = ",".join(f"{i+1}:{p[0]}" for i, p in enumerate(products))
        publisher.write_text(f"symbols={names}\n" + "\n".join(sources) +
                             f"\nudp_receiver=127.0.0.1,{self.udp.getsockname()[1]}\n"
                             f"query_host=127.0.0.1\nquery_port={query}\n")
        topology = root / "cluster.json"
        topology.write_text(json.dumps(self.topology))
        for sock in held:
            sock.close()
        self.children, self.streams, self.generation = [], [], 0
        self.cluster_args = [sys.executable, "-B", ROOT / "src/matching_engine/cluster/run.py",
                             "--config", self.config, "--market-config", publisher, "--cluster-config", topology,
                             "--build-dir", binary, "--data-dir", root / "data", "--log-dir", root / "logs",
                             "--checkpoint-ms", "50", "--trace"]
        self.start()

    def launch(self, args, name):
        stream = (self.root / name).open("w")
        self.streams.append(stream)
        child = subprocess.Popen(list(map(str, args)), stdin=subprocess.DEVNULL,
                                 stdout=stream, stderr=subprocess.STDOUT)
        self.children.append(child)
        return child

    def start(self):
        self.generation += 1
        self.cluster = self.launch(self.cluster_args, f"cluster-{self.generation}.log")
        self.front = self.launch([self.bin / "market_gateway", "gateway", "--config", self.config],
                                 f"gateway-{self.generation}.log")
        self.publisher = self.launch([self.bin / "market_broadcast", "publish", "--config", self.publisher_config],
                                     f"publisher-{self.generation}.log")
        deadline = time.monotonic() + 20
        while time.monotonic() < deadline:
            check(self.cluster.poll() is None, f"cluster exited; see {self.root}")
            try:
                if command(self.control, "status")["ok"]:
                    probe = Trader(self.gateway, "Carol")
                    probe.close()
                    market(self.topology["shards"]["5"]["market"])
                    return
            except (OSError, EOFError, AssertionError):
                pass
            time.sleep(.05)
        raise TimeoutError(f"group startup timed out; see {self.root}")

    def stop(self):
        for process in (self.publisher, self.front, self.cluster):
            if process.poll() is None:
                process.terminate()
                process.wait(timeout=40)
        check(self.cluster.returncode == 0, "clean supervisor exit")
        check("GROUP_SHUTDOWN_DRAINED" in (self.root / f"cluster-{self.generation}.log").read_text(),
              "group was not drained before stopping")

    def close(self):
        for child in reversed(self.children):
            if child.poll() is None:
                child.terminate()
                try:
                    child.wait(timeout=40)
                except subprocess.TimeoutExpired:
                    child.kill()
                    child.wait()
        for stream in self.streams:
            stream.close()
        self.udp.close()

    def status(self):
        reply = command(self.control, "status")
        check(reply["ok"], "supervisor status")
        return reply["result"]

    def views(self):
        return {sid: market(config["market"]) for sid, config in self.topology["shards"].items()}

    def published_view(self, expected):
        deadline = time.monotonic() + 5
        while True:
            try:
                if published(self.query, 4) == expected:
                    return
            except (OSError, EOFError):
                pass
            check(time.monotonic() < deadline, "Publisher did not propagate the merged V2 spread book")
            time.sleep(.05)

    def broken_replacement(self):
        """Real standby recovers, but advertises an unreachable control/peer port."""
        wrapper = self.root / "unreachable-peer.py"
        wrapper.write_text(f"#!{sys.executable}\nimport os, sys\nfrom pathlib import Path\n"
                           "args = sys.argv[1:]\ni = args.index('--config') + 1\np = Path(args[i])\n"
                           "lines = p.read_text().splitlines()\n"
                           f"lines = ['spread_peer=6,127.0.0.1,{self.bad_peer}' if line.startswith('spread_peer=6,') else line for line in lines]\n"
                           "q = p.with_suffix('.unreachable.conf')\nq.write_text('\\n'.join(lines) + '\\n')\nargs[i] = str(q)\n"
                           f"os.execv({str(self.bin / 'market_matching_engine')!r}, [{str(self.bin / 'market_matching_engine')!r}] + args)\n")
        wrapper.chmod(0o700)
        return wrapper


def run(binary, output):
    root = Path(tempfile.mkdtemp(prefix="run-", dir=output))
    started = time.monotonic()
    deployment = None
    client = None
    results = {"artifacts": str(root), "restarts": []}
    try:
        deployment = Deployment(binary, root)
        client = Trader(deployment.gateway, "Alice")
        client.order("A", 2, 120, 20)
        client.order("B", 1, 110, 20)
        client.order("SP_AB", 1, 10, 3)
        client.fills(3)
        client.order("SP_AB", 1, -1, 2)
        client.order("SP_AC", 1, 1, 2)
        client.order("BF_ABC", 1, 5, 2)
        check(len(client.history()) == 3, "seed typed history")
        before = deployment.status()
        snapshot, history, views = client.snapshot(), client.history(), deployment.views()
        deployment.published_view(views["5"][4])
        recovery_at = time.monotonic()
        lost = before["processes"]["shard-6-A"]["pid"]
        os.kill(lost, signal.SIGKILL)
        deadline = time.monotonic() + 10
        while True:
            after = deployment.status()
            if (after["shards"]["6"]["phase"] == "active" and
                    after["processes"]["shard-6-A"]["pid"] != lost):
                break
            check(time.monotonic() < deadline, "supervisor did not rejoin the idle crashed member")
            time.sleep(.02)
        for key in ("router", "coordinator"):
            check(before["processes"][key]["pid"] == after["processes"][key]["pid"],
                  "supervisor discarded surviving control state")
        check(client.snapshot() == snapshot and client.history() == history and deployment.views() == views,
              "supervised member recovery changed public state")
        results["supervised_member_recovery_ms"] = (time.monotonic() - recovery_at) * 1000
        print("PASS supervisor restarted only the lost member and retained control state", flush=True)
        for shard in (5, 6, 7, 8, 5):
            before = deployment.status()
            snapshot, history, views = client.snapshot(), client.history(), deployment.views()
            reply = command(deployment.control, f"restart {shard}")
            check(reply["ok"], f"restart {shard} failed: {reply}")
            results["restarts"].append(reply["result"])
            after = deployment.status()
            for key in ("router", "coordinator"):
                check(before["processes"][key]["pid"] == after["processes"][key]["pid"], f"{key} restarted")
            for sid, state in before["shards"].items():
                if sid != str(shard):
                    key = f"shard-{sid}-{state['active']}"
                    check(before["processes"][key]["pid"] == after["processes"][key]["pid"], "healthy member restarted")
            check(client.snapshot() == snapshot, "resting orders changed during replacement")
            check(client.history() == history, "typed execution bytes changed during replacement")
            check(deployment.views() == views, "published direct/implied view changed during replacement")
            client.order("SP_AB", 1, 10)
            client.fills(3)
            print(f"PASS replacement shard={shard} total_ms={reply['result']['total_ms']:.3f}", flush=True)
        snapshot, history, views = client.snapshot(), client.history(), deployment.views()
        before = deployment.status()
        aborted_at = time.monotonic()
        reply = command(deployment.control, f"restart 6 {deployment.broken_replacement()}")
        check(not reply["ok"], "unreachable candidate unexpectedly promoted")
        after = deployment.status()
        check(after["transition"] is None and after["shards"]["6"]["active"] == before["shards"]["6"]["active"]
              and after["shards"]["6"]["phase"] == "active", "preactivation failure did not restore source")
        check(client.snapshot() == snapshot and client.history() == history and deployment.views() == views,
              "rollback changed public state")
        client.order("SP_AB", 1, 10)
        client.fills(3)
        results["preactivation_abort_ms"] = (time.monotonic() - aborted_at) * 1000
        print("PASS unreachable replacement aborted; original member resumed", flush=True)
        snapshot, history, views = client.snapshot(), client.history(), deployment.views()
        client.close()
        client = None
        deployment.stop()
        recovered_at = time.monotonic()
        deployment.start()
        client = Trader(deployment.gateway, "Alice")
        check(client.snapshot() == snapshot and client.history() == history and deployment.views() == views,
              "clean group restart lost durable state or initial implied views")
        deployment.published_view(views["5"][4])
        client.order("SP_AB", 1, 10)
        client.fills(3)
        results["clean_recovery_ms"] = (time.monotonic() - recovered_at) * 1000
        client.order("SP_AB", 2, -1)
        client.fills(2)
        expected_history = client.history()
        client.close()
        client = None
        cli = subprocess.run([str(binary / "gateway_client"), "--host", "127.0.0.1",
                              "--port", str(deployment.gateway), "--user", "Alice", "--password", "demo"],
                             input="trades\nquit\n", text=True, stdout=subprocess.PIPE,
                             stderr=subprocess.STDOUT, timeout=10)
        (root / "client-typed-history.log").write_text(cli.stdout)
        plain = re.sub(r"\x1b\[[0-9;]*m", "", cli.stdout)
        rows = re.findall(r"^(TRADE|LEG|SPREAD)\s+\d+\s", plain, re.MULTILINE)
        check(cli.returncode == 0 and len(rows) == len(expected_history)
              and "LEG" in rows and "SPREAD" in rows and "TRADE" in rows and "execution=" in plain,
              "CLI did not render every mixed typed execution")
        results["publisher_v2_and_typed_cli"] = True
        deployment.stop()
        # Emulate the durable launcher marker left by an interrupted group.
        # No process is running and no trade files are altered by this check.
        state_path = root / "data/cluster-state.json"
        clean_state = state_path.read_text()
        state = json.loads(clean_state)
        check(state["group_clean_shutdown"], "clean stop did not persist lifecycle boundary")
        state["group_clean_shutdown"] = False
        state_path.write_text(json.dumps(state))
        refused = subprocess.run(list(map(str, deployment.cluster_args)), stdin=subprocess.DEVNULL,
                                 capture_output=True, text=True, timeout=5)
        check(refused.returncode != 0 and "shutdown was not confirmed" in refused.stdout,
              "fresh Router/Coordinator accepted an unconfirmed shutdown")
        state_path.write_text(clean_state)
        results["unclean_start_refused"] = True
        results.update(passed=True, elapsed_ms=(time.monotonic() - started) * 1000)
        (output / "results.json").write_text(json.dumps(results, indent=2) + "\n")
        print(json.dumps(results, indent=2))
    finally:
        if client:
            client.close()
        if deployment:
            deployment.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("binaries", type=Path)
    parser.add_argument("output", type=Path)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    run(args.binaries.resolve(), args.output.resolve())

#!/usr/bin/env python3
"""Own the router and shard processes; execute one reversible planned restart at a time.

All cross-process state transfer and management uses TCP. Python only orchestrates
maintenance: the C++ router and engine own the normal trading path.
"""
import argparse
import fcntl
import json
import os
from pathlib import Path
import shlex
import signal
import socket
import socketserver
import subprocess
import sys
import threading
import time
import uuid

from rpc import Rpc, boundary, worker_status

ROOT = Path(__file__).resolve().parents[3]
PRINT_LOCK = threading.Lock()


def log(event, **fields):
    with PRINT_LOCK:
        print(time.strftime("%Y-%m-%dT%H:%M:%S"), event,
              " ".join(f"{key}={value}" for key, value in fields.items()), flush=True)


def entries(path):
    for raw in Path(path).read_text().splitlines():
        line = raw.split("#", 1)[0].strip()
        if line:
            key, value = line.split("=", 1)
            yield key.strip(), [item.strip() for item in value.split(",")]


def atomic_json(path, value):
    temp = path.with_suffix(".tmp")
    with temp.open("w") as out:
        json.dump(value, out, indent=2)
        out.write("\n")
        out.flush()
        os.fsync(out.fileno())
    os.replace(temp, path)
    descriptor = os.open(path.parent, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


class Cluster:
    def __init__(self, args):
        self.args = args
        self.topology = json.loads(args.cluster_config.read_text())
        self.shards = self.topology["shards"]
        self.validate()
        self.root = args.data_dir or args.config.parent / ".marketlab-data" / args.config.stem
        self.root = self.root.resolve()
        self.root.mkdir(parents=True, exist_ok=True)
        self.lock_file = (self.root / "cluster.lock").open("a")
        fcntl.flock(self.lock_file, fcntl.LOCK_EX | fcntl.LOCK_NB)
        self.state_path = self.root / "cluster-state.json"
        binary = str((args.build_dir / "market_matching_engine").resolve())
        if self.state_path.exists():
            self.state = json.loads(self.state_path.read_text())
            if self.state.get("transition"):
                raise RuntimeError("unfinished promotion in cluster-state.json; inspect the transition before restart")
            if self.group_enabled and not self.state.get("group_clean_shutdown", True):
                raise RuntimeError("previous spread group shutdown was not confirmed; inspect all participant "
                                   "boundaries before restarting Router/Coordinator")
            if not set(self.state["shards"]).issubset(self.shards):
                raise ValueError("cluster config cannot remove persisted shards")
            added = [shard for shard in self.shards if shard not in self.state["shards"]]
            if added:
                # Keep existing active slots, replica roots and binaries intact.
                for shard in added:
                    self.state["shards"][shard] = {
                        "active": "A", "root": str(self.root), "binary": binary}
                self.save()
                log("CLUSTER_EXPANDED", added=added, state=self.state_path)
        else:
            self.state = {"version": 1, "transition": None, "shards": {
                shard: {"active": "A", "root": str(self.root), "binary": binary}
                for shard in self.shards}}
            self.save()
        self.logs = args.log_dir / (time.strftime("run-%Y%m%d-%H%M%S-") + uuid.uuid4().hex[:6])
        self.logs.mkdir(parents=True, exist_ok=False)
        self.processes = {}
        self.process_lock = threading.RLock()
        self.operation = threading.Lock()
        self.stop = threading.Event()
        self.phases = {shard: "active" for shard in self.shards}
        self.metrics = []

    def validate(self):
        exchange = list(entries(self.args.config))
        order_shards = {value[0]: value for key, value in exchange if key == "shard"}
        if set(order_shards) != set(self.shards):
            raise ValueError("cluster config must prepare both instances for every configured shard")
        settings = {key: value for key, value in exchange if key != "shard"}
        self.group_enabled = settings.get("spreads") == ["enabled"]
        self.group_shards = set()
        if settings.get("routing") != ["router"]:
            raise ValueError("planned restart requires routing=router in the exchange config")
        if settings["router_listen"] != settings["engine_endpoint"]:
            raise ValueError("router_listen and engine_endpoint must match")
        ports = [int(settings["gateway_listen"][1]), int(settings["router_listen"][1]),
                 self.topology["router_admin_port"], self.topology["supervisor_port"]]
        products = [value for key, value in exchange if key == "symbol"]
        if self.group_enabled:
            names = settings.get("spread_symbols", ["A", "B", "C", "SP_AB", "SP_AC"])
            owners = {product[0]: product[2] for product in products}
            self.group_shards = {owners[name] for name in names}
            if len(self.group_shards) != 4:
                raise ValueError("spread group requires four distinct shard owners")
            configured = settings.get("coordinator", ["127.0.0.1", "7120"])
            if configured != ["127.0.0.1", str(self.topology.get("coordinator_port"))]:
                raise ValueError("exchange/cluster Coordinator endpoint differs")
            ports.append(self.topology["coordinator_port"])
        market = list(entries(self.args.market_config))
        market_names = {int(item.split(":", 1)[0]): item.split(":", 1)[1]
                        for key, values in market if key == "symbols" for item in values}
        expected_names = {i + 1: product[0] for i, product in enumerate(products)}
        if market_names != expected_names:
            raise ValueError("exchange/publisher symbol IDs differ")
        market_shards = {}
        for key, values in market:
            if key != "shard":
                continue
            owners = {products[int(symbol) - 1][2] for symbol in values[5:]}
            if len(owners) != 1 or values[1:3] != ["tcp", "127.0.0.1"]:
                raise ValueError("managed market routes require one TCP loopback source per shard")
            owner = owners.pop()
            if owner in market_shards:
                raise ValueError("duplicate publisher shard source")
            market_shards[owner] = (int(values[3]), set(map(int, values[5:])))
        for shard, config in self.shards.items():
            if order_shards[shard][1] != "127.0.0.1":
                raise ValueError("this process launcher requires loopback worker endpoints")
            symbols = {i + 1 for i, product in enumerate(products) if product[2] == shard}
            if market_shards.get(shard) != (config["market"], symbols):
                raise ValueError(f"publisher route mismatch for shard {shard}")
            ports.append(config["market"])
            for slot in ("A", "B"):
                ports.extend(config[slot][key] for key in ("order", "control", "market"))
                if shard in self.group_shards:
                    ports.append(config[slot]["peer"])
        if any(type(port) is not int or not 0 < port < 65536 for port in ports) or len(set(ports)) != len(ports):
            raise ValueError("all cluster ports must be distinct integers in 1..65535")
        binaries = ["market_matching_engine", "market_engine_router"]
        if self.group_enabled:
            binaries.append("market_group_coordinator")
        for name in binaries:
            if not os.access(self.args.build_dir / name, os.X_OK):
                raise ValueError(f"missing executable {name}; run src/scripts/build.sh")

    def save(self):
        atomic_json(self.state_path, self.state)

    def config(self, name, slots):
        lines = []
        for raw in self.args.config.read_text().splitlines():
            if raw.strip().startswith("shard="):
                shard = raw.split("=", 1)[1].split(",", 1)[0].strip()
                raw = f"shard={shard},127.0.0.1,{self.shards[shard][slots[shard]]['order']}"
            elif raw.strip().startswith("spread_peer=") and self.group_enabled:
                shard = raw.split("=", 1)[1].split(",", 1)[0].strip()
                raw = f"spread_peer={shard},127.0.0.1,{self.shards[shard][slots[shard]]['peer']}"
            lines.append(raw)
        path = self.logs / f"{name}.conf"
        path.write_text("\n".join(lines) + "\n")
        return path

    def spawn(self, key, arguments):
        path = self.logs / f"{key}-{uuid.uuid4().hex[:8]}.log"
        process = subprocess.Popen(list(map(str, arguments)), stdout=subprocess.PIPE,
                                   stderr=subprocess.STDOUT, text=True, bufsize=1)
        with self.process_lock:
            self.processes[key] = process

        def output():
            with path.open("w") as out:
                for line in process.stdout:
                    out.write(line)
                    out.flush()
                    with PRINT_LOCK:
                        print(f"[{key}] {line}", end="", flush=True)
            process.stdout.close()

        threading.Thread(target=output, daemon=True).start()
        log("PROCESS_STARTED", component=key, pid=process.pid, logfile=path)
        return process

    def launch_worker(self, shard, slot, root, binary, standby=False, deadline=None):
        slots = {key: slot for key in self.shards}
        config = self.config(f"worker-{shard}-{slot}", slots)
        ports = self.shards[shard][slot]
        arguments = [binary, "--config", config, "--shard", shard, "--data-dir", root,
                     "--market-port", ports["market"], "--control-port", ports["control"],
                     "--checkpoint-ms", self.args.checkpoint_ms, "--pending-mib", self.args.pending_mib]
        if standby:
            arguments.append("--standby")
        if self.args.trace:
            arguments.append("--trace")
        process = self.spawn(f"shard-{shard}-{slot}", arguments)
        deadline = deadline or time.monotonic() + self.args.sync_timeout
        while time.monotonic() < deadline:
            if self.stop.is_set():
                raise InterruptedError("cluster is stopping")
            if process.poll() is not None:
                raise RuntimeError(f"worker {shard}/{slot} exited {process.returncode}")
            try:
                with Rpc(ports["control"], timeout=1) as rpc:
                    return worker_status(rpc.call("status"))
            except (OSError, ConnectionError):
                time.sleep(0.02)
        raise TimeoutError("worker recovery timed out")

    def start(self):
        if self.group_enabled:
            # A lifecycle fence, not a transaction log: a fresh Router cannot
            # infer missing cross-shard results after an unclean group stop.
            self.state["group_clean_shutdown"] = False
            self.save()
        for shard, state in self.state["shards"].items():
            self.launch_worker(shard, state["active"], state["root"], state["binary"])
        config = self.config("router", {key: state["active"] for key, state in self.state["shards"].items()})
        if self.group_enabled:
            arguments = [self.args.build_dir / "market_group_coordinator", "--config", config,
                         "--stats", self.logs / "coordinator-stats.json"]
            if self.args.trace:
                arguments.append("--trace")
            self.spawn("coordinator", arguments)
        market = self.logs / "market-routes.txt"
        market.write_text("".join(f"{key} {ports['market']} {ports[self.state['shards'][key]['active']]['market']}\n"
                                  for key, ports in self.shards.items()))
        arguments = [self.args.build_dir / "market_engine_router", "--config", config,
                     "--admin-port", self.topology["router_admin_port"], "--market-routes", market]
        if self.args.trace:
            arguments.append("--trace")
        self.spawn("router", arguments)
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline:
            try:
                with Rpc(self.topology["router_admin_port"], timeout=1) as rpc:
                    rows = rpc.call("status").decode().splitlines()
                    group_ready = not self.group_enabled or rpc.call("group-status").startswith(b"READY ")
                    if group_ready and len(rows) == len(self.shards) and all(row.split()[-1] == "1" for row in rows):
                        log("CLUSTER_READY", logs=self.logs, state=self.state_path)
                        for shard, ports in self.shards.items():
                            log("PORTS", shard=shard, public_market=ports["market"], A=ports["A"], B=ports["B"])
                        return
            except OSError:
                pass
            time.sleep(0.05)
        raise TimeoutError("router startup timed out")

    def status(self):
        with self.process_lock:
            processes = {key: {"pid": p.pid, "exit": p.poll()} for key, p in self.processes.items()}
        return {"shards": {key: {**state, "phase": self.phases[key], "ports": self.shards[key]}
                           for key, state in self.state["shards"].items()},
                "processes": processes, "metrics": list(self.metrics), "log_dir": str(self.logs),
                "transition": self.state.get("transition")}

    def stop_process(self, key):
        with self.process_lock:
            process = self.processes.pop(key, None)
        if process and process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()
            log("PROCESS_RETIRED", component=key, pid=process.pid)

    def copy_prefix(self, source, op, length, path, deadline):
        path.parent.mkdir(parents=True, exist_ok=True)
        started = time.monotonic()
        with path.open("xb") as out:
            offset = 0
            while offset < length:
                if self.stop.is_set():
                    raise InterruptedError("cluster is stopping")
                if time.monotonic() > deadline:
                    raise TimeoutError("baseline copy exceeded sync timeout")
                size = min(1024 * 1024, length - offset)
                data = source.call(f"{op} {offset} {size}")
                if len(data) != size:
                    raise RuntimeError("short baseline read")
                out.write(data)
                offset += size
                if self.args.copy_mib_per_second:
                    delay = offset / (self.args.copy_mib_per_second * 1024 * 1024) - (time.monotonic() - started)
                    if delay > 0:
                        self.stop.wait(min(delay, max(0, deadline - time.monotonic())))
                        if time.monotonic() > deadline:
                            raise TimeoutError("baseline copy exceeded sync timeout")
            out.flush()
            os.fsync(out.fileno())

    def restart(self, shard, binary=None):
        if shard not in self.shards:
            raise ValueError("unknown shard")
        if not self.operation.acquire(blocking=False):
            raise RuntimeError("another restart is in progress; use status")
        old = dict(self.state["shards"][shard])
        origin = old["active"]
        target = "B" if origin == "A" else "A"
        binary = str(Path(binary or self.args.build_dir / "market_matching_engine").resolve())
        if not os.access(binary, os.X_OK):
            self.operation.release()
            raise ValueError("replacement binary is not executable")
        ports = self.shards[shard]
        new_root = self.root / "replicas" / f"shard-{shard}" / target / uuid.uuid4().hex
        metric = {"shard": shard, "from": origin, "to": target, "binary": binary}
        begun = time.monotonic()
        cut = False
        activated = False
        routed = False
        source = destination = router = None
        try:
            source = Rpc(ports[origin]["control"])
            router = Rpc(self.topology["router_admin_port"])
            self.phases[shard] = "freezing"
            log("SYNC_BEGIN", shard=shard, source=origin, target=target)
            deadline = begun + self.args.sync_timeout
            while True:
                reply = source.call("freeze").decode().split()
                if reply[0] == "READY":
                    break
                if time.monotonic() > deadline or self.stop.wait(0.01):
                    raise TimeoutError("checkpoint freeze timed out")
            baseline, book_size, trade_size = map(int, reply[1:])
            metric.update(baseline=baseline, copy_bytes=book_size + trade_size)
            self.phases[shard] = "copying"
            log("BASELINE_COPY", shard=shard, checkpoint=baseline, bytes=book_size + trade_size,
                source_checkpoints="FROZEN", data=new_root)
            copied_at = time.monotonic()
            data = new_root / f"shard-{shard}"
            self.copy_prefix(source, "book", book_size, data / "checkpoints" / f"book-{baseline}.bin", deadline)
            self.copy_prefix(source, "trades", trade_size, data / "trades" / "trades.log", deadline)
            (data / "orders").mkdir()
            metric["copy_ms"] = (time.monotonic() - copied_at) * 1000
            metric["source_after_copy"] = worker_status(source.call("status"))["seq"]
            boot_at = time.monotonic()
            self.phases[shard] = "bootstrapping"
            status = self.launch_worker(shard, target, new_root, binary, standby=True, deadline=deadline)
            metric["bootstrap_ms"] = (time.monotonic() - boot_at) * 1000
            if status["seq"] != baseline or status["trade_end"] != trade_size:
                raise RuntimeError("standby recovered a different baseline")
            destination = Rpc(ports[target]["control"])
            self.phases[shard] = "catching_up"
            catchup_at = time.monotonic()
            last_log = 0
            while True:
                if time.monotonic() > deadline or self.stop.is_set():
                    raise TimeoutError("standby cannot catch up within sync timeout; source remains active")
                chunk = source.call(f"tail {status['seq'] + 1}")
                status = worker_status(destination.call(b"apply\n" + chunk))
                source_status = worker_status(source.call("status"))
                lag = source_status["seq"] - status["seq"]
                if time.monotonic() - last_log >= 0.5:
                    log("SYNC_PROGRESS", shard=shard, source_seq=source_status["seq"],
                        standby_seq=status["seq"], lag=lag, history_ready=status["idle"])
                    last_log = time.monotonic()
                if lag <= 256 and status["idle"]:
                    break
                if not chunk:
                    time.sleep(0.005)
            metric["prepare_ms"] = (time.monotonic() - begun) * 1000
            metric["catchup_ms"] = (time.monotonic() - catchup_at) * 1000
            self.phases[shard] = "draining"
            cut_at = time.monotonic()
            cut = True
            router.call(f"cut {shard} {time.time_ns()}")
            cut_deadline = cut_at + 20  # Gateway safety ceiling is 30 seconds.
            while True:
                marker = router.call(f"barrier {shard}").decode()
                if marker != "WAIT":
                    break
                if time.monotonic() > cut_deadline:
                    raise TimeoutError("drain barrier timed out")
                time.sleep(0.005)
            final = int(marker.split()[0])
            metric["drain_ms"] = (time.monotonic() - cut_at) * 1000
            final_at = time.monotonic()
            self.phases[shard] = "final_sync"
            while status["seq"] < final or not status["idle"]:
                if time.monotonic() > cut_deadline:
                    raise TimeoutError("final catch-up timed out")
                chunk = source.call(f"tail {status['seq'] + 1}")
                status = worker_status(destination.call(b"apply\n" + chunk))
            if boundary(status) != marker:
                raise RuntimeError("final commit boundaries differ")
            metric["final_seq"] = final
            metric["replayed_batches"] = final - baseline
            metric["final_sync_ms"] = (time.monotonic() - final_at) * 1000
            self.state["transition"] = {"shard": shard, "from": old,
                                         "to": {"active": target, "root": str(new_root), "binary": binary},
                                         "boundary": marker}
            self.save()
            self.phases[shard] = "promoting"
            if shard in self.group_shards:
                self.phases[shard] = "group_preparing"
                prepare_at = time.monotonic()
                router.call(f"route {shard} 127.0.0.1 {ports[target]['order']} {ports[target]['market']} {ports[target]['peer']}")
                routed = True
                self.wait_route(router, shard, ports[target]["order"], cut_deadline, "prepared")
                metric["group_prepare_ms"] = (time.monotonic() - prepare_at) * 1000
                log("GROUP_REPLACEMENT_PREPARED", shard=shard, peer_port=ports[target]["peer"], boundary=marker)
                self.phases[shard] = "promoting"
            # Once a promotion request is sent its outcome can be uncertain.
            # Even a lost reply must never cause automatic reactivation of A.
            activated = True
            destination.call("activate " + marker)
            if shard in self.group_shards:
                router.call(f"resume {shard}")
            else:
                router.call(f"route {shard} 127.0.0.1 {ports[target]['order']} {ports[target]['market']}")
            self.wait_route(router, shard, ports[target]["order"], cut_deadline)
            metric["cut_ms"] = (time.monotonic() - cut_at) * 1000
            peaks = list(map(int, router.call(f"metrics {shard}").split()))
            metric["queue_peak_requests"], metric["queue_peak_bytes"] = peaks
            self.state["shards"][shard] = self.state["transition"]["to"]
            self.state["transition"] = None
            self.save()
            # Close control before stopping its owner, so no blocked RPC remains.
            source.close()
            source = None
            self.stop_process(f"shard-{shard}-{origin}")
            self.phases[shard] = "active"
            metric["total_ms"] = (time.monotonic() - begun) * 1000
            self.metrics.append(metric)
            atomic_json(self.logs / "restart-results.json", self.metrics)
            log("RESTART_COMPLETE", **metric)
            return metric
        except Exception as error:
            failed_phase = self.phases[shard]
            self.phases[shard] = "failed"
            log("RESTART_FAILED", shard=shard, phase=failed_phase, error=repr(error), promotion_sent=activated)
            if not activated and source:
                try:
                    # A transport failure may have broken either RPC socket.
                    # Reconnect for explicit state inspection, never replay an
                    # uncertain business request or blindly resend promotion.
                    source.close()
                    source = None
                    if router:
                        router.close()
                        router = None
                    self.resume_source(shard, origin, cut, routed)
                    self.state["transition"] = None
                    self.save()
                    self.phases[shard] = "active"
                    log("RESTART_ABORTED_SOURCE_ACTIVE", shard=shard, active=origin)
                except Exception as rollback_error:
                    log("RESTART_NEEDS_ATTENTION", shard=shard, error=repr(rollback_error))
                self.stop_process(f"shard-{shard}-{target}")
            # Once B has activated, never resume A automatically. Keep both roots
            # and the transition record for diagnosis; avoid a second writer.
            raise
        finally:
            for rpc in (destination, source, router):
                if rpc:
                    rpc.close()
            self.operation.release()

    def resume_source(self, shard, origin, cut, routed=False):
        ports = self.shards[shard][origin]
        deadline = time.monotonic() + 5
        with Rpc(ports["control"], timeout=5) as source, Rpc(self.topology["router_admin_port"], timeout=5) as router:
            while True:
                state = worker_status(source.call("status"))
                routes = {row.split()[0]: row.split()[1:]
                          for row in router.call("status").decode().splitlines()}
                if state["role"] == "active" and routes[shard][1] == "active":
                    source.call("release")
                    return
                if not cut:
                    raise RuntimeError("unexpected source role before cut")
                marker = router.call(f"barrier {shard}").decode()
                if marker != "WAIT":
                    source.call("activate " + marker)
                    if routed:
                        router.call(f"restore {shard} 127.0.0.1 {ports['order']} {ports['market']} {ports['peer']}")
                    else:
                        router.call(f"resume {shard}")
                    self.wait_route(router, shard, ports["order"], deadline)
                    return
                if time.monotonic() >= deadline:
                    raise RuntimeError("old instance has not drained; automatic resume is unsafe")
                time.sleep(.01)

    @staticmethod
    def wait_route(router, shard, port, deadline, stage="active"):
        while time.monotonic() < deadline:
            rows = {row.split()[0]: row.split()[1:] for row in router.call("status").decode().splitlines()}
            if rows[shard] == [str(port), stage, "0", "1"]:
                return
            time.sleep(0.005)
        raise TimeoutError("router did not resume")

    def execute(self, command):
        words = shlex.split(command)
        if words == ["status"]:
            return self.status()
        if len(words) in (2, 3) and words[0] == "restart":
            binary = words[2] if len(words) == 3 else None
            if words[1] == "all":
                return [self.restart(shard, binary) for shard in self.shards]
            return self.restart(words[1], binary)
        raise ValueError("commands: status | restart SHARD|all [PATH_TO_NEW_ENGINE]")

    def close(self):
        self.stop.set()
        # Let an already-started cut complete or abort before stopping workers.
        with self.operation:
            group_drained = False
            if self.group_enabled and "router" in self.processes:
                try:
                    shard = min(self.group_shards, key=int)
                    with Rpc(self.topology["router_admin_port"], timeout=5) as router:
                        router.call(f"cut {shard} {time.time_ns()}")
                        deadline = time.monotonic() + 20
                        while router.call(f"barrier {shard}") == b"WAIT":
                            if time.monotonic() >= deadline:
                                raise TimeoutError("group shutdown drain timed out")
                            time.sleep(.01)
                    log("GROUP_SHUTDOWN_DRAINED", shard=shard)
                    group_drained = True
                except Exception as error:
                    log("GROUP_SHUTDOWN_UNRESOLVED", error=repr(error))
            self.stop_process("router")
            for key in list(self.processes):
                self.stop_process(key)
            if self.group_enabled and group_drained:
                self.state["group_clean_shutdown"] = True
                self.save()
        self.lock_file.close()


class CommandServer(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True


class CommandHandler(socketserver.StreamRequestHandler):
    def handle(self):
        self.connection.settimeout(300)
        command = self.rfile.readline(4097)
        if not command.endswith(b"\n") or len(command) > 4096:
            return
        try:
            result = {"ok": True, "result": self.server.cluster.execute(command.decode().strip())}
        except Exception as error:
            result = {"ok": False, "error": str(error)}
        self.wfile.write(json.dumps(result).encode() + b"\n")


def main():
    parser = argparse.ArgumentParser(description="Managed matching engines with bidirectional planned restart.")
    parser.add_argument("--config", type=Path, default=ROOT / "src/config/exchange.conf")
    parser.add_argument("--market-config", type=Path, default=ROOT / "src/config/publisher.conf")
    parser.add_argument("--cluster-config", type=Path, default=ROOT / "src/config/engine_cluster.json")
    parser.add_argument("--build-dir", type=Path, default=ROOT / "build/bin")
    parser.add_argument("--data-dir", type=Path)
    parser.add_argument("--log-dir", type=Path, default=ROOT / "build/logs/matching_engine")
    parser.add_argument("--checkpoint-ms", type=int, default=600000)
    parser.add_argument("--pending-mib", type=int, default=64)
    parser.add_argument("--sync-timeout", type=float, default=300)
    parser.add_argument("--copy-mib-per-second", type=float, default=0,
                        help="optional baseline-copy bandwidth limit; 0 means unlimited")
    parser.add_argument("--trace", action="store_true")
    parser.add_argument("--command", help="send 'status' or 'restart 1 [binary]' to the running group")
    args = parser.parse_args()
    if args.command:
        topology = json.loads(args.cluster_config.read_text())
        with socket.create_connection(("127.0.0.1", topology["supervisor_port"]), timeout=3600) as connection:
            connection.sendall(args.command.encode() + b"\n")
            with connection.makefile("rb") as stream:
                result = json.loads(stream.readline())
        print(json.dumps(result, indent=2))
        return 0 if result["ok"] else 1
    if args.sync_timeout <= 0 or args.copy_mib_per_second < 0:
        parser.error("timeouts must be positive and copy bandwidth nonnegative")
    os.umask(0o077)
    cluster = Cluster(args)
    server = None
    try:
        signal.signal(signal.SIGINT, lambda *_: cluster.stop.set())
        signal.signal(signal.SIGTERM, lambda *_: cluster.stop.set())
        cluster.start()
        server = CommandServer(("127.0.0.1", cluster.topology["supervisor_port"]), CommandHandler)
        server.cluster = cluster
        threading.Thread(target=server.serve_forever, daemon=True).start()
        log("OPERATIONS", commands="status | restart 1 | restart all | restart 1 /path/to/new/binary",
            control_port=cluster.topology["supervisor_port"])
        log("OPERATIONS", another_terminal="src/scripts/run_matching_engine.sh --command 'restart 1'",
            note="Each restart alternates A/B; Ctrl-C stops this group. All old data roots are retained.")

        def terminal():
            for line in sys.stdin:
                try:
                    log("COMMAND_RESULT", result=cluster.execute(line.strip()))
                except Exception as error:
                    log("COMMAND_ERROR", error=str(error))

        if sys.stdin.isatty():
            threading.Thread(target=terminal, daemon=True).start()
        next_status = 0
        while not cluster.stop.wait(0.2):
            with cluster.process_lock:
                for key, process in cluster.processes.items():
                    if process.poll() is not None and (key in ("router", "coordinator") or any(
                            key == f"shard-{shard}-{state['active']}" and cluster.phases[shard] == "active"
                            for shard, state in cluster.state["shards"].items())):
                        raise RuntimeError(f"active process {key} exited {process.returncode}")
            if time.monotonic() >= next_status:
                log("CLUSTER_STATUS", shards={key: f"{state['active']}:{cluster.phases[key]}"
                                              for key, state in cluster.state["shards"].items()})
                next_status = time.monotonic() + 5
        return 0
    finally:
        if server:
            server.shutdown()
            server.server_close()
        cluster.close()


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as error:
        log("CLUSTER_FAILED", error=repr(error))
        sys.exit(1)

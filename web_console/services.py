"""Own only processes started by this console; reuse the repository's supervisor."""
import json
import os
from pathlib import Path
import signal
import socket
import subprocess
import sys
import threading
import time

import protocol as wire
from deployment import cleanup_orphans

BINARIES = ("market_gateway", "market_broadcast", "market_matching_engine",
            "market_engine_router", "market_group_coordinator")


def supervisor(port, command="status", timeout=2):
    with socket.create_connection(("127.0.0.1", port), timeout) as sock:
        sock.sendall(command.encode() + b"\n")
        with sock.makefile("rb") as stream:
            data = stream.readline(4 * 1024 * 1024)
    result = json.loads(data)
    if not result.get("ok"):
        raise RuntimeError(result.get("error", "Supervisor rejected the command"))
    return result["result"]


def reachable(endpoint):
    try:
        with socket.create_connection(endpoint, 0.3):
            return True
    except OSError:
        return False


class Services:
    def __init__(self, config, repo, runtime, data, binaries, journal, attach=False, disposable=False):
        self.config, self.repo, self.runtime, self.data = config, repo, runtime, data
        self.bin, self.log, self.attach_only = binaries, journal, attach
        self.disposable = disposable
        self.lock, self.operation = threading.RLock(), threading.Lock()
        self.processes, self.paused = {}, {}
        self.state = dict(cluster="Offline", gateway=False, publisher=False, shards=[],
                          group="Offline", owned=False, operation=None, error="", metrics=[],
                          data_dir=str(data), log_dir=str(runtime / "logs"))
        self.attached_log_files = {}
        self.stop = threading.Event()
        self.monitor_thread = threading.Thread(target=self._monitor, daemon=True)
        self.monitor_thread.start()

    def snapshot(self):
        with self.lock:
            return json.loads(json.dumps(self.state))

    def _spawn(self, name, arguments):
        self.runtime.joinpath("logs").mkdir(parents=True, exist_ok=True)
        log_path = self.runtime / "logs" / f"{name}-{time.time_ns()}.log"
        env = dict(os.environ, PYTHONDONTWRITEBYTECODE="1", PYTHONUNBUFFERED="1")
        record = self.runtime / "processes" / f"{name}-{time.time_ns()}.json"
        host = Path(__file__).resolve().with_name("service_host.py")
        command = [sys.executable, "-B", host, "--name", name, "--record", record, "--", *arguments]
        with log_path.open("w") as output:
            process = subprocess.Popen(list(map(str, command)), cwd=self.repo,
                                       stdin=subprocess.PIPE, stdout=output,
                                       stderr=subprocess.STDOUT, start_new_session=True, env=env)
        self.processes[name] = process
        def output():
            with log_path.open(encoding="utf-8", errors="replace") as out:
                while True:
                    line = out.readline()
                    if not line:
                        if process.poll() is not None:
                            break
                        time.sleep(.1)
                        continue
                    source, message = name, line.rstrip()
                    if name == "cluster" and message.startswith("[") and "] " in message:
                        source, message = message[1:].split("] ", 1)
                    self.log.add(source, message)
            process.stdin.close()
            self.log.add(name, f"PROCESS EXIT code={process.wait()}", "info" if process.returncode == 0 else "error")
        threading.Thread(target=output, daemon=True).start()
        self.log.add(name, f"PROCESS START pid={process.pid}")
        return process

    def ensure_binaries(self, force=False):
        if self.attach_only:
            return
        complete = all((self.bin / name).is_file() for name in BINARIES)
        if complete and not force:
            if self.bin != self.runtime / "build/bin":
                return
            oldest_binary = min((self.bin / name).stat().st_mtime for name in BINARIES)
            sources = (p for p in (self.repo / "src").rglob("*")
                       if p.name == "CMakeLists.txt" or p.suffix in (".cpp", ".c", ".hpp", ".h", ".cmake"))
            if not any(p.stat().st_mtime > oldest_binary for p in sources):
                return
        self.log.add("console", "BUILD Compiling the existing C++ services into web_console/runtime/build")
        build = self.runtime / "build"
        build.mkdir(parents=True, exist_ok=True)
        commands = [["cmake", "-S", str(self.repo / "src"), "-B", str(build),
                     "-DCMAKE_BUILD_TYPE=Release", "-DMARKETLAB_BUILD_TESTS=OFF"],
                    ["cmake", "--build", str(build), "--parallel", "4", "--target", *BINARIES]]
        for command in commands:
            process = self._spawn("build", command)
            if process.wait() != 0:
                raise RuntimeError("C++ build failed. See the System log")
        self.bin = build / "bin"

    def action(self, action, target="all", done=None):
        if action not in ("start", "shutdown", "recover", "restart", "interrupt", "resume", "crash", "build", "build-and-start"):
            raise ValueError("Unknown service action")
        if target != "all" and target not in self.config.topology["shards"]:
            raise ValueError("Choose a configured shard")
        if not self.operation.acquire(blocking=False):
            raise RuntimeError("Another maintenance operation is still running")
        with self.lock:
            self.state.update(operation=dict(action=action, target=target, started=time.time()), error="")
        def work():
            before = dict(self.processes)
            try:
                self.log.add("console", f"{action.upper()} target={target}")
                if action in ("build", "build-and-start"):
                    self.ensure_binaries(force=True)
                    if action == "build-and-start":
                        self._start()
                elif action in ("start", "recover"):
                    self._resume_all()
                    self._start()
                elif action == "shutdown":
                    self._shutdown()
                elif action == "restart":
                    if self.paused:
                        raise RuntimeError("Resume interrupted shards before a planned restart")
                    result = supervisor(self.config.topology["supervisor_port"], f"restart {target}", 360)
                    self.log.add("console", "RESTART COMPLETE " + json.dumps(result), "success")
                elif action in ("interrupt", "resume", "crash"):
                    self._signal(action, target)
                if done:
                    done()
                self.log.add("console", f"{action.upper()} COMPLETE", "success")
            except Exception as exc:
                if action in ("start", "recover", "build-and-start"):
                    self._rollback_start(before)
                with self.lock:
                    self.state["error"] = str(exc)
                self.log.add("console", f"{action.upper()} FAILED {exc}", "error")
            finally:
                with self.lock:
                    self.state["operation"] = None
                self.operation.release()
        threading.Thread(target=work, daemon=True, name="maintenance").start()

    def _start(self):
        try:
            current = supervisor(self.config.topology["supervisor_port"])
        except (OSError, ValueError, RuntimeError):
            current = None
        if current:
            if self.disposable and not (self.processes.get("cluster") and self.processes["cluster"].poll() is None):
                raise RuntimeError("Another cluster is already running. Stop its console before starting an empty demo")
            # Verify the running supervisor uses this symbol universe, when available.
            config_files = list(Path(current["log_dir"]).glob("*router*.conf"))
            if config_files:
                from bridge import entries
                actual = [v for k, v in entries(config_files[-1]) if k == "symbol"]
                expected = [[p["symbol"], str(p["close"]), str(p["shard"])] for p in self.config.products]
                if actual != expected:
                    raise RuntimeError("Running cluster has a different symbol configuration; stop it before starting this console")
            self.log.add("console", "ATTACHED Using the running local cluster", "success")
        elif self.attach_only:
            raise RuntimeError("No running supervisor found in attach mode")
        else:
            process = self.processes.get("cluster")
            if process and process.poll() is None:
                raise RuntimeError("Owned cluster is still starting or stopping")
            cleanup_orphans(self.config, self.runtime, self.log)
            if self.disposable:
                for name, endpoint in (("gateway", self.config.gateway), ("publisher", self.config.query)):
                    own = self.processes.get(name)
                    if reachable(endpoint) and not (own and own.poll() is None):
                        raise RuntimeError(f"Another {name} is already running. Stop that deployment before starting an empty demo")
            if reachable(self.config.address(self.config.settings["router_listen"])):
                port = self.config.settings["router_listen"][1]
                raise RuntimeError(f"Router port {port} is occupied by a process outside this console's ownership. Its supervisor is unavailable; see System log")
            self.ensure_binaries()
            self.data.mkdir(parents=True, exist_ok=True)
            args = [sys.executable, "-B", self.repo / "src/matching_engine/cluster/run.py",
                    "--config", self.config.exchange, "--market-config", self.config.publisher,
                    "--cluster-config", self.config.topology_path, "--build-dir", self.bin,
                    "--data-dir", self.data, "--log-dir", self.runtime / "logs" / "engines"]
            process = self._spawn("cluster", args)
            deadline = time.monotonic() + 35
            while time.monotonic() < deadline and not self.stop.is_set():
                if process.poll() is not None:
                    raise RuntimeError("Cluster startup failed; see System log. Existing data has been preserved")
                try:
                    current = supervisor(self.config.topology["supervisor_port"], timeout=0.5)
                    break
                except (OSError, ValueError, RuntimeError):
                    self.stop.wait(0.15)
            else:
                raise TimeoutError("Timed out waiting for cluster readiness")
        if self.attach_only:
            return
        self.ensure_binaries()
        for name, endpoint, args in (
                ("gateway", self.config.gateway, [self.bin / "market_gateway", "gateway", "--config", self.config.exchange]),
                ("publisher", self.config.query, [self.bin / "market_broadcast", "publish", "--config", self.config.publisher])):
            if not reachable(endpoint):
                old = self.processes.get(name)
                if old and old.poll() is None:
                    raise RuntimeError(f"{name.title()} is running but its port is unavailable; inspect its log")
                self._spawn(name, args)
        deadline = time.monotonic() + 20
        while time.monotonic() < deadline:
            for name in ("cluster", "gateway", "publisher"):
                process = self.processes.get(name)
                if process and process.poll() is not None:
                    raise RuntimeError(f"{name.title()} exited during startup; see System log")
            try:
                group = wire.rpc(self.config.topology["router_admin_port"], "group-status", .6)
                routes = wire.rpc(self.config.topology["router_admin_port"], "status", .6)
                connected = [row.split() for row in routes.splitlines()]
                ready = (len(connected) == len(self.config.topology["shards"]) and
                         all(row[-1] == "1" for row in connected) and
                         group.split()[:1] in (["READY"], ["DISABLED"]))
                if ready and reachable(self.config.gateway):
                    first = wire.snapshot(self.config.query, self.config.products[0]["id"])
                    last = wire.snapshot(self.config.query, self.config.products[-1]["id"])
                    if first["epoch"] == last["epoch"]:
                        self.log.add("console", "LOCAL SERVICES READY All engines connected; publisher snapshots verified", "success")
                        return
            except (OSError, ValueError, RuntimeError):
                pass
            time.sleep(.15)
        raise RuntimeError("Startup readiness timed out. Check native service logs; TCP listening alone does not mean the exchange is ready")

    def _rollback_start(self, before):
        for name in ("cluster", "gateway", "publisher", "build"):
            process = self.processes.get(name)
            if not process or process is before.get(name) or process.poll() is not None:
                continue
            try:
                self.log.add("console", f"STARTUP ROLLBACK Stopping newly started {name}", "warning")
                if name == "cluster":
                    self._resume_all()
                process.terminate()
                process.wait(40 if name == "cluster" else 10)
            except (OSError, subprocess.TimeoutExpired) as exc:
                self.log.add("console", f"ROLLBACK INCOMPLETE {name}: {exc}", "error")

    def _owned_cluster(self):
        process = self.processes.get("cluster")
        if not process or process.poll() is not None:
            raise RuntimeError("This operation requires a cluster started by this console")
        return process

    def _signal(self, action, target):
        parent = self._owned_cluster()
        if target == "all":
            raise ValueError("Select one shard for interruption or recovery")
        state = supervisor(self.config.topology["supervisor_port"])
        shard = state["shards"][target]
        name = f"shard-{target}-{shard['active']}"
        process = state["processes"].get(name)
        if not process or process["exit"] is not None:
            raise RuntimeError("Selected engine process is not running")
        pid = process["pid"]
        if os.getpgid(pid) != parent.pid:
            raise RuntimeError("Engine is not in this console's process group")
        if action == "resume":
            paused = self.paused.get(target)
            if paused != pid:
                raise RuntimeError("This shard was not interrupted by the console")
            os.kill(pid, signal.SIGCONT)
            self.paused.pop(target, None)
        else:
            if action == "crash" and int(target) not in self.group_shards:
                raise ValueError("Crash recovery uses a spread-group member; choose shard 5–8. Use Shutdown / Recover for the complete cluster")
            if action == "crash" and target in self.paused:
                raise RuntimeError("Resume the shard before a crash test")
            os.kill(pid, signal.SIGSTOP if action == "interrupt" else signal.SIGKILL)
            if action == "interrupt":
                self.paused[target] = pid
        self.log.add("console", f"{action.upper()} shard={target} pid={pid}", "warning" if action != "resume" else "success")

    @property
    def group_shards(self):
        return {p["shard"] for p in self.config.products if p["group"] == "Spreads"}

    def _resume_all(self):
        parent = self.processes.get("cluster")
        for shard, pid in list(self.paused.items()):
            try:
                if parent and parent.poll() is None and os.getpgid(pid) == parent.pid:
                    os.kill(pid, signal.SIGCONT)
            except ProcessLookupError:
                pass
            self.paused.pop(shard, None)

    def _shutdown(self):
        parent = self._owned_cluster()
        self._resume_all()
        parent.terminate()
        try:
            parent.wait(timeout=35)
        except subprocess.TimeoutExpired:
            raise RuntimeError("Graceful shutdown has not completed. No force kill was performed; inspect System log")
        self.log.add("console", "ENGINES OFFLINE Gateway and publisher remain available for reconnection", "warning")

    def _tail_attached(self, log_dir):
        if self.processes.get("cluster") and self.processes["cluster"].poll() is None:
            return
        root = Path(log_dir)
        for path in sorted(root.glob("*.log"))[-40:]:
            try:
                size = path.stat().st_size
                offset = self.attached_log_files.get(path, max(0, size - 8192))
                if offset > size:
                    offset = 0
                with path.open("rb") as file:
                    file.seek(offset)
                    raw = file.read(32768)
                    end = raw.rfind(b"\n")
                    if end < 0:
                        continue
                    self.attached_log_files[path] = offset + end + 1
                source = path.stem.rsplit("-", 1)[0]
                for line in raw[:end].decode(errors="replace").splitlines():
                    self.log.add(source, line)
            except OSError:
                pass

    def _monitor(self):
        while not self.stop.is_set():
            update = dict(gateway=reachable(self.config.gateway), publisher=reachable(self.config.query))
            try:
                status = supervisor(self.config.topology["supervisor_port"], timeout=0.6)
                parent = self.processes.get("cluster")
                owned = bool(parent and parent.poll() is None)
                rows = []
                try:
                    router = wire.rpc(self.config.topology["router_admin_port"], "status", 0.6)
                    routes = {r.split()[0]: r.split()[2:] for r in router.splitlines()}
                    group = wire.rpc(self.config.topology["router_admin_port"], "group-status", 0.6)
                except (OSError, ValueError, RuntimeError):
                    routes, group = {}, "Unavailable"
                for sid, shard in status["shards"].items():
                    name = f"shard-{sid}-{shard['active']}"
                    process = status["processes"].get(name, {})
                    phase = "interrupted" if sid in self.paused else shard["phase"]
                    route = routes.get(sid, [])
                    rows.append(dict(id=int(sid), active=shard["active"], phase=phase,
                                     pid=process.get("pid"), alive=process.get("exit") is None,
                                     connected=bool(route and route[-1] == "1"),
                                     route=route[0] if route else "unavailable", root=shard["root"]))
                cluster = "Live" if rows and all(r["connected"] and r["alive"] and r["phase"] != "interrupted" for r in rows) and group.split()[:1] in (["READY"], ["DISABLED"]) else "Paused"
                roots = sorted(set(s["root"] for s in status["shards"].values()))
                update.update(cluster=cluster, shards=rows, owned=owned, group=group,
                              log_dir=status["log_dir"], data_dir=str(self.data) if owned else " · ".join(roots),
                              metrics=status.get("metrics", [])[-8:])
                self._tail_attached(status["log_dir"])
            except (OSError, ValueError, RuntimeError, KeyError):
                update.update(cluster="Offline", group="Offline", shards=[], owned=False)
            with self.lock:
                self.state.update(update)
            self.stop.wait(0.5)

    def close(self):
        # Serialize with any restart: never kill a supervisor halfway through promotion.
        with self.operation:
            self._resume_all()
            parent = self.processes.get("cluster")
            if parent and parent.poll() is None:
                try:
                    self._shutdown()
                except Exception as exc:
                    self.log.add("console", f"SHUTDOWN INCOMPLETE {exc}", "error")
            for name in ("gateway", "publisher", "build"):
                process = self.processes.get(name)
                if process and process.poll() is None:
                    process.terminate()
                    try:
                        process.wait(5)
                    except subprocess.TimeoutExpired:
                        self.log.add("console", f"{name} has not exited", "warning")
            self.stop.set()
        self.monitor_thread.join(3)
        return all(process.poll() is not None for process in self.processes.values())

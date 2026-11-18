#!/usr/bin/env python3
"""Dependency-free loopback console. Run start.sh; Ctrl-C shuts down owned services."""
import argparse
import csv
import fcntl
import io
import json
import mimetypes
import os
from pathlib import Path
import secrets
import signal
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlsplit
import urllib.request
import webbrowser

from bridge import Config, Journal, MarketFeed, Trader
from services import Services
from deployment import data_directory

ROOT = Path(__file__).resolve().parent
REPO = ROOT.parent


class Console:
    def __init__(self, args):
        self.config = Config(args.config, args.market_config, args.cluster_config)
        self.runtime, self.token = args.runtime, secrets.token_urlsafe(32)
        self.log = Journal()
        selected_data = data_directory(self.config, args.runtime, args.data_dir, self.log)
        self.market = MarketFeed(self.config, self.log)
        self.traders = {slot: Trader(slot, self.config, self.log) for slot in ("alice", "bob")}
        self.session_lock = threading.Lock()
        self.services = Services(self.config, REPO, args.runtime, selected_data,
                                 args.build_dir, self.log, args.attach)
        self.stop = threading.Event()

    def state(self, after=0):
        logs, sequence = self.log.after(after)
        market = self.market.snapshot()
        services = self.services.snapshot()
        services["publisher_listening"] = services["publisher"]
        services["publisher"] = services["publisher"] and any(book["status"] == "Live" for book in market["books"].values())
        return dict(products=self.config.products, accounts=list(self.config.accounts),
                    services=services, market=market,
                    traders={key: client.snapshot() for key, client in self.traders.items()},
                    logs=logs, log_sequence=sequence, time=time.time(),
                    config_path=str(self.config.exchange), csrf=self.token)

    def command(self, path, body):
        if path == "/api/services":
            action, target = body.get("action"), str(body.get("target", "all"))
            def recovered():
                self.market.request_refresh()
                if action in ("start", "recover", "resume"):
                    for trader in self.traders.values():
                        trader.reconnect()
            self.services.action(action, target, recovered)
            return {"message": "Operation started"}
        if path == "/api/market/refresh":
            self.market.request_refresh()
            return {"message": "Snapshot refresh requested"}
        if path == "/api/stop":
            self.stop.set()
            return {"message": "Console is shutting down"}
        slot = body.get("slot")
        if slot not in self.traders:
            raise ValueError("Unknown trading panel")
        client = self.traders[slot]
        if path == "/api/login":
            with self.session_lock:
                username = body.get("username", slot)
                if not isinstance(username, str):
                    raise ValueError("Invalid username")
                for key, other in self.traders.items():
                    if key != slot and other.username == username and other.status not in ("Logged out", "Login failed", "Disconnected"):
                        raise ValueError("This account is connected in the other panel. Each account supports one session")
                password = self.config.accounts.get(username) if body.get("configured") is True else body.get("password")
                if not isinstance(password, str) or not password:
                    raise ValueError("Enter a password or choose configured local credentials")
                client.login(username, password)
            return {"message": "Login started"}
        if path == "/api/logout":
            client.logout()
            self.log.add(slot, "LOGGED OUT Open orders remain on the exchange", "warning")
            return {"message": "Logged out"}
        if path == "/api/order":
            return {"request_id": client.submit(body.get("symbol"), body.get("side"), body.get("price"), body.get("quantity"))}
        if path == "/api/cancel":
            return {"request_id": client.cancel(body.get("order_id"))}
        if path == "/api/history":
            client.query_history(body.get("more") is True)
            return {"message": "Trade query started"}
        if path == "/api/reconnect":
            client.reconnect()
            return {"message": "Reconnect requested"}
        raise ValueError("Unknown API operation")

    def close(self):
        for trader in self.traders.values():
            trader.logout()
        self.market.close()
        self.services.close()


class Server(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True


class Handler(BaseHTTPRequestHandler):
    server_version = "MarketLabConsole/1.0"

    def log_message(self, *_):
        pass

    def send(self, status, data, mime="application/json; charset=utf-8"):
        if not isinstance(data, bytes):
            data = json.dumps(data, ensure_ascii=True, separators=(",", ":")).encode()
        self.send_response(status)
        self.send_header("Content-Type", mime)
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("X-Frame-Options", "DENY")
        self.send_header("Referrer-Policy", "no-referrer")
        self.send_header("Content-Security-Policy", "default-src 'self'; script-src 'self'; style-src 'self' 'unsafe-inline'; img-src 'self' data:; connect-src 'self'; frame-ancestors 'none'; base-uri 'none'; form-action 'self'")
        self.end_headers()
        try:
            self.wfile.write(data)
        except (BrokenPipeError, ConnectionResetError):
            pass

    def allowed_host(self):
        return self.headers.get("Host") in (f"127.0.0.1:{self.server.server_port}", f"localhost:{self.server.server_port}")

    def do_GET(self):
        if not self.allowed_host():
            self.send(403, {"error": "Loopback host required"})
            return
        path = urlsplit(self.path)
        if path.path == "/api/health":
            self.send(200, {"service": "marketlab-console", "version": 1})
        elif path.path == "/api/state":
            try:
                after = int(parse_qs(path.query).get("after", ["0"])[0])
                self.send(200, self.server.app.state(after))
            except ValueError:
                self.send(400, {"error": "Invalid journal cursor"})
        elif path.path == "/api/logs":
            rows, _ = self.server.app.log.after(0)
            self.send(200, "\n".join(f"{r['time']} [{r['source']}] {r['text']}" for r in rows).encode(), "text/plain; charset=utf-8")
        elif path.path.startswith("/api/history/") and path.path.endswith(".csv"):
            slot = path.path.removeprefix("/api/history/").removesuffix(".csv")
            if slot not in self.server.app.traders:
                self.send(404, {"error": "Unknown account"})
                return
            rows = self.server.app.traders[slot].snapshot()["history"]
            out = io.StringIO()
            columns = ("timestamp", "side", "symbol", "quantity", "price", "type", "id")
            writer = csv.DictWriter(out, columns, extrasaction="ignore")
            writer.writeheader()
            for row in rows:
                safe = {k: ("'" + v if isinstance(v, str) and v and v[0] in "=+-@" else v) for k, v in row.items()}
                writer.writerow(safe)
            self.send(200, out.getvalue().encode(), "text/csv; charset=utf-8")
        else:
            filename = {"/": "index.html", "/index.html": "index.html", "/app.js": "app.js", "/styles.css": "styles.css", "/favicon.svg": "favicon.svg"}.get(path.path)
            if not filename:
                self.send(404, {"error": "Not found"})
                return
            file = ROOT / "static" / filename
            self.send(200, file.read_bytes(), (mimetypes.guess_type(filename)[0] or "text/plain") + "; charset=utf-8")

    def do_POST(self):
        origin = self.headers.get("Origin")
        allowed = {f"http://127.0.0.1:{self.server.server_port}", f"http://localhost:{self.server.server_port}"}
        if not self.allowed_host() or (origin and origin not in allowed) or not secrets.compare_digest(self.headers.get("X-Console-Token", ""), self.server.app.token):
            self.send(403, {"error": "Local console authorization required; reload this page"})
            return
        try:
            length = int(self.headers.get("Content-Length", "0"))
            if not 0 < length <= 8192 or self.headers.get_content_type() != "application/json":
                raise ValueError("Expected a bounded JSON request")
            self.connection.settimeout(5)
            body = json.loads(self.rfile.read(length))
            if not isinstance(body, dict):
                raise ValueError("Expected a JSON object")
            result = self.server.app.command(urlsplit(self.path).path, body)
            self.send(200, {"ok": True, **result})
        except (ValueError, RuntimeError, OSError, KeyError) as exc:
            self.send(400, {"ok": False, "error": str(exc)})


def arguments():
    parser = argparse.ArgumentParser(description="MarketLab local trading console")
    parser.add_argument("--port", type=int, default=8080)
    parser.add_argument("--config", type=Path, default=REPO / "src/config/exchange.conf")
    parser.add_argument("--market-config", type=Path, default=REPO / "src/config/publisher.conf")
    parser.add_argument("--cluster-config", type=Path, default=REPO / "src/config/engine_cluster.json")
    parser.add_argument("--runtime", type=Path, default=ROOT / "runtime")
    parser.add_argument("--data-dir", type=Path, help="Compatible existing cluster data, or a new persistent directory")
    parser.add_argument("--build-dir", type=Path, help="Explicit existing binaries (default: build current source inside this folder)")
    parser.add_argument("--rebuild", action="store_true", help="Build current C++ source inside this folder")
    parser.add_argument("--attach", action="store_true", help="Connect to already running services; never launch them")
    parser.add_argument("--no-start", action="store_true", help="Open the console; start engines from the UI")
    parser.add_argument("--no-open", action="store_true", help="Do not open a browser automatically")
    parser.add_argument("--stop", action="store_true", help="Gracefully stop an existing console at --port")
    args = parser.parse_args()
    if not 1 <= args.port <= 65535:
        parser.error("port must be in 1..65535")
    for field in ("runtime", "config", "market_config", "cluster_config"):
        setattr(args, field, getattr(args, field).resolve())
    args.data_dir = args.data_dir.resolve() if args.data_dir else None
    args.build_dir = (args.build_dir or args.runtime / "build/bin").resolve()
    if args.rebuild:
        args.build_dir = args.runtime / "build/bin"
    return args


def main():
    args = arguments()
    url = f"http://127.0.0.1:{args.port}"
    if args.stop:
        with urllib.request.urlopen(url + "/api/state", timeout=2) as response:
            state = json.load(response)
        request = urllib.request.Request(url + "/api/stop", b"{}",
                                         {"Content-Type": "application/json", "X-Console-Token": state["csrf"]})
        with urllib.request.urlopen(request, timeout=3) as response:
            print(response.read().decode())
        return
    os.umask(0o077)
    args.runtime.mkdir(parents=True, exist_ok=True)
    lock = (args.runtime / "console.lock").open("a")
    try:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        raise RuntimeError("A console already owns this runtime directory. Use another --runtime or stop the existing console")
    # Bind before starting processes, so a duplicate web port cannot leak a deployment.
    server = Server(("127.0.0.1", args.port), Handler)
    app = Console(args)
    server.app = app
    signal.signal(signal.SIGINT, lambda *_: app.stop.set())
    signal.signal(signal.SIGTERM, lambda *_: app.stop.set())
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    print(f"\nMarketLab Console  {url}\nConfig: {args.config}\nData:   {app.services.data}\nCtrl-C stops this console and its owned services.\n", flush=True)
    if args.rebuild and not args.attach:
        app.services.action("build" if args.no_start else "build-and-start")
    elif not args.no_start:
        app.services.action("start")
    if not args.no_open:
        webbrowser.open(url)
    try:
        app.stop.wait()
    finally:
        print("Shutting down owned services gracefully…", flush=True)
        server.shutdown()
        server.server_close()
        app.close()
        lock.close()


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        sys.exit(1)

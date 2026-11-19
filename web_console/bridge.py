"""Threaded local clients for the real gateway and publisher."""
from collections import deque
from concurrent.futures import ThreadPoolExecutor
import copy
import datetime as dt
import json
import re
import select
import socket
import struct
import threading
import time

import protocol as wire


def entries(path):
    for raw in path.read_text().splitlines():
        line = raw.split("#", 1)[0].strip()
        if line:
            key, value = line.split("=", 1)
            yield key.strip(), [part.strip() for part in value.split(",")]


class Config:
    def __init__(self, exchange, publisher, topology):
        self.exchange, self.publisher, self.topology_path = exchange, publisher, topology
        self.accounts, self.products = {}, []
        self.settings = {}
        for key, values in entries(exchange):
            if key == "user":
                self.accounts[values[0]] = values[1]
            elif key == "symbol":
                self.products.append(dict(id=len(self.products) + 1, symbol=values[0],
                                          close=int(values[1]), shard=int(values[2])))
            else:
                self.settings[key] = values
        self.gateway = self.address(self.settings.get("gateway_listen", ["127.0.0.1", "7001"]))
        self.topology = json.loads(topology.read_text())
        self.market_settings = dict(entries(publisher))
        self.query = (self.market_settings.get("query_host", ["127.0.0.1"])[0],
                      int(self.market_settings.get("query_port", ["9100"])[0]))
        self.multicast = self.address(self.market_settings.get("multicast_group", ["239.10.10.1", "9001"]))
        self.interface = self.market_settings.get("multicast_interface", ["127.0.0.1"])[0]
        self.by_symbol = {p["symbol"]: p for p in self.products}
        spread_names = self.settings.get("spread_symbols", ["A", "B", "C", "SP_AB", "SP_AC", "BF_ABC"])
        for p in self.products:
            p["group"] = "Spreads" if p["symbol"] in spread_names else "Outrights"
            p["lower"], p["upper"] = p["close"] * 90 // 100, p["close"] * 110 // 100
        if self.settings.get("spreads") == ["enabled"]:
            for i, coeffs in ((3, (1, -1, 0)), (4, (1, 0, -1)), (5, (1, -2, 1))):
                if i >= len(spread_names) or spread_names[i] not in self.by_symbol:
                    continue
                lower = upper = 0
                for name, n in zip(spread_names[:3], coeffs):
                    leg = self.by_symbol[name]
                    lower += n * leg["lower" if n > 0 else "upper"]
                    upper += n * leg["upper" if n > 0 else "lower"]
                self.by_symbol[spread_names[i]].update(lower=lower, upper=upper)

    @staticmethod
    def address(values):
        return values[0], int(values[1])


class Journal:
    def __init__(self):
        self.lock = threading.RLock()
        self.rows = deque(maxlen=4000)
        self.sequence = 0

    def add(self, source, message, level=None):
        message = re.sub(r"\x1b\[[0-?]*[ -/]*[@-~]", "", str(message)).strip()[:4096]
        if not message:
            return
        if not level:
            upper = message.upper()
            if any(s in upper for s in ("ERROR", "FAILED", "REJECT", "UNRESOLVED", "TIMEOUT")):
                level = "error"
            elif any(s in upper for s in ("WARNING", "PAUSED", "UNAVAILABLE", "LOST", "DISCONNECTED", "INTERRUPT")):
                level = "warning"
            elif "TRADE" in upper and "TRADE_END" not in upper:
                level = "trade"
            elif any(s in upper for s in ("READY", "RECOVERED", "RESUMED", "ACCEPTED", "COMPLETE")):
                level = "success"
            else:
                level = "info"
        with self.lock:
            self.sequence += 1
            self.rows.append(dict(id=self.sequence, source=source, level=level, text=message,
                                  time=dt.datetime.now(dt.timezone.utc).isoformat(timespec="milliseconds")))

    def after(self, sequence):
        with self.lock:
            return [r.copy() for r in self.rows if r["id"] > sequence][-600:], self.sequence


class Trader:
    def __init__(self, slot, config, journal, on_trade=None):
        self.slot, self.config, self.log = slot, config, journal
        self.on_trade = on_trade
        self.on_order = None
        self.lock, self.send_lock = threading.RLock(), threading.Lock()
        self.stop = threading.Event()
        self.thread, self.sock = None, None
        self.username, self.password = slot, ""
        self.status, self.error = "Logged out", ""
        self.sequence = 0
        self.pending = {}
        self.receipts = {}
        self.orders, self.snapshot_orders, self.overlay = {}, {}, {}
        self.fills = deque(maxlen=300)
        self.history, self.history_pages = [], deque()
        self.history_ids = set()
        self.history_busy, self.history_error = False, ""
        self.history_count = 0
        self.history_version, self.history_auto = 0, False
        self.snapshot_request = None
        self.revision = 0
        self.session_generation = 0
        self.reconnect_until = 0

    def snapshot(self, history_version=None):
        with self.lock:
            return dict(slot=self.slot, username=self.username, status=self.status, error=self.error,
                        orders=copy.deepcopy(list(self.orders.values())), fills=list(self.fills),
                        history=None if history_version==self.history_version else copy.deepcopy(self.history),
                        history_version=self.history_version, history_loaded=len(self.history),
                        history_more=bool(self.history_pages),
                        history_busy=self.history_busy, history_error=self.history_error,
                        history_count=self.history_count, revision=self.revision,
                        pending=len(self.pending), can_reconnect=bool(self.password))

    def progress(self):
        """Lightweight counters for replay pacing; avoid copying entire histories."""
        with self.lock:
            return dict(status=self.status, orders=len(self.orders), history=len(self.history),
                        history_more=bool(self.history_pages), history_busy=self.history_busy,
                        history_error=self.history_error)

    def logout(self):
        self.stop.set()
        with self.lock:
            sock = self.sock
        if sock:
            try:
                sock.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
        if self.thread and self.thread is not threading.current_thread():
            self.thread.join(3)
            if self.thread.is_alive():
                raise RuntimeError("Previous session is still closing; try again")
        with self.lock:
            self.sock = None
            self.status = "Logged out"
            self.password = ""
            self.pending.clear()
            self.history_busy = False
            self.revision += 1

    def login(self, username, password):
        self.logout()
        wire.fixed(username)
        wire.fixed(password)
        with self.lock:
            if username != self.username:
                self.orders.clear()
                self.fills.clear()
                self.history.clear()
                self.history_pages.clear()
                self.history_version += 1
            self.username, self.password = username, password
            self.stop = threading.Event()
            self.status, self.error = "Connecting", ""
            self.reconnect_until = time.monotonic() + 20
            self.session_generation += 1
        self.thread = threading.Thread(target=self._run, daemon=True, name=f"trader-{self.slot}")
        self.thread.start()

    def reconnect(self):
        with self.lock:
            username, password = self.username, self.password
            if self.status in ("Ready", "Connecting", "Synchronizing", "Reconnecting"):
                return
        if password:
            self.login(username, password)

    def _send(self, kind, payload, purpose):
        # One lock order everywhere: sending mutex, then state mutex.
        with self.send_lock:
            with self.lock:
                sock = self.sock
                if sock is None:
                    raise ConnectionError("Account is disconnected")
                if len(self.pending) >= 256:
                    raise RuntimeError("Too many pending requests")
                self.sequence += 1
                request = self.sequence
                self.pending[request] = dict(purpose=purpose, sent=time.monotonic())
                if kind == 1:
                    self.snapshot_request = request
                    self.snapshot_orders, self.overlay = {}, {}
                packet = wire.frame(kind, request, payload)
            try:
                sock.sendall(packet)
            except OSError:
                # It may already have reached the exchange: never replay it.
                try:
                    sock.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass
                raise ConnectionError("Connection lost; request outcome is unknown")
        return str(request)

    def submit(self, symbol, side, price, quantity):
        if symbol not in self.config.by_symbol or side not in ("Buy", "Sell"):
            raise ValueError("Choose a configured symbol and a valid side")
        if type(price) is not int or type(quantity) is not int or not -2147483648 <= price <= 2147483647 or not 0 < quantity <= 2147483647:
            raise ValueError("Price must be int32; quantity must be a positive int32")
        self._require_ready()
        rid = self._send(3, struct.pack("<32sBii", wire.fixed(symbol), 1 if side == "Buy" else 2,
                                        price, quantity), "order")
        self.log.add(self.slot, f"SENT {side.upper()} {symbol}  qty={quantity}  price={price}  request={rid}")
        return rid

    def cancel(self, order_id):
        self._require_ready()
        if not isinstance(order_id, str) or not order_id.isdecimal() or not 0 < int(order_id) < 2**64:
            raise ValueError("Enter a valid order ID")
        rid = self._send(4, struct.pack("<Q", int(order_id)), "cancel")
        self.log.add(self.slot, f"CANCEL SENT order={order_id}  request={rid}")
        return rid

    def wait_receipt(self, request, timeout=8):
        """Wait for a real exchange acknowledgement; never retry an uncertain order."""
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            with self.lock:
                result = self.receipts.get(int(request))
                if result is not None:
                    if "error" in result:
                        raise RuntimeError(result["error"])
                    return result.copy()
                if self.status != "Ready":
                    raise RuntimeError("Demo stopped: account disconnected; check orders before continuing")
            time.sleep(.002)
        raise TimeoutError("Demo acknowledgement timed out; sent orders were not replayed")

    def _require_ready(self):
        with self.lock:
            if self.status != "Ready":
                raise RuntimeError("Log in and wait for the complete order snapshot")

    def query_history(self, more=False, all_pages=False):
        self._require_ready()
        with self.lock:
            if self.history_busy:
                raise RuntimeError("A trade query is already running")
            if more and not self.history_pages:
                raise ValueError("All history pages have been received")
            if more and len(self.history) >= 20000:
                raise ValueError("The console retains 20,000 records. Refresh to start a new query")
            self.history_busy, self.history_error = True, ""
            self.history_count = 0
            if not more:
                self.history, self.history_ids = [], set()
                self.history_pages.clear()
                self.history_auto = all_pages
                self.history_version += 1
                page = (0, 0, 0)
            else:
                page = self.history_pages.popleft()
            self.revision += 1
        self._request_history(page)
        self.log.add(self.slot, "QUERY Loading committed trade history…")

    def _request_history(self, page):
        shard, after, through = page
        try:
            self._send(9, struct.pack("<HHQQ", shard, 128, after, through),
                       "history_all" if not shard else "history_page")
        except Exception:
            with self.lock:
                self.history_busy = False
                if shard:
                    self.history_pages.appendleft(page)
            raise

    def _run(self):
        announced = False
        while not self.stop.is_set():
            sock = None
            try:
                sock = socket.create_connection(self.config.gateway, 0.8)
                sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                sock.settimeout(3)
                with self.lock:
                    self.sock = sock
                    self.status = "Synchronizing"
                self._send(1, wire.fixed(self.username) + wire.fixed(self.password), "login")
                login_deadline = time.monotonic() + 3
                while not self.stop.is_set():
                    with self.lock:
                        ready = self.status == "Ready"
                        if ready:
                            announced = False
                        oldest = min((p["sent"] for p in self.pending.values()), default=time.monotonic())
                    if not ready and time.monotonic() > min(login_deadline, self.reconnect_until):
                        raise TimeoutError("Login snapshot timed out")
                    if time.monotonic() - oldest > 35:
                        raise TimeoutError("Request response timed out")
                    if not select.select([sock], [], [], 0.2)[0]:
                        continue
                    kind, request, payload = wire.receive(sock)
                    if not self._event(kind, request, payload):
                        return
            except (OSError, ValueError, struct.error) as exc:
                if self.stop.is_set():
                    break
                with self.lock:
                    if self.status == "Ready":
                        self.reconnect_until = time.monotonic() + 20
                    unknown = [str(k) for k, p in self.pending.items() if p["purpose"] in ("order", "cancel")]
                    self.pending.clear()
                    self.history_busy = False
                    self.status, self.error = "Reconnecting", str(exc)
                    self.revision += 1
                if not announced:
                    self.log.add(self.slot, f"DISCONNECTED {exc}. Reconnecting; sent orders are never replayed.", "warning")
                    announced = True
                if unknown:
                    self.log.add(self.slot, "UNKNOWN OUTCOME requests=" + ", ".join(unknown) + ". Check orders and trade history.", "warning")
                if time.monotonic() >= self.reconnect_until:
                    with self.lock:
                        self.status = "Disconnected"
                    self.log.add(self.slot, "RECONNECT TIMEOUT Login is available to retry.", "error")
                    break
            finally:
                with self.lock:
                    if self.sock is sock:
                        self.sock = None
                if sock:
                    sock.close()
            self.stop.wait(0.2)

    def _event(self, kind, request, payload):
        message, level, execution, order_update = None, None, None, None
        next_history = None
        with self.lock:
            if kind == 2:
                message = "LOGIN ACCEPTED Waiting for active-order snapshot…"
            elif kind == 8:
                _, reason = struct.unpack("<QH", payload)
                pending = self.pending.pop(request, {})
                self.error = wire.REJECTIONS.get(reason, f"Rejected ({reason})")
                if request:
                    self.receipts[request] = {"error": self.error}
                message, level = f"REJECTED {self.error}  request={request}", "error"
                if pending.get("purpose", "").startswith("history"):
                    self.history_busy, self.history_error = False, self.error
                if reason == 1:
                    self.status = "Login failed"
                    self.password = ""
                    self.log.add(self.slot, message, level)
                    return False
            elif kind == 5:
                value = wire.order(payload[8:])
                order_update = value
                self._apply_order(value)
                if self.snapshot_request is not None:
                    self.overlay[value["id"]] = value
                if request:
                    self.pending.pop(request, None)
                    self.receipts[request] = value.copy()
                message = (f"{value['status'].upper()} {value['side'].upper()} {value['symbol']}  "
                           f"qty={value['original']}  filled={value['filled']}  remaining={value['remaining']}  "
                           f"price={value['price']}  order={value['id']}")
                level = "success" if value["status"] == "Accepted" else "warning" if value["status"] == "Canceled" else "info"
            elif kind == 6:
                value = wire.trade(payload)
                execution = value
                self.fills.appendleft(value)
                message = f"TRADE {value['side'].upper()} {value['symbol']}  qty={value['quantity']}  price={value['price']}  trade={value['id']}"
                level = "trade"
            elif kind == 7:
                if request != self.snapshot_request:
                    return True
                if payload[8]:
                    self.orders = self.snapshot_orders
                    for value in self.overlay.values():
                        self._apply_order(value)
                    self.snapshot_orders, self.overlay = {}, {}
                    self.snapshot_request = None
                    self.pending.pop(request, None)
                    self.status, self.error = "Ready", ""
                    message, level = f"READY Logged in as {self.username} · {len(self.orders)} active orders restored", "success"
                else:
                    value = wire.order(payload[9:])
                    self.snapshot_orders[value["id"]] = value
            elif kind in (10, 12, 13, 14):
                if not self.pending.get(request, {}).get("purpose", "").startswith("history"):
                    return True
                value = wire.history(kind, payload, self.username)
                execution = value
                identity = (kind, value["id"])
                if identity not in self.history_ids:
                    self.history_ids.add(identity)
                    self.history.append(value)
                    self.history_count += 1
                    self.history_version += 1
            elif kind == 11:
                purpose = self.pending.get(request, {}).get("purpose", "")
                if not purpose.startswith("history"):
                    return True
                _, shard, flags, count, after, through = wire.HISTORY_END.unpack(payload)
                if flags & 1:
                    self.history_pages.append((shard, after, through))
                if (shard == 0 and flags == 2) or purpose == "history_page":
                    self.pending.pop(request, None)
                    if self.history_auto and self.history_pages and len(self.history)<20000:
                        next_history = self.history_pages.popleft()
                    else:
                        self.history_busy = False
                        self.history.sort(key=lambda r: int(r["timestamp_ns"]), reverse=True)
                        self.history_version += 1
                        message, level = f"QUERY COMPLETE {len(self.history)} records loaded" + (" · more pages available" if self.history_pages else ""), "success"
            self.revision += 1
            while len(self.receipts) > 2048:
                self.receipts.pop(next(iter(self.receipts)))
        if execution and self.on_trade:
            self.on_trade(execution)
        if order_update and self.on_order:
            self.on_order(order_update)
        if next_history:
            self._request_history(next_history)
        if message:
            self.log.add(self.slot, message, level)
        return True

    def _apply_order(self, value):
        if value["remaining"] > 0 and value["status"] in ("Accepted", "Partially filled"):
            self.orders[value["id"]] = value
        else:
            self.orders.pop(value["id"], None)


class MarketFeed:
    """Native UDP deltas, with authoritative TCP snapshots after gaps/epoch changes."""
    def __init__(self, config, journal):
        self.config, self.log = config, journal
        self.lock, self.stop = threading.RLock(), threading.Event()
        self.books = {}
        self.epoch, self.retired = None, deque(maxlen=32)
        self.states = {p["id"]: dict(advanced=0.0, retry=0.0, gap=0.0, pending={}, checking=True)
                       for p in config.products}
        self.udp_ok, self.packets, self.recoveries = False, 0, 0
        self.executor = ThreadPoolExecutor(max_workers=8, thread_name_prefix="snapshot")
        self.futures = {}
        self.thread = threading.Thread(target=self._run, daemon=True, name="market-feed")
        self.thread.start()

    def request_refresh(self):
        with self.lock:
            for state in self.states.values():
                state["checking"], state["retry"] = True, 0.0

    def snapshot(self):
        now = time.monotonic()
        with self.lock:
            books = {}
            for sid, state in self.states.items():
                book = copy.deepcopy(self.books.get(sid, {}))
                age = now - state["advanced"]
                book["status"] = ("Waiting" if not book else "Stale" if age > 3 else
                                  "Recovering" if state["checking"] or state["gap"] else
                                  "Late" if age > 1.5 else "Live")
                book["age_ms"] = round(age * 1000) if state["advanced"] else None
                books[str(sid)] = book
            return dict(books=books, transport="UDP + TCP recovery" if self.udp_ok else "TCP snapshots",
                        packets=self.packets, recoveries=self.recoveries, epoch=self.epoch)

    def _install(self, message, authoritative=False):
        sid, now = message["symbol_id"], time.monotonic()
        if sid not in self.states:
            return
        state = self.states[sid]
        if message["epoch"] != self.epoch:
            if message["epoch"] in self.retired:
                return
            if not authoritative:
                for s in self.states.values():
                    s["checking"] = True
                return
            if self.epoch:
                self.retired.append(self.epoch)
            self.epoch = message["epoch"]
            for s in self.states.values():
                s["pending"].clear()
                s["checking"], s["gap"], s["advanced"] = True, 0.0, 0.0
            self.log.add("feed", "EPOCH CONFIRMED Rebuilding public books from TCP snapshots", "success")
        old = self.books.get(sid)
        same_epoch = old and old["epoch"] == message["epoch"]
        index = int(message["index"])
        previous = int(old["index"]) if same_epoch else -1
        if message["kind"] in (2, 4):
            if same_epoch and index < previous:
                return
            self.books[sid] = message
            if index > previous or not state["advanced"]:
                state["advanced"] = now
            state["checking"] = False
            state["pending"] = {i: m for i, m in state["pending"].items() if i > index}
        elif index <= previous:
            return
        elif not same_epoch or index != previous + 1 or state["checking"]:
            state["gap"] = state["gap"] or now
            if len(state["pending"]) < 64:
                state["pending"][index] = message
            else:
                state["pending"].clear()
                state["checking"] = True
            return
        else:
            self._delta(old, message)
            state["advanced"] = now
        current = self.books[sid]
        while int(current["index"]) + 1 in state["pending"]:
            following = state["pending"].pop(int(current["index"]) + 1)
            self._delta(current, following)
            state["advanced"] = now
        state["gap"] = (state["gap"] or now) if state["pending"] else 0.0

    @staticmethod
    def _delta(old, message):
        levels = {(v["side"], v["price"]): v for v in old["levels"]}
        for level in message["levels"]:
            key = level["side"], level["price"]
            if level["quantity"] or level["implied"]:
                levels[key] = level
            else:
                levels.pop(key, None)
        if any(sum(v["side"] == side for v in levels.values()) > 5 for side in (1, 2)):
            raise ValueError("Delta produces more than five levels")
        old.update(index=message["index"], timestamp=message["timestamp"], timestamp_ns=message["timestamp_ns"],
                   levels=sorted(levels.values(), key=lambda v: (v["side"], -v["price"] if v["side"] == 1 else v["price"])))

    def _run(self):
        udp = None
        try:
            udp = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
            udp.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            udp.bind(("", self.config.multicast[1]))
            udp.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP,
                           socket.inet_aton(self.config.multicast[0]) + socket.inet_aton(self.config.interface))
            udp.setblocking(False)
            self.udp_ok = True
            self.log.add("feed", f"SUBSCRIBED {self.config.multicast[0]}:{self.config.multicast[1]}")
        except OSError as exc:
            if udp:
                udp.close()
            udp = None
            self.log.add("feed", f"UDP unavailable: {exc}. Using authoritative TCP snapshots.", "warning")
        try:
            while not self.stop.is_set():
                if udp and select.select([udp], [], [], 0.05)[0]:
                    for _ in range(256):
                        try:
                            raw, _ = udp.recvfrom(2048)
                        except BlockingIOError:
                            break
                        try:
                            message = wire.market(raw)
                            with self.lock:
                                self.packets += 1
                                self._install(message)
                        except (ValueError, struct.error):
                            self.request_refresh()
                else:
                    self.stop.wait(0.05)
                now = time.monotonic()
                with self.lock:
                    for sid, future in list(self.futures.items()):
                        if not future.done():
                            continue
                        del self.futures[sid]
                        try:
                            self._install(future.result(), authoritative=True)
                            self.recoveries += 1
                            self.states[sid]["retry"] = now + 0.5
                        except (OSError, ValueError, struct.error):
                            self.states[sid]["retry"] = now + 0.5
                    for sid, state in self.states.items():
                        stale = now - state["advanced"] > (1.5 if self.udp_ok else 0.5)
                        missing = state["checking"] or (state["gap"] and now - state["gap"] >= 0.2)
                        if len(self.futures) >= 8:
                            break
                        if sid not in self.futures and now >= state["retry"] and (stale or missing):
                            self.futures[sid] = self.executor.submit(wire.snapshot, self.config.query, sid)
        finally:
            if udp:
                udp.close()

    def close(self):
        self.stop.set()
        self.thread.join(2)
        self.executor.shutdown(wait=True, cancel_futures=True)

"""Adapters for the existing packed C++ protocols. No exchange logic lives here."""
import datetime as dt
import socket
import struct

HEADER = struct.Struct("<HIQ")
ORDER = struct.Struct("<Q32sBiiiiB")
TRADE = struct.Struct("<QQQ32sBii")
HISTORY = struct.Struct("<QQ32siiQQ32s32s")
HISTORY_END = struct.Struct("<QHHIQQ")
SPREAD = struct.Struct("<QQ32siiQIQ32sB")
LEG = struct.Struct("<Q32si")
SIZES = {2: 22, 5: 80, 6: 79, 7: 81, 8: 24, 10: 150,
         11: 46, 12: 162, 13: 211, 14: 299}
STATUSES = {1: "Accepted", 2: "Partially filled", 3: "Filled", 4: "Canceled"}
REJECTIONS = {1: "Invalid credentials", 2: "Invalid order or price outside limits",
              3: "Order cannot be canceled", 4: "Invalid history query",
              5: "Trade history is unavailable", 6: "Trade history is busy"}


def text(raw):
    return raw.split(b"\0", 1)[0].decode("utf-8", errors="replace")


def fixed(value):
    data = value.encode("utf-8")
    if not data or len(data) > 31 or b"\0" in data:
        raise ValueError("Names must contain 1–31 UTF-8 bytes")
    return data.ljust(32, b"\0")


def timestamp(ns):
    return dt.datetime.fromtimestamp(ns / 1e9, dt.timezone.utc).isoformat(timespec="milliseconds")


def exact(sock, count):
    result = bytearray()
    while len(result) < count:
        chunk = sock.recv(count - len(result))
        if not chunk:
            raise ConnectionError("Connection closed by peer")
        result.extend(chunk)
    return bytes(result)


def frame(kind, request, payload=b""):
    return HEADER.pack(kind, HEADER.size + len(payload), request) + payload


def receive(sock):
    kind, length, request = HEADER.unpack(exact(sock, HEADER.size))
    if SIZES.get(kind) != length:
        raise ValueError(f"Invalid gateway frame: type={kind}, length={length}")
    return kind, request, exact(sock, length - HEADER.size)


def order(raw):
    oid, symbol, side, price, original, filled, remaining, status = ORDER.unpack(raw)
    return dict(id=str(oid), symbol=text(symbol), side="Buy" if side == 1 else "Sell",
                price=price, original=original, filled=filled, remaining=remaining,
                status=STATUSES.get(status, "Unknown"))


def trade(raw):
    ns, tid, oid, symbol, side, price, quantity = TRADE.unpack(raw)
    return dict(id=str(tid), order_id=str(oid), timestamp=timestamp(ns), timestamp_ns=str(ns),
                symbol=text(symbol), side="Buy" if side == 1 else "Sell", price=price,
                quantity=quantity, type="Live fill")


def history(kind, raw, username):
    if kind in (10, 12):
        ns, tid, symbol, price, quantity, buy, sell, buyer, seller = HISTORY.unpack_from(raw)
        buyer, seller = text(buyer), text(seller)
        side = "Self" if buyer == seller == username else "Buy" if buyer == username else "Sell"
        out = dict(id=str(tid), timestamp=timestamp(ns), timestamp_ns=str(ns), symbol=text(symbol),
                   price=price, quantity=quantity, side=side, buy_order_id=str(buy),
                   sell_order_id=str(sell), buyer=buyer, seller=seller,
                   type="Direct" if kind == 10 else "Implied leg")
        if kind == 12:
            aggressor, fill = struct.unpack_from("<QI", raw, HISTORY.size)
            out.update(aggressor=str(aggressor), fill=fill)
        return out
    ns, tid, symbol, price, quantity, aggressor, fill, oid, user, side = SPREAD.unpack_from(raw)
    out = dict(id=str(tid), timestamp=timestamp(ns), timestamp_ns=str(ns), symbol=text(symbol),
               price=price, quantity=quantity, side="Buy" if side == 1 else "Sell",
               order_id=str(oid), user=text(user), aggressor=str(aggressor), fill=fill,
               type="Calendar spread" if kind == 13 else "Butterfly")
    legs = []
    for i, name in enumerate(("A", "B / C") if kind == 13 else ("A", "B", "B second", "C")):
        leg_id, leg_user, leg_price = LEG.unpack_from(raw, SPREAD.size + i * LEG.size)
        legs.append(dict(name=name, order_id=str(leg_id), user=text(leg_user), price=leg_price))
    out["legs"] = legs
    return out


def market(raw):
    if len(raw) < 35:
        raise ValueError("Truncated market message")
    kind, epoch, symbol, index, ns = struct.unpack_from("<B16sHQQ", raw)
    if kind not in (1, 2, 3, 4):
        raise ValueError("Unknown market message")
    layout = struct.Struct("<BiQQQ" if kind >= 3 else "<BiQQ")
    if (len(raw) - 35) % layout.size or (len(raw) - 35) // layout.size > (10 if kind in (2, 4) else 20):
        raise ValueError("Invalid market depth length")
    levels = []
    seen = set()
    for at in range(35, len(raw), layout.size):
        values = layout.unpack_from(raw, at)
        side, price, quantity = values[:3]
        implied, orders = values[3:] if kind >= 3 else (0, values[3])
        if side not in (1, 2) or (side, price) in seen:
            raise ValueError("Invalid market level")
        seen.add((side, price))
        levels.append(dict(side=side, price=price, quantity=quantity, implied=implied, orders=orders))
    return dict(kind=kind, epoch=epoch.hex(), symbol_id=symbol, index=str(index),
                timestamp=timestamp(ns), timestamp_ns=str(ns), levels=levels)


def snapshot(endpoint, symbol, timeout=1.0):
    with socket.create_connection(endpoint, timeout) as sock:
        sock.sendall(struct.pack("<H", symbol))
        size, = struct.unpack("<H", exact(sock, 2))
        if not 35 <= size <= 325:
            raise ValueError("Invalid snapshot length")
        message = market(exact(sock, size))
        if message["symbol_id"] != symbol or message["kind"] not in (2, 4):
            raise ValueError("Snapshot response identity mismatch")
        return message


def rpc(port, request, timeout=2):
    data = request.encode()
    with socket.create_connection(("127.0.0.1", port), timeout) as sock:
        sock.sendall(struct.pack("<I", len(data)) + data)
        length, = struct.unpack("<I", exact(sock, 4))
        if not 1 <= length <= 1024 * 1024:
            raise ValueError("Invalid control response")
        response = exact(sock, length)
        if response[0]:
            raise RuntimeError(response[1:].decode())
        return response[1:].decode()

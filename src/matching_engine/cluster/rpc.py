"""Bounded TCP management frames shared by the launcher and process tests."""
import socket
import struct


def receive_exact(connection, length):
    result = bytearray()
    while len(result) < length:
        part = connection.recv(length - len(result))
        if not part:
            raise ConnectionError("management peer closed")
        result.extend(part)
    return bytes(result)


class Rpc:
    def __init__(self, port, timeout=30):
        self.connection = socket.create_connection(("127.0.0.1", port), timeout)
        self.connection.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)

    def call(self, request):
        if isinstance(request, str):
            request = request.encode()
        if not 0 < len(request) <= 128 * 1024 * 1024:
            raise ValueError("invalid management request size")
        self.connection.sendall(struct.pack("<I", len(request)) + request)
        size, = struct.unpack("<I", receive_exact(self.connection, 4))
        if not 0 < size <= 128 * 1024 * 1024:
            raise ValueError("invalid management response size")
        response = receive_exact(self.connection, size)
        if response[0]:
            raise RuntimeError(response[1:].decode())
        return response[1:]

    def close(self):
        self.connection.close()

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()


def worker_status(response):
    fields = response.decode().split()
    if len(fields) != 9:
        raise ValueError("invalid worker status")
    values = list(map(int, fields[:8]))
    return dict(zip(("seq", "next_order", "next_trade", "trade_end", "trade_count",
                     "checkpoint", "idle", "frozen", "role"), values + [fields[8]]))


def boundary(status):
    return " ".join(str(status[key]) for key in
                    ("seq", "next_order", "next_trade", "trade_end", "trade_count"))

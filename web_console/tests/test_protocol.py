"""Wire compatibility and publication semantics; no running exchange required."""
from pathlib import Path
import socket
import struct
import sys
import threading
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import protocol as p
from bridge import Journal, MarketFeed
from collections import deque


class ProtocolTests(unittest.TestCase):
    def test_uint64_ids_remain_decimal_strings(self):
        oid = (65530 << 48) | 123
        raw = p.ORDER.pack(oid, p.fixed("SP_AB"), 2, -3, 7, 2, 5, 2)
        value = p.order(raw)
        self.assertEqual(value["id"], str(oid))
        self.assertEqual((value["price"], value["remaining"], value["status"]), (-3, 5, "Partially filled"))

    def test_v2_quantity_fields_and_negative_price(self):
        raw = struct.pack("<B16sHQQBiQQQ", 4, b"e"*16, 24, 8, 1_700_000_000_000_000_000, 1, -3, 4, 7, 2)
        value = p.market(raw)
        self.assertEqual(value["levels"], [dict(side=1, price=-3, quantity=4, implied=7, orders=2)])
        self.assertEqual(value["index"], "8")

    def test_v1_has_no_implied_field(self):
        raw = struct.pack("<B16sHQQBiQQ", 2, b"e"*16, 1, 0, 1, 2, 100, 4, 2)
        self.assertEqual(p.market(raw)["levels"][0]["implied"], 0)

    def test_fragmented_gateway_frames(self):
        first, second = socket.socketpair()
        packet = p.frame(5, 12, struct.pack("<Q", 1) + p.ORDER.pack(1, p.fixed("A"), 1, 120, 1, 0, 1, 1))
        def writer():
            for byte in packet:
                first.sendall(bytes([byte]))
            first.close()
        thread = threading.Thread(target=writer)
        thread.start()
        kind, request, raw = p.receive(second)
        self.assertEqual((kind, request, p.order(raw[8:])["symbol"]), (5, 12, "A"))
        second.close()
        thread.join()

    def test_all_history_variants(self):
        raw = p.HISTORY.pack(1_700_000_000_000_000_000, 2**63+7, p.fixed("A"), 120, 2, 3, 4, p.fixed("alice"), p.fixed("bob"))
        self.assertEqual(p.history(10, raw, "alice")["side"], "Buy")
        self.assertEqual(p.history(12, raw + struct.pack("<QI", 7, 2), "bob")["type"], "Implied leg")
        spread = p.SPREAD.pack(1_700_000_000_000_000_000, 10, p.fixed("BF_ABC"), 5, 1, 8, 1, 8, p.fixed("bob"), 1)
        legs = b"".join(p.LEG.pack(i, p.fixed("alice"), price) for i, price in enumerate((120,110,110,105),1))
        out = p.history(14, spread + legs, "bob")
        self.assertEqual(len(spread + legs) + 14, 299)
        self.assertEqual([v["price"] for v in out["legs"]], [120,110,110,105])

    def test_unknown_epoch_needs_tcp_and_delta_gap_recovers(self):
        feed = object.__new__(MarketFeed)
        feed.states = {1:dict(advanced=0,retry=0,gap=0,pending={},checking=True)}
        feed.books, feed.epoch, feed.retired, feed.log = {}, None, deque(maxlen=32), Journal()
        base = dict(kind=2,epoch="old",symbol_id=1,index="1",timestamp="t",timestamp_ns="1",levels=[])
        feed._install(base.copy())
        self.assertIsNone(feed.epoch)
        feed._install(base.copy(),authoritative=True)
        delta = dict(base,kind=1,index="3",levels=[dict(side=1,price=100,quantity=5,implied=0,orders=1)])
        feed._install(delta)
        self.assertEqual(feed.books[1]["index"], "1")
        feed._install(dict(base,kind=1,index="2"))
        self.assertEqual(feed.books[1]["index"], "3")
        self.assertEqual(feed.books[1]["levels"][0]["quantity"], 5)
        feed._install(dict(base,epoch="new",index="0"),authoritative=True)
        feed._install(dict(base,index="100"),authoritative=True)
        self.assertEqual(feed.epoch,"new")

    def test_malformed_frames_rejected(self):
        for raw in (b"",b"a"*34,struct.pack("<B16sHQQ",9,b"e"*16,1,0,1),struct.pack("<B16sHQQ",4,b"e"*16,1,0,1)+b"x"):
            with self.assertRaises(ValueError): p.market(raw)
        with self.assertRaises(ValueError): p.fixed("x"*32)

    def test_journal_is_bounded_and_strips_terminal_escape_sequences(self):
        log=Journal()
        log.add("alice","\x1b[32mACCEPTED\x1b[0m")
        self.assertEqual(log.after(0)[0][0]["text"],"ACCEPTED")
        for i in range(5000): log.add("test",str(i))
        self.assertEqual(len(log.rows),4000)
        self.assertEqual(log.after(4999)[1],5001)


if __name__ == "__main__":
    unittest.main()

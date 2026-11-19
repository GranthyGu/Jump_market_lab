import json
from pathlib import Path
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from bridge import Config
from demo import DemoWorkspace, PriceTape, scenario


class DemoTests(unittest.TestCase):
    def test_only_the_marked_session_is_deleted(self):
        with tempfile.TemporaryDirectory() as folder:
            runtime=Path(folder)
            persistent=runtime/"data";persistent.mkdir()
            (persistent/"orders").write_text("retain")
            workspace=DemoWorkspace(runtime)
            workspace.data.mkdir()
            (workspace.data/"orders").write_text("demo")
            (workspace.root/"session.json").write_text('{}')
            with self.assertRaises(RuntimeError):workspace.cleanup()
            self.assertTrue(workspace.data.exists())
            (workspace.root/"session.json").write_text(json.dumps(workspace.marker))
            workspace.cleanup()
            self.assertFalse(workspace.root.exists())
            self.assertEqual((persistent/"orders").read_text(),"retain")

    def test_chart_deduplicates_counterparties_and_history_and_stays_bounded(self):
        tape=PriceTape([dict(symbol="ITEM0001")])
        for i in range(405):
            trade=dict(symbol="ITEM0001",id=str(i),timestamp_ns=str(i*1000000),
                       timestamp="2026-11-19T00:00:00.000+00:00",price=100+i%4,quantity=2)
            tape.record(trade)
            tape.record(dict(trade,side="Sell",type="Direct"))
        points=tape.snapshot("ITEM0001")["points"]
        self.assertEqual(len(points),400)
        self.assertEqual(points[0]["id"],"5")
        self.assertEqual(points[-1]["id"],"404")
        with self.assertRaises(ValueError):tape.snapshot("missing")

    def test_prepared_orders_respect_prices_and_fill_five_levels_per_symbol(self):
        config=Config(ROOT.parent/"src/config/exchange.conf",ROOT.parent/"src/config/publisher.conf",
                      ROOT.parent/"src/config/engine_cluster.json")
        steps=scenario(config,seed=7)
        self.assertEqual(len(steps),20000)
        for step in steps:
            product=config.by_symbol[step["symbol"]]
            self.assertLessEqual(product["lower"],step["price"])
            self.assertLessEqual(step["price"],product["upper"])
            self.assertGreater(step["quantity"],0)
        depth=[s for s in steps if s["phase"]=="Opening liquidity"]
        self.assertEqual(len(depth),1500)
        self.assertTrue(all(s["resting"] for s in steps[:1500]))
        self.assertTrue(any(s["resting"] for s in steps[1500:2500]))
        self.assertTrue(any(not s["resting"] for s in steps[1500:2500]))
        self.assertEqual(steps,scenario(config,seed=7))
        self.assertNotEqual(steps,scenario(config,seed=8))
        for product in config.products:
            for side in ("Buy","Sell"):
                matching=[s for s in depth if s["symbol"]==product["symbol"] and s["side"]==side]
                prices={s["price"] for s in matching}
                self.assertEqual(len(prices),5)
                for price in prices:
                    self.assertEqual(sum(s["price"]==price for s in matching),5)


if __name__=="__main__":unittest.main()

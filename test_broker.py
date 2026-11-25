from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from broker import Broker


class BrokerTest(unittest.TestCase):
    def test_expiry_after_reopen_and_stale_receipt(self):
        with TemporaryDirectory() as tmp:
            path = Path(tmp, "broker.db")
            broker = Broker(path)
            broker.put("first")
            broker.put("second")
            old = broker.take(now=10, lease=5)
            broker = Broker(path)
            second = broker.take(now=14, lease=9)
            self.assertEqual(second.payload, "second")
            self.assertFalse(broker.acknowledge(old, now=15))
            again = broker.take(now=15, lease=5)
            self.assertEqual(again.id, old.id)
            self.assertFalse(broker.acknowledge(old, now=15))
            self.assertTrue(broker.acknowledge(again, now=16))

    def test_concurrent_receivers_get_one_delivery(self):
        with TemporaryDirectory() as tmp:
            broker = Broker(Path(tmp, "broker.db"))
            broker.put("one")
            with ThreadPoolExecutor(4) as pool:
                receipts = list(pool.map(lambda _: broker.take(now=0, lease=5), range(4)))
            self.assertEqual(sum(item is not None for item in receipts), 1)

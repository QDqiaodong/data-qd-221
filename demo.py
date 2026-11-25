from pathlib import Path
from tempfile import TemporaryDirectory
from broker import Broker

with TemporaryDirectory() as root:
    path = Path(root, "broker.db")
    queue = Broker(path)
    queue.put("rebuild-cache")
    first = queue.take(now=0, lease=5)
    print("worker A:", first.payload)
    queue = Broker(path)
    retry = queue.take(now=5, lease=5)
    print("redelivered:", retry.id == first.id and retry.receipt != first.receipt)
    print("stale ack:", queue.acknowledge(first, now=5))
    print("current ack:", queue.acknowledge(retry, now=6))
    print("empty:", queue.take(now=7, lease=5) is None)

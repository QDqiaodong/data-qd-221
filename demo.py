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

    # Maintenance handoff: pause two keys, then take them as one exclusive group.
    queue.put_ordered("swap-original", key="asset-a", needs=None)
    queue.put_ordered("swap-proxy", key="asset-b", needs=None)
    ticket = queue.request_maintenance(("asset-a", "asset-b"), now=10, until=40)
    print("ticket:", ticket)
    print("paused take:", queue.take(now=11, lease=5))
    lease = queue.take_maintenance(ticket, now=12)
    print("handoff until:", lease["until"])
    print("end without receipt:", queue.end_maintenance(ticket, now=13))
    print("end with receipt:", queue.end_maintenance(
        ticket, now=13, receipt=lease["receipt"]
    ))
    print("after end:", queue.take(now=14, lease=5).payload)


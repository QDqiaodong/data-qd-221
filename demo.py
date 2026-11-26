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
    after_end = queue.take(now=14, lease=5)
    print("after end:", after_end.payload)
    queue.acknowledge(after_end, now=14)
    queue.acknowledge(queue.take(now=14, lease=5), now=14)
    # Hedged execution: a slow node runs in pool-a, then pool-b borrows in.
    nodes = [
        {
            "name": "transcode",
            "key": "video",
            "payload": "transcode-video",
            "needs": None,
            "requires": [],
            "after": [],
        },
        {
            "name": "publish",
            "key": "publish-key",
            "payload": "publish",
            "needs": None,
            "requires": ["transcode"],
            "after": [],
        },
    ]
    ids = queue.submit_graph(
        "release", nodes, race_after={"transcode": 10}
    )
    first = queue.take_race("pool-a", capabilities=frozenset(), now=20, lease=20)
    print("race start:", first.payload)
    print("early hedge:", queue.take_hedge(
        first.id, "pool-b", capabilities=frozenset(), now=24, lease=20
    ))
    backup = queue.take_hedge(
        first.id, "pool-b", capabilities=frozenset(), now=30, lease=20
    )
    print("hedged:", backup.id == first.id and backup.receipt != first.receipt)
    print("backup ack:", queue.acknowledge(backup, now=31))
    print("publish now ready:", queue.take(now=31, lease=5).id == ids["publish"])
    print("losing ack:", queue.acknowledge(first, now=31))


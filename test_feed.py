from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from tempfile import TemporaryDirectory
import sqlite3
import threading
import tracemalloc
import unittest

from broker import Broker


class FastFeedBroker(Broker):
    """Fast disk commits for large public-API test setup only."""

    def _connect(self):
        connection = super()._connect()
        connection.execute("PRAGMA synchronous=OFF")
        return connection


class ProgressFeedBroker(Broker):
    progress_callbacks = 0

    def _connect(self):
        connection = super()._connect()
        connection.execute("PRAGMA synchronous=OFF")

        def count_progress():
            self.progress_callbacks += 1
            return 0

        connection.set_progress_handler(count_progress, 1000)
        return connection


class FeedConfigurationTest(unittest.TestCase):
    def test_configure_repeat_reconfiguration_and_invalid_input(self):
        with TemporaryDirectory() as tmp:
            broker = Broker(Path(tmp, "feed.db"))
            names = ("A", "b-1", "c_2")
            self.assertTrue(broker.configure_feed(names))
            self.assertEqual(
                broker.source_checkpoints(),
                {name: (0, -1) for name in names},
            )

            # Registration order is not identity.
            self.assertFalse(broker.configure_feed(("c_2", "A", "b-1")))

            invalid_groups = [
                ["A"],  # list is not the required tuple
                (),
                tuple(f"p{i}" for i in range(9)),
                ("A", "A"),
                ("bad name",),
                ("bad.name",),
                (1,),
                ("x" * 65,),
            ]
            for group in invalid_groups:
                with self.subTest(group=group):
                    with self.assertRaises(ValueError):
                        broker.configure_feed(group)

            with self.assertRaises(ValueError):
                broker.configure_feed(("A", "new-partition"))

            # The rejected reconfiguration did not change the group.
            self.assertEqual(set(broker.source_checkpoints()), set(names))

    def test_new_entries_require_configured_and_registered_partition(self):
        with TemporaryDirectory() as tmp:
            path = Path(tmp, "feed.db")
            broker = Broker(path)

            with self.assertRaises(ValueError):
                broker.source_checkpoints()
            with self.assertRaises(ValueError):
                broker.stage_event(
                    "A",
                    1,
                    event_time=0,
                    key="k",
                    needs=None,
                    payload=None,
                )
            with self.assertRaises(ValueError):
                broker.seal_source("A", through=0, watermark=0)
            with self.assertRaises(ValueError):
                broker.publish_ready(limit=1)

            broker.configure_feed(("A",))
            with self.assertRaises(ValueError):
                broker.stage_event(
                    "B",
                    1,
                    event_time=0,
                    key="k",
                    needs=None,
                    payload=None,
                )
            with self.assertRaises(ValueError):
                broker.seal_source("B", through=0, watermark=0)

    def test_invalid_limits_and_integers(self):
        with TemporaryDirectory() as tmp:
            broker = Broker(Path(tmp, "feed.db"))
            broker.configure_feed(("A",))

            for bad_limit in (0, 129, True, False, 1.0, "1", None):
                with self.subTest(limit=bad_limit):
                    with self.assertRaises(ValueError):
                        broker.publish_ready(limit=bad_limit)

            valid_stage = dict(key="k", needs=None, payload=None)
            for bad_offset in (0, -1, True, 1.0, "1", 2**63):
                with self.subTest(offset=bad_offset):
                    with self.assertRaises(ValueError):
                        broker.stage_event(
                            "A",
                            bad_offset,
                            event_time=1,
                            **valid_stage,
                        )

            for bad_time in (-1, True, 1.0, "1", 2**63):
                with self.subTest(event_time=bad_time):
                    with self.assertRaises(ValueError):
                        broker.stage_event(
                            "A",
                            1,
                            event_time=bad_time,
                            **valid_stage,
                        )

            for value in (-1, True, 1.0, "1", 2**63):
                with self.subTest(checkpoint=value):
                    with self.assertRaises(ValueError):
                        broker.seal_source(
                            "A", through=value, watermark=0
                        )
                    with self.assertRaises(ValueError):
                        broker.seal_source(
                            "A", through=0, watermark=value
                        )


class FeedAcceptanceTest(unittest.TestCase):
    def test_out_of_order_seal_reopen_and_global_order(self):
        with TemporaryDirectory() as tmp:
            path = Path(tmp, "acceptance.db")
            broker = Broker(path)
            broker.configure_feed(("A", "B"))

            stage_kwargs = dict(key="asset", needs=None, payload=None)
            self.assertTrue(
                broker.stage_event("A", 2, event_time=6, **stage_kwargs)
            )
            self.assertTrue(
                broker.stage_event("A", 3, event_time=10, **stage_kwargs)
            )
            self.assertTrue(
                broker.stage_event("A", 4, event_time=30, **stage_kwargs)
            )

            # Unsealed retries are idempotent; identity/content conflicts fail.
            self.assertFalse(
                broker.stage_event("A", 2, event_time=6, **stage_kwargs)
            )
            with self.assertRaises(ValueError):
                broker.stage_event(
                    "A",
                    2,
                    event_time=7,
                    key="asset",
                    needs=None,
                    payload=None,
                )

            # Offset 1 is missing.
            with self.assertRaises(ValueError):
                broker.seal_source("A", through=2, watermark=6)

            self.assertTrue(
                broker.stage_event("A", 1, event_time=9, **stage_kwargs)
            )

            # A1@9 is not covered by watermark 6.
            with self.assertRaises(ValueError):
                broker.seal_source("A", through=2, watermark=6)

            # A3@10 is beyond through=2 and is not strictly after watermark 10.
            with self.assertRaises(ValueError):
                broker.seal_source("A", through=2, watermark=10)

            self.assertTrue(
                broker.seal_source("A", through=3, watermark=10)
            )
            self.assertFalse(
                broker.seal_source("A", through=3, watermark=10)
            )

            # Sealed-prefix retries never compare content or rebuild a job.
            self.assertFalse(
                broker.stage_event(
                    "A",
                    2,
                    event_time=10**9,
                    key="different",
                    needs=frozenset({"gpu"}),
                    payload="different",
                )
            )

            # Reopen: checkpoint is durable, but B's -1 watermark blocks all.
            broker = Broker(path)
            self.assertEqual(
                broker.source_checkpoints()["A"], (3, 10)
            )
            self.assertEqual(broker.publish_ready(limit=10), [])

            self.assertTrue(
                broker.stage_event("B", 2, event_time=10, **stage_kwargs)
            )
            self.assertTrue(
                broker.stage_event("B", 1, event_time=7, **stage_kwargs)
            )
            self.assertTrue(
                broker.seal_source("B", through=2, watermark=10)
            )

            first = broker.publish_ready(limit=2)
            # A committed half-batch survives reopening without source replay.
            broker = Broker(path)
            rest = broker.publish_ready(limit=3)
            published = first + rest

            self.assertEqual(
                [(partition, offset) for partition, offset, _ in published],
                [
                    ("A", 2),
                    ("B", 1),
                    ("A", 1),
                    ("A", 3),
                    ("B", 2),
                ],
            )
            self.assertEqual(
                [job_id for _, _, job_id in published],
                [1, 2, 3, 4, 5],
            )
            self.assertEqual(
                [job_id for _, _, job_id in first],
                [1, 2],
            )

            # The events are ordinary jobs in the original table. A4 remains
            # staged/unsealed, and the five published staging rows are gone.
            with sqlite3.connect(path) as db:
                jobs = db.execute(
                    "SELECT id, resource_key, needs_mask FROM jobs ORDER BY id"
                ).fetchall()
                self.assertEqual(jobs, [(i, "asset", 0) for i in range(1, 6)])
                self.assertEqual(
                    db.execute("SELECT COUNT(*) FROM archive_events").fetchone()[0],
                    1,
                )
                self.assertEqual(
                    db.execute(
                        "SELECT source_offset FROM archive_events"
                    ).fetchone()[0],
                    4,
                )

            # With the prior A prefix converted, A4 can seal even though no
            # worker has claimed or acked those five jobs.
            self.assertTrue(
                broker.seal_source("A", through=4, watermark=30)
            )
            self.assertTrue(
                broker.seal_source("B", through=2, watermark=30)
            )
            a4 = broker.publish_ready(limit=1)
            self.assertEqual(a4, [("A", 4, 6)])

    def test_ascii_partition_and_offset_tie_break(self):
        with TemporaryDirectory() as tmp:
            broker = Broker(Path(tmp, "ties.db"))
            broker.configure_feed(("a", "B", "A"))
            kwargs = dict(event_time=10, key="k", needs=None, payload=None)

            broker.stage_event("A", 2, **kwargs)
            broker.stage_event("A", 1, **kwargs)
            broker.stage_event("B", 1, **kwargs)
            broker.stage_event("a", 1, **kwargs)

            broker.seal_source("A", through=2, watermark=10)
            broker.seal_source("B", through=1, watermark=10)
            broker.seal_source("a", through=1, watermark=10)

            published = broker.publish_ready(limit=4)
            self.assertEqual(
                [(p, o) for p, o, _ in published],
                [("A", 1), ("A", 2), ("B", 1), ("a", 1)],
            )

    def test_none_and_empty_capabilities_are_same_content(self):
        with TemporaryDirectory() as tmp:
            broker = Broker(Path(tmp, "needs.db"))
            broker.configure_feed(("A",))
            kwargs = dict(
                event_time=1,
                key="k",
                payload=None,
            )
            self.assertTrue(
                broker.stage_event("A", 1, needs=None, **kwargs)
            )
            self.assertFalse(
                broker.stage_event(
                    "A", 1, needs=frozenset(), **kwargs
                )
            )

    def test_stage_offset_and_watermark_bounds(self):
        with TemporaryDirectory() as tmp:
            broker = Broker(Path(tmp, "bounds.db"))
            broker.configure_feed(("A",))
            kwargs = dict(key="k", needs=None, payload=None)

            self.assertTrue(
                broker.stage_event(
                    "A", 12_000, event_time=0, **kwargs
                )
            )
            with self.assertRaises(ValueError):
                broker.stage_event(
                    "A", 12_001, event_time=1, **kwargs
                )

            watermark_broker = Broker(Path(tmp, "watermark.db"))
            watermark_broker.configure_feed(("A",))
            watermark_broker.seal_source("A", through=0, watermark=5)
            self.assertTrue(
                watermark_broker.stage_event(
                    "A", 1, event_time=6, **kwargs
                )
            )
            self.assertFalse(
                watermark_broker.stage_event(
                    "A", 1, event_time=6, **kwargs
                )
            )
            with self.assertRaises(ValueError):
                watermark_broker.stage_event(
                    "A", 2, event_time=5, **kwargs
                )
            with self.assertRaises(ValueError):
                watermark_broker.stage_event(
                    "A", 2, event_time=4, **kwargs
                )


class FeedBufferAndConcurrencyTest(unittest.TestCase):
    def test_buffer_blocks_through_not_watermark_and_acks_irrelevant(self):
        with TemporaryDirectory() as tmp:
            path = Path(tmp, "buffer.db")
            broker = Broker(path)
            broker.configure_feed(("A", "B"))
            kwargs = dict(key="asset", needs=None, payload=None)

            broker.stage_event("A", 1, event_time=10, **kwargs)
            broker.stage_event("A", 2, event_time=20, **kwargs)
            broker.seal_source("A", through=2, watermark=20)
            self.assertTrue(
                broker.seal_source("B", through=0, watermark=20)
            )

            first = broker.publish_ready(limit=1)
            self.assertEqual(
                [(p, o) for p, o, _ in first], [("A", 1)]
            )

            # The first job is leased but never acknowledged.
            leased = broker.take_batch(
                {"worker": frozenset()}, now=0, lease=10_000
            )
            self.assertEqual(leased["worker"].id, first[0][2])

            # Watermark-only progress remains possible while A2 is sealed.
            self.assertTrue(
                broker.seal_source("A", through=2, watermark=21)
            )

            broker.stage_event("A", 3, event_time=30, **kwargs)
            with self.assertRaises(BufferError):
                broker.seal_source("A", through=3, watermark=30)
            self.assertEqual(
                broker.source_checkpoints()["A"], (2, 21)
            )

            # Publishing A2 removes the last staging row of the old prefix.
            second = broker.publish_ready(limit=1)
            self.assertEqual(
                [(p, o) for p, o, _ in second], [("A", 2)]
            )

            # No ack happened, yet the next checkpoint is now allowed.
            self.assertTrue(
                broker.seal_source("A", through=3, watermark=30)
            )
            self.assertEqual(
                broker.source_checkpoints()["A"], (3, 30)
            )

    def test_two_concurrent_brokers_partition_batch_without_duplicates(self):
        with TemporaryDirectory() as tmp:
            path = Path(tmp, "concurrent.db")
            setup = FastFeedBroker(path)
            setup.configure_feed(("A",))
            for offset in range(1, 11):
                setup.stage_event(
                    "A",
                    offset,
                    event_time=offset,
                    key="k",
                    needs=None,
                    payload=None,
                )
            setup.seal_source("A", through=10, watermark=10)

            broker1 = Broker(path)
            broker2 = Broker(path)
            barrier = threading.Barrier(2)

            def publish(broker):
                barrier.wait(timeout=10)
                return broker.publish_ready(limit=5)

            with ThreadPoolExecutor(max_workers=2) as pool:
                future1 = pool.submit(publish, broker1)
                future2 = pool.submit(publish, broker2)
                batch1 = future1.result(timeout=30)
                batch2 = future2.result(timeout=30)

            for batch in (batch1, batch2):
                ids = [job_id for _, _, job_id in batch]
                self.assertEqual(ids, sorted(ids))
                self.assertEqual(len(batch), 5)

            combined = batch1 + batch2
            self.assertEqual(len(combined), 10)
            self.assertEqual(
                len({(p, o) for p, o, _ in combined}), 10
            )

            by_id = sorted(combined, key=lambda item: item[2])
            self.assertEqual(
                [job_id for _, _, job_id in by_id],
                list(range(1, 11)),
            )
            self.assertEqual(
                [offset for _, offset, _ in by_id],
                list(range(1, 11)),
            )
            self.assertTrue(
                all(partition == "A" for partition, _, _ in by_id)
            )
            self.assertEqual(Broker(path).publish_ready(limit=10), [])


class FeedQueueIntegrationTest(unittest.TestCase):
    def test_key_fifo_capabilities_old_receipts_workflow_and_sinks(self):
        with TemporaryDirectory() as tmp:
            path = Path(tmp, "integration.db")
            broker = Broker(path)
            broker.register_sink("index", capacity=10)

            old_id = broker.put_ordered(
                "old", key="asset", needs=None
            )
            old_delivery = broker.take_batch(
                {"plain": frozenset()}, now=0, lease=100
            )["plain"]
            self.assertEqual(old_delivery.id, old_id)

            broker.configure_feed(("A",))
            published = []
            published.append(
                broker.stage_event(
                    "A",
                    1,
                    event_time=1,
                    key="asset",
                    needs=frozenset({"gpu"}),
                    payload="gpu-source",
                )
            )
            published.append(
                broker.stage_event(
                    "A",
                    2,
                    event_time=2,
                    key="asset",
                    needs=None,
                    payload="plain-source",
                )
            )
            self.assertTrue(all(published))
            broker.seal_source("A", through=2, watermark=2)
            source_jobs = broker.publish_ready(limit=10)
            self.assertEqual(
                [job_id for _, _, job_id in source_jobs],
                [old_id + 1, old_id + 2],
            )

            # The old lease is still unexpired at now=50 and blocks the key,
            # including a GPU slot that could run the first archive event.
            self.assertEqual(
                broker.take_batch(
                    {"plain": frozenset()}, now=50, lease=10
                ),
                {},
            )
            self.assertEqual(
                broker.take_batch(
                    {"gpu": frozenset({"gpu"})}, now=50, lease=10
                ),
                {},
            )

            # At now == expires the old receipt cannot acknowledge, but the
            # expired job can be reclaimed and then acknowledged.
            self.assertFalse(
                broker.acknowledge(old_delivery, now=100)
            )
            reclaimed = broker.take_batch(
                {"plain": frozenset()}, now=100, lease=100
            )["plain"]
            self.assertEqual(reclaimed.id, old_id)
            self.assertTrue(broker.acknowledge(reclaimed, now=101))

            # GPU source is key head; a label-less slot cannot skip it.
            self.assertEqual(
                broker.take_batch(
                    {"plain": frozenset()}, now=200, lease=10
                ),
                {},
            )
            gpu_delivery = broker.take_batch(
                {"gpu": frozenset({"gpu"})}, now=200, lease=10
            )["gpu"]
            self.assertEqual(gpu_delivery.id, old_id + 1)
            self.assertEqual(gpu_delivery.payload, "gpu-source")
            self.assertTrue(broker.acknowledge(gpu_delivery, now=201))

            plain_delivery = broker.take_batch(
                {"plain": frozenset()}, now=202, lease=10
            )["plain"]
            self.assertEqual(plain_delivery.id, old_id + 2)
            self.assertEqual(plain_delivery.payload, "plain-source")
            self.assertTrue(broker.acknowledge(plain_delivery, now=203))

            # A same-key workflow still produces its normal sink outcome, while
            # archived ordinary jobs produce no graph/outcome records.
            node = {
                "name": "finish",
                "key": "asset",
                "payload": "workflow",
                "needs": None,
                "requires": [],
                "after": [],
            }
            workflow_ids = broker.submit_graph(
                "feed-workflow", [node], sinks=("index",)
            )
            workflow_id = workflow_ids["finish"]
            delivery = broker.take_batch(
                {"plain": frozenset()}, now=300, lease=100
            )["plain"]
            self.assertEqual(delivery.id, workflow_id)
            self.assertTrue(broker.acknowledge(delivery, now=301))

            outcomes = broker.read_outcomes("index", limit=10)
            self.assertEqual(len(outcomes), 1)
            self.assertEqual(outcomes[0]["workflow_id"], "feed-workflow")
            self.assertEqual(outcomes[0]["job_id"], workflow_id)
            self.assertEqual(outcomes[0]["status"], "succeeded")

            source_ids = [job_id for _, _, job_id in source_jobs]
            with sqlite3.connect(path) as db:
                self.assertEqual(
                    db.execute(
                        "SELECT COUNT(*) FROM graph_nodes "
                        "WHERE job_id IN (?, ?)",
                        source_ids,
                    ).fetchone()[0],
                    0,
                )
                self.assertEqual(
                    db.execute(
                        "SELECT COUNT(*) FROM graph_node_sinks "
                        "WHERE node_id IN (?, ?)",
                        source_ids,
                    ).fetchone()[0],
                    0,
                )


class FeedBacklogPerformanceTest(unittest.TestCase):
    def test_extremely_short_ready_frontier_in_large_backlog(self):
        with TemporaryDirectory() as tmp:
            path = Path(tmp, "backlog.db")
            broker = FastFeedBroker(path)
            high_partitions = tuple(f"high{i}" for i in range(7))
            broker.configure_feed(high_partitions + ("single",))

            big_payload = "x" * 16 * 1024
            # Bulk setup is intentionally direct SQL: the performance target is
            # publish_ready(), while stage_event() behavior is covered above.
            with sqlite3.connect(path) as raw:
                raw.execute("PRAGMA synchronous=OFF")
                for partition_index, partition in enumerate(high_partitions):
                    rows = []
                    for offset in range(1, 12_001):
                        if partition == "high6" and offset == 12_000:
                            event_time = 0
                            payload = "time-zero"
                        else:
                            event_time = 10 + offset
                            payload = (
                                big_payload
                                if partition_index == 0 and offset <= 1_024
                                else None
                            )
                        rows.append(
                            (
                                partition,
                                offset,
                                event_time,
                                f"key-{offset % 31}",
                                0,
                                payload,
                                0,
                            )
                        )
                    raw.executemany(
                        """INSERT INTO archive_events(
                               partition, source_offset, event_time,
                               resource_key, needs_mask, payload, sealed
                           ) VALUES (?, ?, ?, ?, ?, ?, ?)""",
                        rows,
                    )
                raw.commit()

            for partition in high_partitions:
                broker.seal_source(
                    partition, through=12_000, watermark=20_000
                )
            del rows, big_payload

            broker.stage_event(
                "single",
                1,
                event_time=10,
                key="single",
                needs=None,
                payload="time-ten",
            )
            broker.seal_source("single", through=1, watermark=10)

            # Reopen and instrument every connection used by each call.
            broker = ProgressFeedBroker(path)
            expected = [
                ("high6", 12_000, "time-zero"),
                ("single", 1, "time-ten"),
                None,
            ]

            tracemalloc.start()
            try:
                for expected_item in expected:
                    broker.progress_callbacks = 0
                    before_current, _ = tracemalloc.get_traced_memory()
                    tracemalloc.reset_peak()

                    published = broker.publish_ready(limit=1)

                    _, peak = tracemalloc.get_traced_memory()
                    self.assertLessEqual(
                        peak - before_current, 6 * 1024 * 1024
                    )
                    self.assertLessEqual(
                        broker.progress_callbacks, 150
                    )

                    if expected_item is None:
                        self.assertEqual(published, [])
                    else:
                        partition, offset, payload = expected_item
                        self.assertEqual(len(published), 1)
                        self.assertEqual(
                            published[0][0:2], (partition, offset)
                        )
                        with sqlite3.connect(path) as db:
                            job_payload = db.execute(
                                "SELECT payload FROM jobs WHERE id = ?",
                                (published[0][2],),
                            ).fetchone()[0]
                        self.assertEqual(job_payload, payload)
            finally:
                if tracemalloc.is_tracing():
                    tracemalloc.stop()


if __name__ == "__main__":
    unittest.main()

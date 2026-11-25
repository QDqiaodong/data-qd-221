from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from tempfile import TemporaryDirectory
import os
import sqlite3
import subprocess
import sys
import threading
import tracemalloc
import unittest

from broker import (
    CAPABILITIES,
    Broker,
    Delivery,
    MAX_GRAPH_NODES,
    MAX_SINKS,
)


class FastCommitBroker(Broker):
    """Test-only setup speedup; scheduling behavior and SQL are unchanged."""

    def _connect(self):
        connection = super()._connect()
        connection.execute("PRAGMA synchronous=OFF")
        return connection


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
                receipts = list(
                    pool.map(lambda _: broker.take(now=0, lease=5), range(4))
                )
            self.assertEqual(sum(item is not None for item in receipts), 1)


class OrderedBatchTest(unittest.TestCase):
    def setUp(self):
        self._tmp = TemporaryDirectory()
        self.path = Path(self._tmp.name, "ordered.db")
        self.broker = Broker(self.path)

    def tearDown(self):
        self._tmp.cleanup()

    def put_acceptance_jobs(self):
        one = self.broker.put_ordered("one", key="A", needs=None)
        two = self.broker.put_ordered(
            "two", key="B", needs=frozenset({"gpu"})
        )
        three = self.broker.put_ordered("three", key="B", needs=None)
        four = self.broker.put_ordered("four", key="C", needs=None)
        self.assertEqual((one, two, three, four), (1, 2, 3, 4))
        return one, two, three, four

    def test_maximum_count_beats_slot_name_preference(self):
        self.put_acceptance_jobs()
        workers = {
            "z-plain": frozenset(),
            "a-gpu": frozenset({"gpu"}),
        }

        batch = self.broker.take_batch(workers, now=0, lease=10)

        self.assertEqual(
            {slot: delivery.id for slot, delivery in batch.items()},
            {"z-plain": 1, "a-gpu": 2},
        )
        self.assertEqual(batch["z-plain"].payload, "one")
        self.assertEqual(batch["a-gpu"].payload, "two")

        # Reversed insertion order must not change the decision.  At now=10
        # the first leases have expired, so the same optimal set is leased
        # again with fresh receipts.
        reversed_workers = {
            "a-gpu": frozenset({"gpu"}),
            "z-plain": frozenset(),
        }
        reversed_batch = self.broker.take_batch(
            reversed_workers, now=10, lease=10
        )
        self.assertEqual(
            {slot: delivery.id for slot, delivery in reversed_batch.items()},
            {"z-plain": 1, "a-gpu": 2},
        )
        self.assertNotEqual(
            batch["a-gpu"].receipt, reversed_batch["a-gpu"].receipt
        )

    def test_final_tie_break_uses_sorted_slot_ids_in_task_order(self):
        for insertion_order in (
            ("a-low", "b-high"),
            ("b-high", "a-low"),
        ):
            with self.subTest(insertion_order=insertion_order):
                with TemporaryDirectory() as tmp:
                    broker = Broker(Path(tmp, "tie.db"))
                    broker.put_ordered("one", key="A", needs=None)
                    broker.put_ordered("two", key="B", needs=None)
                    workers = {
                        slot_id: frozenset() for slot_id in insertion_order
                    }

                    batch = broker.take_batch(workers, now=0, lease=1)

                    self.assertEqual(
                        {slot: delivery.id for slot, delivery in batch.items()},
                        {"a-low": 1, "b-high": 2},
                    )

    def test_incompatible_key_head_blocks_successor_but_not_other_keys(self):
        self.put_acceptance_jobs()

        plain_slots = {
            "plain-0": frozenset(),
            "plain-1": frozenset(),
        }
        batch = self.broker.take_batch(plain_slots, now=0, lease=10)

        self.assertEqual(
            sorted(delivery.id for delivery in batch.values()), [1, 4]
        )
        self.assertEqual(batch["plain-0"].id, 1)
        self.assertEqual(batch["plain-1"].id, 4)

        # Tasks 1 and 4 now have active leases.  Task 2 is an incompatible GPU
        # head for key B, so generic task 3 still cannot skip it.
        again = self.broker.take_batch(
            {"plain-2": frozenset()}, now=1, lease=10
        )
        self.assertEqual(again, {})

    def test_same_key_successor_waits_for_head_ack_after_expiry(self):
        head = self.broker.put_ordered("head", key="K", needs=None)
        child = self.broker.put_ordered("child", key="K", needs=None)
        workers = {"a": frozenset(), "b": frozenset()}

        first = self.broker.take_batch(workers, now=0, lease=10)
        self.assertEqual(
            {slot: delivery.id for slot, delivery in first.items()},
            {"a": head},
        )

        # At the expiry boundary the same head is re-leased, not the child.
        redelivered = self.broker.take_batch(workers, now=10, lease=5)
        self.assertEqual(
            {slot: delivery.id for slot, delivery in redelivered.items()},
            {"a": head},
        )
        self.assertNotEqual(
            first["a"].receipt, redelivered["a"].receipt
        )

        self.assertTrue(
            self.broker.acknowledge(redelivered["a"], now=10)
        )
        after_ack = self.broker.take_batch(workers, now=10, lease=5)
        self.assertEqual(
            {slot: delivery.id for slot, delivery in after_ack.items()},
            {"a": child},
        )

    def test_expiry_receipt_reopen_ack_and_original_take_order(self):
        self.put_acceptance_jobs()
        first = self.broker.take_batch(
            {
                "z-plain": frozenset(),
                "a-gpu": frozenset({"gpu"}),
            },
            now=0,
            lease=10,
        )
        first_one = first["z-plain"]
        first_gpu = first["a-gpu"]

        # Other keys can use a plain slot while B's GPU head remains unacked.
        plain = self.broker.take_batch(
            {"plain": frozenset()}, now=1, lease=10
        )
        self.assertEqual(plain["plain"].id, 4)
        self.assertNotIn(3, (d.id for d in plain.values()))

        # Clear unrelated keys so the final original take has B's successor as
        # the earliest eligible head.
        self.assertTrue(self.broker.acknowledge(first_one, now=1))
        self.assertTrue(self.broker.acknowledge(plain["plain"], now=1))

        # now == expires is the expiry boundary and issues a fresh receipt for
        # the same GPU head; key B still has not advanced.
        reclaimed = self.broker.take_batch(
            {"a-gpu": frozenset({"gpu"})}, now=10, lease=7
        )
        new_gpu = reclaimed["a-gpu"]
        self.assertEqual(new_gpu.id, 2)
        self.assertNotEqual(new_gpu.receipt, first_gpu.receipt)

        reopened = Broker(self.path)
        self.assertFalse(reopened.acknowledge(first_gpu, now=10))
        self.assertTrue(reopened.acknowledge(new_gpu, now=10))

        successor = reopened.take(now=10, lease=5)
        self.assertEqual(successor.id, 3)
        self.assertEqual(successor.payload, "three")

    def test_original_take_has_no_capabilities_and_respects_key_order(self):
        self.broker.put_ordered(
            "gpu-head", key="B", needs=frozenset({"gpu"})
        )
        self.broker.put_ordered("generic-child", key="B", needs=None)
        self.broker.put_ordered("other-key", key="C", needs=None)

        other = self.broker.take(now=0, lease=5)
        self.assertEqual(other.id, 3)
        self.assertIsNone(self.broker.take(now=0, lease=5))

        gpu_batch = self.broker.take_batch(
            {"gpu": frozenset({"gpu"})}, now=0, lease=5
        )
        self.assertEqual(gpu_batch["gpu"].id, 1)
        self.assertTrue(self.broker.acknowledge(gpu_batch["gpu"], now=1))

        child = self.broker.take(now=1, lease=5)
        self.assertEqual(child.id, 2)

    def test_unkeyed_legacy_jobs_compete_independently(self):
        first_id = self.broker.put("legacy-one")
        second_id = self.broker.put("legacy-two")

        first = self.broker.take(now=0, lease=10)
        self.assertEqual(first.id, first_id)

        # The active first job does not block the unrelated unkeyed job.
        batch = self.broker.take_batch(
            {"plain": frozenset()}, now=0, lease=10
        )
        self.assertEqual(batch["plain"].id, second_id)

    def test_case_sensitive_keys_are_independent(self):
        self.broker.put_ordered("upper", key="Key", needs=None)
        self.broker.put_ordered("lower", key="key", needs=None)
        first = self.broker.take_batch(
            {"w": frozenset()}, now=0, lease=10
        )
        self.assertEqual(first["w"].payload, "upper")
        second = self.broker.take_batch(
            {"w": frozenset()}, now=0, lease=10
        )
        self.assertEqual(second["w"].payload, "lower")

    def test_full_capability_set(self):
        all_caps = frozenset(CAPABILITIES)
        self.broker.put_ordered("special", key="k", needs=all_caps)

        self.assertEqual(
            self.broker.take_batch(
                {"plain": frozenset()}, now=0, lease=1
            ),
            {},
        )
        batch = self.broker.take_batch(
            {"special-slot": all_caps}, now=0, lease=1
        )
        self.assertEqual(batch["special-slot"].payload, "special")

    def test_none_payload_round_trips(self):
        job_id = self.broker.put_ordered(None, key="empty", needs=None)
        batch = self.broker.take_batch(
            {"slot": frozenset()}, now=0, lease=1
        )
        delivery = batch["slot"]
        self.assertEqual(delivery.id, job_id)
        self.assertIsNone(delivery.payload)
        self.assertTrue(self.broker.acknowledge(delivery, now=0))

    def test_invalid_arguments_raise_value_error_without_side_effects(self):
        legacy_id = self.broker.put("legacy")

        with sqlite3.connect(self.path) as db:
            before = db.execute(
                "SELECT id, payload, receipt, expires "
                "FROM jobs ORDER BY id"
            ).fetchall()

        invalid_puts = [
            lambda: self.broker.put_ordered(
                "x", key="bad key", needs=None
            ),
            lambda: self.broker.put_ordered(
                "x", key="x" * 65, needs=None
            ),
            lambda: self.broker.put_ordered(
                "x", key="dotted.key", needs=None
            ),
            lambda: self.broker.put_ordered(
                "x", key="non-ascii-é", needs=None
            ),
            lambda: self.broker.put_ordered("x", key=1, needs=None),
            lambda: self.broker.put_ordered(
                "x", key="k", needs={"gpu"}
            ),
            lambda: self.broker.put_ordered(
                "x", key="k", needs=["gpu"]
            ),
            lambda: self.broker.put_ordered(
                "x", key="k", needs=frozenset({"not-a-label"})
            ),
            lambda: self.broker.put_ordered(
                b"bytes", key="k", needs=None
            ),
            lambda: self.broker.put_ordered(
                "a" * (16 * 1024 + 1), key="k", needs=None
            ),
            lambda: self.broker.put_ordered(
                "é" * 8193, key="k", needs=None
            ),
            lambda: self.broker.put_ordered(
                "\udcff", key="k", needs=None
            ),
        ]
        for operation in invalid_puts:
            with self.subTest(operation=operation):
                with self.assertRaises(ValueError):
                    operation()

        invalid_batches = [
            lambda: self.broker.take_batch(
                {"bad slot": frozenset()}, now=0, lease=1
            ),
            lambda: self.broker.take_batch(
                {"x" * 65: frozenset()}, now=0, lease=1
            ),
            lambda: self.broker.take_batch(
                {"slot": set()}, now=0, lease=1
            ),
            lambda: self.broker.take_batch(
                {"slot": None}, now=0, lease=1
            ),
            lambda: self.broker.take_batch(
                {"slot": frozenset({"bogus"})}, now=0, lease=1
            ),
            lambda: self.broker.take_batch(
                [], now=0, lease=1
            ),
            lambda: self.broker.take_batch(
                {f"w{i}": frozenset() for i in range(25)},
                now=0,
                lease=1,
            ),
            lambda: self.broker.take_batch(
                {"w": frozenset()}, now=True, lease=1
            ),
            lambda: self.broker.take_batch(
                {"w": frozenset()}, now=0, lease=False
            ),
            lambda: self.broker.take_batch(
                {"w": frozenset()}, now=-1, lease=1
            ),
            lambda: self.broker.take_batch(
                {"w": frozenset()}, now=0, lease=0
            ),
            lambda: self.broker.take_batch(
                {"w": frozenset()},
                now=2**63 - 1,
                lease=1,
            ),
        ]
        for operation in invalid_batches:
            with self.subTest(operation=operation):
                with self.assertRaises(ValueError):
                    operation()

        # Empty/None worker sets are valid no-op rounds.
        self.assertEqual(self.broker.take_batch({}, now=0, lease=1), {})
        self.assertEqual(self.broker.take_batch(None, now=0, lease=1), {})

        with sqlite3.connect(self.path) as db:
            after = db.execute(
                "SELECT id, payload, receipt, expires "
                "FROM jobs ORDER BY id"
            ).fetchall()
        self.assertEqual(before, after)
        self.assertEqual(after, [(legacy_id, "legacy", None, 0)])

    def test_payload_boundary_accepts_exact_16_kib(self):
        payload = "é" * 8192  # 16,384 UTF-8 bytes, 8,192 characters.
        job_id = self.broker.put_ordered(payload, key="boundary", needs=None)
        batch = self.broker.take_batch(
            {"slot": frozenset()}, now=0, lease=1
        )
        self.assertEqual(batch["slot"].id, job_id)
        self.assertEqual(batch["slot"].payload, payload)

    def test_invalid_batch_is_atomic_and_can_be_retried(self):
        legacy_id = self.broker.put("retry-me")
        workers = {
            "good-slot": frozenset(),
            "bad slot": frozenset(),
        }

        with self.assertRaises(ValueError):
            self.broker.take_batch(workers, now=7, lease=3)

        with sqlite3.connect(self.path) as db:
            untouched = db.execute(
                "SELECT receipt, expires FROM jobs WHERE id = ?",
                (legacy_id,),
            ).fetchone()
        self.assertEqual(untouched, (None, 0))

        retry = self.broker.take_batch(
            {"good-slot": frozenset()}, now=7, lease=3
        )
        delivery = retry["good-slot"]
        self.assertEqual(delivery.id, legacy_id)
        self.assertTrue(self.broker.acknowledge(delivery, now=8))

    def test_two_file_backed_brokers_do_not_duplicate_or_pass_heads(self):
        broker_one = Broker(self.path)
        broker_two = Broker(self.path)
        key_count = 40
        expected_heads = set()
        for index in range(key_count):
            head = broker_one.put_ordered(
                f"head-{index}", key=f"key-{index:02d}", needs=None
            )
            broker_one.put_ordered(
                f"child-{index}", key=f"key-{index:02d}", needs=None
            )
            expected_heads.add(head)

        workers = {
            f"slot-{index:02d}": frozenset() for index in range(24)
        }
        barrier = threading.Barrier(2)

        def compete(broker):
            barrier.wait(timeout=10)
            return broker.take_batch(workers, now=0, lease=10)

        with ThreadPoolExecutor(max_workers=2) as pool:
            future_one = pool.submit(compete, broker_one)
            future_two = pool.submit(compete, broker_two)
            batch_one = future_one.result(timeout=30)
            batch_two = future_two.result(timeout=30)

        ids_one = {delivery.id for delivery in batch_one.values()}
        ids_two = {delivery.id for delivery in batch_two.values()}
        self.assertTrue(ids_one.isdisjoint(ids_two))
        self.assertEqual(ids_one | ids_two, expected_heads)

        child_ids = set(range(2, 2 * key_count + 1, 2))
        with sqlite3.connect(self.path) as db:
            leased_children = db.execute(
                "SELECT COUNT(*) FROM jobs "
                "WHERE id IN ({}) AND receipt IS NOT NULL".format(
                    ",".join("?" for _ in child_ids)
                ),
                tuple(child_ids),
            ).fetchone()[0]
        self.assertEqual(leased_children, 0)


class LegacyDatabaseMigrationTest(unittest.TestCase):
    def test_original_schema_opens_directly(self):
        with TemporaryDirectory() as tmp:
            path = Path(tmp, "legacy.db")
            db = sqlite3.connect(path)
            db.execute(
                """CREATE TABLE jobs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    payload TEXT NOT NULL,
                    receipt TEXT,
                    expires INTEGER NOT NULL DEFAULT 0
                )"""
            )
            db.execute(
                "INSERT INTO jobs(id, payload, receipt, expires) "
                "VALUES (?, ?, ?, ?)",
                (7, "active", "old-receipt", 100),
            )
            db.execute(
                "INSERT INTO jobs(payload, receipt, expires) "
                "VALUES (?, NULL, 0)",
                ("pending",),
            )
            db.commit()
            db.close()

            broker = Broker(path)

            with sqlite3.connect(path) as check:
                preserved = check.execute(
                    "SELECT id, payload, receipt, expires "
                    "FROM jobs WHERE id = 7"
                ).fetchone()
            self.assertEqual(
                preserved, (7, "active", "old-receipt", 100)
            )

            batch = broker.take_batch(
                {"plain": frozenset()}, now=0, lease=10
            )
            self.assertEqual(batch["plain"].id, 8)
            self.assertEqual(batch["plain"].payload, "pending")

            # The original receipt and expiry still work before expiry.
            self.assertTrue(
                broker.acknowledge(
                    Delivery(7, "active", "old-receipt"), now=99
                )
            )

            new_id = broker.put_ordered("new", key="new-key", needs=None)
            self.assertEqual(new_id, 9)

            # Rebuilding the old NOT NULL payload schema allows null payloads.
            null_id = broker.put_ordered(None, key="null-key", needs=None)
            self.assertEqual(null_id, 10)
            with sqlite3.connect(path) as check:
                null_payload = check.execute(
                    "SELECT payload FROM jobs WHERE id = ?", (null_id,)
                ).fetchone()[0]
            self.assertIsNone(null_payload)

            self.assertFalse(
                broker.acknowledge(
                    Delivery(7, "active", "old-receipt"), now=99
                )
            )


class MemoryAndScaleTest(unittest.TestCase):
    def test_first_batch_does_not_load_large_payloads(self):
        with TemporaryDirectory() as tmp:
            path = Path(tmp, "memory.db")
            broker = Broker(path)
            payload = "a" * 16 * 1024
            for index in range(1536):
                broker.put_ordered(
                    payload, key=f"resource-{index}", needs=None
                )

            workers = {
                f"worker-{index:02d}": frozenset() for index in range(24)
            }

            started_here = False
            if not tracemalloc.is_tracing():
                tracemalloc.start()
                started_here = True
            else:
                tracemalloc.reset_peak()
            before_current, _ = tracemalloc.get_traced_memory()

            batch = broker.take_batch(workers, now=0, lease=10)

            _, peak = tracemalloc.get_traced_memory()
            if started_here:
                tracemalloc.stop()

            try:
                self.assertEqual(len(batch), 24)
                deliveries = sorted(batch.values(), key=lambda d: d.id)
                self.assertEqual(
                    [delivery.id for delivery in deliveries],
                    list(range(1, 25)),
                )
                self.assertTrue(
                    all(delivery.payload == payload for delivery in deliveries)
                )
                self.assertLessEqual(
                    peak - before_current, 8 * 1024 * 1024
                )
            finally:
                if tracemalloc.is_tracing() and started_here:
                    tracemalloc.stop()

    def test_12000_unconfirmed_tasks_and_24_slots(self):
        class FastCommitBroker(Broker):
            # Test-only setup speedup; scheduling behavior and SQL are unchanged.
            def _connect(self):
                connection = super()._connect()
                connection.execute("PRAGMA synchronous=OFF")
                return connection

        with TemporaryDirectory() as tmp:
            path = Path(tmp, "scale.db")
            broker = FastCommitBroker(path)
            for index in range(12000):
                broker.put_ordered(
                    "small", key=f"key-{index:05d}", needs=None
                )

            with sqlite3.connect(path) as db:
                self.assertEqual(
                    db.execute("SELECT COUNT(*) FROM jobs").fetchone()[0],
                    12000,
                )

            workers = {
                f"worker-{index:02d}": frozenset() for index in range(24)
            }
            first = broker.take_batch(workers, now=0, lease=10)
            self.assertEqual(len(first), 24)
            self.assertEqual(
                sorted(delivery.id for delivery in first.values()),
                list(range(1, 25)),
            )

            # The first batch is leased, but other independent keys remain
            # available for the next 24-slot round.
            second = broker.take_batch(workers, now=0, lease=10)
            self.assertEqual(
                sorted(delivery.id for delivery in second.values()),
                list(range(25, 49)),
            )


def graph_node(
    name,
    *,
    key=None,
    payload="payload",
    needs=None,
    requires=(),
    after=(),
):
    return {
        "name": name,
        "key": key if key is not None else f"key-{name}",
        "payload": payload,
        "needs": needs,
        "requires": list(requires),
        "after": list(after),
    }


class SubmitGraphValidationTest(unittest.TestCase):
    def setUp(self):
        self._tmp = TemporaryDirectory()
        self.path = Path(self._tmp.name, "graph-validation.db")
        self.broker = Broker(self.path)

    def tearDown(self):
        self._tmp.cleanup()

    def test_forward_references_ids_and_atomic_workflow_reuse(self):
        nodes = [
            graph_node(
                "publish",
                key="asset",
                requires=["transcode"],
                after=["archive"],
            ),
            graph_node("transcode", key="media", needs=frozenset({"gpu"})),
            graph_node("archive", key="archive"),
        ]

        ids = self.broker.submit_graph("release-2026", nodes)

        self.assertEqual(list(ids), ["publish", "transcode", "archive"])
        self.assertEqual(set(ids.values()), {1, 2, 3})
        self.assertEqual(
            self.broker.graph_status("release-2026"),
            {
                "publish": "pending",
                "transcode": "pending",
                "archive": "pending",
            },
        )

        with self.assertRaises(ValueError):
            self.broker.submit_graph("release-2026", nodes)

    def test_same_key_forward_dependency_is_a_cycle_but_key_change_recovers(self):
        nodes = [
            graph_node("publish", key="same-key", requires=["transcode"]),
            graph_node("transcode", key="same-key"),
        ]

        with self.assertRaises(ValueError):
            self.broker.submit_graph("workflow", nodes)

        with sqlite3.connect(self.path) as db:
            self.assertEqual(db.execute("SELECT COUNT(*) FROM jobs").fetchone()[0], 0)
            self.assertEqual(
                db.execute(
                    "SELECT COUNT(*) FROM graph_workflows "
                    "WHERE workflow_id = 'workflow'"
                ).fetchone()[0],
                0,
            )

        nodes[0]["key"] = "publish-key"
        ids = self.broker.submit_graph("workflow", nodes)
        self.assertEqual(set(ids), {"publish", "transcode"})

        # The name remains occupied after every node reaches a terminal state.
        delivery = self.broker.take(now=0, lease=10)
        self.assertEqual(delivery.id, ids["transcode"])
        self.assertTrue(self.broker.acknowledge(delivery, now=1))
        delivery = self.broker.take(now=1, lease=10)
        self.assertEqual(delivery.id, ids["publish"])
        self.assertTrue(self.broker.acknowledge(delivery, now=2))
        with self.assertRaises(ValueError):
            self.broker.submit_graph("workflow", nodes)

    def test_invalid_graphs_and_cycles_have_no_partial_writes(self):
        legacy_id = self.broker.put("legacy")

        def snapshot():
            with sqlite3.connect(self.path) as db:
                return (
                    db.execute(
                        "SELECT id, payload, receipt, expires, resource_key, "
                        "needs_mask FROM jobs ORDER BY id"
                    ).fetchall(),
                    db.execute(
                        "SELECT workflow_id FROM graph_workflows ORDER BY rowid"
                    ).fetchall(),
                    db.execute(
                        "SELECT job_id, workflow_id, name, status "
                        "FROM graph_nodes ORDER BY job_id"
                    ).fetchall(),
                    db.execute(
                        "SELECT node_id, prerequisite_id, kind "
                        "FROM graph_dependencies ORDER BY rowid"
                    ).fetchall(),
                )

        before = snapshot()
        self.assertEqual(before[0], [(legacy_id, "legacy", None, 0, None, 0)])

        invalid_graphs = [
            lambda: [],
            lambda: None,
            lambda: [graph_node("ok")] * (MAX_GRAPH_NODES + 1),
            lambda: ["not-a-dict"],
            lambda: [graph_node("bad name")],
            lambda: [graph_node("dup"), graph_node("dup")],
            lambda: [graph_node("n", key="bad key")],
            lambda: [graph_node("n", payload=b"bytes")],
            lambda: [graph_node("n", payload="x" * (16 * 1024 + 1))],
            lambda: [graph_node("n", needs=["gpu"])],
            lambda: [graph_node("n", needs=frozenset({"bogus"}))],
            lambda: [graph_node("n", requires="transcode")],
            lambda: [graph_node("n", requires=["bad name"])],
            lambda: [
                graph_node("n", requires=["a", "a"]),
                graph_node("a"),
            ],
            lambda: [
                graph_node("n", requires=["a"], after=["a"]),
                graph_node("a"),
            ],
            lambda: [graph_node("n", requires=["missing"])],
            lambda: [graph_node("n", requires=["n"])],
            lambda: [
                graph_node("a", key="ka", requires=["b"]),
                graph_node("b", key="kb", after=["a"]),
            ],
            lambda: [
                graph_node("publish", key="same", requires=["transcode"]),
                graph_node("transcode", key="same"),
            ],
        ]

        for index, make_nodes in enumerate(invalid_graphs):
            with self.subTest(index=index):
                with self.assertRaises(ValueError):
                    self.broker.submit_graph(f"invalid-{index}", make_nodes())
                self.assertEqual(snapshot(), before)

    def test_used_workflow_rejection_does_not_write_second_group(self):
        nodes = [graph_node("only")]
        self.broker.submit_graph("once", nodes)
        with sqlite3.connect(self.path) as db:
            before_jobs = db.execute("SELECT * FROM jobs ORDER BY id").fetchall()
            before_deps = db.execute(
                "SELECT * FROM graph_dependencies ORDER BY rowid"
            ).fetchall()

        with self.assertRaises(ValueError):
            self.broker.submit_graph("once", [graph_node("second")])

        with sqlite3.connect(self.path) as db:
            after_jobs = db.execute("SELECT * FROM jobs ORDER BY id").fetchall()
            after_deps = db.execute(
                "SELECT * FROM graph_dependencies ORDER BY rowid"
            ).fetchall()
            workflow_count = db.execute(
                "SELECT COUNT(*) FROM graph_workflows"
            ).fetchone()[0]

        self.assertEqual(before_jobs, after_jobs)
        self.assertEqual(before_deps, after_deps)
        self.assertEqual(workflow_count, 1)

    def test_graph_status_name_errors(self):
        with self.assertRaises(ValueError):
            self.broker.graph_status("bad workflow")
        with self.assertRaises(KeyError):
            self.broker.graph_status("never-registered")


class GraphDeliveryTest(unittest.TestCase):
    def setUp(self):
        self._tmp = TemporaryDirectory()
        self.path = Path(self._tmp.name, "graph-delivery.db")
        self.broker = Broker(self.path)

    def tearDown(self):
        self._tmp.cleanup()

    def media_workflow_nodes(self):
        return [
            graph_node("transcode", key="transcode", payload="video"),
            graph_node(
                "publish",
                key="publish-key",
                payload="publish",
                requires=["transcode"],
            ),
            graph_node(
                "cleanup",
                key="cleanup",
                payload="clean",
                after=["publish"],
            ),
            graph_node(
                "audit",
                key="audit",
                payload="audit",
                requires=["transcode", "publish"],
            ),
        ]

    def test_requires_success_after_any_terminal_and_failure_skip(self):
        ids = self.broker.submit_graph("media", self.media_workflow_nodes())

        transcode = self.broker.take(now=0, lease=10)
        self.assertEqual(transcode.id, ids["transcode"])
        self.assertFalse(self.broker.fail(transcode, now=10))

        redelivered = self.broker.take(now=10, lease=20)
        self.assertEqual(redelivered.id, ids["transcode"])
        self.assertNotEqual(redelivered.receipt, transcode.receipt)
        self.assertFalse(self.broker.fail(transcode, now=11))
        self.assertTrue(self.broker.fail(redelivered, now=11))

        self.assertEqual(
            self.broker.graph_status("media"),
            {
                "transcode": "failed",
                "publish": "skipped",
                "cleanup": "pending",
                "audit": "skipped",
            },
        )

        cleanup = self.broker.take(now=11, lease=10)
        self.assertEqual(cleanup.id, ids["cleanup"])
        self.assertTrue(self.broker.acknowledge(cleanup, now=12))
        self.assertIsNone(self.broker.take(now=12, lease=10))

        # Terminal statuses and credentials survive reopening.
        reopened = Broker(self.path)
        self.assertEqual(reopened.graph_status("media")["cleanup"], "succeeded")
        self.assertFalse(reopened.acknowledge(cleanup, now=12))
        self.assertFalse(reopened.fail(redelivered, now=12))

    def test_fail_now_validation_and_ordinary_job_is_unchanged(self):
        ordinary_id = self.broker.put_ordered(
            "ordinary", key="ordinary-key", needs=None
        )
        delivery = self.broker.take(now=0, lease=10)

        invalid_now = [-1, True, False, 1.5, "1", 2**63]
        for now in invalid_now:
            with self.subTest(now=now):
                with self.assertRaises(ValueError):
                    self.broker.fail(delivery, now=now)

        with sqlite3.connect(self.path) as db:
            before = db.execute(
                "SELECT id, receipt, expires FROM jobs WHERE id = ?",
                (ordinary_id,),
            ).fetchone()

        self.assertFalse(self.broker.fail(delivery, now=1))

        with sqlite3.connect(self.path) as db:
            after = db.execute(
                "SELECT id, receipt, expires FROM jobs WHERE id = ?",
                (ordinary_id,),
            ).fetchone()
        self.assertEqual(before, after)
        self.assertTrue(self.broker.acknowledge(delivery, now=2))

    def test_skipped_node_does_not_pass_old_same_key_head(self):
        old_head = self.broker.put_ordered("old", key="asset", needs=None)
        old_delivery = self.broker.take(now=0, lease=1000)
        self.assertEqual(old_delivery.id, old_head)

        ids = self.broker.submit_graph(
            "wf",
            [
                graph_node("transcode", key="transcode"),
                graph_node(
                    "publish",
                    key="asset",
                    requires=["transcode"],
                ),
            ],
        )
        successor = self.broker.put_ordered(
            "ordinary-successor", key="asset", needs=None
        )

        transcode = self.broker.take(now=0, lease=10)
        self.assertEqual(transcode.id, ids["transcode"])
        self.assertTrue(self.broker.fail(transcode, now=1))
        self.assertEqual(self.broker.graph_status("wf")["publish"], "skipped")

        # The still-leased old head has a larger effective block than the
        # skipped graph node, so the ordinary successor cannot be claimed.
        self.assertEqual(
            self.broker.take_batch(
                {"plain": frozenset()}, now=2, lease=10
            ),
            {},
        )
        self.assertTrue(self.broker.acknowledge(old_delivery, now=3))
        next_job = self.broker.take(now=3, lease=10)
        self.assertEqual(next_job.id, successor)

    def test_subprocess_failure_persists_for_new_process_cleanup(self):
        old_id = self.broker.put_ordered("old", key="asset", needs=None)
        old_delivery = self.broker.take(now=0, lease=1000)
        self.assertEqual(old_delivery.id, old_id)

        ids = self.broker.submit_graph("subprocess-media", self.media_workflow_nodes())
        successor = self.broker.put_ordered(
            "asset-successor", key="asset", needs=None
        )

        first = self.broker.take(now=0, lease=10)
        self.assertEqual(first.id, ids["transcode"])
        self.assertFalse(self.broker.fail(first, now=10))
        current = self.broker.take(now=10, lease=1000)
        self.assertEqual(current.id, ids["transcode"])

        project_root = Path(__file__).resolve().parent
        child_code = """
import os
import sys
sys.path.insert(0, os.environ["BROKER_PROJECT_ROOT"])
from broker import Broker, Delivery

broker = Broker(os.environ["BROKER_PATH"])
delivery = Delivery(
    int(os.environ["DELIVERY_ID"]),
    os.environ["DELIVERY_PAYLOAD"],
    os.environ["DELIVERY_RECEIPT"],
)
succeeded = broker.fail(delivery, now=11)
raise SystemExit(0 if succeeded else 3)
"""
        env = dict(
            os.environ,
            BROKER_PROJECT_ROOT=str(project_root),
            BROKER_PATH=str(self.path),
            DELIVERY_ID=str(current.id),
            DELIVERY_PAYLOAD=current.payload or "",
            DELIVERY_RECEIPT=current.receipt,
        )
        completed = subprocess.run(
            [sys.executable, "-c", child_code],
            env=env,
            capture_output=True,
            text=True,
            timeout=30,
        )
        self.assertEqual(completed.returncode, 0, completed.stderr)

        reopened = Broker(self.path)
        status = reopened.graph_status("subprocess-media")
        self.assertEqual(status["transcode"], "failed")
        self.assertEqual(status["publish"], "skipped")
        self.assertEqual(status["audit"], "skipped")
        self.assertEqual(status["cleanup"], "pending")

        cleanup = reopened.take(now=11, lease=10)
        self.assertEqual(cleanup.id, ids["cleanup"])
        self.assertTrue(reopened.acknowledge(cleanup, now=12))

        # Cleanup has another key; the asset key still waits for its old head.
        self.assertEqual(
            reopened.take_batch({"plain": frozenset()}, now=12, lease=10),
            {},
        )
        self.assertTrue(reopened.acknowledge(old_delivery, now=13))
        asset_next = reopened.take(now=13, lease=10)
        self.assertEqual(asset_next.id, successor)

    def test_forward_dependency_batch_keeps_key_order_and_assignment(self):
        self.broker.submit_graph(
            "batch-graph",
            [
                graph_node(
                    "publish",
                    key="asset",
                    requires=["transcode"],
                ),
                graph_node("asset-child", key="asset"),
                graph_node(
                    "transcode",
                    key="transcode",
                    needs=frozenset({"gpu"}),
                ),
            ],
        )
        generic_id = self.broker.put_ordered(
            "generic", key="other", needs=None
        )
        gpu_id = self.broker.put_ordered(
            "gpu-job", key="gpu-key", needs=frozenset({"gpu"})
        )

        workers = {
            "plain-0": frozenset(),
            "plain-1": frozenset(),
            "gpu-0": frozenset({"gpu"}),
            "gpu-1": frozenset({"gpu"}),
        }
        batch = self.broker.take_batch(workers, now=0, lease=10)
        ids_by_slot = {slot: delivery.id for slot, delivery in batch.items()}

        # publish=1 and asset-child=2 are absent; transcode=3, generic=4 and
        # the independent GPU job=5 are all assigned compatibly.
        self.assertEqual(
            ids_by_slot,
            {
                "gpu-0": 3,
                "gpu-1": gpu_id,
                "plain-0": generic_id,
            },
        )

    def test_concurrent_success_and_failure_have_one_winner(self):
        ids = self.broker.submit_graph(
            "race",
            [
                graph_node("root", key="root"),
                graph_node(
                    "child",
                    key="child",
                    requires=["root"],
                ),
            ],
        )
        delivery = self.broker.take(now=0, lease=1000)
        barrier = threading.Barrier(2)

        def acknowledge():
            broker = Broker(self.path)
            barrier.wait(timeout=10)
            return broker.acknowledge(delivery, now=1)

        def fail():
            broker = Broker(self.path)
            barrier.wait(timeout=10)
            return broker.fail(delivery, now=1)

        with ThreadPoolExecutor(max_workers=2) as pool:
            ack_result = pool.submit(acknowledge)
            fail_result = pool.submit(fail)
            acknowledged = ack_result.result(timeout=30)
            failed = fail_result.result(timeout=30)

        self.assertEqual(int(acknowledged) + int(failed), 1)
        reopened = Broker(self.path)
        status = reopened.graph_status("race")
        if acknowledged:
            self.assertTrue(acknowledged)
            self.assertFalse(failed)
            self.assertEqual(status["root"], "succeeded")
            self.assertEqual(status["child"], "pending")
            child = reopened.take(now=1, lease=10)
            self.assertEqual(child.id, ids["child"])
        else:
            self.assertFalse(acknowledged)
            self.assertTrue(failed)
            self.assertEqual(status["root"], "failed")
            self.assertEqual(status["child"], "skipped")
            self.assertIsNone(reopened.take(now=1, lease=10))

    def test_sequential_terminal_races_reject_old_credentials(self):
        ids = self.broker.submit_graph(
            "both-orders",
            [
                graph_node("root", key="root"),
                graph_node("child", key="child", requires=["root"]),
            ],
        )

        success_delivery = self.broker.take(now=0, lease=100)
        self.assertTrue(self.broker.acknowledge(success_delivery, now=1))
        self.assertFalse(self.broker.fail(success_delivery, now=2))
        self.assertEqual(
            self.broker.graph_status("both-orders")["root"], "succeeded"
        )
        child = self.broker.take(now=2, lease=10)
        self.assertEqual(child.id, ids["child"])

        # A second workflow exercises fail-first ordering.
        ids = self.broker.submit_graph(
            "fail-first",
            [
                graph_node("root", key="root2"),
                graph_node("child", key="child2", requires=["root"]),
            ],
        )
        fail_delivery = self.broker.take(now=2, lease=100)
        self.assertTrue(self.broker.fail(fail_delivery, now=3))
        self.assertFalse(self.broker.acknowledge(fail_delivery, now=4))
        status = Broker(self.path).graph_status("fail-first")
        self.assertEqual(status["root"], "failed")
        self.assertEqual(status["child"], "skipped")


class GraphCompatibilityAndScaleTest(unittest.TestCase):
    def test_legacy_noninteger_expiry_high_sequence_and_graph_take(self):
        with TemporaryDirectory() as tmp:
            path = Path(tmp, "legacy-graph.db")
            db = sqlite3.connect(path)
            db.execute(
                """CREATE TABLE jobs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    payload TEXT NOT NULL,
                    receipt TEXT,
                    expires INTEGER NOT NULL DEFAULT 0
                )"""
            )
            db.execute(
                "INSERT INTO jobs(id, payload, receipt, expires) "
                "VALUES (7, 'before', 'receipt-before', 10.5)"
            )
            db.execute(
                "INSERT INTO jobs(id, payload, receipt, expires) "
                "VALUES (8, 'boundary', 'receipt-boundary', 10.5)"
            )
            db.execute(
                "INSERT INTO jobs(id, payload, receipt, expires) "
                "VALUES (9, 'pending', NULL, 0)"
            )
            db.execute(
                "UPDATE sqlite_sequence SET seq = 100 WHERE name = 'jobs'"
            )
            db.commit()
            db.close()

            broker = Broker(path)
            before_delivery = Delivery(7, "before", "receipt-before")
            boundary_delivery = Delivery(8, "boundary", "receipt-boundary")

            with sqlite3.connect(path) as check:
                preserved = check.execute(
                    "SELECT expires, typeof(expires) FROM jobs WHERE id = 7"
                ).fetchone()
            self.assertEqual(preserved, (10.5, "real"))

            # Before the non-integral expiry the original receipt works.
            self.assertTrue(broker.acknowledge(before_delivery, now=10))
            # now == expires is still the strict expiry boundary.
            self.assertFalse(broker.acknowledge(boundary_delivery, now=10.5))

            new_id = broker.put_ordered(
                "high-water", key="high-key", needs=None
            )
            self.assertEqual(new_id, 101)
            graph_ids = broker.submit_graph(
                "legacy-graph",
                [
                    graph_node("root", key="graph-root"),
                    graph_node(
                        "child",
                        key="graph-child",
                        requires=["root"],
                    ),
                ],
            )
            self.assertEqual(graph_ids, {"root": 102, "child": 103})

            workers = {"slot": frozenset()}
            first = broker.take_batch(workers, now=0, lease=10)
            self.assertEqual(first["slot"].id, 9)
            self.assertTrue(broker.acknowledge(first["slot"], now=1))

            high_water_job = broker.take(now=1, lease=10)
            self.assertEqual(high_water_job.id, 101)
            self.assertTrue(broker.acknowledge(high_water_job, now=2))

            root = broker.take(now=2, lease=10)
            self.assertEqual(root.id, 102)
            self.assertTrue(broker.acknowledge(root, now=3))
            child = broker.take(now=3, lease=10)
            self.assertEqual(child.id, 103)

    def test_graph_dependency_limit_is_enforced_atomically(self):
        with TemporaryDirectory() as tmp:
            path = Path(tmp, "dependency-limit.db")
            broker = Broker(path)

            def nodes(extra_edge):
                result = [
                    graph_node(f"prereq-{index:05d}", key=f"pkey-{index}")
                    for index in range(480)
                ]
                for dependent in range(100):
                    result.append(
                        graph_node(
                            f"dependent-{dependent}",
                            key=f"dkey-{dependent}",
                            requires=[f"prereq-{i:05d}" for i in range(480)],
                        )
                    )
                if extra_edge:
                    result.append(
                        graph_node(
                            "one-too-many",
                            key="one-too-many",
                            requires=["prereq-00000"],
                        )
                    )
                return result

            with self.assertRaises(ValueError):
                broker.submit_graph("too-many-deps", nodes(True))
            with sqlite3.connect(path) as db:
                self.assertEqual(db.execute("SELECT COUNT(*) FROM jobs").fetchone()[0], 0)
                self.assertEqual(
                    db.execute(
                        "SELECT COUNT(*) FROM graph_workflows"
                    ).fetchone()[0],
                    0,
                )

            broker.submit_graph("exact-deps", nodes(False))
            with sqlite3.connect(path) as db:
                self.assertEqual(
                    db.execute(
                        "SELECT COUNT(*) FROM graph_dependencies"
                    ).fetchone()[0],
                    48_000,
                )

    def test_12000_node_graph_releases_first_24_after_prerequisite(self):
        class FastCommitBroker(Broker):
            def _connect(self):
                connection = super()._connect()
                connection.execute("PRAGMA synchronous=OFF")
                return connection

        with TemporaryDirectory() as tmp:
            path = Path(tmp, "graph-scale.db")
            broker = FastCommitBroker(path)
            nodes = [graph_node("root", key="root", payload="small")]
            nodes.extend(
                graph_node(
                    f"node-{index:05d}",
                    key=f"key-{index:05d}",
                    payload="small",
                    requires=["root"],
                )
                for index in range(1, MAX_GRAPH_NODES)
            )

            ids = broker.submit_graph("large-graph", nodes)
            self.assertEqual(len(ids), MAX_GRAPH_NODES)

            workers = {
                f"worker-{index:02d}": frozenset() for index in range(24)
            }
            before_root = broker.take_batch(workers, now=0, lease=10)
            self.assertEqual(
                [delivery.id for delivery in before_root.values()],
                [ids["root"]],
            )
            self.assertTrue(
                broker.acknowledge(before_root[next(iter(before_root))], now=1)
            )

            first_batch = broker.take_batch(workers, now=1, lease=10)
            self.assertEqual(len(first_batch), 24)
            self.assertEqual(
                sorted(delivery.id for delivery in first_batch.values()),
                list(range(2, 26)),
            )

    def test_failure_batch_skip_does_not_load_payloads_in_python(self):
        with TemporaryDirectory() as tmp:
            path = Path(tmp, "graph-memory.db")
            broker = Broker(path)
            payload = "a" * 16 * 1024
            nodes = [graph_node("root", key="root", payload=payload)]
            nodes.extend(
                graph_node(
                    f"skip-{index:04d}",
                    key=f"skip-key-{index:04d}",
                    payload=payload,
                    requires=["root"],
                )
                for index in range(1534)
            )
            nodes.append(
                graph_node(
                    "cleanup",
                    key="cleanup-key",
                    payload=payload,
                    after=["root"],
                )
            )
            ids = broker.submit_graph("memory-graph", nodes)
            self.assertEqual(len(ids), 1536)

            root_delivery = broker.take(now=0, lease=100)
            self.assertEqual(root_delivery.id, ids["root"])

            started_here = False
            if not tracemalloc.is_tracing():
                tracemalloc.start()
                started_here = True
            else:
                tracemalloc.reset_peak()

            before_current, _ = tracemalloc.get_traced_memory()
            try:
                self.assertTrue(broker.fail(root_delivery, now=1))
                _, peak = tracemalloc.get_traced_memory()
                self.assertLessEqual(
                    peak - before_current, 8 * 1024 * 1024
                )
            finally:
                if started_here and tracemalloc.is_tracing():
                    tracemalloc.stop()

            status = broker.graph_status("memory-graph")
            self.assertEqual(status["root"], "failed")
            self.assertEqual(status["cleanup"], "pending")
            skipped = [name for name, value in status.items() if value == "skipped"]
            self.assertEqual(len(skipped), 1534)

            cleanup = broker.take(now=1, lease=10)
            self.assertEqual(cleanup.id, ids["cleanup"])
            self.assertTrue(broker.acknowledge(cleanup, now=2))


def sink_workflow_node(
    name,
    *,
    key=None,
    payload="payload",
    requires=(),
    after=(),
):
    return graph_node(
        name,
        key=key,
        payload=payload,
        requires=requires,
        after=after,
    )


class SinkRegistrationTest(unittest.TestCase):
    def setUp(self):
        self._tmp = TemporaryDirectory()
        self.path = Path(self._tmp.name, "sinks.db")
        self.broker = Broker(self.path)

    def tearDown(self):
        self._tmp.cleanup()

    def test_register_first_duplicate_and_conflicts(self):
        self.assertTrue(self.broker.register_sink("index", capacity=3))
        self.assertFalse(self.broker.register_sink("index", capacity=3))
        with self.assertRaises(ValueError):
            self.broker.register_sink("index", capacity=4)

        # Names stay case sensitive.
        self.assertTrue(self.broker.register_sink("Index", capacity=1))
        self.assertTrue(self.broker.register_sink("index-2", capacity=2))

    def test_capacity_boundaries_are_inclusive(self):
        self.assertTrue(self.broker.register_sink("low", capacity=1))
        self.assertTrue(
            self.broker.register_sink("high", capacity=12_000)
        )
        with self.assertRaises(ValueError):
            self.broker.register_sink("too-high", capacity=12_001)
        with self.assertRaises(ValueError):
            self.broker.register_sink("too-low", capacity=0)

    def test_invalid_registration_has_no_side_effects(self):
        invalid = [
            ("", 1),
            ("bad name", 1),
            ("x" * 65, 1),
            (1, 1),
            ("a", 0),
            ("a", 12_001),
            ("a", True),
            ("a", 1.5),
        ]
        for name, capacity in invalid:
            with self.subTest(name=name, capacity=capacity):
                with self.assertRaises(ValueError):
                    self.broker.register_sink(name, capacity=capacity)
        with sqlite3.connect(self.path) as db:
            self.assertEqual(
                db.execute("SELECT COUNT(*) FROM sink_registry").fetchone()[0],
                0,
            )

    def test_capacity_limit_and_no_unregister(self):
        for index in range(MAX_SINKS):
            self.assertTrue(
                self.broker.register_sink(f"s{index}", capacity=1)
            )
        with self.assertRaises(ValueError):
            self.broker.register_sink("one-more", capacity=1)
        # Re-registering an existing sink at the limit is still allowed.
        self.assertFalse(self.broker.register_sink("s0", capacity=1))

    def test_read_and_ack_validate_unknown_sink_and_arguments(self):
        self.broker.register_sink("index", capacity=2)

        for limit in (0, 129, True, 1.5, "1"):
            with self.subTest(limit=limit):
                with self.assertRaises(ValueError):
                    self.broker.read_outcomes("index", limit=limit)
        with self.assertRaises(ValueError):
            self.broker.read_outcomes("missing", limit=1)
        with self.assertRaises(ValueError):
            self.broker.read_outcomes("bad name", limit=1)

        self.assertEqual(self.broker.read_outcomes("index", limit=1), [])

        for through in (0, -1, 2**63, True, 1.5, "1"):
            with self.subTest(through=through):
                with self.assertRaises(ValueError):
                    self.broker.ack_outcomes("index", through=through)
        with self.assertRaises(ValueError):
            self.broker.ack_outcomes("missing", through=1)


class SinkSubmitTest(unittest.TestCase):
    def setUp(self):
        self._tmp = TemporaryDirectory()
        self.path = Path(self._tmp.name, "sink-submit.db")
        self.broker = Broker(self.path)

    def tearDown(self):
        self._tmp.cleanup()

    def snapshot(self):
        with sqlite3.connect(self.path) as db:
            return (
                db.execute("SELECT id, payload FROM jobs ORDER BY id").fetchall(),
                db.execute(
                    "SELECT workflow_id FROM graph_workflows ORDER BY rowid"
                ).fetchall(),
                db.execute(
                    "SELECT name, capacity, charged FROM sink_registry "
                    "ORDER BY name"
                ).fetchall(),
                db.execute(
                    "SELECT node_id, sink_name FROM graph_node_sinks "
                    "ORDER BY node_id, sink_name"
                ).fetchall(),
                db.execute(
                    "SELECT MAX(seq) FROM outcome_stream"
                ).fetchone()[0],
            )

    def test_reserves_strictly_and_rejects_full_before_writing(self):
        self.broker.register_sink("index", capacity=2)
        nodes = [sink_workflow_node("a"), sink_workflow_node("b")]

        before = self.snapshot()
        self.broker.submit_graph("w1", nodes, sinks=("index",))
        self.assertEqual(self.broker.read_outcomes("index", limit=1), [])

        # Exactly full: another node cannot be admitted.
        with self.assertRaises(BufferError):
            self.broker.submit_graph(
                "w2", [sink_workflow_node("c")], sinks=("index",)
            )
        with sqlite3.connect(self.path) as db:
            self.assertEqual(
                db.execute(
                    "SELECT charged FROM sink_registry WHERE name = 'index'"
                ).fetchone()[0],
                2,
            )
            self.assertEqual(
                db.execute(
                    "SELECT COUNT(*) FROM graph_workflows"
                ).fetchone()[0],
                1,
            )

    def test_buffer_error_leaves_no_workflow_or_job_ids(self):
        self.broker.register_sink("index", capacity=2)
        self.broker.put_ordered("legacy", key="legacy", needs=None)

        with self.assertRaises(BufferError):
            self.broker.submit_graph(
                "rejected",
                [
                    sink_workflow_node("a"),
                    sink_workflow_node("b"),
                    sink_workflow_node("c"),
                ],
                sinks=("index",),
            )

        # The workflow name and job IDs were never consumed.
        ids = self.broker.submit_graph(
            "accepted",
            [sink_workflow_node("a")],
            sinks=("index",),
        )
        self.assertEqual(ids, {"a": 2})
        self.assertNotIn("rejected", self.workflow_names())

    def workflow_names(self):
        with sqlite3.connect(self.path) as db:
            return {
                row[0]
                for row in db.execute(
                    "SELECT workflow_id FROM graph_workflows"
                ).fetchall()
            }

    def test_invalid_sinks_raise_value_error_even_when_full(self):
        self.broker.register_sink("index", capacity=1)
        self.broker.register_sink("cleanup", capacity=1)
        # Fill both sinks so only validation can explain the failure.
        self.broker.submit_graph(
            "filler", [sink_workflow_node("only")], sinks=("index",)
        )
        self.broker.submit_graph(
            "filler2", [sink_workflow_node("only2")], sinks=("cleanup",)
        )

        nodes = [sink_workflow_node("n1"), sink_workflow_node("n2")]
        for sinks, exc_type in [
            (("unknown",), ValueError),
            (("index", "index"), ValueError),
            (("index", "index", "cleanup"), ValueError),
            (["index"], ValueError),
            (("index",) * 33, ValueError),
            (("index",), BufferError),
        ]:
            with self.subTest(sinks=sinks):
                with self.assertRaises(exc_type):
                    self.broker.submit_graph("bad", nodes, sinks=sinks)

    def test_duplicate_workflow_name_wins_over_full_sink(self):
        self.broker.register_sink("index", capacity=1)
        self.broker.submit_graph(
            "once", [sink_workflow_node("a")], sinks=("index",)
        )
        with self.assertRaises(ValueError):
            self.broker.submit_graph(
                "once", [sink_workflow_node("b")], sinks=("index",)
            )

    def test_ordinary_and_sinkless_workflows_never_reserve(self):
        self.broker.register_sink("index", capacity=1)
        self.broker.submit_graph("plain", [sink_workflow_node("only")])
        self.broker.put_ordered("ordinary", key="ordinary", needs=None)

        # The sinkless workflow leaves the single reserved slot untouched.
        ids = self.broker.submit_graph(
            "with-sink", [sink_workflow_node("watched")], sinks=("index",)
        )
        self.assertEqual(ids, {"watched": 3})
        with sqlite3.connect(self.path) as db:
            self.assertEqual(
                db.execute(
                    "SELECT COUNT(*) FROM graph_node_sinks"
                ).fetchone()[0],
                1,
            )

    def test_two_instances_race_for_the_last_slot(self):
        self.broker.register_sink("index", capacity=1)
        barrier = threading.Barrier(2)

        def attempt(workflow_id):
            broker = Broker(self.path)
            barrier.wait(timeout=10)
            try:
                broker.submit_graph(
                    workflow_id,
                    [sink_workflow_node("only")],
                    sinks=("index",),
                )
                return True
            except BufferError:
                return False

        with ThreadPoolExecutor(max_workers=2) as pool:
            futures = [
                pool.submit(attempt, "first"),
                pool.submit(attempt, "second"),
            ]
            results = [future.result(timeout=30) for future in futures]

        self.assertEqual(sum(results), 1)
        winner, loser = (
            ("first", "second") if results[0] else ("second", "first")
        )

        # Release the slot, then reuse the loser's untouched workflow name.
        delivery = Broker(self.path).take(now=0, lease=10)
        self.assertTrue(Broker(self.path).acknowledge(delivery, now=1))
        outcome = Broker(self.path).read_outcomes("index", limit=1)[0]
        self.assertTrue(
            Broker(self.path).ack_outcomes("index", through=outcome["seq"])
        )
        Broker(self.path).submit_graph(
            loser, [sink_workflow_node("retry")], sinks=("index",)
        )


class OutcomeDeliveryTest(unittest.TestCase):
    def setUp(self):
        self._tmp = TemporaryDirectory()
        self.path = Path(self._tmp.name, "outcomes.db")
        self.broker = Broker(self.path)

    def tearDown(self):
        self._tmp.cleanup()

    def three_node_workflow(self):
        # transcode is declared after publish but publish requires it; cleanup
        # only needs publish to reach any terminal state.
        return [
            sink_workflow_node(
                "publish", key="asset", requires=["transcode"]
            ),
            sink_workflow_node("transcode", key="media"),
            sink_workflow_node("cleanup", key="cleanup", after=["publish"]),
        ]

    def test_failure_outcome_order_skip_propagation_and_cleanup(self):
        self.assertTrue(self.broker.register_sink("index", capacity=3))
        self.assertTrue(self.broker.register_sink("cleanup-sink", capacity=3))
        ids = self.broker.submit_graph(
            "wf", self.three_node_workflow(), sinks=("index", "cleanup-sink")
        )

        first = self.broker.take(now=0, lease=10)
        self.assertEqual(first.id, ids["transcode"])
        # Expiry boundary: old receipt has no valid claim.
        self.assertFalse(self.broker.fail(first, now=10))
        current = self.broker.take(now=10, lease=10)
        self.assertEqual(current.id, ids["transcode"])
        self.assertNotEqual(current.receipt, first.receipt)
        self.assertFalse(self.broker.fail(first, now=11))
        self.assertTrue(self.broker.fail(current, now=11))

        index_out = self.broker.read_outcomes("index", limit=10)
        cleanup_out = self.broker.read_outcomes("cleanup-sink", limit=10)
        self.assertEqual(index_out, cleanup_out)
        self.assertEqual(
            [(item["name"], item["status"]) for item in index_out],
            [("transcode", "failed"), ("publish", "skipped")],
        )
        # The failure transaction orders by production while sharing seqs.
        self.assertEqual([item["seq"] for item in index_out], [1, 2])

        # A full sink does not block a node already admitted.
        cleanup = self.broker.take(now=11, lease=10)
        self.assertEqual(cleanup.id, ids["cleanup"])
        self.assertTrue(self.broker.acknowledge(cleanup, now=12))
        third = self.broker.read_outcomes("index", limit=10)[2]
        self.assertEqual((third["name"], third["status"]), ("cleanup", "succeeded"))

        reopened = Broker(self.path)
        self.assertEqual(
            reopened.graph_status("wf"),
            {
                "publish": "skipped",
                "transcode": "failed",
                "cleanup": "succeeded",
            },
        )
        self.assertEqual(
            reopened.read_outcomes("index", limit=10), index_out + [third]
        )

    def test_repeated_or_stale_terminal_operations_emit_nothing(self):
        self.broker.register_sink("index", capacity=10)
        ids = self.broker.submit_graph(
            "wf",
            [sink_workflow_node("root"), sink_workflow_node("leaf", requires=["root"])],
            sinks=("index",),
        )
        delivery = self.broker.take(now=0, lease=100)
        self.assertTrue(self.broker.acknowledge(delivery, now=1))
        self.assertFalse(self.broker.fail(delivery, now=2))
        self.assertFalse(self.broker.acknowledge(delivery, now=2))
        out = self.broker.read_outcomes("index", limit=10)
        self.assertEqual(len(out), 1)
        self.assertEqual(out[0]["status"], "succeeded")

    def test_skip_does_not_propagate_through_after(self):
        self.broker.register_sink("index", capacity=10)
        ids = self.broker.submit_graph(
            "wf",
            [
                sink_workflow_node("root"),
                sink_workflow_node("cleanup", after=["root"]),
                sink_workflow_node("downstream", key="other", requires=["root"]),
            ],
            sinks=("index",),
        )
        root = self.broker.take(now=0, lease=10)
        self.assertEqual(root.id, ids["root"])
        self.assertTrue(self.broker.fail(root, now=1))
        out = self.broker.read_outcomes("index", limit=10)
        self.assertEqual(
            [(item["name"], item["status"]) for item in out],
            [("root", "failed"), ("downstream", "skipped")],
        )

        cleanup = self.broker.take(now=1, lease=10)
        self.assertEqual(cleanup.id, ids["cleanup"])
        self.assertTrue(self.broker.acknowledge(cleanup, now=2))
        out = self.broker.read_outcomes("index", limit=10)
        self.assertEqual(out[-1]["name"], "cleanup")

    def test_same_seq_for_all_sinks_not_enqueue_id_order(self):
        self.broker.register_sink("index", capacity=10)
        self.broker.register_sink("cleanup", capacity=10)
        ids = self.broker.submit_graph(
            "wf",
            [
                sink_workflow_node("later_id_but_depends", key="x", requires=["first"]),
                sink_workflow_node("first", key="first-key"),
            ],
            sinks=("index", "cleanup"),
        )
        first = self.broker.take(now=0, lease=10)
        self.assertEqual(first.id, ids["first"])
        self.assertTrue(self.broker.acknowledge(first, now=1))
        second = self.broker.take(now=1, lease=10)
        self.assertEqual(second.id, ids["later_id_but_depends"])
        self.assertTrue(self.broker.acknowledge(second, now=2))

        index_out = self.broker.read_outcomes("index", limit=10)
        cleanup_out = self.broker.read_outcomes("cleanup", limit=10)
        self.assertEqual([item["seq"] for item in index_out], [1, 2])
        self.assertEqual(index_out, cleanup_out)
        # seq order follows production, not the larger job ID.
        self.assertEqual(index_out[1]["job_id"], ids["later_id_but_depends"])


class OutcomeAcknowledgementTest(unittest.TestCase):
    def setUp(self):
        self._tmp = TemporaryDirectory()
        self.path = Path(self._tmp.name, "acks.db")
        self.broker = Broker(self.path)

    def tearDown(self):
        self._tmp.cleanup()

    def test_ack_boundary_must_belong_and_releases_actual_count(self):
        self.broker.register_sink("index", capacity=4)
        self.broker.register_sink("cleanup", capacity=4)
        self.broker.submit_graph(
            "shared",
            [sink_workflow_node(f"n{i}", key=f"k{i}") for i in range(3)],
            sinks=("index", "cleanup"),
        )
        for _ in range(3):
            delivery = self.broker.take(now=0, lease=10)
            self.assertTrue(self.broker.acknowledge(delivery, now=1))
        index_out = self.broker.read_outcomes("index", limit=10)
        cleanup_out = self.broker.read_outcomes("cleanup", limit=10)
        self.assertEqual(index_out, cleanup_out)

        # Ack the first two for index only.
        self.assertTrue(
            self.broker.ack_outcomes("index", through=index_out[1]["seq"])
        )
        self.assertFalse(
            self.broker.ack_outcomes("index", through=index_out[1]["seq"])
        )
        self.assertEqual(self.broker.read_outcomes("index", limit=10), index_out[2:])
        # Cleanup still sees the full, identical stream.
        self.assertEqual(
            self.broker.read_outcomes("cleanup", limit=10), cleanup_out
        )
        # Index's ack of the last shared seq is not a boundary for cleanup.
        with self.assertRaises(ValueError):
            self.broker.ack_outcomes(
                "cleanup", through=index_out[2]["seq"] + 100
            )

        # Sharing with cleanup is now impossible (3 reserved, capacity 4), but
        # an index-only workflow still fits; the rejected cleanup submit left
        # index's reservation untouched.
        with self.assertRaises(BufferError):
            self.broker.submit_graph(
                "needs-cleanup",
                [sink_workflow_node("p", key="p1"), sink_workflow_node("q", key="p2")],
                sinks=("cleanup",),
            )

        # Release enough capacity for a 1-node workflow to index only.
        self.broker.submit_graph(
            "index-only", [sink_workflow_node("solo")], sinks=("index",)
        )
        solo = self.broker.take(now=0, lease=10)
        self.assertTrue(self.broker.acknowledge(solo, now=1))
        self.assertEqual(
            self.broker.read_outcomes("index", limit=10)[-1]["name"], "solo"
        )

    def test_interleaved_exclusive_and_shared_prefix_ack(self):
        self.broker.register_sink("index", capacity=6)
        self.broker.register_sink("cleanup", capacity=6)

        counter = 0

        def run(nodes, sinks):
            nonlocal counter
            counter += 1
            self.broker.submit_graph(f"wf-{counter}", nodes, sinks=sinks)
            for _ in nodes:
                delivery = self.broker.take(now=0, lease=10)
                self.assertTrue(self.broker.acknowledge(delivery, now=1))

        run([sink_workflow_node("both-1", key="b1")], ("index", "cleanup"))
        run([sink_workflow_node("idx-1", key="i1")], ("index",))
        run([sink_workflow_node("both-2", key="b2")], ("index", "cleanup"))
        run([sink_workflow_node("cln-1", key="c1")], ("cleanup",))
        run([sink_workflow_node("iii", key="i3")], ("index",))

        index_out = self.broker.read_outcomes("index", limit=10)
        cleanup_out = self.broker.read_outcomes("cleanup", limit=10)
        # Index exclusive seqs interleave with the shared ones; cleanup sees
        # only its own exclusive + shared outcomes, with gaps in seq.
        self.assertEqual(
            [item["name"] for item in index_out],
            ["both-1", "idx-1", "both-2", "iii"],
        )
        self.assertEqual(
            [item["name"] for item in cleanup_out],
            ["both-1", "both-2", "cln-1"],
        )

        # Confirm a prefix spanning exclusive and shared outcomes for index.
        index_boundary = index_out[2]["seq"]
        self.assertTrue(self.broker.ack_outcomes("index", through=index_boundary))
        self.assertEqual(
            [item["name"] for item in self.broker.read_outcomes("index", limit=10)],
            ["iii"],
        )
        # Cleanup's retained prefix is measured against its own delivery
        # rows (three), not the numeric seq distance index skipped.
        self.assertEqual(
            [item["name"] for item in self.broker.read_outcomes("cleanup", limit=10)],
            ["both-1", "both-2", "cln-1"],
        )
        self.assertTrue(
            self.broker.ack_outcomes(
                "cleanup", through=cleanup_out[0]["seq"]
            )
        )
        self.assertEqual(
            [item["name"] for item in self.broker.read_outcomes("cleanup", limit=10)],
            ["both-2", "cln-1"],
        )
        # Repeat ack is a no-op.
        self.assertFalse(
            self.broker.ack_outcomes(
                "cleanup", through=cleanup_out[0]["seq"]
            )
        )
        # Exactly 5 slots free on index (capacity 6 - 1 retained "iii"), so a
        # 5-node workflow fits exactly and one more would overflow.
        self.broker.submit_graph(
            "fill-index",
            [sink_workflow_node(f"f{i}", key=f"f{i}") for i in range(5)],
            sinks=("index",),
        )
        with self.assertRaises(BufferError):
            self.broker.submit_graph(
                "overflow-index", [sink_workflow_node("x", key="x")],
                sinks=("index",),
            )

    def test_cross_sink_and_future_boundaries_rejected(self):
        self.broker.register_sink("a", capacity=3)
        self.broker.register_sink("b", capacity=3)
        self.broker.submit_graph(
            "wf", [sink_workflow_node("n", key="kn")], sinks=("a",)
        )
        delivery = self.broker.take(now=0, lease=10)
        self.assertTrue(self.broker.acknowledge(delivery, now=1))
        seq = self.broker.read_outcomes("a", limit=1)[0]["seq"]

        # "b" has no such seq, and even "a" rejects an unread future seq.
        with self.assertRaises(ValueError):
            self.broker.ack_outcomes("b", through=seq)
        with self.assertRaises(ValueError):
            self.broker.ack_outcomes("a", through=seq + 1)

    def test_capacity_release_uses_actual_outcomes_not_seq_difference(self):
        self.broker.register_sink("a", capacity=2)
        self.broker.submit_graph(
            "wf1", [sink_workflow_node("n1", key="k1")], sinks=("a",)
        )
        delivery = self.broker.take(now=0, lease=10)
        self.assertTrue(self.broker.acknowledge(delivery, now=1))

        # Only sink "b" consumes the next two outcomes, leaving a large gap
        # in "a"'s seq range that must not inflate "a"'s released count.
        self.broker.register_sink("b", capacity=5)
        self.broker.submit_graph(
            "wf2",
            [sink_workflow_node("m1", key="k2"), sink_workflow_node("m2", key="k3")],
            sinks=("b",),
        )
        for _ in range(2):
            delivery = self.broker.take(now=1, lease=10)
            self.assertTrue(self.broker.acknowledge(delivery, now=2))

        out = self.broker.read_outcomes("a", limit=1)
        self.assertEqual(len(out), 1)
        self.assertTrue(self.broker.ack_outcomes("a", through=out[0]["seq"]))

        # "a" released exactly its one outcome: it may now admit two nodes.
        self.broker.submit_graph(
            "wf3",
            [sink_workflow_node("p1", key="k4"), sink_workflow_node("p2", key="k5")],
            sinks=("a",),
        )
        with self.assertRaises(BufferError):
            self.broker.submit_graph(
                "wf4", [sink_workflow_node("q", key="k6")], sinks=("a",)
            )

    def test_acknowledged_stream_storage_is_reclaimed(self):
        self.broker.register_sink("index", capacity=5)
        self.broker.submit_graph(
            "wf",
            [sink_workflow_node(f"n{i}", key=f"k{i}") for i in range(4)],
            sinks=("index",),
        )
        for _ in range(4):
            delivery = self.broker.take(now=0, lease=10)
            self.assertTrue(self.broker.acknowledge(delivery, now=1))
        out = self.broker.read_outcomes("index", limit=10)
        self.assertEqual(len(out), 4)
        self.assertTrue(self.broker.ack_outcomes("index", through=out[-1]["seq"]))

        with sqlite3.connect(self.path) as db:
            self.assertEqual(
                db.execute("SELECT COUNT(*) FROM outcome_stream").fetchone()[0],
                0,
            )
            self.assertEqual(
                db.execute(
                    "SELECT COUNT(*) FROM outcome_deliveries"
                ).fetchone()[0],
                0,
            )
        # Terminal history remains intact.
        self.assertEqual(
            self.broker.graph_status("wf"),
            {f"n{i}": "succeeded" for i in range(4)},
        )
        self.assertEqual(self.broker.read_outcomes("index", limit=10), [])


class LegacySinkUpgradeTest(unittest.TestCase):
    def test_old_graph_database_upgrades_without_reemitting(self):
        with TemporaryDirectory() as tmp:
            path = Path(tmp, "legacy-sink.db")
            create_legacy_graph_database(path)

            broker = Broker(path)
            with sqlite3.connect(path) as db:
                columns = [
                    column[1]
                    for column in db.execute(
                        "PRAGMA table_info(graph_nodes)"
                    ).fetchall()
                ]
            self.assertIn("outcomes", columns)
            self.assertEqual(
                broker.graph_status("old-wf"),
                {"root": "succeeded", "pre": "pending", "succ": "pending"},
            )

            # The old receipt still works and produces no historical outcome.
            self.assertTrue(
                broker.acknowledge(Delivery(2, "pre", "r2"), now=10)
            )

            # New sinks can be registered later and new work delivered.
            self.assertTrue(broker.register_sink("index", capacity=3))
            ids = broker.submit_graph(
                "new-wf",
                [
                    sink_workflow_node("dep", key="dep-key"),
                    sink_workflow_node("sink-node", key="sink-key", requires=["dep"]),
                ],
                sinks=("index",),
            )
            self.assertEqual(
                broker.graph_status("new-wf"),
                {"dep": "pending", "sink-node": "pending"},
            )

            # Continue the old graph: succ releases after pre succeeds.
            succ = broker.take(now=10, lease=5)
            self.assertEqual(succ.id, 3)
            self.assertTrue(broker.acknowledge(succ, now=11))
            self.assertEqual(broker.read_outcomes("index", limit=5), [])

    def test_new_sink_workflow_shares_queue_without_changing_order(self):
        with TemporaryDirectory() as tmp:
            path = Path(tmp, "legacy-sink-order.db")
            create_legacy_graph_database(path)
            broker = Broker(path)
            broker.register_sink("index", capacity=5)

            # A sink-enabled prereq and cleanup on the same keys as ordinary
            # jobs must still honour explicit dependencies and key FIFO.
            ids = broker.submit_graph(
                "sink-wf",
                [
                    sink_workflow_node("pre", key="shared-key"),
                    sink_workflow_node(
                        "post", key="shared-key", requires=["pre"]
                    ),
                ],
                sinks=("index",),
            )
            ordinary = broker.put_ordered(
                "ordinary-after-post", key="shared-key", needs=None
            )

            # Release the legacy pending node first so its successor (id 3,
            # an unkeyed job) is the earliest eligible delivery.
            self.assertTrue(
                broker.acknowledge(Delivery(2, "pre", "r2"), now=10)
            )
            first = broker.take(now=10, lease=20)
            self.assertEqual(first.id, 3)
            self.assertTrue(broker.acknowledge(first, now=11))

            pre = broker.take(now=11, lease=10)
            self.assertEqual(pre.id, ids["pre"])
            self.assertTrue(broker.acknowledge(pre, now=12))

            # post is now dependency-ready, but the ordinary successor on the
            # same key is blocked by post, exactly as without sinks.
            post = broker.take(now=12, lease=10)
            self.assertEqual(post.id, ids["post"])
            self.assertTrue(broker.acknowledge(post, now=13))

            ordinary_delivery = broker.take(now=13, lease=10)
            self.assertEqual(ordinary_delivery.id, ordinary)
            self.assertTrue(broker.acknowledge(ordinary_delivery, now=14))

            self.assertEqual(
                [item["name"] for item in broker.read_outcomes("index", limit=5)],
                ["pre", "post"],
            )
            # Sinkless ordinary jobs never enter the outcome stream.
            self.assertEqual(len(broker.read_outcomes("index", limit=5)), 2)

            # Others still see the sink-enabled workflow's cleanup outcome
            # alongside the ordinary job, proving shared queue order holds.
            self.assertEqual(
                broker.graph_status("sink-wf"),
                {"pre": "succeeded", "post": "succeeded"},
            )

    def test_old_workflow_does_not_reemit_terminal_outcomes(self):
        with TemporaryDirectory() as tmp:
            path = Path(tmp, "legacy-no-reemit.db")
            create_legacy_graph_database(path)
            broker = Broker(path)
            broker.register_sink("index", capacity=5)

            # root already succeeded before upgrades: no outcome is emitted.
            ids = broker.submit_graph(
                "new-wf", [sink_workflow_node("n", key="kn")], sinks=("index",)
            )
            delivery = broker.take(now=0, lease=10)
            self.assertEqual(delivery.id, ids["n"])
            broker.acknowledge(delivery, now=1)
            self.assertEqual(
                [item["name"] for item in broker.read_outcomes("index", limit=5)],
                ["n"],
            )


def create_legacy_graph_database(path):
    """Create a pre-outcome database with graph tables and active jobs."""
    db = sqlite3.connect(path)
    db.execute(
        """CREATE TABLE jobs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            payload TEXT,
            receipt TEXT,
            expires INTEGER NOT NULL DEFAULT 0,
            resource_key TEXT,
            needs_mask INTEGER NOT NULL DEFAULT 0
        )"""
    )
    db.execute(
        "CREATE TABLE graph_workflows (workflow_id TEXT PRIMARY KEY)"
    )
    db.execute(
        """CREATE TABLE graph_nodes (
            job_id INTEGER PRIMARY KEY,
            workflow_id TEXT NOT NULL,
            name TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'pending'
                CHECK (status IN ('pending', 'succeeded', 'failed', 'skipped')),
            UNIQUE (workflow_id, name)
        )"""
    )
    db.execute(
        """CREATE TABLE graph_dependencies (
            node_id INTEGER NOT NULL,
            prerequisite_id INTEGER NOT NULL,
            kind TEXT NOT NULL CHECK (kind IN ('requires', 'after')),
            PRIMARY KEY (node_id, prerequisite_id)
        )"""
    )
    db.execute("INSERT INTO graph_workflows VALUES ('old-wf')")
    db.execute(
        "INSERT INTO jobs(id, payload, receipt, expires) VALUES (1, 'old-root', 'r1', 50)"
    )
    db.execute(
        "INSERT INTO graph_nodes VALUES (1, 'old-wf', 'root', 'succeeded')"
    )
    db.execute(
        "INSERT INTO jobs(id, payload, receipt, expires) VALUES (2, 'pre', 'r2', 100)"
    )
    db.execute(
        "INSERT INTO graph_nodes VALUES (2, 'old-wf', 'pre', 'pending')"
    )
    db.execute(
        "INSERT INTO jobs(id, payload, receipt, expires) VALUES (3, 'succ', 'r3', 0)"
    )
    db.execute(
        "INSERT INTO graph_nodes VALUES (3, 'old-wf', 'succ', 'pending')"
    )
    db.execute("INSERT INTO graph_dependencies VALUES (3, 2, 'requires')")
    db.execute("UPDATE sqlite_sequence SET seq = 3 WHERE name = 'jobs'")
    db.commit()
    db.close()


class OutcomeScaleMemoryTest(unittest.TestCase):
    def test_failure_skip_to_32_sinks_stays_small_and_paged(self):
        with TemporaryDirectory() as tmp:
            path = Path(tmp, "outcome-scale.db")
            broker = FastCommitBroker(path)
            sink_names = tuple(f"sink-{index:02d}" for index in range(MAX_SINKS))
            for name in sink_names:
                broker.register_sink(name, capacity=MAX_GRAPH_NODES)

            payload = "a" * 16 * 1024
            nodes = [graph_node("root", key="root", payload="small")]
            nodes.extend(
                graph_node(
                    f"node-{index:05d}",
                    key=f"key-{index:05d}",
                    payload=payload if index < 512 else "small",
                    requires=["root"],
                )
                for index in range(1, MAX_GRAPH_NODES)
            )
            ids = broker.submit_graph("large", nodes, sinks=sink_names)
            self.assertEqual(len(ids), MAX_GRAPH_NODES)

            root = broker.take(now=0, lease=100)
            self.assertEqual(root.id, ids["root"])

            started_here = False
            if not tracemalloc.is_tracing():
                tracemalloc.start()
                started_here = True
            else:
                tracemalloc.reset_peak()
            before_current, _ = tracemalloc.get_traced_memory()
            try:
                self.assertTrue(broker.fail(root, now=1))
                _, peak = tracemalloc.get_traced_memory()
                self.assertLessEqual(
                    peak - before_current, 8 * 1024 * 1024
                )
            finally:
                if started_here and tracemalloc.is_tracing():
                    tracemalloc.stop()

            status = broker.graph_status("large")
            self.assertEqual(status["root"], "failed")
            self.assertEqual(
                sum(1 for value in status.values() if value == "skipped"),
                MAX_GRAPH_NODES - 1,
            )

            first_sink = broker.read_outcomes(sink_names[0], limit=128)
            self.assertEqual(len(first_sink), 128)
            self.assertEqual(first_sink[0]["name"], "root")
            self.assertEqual(first_sink[0]["status"], "failed")
            self.assertEqual(first_sink[1]["status"], "skipped")

            other_sink = broker.read_outcomes(sink_names[1], limit=128)
            self.assertEqual(
                [item["seq"] for item in first_sink],
                [item["seq"] for item in other_sink],
            )

            # Drain one sink fully; the other can still reread from its head.
            page = first_sink
            while page:
                self.assertTrue(
                    broker.ack_outcomes(sink_names[0], through=page[-1]["seq"])
                )
                page = broker.read_outcomes(sink_names[0], limit=128)
            self.assertEqual(broker.read_outcomes(sink_names[0], limit=1), [])

            reread = broker.read_outcomes(sink_names[1], limit=1)
            self.assertEqual(reread[0]["seq"], first_sink[0]["seq"])
            self.assertEqual(reread[0]["name"], "root")

    def test_isolated_sink_read_is_cheap_after_large_other_stream(self):
        with TemporaryDirectory() as tmp:
            path = Path(tmp, "outcome-isolated.db")
            broker = FastCommitBroker(path)
            broker.register_sink("A", capacity=MAX_GRAPH_NODES)
            broker.register_sink("B", capacity=2)

            nodes = [graph_node("root", key="root", payload="small")]
            nodes.extend(
                graph_node(
                    f"node-{index:05d}",
                    key=f"key-{index:05d}",
                    payload="small",
                    requires=["root"],
                )
                for index in range(1, MAX_GRAPH_NODES)
            )
            broker.submit_graph("to-a", nodes, sinks=("A",))
            root = broker.take(now=0, lease=100)
            self.assertTrue(broker.fail(root, now=1))
            self.assertEqual(
                len(broker.read_outcomes("A", limit=128)), 128
            )

            ids = broker.submit_graph(
                "to-b", [graph_node("only", key="only")], sinks=("B",)
            )
            delivery = broker.take(now=1, lease=10)
            self.assertEqual(delivery.id, ids["only"])
            self.assertTrue(broker.acknowledge(delivery, now=2))

            started_here = False
            if not tracemalloc.is_tracing():
                tracemalloc.start()
                started_here = True
            else:
                tracemalloc.reset_peak()
            before_current, _ = tracemalloc.get_traced_memory()
            try:
                out = broker.read_outcomes("B", limit=1)
                _, peak = tracemalloc.get_traced_memory()
                self.assertLessEqual(peak - before_current, 1 * 1024 * 1024)
            finally:
                if started_here and tracemalloc.is_tracing():
                    tracemalloc.stop()

            self.assertEqual(len(out), 1)
            self.assertEqual(out[0]["name"], "only")
            self.assertEqual(out[0]["workflow_id"], "to-b")


if __name__ == "__main__":
    unittest.main()

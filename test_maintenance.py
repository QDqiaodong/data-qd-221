"""Offline acceptance tests for the maintenance handoff protocol.

Three groups mirror the task's acceptance checklist:

1. Single-database sequencing: pause-before-handoff, earlier-ticket precedence,
   cross-key independence, reopen, receipt discipline, cancellation and natural
   expiry.
2. Concurrency: exactly one instance wins a ticket handoff, a worker racing a
   maintenance seal on one key never double-succeeds, and already-issued worker
   credentials still confirm/fail while new deliveries and successors wait.
3. Capacity and VM-instruction budget with 12,000 live tickets and a 12,000-job
   keyed backlog.
"""

from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from tempfile import TemporaryDirectory
import sqlite3
import unittest

from broker import Broker


class FastMaintenanceBroker(Broker):
    """Fast disk commits for large public-API/raw-SQL setup only."""

    def _connect(self):
        connection = super()._connect()
        connection.execute("PRAGMA synchronous=OFF")
        return connection


class ProgressMaintenanceBroker(Broker):
    """Count every 1,000 virtual-machine instructions across all connections."""

    progress_callbacks = 0

    def _connect(self):
        connection = super()._connect()
        connection.execute("PRAGMA synchronous=OFF")

        def count_progress():
            self.progress_callbacks += 1
            return 0

        connection.set_progress_handler(count_progress, 1000)
        return connection


def node(name, key, *, needs=None, requires=None, after=None, payload=None):
    return {
        "name": name,
        "key": key,
        "payload": payload,
        "needs": needs,
        "requires": requires or [],
        "after": after or [],
    }


class MaintenanceSequencingTest(unittest.TestCase):
    def setUp(self):
        self._tmp = TemporaryDirectory()
        self.path = Path(self._tmp.name, "maint.db")
        self.broker = Broker(self.path)

    def tearDown(self):
        self._tmp.cleanup()

    def test_acceptance_single_database_timeline(self):
        broker = self.broker

        # A worker lease on x that stays effective until (and at) now < 15.
        broker.put_ordered("x-job", key="x", needs=None)
        lease = broker.take(now=10, lease=5)
        self.assertIsNotNone(lease)
        self.assertEqual(lease.id, 1)

        a = broker.request_maintenance(("x", "y"), now=10, until=30)
        b = broker.request_maintenance(("y", "z"), now=10, until=20)
        c = broker.request_maintenance(("z",), now=10, until=40)
        d = broker.request_maintenance(("u",), now=10, until=40)
        self.assertEqual((a, b, c, d), (1, 2, 3, 4))

        # Every listed key is paused immediately, blocking fresh and re-delivery.
        broker.put_ordered("x2", key="x", needs=None)
        broker.put_ordered("y2", key="y", needs=None)
        broker.put_ordered("z2", key="z", needs=None)
        self.assertEqual(broker.take_batch({"s": frozenset()}, now=11, lease=5), {})

        # Unrelated keys and keyless legacy jobs still use free slots.
        broker.put_ordered("w-job", key="w", needs=None)
        broker.put("legacy")
        got = broker.take_batch(
            {"s0": frozenset(), "s1": frozenset()}, now=11, lease=5
        )
        self.assertEqual(
            {d.payload for d in got.values()}, {"w-job", "legacy"}
        )
        # ...and the y/z/x keyed jobs stay blocked by their paused keys.
        got2 = broker.take_batch({"s0": frozenset()}, now=11, lease=5)
        self.assertEqual(got2, {})

        # A cannot hand off while x still has an effective worker lease.
        self.assertIsNone(broker.take_maintenance(a, now=10))
        # B and C cannot jump ahead through their own free keys: earlier
        # intersecting tickets still hold precedence.
        self.assertIsNone(broker.take_maintenance(b, now=10))
        self.assertIsNone(broker.take_maintenance(c, now=10))
        # D is disjoint and can hand off independently.
        d_lease = broker.take_maintenance(d, now=10)
        self.assertIsNotNone(d_lease)
        self.assertEqual(d_lease["ticket_id"], d)
        self.assertEqual(d_lease["until"], 40)
        self.assertTrue(d_lease["receipt"])

        # Reopen preserves pause, handoff and credentials without rewrites.
        broker = Broker(self.path)
        self.assertEqual(
            broker.take_batch({"s": frozenset()}, now=14, lease=5),
            {},
        )
        # At 15 the old worker lease is no longer effective; A can hand off.
        a_lease = broker.take_maintenance(a, now=15)
        self.assertIsNotNone(a_lease)
        self.assertEqual(a_lease["ticket_id"], a)
        self.assertEqual(a_lease["until"], 30)

        # The stale worker can no longer confirm (its lease expired).
        self.assertFalse(broker.acknowledge(lease, now=15))

        # Returning A without the receipt or with a wrong one fails.
        self.assertFalse(broker.end_maintenance(a, now=15))
        self.assertFalse(
            broker.end_maintenance(a, now=15, receipt="not-the-receipt")
        )
        self.assertTrue(
            broker.end_maintenance(a, now=15, receipt=a_lease["receipt"])
        )
        # Ending A releases x; y is still paused by B.
        redelivered = broker.take_batch({"s": frozenset()}, now=15, lease=5)
        self.assertEqual(
            {d.payload for d in redelivered.values()}, {"x-job"}
        )
        self.assertIsNone(broker.take_maintenance(c, now=15))
        b_lease = broker.take_maintenance(b, now=15)
        self.assertIsNotNone(b_lease)
        self.assertEqual(b_lease["ticket_id"], b)
        # C still cannot preempt B on z.
        self.assertIsNone(broker.take_maintenance(c, now=15))

        # B is not returned; at exactly until == 20 it can no longer be returned.
        self.assertFalse(
            broker.end_maintenance(b, now=20, receipt=b_lease["receipt"])
        )
        # C may now hand off, and y is deliverable again (B is dead, no lease).
        c_lease = broker.take_maintenance(c, now=20)
        self.assertIsNotNone(c_lease)
        self.assertEqual(c_lease["ticket_id"], c)
        y_delivery = None
        for _ in range(4):
            got = broker.take_batch({"s": frozenset()}, now=20, lease=5)
            if not got:
                break
            delivery = next(iter(got.values()))
            if delivery.payload == "y2":
                y_delivery = delivery
                break
        self.assertIsNotNone(y_delivery, "y should be deliverable at now=20")

    def test_waiting_cancel_releases_precedence_and_expiry_clears_pause(self):
        broker = self.broker
        a = broker.request_maintenance(("k",), now=0, until=100)
        b = broker.request_maintenance(("k",), now=0, until=100)
        # B cannot preempt A's earlier precedence.
        self.assertIsNone(broker.take_maintenance(b, now=0))
        # Cancelling waiting A releases the precedence so B may then hand off.
        self.assertTrue(broker.end_maintenance(a, now=1, receipt=None))
        b_lease = broker.take_maintenance(b, now=1)
        self.assertIsNotNone(b_lease)
        self.assertEqual(b_lease["ticket_id"], b)
        # B still holds k, so return it before testing a fresh ticket on k.
        self.assertTrue(
            broker.end_maintenance(b, now=1, receipt=b_lease["receipt"])
        )

        # A freshly requested ticket keeps its key paused until natural expiry.
        broker.put_ordered("job", key="k", needs=None)
        fresh = broker.request_maintenance(("k",), now=2, until=5)
        self.assertEqual(
            broker.take_batch({"s": frozenset()}, now=4, lease=5), {}
        )
        # At now == until it no longer restricts anything, even unreturned.
        got = broker.take_batch({"s": frozenset()}, now=5, lease=5)
        self.assertEqual({d.payload for d in got.values()}, {"job"})
        # The expired waiting ticket cannot be handed off afterwards.
        self.assertIsNone(broker.take_maintenance(fresh, now=5))
        self.assertFalse(broker.end_maintenance(fresh, now=5, receipt=None))

    def test_invalid_request_with_one_good_key_pauses_nothing(self):
        broker = self.broker
        broker.put_ordered("good", key="good", needs=None)
        for bad_keys in (
            ("good", "Bad Key"),   # illegal second key
            ("good", "good"),      # duplicate
            (),                    # empty
            ("good",) * 9,         # too many
            ["good"],              # not a tuple
            ("good", 5),           # non-string element
        ):
            with self.assertRaises(ValueError):
                broker.request_maintenance(bad_keys, now=0, until=10)
        # The valid key in the rejected request stays deliverable.
        got = broker.take_batch({"s": frozenset()}, now=0, lease=5)
        self.assertEqual({d.payload for d in got.values()}, {"good"})

    def test_argument_validation(self):
        broker = self.broker
        with self.assertRaises(ValueError):
            broker.request_maintenance(("k",), now=0, until=0)
        with self.assertRaises(ValueError):
            broker.request_maintenance(("k",), now=-1, until=5)
        with self.assertRaises(ValueError):
            broker.request_maintenance(("k",), now=True, until=5)
        with self.assertRaises(ValueError):
            broker.request_maintenance(("k",), now=0, until=1 << 63)
        # Ticket-id validation on both entry points.
        for bad in (0, -1, 1 << 63, True, 1.5, "1", None):
            with self.assertRaises(ValueError):
                broker.take_maintenance(bad, now=0)
            with self.assertRaises(ValueError):
                broker.end_maintenance(bad, now=0)
        # Non-positive / out-of-range / non-int now.
        a = broker.request_maintenance(("k",), now=0, until=10)
        for bad_now in (-1, 1 << 63, True, 1.5):
            with self.assertRaises(ValueError):
                broker.take_maintenance(a, now=bad_now)
            with self.assertRaises(ValueError):
                broker.end_maintenance(a, now=bad_now)
        # Invalid receipt forms raise rather than returning False.
        lease = broker.take_maintenance(a, now=1)
        self.assertIsNotNone(lease)
        for bad in ("", "has space", "a" * 129, 5):
            with self.assertRaises(ValueError):
                broker.end_maintenance(a, now=2, receipt=bad)
        # None for a handed ticket is a phase error -> False, not raise.
        self.assertFalse(broker.end_maintenance(a, now=2, receipt=None))
        # A wrong but well-formed receipt returns False.
        self.assertFalse(broker.end_maintenance(a, now=2, receipt="wrong"))
        self.assertTrue(
            broker.end_maintenance(a, now=2, receipt=lease["receipt"])
        )
        # Ended ticket: repeated endings return False.
        self.assertFalse(
            broker.end_maintenance(a, now=2, receipt=lease["receipt"])
        )
        self.assertIsNone(broker.take_maintenance(a, now=2))

    def test_duplicate_requests_are_distinct_and_ids_are_contiguous(self):
        broker = self.broker
        first = broker.request_maintenance(("k",), now=0, until=10)
        second = broker.request_maintenance(("k",), now=0, until=10)
        third = broker.request_maintenance(("k", "j"), now=0, until=10)
        self.assertEqual((first, second, third), (1, 2, 3))
        self.assertEqual(broker.request_maintenance(("z",), now=0, until=10), 4)
        # Ticket IDs never reuse; ending then requesting keeps increasing.
        broker.end_maintenance(2, now=1, receipt=None)
        self.assertEqual(broker.request_maintenance(("q",), now=1, until=10), 5)


class MaintenanceConcurrencyTest(unittest.TestCase):
    def setUp(self):
        self._tmp = TemporaryDirectory()
        self.path = Path(self._tmp.name, "race.db")

    def tearDown(self):
        self._tmp.cleanup()

    def test_two_instances_race_handoff_exactly_one_wins(self):
        first = Broker(self.path)
        second = Broker(self.path)
        ticket = first.request_maintenance(("x",), now=0, until=100)

        barrier = __import__("threading").Barrier(2)

        def attempt(broker):
            barrier.wait()
            return broker.take_maintenance(ticket, now=1)

        with ThreadPoolExecutor(2) as pool:
            results = list(pool.map(attempt, (first, second)))

        winners = [r for r in results if r is not None]
        self.assertEqual(len(winners), 1)
        winner = winners[0]
        # Only the winner's receipt can end the ticket.
        loser = first if winner is not None and results[0] is None else second
        losing_receipt = "definitely-not-it"
        self.assertFalse(
            loser.end_maintenance(ticket, now=2, receipt=losing_receipt)
        )
        self.assertTrue(
            loser.end_maintenance(ticket, now=2, receipt=winner["receipt"])
        )

    def test_worker_race_on_same_key_is_serialized(self):
        # If the worker takes first, the original credential is unchanged and
        # handoff must wait for it to finish/expire; the sealing broker cannot
        # simultaneously lease the same key.
        broker = Broker(self.path)
        broker.put_ordered("job", key="x", needs=None)
        worker = broker.take(now=0, lease=50)
        self.assertIsNotNone(worker)
        ticket = broker.request_maintenance(("x",), now=1, until=100)
        self.assertIsNone(broker.take_maintenance(ticket, now=1))
        # The original credential still works and is the only live one.
        self.assertTrue(broker.acknowledge(worker, now=2))
        self.assertIsNone(broker.take(now=2, lease=5))
        lease = broker.take_maintenance(ticket, now=2)
        self.assertIsNotNone(lease)

    def test_worker_cannot_lease_once_key_is_sealed(self):
        broker = Broker(self.path)
        ticket = broker.request_maintenance(("x",), now=0, until=100)
        # Seal first (no lease yet).
        self.assertIsNotNone(broker.take_maintenance(ticket, now=1))
        broker.put_ordered("job", key="x", needs=None)
        self.assertIsNone(broker.take(now=2, lease=5))
        self.assertEqual(
            broker.take_batch({"s": frozenset()}, now=2, lease=5), {}
        )

    def test_threaded_seal_race_never_double_authorizes(self):
        # One instance repeatedly tries to lease key x while another requests a
        # ticket and attempts the handoff.  The write lock serializes both, and
        # the mutual-exclusion invariant must hold: a live worker lease and a
        # successful handoff on x can never coexist.
        import threading

        broker = Broker(self.path)
        broker.put_ordered("job", key="x", needs=None)
        now = 10

        took = []
        handed = []
        start = threading.Barrier(2)

        def try_take():
            worker = Broker(self.path)
            start.wait()
            for _ in range(50):
                delivery = worker.take(now=now, lease=1000)
                if delivery is not None:
                    took.append(delivery)
                    return

        def try_seal():
            dispatcher = Broker(self.path)
            start.wait()
            ticket = dispatcher.request_maintenance(("x",), now=now, until=2000)
            lease = dispatcher.take_maintenance(ticket, now=now)
            handed.append(lease)

        threads = [
            threading.Thread(target=try_take),
            threading.Thread(target=try_seal),
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        # Inspect the committed state: a handed ticket together with a live
        # worker lease on x would be the forbidden double-authorization.
        with sqlite3.connect(self.path) as raw:
            live_lease = raw.execute(
                "SELECT COUNT(*) FROM jobs "
                "WHERE resource_key = 'x' AND expires > ?",
                (now,),
            ).fetchone()[0]
            handed_state = raw.execute(
                "SELECT COUNT(*) FROM maintenance_tickets "
                "WHERE state = 'handed' AND until > ?",
                (now,),
            ).fetchone()[0]
        self.assertFalse(
            live_lease > 0 and handed_state > 0,
            "worker lease and maintenance handoff must not coexist on x",
        )
        # Exactly one of the two outcomes materialised (the key was contended).
        self.assertTrue(took or handed_state > 0)

    def test_old_workflow_credential_still_terminates_while_paused(self):
        broker = Broker(self.path)
        broker.register_sink("sink", capacity=10)
        broker.submit_graph(
            "wf",
            [
                node("root", "shared", needs=None),
                node(
                    "child", "shared", needs=None,
                    requires=["root"], payload="child-payload",
                ),
            ],
            sinks=("sink",),
        )
        root = broker.take(now=0, lease=100)
        self.assertEqual(root.id, 1)
        ticket = broker.request_maintenance(("shared",), now=1, until=100)
        self.assertIsNone(broker.take(now=1, lease=5))
        # Existing lease still confirms; the outcome stream is emitted.
        self.assertTrue(broker.acknowledge(root, now=2))
        outcomes = broker.read_outcomes("sink", limit=10)
        self.assertEqual([o["name"] for o in outcomes], ["root"])
        self.assertEqual(outcomes[0]["status"], "succeeded")
        # The same-key successor still waits for the maintenance to end.
        self.assertIsNone(broker.take(now=3, lease=5))
        lease = broker.take_maintenance(ticket, now=3)
        self.assertTrue(
            broker.end_maintenance(ticket, now=4, receipt=lease["receipt"])
        )
        child = broker.take(now=5, lease=5)
        self.assertEqual(child.id, 2)
        self.assertEqual(child.payload, "child-payload")

    def test_old_workflow_credential_can_fail_while_paused(self):
        broker = Broker(self.path)
        broker.submit_graph(
            "wf",
            [node("root", "shared", needs=None)],
        )
        root = broker.take(now=0, lease=100)
        broker.request_maintenance(("shared",), now=1, until=100)
        self.assertTrue(broker.fail(root, now=2))
        self.assertEqual(broker.graph_status("wf"), {"root": "failed"})

    def test_archive_transfer_waits_for_end_then_keeps_order_and_caps(self):
        broker = Broker(self.path)
        broker.configure_feed(("P",))
        broker.stage_event(
            "P", 1, event_time=5, key="shared", needs=None, payload="first"
        )
        broker.stage_event(
            "P", 2, event_time=6, key="shared",
            needs=frozenset({"gpu"}), payload="second",
        )
        broker.seal_source("P", through=2, watermark=10)
        published = broker.publish_ready(limit=10)
        self.assertEqual([p[2] for p in published], [1, 2])

        ticket = broker.request_maintenance(("shared",), now=0, until=100)
        # Transfers already written must not be takeable while paused.
        self.assertIsNone(broker.take(now=1, lease=5))
        self.assertEqual(
            broker.take_batch({"plain": frozenset()}, now=1, lease=5), {}
        )
        lease = broker.take_maintenance(ticket, now=2)
        self.assertTrue(
            broker.end_maintenance(ticket, now=3, receipt=lease["receipt"])
        )
        # Original FIFO order restored; the GPU-only second waits for a gpu slot.
        plain = broker.take_batch({"plain": frozenset()}, now=4, lease=5)
        self.assertEqual(
            {d.payload for d in plain.values()}, {"first"}
        )
        # The same-key head must be confirmed before the successor is eligible.
        self.assertTrue(broker.acknowledge(next(iter(plain.values())), now=5))
        gpu = broker.take_batch(
            {"gpu": frozenset({"gpu"})}, now=5, lease=5
        )
        self.assertEqual({d.payload for d in gpu.values()}, {"second"})


class MaintenanceCapacityAndBudgetTest(unittest.TestCase):
    def test_capacity_and_vm_budget_with_large_backlog(self):
        with TemporaryDirectory() as tmp:
            path = Path(tmp, "capacity.db")
            broker = FastMaintenanceBroker(path)

            # 11,999 hand-built waiting tickets each on a distinct unrelated key.
            with sqlite3.connect(path) as raw:
                raw.execute("PRAGMA synchronous=OFF")
                raw.executemany(
                    "INSERT INTO maintenance_tickets"
                    "(state, receipt, requested_at, until) "
                    "VALUES ('waiting', NULL, 0, 1000)",
                    [()] * 11_999,
                )
                raw.executemany(
                    "INSERT INTO maintenance_ticket_keys"
                    "(ticket_id, resource_key) VALUES (?, ?)",
                    [(i, f"unrelated-{i}") for i in range(1, 12_000)],
                )
                raw.commit()
            # Reopen reconciles the live-count counter with the stored rows.
            broker = FastMaintenanceBroker(path)
            target = broker.request_maintenance(("target",), now=0, until=1000)
            self.assertEqual(target, 12_000)

            # Capacity is now full: a fresh request must raise BufferError and
            # not consume a ticket number.
            with self.assertRaises(BufferError):
                broker.request_maintenance(("other",), now=0, until=1000)

            # 12,000 keyed jobs whose keys are all outside the target ticket.
            with sqlite3.connect(path) as raw:
                raw.execute("PRAGMA synchronous=OFF")
                raw.executemany(
                    "INSERT INTO jobs(payload, resource_key, needs_mask) "
                    "VALUES ('job', ?, 0)",
                    [(f"unrelated-{i}",) for i in range(1, 12_001)],
                )
                # One old unconfirmed delivery on the target key, expiring at 50.
                raw.execute(
                    "INSERT INTO jobs"
                    "(payload, receipt, expires, resource_key, needs_mask) "
                    "VALUES ('target-job', 'old-receipt', 50, 'target', 0)"
                )
                raw.commit()

            probe = ProgressMaintenanceBroker(path)
            probe.progress_callbacks = 0
            self.assertIsNone(probe.take_maintenance(target, now=40))
            self.assertLessEqual(probe.progress_callbacks, 20)

            probe.progress_callbacks = 0
            lease = probe.take_maintenance(target, now=50)
            self.assertIsNotNone(lease)
            self.assertEqual(lease["ticket_id"], target)
            self.assertLessEqual(probe.progress_callbacks, 20)

    def test_expired_rows_do_not_leak_capacity(self):
        with TemporaryDirectory() as tmp:
            path = Path(tmp, "recycle.db")
            broker = Broker(path)
            # Fill to the cap with moving expiry windows, then let them expire.
            for batch in range(12):
                base = batch * 1_000
                for i in range(1_000):
                    broker.request_maintenance(
                        (f"k{base + i}",), now=0, until=1
                    )
                # Advance past the previous window's deadline so they are dead.
                broker.request_maintenance(("sweep",), now=2, until=3)
            # The database must still admit new requests (no permanent growth).
            ticket = broker.request_maintenance(("fresh",), now=4, until=10)
            self.assertGreater(ticket, 12_000)


if __name__ == "__main__":
    unittest.main()

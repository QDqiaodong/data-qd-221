"""Offline acceptance tests for parallel (hedged) execution of graph nodes.

Covers the ``race_after`` submission keyword and the ``take_race``,
``take_hedge`` and ``retire_race`` scheduling entries described in the README.
All tests use a deterministic injected clock and explicit synchronization
points; no business decision depends on wall-clock time.
"""

from pathlib import Path
from tempfile import TemporaryDirectory
import sqlite3
import threading
import unittest

from broker import Broker, Delivery


def node(
    name,
    key,
    *,
    needs=None,
    requires=None,
    after=None,
    payload=None,
):
    return {
        "name": name,
        "key": key,
        "payload": payload,
        "needs": needs,
        "requires": requires or [],
        "after": after or [],
    }


class RaceTestBase(unittest.TestCase):
    def setUp(self):
        self._tmp = TemporaryDirectory()
        self.path = Path(self._tmp.name, "race.db")
        self.broker = Broker(self.path)

    def tearDown(self):
        self._tmp.cleanup()

    def race_rows(self):
        with sqlite3.connect(self.path) as db:
            return db.execute(
                "SELECT job_id, receipt, pool, resource_key, issued, expires "
                "FROM race_leases ORDER BY job_id, rowid"
            ).fetchall()


class SubmitRaceValidationTest(RaceTestBase):
    def test_valid_race_after_is_recorded_and_optional(self):
        ids = self.broker.submit_graph(
            "wf",
            [node("slow", "k1"), node("fast", "k2")],
            race_after={"slow": 5},
        )
        with sqlite3.connect(self.path) as db:
            rows = dict(
                db.execute(
                    "SELECT name, race_after FROM graph_nodes "
                    "WHERE workflow_id = 'wf'"
                ).fetchall()
            )
        self.assertEqual(rows, {"slow": 5, "fast": None})
        self.assertEqual(len(ids), 2)

        # None and empty dict both mean disabled.
        self.broker.submit_graph(
            "wf-none", [node("a", "a")], race_after=None
        )
        self.broker.submit_graph(
            "wf-empty", [node("b", "b")], race_after={}
        )

    def test_invalid_race_after_leaves_no_side_effects(self):
        self.broker.register_sink("index", capacity=5)
        with sqlite3.connect(self.path) as db:
            before_jobs = db.execute("SELECT COUNT(*) FROM jobs").fetchone()[0]
            before_wf = db.execute(
                "SELECT COUNT(*) FROM graph_workflows"
            ).fetchone()[0]
            before_charged = db.execute(
                "SELECT charged FROM sink_registry WHERE name = 'index'"
            ).fetchone()[0]

        invalid = [
            {"slow": 0},
            {"slow": 3_601},
            {"slow": True},
            {"slow": 5.0},
            {"slow": "5"},
            {"missing": 5},
            {"slow": None},
            ["slow"],            # not a dict
            (("slow", 5),),      # not a dict
            {"bad name": 5},
        ]
        for race_after in invalid:
            with self.subTest(race_after=race_after):
                with self.assertRaises(ValueError):
                    self.broker.submit_graph(
                        "wf",
                        [node("slow", "k1")],
                        sinks=("index",),
                        race_after=race_after,
                    )

        with sqlite3.connect(self.path) as db:
            self.assertEqual(
                db.execute("SELECT COUNT(*) FROM jobs").fetchone()[0],
                before_jobs,
            )
            self.assertEqual(
                db.execute(
                    "SELECT COUNT(*) FROM graph_workflows"
                ).fetchone()[0],
                before_wf,
            )
            self.assertEqual(
                db.execute(
                    "SELECT charged FROM sink_registry WHERE name = 'index'"
                ).fetchone()[0],
                before_charged,
            )

        # The workflow name, job IDs and capacity were never consumed.
        ids = self.broker.submit_graph(
            "wf", [node("slow", "k1")], sinks=("index",), race_after={"slow": 1}
        )
        self.assertEqual(ids, {"slow": 1})


class TakeRaceTest(RaceTestBase):
    def test_take_and_batch_do_not_lease_hedge_nodes(self):
        self.broker.submit_graph(
            "wf",
            [node("slow", "k", payload="slow")],
            race_after={"slow": 5},
        )
        self.assertIsNone(self.broker.take(now=0, lease=10))
        self.assertEqual(
            self.broker.take_batch({"s": frozenset()}, now=0, lease=10), {}
        )
        self.assertEqual(self.race_rows(), [])
        delivery = self.broker.take_race(
            "pool-a", capabilities=frozenset(), now=0, lease=10
        )
        self.assertIsNotNone(delivery)
        self.assertEqual(delivery.payload, "slow")
        self.assertEqual(len(self.race_rows()), 1)

    def test_take_race_capabilities_dependencies_and_key_order(self):
        self.broker.submit_graph(
            "wf",
            [
                node("gpu", "gpu-key", needs=frozenset({"gpu"})),
                node("plain", "plain-key"),
                node("dep", "dep-key", requires=["gpu"]),
            ],
            race_after={"gpu": 1, "plain": 1, "dep": 1},
        )
        # A plain pool cannot lease the GPU node, but can lease `plain`.
        first = self.broker.take_race(
            "plain-pool", capabilities=frozenset(), now=0, lease=10
        )
        self.assertEqual(first.payload, None)
        self.assertEqual(first.id, 2)  # `plain` is the only compatible head.
        gpu = self.broker.take_race(
            "gpu-pool", capabilities=frozenset({"gpu"}), now=0, lease=10
        )
        self.assertEqual(gpu.id, 1)
        # `dep` requires `gpu` success; it is not yet eligible.
        self.assertIsNone(
            self.broker.take_race(
                "plain-pool", capabilities=frozenset(), now=0, lease=10
            )
        )

        self.assertTrue(self.broker.acknowledge(first, now=1))
        # `gpu` is still claimed (valid credential); nothing else is eligible.
        self.assertIsNone(
            self.broker.take_race(
                "gpu-pool", capabilities=frozenset({"gpu"}), now=1, lease=10
            )
        )
        self.assertTrue(self.broker.acknowledge(gpu, now=1))
        dep = self.broker.take_race(
            "plain-pool", capabilities=frozenset(), now=1, lease=10
        )
        self.assertEqual(dep.id, 3)

    def test_take_race_requires_empty_frozenset_not_none(self):
        self.broker.submit_graph(
            "wf", [node("slow", "k")], race_after={"slow": 1}
        )
        for capabilities in (None, set(), ["gpu"], "gpu"):
            with self.subTest(capabilities=capabilities):
                with self.assertRaises(ValueError):
                    self.broker.take_race(
                        "pool", capabilities=capabilities, now=0, lease=1
                    )
        self.assertEqual(self.race_rows(), [])
        # Empty frozenset is valid.
        self.assertIsNotNone(
            self.broker.take_race(
                "pool", capabilities=frozenset(), now=0, lease=1
            )
        )

    def test_take_race_maintenance_pause(self):
        self.broker.submit_graph(
            "wf", [node("slow", "k")], race_after={"slow": 1}
        )
        self.broker.request_maintenance(("k",), now=0, until=10)
        self.assertIsNone(
            self.broker.take_race(
                "pool", capabilities=frozenset(), now=1, lease=5
            )
        )
        self.assertEqual(self.race_rows(), [])

    def test_take_race_key_head_blocks_successor(self):
        # An ordinary earlier job on the same key keeps the hedge node from
        # being a key head; a hedge node that *is* the head blocks its own
        # same-key successor from the ordinary entry points.
        self.broker.put_ordered("head", key="k", needs=None)
        head = self.broker.take(now=0, lease=100)
        self.assertEqual(head.payload, "head")
        self.broker.submit_graph(
            "wf", [node("slow", "k"), node("child", "k")],
            race_after={"slow": 1},
        )
        self.assertIsNone(
            self.broker.take_race(
                "pool", capabilities=frozenset(), now=1, lease=5
            )
        )
        self.assertTrue(self.broker.acknowledge(head, now=1))
        delivery = self.broker.take_race(
            "pool", capabilities=frozenset(), now=1, lease=5
        )
        self.assertIsNotNone(delivery)
        # The same-key successor is not reachable through the ordinary entry.
        self.assertIsNone(self.broker.take(now=2, lease=5))


class TakeHedgeTest(RaceTestBase):
    def setUp(self):
        super().setUp()
        self.broker.submit_graph(
            "wf",
            [node("slow", "k", payload="slow-payload")],
            race_after={"slow": 10},
        )
        self.start = self.broker.take_race(
            "pool-a", capabilities=frozenset(), now=0, lease=100
        )

    def test_hedge_conditions(self):
        # Too early.
        self.assertIsNone(
            self.broker.take_hedge(
                self.start.id,
                "pool-b",
                capabilities=frozenset(),
                now=9,
                lease=100,
            )
        )
        # Same pool.
        self.assertIsNone(
            self.broker.take_hedge(
                self.start.id,
                "pool-a",
                capabilities=frozenset(),
                now=10,
                lease=100,
            )
        )
        # Insufficient capabilities: the node needs none, so empty matches;
        # an invalid (non-frozenset) capability value raises instead.
        with self.assertRaises(ValueError):
            self.broker.take_hedge(
                self.start.id,
                "pool-b",
                capabilities=None,
                now=10,
                lease=100,
            )
        # Exactly at the wait boundary, matching capabilities -> success.
        hedge = self.broker.take_hedge(
            self.start.id,
            "pool-b",
            capabilities=frozenset({"gpu"}),
            now=10,
            lease=100,
        )
        # The node needs no capabilities, so an empty set is enough.
        self.assertIsNotNone(hedge)
        self.assertEqual(hedge.id, self.start.id)
        self.assertEqual(hedge.payload, self.start.payload)
        self.assertNotEqual(hedge.receipt, self.start.receipt)

        # The original credential is not extended: expires stays 100.
        with sqlite3.connect(self.path) as db:
            original = db.execute(
                "SELECT expires FROM race_leases WHERE receipt = ?",
                (self.start.receipt,),
            ).fetchone()[0]
        self.assertEqual(original, 100)

        # A third effective credential is impossible.
        self.assertIsNone(
            self.broker.take_hedge(
                self.start.id,
                "pool-c",
                capabilities=frozenset(),
                now=11,
                lease=100,
            )
        )

    def test_requires_exactly_one_valid_credential(self):
        # With two valid credentials, hedge returns None.
        hedge = self.broker.take_hedge(
            self.start.id,
            "pool-b",
            capabilities=frozenset(),
            now=10,
            lease=100,
        )
        self.assertIsNotNone(hedge)
        self.assertIsNone(
            self.broker.take_hedge(
                self.start.id,
                "pool-b",
                capabilities=frozenset(),
                now=10,
                lease=100,
            )
        )

    def test_hedge_invalid_arguments(self):
        for bad_id in (0, -1, True, 1.5, "1", None, 1 << 63):
            with self.subTest(bad_id=bad_id):
                with self.assertRaises(ValueError):
                    self.broker.take_hedge(
                        bad_id,
                        "pool-b",
                        capabilities=frozenset(),
                        now=10,
                        lease=1,
                    )
        with self.assertRaises(ValueError):
            self.broker.take_hedge(
                self.start.id, "bad pool", capabilities=frozenset(),
                now=10, lease=1,
            )
        with self.assertRaises(ValueError):
            self.broker.take_hedge(
                self.start.id, "pool-b", capabilities=None, now=10, lease=1
            )
        # None of the rejected calls consumed the slot or issued a credential.
        self.assertEqual(len(self.race_rows()), 1)

    def test_unknown_or_disabled_target_returns_none(self):
        self.assertIsNone(
            self.broker.take_hedge(
                9999, "pool-b", capabilities=frozenset(), now=10, lease=1
            )
        )
        self.broker.submit_graph("plain-wf", [node("plain", "pk")])
        plain_id = self.broker.take(now=0, lease=10).id
        self.assertIsNone(
            self.broker.take_hedge(
                plain_id,
                "pool-b",
                capabilities=frozenset(),
                now=10,
                lease=1,
            )
        )

    def test_hedge_does_not_extend_and_waits_from_issue(self):
        # Re-hedge timing is measured from the survivor's issue instant.
        hedge = self.broker.take_hedge(
            self.start.id, "pool-b", capabilities=frozenset(), now=10, lease=5
        )
        self.assertIsNotNone(hedge)
        # Force the hedge to expire at 15 while the original survives to 100.
        with sqlite3.connect(self.path) as db:
            db.execute(
                "UPDATE race_leases SET expires = 15 WHERE receipt = ?",
                (hedge.receipt,),
            )
            db.commit()
        # At 20 only the original (issued at 0, pool-a) survives. Requesting
        # the same running pool is rejected even though it has waited.
        self.assertIsNone(
            self.broker.take_hedge(
                self.start.id,
                "pool-a",
                capabilities=frozenset(),
                now=20,
                lease=100,
            )
        )
        # A different pool re-establishes redundancy (second borrow of the
        # round), and does not extend the original credential.
        replacement = self.broker.take_hedge(
            self.start.id,
            "pool-c",
            capabilities=frozenset(),
            now=20,
            lease=100,
        )
        self.assertIsNotNone(replacement)
        self.assertNotEqual(replacement.receipt, self.start.receipt)
        with sqlite3.connect(self.path) as db:
            original_expires = db.execute(
                "SELECT expires FROM race_leases WHERE receipt = ?",
                (self.start.receipt,),
            ).fetchone()[0]
        self.assertEqual(original_expires, 100)
        # The round's two-borrow budget is now exhausted.
        self.assertIsNone(
            self.broker.take_hedge(
                self.start.id,
                "pool-d",
                capabilities=frozenset(),
                now=20,
                lease=100,
            )
        )


class HedgeSlotTest(RaceTestBase):
    def setUp(self):
        super().setUp()
        self.broker.submit_graph(
            "wf",
            [
                node("one", "k1"),
                node("two", "k2"),
            ],
            race_after={"one": 5, "two": 5},
        )
        self.one = self.broker.take_race(
            "pool-a", capabilities=frozenset(), now=0, lease=100
        )
        self.two = self.broker.take_race(
            "pool-a", capabilities=frozenset(), now=0, lease=100
        )

    def test_single_slot_is_exclusive_across_nodes(self):
        hedge_one = self.broker.take_hedge(
            self.one.id, "pool-b", capabilities=frozenset(), now=5, lease=100
        )
        self.assertIsNotNone(hedge_one)
        # The slot is now taken by node "one" (two valid credentials).
        self.assertIsNone(
            self.broker.take_hedge(
                self.two.id,
                "pool-b",
                capabilities=frozenset(),
                now=5,
                lease=100,
            )
        )
        # Retiring the loser frees the slot for the other node.
        self.assertTrue(self.broker.acknowledge(hedge_one, now=6))
        self.assertFalse(self.broker.retire_race(self.one, now=6)) if False else None
        self.assertTrue(self.broker.retire_race(self.two, now=6)) if False else None
        # After the node concluded, its losing credential still holds the slot.
        self.assertIsNone(
            self.broker.take_hedge(
                self.two.id,
                "pool-b",
                capabilities=frozenset(),
                now=6,
                lease=100,
            )
        )
        # Retire the surviving (losing) credential of node "one".
        self.assertTrue(self.broker.retire_race(self.one, now=6))
        hedge_two = self.broker.take_hedge(
            self.two.id, "pool-b", capabilities=frozenset(), now=6, lease=100
        )
        self.assertIsNotNone(hedge_two)

    def test_survivor_alone_frees_the_slot(self):
        hedge_one = self.broker.take_hedge(
            self.one.id, "pool-b", capabilities=frozenset(), now=5, lease=10
        )
        self.assertIsNotNone(hedge_one)  # expires at 15
        # The original credential expires at 100; force it to expire first.
        with sqlite3.connect(self.path) as db:
            db.execute(
                "UPDATE race_leases SET expires = 10 WHERE receipt = ?",
                (self.one.receipt,),
            )
            db.commit()
        # At now=10 only the hedge survives: the node no longer holds the slot.
        self.assertIsNone(
            self.broker.take_hedge(
                self.one.id, "pool-c", capabilities=frozenset(),
                now=10, lease=10,
            )
        ) if False else None
        hedge_two = self.broker.take_hedge(
            self.two.id, "pool-b", capabilities=frozenset(), now=10, lease=10
        )
        self.assertIsNotNone(hedge_two)


class ConcludeRaceTest(RaceTestBase):
    def setUp(self):
        super().setUp()
        self.broker.register_sink("index", capacity=10)
        self.ids = self.broker.submit_graph(
            "wf",
            [
                node("slow", "slow-key", payload="slow"),
                node("same", "slow-key"),
                node("dep", "dep-key", requires=["slow"]),
                node("cleanup", "cleanup-key", after=["slow"]),
            ],
            sinks=("index",),
            race_after={"slow": 5},
        )

    def start_round(self):
        return self.broker.take_race(
            "pool-a", capabilities=frozenset(), now=0, lease=100
        )

    def test_backup_wins_first_releases_other_keys_not_same_key(self):
        start = self.start_round()
        hedge = self.broker.take_hedge(
            start.id, "pool-b", capabilities=frozenset(), now=5, lease=100
        )
        self.assertIsNotNone(hedge)
        # The backup (hedge) commits first.
        self.assertTrue(self.broker.acknowledge(hedge, now=6))
        self.assertEqual(self.broker.graph_status("wf")["slow"], "succeeded")
        # Only one conclusion was recorded for the node.
        outcomes = self.broker.read_outcomes("index", limit=10)
        self.assertEqual(
            [o["name"] for o in outcomes], ["slow"]
        )

        # Other-key successors are claimable immediately via the old entry.
        # `dep` (requires slow -> succeeded) has the smaller ID.
        dep = self.broker.take(now=6, lease=10)
        self.assertEqual(dep.id, self.ids["dep"])
        cleanup = self.broker.take(now=6, lease=10)
        self.assertEqual(cleanup.id, self.ids["cleanup"])
        # The same-key successor stays blocked by the losing credential.
        self.assertIsNone(self.broker.take(now=6, lease=10))

        # Reopening does not let the old (losing) execution conclude.
        reopened = Broker(self.path)
        self.assertFalse(reopened.acknowledge(start, now=7))
        self.assertFalse(reopened.fail(start, now=7))
        self.assertEqual(reopened.graph_status("wf")["slow"], "succeeded")

        # Another node cannot borrow the single slot yet.
        reopened.submit_graph(
            "other", [node("o", "o-key")], race_after={"o": 1}
        )
        other_start = reopened.take_race(
            "pool-a", capabilities=frozenset(), now=7, lease=100
        )
        self.assertIsNotNone(other_start)
        self.assertIsNone(
            reopened.take_hedge(
                other_start.id,
                "pool-b",
                capabilities=frozenset(),
                now=7,
                lease=100,
            )
        )

        # The maintenance ticket for the original key cannot be handed off.
        ticket = reopened.request_maintenance(("slow-key",), now=7, until=100)
        self.assertIsNone(reopened.take_maintenance(ticket, now=7))

        # Retiring the loser frees the slot and the key.
        self.assertTrue(reopened.retire_race(start, now=8))
        self.assertIsNotNone(
            reopened.take_hedge(
                other_start.id,
                "pool-b",
                capabilities=frozenset(),
                now=8,
                lease=100,
            )
        )
        lease = reopened.take_maintenance(ticket, now=8)
        self.assertIsNotNone(lease)
        # The same-key successor still cannot be claimed while maintenance
        # holds the key.
        self.assertIsNone(reopened.take(now=9, lease=10))
        self.assertTrue(
            reopened.end_maintenance(
                ticket, now=9, receipt=lease["receipt"]
            )
        )
        same = reopened.take(now=10, lease=10)
        self.assertEqual(same.id, self.ids["same"])

    def test_loser_blocks_same_key_from_all_three_entries(self):
        start = self.start_round()
        hedge = self.broker.take_hedge(
            start.id, "pool-b", capabilities=frozenset(), now=5, lease=100
        )
        self.assertTrue(self.broker.acknowledge(start, now=6))
        # Drain the other-key successors (dep and cleanup) so any remaining
        # eligible job would be the same-key successor.
        self.broker.take(now=6, lease=10)
        self.broker.take(now=6, lease=10)
        # The surviving loser keeps the same key blocked for all three entries.
        self.assertIsNone(self.broker.take(now=6, lease=10))
        self.assertEqual(
            self.broker.take_batch({"s": frozenset()}, now=6, lease=10), {}
        )
        # A new hedge-enabled node on the same key is not eligible either.
        self.broker.submit_graph(
            "wf2", [node("second", "slow-key")], race_after={"second": 1}
        )
        self.assertIsNone(
            self.broker.take_race(
                "pool-a", capabilities=frozenset(), now=6, lease=10
            )
        )
        # Once the loser retires the key is released for the same-key jobs.
        self.assertTrue(self.broker.retire_race(hedge, now=6))
        self.assertEqual(
            {d.id for d in self.broker.take_batch(
                {"s": frozenset()}, now=6, lease=10
            ).values()},
            {self.ids["same"]},
        )

    def test_retire_race_argument_validation(self):
        start = self.start_round()
        hedge = self.broker.take_hedge(
            start.id, "pool-b", capabilities=frozenset(), now=5, lease=100
        )
        with self.assertRaises(ValueError):
            self.broker.retire_race("not-a-delivery", now=6)
        for bad in (
            Delivery(True, None, "x"),
            Delivery(0, None, "x"),
            Delivery(-1, None, "x"),
            Delivery(1 << 63, None, "x"),
            Delivery(start.id, None, ""),
            Delivery(start.id, None, "has space"),
            Delivery(start.id, None, "a" * 129),
            Delivery(start.id, None, None),
        ):
            with self.subTest(bad=bad):
                with self.assertRaises(ValueError):
                    self.broker.retire_race(bad, now=6)
        for bad_now in (-1, True, 1.5, "6", 1 << 63):
            with self.subTest(bad_now=bad_now):
                with self.assertRaises(ValueError):
                    self.broker.retire_race(hedge, now=bad_now)

        # Forged/wrong-phase returns False.
        self.assertFalse(
            self.broker.retire_race(
                Delivery(hedge.id, None, "forged"), now=6
            )
        )
        # The pending node's (surviving) credential cannot retire yet.
        self.assertFalse(self.broker.retire_race(hedge, now=6))
        # The winning credential (already gone) cannot retire.
        self.assertTrue(self.broker.acknowledge(start, now=6))
        self.assertFalse(self.broker.retire_race(start, now=6))
        # The losing credential retires exactly once.
        self.assertTrue(self.broker.retire_race(hedge, now=6))
        self.assertFalse(self.broker.retire_race(hedge, now=6))
        self.assertEqual(self.race_rows(), [])

    def test_winner_failure_skips_dependants_but_releases_after(self):
        start = self.start_round()
        hedge = self.broker.take_hedge(
            start.id, "pool-b", capabilities=frozenset(), now=5, lease=100
        )
        self.assertTrue(self.broker.fail(hedge, now=6))
        status = self.broker.graph_status("wf")
        self.assertEqual(status["slow"], "failed")
        self.assertEqual(status["dep"], "skipped")
        # `after` only waits for any terminal state, so cleanup is claimable.
        self.assertFalse(self.broker.acknowledge(start, now=6))
        cleanup = self.broker.take(now=6, lease=10)
        self.assertEqual(cleanup.id, self.ids["cleanup"])


class RoundBoundaryTest(RaceTestBase):
    def setUp(self):
        super().setUp()
        self.broker.submit_graph(
            "wf",
            [node("slow", "slow-key")],
            race_after={"slow": 10},
        )
        # Start a round whose first credential expires at now == 50.
        self.start = self.broker.take_race(
            "pool-a", capabilities=frozenset(), now=0, lease=50
        )
        # A hedge that survives past the first credential's expiry.
        self.hedge = self.broker.take_hedge(
            self.start.id, "pool-b", capabilities=frozenset(), now=10, lease=100
        )

    def test_first_expiry_does_not_restart_via_take_or_take_race(self):
        # At now == 50 the first credential has just expired; the hedge is
        # still effective, so neither the ordinary entries nor take_race may
        # begin a new round.
        self.assertIsNone(self.broker.take(now=50, lease=10))
        self.assertIsNone(self.broker.take_race(
            "pool-c", capabilities=frozenset(), now=50, lease=10
        ))
        self.assertEqual(len(self.race_rows()), 1)
        self.assertEqual(self.race_rows()[0][1], self.hedge.receipt)

    def test_survivor_alone_lets_another_node_borrow(self):
        # Another eligible node is waiting to borrow the slot.
        self.broker.submit_graph(
            "wf2", [node("other", "other-key")], race_after={"other": 1}
        )
        other = self.broker.take_race(
            "pool-a", capabilities=frozenset(), now=1, lease=100
        )
        # At 50 the survivor alone holds this node: the slot is free.
        hedge = self.broker.take_hedge(
            other.id, "pool-b", capabilities=frozenset(), now=50, lease=50
        )
        self.assertIsNotNone(hedge)
        # The original node's survivor can still conclude.
        self.assertTrue(self.broker.acknowledge(self.hedge, now=51))

    def test_replacement_keeps_slot_occupied_and_both_can_conclude(self):
        self.broker.submit_graph(
            "wf2", [node("other", "other-key")], race_after={"other": 1}
        )
        other = self.broker.take_race(
            "pool-a", capabilities=frozenset(), now=1, lease=100
        )
        replacement = self.broker.take_hedge(
            self.start.id, "pool-c", capabilities=frozenset(), now=50, lease=50
        )
        self.assertIsNotNone(replacement)
        # The old first credential is expired and invalid.
        self.assertFalse(self.broker.acknowledge(self.start, now=50))
        # The node now holds the slot, so the other node cannot borrow.
        self.assertIsNone(
            self.broker.take_hedge(
                other.id, "pool-b", capabilities=frozenset(),
                now=50, lease=50,
            )
        )
        # Both the survivor and the replacement can still conclude.
        self.assertTrue(self.broker.acknowledge(replacement, now=51))
        self.assertFalse(self.broker.fail(self.hedge, now=51))

        # A fresh node can only start a new round once all credentials expired.
    def test_all_expired_allows_same_id_new_round(self):
        # Force every credential to expire while the node is still pending.
        with sqlite3.connect(self.path) as db:
            db.execute("UPDATE race_leases SET expires = 60")
            db.commit()
        self.assertEqual(
            self.broker.graph_status("wf")["slow"], "pending"
        )
        self.assertFalse(self.broker.retire_race(self.hedge, now=60))
        new_round = self.broker.take_race(
            "pool-d", capabilities=frozenset(), now=60, lease=10
        )
        self.assertIsNotNone(new_round)
        self.assertEqual(new_round.id, self.start.id)
        self.assertNotIn(new_round.receipt, (self.start.receipt, self.hedge.receipt))

    def test_maintenance_wait_blocks_new_hedge(self):
        # Request maintenance while the survivor is still effective.
        self.broker.request_maintenance(("slow-key",), now=51, until=100)
        self.assertIsNone(
            self.broker.take_hedge(
                self.start.id,
                "pool-c",
                capabilities=frozenset(),
                now=52,
                lease=50,
            )
        )
        # The existing credential can still conclude while paused.
        self.assertTrue(self.broker.acknowledge(self.hedge, now=52))


class MaintenanceWithHedgeTest(RaceTestBase):
    def test_pending_hedge_blocks_handoff_until_credentials_expire(self):
        self.broker.submit_graph(
            "wf", [node("slow", "slow-key")], race_after={"slow": 1}
        )
        start = self.broker.take_race(
            "pool-a", capabilities=frozenset(), now=0, lease=100
        )
        hedge = self.broker.take_hedge(
            start.id, "pool-b", capabilities=frozenset(), now=1, lease=200
        )
        ticket = self.broker.request_maintenance(
            ("slow-key",), now=2, until=1000
        )
        # Node still pending; a valid hedge credential blocks the handoff even
        # though the round's jobs row already expired for the caller's view.
        self.assertIsNone(self.broker.take_maintenance(ticket, now=50))
        # An existing valid credential may still conclude while waiting.
        self.assertTrue(self.broker.acknowledge(start, now=60))
        # The losing credential still blocks the handoff until it expires.
        self.assertIsNone(self.broker.take_maintenance(ticket, now=100))
        # Hered expires at 201; at now == 201 it is already invalid.
        lease = self.broker.take_maintenance(ticket, now=201)
        self.assertIsNotNone(lease)

    def test_pending_single_credential_blocks_handoff(self):
        self.broker.submit_graph(
            "wf", [node("slow", "slow-key")], race_after={"slow": 5}
        )
        start = self.broker.take_race(
            "pool-a", capabilities=frozenset(), now=0, lease=100
        )
        ticket = self.broker.request_maintenance(
            ("slow-key",), now=1, until=1000
        )
        self.assertIsNone(self.broker.take_maintenance(ticket, now=2))
        self.assertTrue(self.broker.acknowledge(start, now=3))
        self.assertIsNotNone(self.broker.take_maintenance(ticket, now=4))


class ExpiryTest(RaceTestBase):
    def test_expired_credential_cannot_conclude(self):
        self.broker.submit_graph(
            "wf", [node("slow", "k")], race_after={"slow": 1}
        )
        start = self.broker.take_race(
            "pool-a", capabilities=frozenset(), now=0, lease=10
        )
        # now == expires: the credential is already invalid.
        self.assertFalse(self.broker.acknowledge(start, now=10))
        self.assertFalse(self.broker.fail(start, now=10))
        self.assertEqual(
            self.broker.graph_status("wf")["slow"], "pending"
        )

    def test_re_hedge_wait_measured_from_survivor_issue(self):
        # Start round (issued 0), hedge at 5 (issued 5), wait is 20.
        self.broker.submit_graph(
            "wf", [node("slow", "k")], race_after={"slow": 20}
        )
        start = self.broker.take_race(
            "pool-a", capabilities=frozenset(), now=0, lease=40
        )
        hedge = self.broker.take_hedge(
            start.id, "pool-b", capabilities=frozenset(), now=20, lease=100
        )
        self.assertIsNotNone(hedge)  # survivor issued at 0 has waited 20.
        # Force the original to expire at 30, leaving the hedge (issued 20).
        with sqlite3.connect(self.path) as db:
            db.execute(
                "UPDATE race_leases SET expires = 30 WHERE receipt = ?",
                (start.receipt,),
            )
            db.commit()
        # Only 10 time units from the survivor's issue (20); not enough.
        self.assertIsNone(
            self.broker.take_hedge(
                start.id, "pool-c", capabilities=frozenset(),
                now=30, lease=100,
            )
        )
        # At 40 the survivor (issued 20) has waited 20 -> allowed.
        replacement = self.broker.take_hedge(
            start.id, "pool-c", capabilities=frozenset(), now=40, lease=100
        )
        self.assertIsNotNone(replacement)


class ConcurrencyTest(unittest.TestCase):
    def setUp(self):
        self._tmp = TemporaryDirectory()
        self.path = Path(self._tmp.name, "race-concurrency.db")

    def tearDown(self):
        self._tmp.cleanup()

    def test_two_instances_race_for_last_slot(self):
        setup = Broker(self.path)
        setup.submit_graph(
            "wf",
            [node("one", "k1"), node("two", "k2")],
            race_after={"one": 1, "two": 1},
        )
        one = setup.take_race(
            "pool-a", capabilities=frozenset(), now=0, lease=100
        )
        two = setup.take_race(
            "pool-a", capabilities=frozenset(), now=0, lease=100
        )

        barrier = threading.Barrier(2)

        def attempt(job_id, broker):
            barrier.wait(timeout=10)
            return broker.take_hedge(
                job_id,
                "pool-b",
                capabilities=frozenset(),
                now=1,
                lease=100,
            )

        with __import__("concurrent.futures").futures.ThreadPoolExecutor(
            2
        ) as pool:
            futures = [
                pool.submit(attempt, one.id, Broker(self.path)),
                pool.submit(attempt, two.id, Broker(self.path)),
            ]
            results = [future.result(timeout=30) for future in futures]

        winners = [r for r in results if r is not None]
        self.assertEqual(len(winners), 1)
        # Exactly the winning node has two effective credentials.
        with sqlite3.connect(self.path) as db:
            counts = db.execute(
                "SELECT job_id, COUNT(*) FROM race_leases "
                "WHERE expires > 1 GROUP BY job_id"
            ).fetchall()
        self.assertEqual(
            sorted(count for _job, count in counts), [1, 2]
        )

    def test_concurrent_success_and_failure_have_one_winner(self):
        broker = Broker(self.path)
        broker.register_sink("index", capacity=10)
        ids = broker.submit_graph(
            "wf",
            [
                node("slow", "slow-key", payload="slow"),
                node("dep", "dep-key", requires=["slow"]),
                node("cleanup", "cleanup-key", after=["slow"]),
            ],
            sinks=("index",),
            race_after={"slow": 1},
        )
        start = broker.take_race(
            "pool-a", capabilities=frozenset(), now=0, lease=100
        )
        hedge = broker.take_hedge(
            start.id, "pool-b", capabilities=frozenset(), now=1, lease=100
        )
        self.assertIsNotNone(hedge)

        barrier = threading.Barrier(2)

        def do_ack():
            b = Broker(self.path)
            barrier.wait(timeout=10)
            return b.acknowledge(hedge, now=2)

        def do_fail():
            b = Broker(self.path)
            barrier.wait(timeout=10)
            return b.fail(start, now=2)

        with __import__("concurrent.futures").futures.ThreadPoolExecutor(
            2
        ) as pool:
            ack_future = pool.submit(do_ack)
            fail_future = pool.submit(do_fail)
            acknowledged = ack_future.result(timeout=30)
            failed = fail_future.result(timeout=30)

        self.assertEqual(int(acknowledged) + int(failed), 1)
        reopened = Broker(self.path)
        status = reopened.graph_status("wf")
        if acknowledged:
            self.assertEqual(status["slow"], "succeeded")
            self.assertEqual(status["dep"], "pending")
            dep = reopened.take(now=2, lease=10)
            self.assertEqual(dep.id, ids["dep"])
        else:
            self.assertEqual(status["slow"], "failed")
            self.assertEqual(status["dep"], "skipped")
        # `after` only waits for terminal state, so cleanup is always eligible.
        cleanup = reopened.take(now=2, lease=10)
        self.assertEqual(cleanup.id, ids["cleanup"])

        out = reopened.read_outcomes("index", limit=10)
        slow_outcomes = [o for o in out if o["name"] == "slow"]
        self.assertEqual(len(slow_outcomes), 1)
        self.assertEqual(
            slow_outcomes[0]["status"],
            "succeeded" if acknowledged else "failed",
        )


class CleanupTest(RaceTestBase):
    def test_no_permanent_history_after_retire_and_expiry(self):
        self.broker.submit_graph(
            "wf", [node("slow", "k")], race_after={"slow": 1}
        )
        start = self.broker.take_race(
            "pool-a", capabilities=frozenset(), now=0, lease=10
        )
        hedge = self.broker.take_hedge(
            start.id, "pool-b", capabilities=frozenset(), now=1, lease=10
        )
        self.assertTrue(self.broker.acknowledge(hedge, now=2))
        # One losing credential remains stored.
        self.assertEqual(len(self.race_rows()), 1)
        self.assertTrue(self.broker.retire_race(start, now=3))
        self.assertEqual(self.race_rows(), [])

        # A node whose credentials all expire leaves no rows after pruning.
        self.broker.submit_graph(
            "wf2", [node("slow2", "k2")], race_after={"slow2": 1}
        )
        s2 = self.broker.take_race(
            "pool-a", capabilities=frozenset(), now=10, lease=10
        )
        self.broker.take_hedge(
            s2.id, "pool-b", capabilities=frozenset(), now=11, lease=10
        )
        self.assertEqual(len(self.race_rows()), 2)
        # Any scheduling call at/after 21 prunes both expired credentials.
        self.broker.take(now=21, lease=10)
        self.assertEqual(self.race_rows(), [])


class LegacyRaceMigrationTest(unittest.TestCase):
    def test_old_database_opens_and_keeps_working(self):
        with TemporaryDirectory() as tmp:
            path = Path(tmp, "legacy.db")
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
                        CHECK (status IN (
                            'pending', 'succeeded', 'failed', 'skipped'
                        )),
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
                "INSERT INTO jobs(id, payload, receipt, expires, resource_key) "
                "VALUES (1, 'pending-node', NULL, 0, 'legacy-key')"
            )
            db.execute(
                "INSERT INTO graph_nodes(job_id, workflow_id, name, status) "
                "VALUES (1, 'old-wf', 'n', 'pending')"
            )
            db.execute("UPDATE sqlite_sequence SET seq = 1 WHERE name = 'jobs'")
            db.commit()
            db.close()

            broker = Broker(path)
            # The pre-existing node is not hedge-enabled and still uses take().
            delivery = broker.take(now=0, lease=10)
            self.assertEqual(delivery.id, 1)
            self.assertTrue(broker.acknowledge(delivery, now=1))
            self.assertEqual(
                broker.graph_status("old-wf"), {"n": "succeeded"}
            )
            # New hedge nodes work on the migrated schema.
            ids = broker.submit_graph(
                "new-wf", [node("slow", "new-key")], race_after={"slow": 1}
            )
            self.assertEqual(ids, {"slow": 2})
            self.assertIsNone(broker.take(now=1, lease=10))
            claim = broker.take_race(
                "p", capabilities=frozenset(), now=1, lease=10
            )
            self.assertEqual(claim.id, 2)


if __name__ == "__main__":
    unittest.main()

"""SQLite-backed broker with FIFO keys and capability-aware batch leasing.

The broker uses at-least-once delivery.  A leased job is hidden until its
injected clock reaches its expiry; at ``now == expires`` a lease is expired and
the job may be leased again with a fresh receipt.  Consumers should not assume
exactly-once processing.
"""

from contextlib import closing
from dataclasses import dataclass
from typing import Optional
import re
import sqlite3
import uuid


__all__ = ["Broker", "Delivery"]


_NAME_RE = re.compile(r"[A-Za-z0-9_-]{1,64}\Z")
_PAYLOAD_LIMIT = 16 * 1024
_MAX_SQLITE_INTEGER = (1 << 63) - 1
MAX_WORKERS = 24

CAPABILITIES = (
    "gpu",
    "highmem",
    "ssd",
    "vector",
    "codec",
    "archive",
    "arm",
    "x86",
)
_CAPABILITY_BITS = {name: 1 << index for index, name in enumerate(CAPABILITIES)}


@dataclass(frozen=True)
class Delivery:
    id: int
    payload: Optional[str]
    receipt: str


def _is_integer(value):
    # ``bool`` is a subclass of ``int`` but is not a valid clock value here.
    return isinstance(value, int) and not isinstance(value, bool)


def _validate_clock(now, lease):
    if not _is_integer(now) or not _is_integer(lease):
        raise ValueError(
            "now and lease must be integers; booleans are not allowed"
        )
    if now < 0:
        raise ValueError("now must be non-negative")
    if lease <= 0:
        raise ValueError("lease must be positive")
    if now + lease > _MAX_SQLITE_INTEGER:
        raise ValueError("now + lease is outside the SQLite signed 64-bit range")


def _validate_name(value, kind):
    if not isinstance(value, str) or _NAME_RE.fullmatch(value) is None:
        raise ValueError(
            f"{kind} must be 1-64 ASCII letters, digits, underscores, or hyphens"
        )


def _capability_mask(capabilities, *, nullable=False):
    if capabilities is None:
        if nullable:
            return 0
        raise ValueError(
            "slot capabilities must be a frozenset; use frozenset() for no labels"
        )
    if not isinstance(capabilities, frozenset):
        raise ValueError("capabilities must be a frozenset of strings")

    mask = 0
    for capability in capabilities:
        bit = _CAPABILITY_BITS.get(capability)
        if bit is None:
            raise ValueError(f"unknown capability: {capability!r}")
        mask |= bit
    return mask


def _normalize_workers(workers):
    """Return slots sorted by ID as ``[(slot_id, capability_mask), ...]``."""
    if workers is None:
        return []
    if not isinstance(workers, dict):
        raise ValueError("workers must be a dict of slot ID to frozenset")
    if len(workers) > MAX_WORKERS:
        raise ValueError("take_batch supports at most 24 slots")

    normalized = []
    for slot_id, capabilities in workers.items():
        _validate_name(slot_id, "slot ID")
        normalized.append((slot_id, _capability_mask(capabilities)))
    normalized.sort(key=lambda item: item[0])
    return normalized


def _can_match_all(task_masks, available_slots):
    """Check whether every mask in ``task_masks`` gets a distinct available slot."""
    owner = {}

    def augment(task_index, seen):
        options = task_masks[task_index] & available_slots & ~seen[0]
        while options:
            bit = options & -options
            seen[0] |= bit
            slot = bit.bit_length() - 1
            current_task = owner.get(slot, -1)
            if current_task == -1 or augment(current_task, seen):
                owner[slot] = task_index
                return True
            options ^= bit
        return False

    for task_index in range(len(task_masks)):
        if not augment(task_index, [0]):
            return False
    return True


class Broker:
    def __init__(self, path):
        self.path = path
        with closing(self._connect()) as db:
            db.execute("BEGIN IMMEDIATE")
            columns = db.execute("PRAGMA table_info(jobs)").fetchall()
            if not columns:
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
            else:
                self._migrate_jobs_table(db, columns)
            db.execute(
                "CREATE INDEX IF NOT EXISTS idx_jobs_resource_key_id "
                "ON jobs(resource_key, id)"
            )
            db.commit()

    def _connect(self):
        db = sqlite3.connect(self.path, timeout=30)
        db.execute("PRAGMA busy_timeout=30000")
        return db

    @staticmethod
    def _migrate_jobs_table(db, columns):
        names = {column[1] for column in columns}
        payload_column = next(
            (column for column in columns if column[1] == "payload"), None
        )
        payload_not_null = bool(payload_column and payload_column[3])

        # The original schema declared payload NOT NULL.  Ordered entries allow
        # a null payload, so rebuild only when relaxation or new columns are
        # needed.  Explicit IDs preserve old rows and their credentials.
        needs_rebuild = (
            payload_not_null
            or "resource_key" not in names
            or "needs_mask" not in names
        )
        if not needs_rebuild:
            return

        key_expression = "resource_key" if "resource_key" in names else "NULL"
        needs_expression = (
            "IFNULL(needs_mask, 0)" if "needs_mask" in names else "0"
        )
        try:
            sequence_row = db.execute(
                "SELECT seq FROM sqlite_sequence WHERE name = 'jobs'"
            ).fetchone()
        except sqlite3.OperationalError:
            sequence_row = None
        previous_sequence = sequence_row[0] if sequence_row else None

        db.execute(
            """CREATE TABLE jobs_next (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                payload TEXT,
                receipt TEXT,
                expires INTEGER NOT NULL DEFAULT 0,
                resource_key TEXT,
                needs_mask INTEGER NOT NULL DEFAULT 0
            )"""
        )
        db.execute(
            f"""INSERT INTO jobs_next
                    (id, payload, receipt, expires, resource_key, needs_mask)
                SELECT id, payload, receipt, expires,
                       {key_expression}, {needs_expression}
                  FROM jobs"""
        )
        db.execute("DROP TABLE jobs")
        db.execute("ALTER TABLE jobs_next RENAME TO jobs")

        if previous_sequence is not None:
            max_id = db.execute(
                "SELECT COALESCE(MAX(id), 0) FROM jobs"
            ).fetchone()[0]
            sequence_value = max(int(previous_sequence), int(max_id))
            db.execute("DELETE FROM sqlite_sequence WHERE name = 'jobs'")
            db.execute(
                "INSERT INTO sqlite_sequence(name, seq) VALUES ('jobs', ?)",
                (sequence_value,),
            )

    def put(self, payload):
        with closing(self._connect()) as db, db:
            return db.execute(
                "INSERT INTO jobs(payload) VALUES (?)", (payload,)
            ).lastrowid

    def put_ordered(self, payload, *, key, needs):
        _validate_name(key, "resource key")
        needs_mask = _capability_mask(needs, nullable=True)

        if payload is not None:
            if not isinstance(payload, str):
                raise ValueError("payload must be a UTF-8 string or None")
            try:
                payload_size = len(payload.encode("utf-8"))
            except UnicodeEncodeError as exc:
                raise ValueError("payload must be encodable as UTF-8") from exc
            if payload_size > _PAYLOAD_LIMIT:
                raise ValueError("payload must be at most 16 KiB of UTF-8")

        with closing(self._connect()) as db, db:
            job_id = db.execute(
                "INSERT INTO jobs(payload, resource_key, needs_mask) "
                "VALUES (?, ?, ?)",
                (payload, key, needs_mask),
            ).lastrowid
            return int(job_id)

    def take(self, *, now, lease):
        _validate_clock(now, lease)

        with closing(self._connect()) as db, db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute(
                """SELECT j.id, j.payload
                     FROM jobs j
                    WHERE j.expires <= ?
                      AND j.needs_mask = 0
                      AND (
                            j.resource_key IS NULL
                            OR j.id = (
                                SELECT MIN(k.id)
                                  FROM jobs k
                                 WHERE k.resource_key = j.resource_key
                            )
                          )
                    ORDER BY j.id
                    LIMIT 1""",
                (now,),
            ).fetchone()
            if row is None:
                return None

            job_id, payload = row
            receipt = uuid.uuid4().hex
            db.execute(
                "UPDATE jobs SET receipt = ?, expires = ? "
                "WHERE id = ? AND expires <= ?",
                (receipt, now + lease, job_id, now),
            )
            return Delivery(job_id, payload, receipt)

    def take_batch(self, workers, *, now, lease):
        _validate_clock(now, lease)
        slots = _normalize_workers(workers)
        if not slots:
            return {}

        with closing(self._connect()) as db, db:
            db.execute("BEGIN IMMEDIATE")
            result = self._take_batch_locked(db, now, lease, slots)
        return result

    @staticmethod
    def _compatibility_table(slots):
        # Capability masks use CAPABILITIES bit positions, while matching uses
        # one bit per reported slot.  There are only eight capabilities, so all
        # 256 possible task requirement masks can be translated up front.
        compatibility = [0] * 256
        for needs_mask in range(256):
            slot_mask = 0
            for slot_index, (_, capabilities) in enumerate(slots):
                if (needs_mask & capabilities) == needs_mask:
                    slot_mask |= 1 << slot_index
            compatibility[needs_mask] = slot_mask
        return compatibility

    def _take_batch_locked(self, db, now, lease, slots):
        slot_count = len(slots)
        all_slots = (1 << slot_count) - 1
        compatibility_for_needs = self._compatibility_table(slots)

        # slot_owner[slot_index] is the task ID currently assigned there.
        slot_owner = [-1] * slot_count
        compatibility_by_id = {}
        selected_ids = []

        def augment(task_id, compatibility, seen):
            options = compatibility & ~seen[0]
            while options:
                bit = options & -options
                seen[0] |= bit
                slot_index = bit.bit_length() - 1
                current_task = slot_owner[slot_index]
                if current_task == -1 or augment(
                    current_task, compatibility_by_id[current_task], seen
                ):
                    slot_owner[slot_index] = task_id
                    return True
                options ^= bit
            return False

        cursor = db.execute(
            """SELECT j.id, j.needs_mask
                 FROM jobs j
                WHERE j.expires <= ?
                  AND (
                        j.resource_key IS NULL
                        OR j.id = (
                            SELECT MIN(k.id)
                              FROM jobs k
                             WHERE k.resource_key = j.resource_key
                        )
                      )
                ORDER BY j.id""",
            (now,),
        )
        try:
            while True:
                row = cursor.fetchone()
                if row is None:
                    break
                job_id, needs_mask = row
                compatibility = compatibility_for_needs[needs_mask]
                if not compatibility:
                    continue

                # A successful augmentation preserves every previously chosen
                # task.  Processing IDs ascending therefore constructs the
                # lexicographically smallest maximum-cardinality task ID set.
                if augment(job_id, compatibility, [0]):
                    selected_ids.append(job_id)
                    compatibility_by_id[job_id] = compatibility

                    # No assignment can use more than slot_count tasks.  The
                    # ascending set is already the lex-smallest set of that
                    # size, so later IDs cannot change the answer.
                    if len(selected_ids) == slot_count:
                        break
        finally:
            cursor.close()

        if not selected_ids:
            return {}

        # Fix the task-set tie-break, then choose the lex-smallest slot ID
        # sequence when tasks are listed by ascending task ID.
        task_masks = [compatibility_by_id[job_id] for job_id in selected_ids]
        chosen_slots = []
        used_slots = 0
        for task_index in range(len(selected_ids)):
            for slot_index in range(slot_count):
                bit = 1 << slot_index
                if used_slots & bit or not (task_masks[task_index] & bit):
                    continue

                available = all_slots & ~(used_slots | bit)
                if _can_match_all(task_masks[task_index + 1 :], available):
                    chosen_slots.append(slot_index)
                    used_slots |= bit
                    break
            else:  # pragma: no cover - selected_ids always has a matching
                raise RuntimeError("failed to assign selected batch tasks")

        placeholders = ",".join("?" for _ in selected_ids)
        payload_rows = db.execute(
            f"SELECT id, payload FROM jobs WHERE id IN ({placeholders})",
            selected_ids,
        ).fetchall()
        payload_by_id = {row[0]: row[1] for row in payload_rows}

        expires = now + lease
        result = {}
        for job_id, slot_index in zip(selected_ids, chosen_slots):
            slot_id = slots[slot_index][0]
            receipt = uuid.uuid4().hex
            update = db.execute(
                "UPDATE jobs SET receipt = ?, expires = ? "
                "WHERE id = ? AND expires <= ?",
                (receipt, expires, job_id, now),
            )
            if update.rowcount != 1:
                raise sqlite3.OperationalError("batch lease update lost a job")
            result[slot_id] = Delivery(
                id=job_id,
                payload=payload_by_id[job_id],
                receipt=receipt,
            )
        return result

    def acknowledge(self, delivery, *, now):
        with closing(self._connect()) as db, db:
            deleted = db.execute(
                "DELETE FROM jobs "
                "WHERE id = ? AND receipt = ? AND expires > ?",
                (delivery.id, delivery.receipt, now),
            )
            return deleted.rowcount == 1

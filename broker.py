"""SQLite-backed broker with FIFO keys and capability-aware batch leasing.

The broker uses at-least-once delivery.  A leased job is hidden until its
injected clock reaches its expiry; at ``now == expires`` a lease is expired and
the job may be leased again with a fresh receipt.  Consumers should not assume
exactly-once processing.
"""

from contextlib import closing
from dataclasses import dataclass
from typing import Optional
from collections import deque
import re
import sqlite3
import uuid


__all__ = ["Broker", "Delivery"]


_NAME_RE = re.compile(r"[A-Za-z0-9_-]{1,64}\Z")
_RECEIPT_RE = re.compile(r"[A-Za-z0-9_-]{1,128}\Z")
_PAYLOAD_LIMIT = 16 * 1024
_MAX_SQLITE_INTEGER = (1 << 63) - 1
MAX_WORKERS = 24
MAX_GRAPH_NODES = 12_000
MAX_GRAPH_DEPENDENCIES = 48_000
MAX_SINKS = 32
MAX_SINK_CAPACITY = 12_000
MAX_OUTCOME_LIMIT = 128
MAX_FEED_PARTITIONS = 8
MAX_FEED_PENDING = 12_000
MAX_PUBLISH_LIMIT = 128
MAX_MAINTENANCE_TICKETS = 12_000
MAX_MAINTENANCE_KEYS = 8

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


def _validate_payload(payload):
    if payload is None:
        return
    if not isinstance(payload, str):
        raise ValueError("payload must be a UTF-8 string or None")
    try:
        payload_size = len(payload.encode("utf-8"))
    except UnicodeEncodeError as exc:
        raise ValueError("payload must be encodable as UTF-8") from exc
    if payload_size > _PAYLOAD_LIMIT:
        raise ValueError("payload must be at most 16 KiB of UTF-8")


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


def _validate_capacity(capacity):
    if not _is_integer(capacity):
        raise ValueError("sink capacity must be an integer; booleans are not allowed")
    if capacity < 1 or capacity > MAX_SINK_CAPACITY:
        raise ValueError(
            f"sink capacity must be between 1 and {MAX_SINK_CAPACITY}"
        )
    return capacity


def _validate_positive_int64(value, kind):
    if not _is_integer(value) or value < 1 or value > _MAX_SQLITE_INTEGER:
        raise ValueError(
            f"{kind} must be a positive SQLite signed 64-bit integer; "
            "booleans are not allowed"
        )
    return value


def _validate_non_negative_int64(value, kind):
    if not _is_integer(value) or value < 0 or value > _MAX_SQLITE_INTEGER:
        raise ValueError(
            f"{kind} must be a non-negative SQLite signed 64-bit integer; "
            "booleans are not allowed"
        )
    return value


def _validate_ticket_id(value):
    if not _is_integer(value) or value < 1 or value > _MAX_SQLITE_INTEGER:
        raise ValueError(
            "ticket_id must be a positive SQLite signed 64-bit integer; "
            "booleans are not allowed"
        )
    return value


def _normalize_maintenance_keys(keys):
    """Validate a maintenance ticket's resource keys as an ordered tuple."""
    if not isinstance(keys, tuple):
        raise ValueError("keys must be a tuple of 1 to 8 resource keys")
    if not 1 <= len(keys) <= MAX_MAINTENANCE_KEYS:
        raise ValueError(
            f"keys must contain 1 to {MAX_MAINTENANCE_KEYS} resource keys"
        )
    seen = set()
    for key in keys:
        _validate_name(key, "resource key")
        if key in seen:
            raise ValueError(f"keys contains duplicate resource key {key!r}")
        seen.add(key)
    return tuple(keys)


def _validate_maintenance_receipt(receipt):
    if receipt is None:
        return None
    if not isinstance(receipt, str) or _RECEIPT_RE.fullmatch(receipt) is None:
        raise ValueError(
            "receipt must be 1-128 ASCII letters, digits, underscores, or "
            "hyphens"
        )
    return receipt


def _validate_maintenance_window(now, until):
    _validate_non_negative_int64(now, "now")
    _validate_non_negative_int64(until, "until")
    if until <= now:
        raise ValueError("until must be strictly greater than now")
    return now, until


def _normalize_sinks(sinks):
    """Return a de-duplicated tuple of registered sink names.

    ``sinks`` is optional and defaults to "no sinks".  A tuple (not a set) is
    required so ordering and duplicate detection stay explicit.
    """
    if sinks is None:
        return ()
    if not isinstance(sinks, tuple):
        raise ValueError("sinks must be a tuple of registered sink names")
    if len(sinks) > MAX_SINKS:
        raise ValueError(f"at most {MAX_SINKS} sinks may be requested")
    names = []
    seen = set()
    for name in sinks:
        _validate_name(name, "sink name")
        if name in seen:
            raise ValueError(f"sinks contains duplicate name {name!r}")
        seen.add(name)
        names.append(name)
    return tuple(names)


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
            db.execute(
                "CREATE INDEX IF NOT EXISTS idx_jobs_resource_key_expires "
                "ON jobs(resource_key, expires)"
            )
            self._create_graph_schema(db)
            self._migrate_graph_nodes(db)
            self._create_sink_schema(db)
            self._create_feed_schema(db)
            self._create_maintenance_schema(db)
            db.commit()

    @staticmethod
    def _create_graph_schema(db):
        db.execute(
            """CREATE TABLE IF NOT EXISTS graph_workflows (
                workflow_id TEXT PRIMARY KEY
            )"""
        )
        db.execute(
            """CREATE TABLE IF NOT EXISTS graph_nodes (
                job_id INTEGER PRIMARY KEY,
                workflow_id TEXT NOT NULL,
                name TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending'
                    CHECK (status IN (
                        'pending', 'succeeded', 'failed', 'skipped'
                    )),
                outcomes INTEGER NOT NULL DEFAULT 0,
                UNIQUE (workflow_id, name)
            )"""
        )
        db.execute(
            """CREATE TABLE IF NOT EXISTS graph_dependencies (
                node_id INTEGER NOT NULL,
                prerequisite_id INTEGER NOT NULL,
                kind TEXT NOT NULL CHECK (kind IN ('requires', 'after')),
                PRIMARY KEY (node_id, prerequisite_id)
            )"""
        )
        db.execute(
            "CREATE INDEX IF NOT EXISTS idx_graph_nodes_workflow "
            "ON graph_nodes(workflow_id)"
        )
        db.execute(
            "CREATE INDEX IF NOT EXISTS idx_graph_dependencies_prerequisite "
            "ON graph_dependencies(prerequisite_id)"
        )

    @staticmethod
    def _create_sink_schema(db):
        # Sink names use SQLite's default BINARY collation, so "Index" and
        # "index" are distinct, case-sensitive keys.  Unregistered workflows
        # and ordinary jobs have no graph_node_sinks rows and never allocate
        # any outcome storage.
        db.execute(
            """CREATE TABLE IF NOT EXISTS sink_registry (
                name TEXT PRIMARY KEY,
                capacity INTEGER NOT NULL
                    CHECK (capacity BETWEEN 1 AND 12000),
                charged INTEGER NOT NULL DEFAULT 0
            )"""
        )
        db.execute(
            """CREATE TABLE IF NOT EXISTS sink_watermark (
                sink_name TEXT PRIMARY KEY,
                head_seq INTEGER NOT NULL DEFAULT 0
            )"""
        )
        db.execute(
            """CREATE TABLE IF NOT EXISTS graph_node_sinks (
                node_id INTEGER NOT NULL,
                sink_name TEXT NOT NULL
            )"""
        )
        db.execute(
            "CREATE INDEX IF NOT EXISTS idx_graph_node_sinks_sink "
            "ON graph_node_sinks(sink_name, node_id)"
        )
        db.execute(
            "CREATE INDEX IF NOT EXISTS idx_graph_node_sinks_node "
            "ON graph_node_sinks(node_id, sink_name)"
        )
        # A node taking part in outcome delivery gets exactly one persistent
        # outbox row.  Acknowledged rows are deleted, so a generated rowid
        # (the outcome "seq") is never reused while any row survives, and
        # AUTOINCREMENT keeps the high-water mark even across full deletes.
        # This is what keeps delivery storage bounded: confirmed outcomes
        # vanish once every sink has acknowledged them.
        db.execute(
            """CREATE TABLE IF NOT EXISTS outcome_stream (
                seq INTEGER PRIMARY KEY AUTOINCREMENT,
                node_id INTEGER NOT NULL,
                workflow_id TEXT NOT NULL,
                name TEXT NOT NULL,
                job_id INTEGER NOT NULL,
                status TEXT NOT NULL
                    CHECK (status IN ('succeeded', 'failed', 'skipped')),
                UNIQUE (node_id)
            )"""
        )
        db.execute(
            """CREATE TABLE IF NOT EXISTS outcome_deliveries (
                sink_name TEXT NOT NULL,
                seq INTEGER NOT NULL,
                node_id INTEGER NOT NULL,
                PRIMARY KEY (sink_name, seq),
                UNIQUE (sink_name, node_id)
            )"""
        )
        db.execute(
            "CREATE INDEX IF NOT EXISTS idx_outcome_deliveries_node "
            "ON outcome_deliveries(node_id)"
        )

    @staticmethod
    def _create_feed_schema(db):
        # One row per configured archive partition.  ``position`` preserves the
        # registration tuple order for diagnostics; scheduling order is always
        # the ASCII partition-name tie-break defined by publish_ready().
        db.execute(
            """CREATE TABLE IF NOT EXISTS archive_feed_sources (
                partition TEXT PRIMARY KEY,
                position INTEGER NOT NULL UNIQUE,
                through_offset INTEGER NOT NULL DEFAULT 0
                    CHECK (through_offset >= 0),
                watermark INTEGER NOT NULL DEFAULT -1
                    CHECK (watermark >= -1)
            )"""
        )
        # Rows are retained only until their sealed prefix is published into
        # jobs.  The primary key gives idempotent identity lookup; the partial
        # index lets publish_ready seek the global ready frontier without
        # scanning unsealed or future events.
        db.execute(
            """CREATE TABLE IF NOT EXISTS archive_events (
                partition TEXT NOT NULL,
                source_offset INTEGER NOT NULL
                    CHECK (source_offset > 0),
                event_time INTEGER NOT NULL
                    CHECK (event_time >= 0),
                resource_key TEXT NOT NULL,
                needs_mask INTEGER NOT NULL DEFAULT 0,
                payload TEXT,
                sealed INTEGER NOT NULL DEFAULT 0
                    CHECK (sealed IN (0, 1)),
                PRIMARY KEY (partition, source_offset)
            )"""
        )
        db.execute(
            """CREATE INDEX IF NOT EXISTS idx_archive_events_ready
                ON archive_events(
                    event_time, partition, source_offset
                )
                WHERE sealed = 1"""
        )

    @staticmethod
    def _create_maintenance_schema(db):
        # ``state`` values:
        #   'waiting'   - queued for handoff; blocks new leases on its keys
        #   'handed'    - currently holds the exclusive lease on its keys
        # A ticket whose ``until <= now`` is naturally dead and no longer
        # blocks anything; such rows may be reclaimed lazily.
        #
        # ``ticket_keys`` is the association table used for per-key lookups.
        # A ticket's earliest live ticket on a shared key holds precedence; the
        # unique (resource_key) is intentionally NOT declared because waiting
        # tickets may freely share keys.
        db.execute(
            """CREATE TABLE IF NOT EXISTS maintenance_tickets (
                ticket_id INTEGER PRIMARY KEY AUTOINCREMENT,
                state TEXT NOT NULL
                    CHECK (state IN ('waiting', 'handed')),
                receipt TEXT,
                requested_at INTEGER NOT NULL,
                until INTEGER NOT NULL
            )"""
        )
        db.execute(
            """CREATE TABLE IF NOT EXISTS maintenance_ticket_keys (
                ticket_id INTEGER NOT NULL,
                resource_key TEXT NOT NULL,
                PRIMARY KEY (ticket_id, resource_key)
            )"""
        )
        # Lookup by key is the hot path for both take()/take_batch() (is this
        # key paused?) and take_maintenance() (is there an earlier live ticket
        # on this key?).  The index lets both seek one key instead of scanning
        # unrelated tickets.
        db.execute(
            """CREATE INDEX IF NOT EXISTS idx_maint_keys_key_ticket
                ON maintenance_ticket_keys(resource_key, ticket_id)"""
        )
        # Expired tickets are pruned lazily via a range scan on ``until``; an
        # empty range (everything still live) costs nothing.
        db.execute(
            "CREATE INDEX IF NOT EXISTS idx_maint_tickets_until "
            "ON maintenance_tickets(until)"
        )
        # A single-row counter of stored (i.e. live) tickets keeps the admission
        # check O(1) even with 12,000 rows.  Expired rows are pruned on each
        # request and subtracted here, so the counter tracks live capacity.
        db.execute(
            """CREATE TABLE IF NOT EXISTS maintenance_meta (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                live_count INTEGER NOT NULL DEFAULT 0
                    CHECK (live_count >= 0)
            )"""
        )
        db.execute(
            "INSERT OR IGNORE INTO maintenance_meta(id, live_count) VALUES (1, 0)"
        )
        # Reconcile the counter with the stored rows so a database opened by an
        # older/newer build (or by hand) cannot inherit a drifted count.  The
        # invariant is: live_count == COUNT(*) of maintenance_tickets.  Expired
        # rows still stored count here; the next request/end prunes them.
        db.execute(
            "UPDATE maintenance_meta SET live_count = "
            "(SELECT COUNT(*) FROM maintenance_tickets) WHERE id = 1"
        )

    @staticmethod
    def _migrate_graph_nodes(db):
        """Add the outcome marker to graph tables created by older versions.

        ``ALTER ... DEFAULT 1`` marks every pre-existing node as already
        recorded, so upgrading a live database never re-emits outcomes for
        its historical terminal nodes.
        """
        columns = db.execute("PRAGMA table_info(graph_nodes)").fetchall()
        names = {column[1] for column in columns}
        if columns and "outcomes" not in names:
            db.execute(
                "ALTER TABLE graph_nodes "
                "ADD COLUMN outcomes INTEGER NOT NULL DEFAULT 1"
            )

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
        _validate_payload(payload)

        with closing(self._connect()) as db, db:
            job_id = db.execute(
                "INSERT INTO jobs(payload, resource_key, needs_mask) "
                "VALUES (?, ?, ?)",
                (payload, key, needs_mask),
            ).lastrowid
            return int(job_id)

    def configure_feed(self, partitions):
        """Register the single static archive partition group for this database."""
        if not isinstance(partitions, tuple):
            raise ValueError(
                "partitions must be a tuple containing 1 to 8 distinct names"
            )
        if not 1 <= len(partitions) <= MAX_FEED_PARTITIONS:
            raise ValueError(
                f"partitions must contain 1 to {MAX_FEED_PARTITIONS} names"
            )

        seen = set()
        normalized = []
        for position, name in enumerate(partitions):
            _validate_name(name, "partition name")
            if name in seen:
                raise ValueError(f"partition name {name!r} is duplicated")
            seen.add(name)
            normalized.append((name, position))

        with closing(self._connect()) as db:
            db.execute("BEGIN IMMEDIATE")
            try:
                existing = [
                    row[0]
                    for row in db.execute(
                        "SELECT partition FROM archive_feed_sources "
                        "ORDER BY position"
                    )
                ]
                if existing:
                    if set(existing) != seen:
                        raise ValueError(
                            "this database already has a different archive "
                            "partition group"
                        )
                    db.rollback()
                    return False

                db.executemany(
                    "INSERT INTO archive_feed_sources"
                    "(partition, position, through_offset, watermark) "
                    "VALUES (?, ?, 0, -1)",
                    normalized,
                )
                db.commit()
            except BaseException:
                db.rollback()
                raise
        return True

    @staticmethod
    def _require_feed_partition(db, partition):
        row = db.execute(
            "SELECT through_offset, watermark "
            "FROM archive_feed_sources WHERE partition = ?",
            (partition,),
        ).fetchone()
        if row is None:
            configured = db.execute(
                "SELECT 1 FROM archive_feed_sources LIMIT 1"
            ).fetchone()
            if configured is None:
                raise ValueError("archive feed partitions have not been configured")
            raise ValueError(
                f"partition {partition!r} is not in the configured archive feed"
            )
        return int(row[0]), int(row[1])

    def source_checkpoints(self):
        """Return ``{partition: (through, watermark)}`` for the feed group."""
        with closing(self._connect()) as db:
            rows = db.execute(
                "SELECT partition, through_offset, watermark "
                "FROM archive_feed_sources ORDER BY position"
            ).fetchall()
        if not rows:
            raise ValueError("archive feed partitions have not been configured")
        return {
            partition: (int(through), int(watermark))
            for partition, through, watermark in rows
        }

    def stage_event(
        self,
        partition,
        offset,
        *,
        event_time,
        key,
        needs,
        payload,
    ):
        """Idempotently stage an archive event without creating a queue job."""
        _validate_name(partition, "partition name")
        _validate_positive_int64(offset, "offset")
        _validate_non_negative_int64(event_time, "event_time")
        _validate_name(key, "resource key")
        needs_mask = _capability_mask(needs, nullable=True)
        _validate_payload(payload)

        with closing(self._connect()) as db:
            db.execute("BEGIN IMMEDIATE")
            try:
                through, watermark = self._require_feed_partition(db, partition)

                # A checkpoint is the truncation promise.  Retries inside its
                # prefix are acknowledged from that promise alone, even after
                # the corresponding staging rows were published and removed.
                if offset <= through:
                    db.rollback()
                    return False

                existing = db.execute(
                    "SELECT event_time, resource_key, needs_mask, payload "
                    "FROM archive_events "
                    "WHERE partition = ? AND source_offset = ?",
                    (partition, offset),
                ).fetchone()
                if existing is not None:
                    if tuple(existing) != (
                        event_time,
                        key,
                        needs_mask,
                        payload,
                    ):
                        raise ValueError(
                            f"event identity ({partition!r}, {offset}) was "
                            "already staged with different content"
                        )
                    db.rollback()
                    return False

                if offset > through + MAX_FEED_PENDING:
                    raise ValueError(
                        f"offset is more than {MAX_FEED_PENDING} beyond the "
                        "partition's sealed offset"
                    )
                if event_time <= watermark:
                    raise ValueError(
                        "new event_time must be strictly greater than the "
                        "partition's current watermark"
                    )

                db.execute(
                    """INSERT INTO archive_events(
                            partition, source_offset, event_time,
                            resource_key, needs_mask, payload, sealed
                        ) VALUES (?, ?, ?, ?, ?, ?, 0)""",
                    (
                        partition,
                        offset,
                        event_time,
                        key,
                        needs_mask,
                        payload,
                    ),
                )
                db.commit()
            except BaseException:
                db.rollback()
                raise
        return True

    def seal_source(self, partition, *, through, watermark):
        """Persist a source truncation/watermark checkpoint."""
        _validate_name(partition, "partition name")
        _validate_non_negative_int64(through, "through")
        _validate_non_negative_int64(watermark, "watermark")

        with closing(self._connect()) as db:
            db.execute("BEGIN IMMEDIATE")
            try:
                current_through, current_watermark = (
                    self._require_feed_partition(db, partition)
                )

                if through < current_through or watermark < current_watermark:
                    raise ValueError("source checkpoints cannot move backwards")
                if through == current_through and watermark == current_watermark:
                    db.rollback()
                    return False
                if through > current_through + MAX_FEED_PENDING:
                    raise ValueError(
                        f"through can advance by at most {MAX_FEED_PENDING}"
                    )

                if through > current_through:
                    expected = through - current_through
                    present, late = db.execute(
                        """SELECT COUNT(*),
                                  COALESCE(SUM(
                                      CASE WHEN event_time > ? THEN 1 ELSE 0 END
                                  ), 0)
                             FROM archive_events
                            WHERE partition = ?
                              AND source_offset > ?
                              AND source_offset <= ?""",
                        (
                            watermark,
                            partition,
                            current_through,
                            through,
                        ),
                    ).fetchone()
                    if int(present) != expected:
                        raise ValueError(
                            "sealed offset prefix is missing one or more events"
                        )
                    if int(late):
                        raise ValueError(
                            "one or more events in the sealed prefix are later "
                            "than the proposed watermark"
                        )

                future_conflict = db.execute(
                    """SELECT 1
                         FROM archive_events
                        WHERE partition = ?
                          AND source_offset > ?
                          AND event_time <= ?
                        LIMIT 1""",
                    (partition, through, watermark),
                ).fetchone()
                if future_conflict is not None:
                    raise ValueError(
                        "a staged event beyond through is not strictly after "
                        "the proposed watermark"
                    )

                if through > current_through:
                    unfinished = db.execute(
                        """SELECT 1
                             FROM archive_events
                            WHERE partition = ? AND sealed = 1
                            LIMIT 1""",
                        (partition,),
                    ).fetchone()
                    if unfinished is not None:
                        raise BufferError(
                            "the previous sealed prefix has not all been "
                            "published into jobs yet"
                        )

                db.execute(
                    "UPDATE archive_feed_sources "
                    "SET through_offset = ?, watermark = ? "
                    "WHERE partition = ?",
                    (through, watermark, partition),
                )
                if through > current_through:
                    db.execute(
                        """UPDATE archive_events
                              SET sealed = 1
                            WHERE partition = ?
                              AND source_offset > ?
                              AND source_offset <= ?""",
                        (partition, current_through, through),
                    )
                db.commit()
            except BaseException:
                db.rollback()
                raise
        return True

    def publish_ready(self, *, limit):
        """Publish a small globally ordered batch of ready sealed events."""
        if not _is_integer(limit) or isinstance(limit, bool):
            raise ValueError(
                "limit must be an integer; booleans are not allowed"
            )
        if not 1 <= limit <= MAX_PUBLISH_LIMIT:
            raise ValueError(
                f"limit must be between 1 and {MAX_PUBLISH_LIMIT}"
            )

        with closing(self._connect()) as db:
            db.execute("BEGIN IMMEDIATE")
            try:
                safe_time = db.execute(
                    "SELECT MIN(watermark) FROM archive_feed_sources"
                ).fetchone()[0]
                if safe_time is None:
                    raise ValueError(
                        "archive feed partitions have not been configured"
                    )

                # This uses the partial index on sealed rows and stops after
                # ``limit`` entries; payloads are fetched only for winners.
                ready = db.execute(
                    """SELECT rowid, partition, source_offset
                         FROM archive_events
                        WHERE sealed = 1
                          AND event_time <= ?
                        ORDER BY event_time ASC,
                                 partition ASC,
                                 source_offset ASC
                        LIMIT ?""",
                    (int(safe_time), limit),
                ).fetchall()
                if not ready:
                    db.rollback()
                    return []

                rowids = [row[0] for row in ready]
                placeholders = ",".join("?" for _ in rowids)
                content_rows = db.execute(
                    f"""SELECT rowid, resource_key, needs_mask, payload
                          FROM archive_events
                         WHERE rowid IN ({placeholders})""",
                    rowids,
                ).fetchall()
                content_by_rowid = {
                    row[0]: (row[1], int(row[2]), row[3])
                    for row in content_rows
                }

                published = []
                for rowid, partition, offset in ready:
                    resource_key, needs_mask, payload = content_by_rowid[rowid]
                    job_id = db.execute(
                        "INSERT INTO jobs"
                        "(payload, resource_key, needs_mask) "
                        "VALUES (?, ?, ?)",
                        (payload, resource_key, needs_mask),
                    ).lastrowid
                    deleted = db.execute(
                        "DELETE FROM archive_events WHERE rowid = ?",
                        (rowid,),
                    )
                    if deleted.rowcount != 1:
                        raise sqlite3.OperationalError(
                            "archive event selection no longer matched staging row"
                        )
                    published.append((partition, int(offset), int(job_id)))

                db.commit()
            except BaseException:
                db.rollback()
                raise
        return published

    @staticmethod
    def _validate_reference_list(value, field):
        if not isinstance(value, list):
            raise ValueError(f"{field} must be a list of node names")
        names = []
        seen = set()
        for reference in value:
            _validate_name(reference, "dependency node name")
            if reference in seen:
                raise ValueError(f"{field} contains duplicate node {reference!r}")
            seen.add(reference)
            names.append(reference)
        return names, seen

    @classmethod
    def _validate_graph_nodes(cls, nodes):
        if not isinstance(nodes, list) or not nodes:
            raise ValueError("nodes must be a non-empty list")
        if len(nodes) > MAX_GRAPH_NODES:
            raise ValueError(
                f"a graph may contain at most {MAX_GRAPH_NODES} nodes"
            )

        required_keys = {
            "name",
            "key",
            "payload",
            "needs",
            "requires",
            "after",
        }
        normalized = []
        node_names = set()
        dependency_count = 0

        for raw_node in nodes:
            if not isinstance(raw_node, dict):
                raise ValueError("each graph node must be a dictionary")
            missing = required_keys.difference(raw_node)
            if missing:
                raise ValueError(
                    "graph node is missing required fields: "
                    + ", ".join(sorted(missing))
                )

            name = raw_node["name"]
            _validate_name(name, "node name")
            if name in node_names:
                raise ValueError(f"duplicate graph node name: {name!r}")

            key = raw_node["key"]
            _validate_name(key, "resource key")
            payload = raw_node["payload"]
            _validate_payload(payload)
            needs_mask = _capability_mask(raw_node["needs"], nullable=True)

            requires, requires_set = cls._validate_reference_list(
                raw_node["requires"], "requires"
            )
            after, after_set = cls._validate_reference_list(
                raw_node["after"], "after"
            )
            overlapping = requires_set.intersection(after_set)
            if overlapping:
                raise ValueError(
                    "a node cannot list the same prerequisite in both "
                    "requires and after: "
                    + ", ".join(sorted(overlapping))
                )

            dependency_count += len(requires) + len(after)
            if dependency_count > MAX_GRAPH_DEPENDENCIES:
                raise ValueError(
                    "a graph may contain at most "
                    f"{MAX_GRAPH_DEPENDENCIES} explicit dependencies"
                )

            normalized.append(
                {
                    "name": name,
                    "key": key,
                    "payload": payload,
                    "needs_mask": needs_mask,
                    "requires": requires,
                    "after": after,
                }
            )
            node_names.add(name)

        for node in normalized:
            for reference in node["requires"] + node["after"]:
                if reference == node["name"]:
                    raise ValueError(
                        f"node {node['name']!r} cannot depend on itself"
                    )
                if reference not in node_names:
                    raise ValueError(
                        f"node {node['name']!r} references unknown node "
                        f"{reference!r}"
                    )

        cls._reject_graph_cycle(normalized)
        return normalized

    @staticmethod
    def _reject_graph_cycle(nodes):
        count = len(nodes)
        index_by_name = {node["name"]: index for index, node in enumerate(nodes)}
        adjacency = [[] for _ in range(count)]
        indegree = [0] * count

        def add_edge(prerequisite_index, dependent_index):
            adjacency[prerequisite_index].append(dependent_index)
            indegree[dependent_index] += 1

        for dependent_index, node in enumerate(nodes):
            for reference in node["requires"]:
                add_edge(index_by_name[reference], dependent_index)
            for reference in node["after"]:
                add_edge(index_by_name[reference], dependent_index)

        # Resource-key FIFO gives an earlier ID an ordering edge to later
        # nodes on the same key.  Only consecutive nodes on that key need to
        # be connected; transitive ordering handles the remaining pairs.
        previous_by_key = {}
        for index, node in enumerate(nodes):
            previous_index = previous_by_key.get(node["key"])
            if previous_index is not None:
                add_edge(previous_index, index)
            previous_by_key[node["key"]] = index

        ready = deque(index for index, degree in enumerate(indegree) if degree == 0)
        visited = 0
        while ready:
            node_index = ready.popleft()
            visited += 1
            for dependent_index in adjacency[node_index]:
                indegree[dependent_index] -= 1
                if indegree[dependent_index] == 0:
                    ready.append(dependent_index)

        if visited != count:
            raise ValueError("graph dependencies contain a cycle")

    def submit_graph(self, workflow_id, nodes, *, sinks=None):
        _validate_name(workflow_id, "workflow ID")
        normalized_nodes = self._validate_graph_nodes(nodes)
        sink_names = _normalize_sinks(sinks)

        with closing(self._connect()) as db:
            db.execute("BEGIN IMMEDIATE")
            try:
                try:
                    db.execute(
                        "INSERT INTO graph_workflows(workflow_id) VALUES (?)",
                        (workflow_id,),
                    )
                except sqlite3.IntegrityError as exc:
                    raise ValueError(
                        f"workflow_id {workflow_id!r} has already been used"
                    ) from exc

                node_count = len(normalized_nodes)
                self._reserve_sink_capacity(db, sink_names, node_count)

                ids_by_name = {}
                for node in normalized_nodes:
                    job_id = db.execute(
                        "INSERT INTO jobs(payload, resource_key, needs_mask) "
                        "VALUES (?, ?, ?)",
                        (
                            node["payload"],
                            node["key"],
                            node["needs_mask"],
                        ),
                    ).lastrowid
                    job_id = int(job_id)
                    ids_by_name[node["name"]] = job_id
                    db.execute(
                        "INSERT INTO graph_nodes"
                        "(job_id, workflow_id, name, status, outcomes) "
                        "VALUES (?, ?, ?, 'pending', 0)",
                        (job_id, workflow_id, node["name"]),
                    )
                    if sink_names:
                        db.executemany(
                            "INSERT INTO graph_node_sinks(node_id, sink_name) "
                            "VALUES (?, ?)",
                            [(job_id, name) for name in sink_names],
                        )

                dependency_rows = []
                for node in normalized_nodes:
                    dependent_id = ids_by_name[node["name"]]
                    for reference in node["requires"]:
                        dependency_rows.append(
                            (dependent_id, ids_by_name[reference], "requires")
                        )
                    for reference in node["after"]:
                        dependency_rows.append(
                            (dependent_id, ids_by_name[reference], "after")
                        )
                db.executemany(
                    "INSERT INTO graph_dependencies"
                    "(node_id, prerequisite_id, kind) VALUES (?, ?, ?)",
                    dependency_rows,
                )
                db.commit()
            except BaseException:
                db.rollback()
                raise

            return ids_by_name

    @staticmethod
    def _reserve_sink_capacity(db, sink_names, node_count):
        """Reserve ``node_count`` slots on each named sink or raise.

        Unknown sinks raise ``ValueError`` even when some sink is full.
        Insufficient capacity raises ``BufferError``.  The whole reservation
        lives inside the caller's transaction, so two instances racing for the
        last slot cannot both commit.
        """
        for name in sink_names:
            row = db.execute(
                "SELECT capacity, charged FROM sink_registry WHERE name = ?",
                (name,),
            ).fetchone()
            if row is None:
                raise ValueError(f"sink {name!r} has not been registered")
        for name in sink_names:
            row = db.execute(
                "SELECT capacity, charged FROM sink_registry WHERE name = ?",
                (name,),
            ).fetchone()
            capacity, charged = row
            if charged + node_count > capacity:
                raise BufferError(
                    f"sink {name!r} has no room for {node_count} node(s): "
                    f"{charged}/{capacity} reserved"
                )
        db.executemany(
            "UPDATE sink_registry SET charged = charged + ? WHERE name = ?",
            [(node_count, name) for name in sink_names],
        )

    def register_sink(self, name, *, capacity):
        _validate_name(name, "sink name")
        _validate_capacity(capacity)

        with closing(self._connect()) as db:
            db.execute("BEGIN IMMEDIATE")
            try:
                row = db.execute(
                    "SELECT capacity FROM sink_registry WHERE name = ?",
                    (name,),
                ).fetchone()
                if row is not None:
                    if row[0] == capacity:
                        db.rollback()
                        return False
                    raise ValueError(
                        f"sink {name!r} is already registered with capacity "
                        f"{row[0]}, not {capacity}"
                    )

                count = db.execute(
                    "SELECT COUNT(*) FROM sink_registry"
                ).fetchone()[0]
                if count >= MAX_SINKS:
                    raise ValueError(
                        f"at most {MAX_SINKS} sinks may be registered"
                    )
                db.execute(
                    "INSERT INTO sink_registry(name, capacity, charged) "
                    "VALUES (?, ?, 0)",
                    (name, capacity),
                )
                db.execute(
                    "INSERT INTO sink_watermark(sink_name, head_seq) "
                    "VALUES (?, 0)",
                    (name,),
                )
                db.commit()
            except BaseException:
                db.rollback()
                raise
            return True

    @staticmethod
    def _require_registered_sink(db, name):
        row = db.execute(
            "SELECT 1 FROM sink_registry WHERE name = ?", (name,)
        ).fetchone()
        if row is None:
            raise ValueError(f"sink {name!r} has not been registered")

    def read_outcomes(self, name, *, limit):
        _validate_name(name, "sink name")
        if not _is_integer(limit):
            raise ValueError("limit must be an integer; booleans are not allowed")
        if limit < 1 or limit > MAX_OUTCOME_LIMIT:
            raise ValueError(
                f"limit must be between 1 and {MAX_OUTCOME_LIMIT}"
            )

        with closing(self._connect()) as db:
            self._require_registered_sink(db, name)
            rows = db.execute(
                """SELECT os.seq, os.workflow_id, os.name, os.job_id,
                          os.status
                     FROM outcome_deliveries od
                     JOIN outcome_stream os ON os.seq = od.seq
                    WHERE od.sink_name = ?
                    ORDER BY od.seq
                    LIMIT ?""",
                (name, limit),
            ).fetchall()

        return [
            {
                "seq": seq,
                "workflow_id": workflow_id,
                "name": node_name,
                "job_id": job_id,
                "status": status,
            }
            for seq, workflow_id, node_name, job_id, status in rows
        ]

    @staticmethod
    def _validate_through(through):
        if not _is_integer(through):
            raise ValueError(
                "through must be an integer; booleans are not allowed"
            )
        if through < 1 or through > _MAX_SQLITE_INTEGER:
            raise ValueError(
                "through must be between 1 and the SQLite signed 64-bit maximum"
            )
        return through

    def ack_outcomes(self, name, *, through):
        _validate_name(name, "sink name")
        through = self._validate_through(through)

        with closing(self._connect()) as db:
            db.execute("BEGIN IMMEDIATE")
            try:
                self._require_registered_sink(db, name)
                watermark = db.execute(
                    "SELECT head_seq FROM sink_watermark WHERE sink_name = ?",
                    (name,),
                ).fetchone()[0]
                if through <= watermark:
                    db.rollback()
                    return False

                # The boundary must name an outcome this sink can read right
                # now.  A seq belonging to another sink, or one not yet
                # generated, is rejected instead of silently acking a prefix.
                boundary = db.execute(
                    "SELECT 1 FROM outcome_deliveries "
                    "WHERE sink_name = ? AND seq = ?",
                    (name, through),
                ).fetchone()
                if boundary is None:
                    raise ValueError(
                        f"through={through} is not a readable outcome for "
                        f"sink {name!r}"
                    )

                released = db.execute(
                    "SELECT COUNT(*) FROM outcome_deliveries "
                    "WHERE sink_name = ? AND seq <= ?",
                    (name, through),
                ).fetchone()[0]
                db.execute(
                    """CREATE TEMP TABLE IF NOT EXISTS released_nodes (
                        node_id INTEGER PRIMARY KEY
                    )"""
                )
                db.execute("DELETE FROM released_nodes")
                db.execute(
                    """INSERT OR IGNORE INTO released_nodes(node_id)
                       SELECT node_id FROM outcome_deliveries
                        WHERE sink_name = ? AND seq <= ?""",
                    (name, through),
                )
                db.execute(
                    "UPDATE sink_registry SET charged = charged - ? "
                    "WHERE name = ?",
                    (released, name),
                )
                db.execute(
                    "DELETE FROM outcome_deliveries "
                    "WHERE sink_name = ? AND seq <= ?",
                    (name, through),
                )
                db.execute(
                    "UPDATE sink_watermark SET head_seq = ? WHERE sink_name = ?",
                    (through, name),
                )
                self._collect_garbage_outcomes(db)
                db.commit()
            except BaseException:
                db.rollback()
                raise
            return True

    @staticmethod
    def _collect_garbage_outcomes(db):
        # Once every requested sink has acknowledged an outcome, its stream
        # row is dead storage.  Delete it now so the outbox never grows with
        # the number of already confirmed outcomes.  graph_nodes keeps the
        # terminal status for graph_status().
        #
        # Only nodes whose delivery row was just removed can have become
        # garbage, so the scan is limited to ``released_nodes`` instead of
        # walking the entire stream on every ack.
        db.execute(
            """DELETE FROM outcome_stream
                WHERE node_id IN (SELECT node_id FROM released_nodes)
                  AND NOT EXISTS (
                        SELECT 1 FROM outcome_deliveries od
                         WHERE od.node_id = outcome_stream.node_id
                  )"""
        )
        db.execute("DELETE FROM released_nodes")

    @staticmethod
    def _record_outcomes(db, workflow_id, terminal_statuses):
        """Append outcomes for newly terminal nodes of ``workflow_id``.

        Newly terminal nodes are collected in a connection-local temp table
        ranked by delivery order, then inserted into the outbox in that order.
        A failing node is already terminal before this helper runs, so it
        ranks first; nodes skipped in the same transaction follow in job-ID
        order.  The AUTOINCREMENT high-water mark is never reused, so separate
        transactions receive strictly larger seq values in commit order.

        ``graph_nodes.outcomes`` is a permanent per-node marker.  It is
        deliberately not reclaimed: the outbox rows themselves are deleted
        after every sink acknowledges, and a later failure in the same
        workflow must not re-emit an already delivered outcome.
        """
        status_filter = " OR ".join(
            "gn.status = ?" for _ in terminal_statuses
        )
        db.execute(
            """CREATE TEMP TABLE IF NOT EXISTS newly_recorded (
                job_id INTEGER PRIMARY KEY,
                rank INTEGER NOT NULL
            )"""
        )
        db.execute("DELETE FROM newly_recorded")
        db.execute(
            f"""INSERT INTO newly_recorded(job_id, rank)
               SELECT gn.job_id,
                      ROW_NUMBER() OVER (
                          ORDER BY (gn.status <> 'failed'), gn.job_id
                      )
                 FROM graph_nodes gn
                WHERE gn.workflow_id = ?
                  AND gn.outcomes = 0
                  AND ({status_filter})
                  AND EXISTS (
                        SELECT 1 FROM graph_node_sinks gns
                         WHERE gns.node_id = gn.job_id
                      )""",
            (workflow_id, *terminal_statuses),
        )
        db.execute(
            """UPDATE graph_nodes SET outcomes = 1
                WHERE job_id IN (SELECT job_id FROM newly_recorded)"""
        )
        db.execute(
            """INSERT INTO outcome_stream
                   (node_id, workflow_id, name, job_id, status)
               SELECT gn.job_id, gn.workflow_id, gn.name, gn.job_id, gn.status
                 FROM newly_recorded nr
                 JOIN graph_nodes gn ON gn.job_id = nr.job_id
                ORDER BY nr.rank"""
        )
        db.execute(
            """INSERT INTO outcome_deliveries(sink_name, seq, node_id)
               SELECT gns.sink_name, os.seq, os.node_id
                 FROM outcome_stream os
                 JOIN graph_node_sinks gns ON gns.node_id = os.node_id
                 JOIN newly_recorded nr ON nr.job_id = os.node_id
                ORDER BY os.seq, gns.sink_name"""
        )

    def _handle_graph_terminal(self, db, job_id, status):
        """Record the outcome(s) for a node that just became terminal.

        Runs in the caller's terminal transaction.  Capacity is never
        consulted here: a workflow admitted at submit time must be able to
        finish even when its sinks have become full, and a failure that
        cascades skips must not request more room.
        """
        workflow_id = db.execute(
            "SELECT workflow_id FROM graph_nodes WHERE job_id = ?",
            (job_id,),
        ).fetchone()[0]
        if status == "succeeded":
            # Success only releases ``requires`` dependents; it never
            # cascades skips, so only the node itself produces an outcome.
            self._record_outcomes(db, workflow_id, ("succeeded",))
        else:
            # A failure makes skip propagation visible in the same
            # transaction; delivery order puts the failed node first.
            self._record_outcomes(db, workflow_id, ("failed", "skipped"))

    def take(self, *, now, lease):
        _validate_clock(now, lease)

        with closing(self._connect()) as db, db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute(
                """SELECT j.id, j.payload
                     FROM jobs j
                    WHERE j.expires <= ?
                      AND j.needs_mask = 0
                      AND NOT EXISTS (
                            SELECT 1
                              FROM maintenance_ticket_keys mtk
                              JOIN maintenance_tickets mt
                                ON mt.ticket_id = mtk.ticket_id
                             WHERE mtk.resource_key = j.resource_key
                               AND mt.until > ?
                          )
                      AND NOT EXISTS (
                            SELECT 1
                              FROM graph_dependencies gd
                              LEFT JOIN graph_nodes gp
                                ON gp.job_id = gd.prerequisite_id
                             WHERE gd.node_id = j.id
                               AND (
                                    (
                                        gd.kind = 'requires'
                                        AND (
                                            gp.job_id IS NULL
                                            OR gp.status <> 'succeeded'
                                        )
                                    )
                                    OR (
                                        gd.kind = 'after'
                                        AND (
                                            gp.job_id IS NULL
                                            OR gp.status = 'pending'
                                        )
                                    )
                               )
                          )
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
                (now, now),
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
                  AND NOT EXISTS (
                        SELECT 1
                          FROM maintenance_ticket_keys mtk
                          JOIN maintenance_tickets mt
                            ON mt.ticket_id = mtk.ticket_id
                         WHERE mtk.resource_key = j.resource_key
                           AND mt.until > ?
                      )
                  AND NOT EXISTS (
                        SELECT 1
                          FROM graph_dependencies gd
                          LEFT JOIN graph_nodes gp
                            ON gp.job_id = gd.prerequisite_id
                         WHERE gd.node_id = j.id
                           AND (
                                (
                                    gd.kind = 'requires'
                                    AND (
                                        gp.job_id IS NULL
                                        OR gp.status <> 'succeeded'
                                    )
                                )
                                OR (
                                    gd.kind = 'after'
                                    AND (
                                        gp.job_id IS NULL
                                        OR gp.status = 'pending'
                                    )
                                )
                           )
                      )
                  AND (
                        j.resource_key IS NULL
                        OR j.id = (
                            SELECT MIN(k.id)
                              FROM jobs k
                             WHERE k.resource_key = j.resource_key
                        )
                      )
                ORDER BY j.id""",
            (now, now),
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

    def request_maintenance(self, keys, *, now, until):
        """Pause new leases on ``keys`` and open a handoff ticket.

        Returns a per-database ticket number starting at 1 and never reused.
        The ticket blocks new deliveries on every listed key from the moment it
        is committed until it ends or ``until <= now``.  It does not cancel
        unfinished work; already-issued worker credentials remain usable.
        """
        normalized = _normalize_maintenance_keys(keys)
        _validate_maintenance_window(now, until)

        with closing(self._connect()) as db:
            db.execute("BEGIN IMMEDIATE")
            try:
                self._prune_expired_tickets(db, now)
                live = db.execute(
                    "SELECT live_count FROM maintenance_meta WHERE id = 1"
                ).fetchone()[0]
                if int(live) >= MAX_MAINTENANCE_TICKETS:
                    raise BufferError(
                        "at most "
                        f"{MAX_MAINTENANCE_TICKETS} live maintenance tickets "
                        "are allowed"
                    )
                ticket_id = db.execute(
                    "INSERT INTO maintenance_tickets"
                    "(state, receipt, requested_at, until) "
                    "VALUES ('waiting', NULL, ?, ?)",
                    (now, until),
                ).lastrowid
                ticket_id = int(ticket_id)
                db.executemany(
                    "INSERT INTO maintenance_ticket_keys"
                    "(ticket_id, resource_key) VALUES (?, ?)",
                    [(ticket_id, key) for key in normalized],
                )
                db.execute(
                    "UPDATE maintenance_meta SET live_count = live_count + 1 "
                    "WHERE id = 1"
                )
                db.commit()
            except BaseException:
                db.rollback()
                raise
        return ticket_id

    def take_maintenance(self, ticket_id, *, now):
        """Attempt to hand ``ticket_id`` the exclusive lease on all its keys.

        Succeeds only when the ticket is still waiting, no listed key has a
        valid worker lease (``expires > now``), and no earlier live ticket
        shares any listed key.  Returns ``{"ticket_id", "receipt", "until"}`` or
        ``None``.  A ticket can be handed over at most once; a lost response is
        not recovered by retrying — wait for ``until`` instead.
        """
        _validate_ticket_id(ticket_id)
        _validate_non_negative_int64(now, "now")

        with closing(self._connect()) as db:
            db.execute("BEGIN IMMEDIATE")
            try:
                # No global pruning here: a handoff concerns only its own keys,
                # so it must not scan or delete unrelated ticket rows.  Expired
                # tickets are ignored by the ``until > now`` predicates below
                # and reclaimed lazily by request/end.
                row = db.execute(
                    "SELECT state, until FROM maintenance_tickets "
                    "WHERE ticket_id = ? AND until > ?",
                    (ticket_id, now),
                ).fetchone()
                if row is None:
                    db.rollback()
                    return None
                state, until = row
                if state == "handed":
                    db.rollback()
                    return None

                keys = [
                    key
                    for (key,) in db.execute(
                        "SELECT resource_key FROM maintenance_ticket_keys "
                        "WHERE ticket_id = ? ORDER BY resource_key",
                        (ticket_id,),
                    )
                ]

                for key in keys:
                    # ``expires > now`` is an effective worker credential, which
                    # must finish (or expire) before the group is handed over.
                    leased = db.execute(
                        "SELECT 1 FROM jobs "
                        "WHERE resource_key = ? AND expires > ? LIMIT 1",
                        (key, now),
                    ).fetchone()
                    if leased is not None:
                        db.rollback()
                        return None
                    earlier = db.execute(
                        """SELECT 1
                             FROM maintenance_ticket_keys mtk
                             JOIN maintenance_tickets mt
                               ON mt.ticket_id = mtk.ticket_id
                            WHERE mtk.resource_key = ?
                              AND mt.ticket_id < ?
                              AND mt.until > ?
                            LIMIT 1""",
                        (key, ticket_id, now),
                    ).fetchone()
                    if earlier is not None:
                        db.rollback()
                        return None

                receipt = uuid.uuid4().hex
                db.execute(
                    "UPDATE maintenance_tickets "
                    "SET state = 'handed', receipt = ? "
                    "WHERE ticket_id = ? AND state = 'waiting'",
                    (receipt, ticket_id),
                )
                db.commit()
            except BaseException:
                db.rollback()
                raise
        return {
            "ticket_id": ticket_id,
            "receipt": receipt,
            "until": int(until),
        }

    def end_maintenance(self, ticket_id, *, now, receipt=None):
        """Cancel a waiting ticket (``receipt=None``) or return a handed one.

        Returns ``True`` when a live ticket was ended, releasing its keys while
        leaving every other ticket's restrictions in place.  Expired, ended or
        unknown tickets, wrong receipts, and the wrong phase all return
        ``False``.
        """
        _validate_ticket_id(ticket_id)
        _validate_non_negative_int64(now, "now")
        receipt = _validate_maintenance_receipt(receipt)

        with closing(self._connect()) as db:
            db.execute("BEGIN IMMEDIATE")
            try:
                self._prune_expired_tickets(db, now)
                row = db.execute(
                    "SELECT state, receipt FROM maintenance_tickets "
                    "WHERE ticket_id = ? AND until > ?",
                    (ticket_id, now),
                ).fetchone()
                if row is None:
                    db.rollback()
                    return False
                state, stored_receipt = row

                if state == "waiting":
                    if receipt is not None:
                        db.rollback()
                        return False
                else:  # handed
                    if receipt is None or receipt != stored_receipt:
                        db.rollback()
                        return False

                db.execute(
                    "DELETE FROM maintenance_ticket_keys WHERE ticket_id = ?",
                    (ticket_id,),
                )
                db.execute(
                    "DELETE FROM maintenance_tickets WHERE ticket_id = ?",
                    (ticket_id,),
                )
                db.execute(
                    "UPDATE maintenance_meta "
                    "SET live_count = MAX(0, live_count - 1) WHERE id = 1"
                )
                db.commit()
            except BaseException:
                db.rollback()
                raise
        return True

    @staticmethod
    def _prune_expired_tickets(db, now):
        """Delete tickets that no longer block anything at ``now``.

        Uses the ``until`` index, so a database full of live tickets costs a
        single empty range probe.  Capacity counts only stored (live) rows.
        """
        expired = db.execute(
            "SELECT ticket_id FROM maintenance_tickets WHERE until <= ?",
            (now,),
        ).fetchall()
        if not expired:
            return 0
        ids = [(int(row[0]),) for row in expired]
        db.executemany(
            "DELETE FROM maintenance_ticket_keys WHERE ticket_id = ?", ids
        )
        db.executemany(
            "DELETE FROM maintenance_tickets WHERE ticket_id = ?", ids
        )
        db.execute(
            "UPDATE maintenance_meta SET live_count = MAX(0, live_count - ?) "
            "WHERE id = 1",
            (len(ids),),
        )
        return len(ids)

    def acknowledge(self, delivery, *, now):
        with closing(self._connect()) as db, db:
            db.execute("BEGIN IMMEDIATE")
            deleted = db.execute(
                "DELETE FROM jobs "
                "WHERE id = ? AND receipt = ? AND expires > ?",
                (delivery.id, delivery.receipt, now),
            )
            if deleted.rowcount != 1:
                return False

            # Ordinary jobs have no graph_nodes row.  A graph node's terminal
            # status, queue-row deletion, dependency release and newly
            # readable outcomes all commit together.
            updated = db.execute(
                "UPDATE graph_nodes "
                "SET status = 'succeeded' "
                "WHERE job_id = ? AND status = 'pending'",
                (delivery.id,),
            )
            if updated.rowcount == 1:
                self._handle_graph_terminal(db, delivery.id, "succeeded")
            return True

    _SKIP_REQUIRES_CTE = """
        WITH RECURSIVE skipped(node_id) AS (
            SELECT gd.node_id
              FROM graph_dependencies gd
             WHERE gd.prerequisite_id = ?
               AND gd.kind = 'requires'
            UNION
            SELECT gd.node_id
              FROM graph_dependencies gd
              JOIN skipped s ON s.node_id = gd.prerequisite_id
             WHERE gd.kind = 'requires'
        )
    """

    def _skip_requires_descendants(self, db, failed_job_id):
        db.execute(
            self._SKIP_REQUIRES_CTE
            + """
            UPDATE graph_nodes
               SET status = 'skipped'
             WHERE status = 'pending'
               AND job_id IN (SELECT node_id FROM skipped)
            """,
            (failed_job_id,),
        )
        # Terminal graph nodes do not occupy key order.  Payload columns are
        # never read here; all work stays inside SQLite.
        db.execute(
            self._SKIP_REQUIRES_CTE
            + """
            DELETE FROM jobs
             WHERE id IN (SELECT node_id FROM skipped)
            """,
            (failed_job_id,),
        )

    def fail(self, delivery, now):
        if not _is_integer(now):
            raise ValueError("now must be an integer; booleans are not allowed")
        if now < 0 or now > _MAX_SQLITE_INTEGER:
            raise ValueError(
                "now must be between 0 and the SQLite signed 64-bit maximum"
            )

        with closing(self._connect()) as db, db:
            db.execute("BEGIN IMMEDIATE")
            ended = db.execute(
                """DELETE FROM jobs
                    WHERE id = ?
                      AND receipt = ?
                      AND expires > ?
                      AND EXISTS (
                            SELECT 1
                              FROM graph_nodes gn
                             WHERE gn.job_id = jobs.id
                               AND gn.status = 'pending'
                      )""",
                (delivery.id, delivery.receipt, now),
            )
            if ended.rowcount != 1:
                return False

            db.execute(
                "UPDATE graph_nodes "
                "SET status = 'failed' "
                "WHERE job_id = ? AND status = 'pending'",
                (delivery.id,),
            )
            self._skip_requires_descendants(db, delivery.id)
            self._handle_graph_terminal(db, delivery.id, "failed")
            return True

    def graph_status(self, workflow_id):
        _validate_name(workflow_id, "workflow ID")

        with closing(self._connect()) as db:
            workflow = db.execute(
                "SELECT 1 FROM graph_workflows WHERE workflow_id = ?",
                (workflow_id,),
            ).fetchone()
            if workflow is None:
                raise KeyError(workflow_id)

            rows = db.execute(
                "SELECT name, status "
                "FROM graph_nodes "
                "WHERE workflow_id = ? "
                "ORDER BY rowid",
                (workflow_id,),
            ).fetchall()

        return {name: status for name, status in rows}

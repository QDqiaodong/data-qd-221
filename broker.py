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
_PAYLOAD_LIMIT = 16 * 1024
_MAX_SQLITE_INTEGER = (1 << 63) - 1
MAX_WORKERS = 24
MAX_GRAPH_NODES = 12_000
MAX_GRAPH_DEPENDENCIES = 48_000
MAX_SINKS = 32
MAX_SINK_CAPACITY = 12_000
MAX_OUTCOME_LIMIT = 128

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
            self._create_graph_schema(db)
            self._migrate_graph_nodes(db)
            self._create_sink_schema(db)
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

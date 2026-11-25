"""SQLite-backed single-queue broker; timestamps are injected by the consumer."""
from contextlib import closing
from dataclasses import dataclass
import sqlite3
import uuid


@dataclass(frozen=True)
class Delivery:
    id: int
    payload: str
    receipt: str


class Broker:
    def __init__(self, path):
        self.path = path
        with closing(sqlite3.connect(path)) as db, db:
            db.execute("""CREATE TABLE IF NOT EXISTS jobs (
                id INTEGER PRIMARY KEY AUTOINCREMENT, payload TEXT NOT NULL,
                receipt TEXT, expires INTEGER NOT NULL DEFAULT 0)""")

    def put(self, payload):
        with closing(sqlite3.connect(self.path)) as db, db:
            return db.execute("INSERT INTO jobs(payload) VALUES (?)", (payload,)).lastrowid

    def take(self, *, now, lease):
        if now < 0 or lease <= 0:
            raise ValueError("invalid lease clock or duration")
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute("BEGIN IMMEDIATE")
            job = db.execute(
                "SELECT id, payload FROM jobs WHERE expires <= ? ORDER BY id LIMIT 1",
                (now,),
            ).fetchone()
            if job is None:
                return None
            receipt = uuid.uuid4().hex
            db.execute("UPDATE jobs SET receipt=?, expires=? WHERE id=?",
                       (receipt, now + lease, job[0]))
            return Delivery(*job, receipt)

    def acknowledge(self, delivery, *, now):
        with closing(sqlite3.connect(self.path)) as db, db:
            return db.execute("DELETE FROM jobs WHERE id=? AND receipt=? AND expires>?",
                              (delivery.id, delivery.receipt, now)).rowcount == 1

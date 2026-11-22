"""SQLite persistence for general-ledger receipt documents.

Receipts live in their *own* SQLite file, deliberately separate from the
local posting library: the posting library stays a read-only source of
confirmed batches, while everything the general ledger sends back is
persisted here and survives process restarts.

Only the **currently effective** version of each voucher group is stored as
queryable rows.  A higher revision replaces the whole group in one
transaction (old lines deleted, new lines inserted), so a reconciliation query
can physically never mix contributions from an old and a new version of the
same group.  Every submission -- including rejected ones -- is additionally
recorded in an append-only audit log.

Concurrency follows the same pattern as the existing packages: WAL plus
``synchronous=FULL`` and write transactions opened with ``BEGIN IMMEDIATE``.
Concurrent imports of different groups therefore serialise without losing
each other's committed receipts, and a lagging revision can never overwrite a
newer one.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence

SCHEMA_VERSION = 1

DDL = """
CREATE TABLE IF NOT EXISTS receipt_meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

-- One row per voucher group, pointing at its currently effective revision.
CREATE TABLE IF NOT EXISTS receipt_groups (
    receipt_id       TEXT PRIMARY KEY,
    current_revision INTEGER NOT NULL CHECK (current_revision > 0),
    line_count       INTEGER NOT NULL CHECK (line_count >= 0),
    received_at      TEXT NOT NULL
);

-- The lines of the currently effective version only.  Replacing a group
-- deletes its rows and inserts the new version's rows inside one
-- transaction, so old and new contributions can never be read together.
CREATE TABLE IF NOT EXISTS receipt_lines (
    receipt_id   TEXT NOT NULL,
    line_id      TEXT NOT NULL,
    flow_id      INTEGER NOT NULL,
    signed_cents INTEGER NOT NULL CHECK (signed_cents <> 0),
    PRIMARY KEY (receipt_id, line_id)
);
CREATE INDEX IF NOT EXISTS idx_receipt_lines_flow
    ON receipt_lines(flow_id);

-- Append-only audit of every submission, accepted or rejected.
CREATE TABLE IF NOT EXISTS receipt_import_log (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    received_at TEXT NOT NULL,
    receipt_id  TEXT NOT NULL,
    revision    INTEGER NOT NULL,
    line_count  INTEGER NOT NULL,
    outcome     TEXT NOT NULL CHECK (outcome IN
                    ('accepted', 'replaced', 'idempotent', 'rejected')),
    reason      TEXT
);
"""


def utcnow_iso() -> str:
    from datetime import datetime, timezone

    return datetime.now(timezone.utc).isoformat()


class ReceiptStore:
    """Persistence of receipt groups with serialised write transactions."""

    def __init__(self, receipt_db, now_provider=utcnow_iso):
        self.db_path = Path(receipt_db)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(str(self.db_path), timeout=30,
                                    isolation_level=None)
        self.conn.row_factory = sqlite3.Row
        self.read_conn = sqlite3.connect(
            f"file:{self.db_path}?mode=ro", uri=True, timeout=30,
            isolation_level=None,
        )
        self.read_conn.row_factory = sqlite3.Row
        for conn in (self.conn, self.read_conn):
            conn.execute("PRAGMA busy_timeout=30000")
        self._ensure_wal_mode()
        self.conn.execute("PRAGMA synchronous=FULL")
        self.read_conn.execute("PRAGMA query_only=ON")
        self.now_provider = now_provider
        self._install()

    def _ensure_wal_mode(self, attempts: int = 50, delay: float = 0.05) -> None:
        import time

        for attempt in range(attempts):
            try:
                mode = self.conn.execute(
                    "PRAGMA journal_mode=WAL"
                ).fetchone()[0]
                if str(mode).lower() == "wal":
                    return
            except sqlite3.OperationalError as exc:
                if "locked" not in str(exc).lower() or attempt == attempts - 1:
                    raise
                time.sleep(delay)
        raise sqlite3.OperationalError("could not enable WAL mode: locked")

    def _install(self) -> None:
        self.conn.executescript(DDL)
        self.conn.execute(
            "INSERT OR IGNORE INTO receipt_meta(key, value) VALUES"
            "('schema_version', ?)",
            (str(SCHEMA_VERSION),),
        )

    def close(self) -> None:
        self.read_conn.close()
        self.conn.close()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()

    # --------------------------------------------------------- transactions
    def begin(self) -> None:
        # IMMEDIATE takes the write lock up front: concurrent imports of any
        # two groups serialise here, so neither can lose the other's committed
        # rows and an old revision can never slip past a newer one.
        self.conn.execute("BEGIN IMMEDIATE")

    def commit(self) -> None:
        if self.conn.in_transaction:
            self.conn.execute("COMMIT")

    def rollback(self) -> None:
        if self.conn.in_transaction:
            self.conn.execute("ROLLBACK")

    # --------------------------------------------------------------- groups
    def get_group(self, receipt_id: str) -> Optional[sqlite3.Row]:
        return self.conn.execute(
            "SELECT * FROM receipt_groups WHERE receipt_id=?",
            (receipt_id,),
        ).fetchone()

    def get_lines(self, receipt_id: str) -> List[sqlite3.Row]:
        return self.conn.execute(
            """SELECT receipt_id, line_id, flow_id, signed_cents
                 FROM receipt_lines WHERE receipt_id=?
                ORDER BY line_id""",
            (receipt_id,),
        ).fetchall()

    def replace_group(
        self, receipt_id: str, revision: int,
        lines: Sequence[Dict[str, Any]], now: str,
    ) -> None:
        """Install a whole new effective version in one transaction."""
        self.conn.execute(
            "DELETE FROM receipt_lines WHERE receipt_id=?", (receipt_id,)
        )
        self.conn.executemany(
            """INSERT INTO receipt_lines(receipt_id, line_id, flow_id,
                       signed_cents)
               VALUES(:receipt_id, :line_id, :flow_id, :signed_cents)""",
            [
                {
                    "receipt_id": receipt_id,
                    "line_id": line["line_id"],
                    "flow_id": line["flow_id"],
                    "signed_cents": line["signed_cents"],
                }
                for line in lines
            ],
        )
        self.conn.execute(
            """INSERT INTO receipt_groups(receipt_id, current_revision,
                       line_count, received_at)
               VALUES(?, ?, ?, ?)
               ON CONFLICT(receipt_id) DO UPDATE SET
                   current_revision=excluded.current_revision,
                   line_count=excluded.line_count,
                   received_at=excluded.received_at""",
            (receipt_id, revision, len(lines), now),
        )

    def log_import(
        self, receipt_id: str, revision: int, line_count: int,
        outcome: str, reason: Optional[str], now: str,
    ) -> None:
        self.conn.execute(
            """INSERT INTO receipt_import_log(received_at, receipt_id,
                       revision, line_count, outcome, reason)
               VALUES(?, ?, ?, ?, ?, ?)""",
            (now, receipt_id, revision, line_count, outcome, reason),
        )

    def import_log(self) -> List[Dict[str, Any]]:
        rows = self.read_conn.execute(
            """SELECT id, received_at, receipt_id, revision, line_count,
                      outcome, reason
                 FROM receipt_import_log ORDER BY id"""
        ).fetchall()
        return [dict(r) for r in rows]

    # ------------------------------------------------------------- queries
    def current_lines_for_flows(
        self, flow_ids: Sequence[int]
    ) -> List[sqlite3.Row]:
        """All current-version receipt lines touching the given flows.

        An indexed ``IN`` point lookup: imports and batch queries never scan
        receipt lines unrelated to the flows the task touches.
        """
        if not flow_ids:
            return []
        placeholders = ",".join("?" for _ in flow_ids)
        return self.read_conn.execute(
            f"""SELECT l.receipt_id, g.current_revision, l.line_id,
                       l.flow_id, l.signed_cents
                  FROM receipt_lines l
                  JOIN receipt_groups g ON g.receipt_id = l.receipt_id
                 WHERE l.flow_id IN ({placeholders})
                 ORDER BY l.receipt_id, l.line_id""",
            tuple(flow_ids),
        ).fetchall()

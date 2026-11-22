"""SQLite persistence for selected day revisions, checkpoints and facts.

This database is deliberately independent of the closure service database:
the netting service is the only writer and the closure store stays untouched.
WAL plus ``BEGIN IMMEDIATE`` serialises concurrent applications while readers
keep seeing fully consistent snapshots (a new revision number can never be
visible together with facts derived from the old revision).
"""

from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence

from .engine import Entry, Fact, OpenState

SCHEMA_VERSION = 1

DDL = """
CREATE TABLE IF NOT EXISTS meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

-- One row per business day whose revision has been applied.  The revision
-- column only moves upward; same-revision retries are idempotent and an
-- older revision is rejected while a newer one is selected.
CREATE TABLE IF NOT EXISTS applied_days (
    business_day   TEXT PRIMARY KEY,
    revision       INTEGER NOT NULL,
    binding_sig    TEXT NOT NULL,
    detail_sha256  TEXT NOT NULL,
    applied_at     TEXT NOT NULL
);

-- Immutable per-day snapshots parsed from the bound detail.csv.  Replay of a
-- corrected day reads only these rows, never the closure service or other
-- days' source files.
CREATE TABLE IF NOT EXISTS entries (
    business_day TEXT NOT NULL,
    entry_id     TEXT NOT NULL,
    account_id   TEXT NOT NULL,
    kind         TEXT NOT NULL CHECK (kind IN ('bill', 'payment')),
    cents        INTEGER NOT NULL CHECK (cents > 0 AND cents <= 1000000000000),
    PRIMARY KEY (business_day, entry_id)
);
CREATE INDEX IF NOT EXISTS idx_entries_account_day
    ON entries(account_id, business_day);

-- The maintained fact table: one row per completed bill/payment pairing.
CREATE TABLE IF NOT EXISTS facts (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    account_id     TEXT NOT NULL,
    match_day      TEXT NOT NULL,
    bill_day       TEXT NOT NULL,
    bill_entry_id  TEXT NOT NULL,
    payment_day    TEXT NOT NULL,
    payment_entry_id TEXT NOT NULL,
    cents          INTEGER NOT NULL CHECK (cents > 0),
    UNIQUE(account_id, match_day, bill_day, bill_entry_id,
           payment_day, payment_entry_id)
);
CREATE INDEX IF NOT EXISTS idx_facts_account_day ON facts(account_id, match_day);
CREATE INDEX IF NOT EXISTS idx_facts_bill
    ON facts(bill_day, bill_entry_id);
CREATE INDEX IF NOT EXISTS idx_facts_payment
    ON facts(payment_day, payment_entry_id);

-- Resumable open state per (account, applied business day).  A recomputation
-- starts from the newest checkpoint strictly before the first changed day,
-- so pre-correction pairings are never replayed.
CREATE TABLE IF NOT EXISTS checkpoints (
    account_id   TEXT NOT NULL,
    business_day TEXT NOT NULL,
    state_json   TEXT NOT NULL,
    PRIMARY KEY (account_id, business_day)
);

-- Audit trail of every application attempt, including rejections.
CREATE TABLE IF NOT EXISTS apply_log (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    business_day TEXT NOT NULL,
    revision     INTEGER NOT NULL,
    outcome      TEXT NOT NULL CHECK (outcome IN ('applied', 'idempotent',
                                                  'rejected')),
    detail       TEXT NOT NULL,
    attempted_at TEXT NOT NULL
);
"""


def utcnow_iso() -> str:
    from datetime import datetime, timezone

    return datetime.now(timezone.utc).isoformat()


class NettingStore:
    def __init__(self, db_path, now_provider=utcnow_iso):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        # Dedicated write connection: every mutating transaction runs here
        # under BEGIN IMMEDIATE so concurrent applications serialise.
        self.conn = sqlite3.connect(str(self.db_path), timeout=30,
                                   isolation_level=None)
        self.conn.row_factory = sqlite3.Row
        # Separate read connection so long-lived readers do not pin the WAL
        # read mark and block a writer's commit/checkpoint under concurrency.
        self.read_conn = sqlite3.connect(
            f"file:{self.db_path}?mode=ro", uri=True, timeout=30,
            isolation_level=None,
        )
        self.read_conn.row_factory = sqlite3.Row
        for conn in (self.conn, self.read_conn):
            conn.execute("PRAGMA busy_timeout=30000")
        # journal_mode=WAL is persisted in the database header and the pragma
        # needs an exclusive lock (it does not honour busy_timeout), so on a
        # contended first open retry briefly instead of failing outright.
        self._ensure_wal_mode()
        self.conn.execute("PRAGMA foreign_keys=ON")
        self.conn.execute("PRAGMA synchronous=FULL")
        self.read_conn.execute("PRAGMA query_only=ON")
        self.conn.executescript(DDL)
        row = self.conn.execute(
            "SELECT value FROM meta WHERE key='schema_version'"
        ).fetchone()
        if row is None:
            self.conn.execute(
                "INSERT INTO meta(key, value) VALUES('schema_version', ?)",
                (str(SCHEMA_VERSION),),
            )

    def _ensure_wal_mode(self, attempts: int = 50, delay: float = 0.05) -> None:
        """Switch (or confirm) WAL mode, retrying the exclusive-lock pragma."""
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

    def close(self) -> None:
        self.read_conn.close()
        self.conn.close()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()

    # --------------------------------------------------------- transactions
    def begin(self) -> None:
        # IMMEDIATE takes the write lock up front so two racing applications
        # serialise instead of deadlocking on deferred upgrades.
        self.conn.execute("BEGIN IMMEDIATE")

    def commit(self) -> None:
        if self.conn.in_transaction:
            self.conn.execute("COMMIT")

    def rollback(self) -> None:
        if self.conn.in_transaction:
            self.conn.execute("ROLLBACK")

    # ---------------------------------------------------------- applied days
    def get_applied_day(self, business_day: str) -> Optional[sqlite3.Row]:
        # Used inside the write transaction: must read uncommitted changes, so
        # this goes through the write connection, not the read-only one.
        return self.conn.execute(
            "SELECT * FROM applied_days WHERE business_day=?", (business_day,)
        ).fetchone()

    def all_applied_days(self) -> List[sqlite3.Row]:
        return self.conn.execute(
            "SELECT * FROM applied_days ORDER BY business_day"
        ).fetchall()

    def all_applied_days_published(self) -> List[sqlite3.Row]:
        """Committed-only view for external readers (separate read snapshot)."""
        return self.read_conn.execute(
            "SELECT * FROM applied_days ORDER BY business_day"
        ).fetchall()

    def upsert_applied_day(
        self, business_day: str, revision: int, binding_sig: str,
        detail_sha256: str, now: str,
    ) -> None:
        self.conn.execute(
            """INSERT INTO applied_days(business_day, revision, binding_sig,
                                        detail_sha256, applied_at)
               VALUES(?, ?, ?, ?, ?)
               ON CONFLICT(business_day) DO UPDATE SET
                 revision=excluded.revision,
                 binding_sig=excluded.binding_sig,
                 detail_sha256=excluded.detail_sha256,
                 applied_at=excluded.applied_at""",
            (business_day, revision, binding_sig, detail_sha256, now),
        )

    # -------------------------------------------------------------- entries
    def replace_day_entries(self, business_day: str, entries: Sequence[Entry]) -> None:
        self.conn.execute(
            "DELETE FROM entries WHERE business_day=?", (business_day,)
        )
        self.conn.executemany(
            """INSERT INTO entries(business_day, entry_id, account_id, kind, cents)
               VALUES(?, ?, ?, ?, ?)""",
            [(e.day, e.entry_id, e.account_id, e.kind, e.cents) for e in entries],
        )

    def entries_for_account_from(
        self, account_id: str, from_day: str
    ) -> List[Entry]:
        rows = self.conn.execute(
            """SELECT business_day, entry_id, account_id, kind, cents
               FROM entries
               WHERE account_id=? AND business_day>=?
               ORDER BY business_day, entry_id""",
            (account_id, from_day),
        ).fetchall()
        return [
            Entry(
                day=r["business_day"], entry_id=r["entry_id"],
                account_id=r["account_id"], kind=r["kind"], cents=r["cents"],
            )
            for r in rows
        ]

    def accounts_with_entries_from(self, from_day: str) -> List[str]:
        rows = self.conn.execute(
            "SELECT DISTINCT account_id FROM entries WHERE business_day>=? "
            "ORDER BY account_id",
            (from_day,),
        ).fetchall()
        return [r["account_id"] for r in rows]

    def day_accounts(self, business_day: str) -> List[str]:
        rows = self.conn.execute(
            "SELECT DISTINCT account_id FROM entries WHERE business_day=? "
            "ORDER BY account_id",
            (business_day,),
        ).fetchall()
        return [r["account_id"] for r in rows]

    def day_entries(self, business_day: str) -> List[Entry]:
        rows = self.conn.execute(
            """SELECT business_day, entry_id, account_id, kind, cents
               FROM entries WHERE business_day=?
               ORDER BY entry_id""",
            (business_day,),
        ).fetchall()
        return [
            Entry(
                day=r["business_day"], entry_id=r["entry_id"],
                account_id=r["account_id"], kind=r["kind"], cents=r["cents"],
            )
            for r in rows
        ]

    def day_accounts_published(self, business_day: str) -> List[str]:
        rows = self.read_conn.execute(
            "SELECT DISTINCT account_id FROM entries WHERE business_day=? "
            "ORDER BY account_id",
            (business_day,),
        ).fetchall()
        return [r["account_id"] for r in rows]

    # ---------------------------------------------------------------- facts
    def delete_facts_for_account_from(self, account_id: str, from_day: str) -> int:
        cur = self.conn.execute(
            "DELETE FROM facts WHERE account_id=? AND match_day>=?",
            (account_id, from_day),
        )
        return cur.rowcount

    def insert_facts(self, facts: Sequence[Fact]) -> None:
        self.conn.executemany(
            """INSERT INTO facts(account_id, match_day, bill_day, bill_entry_id,
                                 payment_day, payment_entry_id, cents)
               VALUES(?, ?, ?, ?, ?, ?, ?)""",
            [f.as_row() for f in facts],
        )

    def read_facts(
        self,
        account_id: Optional[str] = None,
        business_day: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        clauses: List[str] = []
        params: List[Any] = []
        if account_id is not None:
            clauses.append("account_id=?")
            params.append(account_id)
        if business_day is not None:
            clauses.append("match_day=?")
            params.append(business_day)
        where = (" WHERE " + " AND ".join(clauses)) if clauses else ""
        rows = self.read_conn.execute(
            f"""SELECT account_id, match_day, bill_day, bill_entry_id,
                       payment_day, payment_entry_id, cents
                  FROM facts{where}
                 ORDER BY account_id, match_day, bill_day, bill_entry_id,
                          payment_day, payment_entry_id""",
            params,
        ).fetchall()
        return [dict(r) for r in rows]

    def fact_rowids_for_account(self, account_id: str) -> List[int]:
        rows = self.read_conn.execute(
            "SELECT id FROM facts WHERE account_id=? ORDER BY id", (account_id,)
        ).fetchall()
        return [r["id"] for r in rows]

    # ---------------------------------------------------------- checkpoints
    def latest_checkpoint_before(
        self, account_id: str, day: str
    ) -> Optional[sqlite3.Row]:
        # Read inside the write transaction (must see this transaction's
        # deletes), so use the write connection.
        return self.conn.execute(
            "SELECT * FROM checkpoints WHERE account_id=? AND business_day<? "
            "ORDER BY business_day DESC LIMIT 1",
            (account_id, day),
        ).fetchone()

    def delete_checkpoints_for_account_from(
        self, account_id: str, from_day: str
    ) -> None:
        self.conn.execute(
            "DELETE FROM checkpoints WHERE account_id=? AND business_day>=?",
            (account_id, from_day),
        )

    def upsert_checkpoint(
        self, account_id: str, business_day: str, state: OpenState
    ) -> None:
        import json

        self.conn.execute(
            """INSERT INTO checkpoints(account_id, business_day, state_json)
               VALUES(?, ?, ?)
               ON CONFLICT(account_id, business_day) DO UPDATE SET
                 state_json=excluded.state_json""",
            (account_id, business_day, json.dumps(state.to_json(),
                                                  sort_keys=True)),
        )

    # ------------------------------------------------------------- audit log
    def record_attempt(
        self, business_day: str, revision: int, outcome: str, detail: str,
        now: str,
    ) -> None:
        self.conn.execute(
            """INSERT INTO apply_log(business_day, revision, outcome, detail,
                                     attempted_at)
               VALUES(?, ?, ?, ?, ?)""",
            (business_day, revision, outcome, detail, now),
        )

    def list_attempts(self, business_day: Optional[str] = None) -> List[Dict[str, Any]]:
        if business_day is None:
            rows = self.read_conn.execute(
                "SELECT * FROM apply_log ORDER BY id"
            ).fetchall()
        else:
            rows = self.read_conn.execute(
                "SELECT * FROM apply_log WHERE business_day=? ORDER BY id",
                (business_day,),
            ).fetchall()
        return [dict(r) for r in rows]

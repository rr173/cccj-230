"""SQLite persistence for the netting posting handoff.

The posting tables live in the *same* SQLite file as the netting fact table.
That is deliberate: a netting application and the posting change records it
produces then commit in one atomic transaction, and a batch confirmation
(flows, live pointers and handoff watermark) commits atomically as well.

The posting package never calls netting application code and never changes a
netting interface's business semantics.  Its only write boundary into the
netting side is two append-only triggers attached to the existing ``facts``
table.  The triggers are the mechanism that satisfies the "no scanning of
unaffected history" rule: instead of diffing the whole fact table after an
application, every fact row delete/insert performed by a netting transaction
is streamed into ``fact_change_log`` automatically, scoped to exactly the
rows the application rewrote.

Two mechanisms drive the handoff protocol, and both are written inside the
netting application's own transaction so they share its commit/rollback:

* ``fact_change_log`` receives one row per physical facts-row
  removal/insertion via the triggers.  Only rows the application actually
  rewrote ever appear, so building a handoff delta never scans historical
  pairings or published flows.  A same-pairing row rewrite (delete +
  identical insert) emits two rows that cancel.
* ``fact_revisions`` receives one boundary marker per application that
  rewrote facts (the netting service writes it; the table is created here as
  well so an already-provisioned database keeps working).  Idempotent
  same-revision retries and rejected applications never reach the rewrite
  path and therefore get no marker.

Business mutation state is derived per marker: the change rows of one
application are compacted into a multiset difference, which is empty exactly
for a pure row-only rewrite (revision number changed, pairings and amounts
unchanged) and non-empty for every genuine content change -- including a
change committed by one application and undone by a later one, because each
application is observed separately.  A monotonic ``fact_clock`` counts only
applications with a non-empty difference.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence

# Business identity of one pairing.  The netting row id is deliberately not
# part of this tuple: database row numbers are not business identities.
IDENTITY_COLUMNS = (
    "account_id",
    "match_day",
    "bill_day",
    "bill_entry_id",
    "payment_day",
    "payment_entry_id",
)
IDENTITY_SQL = ", ".join(IDENTITY_COLUMNS)

SCHEMA_VERSION = 1

DDL = """
CREATE TABLE IF NOT EXISTS posting_meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

-- Every physical facts-row removal/insertion, streamed by triggers inside the
-- netting application's own transaction.  Only rows the application touched
-- ever appear here, so building a handoff delta never scans historical
-- pairings or published flows.
CREATE TABLE IF NOT EXISTS fact_change_log (
    seq           INTEGER PRIMARY KEY AUTOINCREMENT,
    op            TEXT NOT NULL CHECK (op IN ('INSERT', 'DELETE')),
    account_id    TEXT NOT NULL,
    match_day     TEXT NOT NULL,
    bill_day      TEXT NOT NULL,
    bill_entry_id TEXT NOT NULL,
    payment_day      TEXT NOT NULL,
    payment_entry_id TEXT NOT NULL,
    cents         INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_fact_change_seq
    ON fact_change_log(seq);

-- Application boundary markers, written by the netting service inside each
-- application transaction.  Declared here too so a database provisioned by
-- the posting package alone has the full contract.
CREATE TABLE IF NOT EXISTS fact_revisions (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    business_day   TEXT NOT NULL,
    revision       INTEGER NOT NULL,
    facts_deleted  INTEGER NOT NULL,
    facts_inserted INTEGER NOT NULL,
    recorded_at    TEXT NOT NULL
);

-- Stable handoff watermarks (keys in posting_meta):
--   fact_clock_value  monotonic business-mutation counter (derived per
--                     fact_revisions marker; pure row rewrites add zero)
--   last_revision_id  last fact_revisions row folded into the clock
--   clock_window_seq  change-log seq where the next clock fold starts
--   last_change_seq   last fact_change_log row consumed by a confirm
--   last_batch_no     highest published batch number (also a table column)
--   last_posting_day  posting day of the latest published batch
--   initialised       '1' once bootstrap state has been installed

-- A confirmed, immutable posting batch.  Rows are never updated or deleted
-- after confirmation; reading a batch only ever sees committed rows.
CREATE TABLE IF NOT EXISTS posting_batches (
    batch_no        INTEGER PRIMARY KEY CHECK (batch_no > 0),
    posting_day     TEXT NOT NULL,
    confirmed_at    TEXT NOT NULL,
    entry_count     INTEGER NOT NULL CHECK (entry_count > 0),
    basis_clock     INTEGER NOT NULL,
    basis_last_seq  INTEGER NOT NULL,
    basis_last_batch INTEGER NOT NULL,
    preview_id      TEXT NOT NULL UNIQUE
);

-- Stable flow lines inside confirmed batches.  entry_no is unique inside one
-- batch and reversal lines always sort before the new forward lines of the
-- same pairing.  A reversal names the still-live forward line it cancels;
-- foreign keys enforce that the target exists and was not itself reversed.
CREATE TABLE IF NOT EXISTS posting_entries (
    flow_id            INTEGER PRIMARY KEY AUTOINCREMENT,
    batch_no           INTEGER NOT NULL REFERENCES posting_batches(batch_no),
    entry_no           INTEGER NOT NULL CHECK (entry_no > 0),
    line_type          TEXT NOT NULL CHECK (line_type IN ('forward',
                                                          'reversal')),
    account_id         TEXT NOT NULL,
    match_day          TEXT NOT NULL,
    bill_day           TEXT NOT NULL,
    bill_entry_id      TEXT NOT NULL,
    payment_day        TEXT NOT NULL,
    payment_entry_id   TEXT NOT NULL,
    cents              INTEGER NOT NULL CHECK (cents > 0),
    reverses_flow_id   INTEGER REFERENCES posting_entries(flow_id),
    UNIQUE(batch_no, entry_no),
    CHECK ((line_type = 'reversal') = (reverses_flow_id IS NOT NULL))
);
CREATE INDEX IF NOT EXISTS idx_posting_entries_batch
    ON posting_entries(batch_no, entry_no);
CREATE INDEX IF NOT EXISTS idx_posting_entries_identity
    ON posting_entries(account_id, match_day, bill_day, bill_entry_id,
                       payment_day, payment_entry_id);

-- The single still-live forward line per pairing identity.  A forward insert
-- adds a row, a reversal removes it, so a reversal can only ever point at the
-- live version -- never at an already-reversed one.
CREATE TABLE IF NOT EXISTS posting_live (
    flow_id          INTEGER PRIMARY KEY
                     REFERENCES posting_entries(flow_id),
    account_id       TEXT NOT NULL,
    match_day        TEXT NOT NULL,
    bill_day         TEXT NOT NULL,
    bill_entry_id    TEXT NOT NULL,
    payment_day      TEXT NOT NULL,
    payment_entry_id TEXT NOT NULL,
    cents            INTEGER NOT NULL,
    UNIQUE(account_id, match_day, bill_day, bill_entry_id,
           payment_day, payment_entry_id)
);

-- Unconfirmed previews.  A new preview never overwrites an older one: each
-- keeps the exact basis on which it was computed and is accepted or rejected
-- on its own merits at confirm time.  Confirming one preview makes every
-- other pending preview stale via basis_last_batch_no.
CREATE TABLE IF NOT EXISTS posting_previews (
    preview_id        TEXT PRIMARY KEY,
    created_at        TEXT NOT NULL,
    posting_day       TEXT NOT NULL,
    basis_clock       INTEGER NOT NULL,
    basis_last_seq    INTEGER NOT NULL,
    basis_last_batch  INTEGER NOT NULL,
    status            TEXT NOT NULL CHECK (status IN ('pending',
                                                      'confirmed',
                                                      'superseded'))
);

CREATE TABLE IF NOT EXISTS posting_preview_entries (
    preview_id       TEXT NOT NULL
                     REFERENCES posting_previews(preview_id),
    entry_no         INTEGER NOT NULL,
    line_type        TEXT NOT NULL CHECK (line_type IN ('forward',
                                                        'reversal')),
    account_id       TEXT NOT NULL,
    match_day        TEXT NOT NULL,
    bill_day         TEXT NOT NULL,
    bill_entry_id    TEXT NOT NULL,
    payment_day      TEXT NOT NULL,
    payment_entry_id TEXT NOT NULL,
    cents            INTEGER NOT NULL CHECK (cents > 0),
    -- For reversal lines: the live forward flow id captured at preview time.
    target_flow_id   INTEGER,
    PRIMARY KEY (preview_id, entry_no)
);
"""

# The triggers below are the only write the posting package adds to the
# netting side.  They are append-only streamers: all business meaning
# (rewrite cancellation, the mutation clock) is derived in Python against the
# fact_revisions boundary markers.  The triggers run inside whatever
# transaction mutates ``facts``, so they share its commit/rollback.
TRIGGERS = """
CREATE TRIGGER IF NOT EXISTS trg_facts_ai AFTER INSERT ON facts
BEGIN
    INSERT INTO fact_change_log(op, account_id, match_day, bill_day,
                                bill_entry_id, payment_day, payment_entry_id,
                                cents)
    VALUES ('INSERT', NEW.account_id, NEW.match_day, NEW.bill_day,
            NEW.bill_entry_id, NEW.payment_day, NEW.payment_entry_id,
            NEW.cents);
END;

CREATE TRIGGER IF NOT EXISTS trg_facts_ad AFTER DELETE ON facts
BEGIN
    INSERT INTO fact_change_log(op, account_id, match_day, bill_day,
                                bill_entry_id, payment_day, payment_entry_id,
                                cents)
    VALUES ('DELETE', OLD.account_id, OLD.match_day, OLD.bill_day,
            OLD.bill_entry_id, OLD.payment_day, OLD.payment_entry_id,
            OLD.cents);
END;
"""


def utcnow_iso() -> str:
    from datetime import datetime, timezone

    return datetime.now(timezone.utc).isoformat()


class PostingStore:
    """Posting-side persistence over one shared netting SQLite database."""

    def __init__(self, db_path, now_provider=utcnow_iso):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        # Independent connections: the posting service may be opened without a
        # NettingService handle (e.g. a reader process, or first enablement on
        # an existing netting database).
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
        self.conn.execute("PRAGMA foreign_keys=ON")
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
        """Create posting tables/triggers once; safe on an existing netting db."""
        self.conn.executescript(DDL)
        self.conn.execute(
            "INSERT OR IGNORE INTO posting_meta(key, value) VALUES"
            "('fact_clock_value', '0')"
        )
        self.conn.execute(
            "INSERT OR IGNORE INTO posting_meta(key, value) VALUES"
            "('last_revision_id', '0')"
        )
        self.conn.execute(
            "INSERT OR IGNORE INTO posting_meta(key, value) VALUES"
            "('clock_window_seq', '0')"
        )
        self.conn.executescript(TRIGGERS)
        row = self.conn.execute(
            "SELECT value FROM posting_meta WHERE key='schema_version'"
        ).fetchone()
        if row is None:
            self.conn.execute(
                "INSERT INTO posting_meta(key, value) VALUES"
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
        # IMMEDIATE serialises concurrent confirms against concurrent netting
        # applications and against each other: an old preview can never be
        # published as a new result and two confirms cannot double-post.
        self.conn.execute("BEGIN IMMEDIATE")

    def commit(self) -> None:
        if self.conn.in_transaction:
            self.conn.execute("COMMIT")

    def rollback(self) -> None:
        if self.conn.in_transaction:
            self.conn.execute("ROLLBACK")

    # ----------------------------------------------------------------- meta
    def get_meta(self, key: str, default: Optional[str] = None) -> Optional[str]:
        row = self.conn.execute(
            "SELECT value FROM posting_meta WHERE key=?", (key,)
        ).fetchone()
        return row["value"] if row is not None else default

    def set_meta(self, key: str, value: Any) -> None:
        self.conn.execute(
            """INSERT INTO posting_meta(key, value) VALUES(?, ?)
               ON CONFLICT(key) DO UPDATE SET value=excluded.value""",
            (key, str(value)),
        )

    def max_change_seq(self) -> int:
        row = self.conn.execute(
            "SELECT COALESCE(MAX(seq), 0) AS m FROM fact_change_log"
        ).fetchone()
        return int(row["m"])

    def max_revision_id(self) -> int:
        row = self.conn.execute(
            "SELECT COALESCE(MAX(id), 0) AS m FROM fact_revisions"
        ).fetchone()
        return int(row["m"])

    def revisions_after(self, revision_id: int) -> List[sqlite3.Row]:
        return self.conn.execute(
            "SELECT * FROM fact_revisions WHERE id > ? ORDER BY id",
            (revision_id,),
        ).fetchall()

    # ------------------------------------------------------------- bootstrap
    def all_facts_for_bootstrap(self) -> List[sqlite3.Row]:
        """One-time first-enablement read of the existing netting fact table.

        Only ever called while the handoff is uninitialised; afterwards every
        delta is derived from ``fact_change_log`` alone.
        """
        return self.conn.execute(
            f"""SELECT {IDENTITY_SQL}, cents FROM facts
                ORDER BY account_id, match_day, bill_day, bill_entry_id,
                         payment_day, payment_entry_id"""
        ).fetchall()

    # ------------------------------------------------------------ change log
    def change_rows_between(self, lo_seq: int, hi_seq: int) -> List[sqlite3.Row]:
        """Change rows with ``lo_seq < seq <= hi_seq`` (one marker's window)."""
        return self.conn.execute(
            """SELECT seq, op, account_id, match_day, bill_day, bill_entry_id,
                      payment_day, payment_entry_id, cents
                 FROM fact_change_log
                WHERE seq > ? AND seq <= ?
                ORDER BY seq""",
            (lo_seq, hi_seq),
        ).fetchall()

    def change_rows_after(self, seq: int) -> List[sqlite3.Row]:
        return self.conn.execute(
            """SELECT seq, op, account_id, match_day, bill_day, bill_entry_id,
                      payment_day, payment_entry_id, cents
                 FROM fact_change_log WHERE seq > ?
                 ORDER BY seq""",
            (seq,),
        ).fetchall()

    # -------------------------------------------------------------- live map
    def live_for_identity(self, identity: Sequence[Any]) -> Optional[sqlite3.Row]:
        where = " AND ".join(f"{c}=?" for c in IDENTITY_COLUMNS)
        return self.conn.execute(
            f"SELECT * FROM posting_live WHERE {where}", tuple(identity)
        ).fetchone()

    # -------------------------------------------------------------- previews
    def insert_preview(
        self, preview_id: str, posting_day: str, basis_clock: int,
        basis_last_seq: int, basis_last_batch: int, now: str,
    ) -> None:
        self.conn.execute(
            """INSERT INTO posting_previews(preview_id, created_at, posting_day,
                                            basis_clock, basis_last_seq,
                                            basis_last_batch, status)
               VALUES(?, ?, ?, ?, ?, ?, 'pending')""",
            (preview_id, now, posting_day, basis_clock, basis_last_seq,
             basis_last_batch),
        )

    def insert_preview_entry(self, entry: Dict[str, Any]) -> None:
        self.conn.execute(
            """INSERT INTO posting_preview_entries(preview_id, entry_no,
                   line_type, account_id, match_day, bill_day, bill_entry_id,
                   payment_day, payment_entry_id, cents, target_flow_id)
               VALUES(:preview_id, :entry_no, :line_type, :account_id,
                      :match_day, :bill_day, :bill_entry_id, :payment_day,
                      :payment_entry_id, :cents, :target_flow_id)""",
            entry,
        )

    def get_preview(self, preview_id: str) -> Optional[sqlite3.Row]:
        return self.conn.execute(
            "SELECT * FROM posting_previews WHERE preview_id=?",
            (preview_id,),
        ).fetchone()

    def preview_entries(self, preview_id: str) -> List[sqlite3.Row]:
        return self.conn.execute(
            """SELECT * FROM posting_preview_entries
               WHERE preview_id=? ORDER BY entry_no""",
            (preview_id,),
        ).fetchall()

    def mark_preview(self, preview_id: str, status: str) -> None:
        self.conn.execute(
            "UPDATE posting_previews SET status=? WHERE preview_id=?",
            (status, preview_id),
        )

    # ------------------------------------------------------------- batches
    def insert_batch(
        self, batch_no: int, posting_day: str, now: str, entry_count: int,
        basis_clock: int, basis_last_seq: int, basis_last_batch: int,
        preview_id: str,
    ) -> None:
        self.conn.execute(
            """INSERT INTO posting_batches(batch_no, posting_day, confirmed_at,
                       entry_count, basis_clock, basis_last_seq,
                       basis_last_batch, preview_id)
               VALUES(?, ?, ?, ?, ?, ?, ?, ?)""",
            (batch_no, posting_day, now, entry_count, basis_clock,
             basis_last_seq, basis_last_batch, preview_id),
        )

    def insert_entry(self, entry: Dict[str, Any]) -> int:
        cur = self.conn.execute(
            """INSERT INTO posting_entries(batch_no, entry_no, line_type,
                       account_id, match_day, bill_day, bill_entry_id,
                       payment_day, payment_entry_id, cents, reverses_flow_id)
               VALUES(:batch_no, :entry_no, :line_type, :account_id,
                      :match_day, :bill_day, :bill_entry_id, :payment_day,
                      :payment_entry_id, :cents, :reverses_flow_id)""",
            entry,
        )
        return int(cur.lastrowid)

    def insert_live(self, flow_id: int, entry: Dict[str, Any]) -> None:
        self.conn.execute(
            """INSERT INTO posting_live(flow_id, account_id, match_day,
                       bill_day, bill_entry_id, payment_day, payment_entry_id,
                       cents)
               VALUES(?, ?, ?, ?, ?, ?, ?, ?)""",
            (flow_id, entry["account_id"], entry["match_day"],
             entry["bill_day"], entry["bill_entry_id"], entry["payment_day"],
             entry["payment_entry_id"], entry["cents"]),
        )

    def delete_live(self, flow_id: int) -> None:
        self.conn.execute(
            "DELETE FROM posting_live WHERE flow_id=?", (flow_id,)
        )

    def get_batch(self, batch_no: int) -> Optional[sqlite3.Row]:
        return self.read_conn.execute(
            "SELECT * FROM posting_batches WHERE batch_no=?", (batch_no,)
        ).fetchone()

    def get_batch_entries(self, batch_no: int) -> List[sqlite3.Row]:
        return self.read_conn.execute(
            "SELECT * FROM posting_entries WHERE batch_no=? ORDER BY entry_no",
            (batch_no,),
        ).fetchall()

    def max_batch_no(self) -> int:
        row = self.conn.execute(
            "SELECT COALESCE(MAX(batch_no), 0) AS m FROM posting_batches"
        ).fetchone()
        return int(row["m"])

"""SQLite persistence layer.

All durable state lives in one SQLite database (WAL mode, every write is
committed and fsynced) plus a content-addressed blob directory.  A second
process opening the same directory sees every committed reception and closure
decision, which is what makes both recovery and multi-process claiming work.
"""

from __future__ import annotations

import json
import os
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

from .contract import CompiledContract, canonical_json

SCHEMA_VERSION = 1

DDL = """
CREATE TABLE IF NOT EXISTS meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS contracts (
    contract_id    TEXT NOT NULL,
    version        TEXT NOT NULL,
    digest         TEXT NOT NULL PRIMARY KEY,
    canonical_json TEXT NOT NULL,
    created_at     TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS batches (
    id                 TEXT NOT NULL PRIMARY KEY,
    contract_digest    TEXT NOT NULL REFERENCES contracts(digest),
    batch_key_json     TEXT NOT NULL,
    business_date      TEXT NOT NULL,
    deadline           TEXT NOT NULL,
    late_window_end    TEXT NOT NULL,
    current_state      TEXT NOT NULL,
    current_revision   INTEGER NOT NULL DEFAULT 0,
    created_at         TEXT NOT NULL,
    updated_at         TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS events (
    seq         INTEGER PRIMARY KEY AUTOINCREMENT,
    batch_id    TEXT NOT NULL,
    event_type  TEXT NOT NULL,
    payload     TEXT NOT NULL,
    created_at  TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_events_batch ON events(batch_id, seq);

-- One row per *reception attempt*.  Quarantined files stay as audit rows and
-- never produce an effective member (valid_arrival = 0).
CREATE TABLE IF NOT EXISTS member_arrivals (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    batch_id            TEXT NOT NULL,
    member_name         TEXT NOT NULL,
    seq                 INTEGER NOT NULL,
    received_at         TEXT NOT NULL,
    source_path         TEXT,
    blob_sha256         TEXT,
    size_bytes          INTEGER,
    raw_digests_json    TEXT,
    payload_digests_json TEXT,
    compression         TEXT,
    valid_arrival       INTEGER NOT NULL,
    quarantine_reason   TEXT,
    detail_json         TEXT
);
CREATE INDEX IF NOT EXISTS idx_arrivals_batch ON member_arrivals(batch_id, seq);

CREATE TABLE IF NOT EXISTS validation_events (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    batch_id     TEXT NOT NULL,
    arrival_id   INTEGER,
    kind         TEXT NOT NULL,
    member_name  TEXT,
    detail_json  TEXT NOT NULL,
    created_at   TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_validation_batch ON validation_events(batch_id, id);

CREATE TABLE IF NOT EXISTS closures (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    batch_id         TEXT NOT NULL,
    revision         INTEGER NOT NULL,
    state            TEXT NOT NULL,
    contract_digest  TEXT NOT NULL,
    binding_json     TEXT NOT NULL,
    binding_sig      TEXT NOT NULL,
    missing_json     TEXT NOT NULL,
    parent_revision  INTEGER,
    impact_json      TEXT NOT NULL,
    decided_at       TEXT NOT NULL,
    prev_evidence_hash TEXT,
    evidence_hash    TEXT NOT NULL,
    UNIQUE(batch_id, revision)
);

CREATE TABLE IF NOT EXISTS claims (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    batch_id      TEXT NOT NULL,
    revision      INTEGER NOT NULL,
    token         TEXT NOT NULL UNIQUE,
    status        TEXT NOT NULL,
    claimed_by    TEXT,
    claimed_at    TEXT NOT NULL,
    completed_at  TEXT,
    binding_sig   TEXT NOT NULL,
    UNIQUE(batch_id, revision)
);
"""


def utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class Store:
    def __init__(self, db_path, blob_root: Optional[Path] = None,
                 now_provider=utcnow_iso):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.blob_root = Path(blob_root) if blob_root else self.db_path.parent / "blobs"
        self.blob_root.mkdir(parents=True, exist_ok=True)
        self._now_provider = now_provider
        self.conn = sqlite3.connect(
            str(self.db_path), timeout=30, isolation_level=None
        )
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA foreign_keys=ON")
        self.conn.execute("PRAGMA synchronous=FULL")
        self.conn.execute("PRAGMA busy_timeout=30000")
        self.conn.executescript(DDL)
        self._init_meta()

    def _init_meta(self) -> None:
        row = self.conn.execute("SELECT value FROM meta WHERE key='schema_version'").fetchone()
        if row is None:
            self.conn.execute(
                "INSERT INTO meta(key, value) VALUES('schema_version', ?)",
                (str(SCHEMA_VERSION),),
            )
        secret = self.conn.execute(
            "SELECT value FROM meta WHERE key='hmac_secret'"
        ).fetchone()
        if secret is None:
            import secrets

            self.conn.execute(
                "INSERT INTO meta(key, value) VALUES('hmac_secret', ?)",
                (secrets.token_hex(32),),
            )

    @property
    def hmac_secret(self) -> bytes:
        row = self.conn.execute(
            "SELECT value FROM meta WHERE key='hmac_secret'"
        ).fetchone()
        return row["value"].encode("ascii")

    def close(self) -> None:
        self.conn.close()

    # ------------------------------------------------------------------ utils
    @staticmethod
    def dumps(obj: Any) -> str:
        return canonical_json(obj)

    def begin(self):
        self.conn.execute("BEGIN IMMEDIATE")

    def commit(self) -> None:
        if self.conn.in_transaction:
            self.conn.execute("COMMIT")

    def rollback(self) -> None:
        if self.conn.in_transaction:
            self.conn.execute("ROLLBACK")

    # --------------------------------------------------------------- contracts
    def upsert_contract(self, contract: CompiledContract) -> None:
        self.conn.execute(
            """INSERT INTO contracts(contract_id, version, digest, canonical_json, created_at)
               VALUES(?, ?, ?, ?, ?)
               ON CONFLICT(digest) DO NOTHING""",
            (
                contract.contract_id,
                contract.version,
                contract.digest,
                contract.canonical_json,
                self._now_provider(),
            ),
        )

    def get_contract(self, digest: str) -> Optional[sqlite3.Row]:
        return self.conn.execute(
            "SELECT * FROM contracts WHERE digest=?", (digest,)
        ).fetchone()

    def find_contract(self, contract_id: str, version: Optional[str] = None):
        if version is None:
            return self.conn.execute(
                "SELECT * FROM contracts WHERE contract_id=? ORDER BY created_at DESC LIMIT 1",
                (contract_id,),
            ).fetchall()
        return self.conn.execute(
            "SELECT * FROM contracts WHERE contract_id=? AND version=?",
            (contract_id, version),
        ).fetchall()

    # ----------------------------------------------------------------- batches
    def get_batch(self, batch_id: str) -> Optional[sqlite3.Row]:
        return self.conn.execute(
            "SELECT * FROM batches WHERE id=?", (batch_id,)
        ).fetchone()

    def insert_batch(self, row: Dict[str, Any]) -> None:
        self.conn.execute(
            """INSERT INTO batches(id, contract_digest, batch_key_json, business_date,
                                   deadline, late_window_end, current_state,
                                   current_revision, created_at, updated_at)
               VALUES(:id, :contract_digest, :batch_key_json, :business_date,
                      :deadline, :late_window_end, :current_state,
                      :current_revision, :created_at, :updated_at)""",
            row,
        )

    def update_batch_state(self, batch_id: str, state: str, revision: int) -> None:
        self.conn.execute(
            "UPDATE batches SET current_state=?, current_revision=?, updated_at=? WHERE id=?",
            (state, revision, self._now_provider(), batch_id),
        )

    # ------------------------------------------------------------------ events
    def append_event(
        self, batch_id: str, event_type: str, payload: Dict[str, Any]
    ) -> int:
        cur = self.conn.execute(
            "INSERT INTO events(batch_id, event_type, payload, created_at) VALUES(?, ?, ?, ?)",
            (batch_id, event_type, self.dumps(payload), self._now_provider()),
        )
        return int(cur.lastrowid)

    def events_for(self, batch_id: str) -> List[sqlite3.Row]:
        return self.conn.execute(
            "SELECT * FROM events WHERE batch_id=? ORDER BY seq", (batch_id,)
        ).fetchall()

    def all_events(self) -> List[sqlite3.Row]:
        return self.conn.execute(
            "SELECT batch_id, seq, event_type, payload, created_at FROM events ORDER BY seq"
        ).fetchall()

    # --------------------------------------------------------------- arrivals
    def insert_arrival(self, row: Dict[str, Any]) -> int:
        cur = self.conn.execute(
            """INSERT INTO member_arrivals(batch_id, member_name, seq, received_at,
                    source_path, blob_sha256, size_bytes, raw_digests_json,
                    payload_digests_json, compression, valid_arrival,
                    quarantine_reason, detail_json)
               VALUES(:batch_id, :member_name, :seq, :received_at, :source_path,
                      :blob_sha256, :size_bytes, :raw_digests_json,
                      :payload_digests_json, :compression, :valid_arrival,
                      :quarantine_reason, :detail_json)""",
            row,
        )
        return int(cur.lastrowid)

    def next_arrival_seq(self, batch_id: str) -> int:
        row = self.conn.execute(
            "SELECT COALESCE(MAX(seq), 0) AS m FROM member_arrivals WHERE batch_id=?",
            (batch_id,),
        ).fetchone()
        return int(row["m"]) + 1

    def arrivals_for(self, batch_id: str) -> List[sqlite3.Row]:
        return self.conn.execute(
            "SELECT * FROM member_arrivals WHERE batch_id=? ORDER BY seq",
            (batch_id,),
        ).fetchall()

    def latest_arrival(self, batch_id: str, member: str) -> Optional[sqlite3.Row]:
        return self.conn.execute(
            "SELECT * FROM member_arrivals WHERE batch_id=? AND member_name=? "
            "ORDER BY seq DESC LIMIT 1",
            (batch_id, member),
        ).fetchone()

    # ----------------------------------------------------- validation findings
    def add_validation(
        self,
        batch_id: str,
        kind: str,
        member_name: Optional[str] = None,
        detail: Optional[Dict[str, Any]] = None,
        arrival_id: Optional[int] = None,
    ) -> int:
        cur = self.conn.execute(
            """INSERT INTO validation_events(batch_id, arrival_id, kind, member_name,
                                              detail_json, created_at)
               VALUES(?, ?, ?, ?, ?, ?)""",
            (
                batch_id,
                arrival_id,
                kind,
                member_name,
                self.dumps(detail or {}),
                self._now_provider(),
            ),
        )
        return int(cur.lastrowid)

    def validation_for(self, batch_id: str) -> List[sqlite3.Row]:
        return self.conn.execute(
            "SELECT * FROM validation_events WHERE batch_id=? ORDER BY id",
            (batch_id,),
        ).fetchall()

    # ---------------------------------------------------------------- closures
    def insert_closure(self, row: Dict[str, Any]) -> int:
        cur = self.conn.execute(
            """INSERT INTO closures(batch_id, revision, state, contract_digest,
                    binding_json, binding_sig, missing_json, parent_revision,
                    impact_json, decided_at, prev_evidence_hash, evidence_hash)
               VALUES(:batch_id, :revision, :state, :contract_digest,
                      :binding_json, :binding_sig, :missing_json, :parent_revision,
                      :impact_json, :decided_at, :prev_evidence_hash,
                      :evidence_hash)""",
            row,
        )
        return int(cur.lastrowid)

    def latest_closure(self, batch_id: str) -> Optional[sqlite3.Row]:
        return self.conn.execute(
            "SELECT * FROM closures WHERE batch_id=? ORDER BY revision DESC LIMIT 1",
            (batch_id,),
        ).fetchone()

    def closure_at(self, batch_id: str, revision: int) -> Optional[sqlite3.Row]:
        return self.conn.execute(
            "SELECT * FROM closures WHERE batch_id=? AND revision=?",
            (batch_id, revision),
        ).fetchone()

    def closures_for(self, batch_id: str) -> List[sqlite3.Row]:
        return self.conn.execute(
            "SELECT * FROM closures WHERE batch_id=? ORDER BY revision", (batch_id,)
        ).fetchall()

    # ------------------------------------------------------------------ claims
    def insert_claim(self, row: Dict[str, Any]) -> None:
        self.conn.execute(
            """INSERT INTO claims(batch_id, revision, token, status, claimed_by,
                                  claimed_at, completed_at, binding_sig)
               VALUES(:batch_id, :revision, :token, :status, :claimed_by,
                      :claimed_at, :completed_at, :binding_sig)""",
            row,
        )

    def get_claim(self, batch_id: str, revision: int) -> Optional[sqlite3.Row]:
        return self.conn.execute(
            "SELECT * FROM claims WHERE batch_id=? AND revision=?",
            (batch_id, revision),
        ).fetchone()

    def active_claims(self, batch_id: str) -> List[sqlite3.Row]:
        return self.conn.execute(
            "SELECT * FROM claims WHERE batch_id=? AND status='CLAIMED' ORDER BY id",
            (batch_id,),
        ).fetchall()

    def claim_by_token(self, token: str) -> Optional[sqlite3.Row]:
        return self.conn.execute(
            "SELECT * FROM claims WHERE token=?", (token,)
        ).fetchone()

    def complete_claim(self, token: str, status: str) -> None:
        self.conn.execute(
            "UPDATE claims SET status=?, completed_at=? WHERE token=?",
            (status, self._now_provider(), token),
        )

    # ------------------------------------------------------------------- blobs
    def blob_path(self, sha256_hex: str) -> Path:
        from .paths import relative_blob_name

        p = self.blob_root / relative_blob_name(sha256_hex)
        p.parent.mkdir(parents=True, exist_ok=True)
        return p

    def blob_exists(self, sha256_hex: str) -> bool:
        from .paths import relative_blob_name

        return (self.blob_root / relative_blob_name(sha256_hex)).exists()

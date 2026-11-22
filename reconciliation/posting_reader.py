"""Read-only view of the fixed local posting library.

The posting library is the SQLite file provisioned by the existing
``posting`` package (the same file that holds the netting facts).  This
module only ever opens it through a ``mode=ro`` URI connection: reconciliation
must not write to it, must not regenerate posting flows and must not alter any
published batch.  Every lookup is an indexed point lookup -- reconciliation
imports and batch queries never scan posting history unrelated to the flows
they are asked about.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any, Dict, List, Optional

from .errors import BatchNotFound, UnreadablePostingLibrary


class PostingReader:
    """Point-lookup reads against the immutable confirmed posting tables."""

    def __init__(self, posting_db: str):
        self.db_path = Path(posting_db)
        if not self.db_path.is_file():
            raise UnreadablePostingLibrary(
                f"posting library not found: {self.db_path}"
            )
        uri = f"file:{self.db_path}?mode=ro"
        try:
            self.conn = sqlite3.connect(uri, uri=True, timeout=30,
                                        isolation_level=None)
        except sqlite3.OperationalError as exc:
            raise UnreadablePostingLibrary(
                f"cannot open posting library {self.db_path}: {exc}"
            ) from exc
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA query_only=ON")
        self.conn.execute("PRAGMA busy_timeout=30000")
        # Defence in depth: this connection must never hold a write lock.
        self.conn.execute("PRAGMA query_only=ON")

    def close(self) -> None:
        self.conn.close()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()

    def batch_exists(self, batch_no: int) -> bool:
        row = self.conn.execute(
            "SELECT 1 FROM posting_batches WHERE batch_no=?", (batch_no,)
        ).fetchone()
        return row is not None

    def require_batch(self, batch_no: int) -> Dict[str, Any]:
        row = self.conn.execute(
            """SELECT batch_no, posting_day, confirmed_at, entry_count
                 FROM posting_batches WHERE batch_no=?""",
            (batch_no,),
        ).fetchone()
        if row is None:
            raise BatchNotFound(f"no confirmed posting batch {batch_no}")
        return dict(row)

    def entries_of_batch(self, batch_no: int) -> List[sqlite3.Row]:
        """All confirmed flow lines of one batch, in stable entry order."""
        return self.conn.execute(
            """SELECT flow_id, batch_no, entry_no, line_type, cents,
                      reverses_flow_id
                 FROM posting_entries
                WHERE batch_no=?
                ORDER BY entry_no""",
            (batch_no,),
        ).fetchall()

    def flow_exists(self, flow_id: int) -> bool:
        """A flow is referenceable only if it is a confirmed posting line."""
        row = self.conn.execute(
            "SELECT 1 FROM posting_entries WHERE flow_id=?", (flow_id,)
        ).fetchone()
        return row is not None

    def get_flow(self, flow_id: int) -> Optional[Dict[str, Any]]:
        row = self.conn.execute(
            """SELECT flow_id, batch_no, entry_no, line_type, cents,
                      reverses_flow_id
                 FROM posting_entries WHERE flow_id=?""",
            (flow_id,),
        ).fetchone()
        return dict(row) if row is not None else None

    def flows_exist_many(self, flow_ids: List[int]) -> List[int]:
        """Return, in input order, the ids that are not confirmed flows."""
        unknown: List[int] = []
        for flow_id in flow_ids:
            if not self.flow_exists(flow_id):
                unknown.append(flow_id)
        return unknown

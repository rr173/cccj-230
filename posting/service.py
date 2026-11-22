"""Local netting-posting handoff service.

The finance team hands the netting result off to a booking system.  The
downstream ledger cannot delete already-booked flows, so every difference
against the previously handed-over state is expressed as new flow lines:

* a pairing that appeared produces a ``forward`` line;
* a pairing that disappeared produces a full-amount ``reversal`` line naming
  the still-live forward line it cancels;
* an amount change on the same pairing is a full reversal of the old amount
  followed by a new forward line -- never a net difference;
* a pairing that moved to another netting day or changed either event
  identity is handled as disappearance of the old pairing plus appearance of
  a new one, because the business identity includes the netting business day
  and both event identities (``original business day + entry_id``);
* pairings that only flickered between two handoffs and left no net change
  produce no lines.

Posting days are independent of the netting business days and may equal the
previous batch's posting day but never precede it.  Corrections are booked on
the current posting day while each flow line keeps the original pairing's
business dates.

Only local state is touched: there is no external ledger connection and no
change to any netting matching rule.
"""

from __future__ import annotations

import uuid
from collections import Counter
from datetime import date
from typing import Any, Dict, List, Optional, Tuple

from .errors import (
    BatchNotFound,
    InvalidPostingDay,
    NothingToPost,
    PreviewStale,
    UnknownPreview,
)
from .store import IDENTITY_COLUMNS, PostingStore, utcnow_iso

Identity = Tuple[str, str, str, str, str, str]


def normalise_day(day: str) -> str:
    """Validate a strict ``YYYY-MM-DD`` calendar date."""
    if not isinstance(day, str):
        raise InvalidPostingDay(
            f"posting day must be a YYYY-MM-DD string, got "
            f"{type(day).__name__}"
        )
    text = day.strip()
    if len(text) != 10 or text[4] != "-" or text[7] != "-":
        raise InvalidPostingDay(f"posting day {day!r} is not YYYY-MM-DD")
    try:
        parsed = date.fromisoformat(text)
    except ValueError as exc:
        raise InvalidPostingDay(f"posting day {day!r}: {exc}") from exc
    if parsed.isoformat() != text:
        raise InvalidPostingDay(
            f"posting day {day!r} is not a strict calendar date"
        )
    return text


def _identity_of(row: Any) -> Identity:
    return tuple(row[c] for c in IDENTITY_COLUMNS)  # type: ignore[return-value]


def _event(day: str, entry_id: str) -> str:
    return f"{day}:{entry_id}"


class PostingService:
    """Preview / confirm / read interface backed by the netting SQLite file."""

    def __init__(self, netting_db, now_provider=utcnow_iso):
        """Open (and first-enable) the handoff on an existing netting database.

        The database may already contain netting facts: first enablement
        bootstraps the initial pending set from those facts, without
        re-importing any source file.
        """
        self.store = PostingStore(netting_db, now_provider=now_provider)

    def close(self) -> None:
        self.store.close()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()

    # ------------------------------------------------------------ internals
    def _is_initialised(self) -> bool:
        return self.store.get_meta("initialised") == "1"

    def _advance_clock(self) -> int:
        """Fold every unprocessed application marker into the mutation clock.

        One marker covers one committed netting application.  Its change rows
        are compacted as a multiset difference: identical delete/insert pairs
        (the row-only rewrite an account recompute performs when pairings and
        amounts are unchanged) cancel to an empty difference and add zero to
        the clock; every application with a genuine business change -- new
        pairing, removed pairing or changed amount -- adds exactly one.

        Because each marker is evaluated separately, a change applied and
        later undone by another application still moves the clock twice: the
        interim mutation genuinely happened between two handoffs.

        The fold position is two coupled watermarks: ``last_revision_id``
        (which markers have been folded) and ``clock_window_seq`` (the change
        log seq the next marker window starts at).  They always move together
        so a later fold can never skip or re-read a marker's rows.
        """
        clock = int(self.store.get_meta("fact_clock_value", "0"))
        last_marker = int(self.store.get_meta("last_revision_id", "0"))
        window_start = int(self.store.get_meta("clock_window_seq", "0"))
        markers = self.store.revisions_after(last_marker)
        for marker in markers:
            # The triggers append exactly facts_deleted + facts_inserted rows
            # for this application, which delimits its change-log window.
            window_end = window_start + int(marker["facts_deleted"]) \
                + int(marker["facts_inserted"])
            rows = self.store.change_rows_between(window_start, window_end)
            if self._compacted_difference(rows):
                clock += 1
            last_marker = int(marker["id"])
            window_start = window_end
        if markers:
            self.store.set_meta("fact_clock_value", clock)
            self.store.set_meta("last_revision_id", last_marker)
            self.store.set_meta("clock_window_seq", window_start)
        return clock

    @staticmethod
    def _compacted_difference(rows) -> List[Tuple[Identity, str, int]]:
        """Multiset-difference one application's change rows.

        Returns the surviving ``(identity, op, cents)`` terms after cancelling
        identical opposite operations -- empty for a pure row rewrite.
        """
        counts: Counter = Counter()
        for row in rows:
            identity = tuple(row[c] for c in IDENTITY_COLUMNS)
            counts[(identity, row["op"], int(row["cents"]))] += 1
        # Cancel an INSERT term against the matching DELETE term of the same
        # identity and amount (the two halves of a row rewrite).
        delta: Counter = Counter()
        for term, count in counts.items():
            identity, op, cents = term
            opposite = (identity,
                        "DELETE" if op == "INSERT" else "INSERT", cents)
            paired = min(count, counts.get(opposite, 0))
            remaining = count - paired
            if remaining:
                delta[term] += remaining
        return list(delta.elements())

    def _initialise_bootstrap(self) -> int:
        """Mark the handoff initialised and fold pre-enablement applications.

        The existing netting facts are read in full once for the bootstrap
        preview.  Application markers that committed before the handoff was
        enabled are folded into the clock here so later staleness checks start
        from a coherent baseline; the bootstrap delta itself always comes from
        the current facts, not from the change log.  Returns the folded clock.
        """
        clock = self._advance_clock()
        self.store.set_meta("initialised", "1")
        # Watermarks (last_change_seq) are written at the first confirm; the
        # bootstrap preview only reads, so repeated previews before the first
        # confirm stay idempotent.
        return clock

    def _bootstrap_entries(self) -> List[Dict[str, Any]]:
        """Every current pairing as a forward line (nothing handed over yet)."""
        entries: List[Dict[str, Any]] = []
        for row in self.store.all_facts_for_bootstrap():
            entries.append({
                "line_type": "forward",
                "identity": _identity_of(row),
                "cents": int(row["cents"]),
                "target_flow_id": None,
            })
        return entries

    def _incremental_entries(self) -> List[Dict[str, Any]]:
        """Compact unconsumed change-log rows into handoff flow lines.

        Only identities with at least one change-log row past the watermark
        are touched; each identity resolves to an indexed point lookup in
        ``posting_live``.  No historical pairing or published flow is scanned.
        """
        last_seq = int(self.store.get_meta("last_change_seq", "0"))
        rows = self.store.change_rows_after(last_seq)

        # Last-writer-wins per identity: a pairing that changed several times
        # between handoffs collapses to its final presence state, so flickers
        # that leave no trace produce no flow at all.
        final_state: Dict[Identity, Tuple[str, int]] = {}
        for row in rows:
            final_state[_identity_of(row)] = (row["op"], int(row["cents"]))

        entries: List[Dict[str, Any]] = []
        for identity, (op, cents) in final_state.items():
            live = self.store.live_for_identity(identity)
            if op == "DELETE":
                # The pairing disappeared.  If it was never handed over (no
                # live forward line) there is nothing downstream to reverse.
                if live is None:
                    continue
                entries.append({
                    "line_type": "reversal",
                    "identity": identity,
                    "cents": int(live["cents"]),
                    "target_flow_id": int(live["flow_id"]),
                })
                continue

            # op == "INSERT": the pairing exists now.
            if live is None:
                # New pairing relative to the last handoff.
                entries.append({
                    "line_type": "forward",
                    "identity": identity,
                    "cents": cents,
                    "target_flow_id": None,
                })
            elif int(live["cents"]) != cents:
                # Amount change: reverse the whole old amount first, then
                # register the new amount.  No net-difference line.
                entries.append({
                    "line_type": "reversal",
                    "identity": identity,
                    "cents": int(live["cents"]),
                    "target_flow_id": int(live["flow_id"]),
                })
                entries.append({
                    "line_type": "forward",
                    "identity": identity,
                    "cents": cents,
                    "target_flow_id": None,
                })
            # Same identity and same cents => no business change, no line.
        return entries

    def _order_entries(
        self, raw: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """Give lines deterministic, stable entry numbers.

        Reversal of a pairing always sorts before that pairing's new forward
        line.  Everything else is ordered by the full business identity so the
        numbers are reproducible across processes.
        """
        ordered = sorted(
            raw,
            key=lambda e: (
                e["identity"],
                0 if e["line_type"] == "reversal" else 1,
            ),
        )
        for index, entry in enumerate(ordered, start=1):
            entry["entry_no"] = index
        return ordered

    def _serialise_entry(
        self, entry: Dict[str, Any], preview_id: Optional[str] = None,
        batch_no: Optional[int] = None,
    ) -> Dict[str, Any]:
        identity = entry["identity"]
        payload = {
            "entry_no": entry["entry_no"],
            "line_type": entry["line_type"],
            "account_id": identity[0],
            "match_day": identity[1],
            "bill_day": identity[2],
            "bill_entry_id": identity[3],
            "payment_day": identity[4],
            "payment_entry_id": identity[5],
            "cents": entry["cents"],
            "bill_event": _event(identity[2], identity[3]),
            "payment_event": _event(identity[4], identity[5]),
            "reverses_flow_id": entry.get("target_flow_id"),
        }
        if preview_id is not None:
            payload["preview_id"] = preview_id
            payload["target_flow_id"] = entry.get("target_flow_id")
        if batch_no is not None:
            payload["batch_no"] = batch_no
        return payload

    # -------------------------------------------------------------- preview
    def preview(self, posting_day: str) -> Dict[str, Any]:
        """Build the handoff preview for one posting day; handoff state is unchanged.

        Raises :class:`NothingToPost` when there is no pending flow line: an
        empty batch must never be published.
        """
        day = normalise_day(posting_day)
        now = self.store.now_provider()
        preview_id = uuid.uuid4().hex

        self.store.begin()
        try:
            last_posting_day = self.store.get_meta("last_posting_day")
            if last_posting_day is not None and day < last_posting_day:
                raise InvalidPostingDay(
                    f"posting day {day} is earlier than the previous batch's "
                    f"posting day {last_posting_day}"
                )

            bootstrap = not self._is_initialised()
            if bootstrap:
                basis_clock = self._initialise_bootstrap()
                raw = self._bootstrap_entries()
            else:
                basis_clock = self._advance_clock()
                raw = self._incremental_entries()
            # The clock is a derived read of the immutable change log, so
            # folding it inside a preview does not mutate the handoff state;
            # the watermarks only move at confirm time.

            if not raw:
                # No pending lines: do not persist a preview and never publish
                # an empty batch.
                self.store.rollback()
                raise NothingToPost(
                    "no netting fact changes pending handoff; nothing to post"
                )

            entries = self._order_entries(raw)
            basis_last_seq = self.store.max_change_seq()
            basis_last_batch = self.store.max_batch_no()

            self.store.insert_preview(
                preview_id, day, basis_clock, basis_last_seq,
                basis_last_batch, now,
            )
            for entry in entries:
                self.store.insert_preview_entry(
                    self._serialise_entry(entry, preview_id=preview_id)
                )
            self.store.commit()
        except Exception:
            self.store.rollback()
            raise

        return {
            "preview_id": preview_id,
            "posting_day": day,
            "status": "pending",
            "bootstrap": bootstrap,
            "basis": {
                "fact_clock": basis_clock,
                "last_change_seq": basis_last_seq,
                "last_batch_no": basis_last_batch,
            },
            "entry_count": len(entries),
            "entries": [self._serialise_entry(e) for e in entries],
        }

    # -------------------------------------------------------------- confirm
    def confirm(self, preview_id: str) -> Dict[str, Any]:
        """Publish exactly the lines of one preview as a new immutable batch.

        Confirming an already-confirmed preview is idempotent and returns the
        same batch without writing any flow.  A stale preview raises
        :class:`PreviewStale`; the caller must preview again.
        """
        if not isinstance(preview_id, str) or not preview_id:
            raise UnknownPreview("preview_id must be a non-empty string")

        self.store.begin()
        try:
            preview = self.store.get_preview(preview_id)
            if preview is None:
                self.store.rollback()
                raise UnknownPreview(f"no preview {preview_id!r}")

            if preview["status"] == "confirmed":
                # Idempotent retry: hand back the exact same batch.  Later
                # corrections do not turn a successful confirmation into a
                # failure; they simply belong to the next preview.
                self.store.rollback()
                return self.read_batch(
                    self._batch_no_of_preview(preview_id))
            if preview["status"] == "superseded":
                self.store.rollback()
                raise PreviewStale(
                    "preview was already superseded; preview again"
                )

            # Fold any applications that committed after the preview was
            # built.  The fold is what makes a later change-back still stale.
            self._advance_clock()
            staleness = self._staleness_reason(preview)
            if staleness is not None:
                self.store.mark_preview(preview_id, "superseded")
                self.store.commit()
                raise PreviewStale(staleness)

            preview_rows = self.store.preview_entries(preview_id)
            if not preview_rows:  # defensive, previews are never stored empty
                self.store.rollback()
                raise NothingToPost("preview carries no entries")

            now = self.store.now_provider()
            batch_no = self.store.max_batch_no() + 1
            posting_day = preview["posting_day"]
            self.store.insert_batch(
                batch_no, posting_day, now, len(preview_rows),
                int(preview["basis_clock"]),
                int(preview["basis_last_seq"]),
                int(preview["basis_last_batch"]), preview_id,
            )

            for row in preview_rows:
                entry = self._entry_from_preview_row(row, batch_no)
                if entry["line_type"] == "reversal":
                    target = entry["reverses_flow_id"]
                    # The reversal must still point at the live forward line.
                    live = self.store.conn.execute(
                        "SELECT flow_id FROM posting_live WHERE flow_id=?",
                        (target,),
                    ).fetchone()
                    if live is None:
                        raise PreviewStale(
                            "reversal target is no longer the live forward "
                            "line; preview again"
                        )
                    flow_id = self.store.insert_entry(entry)
                    self.store.delete_live(target)
                else:
                    flow_id = self.store.insert_entry(entry)
                    self.store.insert_live(flow_id, entry)

            self.store.mark_preview(preview_id, "confirmed")
            # Fold up to the current instant, then consume every change-log
            # row up to this instant -- including the paired rows of pure row
            # rewrites -- so the next handoff's delta starts from the current
            # facts and live map.  The delta watermark and the clock window
            # are both set to the current end of the log, and the marker
            # watermark to the current end of markers, keeping all three
            # coherent for the next fold.
            self._advance_clock()
            self.store.set_meta("last_change_seq",
                                self.store.max_change_seq())
            self.store.set_meta("clock_window_seq",
                                self.store.max_change_seq())
            self.store.set_meta("last_revision_id",
                                self.store.max_revision_id())
            self.store.set_meta("last_batch_no", batch_no)
            self.store.set_meta("last_posting_day", posting_day)
            self.store.commit()
        except Exception:
            self.store.rollback()
            raise

        return self.read_batch(batch_no)

    def _batch_no_of_preview(self, preview_id: str) -> int:
        row = self.store.read_conn.execute(
            "SELECT batch_no FROM posting_batches WHERE preview_id=?",
            (preview_id,),
        ).fetchone()
        if row is None:  # pragma: no cover - confirmed preview always has one
            raise UnknownPreview(f"confirmed preview {preview_id!r} has no batch")
        return int(row["batch_no"])

    def _staleness_reason(self, preview) -> Optional[str]:
        """Why a pending preview can no longer be confirmed, if at all."""
        current_clock = int(self.store.get_meta("fact_clock_value", "0"))
        if current_clock != int(preview["basis_clock"]):
            return (
                "netting facts changed after this preview was built "
                f"(fact clock {preview['basis_clock']} -> {current_clock}); "
                "preview again"
            )
        current_batch = self.store.max_batch_no()
        if current_batch != int(preview["basis_last_batch"]):
            return (
                "another preview was confirmed first "
                f"(batch {current_batch}); preview again"
            )
        return None

    @staticmethod
    def _entry_from_preview_row(
        row, batch_no: int
    ) -> Dict[str, Any]:
        return {
            "batch_no": batch_no,
            "entry_no": int(row["entry_no"]),
            "line_type": row["line_type"],
            "account_id": row["account_id"],
            "match_day": row["match_day"],
            "bill_day": row["bill_day"],
            "bill_entry_id": row["bill_entry_id"],
            "payment_day": row["payment_day"],
            "payment_entry_id": row["payment_entry_id"],
            "cents": int(row["cents"]),
            "reverses_flow_id": row["target_flow_id"],
        }

    # ---------------------------------------------------------------- reads
    def read_batch(self, batch_no: int) -> Dict[str, Any]:
        """Read one confirmed, immutable batch by its stable batch number."""
        if not isinstance(batch_no, int) or isinstance(batch_no, bool) \
                or batch_no <= 0:
            raise BatchNotFound(f"invalid batch number {batch_no!r}")
        batch = self.store.get_batch(batch_no)
        if batch is None:
            raise BatchNotFound(f"no confirmed batch {batch_no}")
        entries = [
            self._serialise_stored_entry(row)
            for row in self.store.get_batch_entries(batch_no)
        ]
        return {
            "batch_no": int(batch["batch_no"]),
            "posting_day": batch["posting_day"],
            "confirmed_at": batch["confirmed_at"],
            "entry_count": int(batch["entry_count"]),
            "preview_id": batch["preview_id"],
            "entries": entries,
        }

    def list_batches(self) -> List[Dict[str, Any]]:
        """Batch headers in stable ascending order (entries not included)."""
        rows = self.store.read_conn.execute(
            """SELECT batch_no, posting_day, confirmed_at, entry_count,
                      preview_id
                 FROM posting_batches ORDER BY batch_no"""
        ).fetchall()
        return [dict(r) for r in rows]

    def _serialise_stored_entry(self, row) -> Dict[str, Any]:
        return {
            "flow_id": int(row["flow_id"]),
            "entry_no": int(row["entry_no"]),
            "line_type": row["line_type"],
            "account_id": row["account_id"],
            "match_day": row["match_day"],
            "bill_day": row["bill_day"],
            "bill_entry_id": row["bill_entry_id"],
            "payment_day": row["payment_day"],
            "payment_entry_id": row["payment_entry_id"],
            "bill_event": _event(row["bill_day"], row["bill_entry_id"]),
            "payment_event": _event(row["payment_day"],
                                    row["payment_entry_id"]),
            "cents": int(row["cents"]),
            "reverses_flow_id": row["reverses_flow_id"],
        }

    # ---------------------------------------------------------- inspection
    def handoff_state(self) -> Dict[str, Any]:
        """Snapshot of how far the handoff has progressed (for tests/ops)."""
        return {
            "initialised": self._is_initialised(),
            "fact_clock": int(self.store.get_meta("fact_clock_value", "0")),
            "last_revision_id": int(
                self.store.get_meta("last_revision_id", "0")),
            "clock_window_seq": int(
                self.store.get_meta("clock_window_seq", "0")),
            "last_change_seq": int(
                self.store.get_meta("last_change_seq", "0")),
            "last_batch_no": self.store.max_batch_no(),
            "last_posting_day": self.store.get_meta("last_posting_day"),
        }

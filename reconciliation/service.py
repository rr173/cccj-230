"""Local receipt reconciliation service.

Two responsibilities, both purely local:

* :meth:`import_receipt` validates and persists one complete new version of a
  general-ledger voucher group into the separate receipt database.  The local
  posting library is only read to confirm that every referenced flow exists.
* :meth:`batch_status` reconciles one confirmed posting batch by flow id: for
  every flow of the batch (and the original forward flows explicitly named by
  its reversal lines) it compares the amount the local posting library says
  should be booked with the same-/opposite-direction contributions of the
  currently effective receipt versions, and decides whether finance can close
  the batch.

Nothing here ever writes to the posting library, mutates a published batch or
regenerates posting flows, and no network interface is provided.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence

from .errors import BatchNotFound, MalformedReceipt, StaleRevision
from .posting_reader import PostingReader
from .store import ReceiptStore, utcnow_iso
from .validation import canonical_lines, parse_receipt


class ReconciliationService:
    """Import receipts and answer batch closeability questions."""

    def __init__(self, posting_db: str, receipt_db: str,
                 now_provider=utcnow_iso):
        self.reader = PostingReader(posting_db)
        self.store = ReceiptStore(receipt_db, now_provider=now_provider)

    def close(self) -> None:
        self.store.close()
        self.reader.close()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()

    # -------------------------------------------------------------- imports
    def import_receipt_file(self, path: str) -> Dict[str, Any]:
        """Load a UTF-8 JSON receipt document from ``path`` and import it."""
        import json

        try:
            text = Path(path).read_text(encoding="utf-8")
        except OSError as exc:
            raise MalformedReceipt(f"cannot read receipt file {path}: {exc}") \
                from exc
        try:
            document = json.loads(text)
        except json.JSONDecodeError as exc:
            self._audit_unparseable(path, f"invalid JSON: {exc}")
            raise MalformedReceipt(f"invalid JSON in {path}: {exc}") from exc
        return self.import_receipt(document)

    def import_receipt(self, document: Any) -> Dict[str, Any]:
        """Validate and apply one receipt document.

        Returns a result descriptor.  Raises :class:`MalformedReceipt` for an
        illegal document or unknown flows and :class:`StaleRevision` for a
        lower revision or differing same-revision content.  In every rejection
        case the group's current effective version is unchanged.
        """
        try:
            receipt_id, revision, lines = parse_receipt(document)
        except MalformedReceipt as exc:
            self._audit_unparseable(
                self._safe_receipt_id(document), str(exc),
                revision=self._safe_revision(document),
                line_count=self._safe_line_count(document),
            )
            raise

        # The posting library uses integer flow primary keys; receipt flow ids
        # are ASCII alphanumeric strings.  A non-numeric id cannot denote a
        # local flow, so it is an unknown flow like any missing numeric id.
        numeric_lines: List[Dict[str, Any]] = []
        unknown: List[str] = []
        for line in lines:
            raw_id = line["flow_id"]
            if not raw_id.isdigit():
                unknown.append(raw_id)
                continue
            flow_id = int(raw_id)
            if not self.reader.flow_exists(flow_id):
                unknown.append(raw_id)
                continue
            numeric_lines.append({
                "line_id": line["line_id"],
                "flow_id": flow_id,
                "signed_cents": line["signed_cents"],
            })
        if unknown:
            reason = f"unknown posting flows referenced: {sorted(unknown)}"
            self._audit_rejection(receipt_id, revision, len(lines), reason)
            raise MalformedReceipt(reason)

        now = self.store.now_provider()
        self.store.begin()
        try:
            group = self.store.get_group(receipt_id)
            if group is None:
                # First version need not start at 1; an empty first document
                # simply installs an already-withdrawn group.
                self.store.replace_group(receipt_id, revision,
                                         numeric_lines, now)
                outcome = "accepted"
                reason: Optional[str] = None
            else:
                current_revision = int(group["current_revision"])
                if revision < current_revision:
                    reason = (
                        f"revision {revision} is older than current "
                        f"revision {current_revision}"
                    )
                    self.store.log_import(
                        receipt_id, revision, len(numeric_lines),
                        "rejected", reason, now,
                    )
                    self.store.commit()
                    raise StaleRevision(reason)
                if revision == current_revision:
                    stored = [dict(r) for r in
                              self.store.get_lines(receipt_id)]
                    if canonical_lines(stored) == \
                            canonical_lines(numeric_lines):
                        # Idempotent resend: same flow and amount per line id,
                        # regardless of line order.  Nothing is counted twice.
                        self.store.log_import(
                            receipt_id, revision, len(numeric_lines),
                            "idempotent", None, now,
                        )
                        self.store.commit()
                        return self._result(receipt_id, revision,
                                            numeric_lines, "idempotent")
                    reason = (
                        f"revision {revision} already exists with different "
                        "content; only a higher revision may replace it"
                    )
                    self.store.log_import(
                        receipt_id, revision, len(numeric_lines),
                        "rejected", reason, now,
                    )
                    self.store.commit()
                    raise StaleRevision(reason)
                # Higher revision: replace the whole group atomically.  An
                # empty line array withdraws the group (group row kept at the
                # new revision with zero contributing lines).
                self.store.replace_group(receipt_id, revision,
                                         numeric_lines, now)
                outcome = "replaced"

            self.store.log_import(
                receipt_id, revision, len(numeric_lines),
                outcome, None, now,
            )
            self.store.commit()
        except Exception:
            self.store.rollback()
            raise

        return self._result(receipt_id, revision, numeric_lines, outcome)

    def _audit_rejection(self, receipt_id: str, revision: int,
                         line_count: int, reason: str) -> None:
        """Persist a rejection that happened before the write transaction."""
        now = self.store.now_provider()
        self.store.begin()
        try:
            self.store.log_import(receipt_id, revision, line_count,
                                  "rejected", reason, now)
            self.store.commit()
        except Exception:
            self.store.rollback()
            raise

    def _audit_unparseable(self, receipt_id: str, reason: str,
                           revision: int = 0, line_count: int = 0) -> None:
        try:
            self._audit_rejection(receipt_id or "__unparseable__",
                                  revision, line_count, reason)
        except Exception:  # pragma: no cover - auditing must not mask cause
            pass

    @staticmethod
    def _safe_receipt_id(document: Any) -> str:
        if isinstance(document, dict):
            value = document.get("receipt_id")
            if isinstance(value, str):
                return value
        return "__unparseable__"

    @staticmethod
    def _safe_revision(document: Any) -> int:
        if isinstance(document, dict):
            value = document.get("revision")
            if isinstance(value, int) and not isinstance(value, bool):
                return value
        return 0

    @staticmethod
    def _safe_line_count(document: Any) -> int:
        if isinstance(document, dict) \
                and isinstance(document.get("lines"), list):
            return len(document["lines"])
        return 0

    def _result(self, receipt_id: str, revision: int,
                lines: List[Dict[str, Any]], outcome: str) -> Dict[str, Any]:
        return {
            "receipt_id": receipt_id,
            "revision": revision,
            "outcome": outcome,
            "line_count": len(lines),
            "withdrawn": len(lines) == 0,
            "lines": [dict(line) for line in lines],
        }

    # ------------------------------------------------------------- queries
    def list_groups(self) -> List[Dict[str, Any]]:
        rows = self.store.read_conn.execute(
            """SELECT receipt_id, current_revision, line_count, received_at
                 FROM receipt_groups ORDER BY receipt_id"""
        ).fetchall()
        return [dict(r) for r in rows]

    def get_group(self, receipt_id: str) -> Optional[Dict[str, Any]]:
        group = self.store.get_group(receipt_id)
        if group is None:
            return None
        result = dict(group)
        result["lines"] = [dict(r) for r in
                           self.store.get_lines(receipt_id)]
        return result

    def import_log(self) -> List[Dict[str, Any]]:
        return self.store.import_log()

    def batch_status(self, batch_no: int) -> Dict[str, Any]:
        """Reconcile one confirmed posting batch and judge closeability.

        A non-existent batch raises :class:`BatchNotFound` -- it can never be
        treated as closeable.
        """
        if not isinstance(batch_no, int) or isinstance(batch_no, bool) \
                or batch_no <= 0:
            raise BatchNotFound(f"invalid batch number {batch_no!r}")
        header = self.reader.require_batch(batch_no)
        entries = self.reader.entries_of_batch(batch_no)

        # Read scope: the queried batch's own flows plus, for each reversal,
        # the original forward flow it explicitly names -- even when that flow
        # lives in another batch.  Nothing else is touched.
        batch_flow_ids = [int(r["flow_id"]) for r in entries]
        origin_ids = [int(r["reverses_flow_id"]) for r in entries
                      if r["line_type"] == "reversal"]
        scoped_ids = sorted(set(batch_flow_ids) | set(origin_ids))

        receipt_rows = self.store.current_lines_for_flows(scoped_ids)
        contributions: Dict[int, List[Dict[str, Any]]] = {
            fid: [] for fid in scoped_ids
        }
        for row in receipt_rows:
            contributions[int(row["flow_id"])].append({
                "receipt_id": row["receipt_id"],
                "revision": int(row["current_revision"]),
                "line_id": row["line_id"],
                "signed_cents": int(row["signed_cents"]),
            })

        flow_records: Dict[int, Dict[str, Any]] = {}
        for flow_id in scoped_ids:
            flow = self.reader.get_flow(flow_id)
            # scoped ids come from confirmed rows (own entries or an FK target),
            # so the lookup is always a hit.
            assert flow is not None
            expected_signed = int(flow["cents"]) * (
                -1 if flow["line_type"] == "reversal" else 1)
            same, opposite = self._sum_directions(
                expected_signed, contributions[flow_id])
            flow_records[flow_id] = {
                "flow_id": flow_id,
                "batch_no": int(flow["batch_no"]),
                "entry_no": int(flow["entry_no"]),
                "line_type": flow["line_type"],
                "expected_signed_cents": expected_signed,
                "same_direction_cents": same,
                "opposite_direction_cents": opposite,
                "amount_matched":
                    same == abs(expected_signed) and opposite == 0,
                "reverses_flow_id": flow["reverses_flow_id"],
                "contributions": contributions[flow_id],
                "blockers": [],
            }

        # Blockers for the batch's own flows.  Reversal checks use the original
        # flow's id directly -- a later new forward for the same pairing does
        # not change which flow is checked.
        batch_flows: List[Dict[str, Any]] = []
        for flow_id in batch_flow_ids:
            record = flow_records[flow_id]
            record["blockers"] = self._blockers_for(record, flow_records)
            batch_flows.append(record)

        closeable = bool(batch_flows) and all(
            not record["blockers"] for record in batch_flows)
        matched_count = sum(1 for r in batch_flows if r["amount_matched"])

        return {
            "batch_no": int(header["batch_no"]),
            "posting_day": header["posting_day"],
            "confirmed_at": header["confirmed_at"],
            "closeable": closeable,
            "flow_count": len(batch_flows),
            "amount_matched_count": matched_count,
            "flows": batch_flows,
            "referenced_origins": [
                self._origin_summary(flow_records[origin])
                for origin in sorted(set(origin_ids))
            ],
        }

    @staticmethod
    def _sum_directions(expected_signed: int,
                        lines: Sequence[Dict[str, Any]]) -> tuple:
        """Absolute-cent totals split into same- and opposite-direction.

        Positive and negative bookings never net: a +130/-10 pair against a
        +120 expectation yields same=130 and opposite=10, not a balanced 120.
        """
        same = 0
        opposite = 0
        expected_positive = expected_signed > 0
        for line in lines:
            amount = abs(int(line["signed_cents"]))
            line_positive = int(line["signed_cents"]) > 0
            if line_positive == expected_positive:
                same += amount
            else:
                opposite += amount
        return same, opposite

    def _blockers_for(self, record: Dict[str, Any],
                      flow_records: Dict[int, Dict[str, Any]]
                      ) -> List[Dict[str, Any]]:
        blockers: List[Dict[str, Any]] = []
        expected_abs = abs(record["expected_signed_cents"])
        if not record["amount_matched"]:
            detail = {
                "reason": "amount_mismatch",
                "expected_abs_cents": expected_abs,
                "same_direction_cents": record["same_direction_cents"],
                "opposite_direction_cents":
                    record["opposite_direction_cents"],
            }
            if record["same_direction_cents"] < expected_abs:
                detail["problem"] = "underbooked"
                detail["shortfall_cents"] = expected_abs - \
                    record["same_direction_cents"]
            elif record["same_direction_cents"] > expected_abs:
                detail["problem"] = "overbooked"
                detail["excess_cents"] = record["same_direction_cents"] - \
                    expected_abs
            if record["opposite_direction_cents"]:
                detail["problem"] = (
                    detail["problem"] + "+opposite_direction"
                    if "problem" in detail else "opposite_direction")
            blockers.append(detail)

        if record["line_type"] == "reversal":
            origin_id = record["reverses_flow_id"]
            origin = flow_records[origin_id]
            if not origin["amount_matched"]:
                # Even a fully matched reversal keeps its batch open while the
                # explicit original forward flow is not amount-matched.
                blockers.append({
                    "reason": "reversed_original_unmatched",
                    "original_flow_id": origin_id,
                    "original_expected_signed_cents":
                        origin["expected_signed_cents"],
                    "original_same_direction_cents":
                        origin["same_direction_cents"],
                    "original_opposite_direction_cents":
                        origin["opposite_direction_cents"],
                })
        return blockers

    @staticmethod
    def _origin_summary(record: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "flow_id": record["flow_id"],
            "batch_no": record["batch_no"],
            "expected_signed_cents": record["expected_signed_cents"],
            "same_direction_cents": record["same_direction_cents"],
            "opposite_direction_cents":
                record["opposite_direction_cents"],
            "amount_matched": record["amount_matched"],
            "contributions": record["contributions"],
        }

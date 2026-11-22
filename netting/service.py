"""Per-entry netting fact service.

The service maintains a durable, correction-safe fact table on top of closed
batch-closure revisions:

* :meth:`NettingService.apply_revision` selects one explicit
  ``(business_date, revision)``, reads only that ``CLOSED`` binding's
  ``detail.csv`` via its binding summary, validates the payload and folds the
  day into the fact table;
* corrections on an old day never replay pre-correction pairings.  Only the
  accounts whose day-D rows changed are recomputed, starting from the newest
  open-state checkpoint strictly before D and replaying the already-stored
  per-day snapshots forward;
* facts of unaffected accounts are never rewritten and other days' source
  files are never opened;
* the whole application is one SQLite transaction: concurrent apps serialise,
  stale revisions lose, and readers can never see a new revision number with
  old facts.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Set, Tuple

from batch_closure import BatchClosureService

from .csv_detail import parse_detail_csv
from .engine import Entry, Fact, OpenState, advance_day, replay_account
from .errors import (
    NettingError,
    StaleRevision,
)
from .source import ClosedDetail, load_closed_detail, normalise_business_day, resolve_contract
from .store import NettingStore, utcnow_iso


class NettingService:
    """Open-state application + fact reads backed by one SQLite database."""

    def __init__(
        self,
        netting_db,
        closure: Optional[BatchClosureService] = None,
        closure_data_dir=None,
        closure_inbound_dir=None,
        closure_clock=None,
    ):
        """Either pass an opened ``BatchClosureService`` or the directories so
        the service can open its own read-only handle to the closure store."""
        self.store = NettingStore(netting_db)
        if closure is not None:
            self.closure = closure
            self._owns_closure = False
        else:
            if closure_data_dir is None:
                raise ValueError(
                    "provide either an opened closure service or closure_data_dir"
                )
            self.closure = BatchClosureService(
                Path(closure_data_dir),
                Path(closure_inbound_dir) if closure_inbound_dir else None,
                clock=closure_clock,
            )
            self._owns_closure = True

    def close(self) -> None:
        self.store.close()
        if self._owns_closure:
            self.closure.close()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()

    # ------------------------------------------------------------- applying
    def apply_revision(
        self,
        business_day: str,
        revision: int,
        contract_id: str,
        contract_version: Optional[str] = None,
        contract_digest: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Apply one explicit closed revision for one business day.

        Returns a summary dictionary.  Raises a :class:`NettingError`
        subclass for any rejection; rejections leave all published state
        untouched.
        """
        day = normalise_business_day(business_day)
        now = utcnow_iso()
        try:
            contract = resolve_contract(
                self.closure, contract_id, contract_version, contract_digest
            )
            # All source reads happen before the write transaction: a
            # malformed payload cannot leave partial writes behind, and the
            # closure database is only ever read.
            detail = load_closed_detail(self.closure, contract, day, revision)
            entries = parse_detail_csv(detail.detail_bytes, day)

            self.store.begin()
            try:
                existing = self.store.get_applied_day(day)
                if existing is not None and existing["revision"] > revision:
                    raise StaleRevision(
                        f"day {day} already selected at revision "
                        f"{existing['revision']}; revision {revision} is older"
                    )
                if existing is not None and existing["revision"] == revision:
                    # Same-version retry: the stored snapshot must equal the
                    # bound payload (defence in depth), then report
                    # idempotency without rewriting any fact.
                    if existing["detail_sha256"] != detail.detail_blob_sha256:
                        raise StaleRevision(
                            f"day {day} rev {revision} is stored with a "
                            "different binding payload; refusing to overwrite"
                        )
                    self.store.record_attempt(
                        day, revision, "idempotent",
                        f"revision {revision} already applied", now,
                    )
                    self.store.commit()
                    return {
                        "business_day": day,
                        "revision": revision,
                        "outcome": "idempotent",
                        "affected_accounts": [],
                        "facts_deleted": 0,
                        "facts_inserted": 0,
                    }

                affected = self._collect_affected_accounts(day, entries)
                deleted, inserted = self._install_revision(
                    day, revision, detail, entries, affected, now
                )
                self.store.record_attempt(
                    day, revision, "applied",
                    f"selected revision {revision}; accounts="
                    f"{sorted(affected)}; deleted={deleted}; "
                    f"inserted={inserted}",
                    now,
                )
                self.store.commit()
                return {
                    "business_day": day,
                    "revision": revision,
                    "outcome": "applied",
                    "affected_accounts": sorted(affected),
                    "facts_deleted": deleted,
                    "facts_inserted": inserted,
                }
            except Exception:
                self.store.rollback()
                raise
        except NettingError:
            # Published state is untouched; persist the rejection as an
            # independent audit row (its own short transaction).
            self._record_rejection(day, revision, now)
            raise

    def _record_rejection(self, day: str, revision: int, now: str) -> None:
        """Persist a rejection in its own short transaction.

        The main transaction was rolled back, so published state is unchanged;
        the audit row is intentionally independent of that rollback.
        """
        try:
            self.store.begin()
            self.store.record_attempt(
                day, revision, "rejected",
                "application rejected; published state unchanged", now,
            )
            self.store.commit()
        except Exception:  # pragma: no cover - audit must never mask the error
            self.store.rollback()

    def _collect_affected_accounts(
        self, day: str, new_entries: Sequence[Entry]
    ) -> Set[str]:
        """Accounts touched by a row-level add/delete/change on day D.

        The day snapshots are diffed on the entry identity
        ``(entry_id, account_id, kind, cents)``.  Only an account whose own
        rows actually differ is recomputed: an account whose day-D snapshot is
        byte-identical is left completely untouched (no fact rewrite, no
        checkpoint rewrite).  Rows moving between accounts surface on both the
        old and the new side.
        """
        old_entries = self.store.day_entries(day)

        def signature(rows: Sequence[Entry]) -> Dict[Tuple[str, str, str, int], str]:
            return {
                (e.entry_id, e.account_id, e.kind, e.cents): e.account_id
                for e in rows
            }

        old_sig = signature(old_entries)
        new_sig = signature(new_entries)
        changed_keys = set(old_sig) ^ set(new_sig)
        affected: Set[str] = set()
        for key in changed_keys:
            # The account id is part of the key; account moves therefore
            # naturally include both the losing and the winning account.
            affected.add(key[1])
        return affected

    def _install_revision(
        self,
        day: str,
        revision: int,
        detail: ClosedDetail,
        entries: Sequence[Entry],
        affected: Set[str],
        now: str,
    ) -> Tuple[int, int]:
        self.store.replace_day_entries(day, entries)
        self.store.upsert_applied_day(
            day, revision, detail.binding_sig, detail.detail_blob_sha256, now
        )

        applied_days = [
            r["business_day"] for r in self.store.all_applied_days()
        ]
        total_deleted = 0
        total_inserted = 0
        for account_id in sorted(affected):
            deleted, inserted = self._recompute_account(
                account_id, day, applied_days
            )
            total_deleted += deleted
            total_inserted += inserted
        return total_deleted, total_inserted

    def _recompute_account(
        self,
        account_id: str,
        changed_day: str,
        applied_days: Sequence[str],
    ) -> Tuple[int, int]:
        """Recompute one account from the checkpoint before the changed day.

        Only rows with ``match_day >= changed_day`` are deleted and rewritten;
        the pre-correction facts and checkpoints of this account stay exactly
        as they were.
        """
        checkpoint_row = self.store.latest_checkpoint_before(
            account_id, changed_day
        )
        if checkpoint_row is not None:
            state = OpenState.from_json(json.loads(checkpoint_row["state_json"]))
        else:
            state = OpenState(bills=[], payments=[])

        # Drop this account's maintained output from D onward, plus every
        # checkpoint from D onward (the ones before D are the resume anchor).
        deleted = self.store.delete_facts_for_account_from(
            account_id, changed_day
        )
        self.store.delete_checkpoints_for_account_from(account_id, changed_day)

        # Replay every selected day from D onward, using the stored per-day
        # snapshots only (never the closure service, never other source
        # files).  Each day is fetched individually so the changed day itself
        # is always included, even when resuming from an empty state.
        days_to_run = [d for d in applied_days if d >= changed_day]
        all_facts: List[Fact] = []
        for d in days_to_run:
            day_entries = [
                e for e in self.store.entries_for_account_from(account_id, d)
                if e.day == d
            ]
            _, facts = advance_day(state, account_id, d, day_entries)
            all_facts.extend(facts)
            # A checkpoint after every applied day makes any future older-day
            # correction resumable without replaying this prefix.
            self.store.upsert_checkpoint(account_id, d, state)

        self.store.insert_facts(all_facts)
        return deleted, len(all_facts)

    # -------------------------------------------------------------- reading
    def read_facts(
        self,
        account_id: Optional[str] = None,
        business_day: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """Return maintained pairings as plain dictionaries.

        Each row carries the two event identities both split and composed:
        ``bill_event`` / ``payment_event`` are ``"YYYY-MM-DD:entry_id"``.
        """
        if business_day is not None:
            normalise_business_day(business_day)
        rows = self.store.read_facts(account_id, business_day)
        for row in rows:
            row["bill_event"] = f"{row['bill_day']}:{row['bill_entry_id']}"
            row["payment_event"] = (
                f"{row['payment_day']}:{row['payment_entry_id']}"
            )
        return rows

    def selected_revisions(self) -> List[Dict[str, Any]]:
        """List the currently selected revision of every applied day."""
        return [dict(r) for r in self.store.all_applied_days_published()]

    def application_log(self, business_day: Optional[str] = None) -> List[Dict[str, Any]]:
        if business_day is not None:
            normalise_business_day(business_day)
        return self.store.list_attempts(business_day)

    def open_state_checkpoint(
        self, account_id: str, business_day: str
    ) -> Optional[Dict[str, Any]]:
        """Inspect the resumable open state after a given applied day."""
        day = normalise_business_day(business_day)
        row = self.store.read_conn.execute(
            "SELECT state_json FROM checkpoints WHERE account_id=? AND business_day=?",
            (account_id, day),
        ).fetchone()
        if row is None:
            return None
        return json.loads(row["state_json"])

    # -------------------------------------------------- verification helper
    def full_replay(self) -> List[Dict[str, Any]]:
        """Recompute every fact from stored snapshots, independent of checkpoints.

        Used by tests/demos to prove that the maintained incremental result
        always equals a from-scratch recomputation over all selected days.
        """
        applied_days = [r["business_day"]
                        for r in self.store.all_applied_days_published()]
        accounts = sorted({
            r["account_id"]
            for d in applied_days
            for r in self.store.read_conn.execute(
                "SELECT DISTINCT account_id FROM entries WHERE business_day=?",
                (d,),
            ).fetchall()
        })
        result: List[Dict[str, Any]] = []
        for account_id in accounts:
            days = [
                (d, [e for e in self.store.entries_for_account_from(account_id, d)
                     if e.day == d])
                for d in applied_days
            ]
            facts, _ = replay_account(account_id, days)
            for fact in facts:
                row = dict(zip(
                    ("account_id", "match_day", "bill_day", "bill_entry_id",
                     "payment_day", "payment_entry_id", "cents"),
                    fact.as_row(),
                ))
                row["bill_event"] = f"{row['bill_day']}:{row['bill_entry_id']}"
                row["payment_event"] = (
                    f"{row['payment_day']}:{row['payment_entry_id']}"
                )
                result.append(row)
        result.sort(key=lambda r: (
            r["account_id"], r["match_day"], r["bill_day"], r["bill_entry_id"],
            r["payment_day"], r["payment_entry_id"],
        ))
        return result

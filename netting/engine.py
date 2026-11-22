"""Pure account-scoped netting engine.

No SQLite, no closure-service calls and no clock live here: the engine only
advances one account's open state across one business day.  The persistence
layer drives it with immutable per-day entry snapshots, which is what lets a
correction on an old day be re-derived by replaying the stored snapshots
forward, without ever touching the closure service again.

Rules implemented (see the repository README for the full contract):

* matching only happens inside one ``account_id`` and partial application is
  allowed;
* per business day: unused payments whose life ended before the day are
  dropped first, then the day's bills/payments are admitted, then the oldest
  open bill is paired against the oldest still-usable payment;
* a payment is usable on its payment day through two calendar days later
  (inclusive) and may also cover bills from earlier days;
* bills never expire, expiring a payment never undoes completed matches;
* ties on the same day are broken by ``entry_id`` ASCII order, never by the
  physical row order inside the CSV.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

# A payment arriving on day D is usable on D, D+1 and D+2.
PAYMENT_LIFE_DAYS = 2


@dataclass(frozen=True)
class Entry:
    """One parsed CSV row.

    ``day`` is the normalised ``YYYY-MM-DD`` business date; the event identity
    of the row is ``(day, entry_id)``.
    """

    day: str
    entry_id: str
    account_id: str
    kind: str  # "bill" | "payment"
    cents: int

    @property
    def event_id(self) -> str:
        return f"{self.day}:{self.entry_id}"


@dataclass
class OpenBill:
    day: str
    entry_id: str
    remaining: int


@dataclass
class OpenPayment:
    day: str
    entry_id: str
    remaining: int

    @property
    def last_usable_day(self) -> date:
        return date.fromisoformat(self.day) + timedelta(days=PAYMENT_LIFE_DAYS)


@dataclass
class Fact:
    """One completed bill/payment pairing."""

    account_id: str
    match_day: str
    bill_day: str
    bill_entry_id: str
    payment_day: str
    payment_entry_id: str
    cents: int

    def as_row(self) -> Tuple[str, str, str, str, str, str, int]:
        return (
            self.account_id,
            self.match_day,
            self.bill_day,
            self.bill_entry_id,
            self.payment_day,
            self.payment_entry_id,
            self.cents,
        )


# An open-state checkpoint persisted after each applied business day.  The
# lists stay ordered by (day, entry_id) so replay resumes deterministically.
@dataclass
class OpenState:
    bills: List[OpenBill]
    payments: List[OpenPayment]

    def is_empty(self) -> bool:
        return not self.bills and not self.payments

    def to_json(self) -> Dict[str, list]:
        return {
            "bills": [b.__dict__ for b in self.bills],
            "payments": [p.__dict__ for p in self.payments],
        }

    @classmethod
    def from_json(cls, payload: Optional[Dict[str, list]]) -> "OpenState":
        payload = payload or {}
        return cls(
            bills=[OpenBill(**row) for row in payload.get("bills", [])],
            payments=[OpenPayment(**row) for row in payload.get("payments", [])],
        )


def _drop_expired_payments(state: OpenState, current: date) -> None:
    """Remove payments whose inclusive two-day window ended before ``current``."""
    state.payments = [
        p for p in state.payments if p.last_usable_day >= current
    ]


def _admit_day_entries(
    state: OpenState, day: str, entries: Sequence[Entry]
) -> None:
    """Fold the day's rows in ``entry_id`` ASCII order (row order is ignored)."""
    for entry in sorted(entries, key=lambda e: e.entry_id):
        if entry.kind == "bill":
            state.bills.append(
                OpenBill(day=entry.day, entry_id=entry.entry_id,
                         remaining=entry.cents)
            )
        else:
            state.payments.append(
                OpenPayment(day=entry.day, entry_id=entry.entry_id,
                            remaining=entry.cents)
            )
    # Both queues are keyed by (day, entry_id); admitting a single sorted day
    # into already-sorted queues keeps the overall ordering, but re-sorting is
    # cheap and makes the invariant explicit and self-healing.
    state.bills.sort(key=lambda b: (b.day, b.entry_id))
    state.payments.sort(key=lambda p: (p.day, p.entry_id))


def _pair(state: OpenState, account_id: str, match_day: str) -> List[Fact]:
    """Greedily match the oldest open bill against the oldest usable payment."""
    facts: List[Fact] = []
    current = date.fromisoformat(match_day)
    while state.bills and state.payments:
        # The expiry sweep already ran; this only guards a queue whose head
        # became stale between sweeps and keeps the loop invariant explicit.
        if state.payments[0].last_usable_day < current:
            _drop_expired_payments(state, current)
            continue
        bill = state.bills[0]
        payment = state.payments[0]
        applied = min(bill.remaining, payment.remaining)
        facts.append(
            Fact(
                account_id=account_id,
                match_day=match_day,
                bill_day=bill.day,
                bill_entry_id=bill.entry_id,
                payment_day=payment.day,
                payment_entry_id=payment.entry_id,
                cents=applied,
            )
        )
        bill.remaining -= applied
        payment.remaining -= applied
        if bill.remaining == 0:
            state.bills.pop(0)
        if payment.remaining == 0:
            state.payments.pop(0)
    return facts


def advance_day(
    state: OpenState,
    account_id: str,
    day: str,
    day_entries: Sequence[Entry],
) -> Tuple[OpenState, List[Fact]]:
    """Advance one account from ``state`` through business day ``day``.

    Returns the new open state and the facts created on this day.  ``state``
    is mutated in place and also returned for convenience.
    """
    current = date.fromisoformat(day)
    _drop_expired_payments(state, current)
    _admit_day_entries(state, day, day_entries)
    facts = _pair(state, account_id, day)
    return state, facts


def replay_account(
    account_id: str,
    days: Iterable[Tuple[str, Sequence[Entry]]],
) -> Tuple[List[Fact], OpenState]:
    """Recompute every fact of one account from stored per-day snapshots.

    ``days`` is an iterable of ``(day, entries)``; it does not need to be
    sorted (empty days are skipped because they can neither expire a payment
    nor create a pairing that an applied day would not also produce).
    """
    state = OpenState(bills=[], payments=[])
    all_facts: List[Fact] = []
    for day, entries in sorted(days, key=lambda item: item[0]):
        if not entries:
            # Still honour expiries: an empty applied day may be the first day
            # on which an old payment is dead.  Sweep then continue.
            _drop_expired_payments(state, date.fromisoformat(day))
            continue
        _, facts = advance_day(state, account_id, day, entries)
        all_facts.extend(facts)
    return all_facts, state

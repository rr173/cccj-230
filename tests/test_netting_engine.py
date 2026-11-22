"""Pure engine tests: expiry, partial application, ordering and replay.

These tests never touch SQLite or the closure service; they pin the matching
semantics the service layer relies on.
"""

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from netting import Entry, OpenState, advance_day, replay_account  # noqa: E402


def e(day, entry_id, account, kind, cents):
    return Entry(day, entry_id, account, kind, cents)


def fact_keys(facts):
    return [
        (f.match_day, f.bill_entry_id, f.payment_entry_id, f.cents)
        for f in facts
    ]


class EngineTests(unittest.TestCase):
    def test_payment_covers_earlier_bill_with_partial_application(self):
        state = OpenState(bills=[], payments=[])
        _, f1 = advance_day(
            state, "A", "2026-11-01",
            [e("2026-11-01", "b1", "A", "bill", 1000)],
        )
        self.assertEqual(f1, [])
        state, f2 = advance_day(
            state, "A", "2026-11-02",
            [e("2026-11-02", "p1", "A", "payment", 300)],
        )
        self.assertEqual(fact_keys(f2),
                         [("2026-11-02", "b1", "p1", 300)])
        self.assertEqual(state.bills[0].remaining, 700)
        state, f3 = advance_day(
            state, "A", "2026-11-03",
            [e("2026-11-03", "p2", "A", "payment", 700)],
        )
        self.assertEqual(fact_keys(f3),
                         [("2026-11-03", "b1", "p2", 700)])
        self.assertTrue(state.is_empty())

    def test_payment_splits_across_multiple_bills(self):
        state = OpenState(bills=[], payments=[])
        advance_day(state, "A", "2026-11-01", [
            e("2026-11-01", "b1", "A", "bill", 100),
            e("2026-11-01", "b2", "A", "bill", 200),
        ])
        _, facts = advance_day(
            state, "A", "2026-11-01",
            [e("2026-11-01", "p1", "A", "payment", 250)],
        )
        self.assertEqual(fact_keys(facts), [
            ("2026-11-01", "b1", "p1", 100),
            ("2026-11-01", "b2", "p1", 150),
        ])
        self.assertEqual(state.payments, [])
        self.assertEqual(state.bills[0].remaining, 50)

    def test_payment_expires_after_two_calendar_days(self):
        # Payment on D1; usable D1, D2, D3; a bill arriving D4 cannot use it.
        state = OpenState(bills=[], payments=[])
        advance_day(state, "A", "2026-11-01",
                    [e("2026-11-01", "p1", "A", "payment", 500)])
        advance_day(state, "A", "2026-11-02", [])
        advance_day(state, "A", "2026-11-03", [])
        # Still usable on D3: a same-day bill consumes it.
        state3, facts3 = advance_day(
            state, "A", "2026-11-03",
            [e("2026-11-03", "b3", "A", "bill", 600)],
        )
        self.assertEqual(fact_keys(facts3),
                         [("2026-11-03", "b3", "p1", 500)])
        self.assertEqual(state3.bills[0].remaining, 100)

        # Fresh payment, never consumed: must be gone by D4.
        state = OpenState(bills=[], payments=[])
        advance_day(state, "A", "2026-11-01",
                    [e("2026-11-01", "p1", "A", "payment", 500)])
        advance_day(state, "A", "2026-11-02", [])
        advance_day(state, "A", "2026-11-03", [])
        state4, facts4 = advance_day(
            state, "A", "2026-11-04",
            [e("2026-11-04", "b4", "A", "bill", 100)],
        )
        self.assertEqual(facts4, [])
        self.assertEqual(state4.payments, [])
        self.assertEqual(state4.bills[0].remaining, 100)

    def test_expiry_does_not_undo_completed_matches(self):
        state = OpenState(bills=[], payments=[])
        advance_day(state, "A", "2026-11-01", [
            e("2026-11-01", "b1", "A", "bill", 100),
            e("2026-11-01", "p1", "A", "payment", 300),
        ])
        # 100 used on D1, 200 remains and later expires; the D1 fact stands.
        state, facts = advance_day(state, "A", "2026-11-04", [
            e("2026-11-04", "b4", "A", "bill", 200),
        ])
        self.assertEqual(facts, [])
        self.assertEqual(state.payments, [])
        self.assertEqual(state.bills[0].remaining, 200)

    def test_same_day_tie_breaks_by_entry_id_ascii_order(self):
        state = OpenState(bills=[], payments=[])
        _, facts = advance_day(state, "A", "2026-11-01", [
            # Deliberately shuffled physical order.
            e("2026-11-01", "p9", "A", "payment", 100),
            e("2026-11-01", "b2", "A", "bill", 150),
            e("2026-11-01", "p1", "A", "payment", 100),
            e("2026-11-01", "b1", "A", "bill", 50),
        ])
        # Oldest bill b1 pairs with oldest payment p1 first; p1 is exhausted
        # on b2, then p9 covers the rest of b2.
        self.assertEqual(fact_keys(facts), [
            ("2026-11-01", "b1", "p1", 50),
            ("2026-11-01", "b2", "p1", 50),
            ("2026-11-01", "b2", "p9", 100),
        ])
        # Totals match, so every bill closes and no payment remains.
        self.assertEqual(state.payments, [])
        self.assertEqual(state.bills, [])

    def test_matching_never_crosses_accounts(self):
        # The engine is account scoped: run two independent accounts and show
        # that A's bill is never settled by B's payment.
        state_a = OpenState(bills=[], payments=[])
        advance_day(state_a, "A", "2026-11-01",
                    [e("2026-11-01", "bA", "A", "bill", 100)])
        state_b = OpenState(bills=[], payments=[])
        _, facts_b = advance_day(
            state_b, "B", "2026-11-01",
            [e("2026-11-01", "pB", "B", "payment", 100)])
        self.assertEqual(facts_b, [])
        self.assertEqual(state_a.bills[0].entry_id, "bA")
        self.assertEqual(state_a.payments, [])
        self.assertEqual(state_b.payments[0].entry_id, "pB")
        self.assertEqual(state_b.bills, [])

    def test_replay_matches_sequential_advance(self):
        days = [
            ("2026-11-01", [e("2026-11-01", "b1", "A", "bill", 1000)]),
            ("2026-11-02", [e("2026-11-02", "p1", "A", "payment", 300)]),
            ("2026-11-03", [e("2026-11-03", "p2", "A", "payment", 700)]),
        ]
        facts_replay, final_replay = replay_account("A", days)

        state = OpenState(bills=[], payments=[])
        sequential = []
        for day, rows in days:
            sequential += advance_day(state, "A", day, rows)[1]
        self.assertEqual(fact_keys(facts_replay), fact_keys(sequential))
        self.assertEqual(final_replay.is_empty(), state.is_empty())

    def test_cumulative_application_never_exceeds_original_amounts(self):
        # One bill of 100 against two payments of 60: total applied to the bill
        # must stop at exactly 100, and the second payment keeps 20 open.
        state = OpenState(bills=[], payments=[])
        _, facts = advance_day(state, "A", "2026-11-01", [
            e("2026-11-01", "b", "A", "bill", 100),
            e("2026-11-01", "p1", "A", "payment", 60),
            e("2026-11-01", "p2", "A", "payment", 60),
        ])
        self.assertEqual(fact_keys(facts), [
            ("2026-11-01", "b", "p1", 60),
            ("2026-11-01", "b", "p2", 40),
        ])
        self.assertEqual(sum(f.cents for f in facts), 100)
        self.assertEqual(state.bills, [])
        self.assertEqual(state.payments[0].remaining, 20)

    def test_replay_with_empty_days_still_expires_payments(self):
        days = [
            ("2026-11-01", [e("2026-11-01", "p1", "A", "payment", 100)]),
            ("2026-11-02", []),
            ("2026-11-03", []),
            ("2026-11-04", [e("2026-11-04", "b1", "A", "bill", 100)]),
        ]
        facts, state = replay_account("A", days)
        self.assertEqual(facts, [])
        self.assertEqual(state.payments, [])
        self.assertEqual(state.bills[0].remaining, 100)


if __name__ == "__main__":
    unittest.main(verbosity=2)

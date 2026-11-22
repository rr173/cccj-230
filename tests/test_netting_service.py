"""Integration tests for the correction-safe netting service.

The closure service is used for real (fixtures build genuine CLOSED
revisions and content-addressed blobs); the netting database is an
independent SQLite file.  Every scenario is cross-checked against a
from-scratch replay over the selected snapshots.
"""

import shutil
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from netting import (  # noqa: E402
    BindingInvalid,
    InvalidDetailCsv,
    NettingService,
    RevisionNotAvailable,
    StaleRevision,
)
from netting_helpers import (  # noqa: E402
    CONTRACT_ID,
    CONTRACT_VERSION,
    csv_bytes,
    header_only_csv,
    make_fixture,
)


def fact_index(facts):
    """Identity -> cents, ignoring presentation order."""
    return {
        (f["account_id"], f["match_day"], f["bill_event"],
         f["payment_event"]): f["cents"]
        for f in facts
    }


class NettingServiceTests(unittest.TestCase):
    def setUp(self):
        self.fx = make_fixture()
        self.tmp = Path(tempfile.mkdtemp(prefix="netting_it_"))
        self.netting_db = self.tmp / "netting.db"
        self.net = NettingService(self.netting_db,
                                  closure=self.fx.raw_store_handle())

    def tearDown(self):
        self.net.close()
        self.fx.cleanup()
        shutil.rmtree(self.tmp, ignore_errors=True)

    def apply(self, day, revision=1):
        return self.net.apply_revision(
            day, revision, CONTRACT_ID, CONTRACT_VERSION)

    # ------------------------------------------------------------- scenario
    def test_out_of_order_apply_matches_from_scratch_replay(self):
        # Account A: D1 bill 1000, D2 payment 300 (partial), D3 payment 700.
        self.fx.publish_day("2026-11-01",
                            csv_bytes([("b1", "A", "bill", 1000)]), 1)
        self.fx.publish_day("2026-11-02",
                            csv_bytes([("p1", "A", "payment", 300)]), 1)
        self.fx.publish_day("2026-11-03",
                            csv_bytes([("p2", "A", "payment", 700)]), 1)
        # Import the later days first, then backfill the earlier day.
        self.apply("2026-11-03")
        self.apply("2026-11-02")
        self.assertEqual(self.net.read_facts(), [])
        self.apply("2026-11-01")

        facts = self.net.read_facts()
        self.assertEqual(fact_index(facts), {
            ("A", "2026-11-02", "2026-11-01:b1", "2026-11-02:p1"): 300,
            ("A", "2026-11-03", "2026-11-01:b1", "2026-11-03:p2"): 700,
        })
        self.assertEqual(facts, self.net.full_replay())

    def test_correction_redirects_payment_and_drops_deleted_bill_facts(self):
        # Revision 1 across seven days, one account.
        rev1 = {
            "2026-11-01": [("b1", "A", "bill", 1000),
                           ("p1", "A", "payment", 600)],
            "2026-11-02": [("p2", "A", "payment", 400)],
            "2026-11-03": [("b3", "A", "bill", 500)],
            # p4 arrives D4 with b3 still open; it partially covers b3, then
            # its window (D4..D6) ends before b6 appears on D7.
            "2026-11-04": [("p4", "A", "payment", 200)],
            "2026-11-05": [],
            "2026-11-06": [],
            "2026-11-07": [("b6", "A", "bill", 300)],
        }
        for day, rows in rev1.items():
            self.fx.publish_day(day, csv_bytes(rows) if rows
                                else header_only_csv(), 1)
            self.apply(day)

        before = fact_index(self.net.read_facts())
        self.assertEqual(before, {
            ("A", "2026-11-01", "2026-11-01:b1", "2026-11-01:p1"): 600,
            ("A", "2026-11-02", "2026-11-01:b1", "2026-11-02:p2"): 400,
            # p4 on D4 is still within its window and b3 is open, so it
            # partially covers b3 before its window ends.
            ("A", "2026-11-04", "2026-11-03:b3", "2026-11-04:p4"): 200,
        })
        # After p4's window (D4..D6) ends, b3 is 300 short, b6 fully open.
        checkpoint_before = self.net.open_state_checkpoint("A", "2026-11-07")
        self.assertEqual(
            checkpoint_before["bills"],
            [{"day": "2026-11-03", "entry_id": "b3", "remaining": 300},
             {"day": "2026-11-07", "entry_id": "b6", "remaining": 300}],
        )
        self.assertEqual(checkpoint_before["payments"], [])

        # Revision 2 of D1 deletes bill b1 but keeps the p1 payment.  That
        # payment, previously consumed by b1, must be redirected onto b3.
        self.fx.publish_day(
            "2026-11-01",
            csv_bytes([("p1", "A", "payment", 600)]), 2)
        result = self.apply("2026-11-01", 2)
        self.assertEqual(result["outcome"], "applied")
        self.assertEqual(result["affected_accounts"], ["A"])

        after = fact_index(self.net.read_facts())
        # The two old b1 pairings and the old p4 pairing all disappear.  p1
        # now fully settles b3 on D3 (its first usable day), so p2 and p4
        # expire unused; b6 stays fully open.
        self.assertNotIn(
            ("A", "2026-11-01", "2026-11-01:b1", "2026-11-01:p1"), after)
        self.assertNotIn(
            ("A", "2026-11-02", "2026-11-01:b1", "2026-11-02:p2"), after)
        self.assertNotIn(
            ("A", "2026-11-04", "2026-11-03:b3", "2026-11-04:p4"), after)
        self.assertEqual(after, {
            ("A", "2026-11-03", "2026-11-03:b3", "2026-11-01:p1"): 500,
        })
        checkpoint = self.net.open_state_checkpoint("A", "2026-11-07")
        self.assertEqual(
            checkpoint["bills"],
            [{"day": "2026-11-07", "entry_id": "b6", "remaining": 300}],
        )
        self.assertEqual(checkpoint["payments"], [])
        # Maintained incremental table equals a full from-scratch replay.
        self.assertEqual(self.net.read_facts(), self.net.full_replay())

    def test_old_prefix_is_not_replayed_and_bypass_account_is_untouched(self):
        # Long stable prefix for account A, plus an unrelated account B that
        # has rows on every day but never changes on the corrected day's
        # revision-2 snapshot.
        prefix_days = [f"2026-10-{d:02d}" for d in range(1, 16)]
        for i, day in enumerate(prefix_days):
            rows = [(f"b{i}", "A", "bill", 100),
                    (f"p{i}", "A", "payment", 100),
                    ("bx", "B", "bill", 10),
                    ("px", "B", "payment", 10)]
            self.fx.publish_day(day, csv_bytes(rows), 1)
            self.apply(day)

        corrected = prefix_days[1]
        b_ids_before = self._fact_rowids("B")
        b_facts_before = self.net.read_facts(account_id="B")
        a_pre_rowids_before = [
            r["id"] for r in self.net.store.read_conn.execute(
                "SELECT id FROM facts WHERE account_id='A' AND match_day<? "
                "ORDER BY id", (corrected,)).fetchall()
        ]

        # Revision 2 of D2 changes only A's rows.  B keeps identical rows on
        # every day, so it must not appear among the affected accounts.
        self.fx.publish_day(
            corrected,
            csv_bytes([("b1", "A", "bill", 250),  # was 100
                       ("bx", "B", "bill", 10),
                       ("px", "B", "payment", 10)]),
            2,
        )
        result = self.apply(corrected, 2)
        self.assertEqual(result["affected_accounts"], ["A"])

        # Bypass account B: same rowids and same contents, nothing rewritten.
        self.assertEqual(self._fact_rowids("B"), b_ids_before)
        self.assertEqual(self.net.read_facts(account_id="B"), b_facts_before)

        # Account A's facts strictly before D2 kept their rowids: the old
        # prefix pairing was never replayed.
        a_pre_rowids_after = [
            r["id"] for r in self.net.store.read_conn.execute(
                "SELECT id FROM facts WHERE account_id='A' AND match_day<? "
                "ORDER BY id", (corrected,)).fetchall()
        ]
        self.assertEqual(a_pre_rowids_after, a_pre_rowids_before)
        # Everything still matches a full from-scratch replay.
        self.assertEqual(self.net.read_facts(), self.net.full_replay())

    def _fact_rowids(self, account):
        return [r["id"] for r in self.net.store.read_conn.execute(
            "SELECT id FROM facts WHERE account_id=? ORDER BY id",
            (account,)).fetchall()]

    def test_header_only_correction_deletes_whole_day_and_its_pairings(self):
        # A header-only revision means "no records", not "missing file": every
        # row of the day disappears and pairings that referenced them go too.
        self.fx.publish_day("2026-11-01", csv_bytes([
            ("b1", "A", "bill", 100), ("p1", "A", "payment", 100),
        ]), 1)
        self.fx.publish_day("2026-11-02",
                            csv_bytes([("p2", "A", "payment", 100)]), 1)
        self.apply("2026-11-01")
        self.apply("2026-11-02")
        self.assertEqual(len(self.net.read_facts()), 1)

        self.fx.publish_day("2026-11-01", header_only_csv(), 2)
        result = self.apply("2026-11-01", 2)
        self.assertEqual(result["affected_accounts"], ["A"])
        # Both day-1 entries are gone; the old pairing disappears and p2
        # simply expires unused (no bill exists in its window).
        self.assertEqual(self.net.store.day_entries("2026-11-01"), [])
        self.assertEqual(self.net.read_facts(), [])
        self.assertEqual(self.net.read_facts(), self.net.full_replay())

    def test_account_move_recomputes_both_old_and_new_account(self):
        self.fx.publish_day("2026-11-01", csv_bytes([
            ("b1", "OLDA", "bill", 500),
            ("p1", "OLDA", "payment", 500),
        ]), 1)
        self.fx.publish_day("2026-11-02", csv_bytes([
            ("b2", "NEWA", "bill", 500),
        ]), 1)
        self.apply("2026-11-01")
        self.apply("2026-11-02")
        self.assertEqual(len(self.net.read_facts(account_id="OLDA")), 1)
        self.assertEqual(self.net.read_facts(account_id="NEWA"), [])

        # Correction moves the payment from OLDA to NEWA.
        self.fx.publish_day("2026-11-01", csv_bytes([
            ("b1", "OLDA", "bill", 500),
            ("p1", "NEWA", "payment", 500),
        ]), 2)
        result = self.apply("2026-11-01", 2)
        self.assertEqual(sorted(result["affected_accounts"]), ["NEWA", "OLDA"])
        self.assertEqual(self.net.read_facts(account_id="OLDA"), [])
        new_facts = self.net.read_facts(account_id="NEWA")
        self.assertEqual(len(new_facts), 1)
        self.assertEqual(new_facts[0]["payment_event"], "2026-11-01:p1")
        self.assertEqual(new_facts[0]["bill_event"], "2026-11-02:b2")
        self.assertEqual(self.net.read_facts(), self.net.full_replay())

    def test_same_revision_retry_is_idempotent_and_older_revision_rejected(self):
        self.fx.publish_day("2026-11-01",
                            csv_bytes([("b1", "A", "bill", 100)]), 1)
        first = self.apply("2026-11-01")
        second = self.apply("2026-11-01")
        self.assertEqual(first["outcome"], "applied")
        self.assertEqual(second["outcome"], "idempotent")
        self.assertEqual(second["facts_inserted"], 0)
        self.assertEqual(len(self.net.read_facts()), 0)

        # A newer revision supersedes; the old one can never come back.
        self.fx.publish_day("2026-11-01",
                            csv_bytes([("b1", "A", "bill", 200)]), 2)
        self.apply("2026-11-01", 2)
        with self.assertRaises(StaleRevision):
            self.apply("2026-11-01", 1)
        self.assertEqual(
            self.net.selected_revisions()[0]["revision"], 2)

    def test_bad_revisions_do_not_change_published_results(self):
        # Baseline published day.
        self.fx.publish_day("2026-11-01",
                            csv_bytes([("b1", "A", "bill", 100),
                                       ("p1", "A", "payment", 100)]), 1)
        self.apply("2026-11-01")
        good_facts = self.net.read_facts()
        good_revisions = self.net.selected_revisions()

        # 1) unknown revision number
        with self.assertRaises(RevisionNotAvailable):
            self.apply("2026-11-01", 99)
        # 2) revision of a day that never closed
        with self.assertRaises(RevisionNotAvailable):
            self.apply("2026-12-25", 1)
        # 3) malformed CSV in a real closed revision
        self.fx.publish_day("2026-11-02",
                            b"entry_id,account_id,kind,cents\nbad,A,bill,\n",
                            1)
        with self.assertRaises(InvalidDetailCsv):
            self.apply("2026-11-02")
        # 4) gzip-compressed detail in a real closed revision is refused.
        import gzip
        import io
        buf = io.BytesIO()
        with gzip.GzipFile(fileobj=buf, mode="wb", mtime=0) as gz:
            gz.write(csv_bytes([("b9", "A", "bill", 1)]))
        gz_fixture = self._publish_compressed_day("2026-11-03", buf.getvalue())
        try:
            gz_net = NettingService(
                self.tmp / "gz_netting.db",
                closure=gz_fixture.raw_store_handle())
            try:
                with self.assertRaises(BindingInvalid):
                    gz_net.apply_revision(
                        "2026-11-03", 1,
                        gz_fixture.GZIP_CONTRACT_ID, "1")
            finally:
                gz_net.close()
        finally:
            gz_fixture.cleanup()
        # 5) malformed business date
        from netting import InvalidBusinessDate
        with self.assertRaises(InvalidBusinessDate):
            self.apply("2026-11-1", 1)

        # Published facts and selected revisions are untouched.
        self.assertEqual(self.net.read_facts(), good_facts)
        self.assertEqual(self.net.selected_revisions(), good_revisions)
        # Rejections are audited and the good retry still works afterwards.
        outcomes = {r["outcome"] for r in self.net.application_log()}
        self.assertIn("rejected", outcomes)
        self.assertEqual(self.apply("2026-11-01")["outcome"], "idempotent")

    def _publish_compressed_day(self, day, gz_payload):
        from netting_helpers import make_gzip_fixture

        gz_fixture = make_gzip_fixture()
        gz_fixture.publish_gzip_day(day, gz_payload)
        return gz_fixture

    def test_closed_partial_revision_is_rejected(self):
        # A separate closure contract that publishes with missing required
        # members produces CLOSED_PARTIAL; the netting source contract only
        # accepts fully CLOSED bindings.
        from netting_helpers import ClosureFixture
        from datetime import datetime, timedelta, timezone

        partial = ClosureFixture()
        try:
            partial_contract = partial.service.compile_contract({
                "contract_id": "netting-partial-source",
                "version": "1",
                "batch_key": ["business_date"],
                "business_date_field": "business_date",
                "max_file_size": 16 * 1024 * 1024,
                "deadline": {"type": "relative", "offset_days": 1,
                             "hour": 12, "minute": 0},
                "late_window_minutes": 60,
                "on_missing_required": "publish_with_missing",
                "supported_compression": [],
                "members": {
                    "manifest.json": {"required": True, "role": "manifest"},
                    "detail.csv": {"required": True, "role": "data"},
                },
            })
            import json as _json
            day = "2026-11-10"
            day_dir = partial.inbound / day
            day_dir.mkdir(parents=True, exist_ok=True)
            manifest = {"members": [
                {"name": "manifest.json", "path": "manifest.json"},
                {"name": "detail.csv", "path": "detail.csv"},
            ]}
            (day_dir / "manifest.json").write_bytes(
                _json.dumps(manifest).encode())
            partial.service.receive_file(
                partial_contract, {"business_date": day}, "manifest.json",
                day_dir / "manifest.json")
            # detail.csv never arrives; evaluate past the deadline -> partial.
            at = datetime.fromisoformat(day).replace(
                tzinfo=timezone.utc) + timedelta(days=2)
            decision = partial.service.evaluate_closure(
                partial_contract, {"business_date": day}, at=at)
            self.assertEqual(decision["state"], "CLOSED_PARTIAL")

            partial_net = NettingService(
                self.tmp / "partial_netting.db",
                closure=partial.raw_store_handle())
            try:
                with self.assertRaises(RevisionNotAvailable):
                    partial_net.apply_revision(
                        day, decision["revision"],
                        "netting-partial-source", "1")
            finally:
                partial_net.close()
        finally:
            partial.cleanup()

    def test_reopen_database_preserves_and_continues_state(self):
        self.fx.publish_day("2026-11-01",
                            csv_bytes([("b1", "A", "bill", 100)]), 1)
        self.apply("2026-11-01")
        self.net.close()

        reopened = NettingService(self.netting_db,
                                  closure=self.fx.raw_store_handle())
        self.assertEqual(reopened.selected_revisions()[0]["revision"], 1)
        self.fx.publish_day("2026-11-02",
                            csv_bytes([("p1", "A", "payment", 100)]), 1)
        reopened.apply_revision("2026-11-02", 1,
                                CONTRACT_ID, CONTRACT_VERSION)
        facts = reopened.read_facts()
        self.assertEqual(len(facts), 1)
        self.assertEqual(facts[0]["cents"], 100)
        self.assertEqual(facts, reopened.full_replay())
        reopened.close()
        # Reopen the service object for tearDown's close() safety.
        self.net = NettingService(self.netting_db,
                                  closure=self.fx.raw_store_handle())

    def test_read_filters_by_account_and_day(self):
        self.fx.publish_day("2026-11-01", csv_bytes([
            ("b1", "A", "bill", 100), ("p1", "A", "payment", 100),
            ("b2", "B", "bill", 50), ("p2", "B", "payment", 50),
        ]), 1)
        self.apply("2026-11-01")
        self.assertEqual({f["account_id"] for f in self.net.read_facts()},
                         {"A", "B"})
        self.assertEqual(
            {f["account_id"] for f in self.net.read_facts(account_id="B")},
            {"B"})
        self.assertEqual(
            self.net.read_facts(business_day="2026-11-01"),
            self.net.read_facts())
        self.assertEqual(
            self.net.read_facts(account_id="A",
                                business_day="2026-11-01")[0]["cents"], 100)


if __name__ == "__main__":
    unittest.main(verbosity=2)

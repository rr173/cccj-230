"""Integration tests for local receipt reconciliation.

Real confirmed posting batches are generated through the genuine netting +
posting services (runtime data in temporary directories); the reconciliation
service opens that posting library read-only and persists receipts in a
separate SQLite file.  Coverage maps one-to-one to the finance requirements:

* receipt import discipline: only confirmed flows are referenceable, whole
  group rejection on any illegal/unknown line, first revision need not be 1,
  only higher revisions replace, same-revision resends are idempotent
  regardless of line order, lower/different-content revisions are rejected,
  empty arrays withdraw the group;
* reconciliation by flow id: same-/opposite-direction amounts never net,
  exact same-direction match with no opposite amount only, reversal closure
  blocked by an unmatched explicit original forward even across batches;
* corrections of one group never mix old/new contributions and are reflected
  when re-querying any affected batch; flows without receipts never succeed;
* restart persistence; read-only posting library; non-existent batches fail.
"""

import json
import shutil
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from netting_helpers import (  # noqa: E402
    CONTRACT_ID,
    CONTRACT_VERSION,
    csv_bytes,
    make_fixture,
)
from netting import NettingService  # noqa: E402
from posting import PostingService  # noqa: E402
from reconciliation import (  # noqa: E402
    BatchNotFound,
    MalformedReceipt,
    ReconciliationService,
    StaleRevision,
)


class ReconTestBase(unittest.TestCase):
    def setUp(self):
        self.fx = make_fixture()
        self.tmp = Path(tempfile.mkdtemp(prefix="recon_it_"))
        self.posting_db = self.tmp / "netting.db"
        self.receipt_db = self.tmp / "receipts.db"
        self.net = NettingService(
            self.posting_db, closure=self.fx.raw_store_handle())
        self.post = PostingService(self.posting_db)
        self.svc = ReconciliationService(
            str(self.posting_db), str(self.receipt_db))

    def tearDown(self):
        self.svc.close()
        self.post.close()
        self.net.close()
        self.fx.cleanup()
        shutil.rmtree(self.tmp, ignore_errors=True)

    def publish_apply(self, day, rows, revision=1):
        self.fx.publish_day(day, csv_bytes(rows), revision)
        self.net.apply_revision(day, revision, CONTRACT_ID,
                                CONTRACT_VERSION)

    def confirm(self, posting_day):
        return self.post.confirm(
            self.post.preview(posting_day)["preview_id"])

    def receipt(self, receipt_id, revision, lines):
        return {"receipt_id": receipt_id, "revision": revision,
                "lines": lines}

    def line(self, flow_id, cents, line_id="l1"):
        return {"line_id": line_id, "flow_id": str(flow_id),
                "signed_cents": cents}

    def build_two_batches(self):
        """Batch 1: forwards A60, C120.  Batch 2: C-120 reversal, C200."""
        self.publish_apply("2026-11-01", [
            ("bA", "A", "bill", 60), ("pA", "A", "payment", 60),
            ("bC", "C", "bill", 120), ("pC", "C", "payment", 120),
        ])
        batch1 = self.confirm("2026-11-02")
        self.publish_apply("2026-11-01", [
            ("bA", "A", "bill", 60), ("pA", "A", "payment", 60),
            ("bC", "C", "bill", 200), ("pC", "C", "payment", 200),
        ], revision=2)
        self.publish_apply("2026-11-03", [
            ("bD", "D", "bill", 30), ("pD", "D", "payment", 30),
        ])
        batch2 = self.confirm("2026-11-04")
        flows = {}
        for batch in (batch1, batch2):
            for e in self.post.read_batch(batch["batch_no"])["entries"]:
                flows[(e["line_type"], e["account_id"], e["cents"])] = e
        return {
            "batch1": batch1["batch_no"],
            "batch2": batch2["batch_no"],
            "a60": flows[("forward", "A", 60)]["flow_id"],
            "c120": flows[("forward", "C", 120)]["flow_id"],
            "crev": flows[("reversal", "C", 120)]["flow_id"],
            "c200": flows[("forward", "C", 200)]["flow_id"],
            "d30": flows[("forward", "D", 30)]["flow_id"],
        }

    def flow_status(self, status, flow_id):
        return next(f for f in status["flows"]
                    if f["flow_id"] == flow_id)


class ImportDisciplineTests(ReconTestBase):
    def setUp(self):
        super().setUp()
        self.ids = self.build_two_batches()

    def test_unknown_numeric_flow_rejects_whole_group(self):
        before = self.svc.list_groups()
        with self.assertRaises(MalformedReceipt):
            self.svc.import_receipt(self.receipt("G1", 1, [
                self.line(self.ids["c120"], 10),
                self.line(999999, 20, line_id="ghost"),
            ]))
        self.assertEqual(self.svc.list_groups(), before)
        log = self.svc.import_log()
        self.assertEqual(log[-1]["outcome"], "rejected")

    def test_non_numeric_flow_id_is_unknown_flow(self):
        with self.assertRaises(MalformedReceipt):
            self.svc.import_receipt(
                self.receipt("G1", 1, [self.line("ABC9", 10)]))

    def test_illegal_line_rejects_whole_group(self):
        with self.assertRaises(MalformedReceipt):
            self.svc.import_receipt({
                "receipt_id": "G1", "revision": 1,
                "lines": [{"line_id": "x", "flow_id": str(self.ids["c120"]),
                           "signed_cents": 0}],
            })
        self.assertIsNone(self.svc.get_group("G1"))

    def test_first_revision_need_not_be_one(self):
        result = self.svc.import_receipt(
            self.receipt("G7", 7, [self.line(self.ids["a60"], 60)]))
        self.assertEqual(result["outcome"], "accepted")
        self.assertEqual(self.svc.get_group("G7")["current_revision"], 7)

    def test_higher_revision_replaces_and_lower_is_rejected(self):
        self.svc.import_receipt(
            self.receipt("G1", 5, [self.line(self.ids["a60"], 60)]))
        replaced = self.svc.import_receipt(
            self.receipt("G1", 6, [self.line(self.ids["d30"], 30)]))
        self.assertEqual(replaced["outcome"], "replaced")
        group = self.svc.get_group("G1")
        self.assertEqual(group["current_revision"], 6)
        self.assertEqual(group["lines"][0]["flow_id"], self.ids["d30"])
        with self.assertRaises(StaleRevision):
            self.svc.import_receipt(
                self.receipt("G1", 5, [self.line(self.ids["a60"], 60)]))
        # The lower revision left the current version untouched.
        self.assertEqual(
            self.svc.get_group("G1")["current_revision"], 6)

    def test_same_revision_same_content_idempotent_any_order(self):
        payload = self.receipt("G1", 2, [
            self.line(self.ids["a60"], 60, line_id="a"),
            self.line(self.ids["d30"], 30, line_id="b"),
        ])
        self.svc.import_receipt(payload)
        resend = self.receipt("G1", 2, list(reversed(payload["lines"])))
        result = self.svc.import_receipt(resend)
        self.assertEqual(result["outcome"], "idempotent")
        # No duplicate contribution: query sees exactly the two amounts once.
        status = self.svc.batch_status(1)
        a60 = self.flow_status(status, self.ids["a60"])
        self.assertEqual(a60["same_direction_cents"], 60)
        self.assertEqual(len(a60["contributions"]), 1)

    def test_same_revision_different_content_rejected(self):
        self.svc.import_receipt(
            self.receipt("G1", 2, [self.line(self.ids["a60"], 60)]))
        with self.assertRaises(StaleRevision):
            self.svc.import_receipt(
                self.receipt("G1", 2, [self.line(self.ids["a60"], 61)]))
        with self.assertRaises(StaleRevision):
            self.svc.import_receipt(
                self.receipt("G1", 2, [self.line(self.ids["d30"], 60)]))
        group = self.svc.get_group("G1")
        self.assertEqual(group["lines"][0]["signed_cents"], 60)

    def test_empty_array_withdraws_group_at_new_revision(self):
        self.svc.import_receipt(
            self.receipt("G1", 1, [self.line(self.ids["a60"], 60)]))
        result = self.svc.import_receipt(
            self.receipt("G1", 2, []))
        self.assertTrue(result["withdrawn"])
        group = self.svc.get_group("G1")
        self.assertEqual(group["current_revision"], 2)
        self.assertEqual(group["lines"], [])
        # Withdrawn group contributes nothing.
        status = self.svc.batch_status(1)
        a60 = self.flow_status(status, self.ids["a60"])
        self.assertEqual(a60["contributions"], [])
        self.assertFalse(a60["amount_matched"])

    def test_multiple_groups_and_lines_can_share_a_flow(self):
        self.svc.import_receipt(
            self.receipt("G1", 1, [self.line(self.ids["c120"], 45)]))
        self.svc.import_receipt(
            self.receipt("G2", 1, [
                self.line(self.ids["c120"], 70, line_id="x"),
                self.line(self.ids["c120"], 5, line_id="y"),
            ]))
        status = self.svc.batch_status(1)
        c120 = self.flow_status(status, self.ids["c120"])
        self.assertEqual(c120["same_direction_cents"], 120)
        self.assertEqual(len(c120["contributions"]), 3)
        self.assertTrue(c120["amount_matched"])

    def test_import_file_roundtrip_and_bad_json(self):
        path = self.tmp / "r.json"
        path.write_text(json.dumps(
            self.receipt("G9", 1, [self.line(self.ids["a60"], 60)])),
            encoding="utf-8")
        result = self.svc.import_receipt_file(str(path))
        self.assertEqual(result["outcome"], "accepted")
        bad = self.tmp / "bad.json"
        bad.write_text("{not json", encoding="utf-8")
        with self.assertRaises(MalformedReceipt):
            self.svc.import_receipt_file(str(bad))
        self.assertEqual(self.svc.import_log()[-1]["outcome"], "rejected")


class ReconciliationRuleTests(ReconTestBase):
    def setUp(self):
        super().setUp()
        self.ids = self.build_two_batches()

    def test_no_receipts_means_not_closeable(self):
        status = self.svc.batch_status(1)
        self.assertFalse(status["closeable"])
        self.assertTrue(all(not f["amount_matched"] for f in status["flows"]))
        for flow in status["flows"]:
            self.assertTrue(
                any(b["reason"] == "amount_mismatch"
                    for b in flow["blockers"]))

    def test_exact_match_across_two_groups_closes_batch(self):
        self.svc.import_receipt(
            self.receipt("G45", 1, [self.line(self.ids["c120"], 45)]))
        self.svc.import_receipt(
            self.receipt("G75", 1, [self.line(self.ids["c120"], 75)]))
        self.svc.import_receipt(
            self.receipt("GA", 1, [self.line(self.ids["a60"], 60)]))
        status = self.svc.batch_status(1)
        self.assertTrue(status["closeable"])
        c120 = self.flow_status(status, self.ids["c120"])
        self.assertEqual(
            sorted((c["receipt_id"], c["revision"], c["line_id"])
                   for c in c120["contributions"]),
            [("G45", 1, "l1"), ("G75", 1, "l1")])

    def test_under_and_over_booking_detail(self):
        self.svc.import_receipt(
            self.receipt("GA", 1, [self.line(self.ids["a60"], 50)]))
        status = self.svc.batch_status(1)
        a60 = self.flow_status(status, self.ids["a60"])
        blocker = a60["blockers"][0]
        self.assertEqual(blocker["problem"], "underbooked")
        self.assertEqual(blocker["shortfall_cents"], 10)

        self.svc.import_receipt(
            self.receipt("GA", 2, [self.line(self.ids["a60"], 70)]))
        status = self.svc.batch_status(1)
        a60 = self.flow_status(status, self.ids["a60"])
        blocker = a60["blockers"][0]
        self.assertEqual(blocker["problem"], "overbooked")
        self.assertEqual(blocker["excess_cents"], 10)

    def test_net_equal_but_opposite_direction_does_not_match(self):
        # +130 and -10 against a +120 expectation: net is 120 but the flow
        # must stay unmatched.
        self.svc.import_receipt(self.receipt("G", 1, [
            self.line(self.ids["c120"], 130, line_id="p"),
            self.line(self.ids["c120"], -10, line_id="n"),
        ]))
        status = self.svc.batch_status(1)
        c120 = self.flow_status(status, self.ids["c120"])
        self.assertEqual(c120["same_direction_cents"], 130)
        self.assertEqual(c120["opposite_direction_cents"], 10)
        self.assertFalse(c120["amount_matched"])
        self.assertFalse(status["closeable"])

    def test_negative_expectation_same_and_opposite(self):
        # Reversal expects -120. A -120 receipt matches; a stray +20 must block.
        self.svc.import_receipt(self.receipt("GR", 1, [
            self.line(self.ids["crev"], -120)]))
        status = self.svc.batch_status(2)
        crev = self.flow_status(status, self.ids["crev"])
        self.assertEqual(crev["same_direction_cents"], 120)
        self.assertTrue(crev["amount_matched"])

        self.svc.import_receipt(self.receipt("GR", 2, [
            self.line(self.ids["crev"], -120, line_id="m"),
            self.line(self.ids["crev"], 20, line_id="stray"),
        ]))
        status = self.svc.batch_status(2)
        crev = self.flow_status(status, self.ids["crev"])
        self.assertEqual(crev["opposite_direction_cents"], 20)
        self.assertFalse(crev["amount_matched"])

    def test_reversal_blocked_by_unmatched_original_even_if_fully_booked(self):
        # Fully book the reversal and the other batch-2 flows, but leave the
        # original C120 forward underbooked.
        self.svc.import_receipt(
            self.receipt("GR", 1, [self.line(self.ids["crev"], -120)]))
        self.svc.import_receipt(
            self.receipt("G200", 1, [self.line(self.ids["c200"], 200)]))
        self.svc.import_receipt(
            self.receipt("GD", 1, [self.line(self.ids["d30"], 30)]))
        self.svc.import_receipt(
            self.receipt("GA", 1, [self.line(self.ids["a60"], 60)]))
        self.svc.import_receipt(
            self.receipt("GC", 1, [self.line(self.ids["c120"], 45)]))

        status2 = self.svc.batch_status(2)
        crev = self.flow_status(status2, self.ids["crev"])
        self.assertTrue(crev["amount_matched"])
        self.assertFalse(status2["closeable"])
        blocker = next(b for b in crev["blockers"]
                       if b["reason"] == "reversed_original_unmatched")
        self.assertEqual(blocker["original_flow_id"], self.ids["c120"])
        # The cross-batch original flow is reported explicitly.
        origin = status2["referenced_origins"][0]
        self.assertEqual(origin["flow_id"], self.ids["c120"])
        self.assertFalse(origin["amount_matched"])

        # Once the original forward is fully booked, batch 2 closes.
        self.svc.import_receipt(
            self.receipt("GC", 2, [self.line(self.ids["c120"], 120)]))
        status2 = self.svc.batch_status(2)
        self.assertTrue(status2["closeable"])

    def test_check_uses_named_original_even_after_new_forward_exists(self):
        # The correction already created a new 200 forward for the same
        # pairing. Booking that new flow fully must not satisfy the reversal;
        # only the explicitly named original flow id matters.
        self.svc.import_receipt(
            self.receipt("GR", 1, [self.line(self.ids["crev"], -120)]))
        self.svc.import_receipt(
            self.receipt("G200", 1, [self.line(self.ids["c200"], 200)]))
        self.svc.import_receipt(
            self.receipt("GD", 1, [self.line(self.ids["d30"], 30)]))
        status2 = self.svc.batch_status(2)
        crev = self.flow_status(status2, self.ids["crev"])
        self.assertTrue(crev["amount_matched"])
        self.assertFalse(status2["closeable"])
        blocker = next(b for b in crev["blockers"]
                       if b["reason"] == "reversed_original_unmatched")
        self.assertEqual(blocker["original_flow_id"], self.ids["c120"])

    def test_correction_is_reflected_on_both_affected_batches(self):
        self.svc.import_receipt(
            self.receipt("G45", 1, [self.line(self.ids["c120"], 45)]))
        self.svc.import_receipt(
            self.receipt("G75", 1, [self.line(self.ids["c120"], 75)]))
        self.svc.import_receipt(
            self.receipt("GA", 1, [self.line(self.ids["a60"], 60)]))
        self.assertTrue(self.svc.batch_status(1)["closeable"])

        # Move the 75-cent voucher onto the new 200 flow.
        self.svc.import_receipt(
            self.receipt("G75", 2, [self.line(self.ids["c200"], 75)]))
        status1 = self.svc.batch_status(1)
        c120 = self.flow_status(status1, self.ids["c120"])
        self.assertEqual(c120["same_direction_cents"], 45)
        # Old version contribution must not linger.
        self.assertEqual(
            [c["receipt_id"] for c in c120["contributions"]], ["G45"])
        status2 = self.svc.batch_status(2)
        c200 = self.flow_status(status2, self.ids["c200"])
        self.assertEqual(c200["same_direction_cents"], 75)
        # No mixing of revisions anywhere in the report: every contribution
        # shown is the currently effective revision of its group.
        for status in (status1, status2):
            for flow in status["flows"]:
                for c in flow["contributions"]:
                    group = self.svc.get_group(c["receipt_id"])
                    self.assertEqual(
                        c["revision"], group["current_revision"])


class PersistenceAndIsolationTests(ReconTestBase):
    def test_reopen_preserves_and_continues_state(self):
        ids = self.build_two_batches()
        self.svc.import_receipt(
            self.receipt("G", 1, [self.line(ids["a60"], 60)]))
        self.svc.close()

        again = ReconciliationService(
            str(self.posting_db), str(self.receipt_db))
        status = again.batch_status(1)
        a60 = self.flow_status(status, ids["a60"])
        self.assertTrue(a60["amount_matched"])
        result = again.import_receipt(
            self.receipt("G", 2, [self.line(ids["d30"], 30)]))
        self.assertEqual(result["outcome"], "replaced")
        again.close()

    def test_nonexistent_batch_is_an_error_not_closeable(self):
        with self.assertRaises(BatchNotFound):
            self.svc.batch_status(99)
        with self.assertRaises(BatchNotFound):
            self.svc.batch_status(0)
        with self.assertRaises(BatchNotFound):
            self.svc.batch_status("1")

    def test_posting_library_is_opened_read_only(self):
        # The reconciliation connection cannot write even if asked to.
        with self.assertRaises(Exception):
            self.svc.reader.conn.execute(
                "INSERT INTO posting_meta(key, value) VALUES('x', 'y')")
        # And nothing created extra tables inside the posting library.
        names = {r[0] for r in self.net.store.conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
        self.assertNotIn("receipt_lines", names)
        self.assertNotIn("receipt_groups", names)

    def test_missing_posting_library_is_rejected(self):
        from reconciliation import UnreadablePostingLibrary

        with self.assertRaises(UnreadablePostingLibrary):
            ReconciliationService(str(self.tmp / "nope.db"),
                                  str(self.receipt_db))

    def test_receipt_db_is_a_separate_file(self):
        ids = self.build_two_batches()
        self.svc.import_receipt(
            self.receipt("G", 1, [self.line(ids["a60"], 60)]))
        self.assertTrue(self.receipt_db.is_file())
        receipt_tables = {
            r[0] for r in
            self.svc.store.read_conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        }
        self.assertIn("receipt_groups", receipt_tables)
        self.assertNotIn("posting_batches", receipt_tables)


class ScopedReadTests(ReconTestBase):
    def test_query_only_reads_scoped_flows_indexed_lookups(self):
        ids = self.build_two_batches()
        self.svc.import_receipt(
            self.receipt("G", 1, [self.line(ids["a60"], 60)]))
        # No flow outside the batch (plus named origins) may be queried:
        # the store helper with an unrelated id set must return nothing even
        # though receipts exist for batch-1 flows.
        rows = self.svc.store.current_lines_for_flows([987654])
        self.assertEqual(rows, [])
        rows = self.svc.store.current_lines_for_flows([ids["a60"]])
        self.assertEqual(len(rows), 1)

    def test_only_current_version_rows_exist_physically(self):
        ids = self.build_two_batches()
        self.svc.import_receipt(
            self.receipt("G", 1, [self.line(ids["a60"], 60)]))
        self.svc.import_receipt(
            self.receipt("G", 2, [self.line(ids["d30"], 30)]))
        rows = self.svc.store.read_conn.execute(
            "SELECT * FROM receipt_lines WHERE receipt_id='G'").fetchall()
        self.assertEqual(len(rows), 1)
        self.assertEqual(int(rows[0]["flow_id"]), ids["d30"])

    def test_reversal_alone_blocked_when_original_has_no_receipt(self):
        ids = self.build_two_batches()
        # Book every batch-2 flow but nothing at all for the original C120.
        self.svc.import_receipt(
            self.receipt("GR", 1, [self.line(ids["crev"], -120)]))
        self.svc.import_receipt(
            self.receipt("G200", 1, [self.line(ids["c200"], 200)]))
        self.svc.import_receipt(
            self.receipt("GD", 1, [self.line(ids["d30"], 30)]))
        status = self.svc.batch_status(2)
        self.assertFalse(status["closeable"])
        crev = self.flow_status(status, ids["crev"])
        self.assertTrue(crev["amount_matched"])
        self.assertEqual(
            [b["reason"] for b in crev["blockers"]],
            ["reversed_original_unmatched"])

    def test_mixed_sign_lines_from_several_groups_aggregate_per_direction(self):
        ids = self.build_two_batches()
        self.svc.import_receipt(self.receipt("G1", 1, [
            self.line(ids["c120"], 50, line_id="a"),
            self.line(ids["c120"], -5, line_id="b"),
        ]))
        self.svc.import_receipt(self.receipt("G2", 1, [
            self.line(ids["c120"], 60, line_id="c"),
            self.line(ids["c120"], -5, line_id="d"),
        ]))
        status = self.svc.batch_status(1)
        c120 = self.flow_status(status, ids["c120"])
        self.assertEqual(c120["same_direction_cents"], 110)
        self.assertEqual(c120["opposite_direction_cents"], 10)
        self.assertFalse(c120["amount_matched"])

    def test_import_does_not_touch_other_groups(self):
        ids = self.build_two_batches()
        self.svc.import_receipt(
            self.receipt("G1", 1, [self.line(ids["a60"], 60)]))
        self.svc.import_receipt(
            self.receipt("G2", 1, [self.line(ids["d30"], 30)]))
        self.svc.import_receipt(
            self.receipt("G1", 2, [self.line(ids["a60"], 60)]))
        g2 = self.svc.get_group("G2")
        self.assertEqual(g2["current_revision"], 1)
        self.assertEqual(g2["lines"][0]["flow_id"], ids["d30"])


if __name__ == "__main__":
    unittest.main()

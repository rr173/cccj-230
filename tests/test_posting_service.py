"""Integration tests for the local netting posting handoff.

Real CLOSED revisions are built through the closure fixture (runtime test
data only), netting facts come from the genuine NettingService, and the
posting handoff runs on the same SQLite file.  Coverage follows the task
scenario list:

* 100-cent payment first covering 60 of an old bill then 40 of a new bill,
  corrected to 30/70: the equal-total correction still delivers two full
  reversals and two new forwards, each reversal naming its live flow;
* applying the correction before the first handoff registers only the two
  current pairings;
* a correction landing between preview and confirm rejects the old preview,
  the re-preview is correct, and retrying the success never double writes;
* an unrelated historical pairing is never scanned on later handoffs;
* identity/day moves, flicker suppression, posting-day ordering, reopen,
  pure row rewrites and failed/idempotent applications keep previews valid;
* concurrent confirms cannot double-post and an old preview cannot publish
  new facts.
"""

import json
import shutil
import subprocess
import sys
import tempfile
import unittest
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from netting import InvalidDetailCsv, NettingService  # noqa: E402
from netting_helpers import (  # noqa: E402
    CONTRACT_ID,
    CONTRACT_VERSION,
    csv_bytes,
    header_only_csv,
    make_fixture,
)
from posting import (  # noqa: E402
    BatchNotFound,
    InvalidPostingDay,
    NothingToPost,
    PostingService,
    PreviewStale,
    UnknownPreview,
)

ROOT = Path(__file__).resolve().parent.parent
POSTING_CLI = ROOT / "posting" / "cli.py"


class PostingTestBase(unittest.TestCase):
    def setUp(self):
        self.fx = make_fixture()
        self.tmp = Path(tempfile.mkdtemp(prefix="posting_it_"))
        self.db = self.tmp / "netting.db"
        self.net = NettingService(self.db,
                                  closure=self.fx.raw_store_handle())
        self.post = PostingService(self.db)

    def tearDown(self):
        self.post.close()
        self.net.close()
        self.fx.cleanup()
        shutil.rmtree(self.tmp, ignore_errors=True)

    def apply(self, day, revision=1):
        return self.net.apply_revision(
            day, revision, CONTRACT_ID, CONTRACT_VERSION)

    def publish_apply(self, day, rows, revision=1):
        self.fx.publish_day(day, csv_bytes(rows) if rows
                            else header_only_csv(), revision)
        return self.apply(day, revision)

    def identity_set(self, entries):
        return {
            (e["account_id"], e["match_day"], e["bill_event"],
             e["payment_event"]): e["cents"]
            for e in entries
        }


class CoreScenarioTests(PostingTestBase):
    def _build_initial_100_split(self):
        """100-cent payment: 60 onto old bill, 40 onto a later new bill."""
        self.publish_apply("2026-11-01", [
            ("b1", "A", "bill", 60),
            ("p1", "A", "payment", 100),
        ])
        self.publish_apply("2026-11-02", [
            ("b2", "A", "bill", 40),
        ])
        facts = self.net.read_facts()
        self.assertEqual(
            {(f["bill_event"], f["payment_event"]): f["cents"]
             for f in facts},
            {("2026-11-01:b1", "2026-11-01:p1"): 60,
             ("2026-11-02:b2", "2026-11-01:p1"): 40},
        )
        return facts

    def test_equal_total_correction_delivers_two_reversals_and_two_forwards(self):
        self._build_initial_100_split()

        preview = self.post.preview("2026-11-03")
        self.assertTrue(preview["bootstrap"])
        self.assertEqual(preview["entry_count"], 2)
        self.assertTrue(all(e["line_type"] == "forward"
                            for e in preview["entries"]))
        batch1 = self.post.confirm(preview["preview_id"])
        self.assertEqual(batch1["batch_no"], 1)
        self.assertEqual(batch1["posting_day"], "2026-11-03")
        flow_by_identity = {
            (e["bill_event"], e["payment_event"]): e["flow_id"]
            for e in batch1["entries"]
        }

        # Correct the old bill to 30 and the new bill to 70.  The payment
        # total stays 100, but every pairing amount changes.
        self.publish_apply("2026-11-01", [
            ("b1", "A", "bill", 30),
            ("p1", "A", "payment", 100),
        ], revision=2)
        self.publish_apply("2026-11-02", [
            ("b2", "A", "bill", 70),
        ], revision=2)
        self.assertEqual(
            {(f["bill_event"], f["payment_event"]): f["cents"]
             for f in self.net.read_facts()},
            {("2026-11-01:b1", "2026-11-01:p1"): 30,
             ("2026-11-02:b2", "2026-11-01:p1"): 70},
        )

        preview2 = self.post.preview("2026-11-04")
        entries = preview2["entries"]
        self.assertEqual(len(entries), 4)
        # Reversal of each pairing must precede its new forward line.
        for e in entries:
            self.assertIn(e["match_day"], ("2026-11-01", "2026-11-02"))
        by_identity = {}
        for e in entries:
            by_identity.setdefault(
                (e["bill_event"], e["payment_event"]), []).append(e)
        for key, lines in by_identity.items():
            self.assertEqual([l["line_type"] for l in lines],
                             ["reversal", "forward"])
            self.assertLess(lines[0]["entry_no"], lines[1]["entry_no"])
            # The reversal names the still-live forward line from batch 1,
            # never an already-reversed version.
            self.assertEqual(lines[0]["reverses_flow_id"],
                             flow_by_identity[key])
            self.assertIsNone(lines[1]["reverses_flow_id"])
        # Full amounts, no net-difference lines.
        amounts = sorted(
            (e["line_type"], e["bill_event"], e["cents"]) for e in entries)
        self.assertEqual(amounts, [
            ("forward", "2026-11-01:b1", 30),
            ("forward", "2026-11-02:b2", 70),
            ("reversal", "2026-11-01:b1", 60),
            ("reversal", "2026-11-02:b2", 40),
        ])
        # Flow lines keep the original pairing business dates even though the
        # posting day is the new one.
        self.assertTrue(all(e["match_day"] != "2026-11-04" for e in entries))

        batch2 = self.post.confirm(preview2["preview_id"])
        self.assertEqual(batch2["batch_no"], 2)
        # Batch 1 is still readable and unchanged.
        self.assertEqual(
            [e["flow_id"] for e in
             self.post.read_batch(1)["entries"]],
            [e["flow_id"] for e in batch1["entries"]],
        )
        # Reversal targets resolve to batch-1 flow ids.
        batch2_reversals = [e for e in batch2["entries"]
                            if e["line_type"] == "reversal"]
        batch1_ids = {e["flow_id"] for e in batch1["entries"]}
        self.assertTrue(all(e["reverses_flow_id"] in batch1_ids
                            for e in batch2_reversals))

    def test_correction_before_first_handoff_registers_current_pairings_only(self):
        # Apply revision 1, then revision 2, before any preview exists.
        self.publish_apply("2026-11-01", [
            ("b1", "A", "bill", 60),
            ("p1", "A", "payment", 100),
        ])
        self.publish_apply("2026-11-02", [("b2", "A", "bill", 40)])
        self.publish_apply("2026-11-01", [
            ("b1", "A", "bill", 30),
            ("p1", "A", "payment", 100),
        ], revision=2)
        self.publish_apply("2026-11-02", [("b2", "A", "bill", 70)],
                           revision=2)

        preview = self.post.preview("2026-11-05")
        # Only the two current pairings are registered: no reversal for the
        # 60/40 state because nothing was ever handed over for it.
        self.assertEqual(preview["entry_count"], 2)
        self.assertTrue(all(e["line_type"] == "forward"
                            for e in preview["entries"]))
        self.assertEqual(
            {e["bill_event"]: e["cents"] for e in preview["entries"]},
            {"2026-11-01:b1": 30, "2026-11-02:b2": 70},
        )
        batch = self.post.confirm(preview["preview_id"])
        self.assertEqual(batch["batch_no"], 1)

    def test_correction_between_preview_and_confirm_rejects_then_repreview(self):
        self._build_initial_100_split()
        preview1 = self.post.preview("2026-11-03")
        batch1 = self.post.confirm(preview1["preview_id"])

        # Build a correction and preview it.
        self.publish_apply("2026-11-01", [
            ("b1", "A", "bill", 30),
            ("p1", "A", "payment", 100),
        ], revision=2)
        self.publish_apply("2026-11-02", [("b2", "A", "bill", 70)],
                           revision=2)
        stale_preview = self.post.preview("2026-11-04")
        self.assertEqual(stale_preview["entry_count"], 4)

        # Another correction lands before confirm, then changes back: the
        # facts finally equal the preview's expectation, but an application
        # genuinely mutated them in between, so the preview must be stale.
        self.publish_apply("2026-11-01", [
            ("b1", "A", "bill", 20),
            ("p1", "A", "payment", 100),
        ], revision=3)
        self.publish_apply("2026-11-02", [("b2", "A", "bill", 80)],
                           revision=3)
        self.publish_apply("2026-11-01", [
            ("b1", "A", "bill", 30),
            ("p1", "A", "payment", 100),
        ], revision=4)
        self.publish_apply("2026-11-02", [("b2", "A", "bill", 70)],
                           revision=4)

        with self.assertRaises(PreviewStale):
            self.post.confirm(stale_preview["preview_id"])
        # The stale preview stays rejected on retry.
        with self.assertRaises(PreviewStale):
            self.post.confirm(stale_preview["preview_id"])
        # No batch was published for it.
        self.assertEqual(self.post.list_batches()[-1]["batch_no"], 1)

        fresh = self.post.preview("2026-11-04")
        # Facts are back at 30/70 so the re-preview content matches the stale
        # preview's content, but it is a brand-new preview id.
        self.assertNotEqual(fresh["preview_id"], stale_preview["preview_id"])
        self.assertEqual(
            sorted((e["line_type"], e["bill_event"], e["cents"])
                   for e in fresh["entries"]),
            sorted((e["line_type"], e["bill_event"], e["cents"])
                   for e in stale_preview["entries"]),
        )
        batch2 = self.post.confirm(fresh["preview_id"])
        self.assertEqual(batch2["batch_no"], 2)

        # Retrying the successful confirm returns the same batch and writes
        # nothing again.
        entries_before = self.post.read_batch(2)["entries"]
        retry = self.post.confirm(fresh["preview_id"])
        self.assertEqual(retry, batch2)
        self.assertEqual(
            [e["flow_id"] for e in retry["entries"]],
            [e["flow_id"] for e in entries_before],
        )
        self.assertEqual(len(self.post.list_batches()), 2)

    def test_unrelated_history_is_not_scanned_on_later_handoffs(self):
        # A stable historical pairing on D1.
        self.publish_apply("2026-11-01", [
            ("bh", "H", "bill", 100),
            ("ph", "H", "payment", 100),
        ])
        first = self.post.preview("2026-11-02")
        first_batch = self.post.confirm(first["preview_id"])
        history_flow = first_batch["entries"][0]["flow_id"]
        seq_after_first = self.post.handoff_state()["last_change_seq"]

        # A brand-new unrelated day/account is applied later.
        self.publish_apply("2026-11-10", [
            ("bn", "N", "bill", 25),
            ("pn", "N", "payment", 25),
        ])
        preview = self.post.preview("2026-11-11")
        self.assertEqual(preview["entry_count"], 1)
        entry = preview["entries"][0]
        self.assertEqual(entry["account_id"], "N")
        self.assertEqual(entry["cents"], 25)
        self.post.confirm(preview["preview_id"])

        # The historical flow is untouched and no change row was produced for
        # the H account by the new application.
        self.assertEqual(
            self.post.read_batch(1)["entries"][0]["flow_id"], history_flow)
        # Only the two rows of the new application exist past the old
        # watermark: history was never re-read.
        new_rows = self.store_change_rows(seq_after_first)
        self.assertTrue(all(r["account_id"] == "N" for r in new_rows))

    def store_change_rows(self, after_seq):
        return self.post.store.change_rows_after(after_seq)


class DeltaSemanticsTests(PostingTestBase):
    def test_disappeared_pairing_is_fully_reversed_against_live_flow(self):
        self.publish_apply("2026-11-01", [
            ("b1", "A", "bill", 100),
            ("p1", "A", "payment", 100),
        ])
        preview = self.post.preview("2026-11-02")
        batch = self.post.confirm(preview["preview_id"])
        live_flow = batch["entries"][0]["flow_id"]

        # Header-only correction deletes the whole day and its pairing.
        self.publish_apply("2026-11-01", [], revision=2)
        next_preview = self.post.preview("2026-11-03")
        self.assertEqual(next_preview["entry_count"], 1)
        line = next_preview["entries"][0]
        self.assertEqual(line["line_type"], "reversal")
        self.assertEqual(line["cents"], 100)
        self.assertEqual(line["reverses_flow_id"], live_flow)
        self.post.confirm(next_preview["preview_id"])

        # A further preview has nothing to post.
        with self.assertRaises(NothingToPost):
            self.post.preview("2026-11-04")

    def test_identity_move_is_disappear_plus_appear_even_when_total_unchanged(self):
        self.publish_apply("2026-11-01", [
            ("b1", "A", "bill", 100),
            ("p1", "A", "payment", 100),
        ])
        batch1 = self.post.confirm(
            self.post.preview("2026-11-02")["preview_id"])
        old_flow = batch1["entries"][0]["flow_id"]

        # Rename the bill event: same 100 cents, but the event identity
        # changes, so it must be treated as old pairing gone + new pairing.
        self.publish_apply("2026-11-01", [
            ("b9", "A", "bill", 100),
            ("p1", "A", "payment", 100),
        ], revision=2)
        preview = self.post.preview("2026-11-03")
        self.assertEqual(preview["entry_count"], 2)
        reversal = next(e for e in preview["entries"]
                        if e["line_type"] == "reversal")
        forward = next(e for e in preview["entries"]
                       if e["line_type"] == "forward")
        self.assertEqual(reversal["bill_event"], "2026-11-01:b1")
        self.assertEqual(reversal["reverses_flow_id"], old_flow)
        self.assertEqual(forward["bill_event"], "2026-11-01:b9")
        self.assertEqual(forward["cents"], 100)
        self.assertLess(reversal["entry_no"], forward["entry_no"])

    def test_match_day_move_is_disappear_plus_appear(self):
        # Bill on D1, payment arriving D2: pairing is matched on D2.
        self.publish_apply("2026-11-01", [("b1", "A", "bill", 100)])
        self.publish_apply("2026-11-02", [("p1", "A", "payment", 100)])
        self.post.confirm(self.post.preview("2026-11-03")["preview_id"])

        # Move the payment to D1: the same identities now match on D1, so the
        # netting business day (part of the pairing identity) changes.
        self.publish_apply("2026-11-02", [], revision=2)
        self.publish_apply("2026-11-01", [
            ("b1", "A", "bill", 100),
            ("p1", "A", "payment", 100),
        ], revision=2)
        preview = self.post.preview("2026-11-04")
        types = sorted(e["line_type"] for e in preview["entries"])
        self.assertEqual(types, ["forward", "reversal"])
        self.assertEqual({e["match_day"] for e in preview["entries"]},
                         {"2026-11-01", "2026-11-02"})

    def test_flicker_between_handoffs_leaves_no_flow(self):
        self.publish_apply("2026-11-01", [
            ("b1", "A", "bill", 100),
            ("p1", "A", "payment", 100),
        ])
        self.post.confirm(self.post.preview("2026-11-02")["preview_id"])

        # Add then delete a pairing across two applications before the next
        # handoff, ending exactly where we started.
        self.publish_apply("2026-11-03", [
            ("b2", "A", "bill", 50),
            ("p2", "A", "payment", 50),
        ])
        self.publish_apply("2026-11-03", [], revision=2)
        with self.assertRaises(NothingToPost):
            self.post.preview("2026-11-04")

    def test_row_only_rewrite_and_failed_application_do_not_stale_preview(self):
        self.publish_apply("2026-11-01", [
            ("b1", "A", "bill", 100),
            ("p1", "A", "payment", 100),
        ])
        self.post.confirm(self.post.preview("2026-11-02")["preview_id"])

        # A new self-contained pairing gives the pending preview something to
        # publish; capture its clock basis.
        self.publish_apply("2026-11-03", [
            ("b2", "B", "bill", 70),
            ("p2", "B", "payment", 70),
        ])
        preview = self.post.preview("2026-11-04")
        basis = preview["basis"]["fact_clock"]

        # 1) Same-revision retry: idempotent, no fact rewrite, no marker.
        retry = self.apply("2026-11-03", 1)
        self.assertEqual(retry["outcome"], "idempotent")
        # 2) A failed application (malformed CSV) rolls everything back.
        self.fx.publish_day("2026-11-04",
                            b"entry_id,account_id,kind,cents\nx,A,bill,\n", 1)
        with self.assertRaises(InvalidDetailCsv):
            self.apply("2026-11-04", 1)
        # 3) A stale (older) revision is rejected without touching facts.
        with self.assertRaises(Exception):
            self.apply("2026-11-03", 0)

        self.assertEqual(
            self.post.handoff_state()["fact_clock"], basis)
        # The preview still confirms successfully.
        batch = self.post.confirm(preview["preview_id"])
        self.assertEqual(batch["batch_no"], 2)


class ProtocolTests(PostingTestBase):
    def test_posting_day_may_equal_but_not_precede_previous_batch(self):
        self.publish_apply("2026-11-01", [
            ("b1", "A", "bill", 10), ("p1", "A", "payment", 10)])
        self.post.confirm(self.post.preview("2026-11-05")["preview_id"])

        self.publish_apply("2026-11-02", [
            ("b2", "A", "bill", 20), ("p2", "A", "payment", 20)])
        # Same posting day as the previous batch is allowed.
        batch = self.post.confirm(
            self.post.preview("2026-11-05")["preview_id"])
        self.assertEqual(batch["posting_day"], "2026-11-05")

        self.publish_apply("2026-11-03", [
            ("b3", "A", "bill", 30), ("p3", "A", "payment", 30)])
        with self.assertRaises(InvalidPostingDay):
            self.post.preview("2026-11-04")
        with self.assertRaises(InvalidPostingDay):
            self.post.preview("2026-11-01")

    def test_empty_preview_is_never_published_and_unknown_ids_raise(self):
        self.publish_apply("2026-11-01", [
            ("b1", "A", "bill", 10), ("p1", "A", "payment", 10)])
        self.post.confirm(self.post.preview("2026-11-02")["preview_id"])
        with self.assertRaises(NothingToPost):
            self.post.preview("2026-11-03")
        with self.assertRaises(UnknownPreview):
            self.post.confirm("does-not-exist")
        with self.assertRaises(BatchNotFound):
            self.post.read_batch(99)
        # No empty batch exists.
        self.assertEqual(len(self.post.list_batches()), 1)

    def test_second_preview_confirm_makes_first_preview_stale(self):
        self.publish_apply("2026-11-01", [
            ("b1", "A", "bill", 10), ("p1", "A", "payment", 10)])
        first = self.post.preview("2026-11-02")
        # Another preview on the same basis (both pending, neither confirmed).
        second = self.post.preview("2026-11-02")
        self.post.confirm(second["preview_id"])
        with self.assertRaises(PreviewStale):
            self.post.confirm(first["preview_id"])

    def test_reopen_preserves_batches_and_pending_preview_rules(self):
        self.publish_apply("2026-11-01", [
            ("b1", "A", "bill", 10), ("p1", "A", "payment", 10)])
        preview = self.post.preview("2026-11-02")
        self.post.confirm(preview["preview_id"])

        self.publish_apply("2026-11-02", [
            ("b2", "A", "bill", 20), ("p2", "A", "payment", 20)])
        pending = self.post.preview("2026-11-03")

        self.post.close()
        self.net.close()
        self.net = NettingService(self.db,
                                  closure=self.fx.raw_store_handle())
        self.post = PostingService(self.db)

        # Published batch survives.
        batch = self.post.read_batch(1)
        self.assertEqual(batch["entry_count"], 1)
        # Pending preview still confirms.
        batch2 = self.post.confirm(pending["preview_id"])
        self.assertEqual(batch2["batch_no"], 2)
        # Idempotent retry after reopen still returns the same batch.
        again = self.post.confirm(pending["preview_id"])
        self.assertEqual(again["batch_no"], 2)

    def test_bootstrap_on_preexisting_netting_database(self):
        # Simulate a netting database that existed before posting was enabled.
        self.publish_apply("2026-11-01", [
            ("b1", "A", "bill", 100), ("p1", "A", "payment", 100)])
        self.publish_apply("2026-11-02", [
            ("b2", "B", "bill", 40), ("p2", "B", "payment", 40)])
        self.post.close()

        # Enable the handoff on the existing database for the first time: no
        # re-import, facts are bootstrapped as the initial pending set.
        self.post = PostingService(self.db)
        preview = self.post.preview("2026-11-09")
        self.assertTrue(preview["bootstrap"])
        self.assertEqual(preview["entry_count"], 2)
        batch = self.post.confirm(preview["preview_id"])
        self.assertEqual(batch["batch_no"], 1)
        with self.assertRaises(NothingToPost):
            self.post.preview("2026-11-10")


class ConcurrencyTests(PostingTestBase):
    def _cli_args(self, command, **extra):
        args = {"command": command, "netting_db": str(self.db)}
        args.update(extra)
        path = self.tmp / f"cli_{uuid.uuid4().hex}.json"
        path.write_text(json.dumps(args), encoding="utf-8")
        return path

    def run_cli(self, command, **extra):
        proc = subprocess.run(
            [sys.executable, "-I", str(POSTING_CLI),
             "--args", str(self._cli_args(command, **extra))],
            capture_output=True, text=True, timeout=120,
        )
        if proc.returncode not in (0, 3):
            self.fail(f"CLI rc={proc.returncode}: {proc.stderr}\n{proc.stdout}")
        return json.loads(proc.stdout)

    def test_concurrent_confirms_only_one_wins_and_no_double_post(self):
        self.publish_apply("2026-11-01", [
            ("b1", "A", "bill", 100), ("p1", "A", "payment", 100)])
        pv1 = self.run_cli("preview", posting_day="2026-11-02")["preview"]
        pv2 = self.run_cli("preview", posting_day="2026-11-02")["preview"]

        def confirm(preview_id):
            return self.run_cli("confirm", preview_id=preview_id)

        with ThreadPoolExecutor(max_workers=2) as pool:
            results = list(pool.map(confirm,
                                    [pv1["preview_id"], pv2["preview_id"]]))
        statuses = sorted(
            "ok" if r.get("ok") else r.get("error") for r in results)
        self.assertEqual(statuses, ["PreviewStale", "ok"])
        batches = self.run_cli("batches")["batches"]
        self.assertEqual(len(batches), 1)
        # The single batch has exactly one forward line, no duplicate flows.
        batch = self.run_cli("read_batch", batch_no=1)["batch"]
        self.assertEqual(batch["entry_count"], 1)

    def test_application_that_commits_before_confirm_makes_preview_stale(self):
        # Same-process serial interleaving: the write lock serialises the
        # application before the confirm begins, and the confirm must reject
        # the stale preview rather than publish old facts as new.
        self.publish_apply("2026-11-01", [
            ("b1", "A", "bill", 100), ("p1", "A", "payment", 100)])
        self.post.confirm(self.post.preview("2026-11-02")["preview_id"])
        self.publish_apply("2026-11-03", [
            ("b2", "A", "bill", 50), ("p2", "A", "payment", 50)])
        preview = self.post.preview("2026-11-04")

        # A correction commits before confirm runs.
        self.publish_apply("2026-11-03", [
            ("b2", "A", "bill", 55), ("p2", "A", "payment", 55)],
            revision=2)
        with self.assertRaises(PreviewStale):
            self.post.confirm(preview["preview_id"])
        fresh = self.post.preview("2026-11-04")
        self.assertEqual(
            [e["cents"] for e in fresh["entries"]
             if e["line_type"] == "forward"], [55])
        self.post.confirm(fresh["preview_id"])

    def test_multiprocess_application_vs_confirm_keeps_publishing_consistent(self):
        # Cross-process version of the interleaving: the netting application
        # runs in an independent python -I process on the same SQLite file.
        self.publish_apply("2026-11-01", [
            ("b1", "A", "bill", 100), ("p1", "A", "payment", 100)])
        self.post.confirm(self.post.preview("2026-11-02")["preview_id"])
        self.publish_apply("2026-11-03", [
            ("b2", "A", "bill", 50), ("p2", "A", "payment", 50)])
        preview = self.run_cli("preview", posting_day="2026-11-04")["preview"]

        # A second process applies a correction in parallel.
        self.fx.publish_day("2026-11-03",
                            csv_bytes([("b2", "A", "bill", 55),
                                       ("p2", "A", "payment", 55)]), 2)
        applied = self.run_cli_apply("2026-11-03", 2)
        self.assertTrue(applied["ok"])

        outcome = self.run_cli("confirm", preview_id=preview["preview_id"])
        self.assertEqual(outcome["error"], "PreviewStale")
        fresh = self.run_cli("preview", posting_day="2026-11-04")["preview"]
        confirmed = self.run_cli("confirm",
                                 preview_id=fresh["preview_id"])
        self.assertTrue(confirmed["ok"])
        self.assertEqual(confirmed["batch"]["batch_no"], 2)

    def run_cli_apply(self, day, revision):
        args = {
            "command": "apply",
            "netting_db": str(self.db),
            "closure_data_dir": str(self.fx.data_dir),
            "closure_inbound_dir": str(self.fx.inbound),
            "business_day": day,
            "revision": revision,
            "contract_id": CONTRACT_ID,
            "contract_version": CONTRACT_VERSION,
        }
        path = self.tmp / f"cli_{uuid.uuid4().hex}.json"
        path.write_text(json.dumps(args), encoding="utf-8")
        proc = subprocess.run(
            [sys.executable, "-I",
             str(ROOT / "netting" / "cli.py"), "--args", str(path)],
            capture_output=True, text=True, timeout=120,
        )
        if proc.returncode not in (0, 3):
            self.fail(f"apply CLI rc={proc.returncode}: {proc.stderr}")
        return json.loads(proc.stdout)


if __name__ == "__main__":
    unittest.main(verbosity=2)

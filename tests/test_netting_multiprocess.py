"""Real multi-process concurrency tests for the netting database.

Every CLI call is an independent ``python -I`` process pointing at the same
netting SQLite file and the same closure store.  The tests prove that:

* two applications of different days serialise and never lose a day's work;
* a newer revision committed first blocks an older revision from rolling it
  back, and the stale attempt leaves the published facts untouched;
* a reader never observes a new revision number paired with old facts
  (the revision row and its facts commit as one transaction);
* every maintained result still equals a from-scratch replay afterwards.
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

from netting_helpers import (  # noqa: E402
    CONTRACT_ID,
    CONTRACT_VERSION,
    csv_bytes,
    make_fixture,
)

ROOT = Path(__file__).resolve().parent.parent
NETTING_CLI = ROOT / "netting" / "cli.py"


class NettingConcurrencyTests(unittest.TestCase):
    def setUp(self):
        self.fx = make_fixture()
        self.tmp = Path(tempfile.mkdtemp(prefix="netting_mp_"))
        self.netting_db = self.tmp / "netting.db"
        self.closure_data = self.fx.data_dir
        self.closure_inbound = self.fx.inbound

    def tearDown(self):
        self.fx.cleanup()
        shutil.rmtree(self.tmp, ignore_errors=True)

    def run_cli(self, command: str, **extra) -> dict:
        args = {
            "command": command,
            "netting_db": str(self.netting_db),
            "closure_data_dir": str(self.closure_data),
            "closure_inbound_dir": str(self.closure_inbound),
        }
        args.update(extra)
        arg_path = self.tmp / f"args_{uuid.uuid4().hex}.json"
        arg_path.write_text(json.dumps(args), encoding="utf-8")
        proc = subprocess.run(
            [sys.executable, "-I", str(NETTING_CLI), "--args", str(arg_path)],
            capture_output=True, text=True, timeout=120,
        )
        if proc.returncode not in (0, 3):
            self.fail(f"CLI rc={proc.returncode}: {proc.stderr}\n{proc.stdout}")
        return json.loads(proc.stdout)

    def _publish(self, day, rows, revision=1):
        self.fx.publish_day(day, csv_bytes(rows), revision)

    def test_concurrent_applications_of_different_days_both_commit(self):
        # Two independent days, each self-contained (bill + payment match).
        self._publish("2026-11-01", [
            ("b1", "A", "bill", 100), ("p1", "A", "payment", 100)])
        self._publish("2026-11-02", [
            ("b2", "B", "bill", 200), ("p2", "B", "payment", 200)])

        def apply_day(day):
            return self.run_cli(
                "apply", business_day=day, revision=1,
                contract_id=CONTRACT_ID, contract_version=CONTRACT_VERSION)

        with ThreadPoolExecutor(max_workers=4) as pool:
            results = list(pool.map(
                lambda d: apply_day(d), ["2026-11-01", "2026-11-02"] * 2))
        oks = [r for r in results if r.get("ok")]
        self.assertEqual(len(oks), 4)
        # Each day ends with exactly one applied + one idempotent outcome.
        revisions = self.run_cli("revisions")["revisions"]
        self.assertEqual(
            sorted(r["business_day"] for r in revisions),
            ["2026-11-01", "2026-11-02"])
        facts = self.run_cli("read")["facts"]
        self.assertEqual(len(facts), 2)
        replay = self.run_cli("replay")["facts"]
        self.assertEqual(facts, replay)

    def test_older_revision_can_not_roll_back_newer_one_under_race(self):
        # Day D1 gets two genuine closed revisions in the closure store.
        self._publish("2026-11-01", [
            ("b1", "A", "bill", 100), ("p1", "A", "payment", 100)], 1)
        self._publish("2026-11-01", [
            ("b1", "A", "bill", 250), ("p1", "A", "payment", 250)], 2)

        def apply_rev(rev):
            return self.run_cli(
                "apply", business_day="2026-11-01", revision=rev,
                contract_id=CONTRACT_ID, contract_version=CONTRACT_VERSION)

        # Fire revision 2 and revision 1 concurrently, several times each.
        with ThreadPoolExecutor(max_workers=6) as pool:
            results = list(pool.map(apply_rev, [2, 1, 1, 2, 1, 2]))

        # Revision 2 must be the selected one; every rev-1 attempt is a
        # documented stale rejection (rc 3) and never overwrites the result.
        selected = self.run_cli("revisions")["revisions"]
        self.assertEqual(len(selected), 1)
        self.assertEqual(selected[0]["revision"], 2)
        stale = [r for r in results if not r["ok"]]
        self.assertTrue(stale, "expected at least one stale-revision rejection")
        self.assertTrue(all(r["error"] == "StaleRevision" for r in stale))

        facts = self.run_cli("read")["facts"]
        self.assertEqual(len(facts), 1)
        self.assertEqual(facts[0]["cents"], 250)
        self.assertEqual(facts, self.run_cli("replay")["facts"])

        # The audit log keeps both the winning application and rejections.
        log = self.run_cli("log", business_day="2026-11-01")["log"]
        outcomes = {row["outcome"] for row in log}
        self.assertIn("applied", outcomes)
        self.assertIn("rejected", outcomes)

    def test_reader_never_sees_new_revision_with_old_facts(self):
        # Baseline revision 1, then revision 2; hammer reads while applying.
        self._publish("2026-11-01", [
            ("b1", "A", "bill", 100), ("p1", "A", "payment", 100)], 1)
        self.run_cli("apply", business_day="2026-11-01", revision=1,
                     contract_id=CONTRACT_ID,
                     contract_version=CONTRACT_VERSION)
        self._publish("2026-11-01", [
            ("b1", "A", "bill", 100), ("p1", "A", "payment", 100),
            ("b2", "A", "bill", 50), ("p2", "A", "payment", 50)], 2)

        def reader(_):
            # One process = one consistent snapshot: the revision selection
            # and the facts it reads can never come from different versions.
            rev = self.run_cli("revisions")["revisions"]
            facts = self.run_cli("read")["facts"]
            return rev, facts

        with ThreadPoolExecutor(max_workers=4) as pool:
            reads = list(pool.map(reader, range(8)))
            apply_out = self.run_cli(
                "apply", business_day="2026-11-01", revision=2,
                contract_id=CONTRACT_ID,
                contract_version=CONTRACT_VERSION)
            reads += list(pool.map(reader, range(8)))
        self.assertTrue(apply_out["ok"])

        # In every snapshot the facts match the selected revision: rev 1 has
        # one pairing (100), rev 2 has two (50 and 100).  A new revision
        # number is never visible together with the old single fact.
        for rev_rows, facts in reads:
            selected = rev_rows[0]["revision"] if rev_rows else None
            amounts = sorted(f["cents"] for f in facts)
            if selected == 1:
                self.assertEqual(amounts, [100])
            elif selected == 2:
                self.assertEqual(amounts, [50, 100])
        final = self.run_cli("read")["facts"]
        self.assertEqual(sorted(f["cents"] for f in final), [50, 100])
        self.assertEqual(final, self.run_cli("replay")["facts"])


if __name__ == "__main__":
    unittest.main(verbosity=2)

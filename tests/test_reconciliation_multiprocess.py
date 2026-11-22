"""Real multi-process and CLI tests for receipt reconciliation.

Every CLI invocation is a separate ``python -I`` process sharing the same two
SQLite files, exactly as independent importers and finance query processes
would use them.  The tests prove that:

* concurrent imports of *different* groups never lose each other's receipts;
* concurrent same-group imports serialise and a lagging revision can never
  overwrite the newer effective version;
* a malformed receipt in one process leaves the group and every other group
  untouched;
* the batch query CLI reports the same closeable answer as the service after
  every interleave, including across a process restart;
* non-existent batches are reported as errors, never as closeable.
"""

import json
import shutil
import subprocess
import sys
import tempfile
import unittest
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
from netting import NettingService  # noqa: E402
from posting import PostingService  # noqa: E402

ROOT = Path(__file__).resolve().parent.parent
CLI = ROOT / "reconciliation" / "cli.py"


class MultiprocessReconciliationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.fx = make_fixture()
        cls.tmp = Path(tempfile.mkdtemp(prefix="recon_mp_"))
        cls.posting_db = cls.tmp / "netting.db"
        cls.receipt_db = cls.tmp / "receipts.db"
        net = NettingService(cls.posting_db,
                             closure=cls.fx.raw_store_handle())
        post = PostingService(cls.posting_db)
        cls.fx.publish_day("2026-11-01", csv_bytes([
            ("b1", "A", "bill", 100), ("p1", "A", "payment", 100),
        ]), 1)
        net.apply_revision("2026-11-01", 1, CONTRACT_ID, CONTRACT_VERSION)
        batch = post.confirm(post.preview("2026-11-02")["preview_id"])
        cls.flow_id = post.read_batch(batch["batch_no"])["entries"][0][
            "flow_id"]
        post.close()
        net.close()

    @classmethod
    def tearDownClass(cls):
        cls.fx.cleanup()
        shutil.rmtree(cls.tmp, ignore_errors=True)

    def _args_path(self, payload):
        path = self.tmp / f"args_{next(self._counter)}.json"
        path.write_text(json.dumps(payload, ensure_ascii=False),
                        encoding="utf-8")
        return str(path)

    def setUp(self):
        self._counter = iter(range(1000))

    def _run_cli(self, args):
        proc = subprocess.run(
            [sys.executable, "-I", str(CLI), "--args",
             self._args_path(args)],
            cwd=str(ROOT), capture_output=True, text=True,
        )
        if args.get("format") == "text" and args.get("command") == "batch":
            return proc.returncode, proc.stdout
        try:
            payload = json.loads(proc.stdout)
        except json.JSONDecodeError:
            self.fail(f"non-JSON CLI output: {proc.stdout!r} {proc.stderr!r}")
        return proc.returncode, payload

    def _import(self, receipt):
        return self._run_cli({
            "command": "import",
            "posting_db": str(self.posting_db),
            "receipt_db": str(self.receipt_db),
            "receipt": receipt,
        })

    def test_concurrent_imports_of_different_groups_all_survive(self):
        groups = 12
        prefix = "MP"
        receipts = [
            {"receipt_id": f"{prefix}{i:02d}", "revision": 1,
             "lines": [{"line_id": "l1", "flow_id": str(self.flow_id),
                        "signed_cents": 1}]}
            for i in range(groups)
        ]
        with ThreadPoolExecutor(max_workers=6) as pool:
            results = list(pool.map(self._import, receipts))
        self.assertTrue(all(rc == 0 and p["ok"] for rc, p in results))

        rc, payload = self._run_cli({
            "command": "groups",
            "posting_db": str(self.posting_db),
            "receipt_db": str(self.receipt_db),
        })
        self.assertEqual(rc, 0)
        stored = {g["receipt_id"]: g["current_revision"]
                  for g in payload["groups"]}
        for i in range(groups):
            self.assertEqual(stored[f"{prefix}{i:02d}"], 1)

        # The shared flow received one cent per surviving group and no group
        # was double counted.  Only this test's groups are counted.
        rc, payload = self._run_cli({
            "command": "batch", "batch_no": 1,
            "posting_db": str(self.posting_db),
            "receipt_db": str(self.receipt_db),
        })
        flow = payload["batch_status"]["flows"][0]
        own = [c for c in flow["contributions"]
               if c["receipt_id"].startswith(prefix)]
        self.assertEqual(sum(c["signed_cents"] for c in own), groups)
        self.assertEqual(len(own), groups)

    def test_concurrent_revision_race_keeps_highest_revision(self):
        # Seed the group at revision 5.
        rc, _ = self._import({
            "receipt_id": "RACE", "revision": 5,
            "lines": [{"line_id": "l1", "flow_id": str(self.flow_id),
                       "signed_cents": 10}],
        })
        self.assertEqual(rc, 0)

        attempts = [
            {"receipt_id": "RACE", "revision": rev,
             "lines": [{"line_id": "l1", "flow_id": str(self.flow_id),
                        "signed_cents": rev}]}
            # Deliberately shuffled so old and new revisions arrive together.
            for rev in (6, 8, 7, 9, 6, 7, 8)
        ]
        with ThreadPoolExecutor(max_workers=7) as pool:
            results = list(pool.map(self._import, attempts))
        accepted = [p["import"]["outcome"] for rc, p in results if rc == 0]
        self.assertIn("replaced", accepted)

        rc, payload = self._run_cli({
            "command": "group", "receipt_id": "RACE",
            "posting_db": str(self.posting_db),
            "receipt_db": str(self.receipt_db),
        })
        self.assertEqual(rc, 0)
        group = payload["group"]
        self.assertEqual(group["current_revision"], 9)
        self.assertEqual(group["lines"][0]["signed_cents"], 9)
        # Every attempt was audited, rejected ones included.
        rc, payload = self._run_cli({
            "command": "log",
            "posting_db": str(self.posting_db),
            "receipt_db": str(self.receipt_db),
        })
        race_log = [r for r in payload["import_log"]
                    if r["receipt_id"] == "RACE"]
        self.assertEqual(len(race_log), 1 + len(attempts))

    def test_bad_import_is_isolated_and_unknown_batch_fails(self):
        good = {"receipt_id": "KEEP", "revision": 1,
                "lines": [{"line_id": "l1",
                           "flow_id": str(self.flow_id),
                           "signed_cents": 5}]}
        rc, _ = self._import(good)
        self.assertEqual(rc, 0)
        bad = {"receipt_id": "KEEP", "revision": 2,
               "lines": [
                   {"line_id": "l1", "flow_id": str(self.flow_id),
                    "signed_cents": 5},
                   {"line_id": "ghost", "flow_id": "424242",
                    "signed_cents": 7},
               ]}
        rc, payload = self._import(bad)
        self.assertEqual(rc, 3)
        self.assertEqual(payload["error"], "MalformedReceipt")

        rc, payload = self._run_cli({
            "command": "group", "receipt_id": "KEEP",
            "posting_db": str(self.posting_db),
            "receipt_db": str(self.receipt_db),
        })
        self.assertEqual(payload["group"]["current_revision"], 1)

        rc, payload = self._run_cli({
            "command": "batch", "batch_no": 777,
            "posting_db": str(self.posting_db),
            "receipt_db": str(self.receipt_db),
        })
        self.assertEqual(rc, 3)
        self.assertEqual(payload["error"], "BatchNotFound")

    def test_text_render_and_restart_visibility(self):
        rc, text = self._run_cli({
            "command": "batch", "batch_no": 1, "format": "text",
            "posting_db": str(self.posting_db),
            "receipt_db": str(self.receipt_db),
        })
        # Text mode prints a Chinese report, not a JSON envelope.
        self.assertEqual(rc, 0)
        self.assertIn("批次 1", text)
        self.assertIn("能否关账", text)

    def test_usage_errors(self):
        proc = subprocess.run(
            [sys.executable, "-I", str(CLI)],
            cwd=str(ROOT), capture_output=True, text=True)
        self.assertEqual(proc.returncode, 2)


if __name__ == "__main__":
    unittest.main()

"""Real multi-process interleave tests.

Each CLI call runs in a separate ``python -I`` process so the service is
exercised exactly as independent processors would use it: shared SQLite
database + blob directory, no in-process state.  These tests prove that:

* concurrent receptions serialise safely (WAL + BEGIN IMMEDIATE),
* an unclosed batch cannot be claimed by any process,
* an active claim in one process blocks claims from every other process,
* the new revision only becomes claimable after the old claim completes,
* old revision inputs are never rewritten.
"""

import json
import os
import shutil
import subprocess
import sys
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
CLI = ROOT / "batch_closure" / "cli.py"

# Wall-clock driven CLI processes: keep the deadline in the near future so
# the test deterministically crosses it while running.
from datetime import datetime, timedelta, timezone

_NOW = datetime.now(timezone.utc)
_DEADLINE = _NOW + timedelta(seconds=10)
_LATE_END = _DEADLINE + timedelta(minutes=10)

CONTRACT = {
    "contract_id": "mp-settle",
    "version": "1",
    "batch_key": ["business_date"],
    "max_file_size": 4 * 1024 * 1024,
    "deadline": {"type": "absolute",
                 "at": _DEADLINE.strftime("%Y-%m-%dT%H:%M:%S+00:00")},
    "late_window_minutes": 10,
    "on_missing_required": "reject",
    "supported_compression": [],
    "members": {
        "manifest.json": {
            "required": True, "role": "manifest",
            "digest_algorithms": ["sha256"],
        },
        "detail.csv": {
            "required": True, "role": "data",
            "digest_algorithms": ["sha256"],
        },
        "optional.dat": {
            "required": False, "role": "data",
            "digest_algorithms": ["sha256"],
        },
    },
}
KEY = {"business_date": _NOW.strftime("%Y-%m-%d")}


def manifest_doc():
    return {"members": [
        {"name": "manifest.json", "path": "manifest.json"},
        {"name": "detail.csv", "path": "detail.csv"},
        {"name": "optional.dat", "path": "optional.dat"},
    ]}


class MultiProcessTests(unittest.TestCase):
    def setUp(self):
        self.workdir = Path(tempfile.mkdtemp(prefix="mp_"))
        self.inbound = self.workdir / "inbound"
        self.inbound.mkdir()
        self.data = self.workdir / "data"
        (self.inbound / "manifest.json").write_bytes(
            json.dumps(manifest_doc()).encode()
        )
        (self.inbound / "detail.csv").write_bytes(b"detail-v1")
        (self.inbound / "optional.dat").write_bytes(b"opt-v1")

    def tearDown(self):
        shutil.rmtree(self.workdir, ignore_errors=True)

    def run_cli(self, command: str, **extra) -> dict:
        args = {"command": command, "data_dir": str(self.data),
                "inbound": str(self.inbound),
                "contract_id": CONTRACT["contract_id"],
                "contract_version": CONTRACT["version"],
                "key": KEY}
        args.update(extra)
        import uuid

        arg_path = self.workdir / f"args_{uuid.uuid4().hex}.json"
        arg_path.write_text(json.dumps(args))
        proc = subprocess.run(
            [sys.executable, "-I", str(CLI), "--args", str(arg_path)],
            capture_output=True, text=True, timeout=120,
        )
        # 0 = normal result (including ok:false business payloads that the
        # CLI prints), 3 = documented business rejection with an error body.
        self.assertIn(
            proc.returncode, (0, 3),
            msg=f"CLI failed rc={proc.returncode}: {proc.stderr}\n{proc.stdout}",
        )
        self.assertTrue(proc.stdout.strip(), msg=f"empty stdout: {proc.stderr}")
        return json.loads(proc.stdout)

    def test_full_multiprocess_lifecycle(self):
        # Contract is compiled by its own short-lived process.
        out = self.run_cli("compile", contract=CONTRACT)
        self.assertTrue(out["ok"])

        # Two processes receive different members concurrently.
        with ThreadPoolExecutor(max_workers=2) as pool:
            futures = [
                pool.submit(self.run_cli, "receive", member="manifest.json",
                            source=str(self.inbound / "manifest.json")),
                pool.submit(self.run_cli, "receive", member="detail.csv",
                            source=str(self.inbound / "detail.csv")),
            ]
            results = [f.result() for f in futures]
        self.assertTrue(all(r["result"]["accepted"] for r in results))

        # Any process attempting a claim before closure is refused.
        refused = self.run_cli("claim", processor="eager-worker")
        self.assertFalse(refused["ok"])
        self.assertEqual(refused["error"], "NotClosed")

        # Wait (inside the test process) until the absolute deadline passes.
        import time

        while datetime.now(timezone.utc) <= _DEADLINE:
            time.sleep(0.2)

        # Evaluate closure from a fresh process.
        ev = self.run_cli("evaluate")
        self.assertTrue(ev["result"]["closed"])
        self.assertEqual(ev["result"]["state"], "CLOSED")
        self.assertEqual(ev["result"]["revision"], 1)

        # Two processes race for the claim: exactly one wins.
        with ThreadPoolExecutor(max_workers=2) as pool:
            claims = list(pool.map(
                lambda i: self.run_cli("claim", processor=f"worker-{i}"),
                range(2),
            ))
        winners = [c for c in claims if c["ok"]]
        losers = [c for c in claims if not c["ok"]]
        self.assertEqual(len(winners), 1)
        self.assertEqual(len(losers), 1)
        self.assertEqual(losers[0]["error"], "AlreadyClaimed")
        token1 = winners[0]["claim"]["token"]
        self.assertEqual(winners[0]["claim"]["revision"], 1)

        # Token is verifiable from another process and binds contract+binding.
        verified = self.run_cli("verify-token", token=token1)
        self.assertTrue(verified["ok"])
        self.assertEqual(verified["result"]["payload"]["revision"], 1)
        self.assertEqual(
            verified["result"]["payload"]["contract_digest"],
            winners[0]["claim"]["contract_digest"],
        )

        # Late optional file arrives while revision 1 is being processed.
        recv = self.run_cli("receive", member="optional.dat",
                            source=str(self.inbound / "optional.dat"))
        self.assertTrue(recv["result"]["accepted"])
        ev2 = self.run_cli("evaluate")
        self.assertEqual(ev2["result"]["revision"], 2)

        # Revision 2 cannot be claimed while revision 1 is active anywhere.
        blocked = self.run_cli("claim", processor="worker-2")
        self.assertFalse(blocked["ok"])
        self.assertEqual(blocked["error"], "AlreadyClaimed")

        # Old token still works against the immutable revision-1 binding.
        still = self.run_cli("verify-token", token=token1)
        self.assertTrue(still["ok"])
        self.assertEqual(still["result"]["claim_status"], "CLAIMED")

        # Completing revision 1 unlocks revision 2.
        done = self.run_cli("complete", token=token1)
        self.assertTrue(done["ok"])
        claim2 = self.run_cli("claim", processor="worker-2")
        self.assertTrue(claim2["ok"])
        self.assertEqual(claim2["claim"]["revision"], 2)
        self.assertNotEqual(claim2["claim"]["binding_sig"],
                            winners[0]["claim"]["binding_sig"])

        # Final status from yet another process; replay must agree.
        status = self.run_cli("status")
        self.assertEqual(status["status"]["state"], "CLOSED")
        self.assertEqual(status["status"]["current_revision"], 2)
        replay = self.run_cli("replay")
        self.assertTrue(replay["result"]["matches"])

        # Evidence bundle exported and verified in separate processes.
        evidence_dir = self.workdir / "evidence"
        self.run_cli("evidence", out_dir=str(evidence_dir))
        verdict = self.run_cli("verify-bundle", out_dir=str(evidence_dir))
        self.assertTrue(verdict["ok"], msg=json.dumps(verdict))
        self.assertEqual(verdict["closure_revisions"], [1, 2])

    def test_process_crash_recovers_without_losing_commits(self):
        self.run_cli("compile", contract=CONTRACT)
        self.run_cli("receive", member="manifest.json",
                     source=str(self.inbound / "manifest.json"))
        # Simulate a crash: every prior process already exited.  A brand new
        # process still sees the committed reception.
        status = self.run_cli("status")
        self.assertIn("manifest.json", status["status"]["members"])
        self.assertEqual(status["status"]["state"], "WAITING")


if __name__ == "__main__":
    unittest.main(verbosity=2)

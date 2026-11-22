#!/usr/bin/env python3
"""End-to-end demonstration of the batch closure service API.

Run with:  python3 -I demo.py
It uses an injected clock so the whole timeline (arrivals, deadline, late
window, revisions, claims, evidence export) plays out deterministically.
"""

import json
import shutil
import sys
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from batch_closure import BatchClosureService, compile_contract  # noqa: E402
from batch_closure.evidence import verify_bundle  # noqa: E402

UTC = timezone.utc


def main() -> None:
    workdir = Path(tempfile.mkdtemp(prefix="closure_demo_"))
    inbound = workdir / "inbound"
    inbound.mkdir()

    # 1) Declarative arrival contract --------------------------------------
    contract = compile_contract({
        "contract_id": "daily-settlement",
        "version": "2026-11",
        "batch_key": ["business_date"],
        "max_file_size": 64 * 1024 * 1024,
        "deadline": {"type": "relative", "offset_days": 1, "hour": 12, "minute": 0},
        "late_window_minutes": 60,
        "on_missing_required": "publish_with_missing",
        "supported_compression": ["gzip"],
        "members": {
            "manifest.json": {"required": True, "role": "manifest"},
            "detail.csv": {"required": True, "role": "data",
                           "digest_algorithms": ["sha256"]},
            "control.json": {"required": False, "role": "summary"},
        },
    })
    print(f"contract compiled: {contract.contract_id} digest={contract.digest[:12]}...")

    key = {"business_date": "2026-11-22T00:00:00+00:00"}
    deadline = datetime(2026, 11, 23, 12, 0, tzinfo=UTC)

    class Clock:
        t = deadline - timedelta(hours=4)

        def __call__(self):
            return self.t

    clock = Clock()
    svc = BatchClosureService(workdir / "data", inbound, clock=clock)
    svc.compile_contract(contract.to_dict())

    # 2) Files arrive one at a time ----------------------------------------
    manifest = {"members": [
        {"name": "manifest.json", "path": "manifest.json"},
        {"name": "detail.csv", "path": "detail.csv"},
        {"name": "control.json", "path": "control.json"},
    ]}
    (inbound / "manifest.json").write_bytes(json.dumps(manifest).encode())
    svc.receive_file(contract, key, "manifest.json", inbound / "manifest.json")

    detail_v1 = b"id,amount\n1,100\n"
    (inbound / "detail.csv").write_bytes(detail_v1)
    svc.receive_file(
        contract, key, "detail.csv", inbound / "detail.csv",
        expected_digests={"sha256": __import__("hashlib").sha256(detail_v1).hexdigest()},
    )
    print("status before deadline:", svc.get_status(contract, key)["state"])
    print("early closure attempt  :",
          svc.evaluate_closure(contract, key)["reason"])

    # 3) Deadline passes: first closure (partial publish policy) -----------
    clock.t = deadline + timedelta(minutes=1)
    first = svc.evaluate_closure(contract, key)
    print("closure at deadline    :", first["state"], "rev", first["revision"],
          "missing", first["missing"])

    # 4) Late optional file inside the window creates a revision -----------
    clock.t = deadline + timedelta(minutes=20)
    (inbound / "control.json").write_bytes(json.dumps({"digests": {}}).encode())
    svc.receive_file(contract, key, "control.json", inbound / "control.json")
    second = svc.evaluate_closure(contract, key)
    print("late revision          : rev", second["revision"],
          "impact:", second["impact"]["summary"])

    # 5) Processor claims the closed revision ------------------------------
    claim = svc.claim(contract, key, processor="settlement-worker-1")
    print("claim token            :", claim["token"][:48] + "...")
    print("token verifies         :", svc.verify_token(claim["token"])["valid"])
    svc.complete_claim(claim["token"])

    # 6) Replayable evidence ------------------------------------------------
    out = svc.export_evidence(contract, key, workdir / "evidence")
    verdict = verify_bundle(out)
    print("evidence bundle        :", out)
    print("evidence verification  :", "OK" if verdict["ok"] else verdict["errors"])
    print("independent replay     :",
          "matches" if svc.replay(contract, key)["matches"] else "MISMATCH")
    svc.close()
    shutil.rmtree(workdir)


if __name__ == "__main__":
    main()

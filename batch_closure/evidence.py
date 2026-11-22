"""Replayable evidence bundle verification.

A bundle produced by :meth:`BatchClosureService.export_evidence` can be
verified independently of the running service: blob hashes are recomputed,
the hash chain linking revisions is checked, and the contract digest is
re-derived from the canonical contract.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any, Dict, List

from .contract import canonical_json


def _read(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def verify_bundle(out_dir) -> Dict[str, Any]:
    out = Path(out_dir)
    manifest = _read(out / "evidence_manifest.json")
    contract = _read(out / "contract.json")
    closures = _read(out / "closures.json")
    arrivals = _read(out / "arrivals.json")
    events = _read(out / "events.json")

    errors: List[str] = []

    # 1) contract digest re-derivation
    from .contract import compile_contract

    recompiled = compile_contract(contract)
    if recompiled.digest != manifest["contract_digest"]:
        errors.append("contract digest does not re-derive from contract.json")

    # 2) every stored blob re-hashes to its content-addressed name
    blob_checks = []
    for entry in manifest["blob_manifest"]:
        blob = out / "blobs" / entry["sha256"][:2] / entry["sha256"]
        if not blob.exists():
            errors.append(f"missing blob {entry['sha256']}")
            blob_checks.append({**entry, "ok": False})
            continue
        h = hashlib.sha256()
        with open(blob, "rb") as fh:
            for chunk in iter(lambda: fh.read(64 * 1024), b""):
                h.update(chunk)
        ok = h.hexdigest() == entry["sha256"]
        if not ok:
            errors.append(f"blob hash mismatch for {entry['sha256']}")
        blob_checks.append({**entry, "ok": ok})

    # 3) arrival rows reference exactly the bundled blobs
    for arrival in arrivals:
        sha = arrival["blob_sha256"]
        if sha and not (out / "blobs" / sha[:2] / sha).exists():
            errors.append(f"arrival seq {arrival['seq']} references absent blob {sha}")

    # 4) evidence hash chain: each revision links to the previous one
    prev = None
    for row in closures:
        if row["prev_evidence_hash"] != prev:
            errors.append(f"evidence chain broken at revision {row['revision']}")
        prev = row["evidence_hash"]

    # 5) event log covers the closure decisions
    decided = [e for e in events if e["event_type"] == "closure_decided"]
    if len(decided) != len(closures):
        errors.append("closure_decided event count does not match closures table")

    return {
        "ok": not errors,
        "errors": errors,
        "blob_checks": blob_checks,
        "closure_revisions": [row["revision"] for row in closures],
        "contract_digest": manifest["contract_digest"],
        "recomputed_contract_digest": recompiled.digest,
    }

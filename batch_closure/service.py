"""Batch closure service.

The service owns all state transitions.  Every reception and every closure
decision is:

1. committed to SQLite (WAL + synchronous=FULL) inside an immediate
   transaction, and
2. appended to the append-only ``events`` log,

so a restart (or a second process) can recover the exact state and replay the
whole history.  File bytes live in a content-addressed blob store keyed by
their raw sha256; blobs are written once and never mutated, which is what keeps
the inputs of an old revision immutable when a later revision arrives.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import os
import shutil
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, List, Mapping, Optional, Tuple

from .contract import (
    CompiledContract,
    canonical_bytes,
    canonical_json,
    compile_contract,
    deadline_for,
    late_window_end,
    parse_utc,
    utcnow,
)
from .errors import (
    AlreadyClaimed,
    BadContract,
    InvalidToken,
    NotClosed,
    UnknownContract,
)
from .hashing import StreamSizeExceeded, gzip_digest, sniff_compression, stream_digest
from .paths import safe_resolve
from .store import Store, utcnow_iso

# Validation finding / quarantine reason taxonomy.  Each failure mode has its
# own distinct code so the audit trail can separate them.
KIND_MANIFEST_MISSING = "manifest_missing"
KIND_DIGEST_MISMATCH = "digest_mismatch"
KIND_DUPLICATE_MEMBER = "duplicate_member"
KIND_SIZE_EXCEEDED = "size_exceeded"
KIND_UNSUPPORTED_COMPRESSION = "unsupported_compression"
KIND_COMPRESSION_MISMATCH = "compression_mismatch"
KIND_COMPRESSION_CORRUPT = "compression_corrupt"
KIND_PATH_ESCAPE = "path_escape"
KIND_UNKNOWN_MEMBER = "unknown_member"
KIND_MANIFEST_MALFORMED = "manifest_malformed"
KIND_MANIFEST_DUPLICATE = "manifest_duplicate"
KIND_SUMMARY_MALFORMED = "summary_malformed"
KIND_LATE_ARRIVAL = "late_arrival"
KIND_REJECTED_MISSING = "rejected_missing"

CLAIMABLE_STATES = {"CLOSED", "CLOSED_PARTIAL"}


def _load_compiled(row: sqlite3.Row) -> CompiledContract:
    return compile_contract(json.loads(row["canonical_json"]))


def batch_identity(
    contract: CompiledContract, key_values: Mapping[str, Any]
) -> Tuple[str, Dict[str, str]]:
    missing = [k for k in contract.batch_key if k not in key_values]
    if missing:
        raise BadContract(f"batch key values missing fields: {missing}")
    normalised = {k: str(key_values[k]) for k in contract.batch_key}
    key_json = canonical_json(normalised)
    batch_id = hashlib.sha256(
        f"{contract.contract_id}|{contract.digest}|{key_json}".encode("utf-8")
    ).hexdigest()[:24]
    return batch_id, normalised


class BatchClosureService:
    def __init__(
        self,
        data_dir,
        inbound_root: Optional[Path] = None,
        clock: Optional[Callable[[], datetime]] = None,
    ):
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.inbound_root = Path(inbound_root) if inbound_root else self.data_dir / "inbound"
        self.inbound_root.mkdir(parents=True, exist_ok=True)
        self.store = Store(
            self.data_dir / "closure.db",
            self.data_dir / "blobs",
            now_provider=lambda: self._clock().isoformat(),
        )
        self._clock = clock or utcnow
        self._contract_cache: Dict[str, CompiledContract] = {}

    def close(self) -> None:
        self.store.close()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()

    def _now(self) -> datetime:
        return self._clock()

    # ------------------------------------------------------------ contracts
    def compile_contract(self, declaration: Mapping[str, Any]) -> CompiledContract:
        """Compile, validate and persist a contract version."""
        contract = compile_contract(declaration)
        self.store.upsert_contract(contract)
        self._contract_cache[contract.digest] = contract
        return contract

    def load_contract(self, contract_id: str, version: Optional[str] = None) -> CompiledContract:
        rows = self.store.find_contract(contract_id, version)
        if not rows:
            raise UnknownContract(f"no stored contract {contract_id!r} version {version!r}")
        digest = rows[0]["digest"]
        if digest not in self._contract_cache:
            self._contract_cache[digest] = _load_compiled(rows[0])
        return self._contract_cache[digest]

    def load_contract_by_digest(self, digest: str) -> CompiledContract:
        if digest in self._contract_cache:
            return self._contract_cache[digest]
        row = self.store.get_contract(digest)
        if row is None:
            raise UnknownContract(f"unknown contract digest {digest}")
        contract = _load_compiled(row)
        self._contract_cache[digest] = contract
        return contract

    # --------------------------------------------------------------- batches
    def _ensure_batch(
        self, contract: CompiledContract, key_values: Mapping[str, Any]
    ) -> sqlite3.Row:
        batch_id, normalised = batch_identity(contract, key_values)
        existing = self.store.get_batch(batch_id)
        if existing is not None:
            return existing
        deadline = deadline_for(contract, key_values)
        late_end = late_window_end(contract, deadline)
        now_iso = utcnow_iso()
        self.store.insert_batch(
            {
                "id": batch_id,
                "contract_digest": contract.digest,
                "batch_key_json": canonical_json(normalised),
                "business_date": str(key_values[contract.business_date_field]),
                "deadline": deadline.isoformat(),
                "late_window_end": late_end.isoformat(),
                "current_state": "WAITING",
                "current_revision": 0,
                "created_at": now_iso,
                "updated_at": now_iso,
            }
        )
        self.store.append_event(
            batch_id,
            "batch_opened",
            {
                "contract_digest": contract.digest,
                "batch_key": normalised,
                "deadline": deadline.isoformat(),
                "late_window_end": late_end.isoformat(),
            },
        )
        return self.store.get_batch(batch_id)

    def list_batches(self) -> List[Dict[str, Any]]:
        rows = self.store.conn.execute("SELECT * FROM batches ORDER BY created_at").fetchall()
        return [dict(r) for r in rows]

    # ------------------------------------------------------------- receiving
    def receive_file(
        self,
        contract: CompiledContract,
        key_values: Mapping[str, Any],
        member_name: str,
        source_path,
        expected_digests: Optional[Mapping[str, str]] = None,
        at: Optional[datetime] = None,
    ) -> Dict[str, Any]:
        """Receive one member file.

        Never raises for business validation failures: they are persisted as
        validation findings + quarantined arrival rows and returned in the
        result so callers can react.  Programmer errors (unknown contract ...)
        still raise.
        """
        now = at or self._now()
        source_path = Path(source_path)
        self.store.begin()
        try:
            batch = self._ensure_batch(contract, key_values)
            batch_id = batch["id"]
            deadline = parse_utc(batch["deadline"])
            late_end = parse_utc(batch["late_window_end"])
            seq = self.store.next_arrival_seq(batch_id)
            result: Dict[str, Any] = {
                "batch_id": batch_id,
                "member": member_name,
                "seq": seq,
                "received_at": now.isoformat(),
                "accepted": False,
                "idempotent": False,
                "revision_candidate": False,
                "quarantine_reason": None,
                "findings": [],
            }

            # 1) member must be declared ----------------------------------
            if member_name not in contract.members:
                return self._quarantine(
                    batch_id, member_name, seq, now, source_path, None, 0,
                    {}, {}, "none", "unknown_member",
                    {"error": f"member {member_name!r} not declared in contract"},
                    result,
                )

            spec = contract.members[member_name]
            limit = contract.member_limit(member_name)

            # 2) late window: files after the window are quarantined, never
            #    mutate an existing revision --------------------------------
            if now > late_end:
                return self._quarantine(
                    batch_id, member_name, seq, now, source_path, None, 0,
                    {}, {}, spec["compression"], KIND_LATE_ARRIVAL,
                    {
                        "late_window_end": late_end.isoformat(),
                        "received_at": now.isoformat(),
                    },
                    result,
                )

            # 3) lexical safety of the member name / declared relative path.
            #    Manifest-declared member paths are checked again when the
            #    manifest is parsed below.
            try:
                safe_resolve(self.inbound_root, member_name)
            except Exception as exc:
                return self._quarantine(
                    batch_id, member_name, seq, now, source_path, None, 0,
                    {}, {}, "none", KIND_PATH_ESCAPE,
                    {"path": member_name, "error": str(exc)}, result,
                )

            # 4) stream the bytes: size cap + raw digests, then blob store --
            try:
                size, raw_digests = self._stream_to_blob(source_path, limit)
            except StreamSizeExceeded as exc:
                return self._quarantine(
                    batch_id, member_name, seq, now, source_path, None,
                    exc.actual, {}, {}, spec["compression"],
                    KIND_SIZE_EXCEEDED,
                    {"limit": exc.max_size, "actual": exc.actual}, result,
                )
            except FileNotFoundError as exc:
                return self._quarantine(
                    batch_id, member_name, seq, now, source_path, None, 0,
                    {}, {}, spec["compression"], "source_missing",
                    {"error": str(exc)}, result,
                )
            blob_sha = raw_digests["sha256"]

            # 5) compression contract check --------------------------------
            blob_path = self.store.blob_path(blob_sha)
            detected = sniff_compression(blob_path)
            gz = detected == "gzip"
            if spec["compression"] == "gzip" and not gz:
                return self._quarantine(
                    batch_id, member_name, seq, now, source_path, blob_sha,
                    size, raw_digests, {}, spec["compression"],
                    KIND_COMPRESSION_MISMATCH,
                    {"expected": "gzip",
                     "actual": detected or "plain"}, result,
                )
            if spec["compression"] == "none" and detected is not None:
                # Any recognised compressed payload on a plain member is
                # unsupported (not just gzip): bzip2/xz/zip are all rejected.
                return self._quarantine(
                    batch_id, member_name, seq, now, source_path, blob_sha,
                    size, raw_digests, {}, spec["compression"],
                    KIND_UNSUPPORTED_COMPRESSION,
                    {"detail": f"{detected} compressed payload on a member "
                               f"declared compression=none",
                     "detected_format": detected},
                    result,
                )
            payload_digests: Dict[str, str] = {}
            if gz:
                try:
                    _, payload_digests = gzip_digest(
                        blob_path, spec["digest_algorithms"]
                    )
                except Exception as exc:  # gzip CRC/format failure
                    return self._quarantine(
                        batch_id, member_name, seq, now, source_path, blob_sha,
                        size, raw_digests, {}, "gzip",
                        KIND_COMPRESSION_CORRUPT,
                        {"error": str(exc)}, result,
                    )

            # 6) idempotency: same member, same content, same contract ------
            prior = self.store.latest_arrival(batch_id, member_name)
            if prior is not None and prior["valid_arrival"] == 1 and prior["blob_sha256"] == blob_sha:
                arrival_id = self._record_arrival(
                    batch_id, member_name, seq, now, source_path, blob_sha,
                    size, raw_digests, payload_digests, spec["compression"],
                    valid=True, reason=None, detail={"idempotent_of": prior["seq"]},
                )
                self.store.append_event(
                    batch_id, "file_received",
                    {
                        "member": member_name, "seq": seq, "blob_sha256": blob_sha,
                        "size": size, "idempotent": True,
                        "idempotent_of": prior["seq"],
                        "arrival_id": arrival_id,
                    },
                )
                result.update(
                    accepted=True, idempotent=True, blob_sha256=blob_sha,
                    size=size, arrival_id=arrival_id,
                )
                self.store.commit()
                return result

            # 7) role-specific parsing (manifest / summary) -----------------
            findings: List[Dict[str, Any]] = []
            role_failure = self._check_role_content(
                contract, batch_id, member_name, spec, blob_sha, seq, findings
            )
            if role_failure is not None:
                return self._quarantine(
                    batch_id, member_name, seq, now, source_path, blob_sha,
                    size, raw_digests, payload_digests, spec["compression"],
                    role_failure, {"findings": findings}, result,
                )

            # 8) digest verification ----------------------------------------
            expected = self._expected_digests(
                contract, batch_id, member_name, expected_digests or {}
            )
            digest_bad = self._verify_digests(
                spec, expected, raw_digests, payload_digests, gz, findings
            )
            for f in findings:
                self.store.add_validation(batch_id, f["kind"], member_name, f["detail"], seq)
                result["findings"].append(f)
            if digest_bad:
                return self._quarantine(
                    batch_id, member_name, seq, now, source_path, blob_sha,
                    size, raw_digests, payload_digests, spec["compression"],
                    KIND_DIGEST_MISMATCH, {"findings": findings, "expected": expected},
                    result,
                )

            # 9) valid arrival ----------------------------------------------
            changed = prior is not None and prior["valid_arrival"] == 1 and prior["blob_sha256"] != blob_sha
            arrival_id = self._record_arrival(
                batch_id, member_name, seq, now, source_path, blob_sha,
                size, raw_digests, payload_digests, spec["compression"],
                valid=True, reason=None,
                detail={"replaces_arrival": prior["seq"] if changed else None},
            )
            if changed:
                self.store.add_validation(
                    batch_id, KIND_DUPLICATE_MEMBER, member_name,
                    {
                        "detail": "same-name member received with different content; "
                                  "recorded as a revision candidate, original retained",
                        "previous_blob": prior["blob_sha256"],
                        "new_blob": blob_sha,
                        "previous_seq": prior["seq"],
                        "new_seq": seq,
                    },
                    arrival_id,
                )
            self.store.append_event(
                batch_id, "file_received",
                {
                    "member": member_name, "seq": seq, "blob_sha256": blob_sha,
                    "size": size, "idempotent": False,
                    "content_changed": bool(changed),
                    "arrival_id": arrival_id,
                    "raw_digests": raw_digests,
                    "payload_digests": payload_digests,
                    "compression": spec["compression"],
                },
            )
            result.update(
                accepted=True, blob_sha256=blob_sha, size=size,
                arrival_id=arrival_id, revision_candidate=bool(changed),
                content_changed=bool(changed),
            )
            self.store.commit()
            return result
        except Exception:
            self.store.rollback()
            raise

    def _stream_to_blob(self, source_path: Path, limit: int):
        """Copy a source file into the CAS, enforcing the size cap while streaming."""
        with open(source_path, "rb") as src:
            size, digests = stream_digest(src, ["sha256"], max_size=limit)
        target = self.store.blob_path(digests["sha256"])
        if not target.exists():
            # Re-stream into the content-addressed path only for new content.
            tmp = target.with_suffix(".tmp")
            with open(source_path, "rb") as src, open(tmp, "wb") as dst:
                while True:
                    chunk = src.read(64 * 1024)
                    if not chunk:
                        break
                    dst.write(chunk)
                dst.flush()
                os.fsync(dst.fileno())
            os.replace(tmp, target)
        return size, digests

    def _record_arrival(
        self, batch_id, member_name, seq, now, source_path, blob_sha,
        size, raw_digests, payload_digests, compression, valid, reason, detail
    ) -> int:
        return self.store.insert_arrival(
            {
                "batch_id": batch_id,
                "member_name": member_name,
                "seq": seq,
                "received_at": now.isoformat(),
                "source_path": str(source_path),
                "blob_sha256": blob_sha,
                "size_bytes": size,
                "raw_digests_json": canonical_json(raw_digests),
                "payload_digests_json": canonical_json(payload_digests),
                "compression": compression,
                "valid_arrival": 1 if valid else 0,
                "quarantine_reason": reason,
                "detail_json": canonical_json(detail or {}),
            }
        )

    def _quarantine(
        self, batch_id, member_name, seq, now, source_path, blob_sha, size,
        raw_digests, payload_digests, compression, reason, detail, result
    ) -> Dict[str, Any]:
        arrival_id = self._record_arrival(
            batch_id, member_name, seq, now, source_path, blob_sha, size,
            raw_digests, payload_digests, compression, valid=False,
            reason=reason, detail=detail,
        )
        self.store.add_validation(batch_id, reason, member_name, detail, arrival_id)
        self.store.append_event(
            batch_id, "file_quarantined",
            {
                "member": member_name, "seq": seq, "reason": reason,
                "detail": detail, "arrival_id": arrival_id,
            },
        )
        result["quarantine_reason"] = reason
        result["arrival_id"] = arrival_id
        result["findings"].append({"kind": reason, "detail": detail})
        self.store.commit()
        return result

    def _check_role_content(
        self, contract, batch_id, member_name, spec, blob_sha, seq, findings
    ) -> Optional[str]:
        role = spec["role"]
        if role not in ("manifest", "summary"):
            return None
        path = self.store.blob_path(blob_sha)
        try:
            with open(path, "rb") as fh:
                doc = json.loads(fh.read().decode("utf-8"))
        except Exception as exc:
            findings.append(
                {"kind": KIND_MANIFEST_MALFORMED if role == "manifest" else KIND_SUMMARY_MALFORMED,
                 "detail": {"error": str(exc)}}
            )
            return KIND_MANIFEST_MALFORMED if role == "manifest" else KIND_SUMMARY_MALFORMED

        if role == "manifest":
            entries = doc.get("members") if isinstance(doc, dict) else None
            if not isinstance(entries, list) or not entries:
                findings.append({"kind": KIND_MANIFEST_MALFORMED,
                                 "detail": {"error": "manifest.members must be a non-empty list"}})
                return KIND_MANIFEST_MALFORMED
            seen: Dict[str, int] = {}
            failure: Optional[str] = None
            for entry in entries:
                if not isinstance(entry, dict) or "name" not in entry:
                    findings.append({"kind": KIND_MANIFEST_MALFORMED,
                                     "detail": {"entry": entry}})
                    failure = failure or KIND_MANIFEST_MALFORMED
                    continue
                name = entry["name"]
                seen[name] = seen.get(name, 0) + 1
                if seen[name] > 1:
                    findings.append({"kind": KIND_MANIFEST_DUPLICATE,
                                     "detail": {"member": name}})
                    failure = failure or KIND_MANIFEST_DUPLICATE
                if name not in contract.members:
                    findings.append({"kind": KIND_UNKNOWN_MEMBER,
                                     "detail": {"member": name}})
                rel = entry.get("path", name)
                try:
                    safe_resolve(self.inbound_root, rel)
                except Exception as exc:
                    findings.append({"kind": KIND_PATH_ESCAPE,
                                     "detail": {"member": name, "path": rel, "error": str(exc)}})
                    failure = failure or KIND_PATH_ESCAPE
            if failure:
                return failure
        return None

    def _expected_digests(self, contract, batch_id, member_name, inline) -> Dict[str, str]:
        expected: Dict[str, str] = dict(inline)
        # A summary member may carry authoritative digests for other members.
        for arrival in self.store.arrivals_for(batch_id):
            if arrival["valid_arrival"] != 1:
                continue
            spec = contract.members.get(arrival["member_name"])
            if not spec or spec["role"] != "summary" or not arrival["blob_sha256"]:
                continue
            try:
                with open(self.store.blob_path(arrival["blob_sha256"]), "rb") as fh:
                    summary = json.loads(fh.read().decode("utf-8"))
            except Exception:
                continue
            digests = (
                summary.get("digests", {}).get(member_name)
                if isinstance(summary, dict)
                else None
            )
            if isinstance(digests, dict):
                for alg, val in digests.items():
                    expected.setdefault(alg, val)
        # Contract-level expected digests are the weakest source.
        for alg, val in contract.members[member_name].get("expected_digests", {}).items():
            expected.setdefault(alg, val)
        return expected

    @staticmethod
    def _verify_digests(spec, expected, raw_digests, payload_digests, gz, findings) -> bool:
        bad = False
        for alg in spec["digest_algorithms"]:
            if alg not in expected:
                continue
            want = expected[alg].lower()
            got = (payload_digests if gz else raw_digests).get(alg, "").lower()
            if got != want:
                bad = True
                findings.append(
                    {
                        "kind": KIND_DIGEST_MISMATCH,
                        "detail": {
                            "algorithm": alg,
                            "expected": want,
                            "actual": got,
                            "basis": "decompressed" if gz else "raw",
                        },
                    }
                )
        return bad

    # ------------------------------------------------------------- status
    def _effective_members(self, contract: CompiledContract, batch_id: str):
        """Return (latest effective arrival per member, first-seen order).

        ``latest`` always points at the most recent *valid* arrival row; the
        binding builder separately keeps the first valid arrival seq so that
        idempotent re-sends never alter the recorded arrival order.
        """
        latest: Dict[str, sqlite3.Row] = {}
        first: Dict[str, sqlite3.Row] = {}
        order: List[Tuple[str, int]] = []
        for arrival in self.store.arrivals_for(batch_id):
            if arrival["valid_arrival"] != 1:
                continue
            name = arrival["member_name"]
            if name not in latest:
                first[name] = arrival
                order.append((name, arrival["seq"]))
            latest[name] = arrival
        return latest, order, first

    def _build_binding(self, contract: CompiledContract, latest, order, first) -> Dict[str, Any]:
        binding: Dict[str, Any] = {}
        for name, first_seq in sorted(order, key=lambda x: x[1]):
            arrival = latest[name]
            raw = json.loads(arrival["raw_digests_json"])
            payload = json.loads(arrival["payload_digests_json"])
            # arrival_seq reflects the content actually bound: for a changed
            # member it is the new arrival's seq; for an idempotent re-send
            # the blob is unchanged so we retain the original arrival seq.
            content_seq = (
                arrival["seq"]
                if first[name]["blob_sha256"] != arrival["blob_sha256"]
                else first[name]["seq"]
            )
            binding[name] = {
                "arrival_seq": content_seq,
                "first_arrival_seq": first_seq,
                "blob_sha256": arrival["blob_sha256"],
                "size_bytes": arrival["size_bytes"],
                "compression": arrival["compression"],
                "raw_digests": raw,
                "payload_digests": payload,
                "required": bool(contract.members[name]["required"]),
                "role": contract.members[name]["role"],
            }
        return binding

    def get_status(self, contract: CompiledContract, key_values: Mapping[str, Any]) -> Dict[str, Any]:
        batch_id, _ = batch_identity(contract, key_values)
        batch = self.store.get_batch(batch_id)
        if batch is None:
            return {"batch_id": batch_id, "state": "UNKNOWN", "members": {}, "missing": [],
                    "waiting": [], "closures": [], "validation": []}
        now = self._now()
        latest, order, _first = self._effective_members(contract, batch_id)
        deadline = parse_utc(batch["deadline"])
        late_end = parse_utc(batch["late_window_end"])
        required = set(contract.required_names())
        optional = set(contract.optional_names())
        present = set(latest)
        missing_required = sorted(required - present)
        missing_optional = sorted(optional - present)

        if now < deadline:
            phase = "BEFORE_DEADLINE"
        elif now <= late_end:
            phase = "LATE_WINDOW"
        else:
            phase = "WINDOW_CLOSED"

        closures = [dict(r) for r in self.store.closures_for(batch_id)]
        validation = [dict(r) for r in self.store.validation_for(batch_id)]
        return {
            "batch_id": batch_id,
            "state": batch["current_state"],
            "phase": phase,
            "deadline": deadline.isoformat(),
            "late_window_end": late_end.isoformat(),
            "now": now.isoformat(),
            "current_revision": batch["current_revision"],
            "members": {
                name: {
                    "seq": row["seq"],
                    "blob_sha256": row["blob_sha256"],
                    "size_bytes": row["size_bytes"],
                    "received_at": row["received_at"],
                    "compression": row["compression"],
                }
                for name, row in latest.items()
            },
            "arrival_order": [name for name, _ in sorted(order, key=lambda x: x[1])],
            "missing_required": missing_required,
            "missing_optional": missing_optional,
            # Before the deadline missing files are *waiting*, never rejected.
            "waiting": sorted((required | optional) - present) if phase != "WINDOW_CLOSED" else [],
            "quarantined": [
                {
                    "member": r["member_name"],
                    "seq": r["seq"],
                    "reason": r["quarantine_reason"],
                    "received_at": r["received_at"],
                }
                for r in self.store.arrivals_for(batch_id)
                if r["valid_arrival"] == 0
            ],
            "closures": closures,
            "validation": validation,
        }

    # ------------------------------------------------ closure / revision
    def evaluate_closure(
        self, contract: CompiledContract, key_values: Mapping[str, Any],
        at: Optional[datetime] = None,
    ) -> Dict[str, Any]:
        """Evaluate whether the batch can close.  Never closes early."""
        now = (at or self._now())
        self.store.begin()
        try:
            batch_id, _ = batch_identity(contract, key_values)
            batch = self.store.get_batch(batch_id)
            if batch is None:
                self.store.rollback()
                return {"batch_id": batch_id, "closed": False, "reason": "no_arrivals"}
            deadline = parse_utc(batch["deadline"])
            late_end = parse_utc(batch["late_window_end"])

            # Rule: a file arriving first must never trigger early closure.
            if now < deadline:
                self.store.commit()
                return {
                    "batch_id": batch_id, "closed": False,
                    "reason": "before_deadline",
                    "deadline": deadline.isoformat(),
                }

            latest, order, first = self._effective_members(contract, batch_id)
            binding = self._build_binding(contract, latest, order, first)
            binding_sig = self._binding_signature(contract, binding)
            missing_required = sorted(set(contract.required_names()) - set(latest))
            prior = self.store.latest_closure(batch_id)

            # Record manifest-missing findings once the deadline has passed.
            if missing_required and (
                prior is None or json.loads(prior["missing_json"]) != missing_required
            ):
                for name in missing_required:
                    self.store.add_validation(
                        batch_id, KIND_MANIFEST_MISSING, name,
                        {"at": now.isoformat(), "deadline": deadline.isoformat()},
                    )

            decide: Optional[Tuple[str, int, List[str], Dict[str, Any]]] = None
            if prior is None:
                # First decision is only taken at/after the deadline.
                if not missing_required:
                    decide = ("CLOSED", 1, [], {})
                elif contract.on_missing_required == "publish_with_missing":
                    # Partial publish happens immediately after the deadline;
                    # late files can still turn it into a complete revision.
                    decide = ("CLOSED_PARTIAL", 1, missing_required,
                              {"policy": "publish_with_missing",
                               "missing": missing_required})
                elif now > late_end:
                    decide = ("REJECTED", 1, missing_required,
                              {"policy": "reject", "missing": missing_required})
            else:
                prior_sig = prior["binding_sig"]
                prior_missing = json.loads(prior["missing_json"])
                if binding_sig != prior_sig or prior_missing != missing_required:
                    if now <= late_end:
                        # A revision can still be produced inside the window.
                        if not missing_required:
                            decide = ("CLOSED", prior["revision"] + 1, [], {})
                        elif contract.on_missing_required == "publish_with_missing":
                            decide = ("CLOSED_PARTIAL", prior["revision"] + 1,
                                      missing_required,
                                      {"policy": "publish_with_missing",
                                       "missing": missing_required})
                        # reject-policy batches with missing members simply
                        # keep waiting until the window closes.
                    else:
                        # Window just closed: finalise the decision.
                        if not missing_required:
                            decide = ("CLOSED", prior["revision"] + 1, [], {})
                        elif contract.on_missing_required == "reject":
                            decide = ("REJECTED", prior["revision"] + 1,
                                      missing_required,
                                      {"policy": "reject", "missing": missing_required})
                        else:
                            decide = ("CLOSED_PARTIAL", prior["revision"] + 1,
                                      missing_required,
                                      {"policy": "publish_with_missing",
                                       "missing": missing_required})

            if decide is None:
                self.store.commit()
                return {
                    "batch_id": batch_id, "closed": False,
                    "reason": "no_change_or_waiting",
                    "latest_revision": prior["revision"] if prior else 0,
                    "state": prior["state"] if prior else "WAITING",
                    "missing_required": missing_required,
                }

            state, revision, missing, decision_detail = decide
            impact = self._impact(prior, binding)
            evidence_hash = self._evidence_hash(
                batch, prior, revision, state, contract, binding, binding_sig,
                missing, impact, now,
            )
            closure_id = self.store.insert_closure(
                {
                    "batch_id": batch_id,
                    "revision": revision,
                    "state": state,
                    "contract_digest": contract.digest,
                    "binding_json": canonical_json(binding),
                    "binding_sig": binding_sig,
                    "missing_json": canonical_json(missing),
                    "parent_revision": prior["revision"] if prior else None,
                    "impact_json": canonical_json(impact),
                    "decided_at": now.isoformat(),
                    "prev_evidence_hash": prior["evidence_hash"] if prior else None,
                    "evidence_hash": evidence_hash,
                }
            )
            self.store.update_batch_state(batch_id, state, revision)
            self.store.append_event(
                batch_id, "closure_decided",
                {
                    "revision": revision,
                    "state": state,
                    "binding_sig": binding_sig,
                    "missing": missing,
                    "parent_revision": prior["revision"] if prior else None,
                    "impact": impact,
                    "decision_detail": decision_detail,
                    "evidence_hash": evidence_hash,
                    "closure_id": closure_id,
                },
            )
            if state == "REJECTED":
                for name in missing:
                    self.store.add_validation(
                        batch_id, KIND_REJECTED_MISSING, name,
                        {"revision": revision, "at": now.isoformat()},
                    )
            self.store.commit()
            return {
                "batch_id": batch_id, "closed": True, "revision": revision,
                "state": state, "missing": missing, "impact": impact,
                "binding_sig": binding_sig, "evidence_hash": evidence_hash,
                "parent_revision": prior["revision"] if prior else None,
            }
        except Exception:
            self.store.rollback()
            raise

    def _binding_signature(self, contract: CompiledContract, binding: Dict[str, Any]) -> str:
        payload = canonical_bytes(
            {"contract_digest": contract.digest, "binding": binding}
        )
        return hmac.new(self.store.hmac_secret, payload, hashlib.sha256).hexdigest()

    @staticmethod
    def _impact(prior, new_binding: Dict[str, Any]) -> Dict[str, Any]:
        if prior is None:
            return {
                "added": sorted(new_binding.keys()),
                "removed": [],
                "changed": [],
                "summary": "initial closure",
            }
        old_binding = json.loads(prior["binding_json"])
        old_names, new_names = set(old_binding), set(new_binding)
        added = sorted(new_names - old_names)
        removed = sorted(old_names - new_names)
        changed = sorted(
            name for name in old_names & new_names
            if old_binding[name]["blob_sha256"] != new_binding[name]["blob_sha256"]
            or old_binding[name]["arrival_seq"] != new_binding[name]["arrival_seq"]
        )
        parts = []
        if added:
            parts.append(f"added {len(added)} member(s): {', '.join(added)}")
        if removed:
            parts.append(f"removed {len(removed)} member(s): {', '.join(removed)}")
        if changed:
            parts.append(f"content changed for {len(changed)} member(s): {', '.join(changed)}")
        return {
            "added": added,
            "removed": removed,
            "changed": changed,
            "summary": "; ".join(parts) if parts else "no material change",
        }

    def _evidence_hash(self, batch, prior, revision, state, contract, binding,
                       binding_sig, missing, impact, now) -> str:
        material = canonical_bytes(
            {
                "batch_id": batch["id"],
                "revision": revision,
                "state": state,
                "contract_digest": contract.digest,
                "binding": binding,
                "binding_sig": binding_sig,
                "missing": missing,
                "impact": impact,
                "decided_at": now.isoformat(),
                "prev_evidence_hash": prior["evidence_hash"] if prior else None,
            }
        )
        return hashlib.sha256(material).hexdigest()

    # ---------------------------------------------------------------- claims
    def claim(
        self, contract: CompiledContract, key_values: Mapping[str, Any],
        processor: str = "processor", guard_old_revisions: bool = True,
        at: Optional[datetime] = None,
    ) -> Dict[str, Any]:
        """Claim the latest *closed* revision.  Unclosed inputs cannot be claimed."""
        now = (at or self._now())
        batch_id, _ = batch_identity(contract, key_values)
        self.store.begin()
        try:
            batch = self.store.get_batch(batch_id)
            if batch is None:
                self.store.rollback()
                raise NotClosed("batch has no arrivals and no closure")
            closure = self.store.latest_closure(batch_id)
            if closure is None or closure["state"] not in CLAIMABLE_STATES:
                self.store.rollback()
                raise NotClosed(
                    f"batch is not in a claimable closed state "
                    f"(state={closure['state'] if closure else 'WAITING'})"
                )
            existing = self.store.get_claim(batch_id, closure["revision"])
            if existing is not None:
                self.store.rollback()
                raise AlreadyClaimed(
                    f"revision {closure['revision']} already claimed "
                    f"(status={existing['status']})"
                )
            if guard_old_revisions:
                active = self.store.active_claims(batch_id)
                older = [r for r in active if r["revision"] < closure["revision"]]
                if older:
                    self.store.rollback()
                    raise AlreadyClaimed(
                        "older revision(s) still being processed: "
                        + ", ".join(str(r["revision"]) for r in older)
                        + "; old inputs remain immutable, claim after they complete"
                    )

            token_payload = {
                "batch_id": batch_id,
                "revision": closure["revision"],
                "contract_digest": contract.digest,
                "binding_sig": closure["binding_sig"],
                "processor": processor,
                "issued_at": now.isoformat(),
            }
            token = self._sign_token(token_payload)
            self.store.insert_claim(
                {
                    "batch_id": batch_id,
                    "revision": closure["revision"],
                    "token": token,
                    "status": "CLAIMED",
                    "claimed_by": processor,
                    "claimed_at": now.isoformat(),
                    "completed_at": None,
                    "binding_sig": closure["binding_sig"],
                }
            )
            self.store.append_event(
                batch_id, "claim_granted",
                {"revision": closure["revision"], "processor": processor,
                 "binding_sig": closure["binding_sig"]},
            )
            self.store.commit()
            return {
                "token": token,
                "batch_id": batch_id,
                "revision": closure["revision"],
                "state": closure["state"],
                "binding_sig": closure["binding_sig"],
                "contract_digest": contract.digest,
                "missing": json.loads(closure["missing_json"]),
            }
        except Exception:
            self.store.rollback()
            raise

    def _sign_token(self, payload: Dict[str, Any]) -> str:
        body = canonical_bytes(payload)
        mac = hmac.new(self.store.hmac_secret, body, hashlib.sha256).hexdigest()
        import base64

        return (
            base64.urlsafe_b64encode(body).rstrip(b"=").decode("ascii")
            + "."
            + mac
        )

    def verify_token(self, token: str) -> Dict[str, Any]:
        import base64

        try:
            body_b64, mac = token.split(".", 1)
            padded = body_b64 + "=" * (-len(body_b64) % 4)
            body = base64.urlsafe_b64decode(padded.encode("ascii"))
        except Exception as exc:
            raise InvalidToken(f"malformed token: {exc}") from exc
        expected = hmac.new(self.store.hmac_secret, body, hashlib.sha256).hexdigest()
        if not hmac.compare_digest(expected, mac):
            raise InvalidToken("token signature does not match")
        payload = json.loads(body.decode("utf-8"))
        row = self.store.claim_by_token(token)
        if row is None:
            raise InvalidToken("token is not registered to a claim")
        closure = self.store.closure_at(payload["batch_id"], payload["revision"])
        if closure is None or closure["binding_sig"] != payload["binding_sig"]:
            raise InvalidToken("token is no longer bound to the stored closure")
        if closure["contract_digest"] != payload["contract_digest"]:
            raise InvalidToken("contract version mismatch")
        return {"payload": payload, "claim_status": row["status"], "valid": True}

    def complete_claim(self, token: str, status: str = "COMPLETED") -> Dict[str, Any]:
        if status not in ("COMPLETED", "FAILED"):
            raise ValueError("claim completion status must be COMPLETED or FAILED")
        info = self.verify_token(token)
        self.store.begin()
        try:
            self.store.complete_claim(token, status)
            self.store.append_event(
                info["payload"]["batch_id"], "claim_completed",
                {"revision": info["payload"]["revision"], "status": status},
            )
            self.store.commit()
        except Exception:
            self.store.rollback()
            raise
        return {"token": token, "status": status, **info["payload"]}

    def list_claims(self, contract: CompiledContract, key_values: Mapping[str, Any]):
        batch_id, _ = batch_identity(contract, key_values)
        return [dict(r) for r in self.store.conn.execute(
            "SELECT * FROM claims WHERE batch_id=? ORDER BY id", (batch_id,)
        ).fetchall()]

    # -------------------------------------------------------------- evidence
    def export_evidence(
        self, contract: CompiledContract, key_values: Mapping[str, Any], out_dir
    ) -> Path:
        """Export a self-describing, replayable closure evidence bundle."""
        batch_id, _ = batch_identity(contract, key_values)
        out = Path(out_dir)
        out.mkdir(parents=True, exist_ok=True)
        blob_dir = out / "blobs"
        blob_dir.mkdir(exist_ok=True)

        batch = self.store.get_batch(batch_id)
        events = [dict(r) for r in self.store.events_for(batch_id)]
        closures = [dict(r) for r in self.store.closures_for(batch_id)]
        arrivals = [dict(r) for r in self.store.arrivals_for(batch_id)]
        validation = [dict(r) for r in self.store.validation_for(batch_id)]
        claims = [dict(r) for r in self.store.conn.execute(
            "SELECT id, batch_id, revision, status, claimed_by, claimed_at, "
            "completed_at, binding_sig FROM claims WHERE batch_id=? ORDER BY id",
            (batch_id,),
        ).fetchall()]

        (out / "contract.json").write_text(
            json.dumps(contract.to_dict(), indent=2, ensure_ascii=False), encoding="utf-8"
        )
        (out / "events.json").write_text(json.dumps(events, indent=2), encoding="utf-8")
        (out / "closures.json").write_text(json.dumps(closures, indent=2), encoding="utf-8")
        (out / "arrivals.json").write_text(json.dumps(arrivals, indent=2), encoding="utf-8")
        (out / "validation.json").write_text(json.dumps(validation, indent=2), encoding="utf-8")
        (out / "claims.json").write_text(json.dumps(claims, indent=2), encoding="utf-8")

        # Copy every referenced blob; the bundle is self contained.
        blob_manifest = []
        for arrival in arrivals:
            sha = arrival["blob_sha256"]
            if not sha:
                continue
            src = self.store.blob_path(sha)
            dst = blob_dir / sha[:2] / sha
            dst.parent.mkdir(parents=True, exist_ok=True)
            if not dst.exists():
                shutil.copy2(src, dst)
            with open(dst, "rb") as fh:
                actual = hashlib.sha256(fh.read()).hexdigest()
            blob_manifest.append(
                {
                    "member": arrival["member_name"],
                    "seq": arrival["seq"],
                    "sha256": sha,
                    "size": arrival["size_bytes"],
                    "verified": actual == sha,
                }
            )

        chain_ok = self._verify_evidence_chain(closures)
        manifest = {
            "batch_id": batch_id,
            "batch": dict(batch) if batch else None,
            "contract_digest": contract.digest,
            "contract_canonical_sha256": hashlib.sha256(
                contract.canonical_json.encode("utf-8")
            ).hexdigest(),
            "event_count": len(events),
            "closure_count": len(closures),
            "blob_manifest": blob_manifest,
            "evidence_chain_ok": chain_ok,
            "exported_at": utcnow_iso(),
        }
        (out / "evidence_manifest.json").write_text(
            json.dumps(manifest, indent=2), encoding="utf-8"
        )
        return out

    @staticmethod
    def _verify_evidence_chain(closures) -> bool:
        prev = None
        for row in closures:
            if row["prev_evidence_hash"] != prev:
                return False
            prev = row["evidence_hash"]
        return True

    def replay(self, contract: CompiledContract, key_values: Mapping[str, Any]) -> Dict[str, Any]:
        """Re-derive every state transition from the event log and compare it
        against the persisted decisions (independent recomputation)."""
        from .state_machine import ReferenceMachine

        batch_id, _ = batch_identity(contract, key_values)
        events = [
            {
                "seq": r["seq"],
                "type": r["event_type"],
                "payload": json.loads(r["payload"]),
                "created_at": r["created_at"],
            }
            for r in self.store.events_for(batch_id)
        ]
        batch = self.store.get_batch(batch_id)
        deadline = parse_utc(batch["deadline"]) if batch else None
        machine = ReferenceMachine(
            contract, deadline, hmac_secret=self.store.hmac_secret
        )
        trace = machine.replay(events)
        stored = [
            {
                "revision": r["revision"],
                "state": r["state"],
                "binding_sig": r["binding_sig"],
                "evidence_hash": r["evidence_hash"],
            }
            for r in self.store.closures_for(batch_id)
        ]
        derived = [
            {
                "revision": c["revision"],
                "state": c["state"],
                "binding_sig": self._binding_signature(contract, c["binding"]),
                "evidence_hash": None,  # machine recomputes material independently
            }
            for c in trace["closures"]
        ]
        # Compare states, revisions and binding signatures (the durable
        # invariants); evidence hash chain continuity is checked separately.
        match = (
            len(stored) == len(derived)
            and all(
                s["revision"] == d["revision"]
                and s["state"] == d["state"]
                and s["binding_sig"] == d["binding_sig"]
                for s, d in zip(stored, derived)
            )
        and self._verify_evidence_chain(self.store.closures_for(batch_id))
        )
        return {
            "batch_id": batch_id,
            "matches": match,
            "event_count": len(events),
            "stored_closures": stored,
            "derived_closures": derived,
            "derived_states": trace["states"],
        }

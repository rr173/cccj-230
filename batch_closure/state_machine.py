"""Independent reference state machine.

This module deliberately re-implements the closure rules instead of calling
the service, so the tests can cross-check two independent derivations of the
same truth.  It only shares pure helpers (contract model, canonical JSON,
digest primitives) with the service -- never its transition code.

Inputs are plain event dictionaries as emitted by the service's event log:

    {"type": "file_received", "payload": {"member", "seq", "blob_sha256",
        "size", "raw_digests", "payload_digests", "compression",
        "content_changed", ...}, "created_at": ...}
    {"type": "file_quarantined", "payload": {"member", "seq", "reason", ...}}
    {"type": "batch_opened", ...}
    {"type": "closure_decided", ...}   # ignored for derivation, used for audit

The machine is driven by a timeline of instants (each event instant plus the
deadline and the late-window end).  At every instant it independently decides
which state the batch *should* be in and whether a new revision should exist.
"""

from __future__ import annotations

import hashlib
import hmac
from datetime import datetime
from typing import Any, Dict, List, Mapping, Optional

from .contract import CompiledContract, canonical_bytes, parse_utc


class ReferenceMachine:
    def __init__(
        self,
        contract: CompiledContract,
        deadline: Optional[datetime] = None,
        hmac_secret: Optional[bytes] = None,
    ):
        self.contract = contract
        self.deadline = deadline
        self.late_end = None
        if deadline is not None:
            from .contract import late_window_end

            self.late_end = late_window_end(contract, deadline)
        # Secret is only needed when the test wants to compare binding
        # signatures with the service's HMAC values.
        self._secret = hmac_secret or b"reference-machine-secret"
        self.latest: Dict[str, Dict[str, Any]] = {}
        self.first_seq: Dict[str, int] = {}
        self.closures: List[Dict[str, Any]] = []
        self.states: List[Dict[str, Any]] = []
        self.validation: List[Dict[str, Any]] = []

    # ------------------------------------------------------------------ core
    def _apply_reception(self, event: Mapping[str, Any], at: datetime) -> None:
        p = event["payload"]
        member = p["member"]
        if member not in self.contract.members:
            self.validation.append(
                {"at": at.isoformat(), "kind": "unknown_member", "member": member}
            )
            return
        # Idempotent re-send of identical content must not rewrite the
        # binding: keep the original arrival sequence, mirroring the service.
        if member in self.latest and self.latest[member]["blob_sha256"] == p["blob_sha256"]:
            return
        if member not in self.first_seq:
            self.first_seq[member] = p["seq"]
        self.latest[member] = {
            "arrival_seq": p["seq"],
            "first_arrival_seq": self.first_seq[member],
            "blob_sha256": p["blob_sha256"],
            "size_bytes": p["size"],
            "compression": p.get("compression", "none"),
            "raw_digests": p.get("raw_digests", {}),
            "payload_digests": p.get("payload_digests", {}),
            "required": bool(self.contract.members[member]["required"]),
            "role": self.contract.members[member]["role"],
        }

    def _apply_quarantine(self, event: Mapping[str, Any], at: datetime) -> None:
        p = event["payload"]
        self.validation.append(
            {"at": at.isoformat(), "kind": p["reason"], "member": p.get("member")}
        )

    def _binding(self) -> Dict[str, Any]:
        return {
            name: dict(self.latest[name])
            for name in sorted(self.latest, key=lambda n: self.first_seq[n])
        }

    def _signature(self, binding: Mapping[str, Any]) -> str:
        payload = canonical_bytes(
            {"contract_digest": self.contract.digest, "binding": binding}
        )
        return hmac.new(self._secret, payload, hashlib.sha256).hexdigest()

    def _missing_required(self) -> List[str]:
        return sorted(set(self.contract.required_names()) - set(self.latest))

    def _decide(self, at: datetime) -> Optional[Dict[str, Any]]:
        """Independently compute whether a closure decision exists at ``at``."""
        if self.deadline is None or at < self.deadline:
            # Early closure is impossible no matter how many files arrived.
            return None
        missing = self._missing_required()
        binding = self._binding()
        sig = self._signature(binding)
        prior = self.closures[-1] if self.closures else None

        if prior is None:
            if not missing:
                state = "CLOSED"
            elif self.contract.on_missing_required == "publish_with_missing":
                # Partial publication immediately after the deadline.
                state = "CLOSED_PARTIAL"
            elif at <= self.late_end:
                return None  # reject policy: keep waiting inside the window
            else:
                state = "REJECTED"
            return {
                "revision": 1,
                "state": state,
                "binding": binding,
                "binding_sig": sig,
                "missing": missing,
                "parent_revision": None,
                "impact": {
                    "added": sorted(binding.keys()),
                    "removed": [],
                    "changed": [],
                    "summary": "initial closure",
                },
            }

        # A later revision requires a material change inside the window, or a
        # finalisation when the window closes.
        material_change = (
            prior["binding_sig"] != sig or prior["missing"] != missing
        )
        if not material_change:
            return None
        if at <= self.late_end:
            if not missing:
                state = "CLOSED"
            elif self.contract.on_missing_required == "publish_with_missing":
                state = "CLOSED_PARTIAL"
            else:
                return None
        else:
            if not missing:
                state = "CLOSED"
            elif self.contract.on_missing_required == "reject":
                state = "REJECTED"
            else:
                state = "CLOSED_PARTIAL"
        old_binding = prior["binding"]
        added = sorted(set(binding) - set(old_binding))
        removed = sorted(set(old_binding) - set(binding))
        changed = sorted(
            name
            for name in set(binding) & set(old_binding)
            if binding[name]["blob_sha256"] != old_binding[name]["blob_sha256"]
            or binding[name]["arrival_seq"] != old_binding[name]["arrival_seq"]
        )
        return {
            "revision": prior["revision"] + 1,
            "state": state,
            "binding": binding,
            "binding_sig": sig,
            "missing": missing,
            "parent_revision": prior["revision"],
            "impact": {
                "added": added,
                "removed": removed,
                "changed": changed,
                "summary": self._impact_summary(added, removed, changed),
            },
        }

    @staticmethod
    def _impact_summary(added, removed, changed) -> str:
        parts = []
        if added:
            parts.append(f"added {len(added)} member(s): {', '.join(added)}")
        if removed:
            parts.append(f"removed {len(removed)} member(s): {', '.join(removed)}")
        if changed:
            parts.append(
                f"content changed for {len(changed)} member(s): {', '.join(changed)}"
            )
        return "; ".join(parts) if parts else "no material change"

    def tick(self, at: datetime) -> Dict[str, Any]:
        decision = self._decide(at)
        if decision is not None:
            last = self.closures[-1] if self.closures else None
            if (
                last is None
                or last["binding_sig"] != decision["binding_sig"]
                or last["state"] != decision["state"]
            ):
                self.closures.append(decision)
        state = self.closures[-1]["state"] if self.closures else "WAITING"
        snapshot = {
            "at": at.isoformat(),
            "state": state,
            "revision": self.closures[-1]["revision"] if self.closures else 0,
            "missing_required": self._missing_required(),
            "present": sorted(self.latest),
        }
        self.states.append(snapshot)
        return snapshot

    def replay(self, events: List[Mapping[str, Any]]) -> Dict[str, Any]:
        timeline: List[datetime] = []
        per_instant: Dict[datetime, List[Mapping[str, Any]]] = {}
        for ev in events:
            at = parse_utc(ev["created_at"])
            timeline.append(at)
            per_instant.setdefault(at, []).append(ev)
        if self.deadline is not None:
            timeline.append(self.deadline)
            if self.late_end != self.deadline:
                timeline.append(self.late_end)

        last_closure_sig = None
        for at in sorted(set(timeline)):
            for ev in per_instant.get(at, []):
                if ev["type"] == "file_received":
                    self._apply_reception(ev, at)
                elif ev["type"] == "file_quarantined":
                    self._apply_quarantine(ev, at)
            # Decide against exactly the events visible at this instant.
            decision = self._decide(at)
            if decision is not None and decision["binding_sig"] != last_closure_sig:
                # Materially new revision at this point in the timeline.
                self.closures.append(decision)
                last_closure_sig = decision["binding_sig"]
            elif decision is not None and (
                not self.closures
                or self.closures[-1]["state"] != decision["state"]
            ):
                self.closures.append(decision)
                last_closure_sig = decision["binding_sig"]
            state = self.closures[-1]["state"] if self.closures else "WAITING"
            self.states.append({
                "at": at.isoformat(),
                "state": state,
                "revision": self.closures[-1]["revision"] if self.closures else 0,
                "missing_required": self._missing_required(),
                "present": sorted(self.latest),
            })
        return {"closures": list(self.closures), "states": list(self.states),
                "validation": list(self.validation)}

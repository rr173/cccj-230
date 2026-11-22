"""Declarative arrival contract compiler.

A contract declares:

* ``contract_id`` / ``version``           -- identity of the contract version
* ``batch_key``                            -- fields that identify a batch
* ``business_date_field``                  -- which key field is the business day
* ``members``                              -- required / optional member catalog
* ``max_file_size``                        -- per-file size cap (bytes)
* ``deadline``                             -- arrival deadline (relative or abs.)
* ``late_window_minutes``                  -- how long late files may revise
* ``on_missing_required``                  -- reject | publish_with_missing
* ``supported_compression``                -- allowed compression names ("gzip")

The compiler validates the declaration, normalises it, freezes it into an
immutable object and produces a canonical serialisation whose sha256 digest is
the *contract version hash* that every closure/claim is bound to.
"""

from __future__ import annotations

import copy
import hashlib
import json
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Mapping, Optional, Tuple

from .errors import BadContract

DEFAULT_LATE_WINDOW_MINUTES = 120
DEFAULT_MAX_FILE_SIZE = 256 * 1024 * 1024
ALLOWED_MISSING_POLICIES = {"reject", "publish_with_missing"}
ALLOWED_COMPRESSION = {"gzip"}
KNOWN_ALGORITHMS = {"sha256", "sha1", "md5", "sha512", "sha384"}


def canonical_json(obj: Any) -> str:
    """Deterministic JSON: sorted keys, compact separators, stable unicode."""
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def canonical_bytes(obj: Any) -> bytes:
    return canonical_json(obj).encode("utf-8")


def parse_utc(value: str) -> datetime:
    """Parse an ISO-8601 timestamp; naive values are treated as UTC."""
    if not isinstance(value, str):
        raise BadContract(f"timestamp must be a string, got {type(value).__name__}")
    text = value.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(text)
    except ValueError as exc:
        raise BadContract(f"unparseable timestamp {value!r}: {exc}") from exc
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _validate_member(name: str, spec: Any) -> Dict[str, Any]:
    if not isinstance(name, str) or not name:
        raise BadContract("member names must be non-empty strings")
    if not isinstance(spec, Mapping):
        raise BadContract(f"member {name!r} spec must be an object")
    required = bool(spec.get("required", False))
    role = spec.get("role", "data")
    if role not in ("data", "manifest", "summary"):
        raise BadContract(f"member {name!r} has unknown role {role!r}")
    algorithms = spec.get("digest_algorithms") or ["sha256"]
    if not isinstance(algorithms, list) or not algorithms:
        raise BadContract(f"member {name!r} digest_algorithms must be a list")
    for alg in algorithms:
        if alg not in KNOWN_ALGORITHMS:
            raise BadContract(f"member {name!r} uses unsupported digest {alg!r}")
    compression = spec.get("compression", "none")
    if compression not in ("none", "gzip"):
        raise BadContract(f"member {name!r} uses unsupported compression {compression!r}")
    if spec.get("max_size") is not None and (
        not isinstance(spec["max_size"], int) or spec["max_size"] <= 0
    ):
        raise BadContract(f"member {name!r} max_size must be a positive int")
    return {
        "name": name,
        "required": required,
        "role": role,
        "digest_algorithms": list(algorithms),
        "compression": compression,
        "max_size": spec.get("max_size"),
        "expected_digests": dict(spec.get("expected_digests") or {}),
    }


def _compile_deadline(decl: Mapping[str, Any]) -> Dict[str, Any]:
    deadline = decl.get("deadline")
    if not isinstance(deadline, Mapping) or "type" not in deadline:
        raise BadContract("contract.deadline must be an object with a type")
    dtype = deadline["type"]
    if dtype == "relative":
        offset_days = deadline.get("offset_days", 0)
        hour = deadline.get("hour", 23)
        minute = deadline.get("minute", 59)
        if not isinstance(offset_days, int):
            raise BadContract("deadline.offset_days must be an integer")
        if not (0 <= hour <= 23 and 0 <= minute <= 59):
            raise BadContract("deadline hour/minute out of range")
        return {
            "type": "relative",
            "offset_days": offset_days,
            "hour": hour,
            "minute": minute,
        }
    if dtype == "absolute":
        moment = parse_utc(deadline.get("at", ""))
        return {"type": "absolute", "at": moment.isoformat()}
    raise BadContract(f"unknown deadline type {dtype!r}")


@dataclass(frozen=True)
class CompiledContract:
    contract_id: str
    version: str
    raw: Mapping[str, Any]
    members: Mapping[str, Mapping[str, Any]] = field(default_factory=dict)
    batch_key: Tuple[str, ...] = ()
    business_date_field: str = "business_date"
    max_file_size: int = DEFAULT_MAX_FILE_SIZE
    deadline_spec: Mapping[str, Any] = field(default_factory=dict)
    late_window_minutes: int = DEFAULT_LATE_WINDOW_MINUTES
    on_missing_required: str = "reject"
    supported_compression: Tuple[str, ...] = ()
    canonical_json: str = ""
    digest: str = ""

    def member(self, name: str) -> Mapping[str, Any]:
        if name not in self.members:
            from .errors import UnknownMember

            raise UnknownMember(f"member {name!r} is not declared in contract")
        return self.members[name]

    def member_limit(self, name: str) -> int:
        spec = self.members[name]
        return int(spec["max_size"] or self.max_file_size)

    def required_names(self) -> List[str]:
        return sorted(n for n, s in self.members.items() if s["required"])

    def optional_names(self) -> List[str]:
        return sorted(n for n, s in self.members.items() if not s["required"])

    def digest_algorithms(self) -> List[str]:
        algs = set()
        for spec in self.members.values():
            algs.update(spec["digest_algorithms"])
        return sorted(algs)

    def to_dict(self) -> Dict[str, Any]:
        return copy.deepcopy(self.raw)


def compile_contract(declaration: Mapping[str, Any]) -> CompiledContract:
    """Validate and compile a contract declaration into a frozen contract."""
    if not isinstance(declaration, Mapping):
        raise BadContract("contract declaration must be an object")

    contract_id = declaration.get("contract_id")
    if not isinstance(contract_id, str) or not contract_id:
        raise BadContract("contract_id is required and must be a non-empty string")
    version = declaration.get("version", "1")
    if not isinstance(version, str) or not version:
        raise BadContract("version must be a non-empty string")

    members_decl = declaration.get("members")
    if not isinstance(members_decl, Mapping) or not members_decl:
        raise BadContract("contract must declare at least one member")
    members = {name: _validate_member(name, spec) for name, spec in members_decl.items()}
    roles = [s["role"] for s in members.values()]
    if roles.count("manifest") > 1 or roles.count("summary") > 1:
        raise BadContract("at most one manifest and one summary member are allowed")

    batch_key = declaration.get("batch_key") or ["business_date"]
    if not isinstance(batch_key, list) or not all(isinstance(k, str) for k in batch_key):
        raise BadContract("batch_key must be a list of field-name strings")
    business_date_field = declaration.get("business_date_field", "business_date")
    if business_date_field not in batch_key:
        raise BadContract("business_date_field must appear in batch_key")

    max_file_size = declaration.get("max_file_size", DEFAULT_MAX_FILE_SIZE)
    if not isinstance(max_file_size, int) or max_file_size <= 0:
        raise BadContract("max_file_size must be a positive integer")

    late_window = int(declaration.get("late_window_minutes", DEFAULT_LATE_WINDOW_MINUTES))
    if late_window < 0:
        raise BadContract("late_window_minutes cannot be negative")

    on_missing = declaration.get("on_missing_required", "reject")
    if on_missing not in ALLOWED_MISSING_POLICIES:
        raise BadContract(
            f"on_missing_required must be one of {sorted(ALLOWED_MISSING_POLICIES)}"
        )

    supported = tuple(declaration.get("supported_compression", []))
    for comp in supported:
        if comp not in ALLOWED_COMPRESSION:
            raise BadContract(f"unsupported compression format declared: {comp!r}")
    for name, spec in members.items():
        if spec["compression"] == "gzip" and "gzip" not in supported:
            raise BadContract(
                f"member {name!r} requires gzip but supported_compression does not list it"
            )

    deadline_spec = _compile_deadline(declaration)

    normalised = {
        "contract_id": contract_id,
        "version": version,
        "batch_key": list(batch_key),
        "business_date_field": business_date_field,
        "max_file_size": max_file_size,
        "late_window_minutes": late_window,
        "on_missing_required": on_missing,
        "supported_compression": list(supported),
        "deadline": deadline_spec,
        "members": {name: members[name] for name in sorted(members)},
    }
    canon = canonical_json(normalised)
    digest = hashlib.sha256(canon.encode("utf-8")).hexdigest()

    return CompiledContract(
        contract_id=contract_id,
        version=version,
        raw=normalised,
        members=normalised["members"],
        batch_key=tuple(batch_key),
        business_date_field=business_date_field,
        max_file_size=max_file_size,
        deadline_spec=deadline_spec,
        late_window_minutes=late_window,
        on_missing_required=on_missing,
        supported_compression=supported,
        canonical_json=canon,
        digest=digest,
    )


def deadline_for(contract: CompiledContract, batch_key_values: Mapping[str, Any]) -> datetime:
    """Compute the arrival deadline instant for a specific batch."""
    spec = contract.deadline_spec
    if spec["type"] == "absolute":
        return parse_utc(spec["at"])

    date_value = batch_key_values.get(contract.business_date_field)
    if isinstance(date_value, datetime):
        base = date_value.astimezone(timezone.utc)
    elif isinstance(date_value, str):
        base = parse_utc(date_value)
    else:
        raise BadContract(
            f"batch key field {contract.business_date_field!r} must be an ISO date/datetime"
        )
    day = datetime(base.year, base.month, base.day, tzinfo=timezone.utc)
    return day + timedelta(
        days=spec["offset_days"], hours=spec["hour"], minutes=spec["minute"]
    )


def late_window_end(contract: CompiledContract, deadline: datetime) -> datetime:
    return deadline + timedelta(minutes=contract.late_window_minutes)

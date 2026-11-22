"""Read-only access to a specific closed binding of the closure service.

The netting service never scans arrival directories, never opens a file named
``detail.csv`` from disk and never defaults to the latest revision.  For an
application the caller names the exact ``(business_date, revision)``; this
module then:

1. resolves the batch through the same identity function the closure service
   uses (same contract, same batch key),
2. loads exactly that closure row and requires state ``CLOSED``,
3. takes the ``detail.csv`` member from the row's binding summary and reads
   the content-addressed blob whose sha256 the binding records,
4. refuses compressed payloads because the netting contract is plain UTF-8 CSV.

Nothing here writes to the closure database, the blob store or the claim
state.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Any, Mapping, Optional

from batch_closure import BatchClosureService
from batch_closure.errors import UnknownContract
from batch_closure.service import batch_identity

from .errors import BindingInvalid, InvalidBusinessDate, RevisionNotAvailable

DETAIL_MEMBER = "detail.csv"


def normalise_business_day(business_day: str) -> str:
    """Validate and normalise a business day to strict ``YYYY-MM-DD``."""
    if not isinstance(business_day, str):
        raise InvalidBusinessDate(
            f"business_date must be a YYYY-MM-DD string, got "
            f"{type(business_day).__name__}"
        )
    text = business_day.strip()
    if len(text) != 10 or text[4] != "-" or text[7] != "-":
        raise InvalidBusinessDate(f"business_date {business_day!r} is not YYYY-MM-DD")
    from datetime import date

    try:
        parsed = date.fromisoformat(text)
    except ValueError as exc:
        raise InvalidBusinessDate(f"business_date {business_day!r}: {exc}") from exc
    if parsed.isoformat() != text:
        raise InvalidBusinessDate(
            f"business_date {business_day!r} is not a strict calendar date"
        )
    return text


@dataclass(frozen=True)
class ClosedDetail:
    """The bytes and binding coordinates of one closed revision's detail."""

    business_day: str
    revision: int
    binding_sig: str
    detail_blob_sha256: str
    detail_bytes: bytes


def load_closed_detail(
    closure: BatchClosureService,
    contract: Any,
    business_day: str,
    revision: int,
) -> ClosedDetail:
    """Load the exact ``detail.csv`` bytes of one ``CLOSED`` revision.

    Raises a netting error for every unusable input so the caller can reject
    the application without touching the netting database.
    """
    day = normalise_business_day(business_day)
    if not isinstance(revision, int) or isinstance(revision, bool) or revision < 1:
        raise RevisionNotAvailable(
            f"revision must be a positive integer, got {revision!r}"
        )

    key_values = {contract.business_date_field: day}
    try:
        batch_id, _ = batch_identity(contract, key_values)
    except Exception as exc:  # pragma: no cover - defensive identity guard
        raise RevisionNotAvailable(f"cannot resolve batch for {day}: {exc}") from exc

    closure_row = closure.store.closure_at(batch_id, revision)
    if closure_row is None:
        raise RevisionNotAvailable(
            f"no stored revision {revision} for business day {day}"
        )
    if closure_row["state"] != "CLOSED":
        raise RevisionNotAvailable(
            f"revision {revision} of {day} is {closure_row['state']!r}, "
            "only CLOSED revisions can be applied"
        )

    try:
        binding = json.loads(closure_row["binding_json"])
    except (ValueError, TypeError) as exc:
        raise BindingInvalid(f"binding of {day} rev {revision} is not JSON: {exc}")
    if not isinstance(binding, Mapping) or DETAIL_MEMBER not in binding:
        raise BindingInvalid(
            f"binding of {day} rev {revision} does not contain {DETAIL_MEMBER}"
        )
    member = binding[DETAIL_MEMBER]
    if not isinstance(member, Mapping):
        raise BindingInvalid(
            f"binding entry for {DETAIL_MEMBER} on {day} rev {revision} is malformed"
        )

    compression = member.get("compression")
    if compression not in (None, "none"):
        raise BindingInvalid(
            f"{DETAIL_MEMBER} on {day} rev {revision} is compressed "
            f"({compression!r}); only uncompressed CSV is accepted"
        )
    blob_sha = member.get("blob_sha256")
    if not isinstance(blob_sha, str) or len(blob_sha) != 64:
        raise BindingInvalid(
            f"{DETAIL_MEMBER} binding on {day} rev {revision} lacks a blob sha256"
        )

    blob_path = closure.store.blob_path(blob_sha)
    try:
        raw = blob_path.read_bytes()
    except FileNotFoundError as exc:
        raise BindingInvalid(
            f"bound blob {blob_sha} for {day} rev {revision} is missing"
        ) from exc
    actual_sha = hashlib.sha256(raw).hexdigest()
    if actual_sha != blob_sha:
        raise BindingInvalid(
            f"bound blob for {day} rev {revision} failed its sha256 check "
            f"(binding={blob_sha}, actual={actual_sha})"
        )

    return ClosedDetail(
        business_day=day,
        revision=revision,
        binding_sig=closure_row["binding_sig"],
        detail_blob_sha256=blob_sha,
        detail_bytes=raw,
    )


def resolve_contract(
    closure: BatchClosureService,
    contract_id: str,
    contract_version: Optional[str],
    contract_digest: Optional[str] = None,
):
    """Load a persisted contract by id/version or by digest (read-only)."""
    try:
        if contract_digest:
            return closure.load_contract_by_digest(contract_digest)
        return closure.load_contract(contract_id, contract_version)
    except UnknownContract as exc:
        raise RevisionNotAvailable(str(exc)) from exc

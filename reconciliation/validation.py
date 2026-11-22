"""Strict validation of one receipt document.

A document is one complete new version of a voucher group::

    {
      "receipt_id":   "<non-empty ASCII alphanumeric>",
      "revision":     <positive integer>,
      "lines": [
        {"line_id":      "<non-empty ASCII alphanumeric, unique in group>",
         "flow_id":      "<non-empty ASCII alphanumeric>",
         "signed_cents": <non-zero integer>},
        ...
      ]
    }

Validation is deliberately strict and all-or-nothing: any illegal line makes
the whole document ineffective, so the group's previously effective version is
left untouched.  Existence of every referenced flow is checked against the
local posting library here as well -- an unknown flow is just another illegal
line.
"""

from __future__ import annotations

from typing import Any, Dict, List, Tuple

from .errors import MalformedReceipt

_ID_CHARS = set(
    "abcdefghijklmnopqrstuvwxyz"
    "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    "0123456789"
)


def is_ascii_alnum(value: Any) -> bool:
    return isinstance(value, str) and len(value) > 0 \
        and all(ch in _ID_CHARS for ch in value)


def _is_plain_int(value: Any) -> bool:
    # bool is a subclass of int; a JSON boolean is not an amount here.
    return isinstance(value, int) and not isinstance(value, bool)


def parse_receipt(document: Any) -> Tuple[str, int, List[Dict[str, Any]]]:
    """Validate shape and field-level rules; return the normalised payload.

    The returned lines keep the caller's ``flow_id`` strings.  Numeric flow
    ids (the local posting library uses integer primary keys) are converted by
    the service once existence has been checked.
    """
    if not isinstance(document, dict):
        raise MalformedReceipt(
            f"receipt root must be a JSON object, got {type(document).__name__}"
        )
    if set(document.keys()) != {"receipt_id", "revision", "lines"}:
        raise MalformedReceipt(
            "receipt must contain exactly receipt_id, revision and lines"
        )

    receipt_id = document["receipt_id"]
    if not is_ascii_alnum(receipt_id):
        raise MalformedReceipt(
            "receipt_id must be a non-empty ASCII alphanumeric string"
        )

    revision = document["revision"]
    if not _is_plain_int(revision) or revision <= 0:
        raise MalformedReceipt("revision must be a positive integer")

    raw_lines = document["lines"]
    if not isinstance(raw_lines, list):
        raise MalformedReceipt("lines must be an array (empty withdraws group)")

    normalised: List[Dict[str, Any]] = []
    seen_line_ids: set = set()
    for index, raw in enumerate(raw_lines):
        where = f"line at index {index}"
        if not isinstance(raw, dict):
            raise MalformedReceipt(f"{where} must be a JSON object")
        if set(raw.keys()) != {"line_id", "flow_id", "signed_cents"}:
            raise MalformedReceipt(
                f"{where} must contain exactly line_id, flow_id, signed_cents"
            )

        line_id = raw["line_id"]
        if not is_ascii_alnum(line_id):
            raise MalformedReceipt(
                f"{where}: line_id must be a non-empty ASCII alphanumeric string"
            )
        if line_id in seen_line_ids:
            raise MalformedReceipt(
                f"duplicate line_id {line_id!r} inside group {receipt_id!r}"
            )
        seen_line_ids.add(line_id)

        flow_id = raw["flow_id"]
        if not is_ascii_alnum(flow_id):
            raise MalformedReceipt(
                f"{where}: flow_id must be a non-empty ASCII alphanumeric string"
            )

        signed_cents = raw["signed_cents"]
        if not _is_plain_int(signed_cents) or signed_cents == 0:
            raise MalformedReceipt(
                f"{where}: signed_cents must be a non-zero integer"
            )

        normalised.append({
            "line_id": line_id,
            "flow_id": flow_id,
            "signed_cents": signed_cents,
        })

    return receipt_id, revision, normalised


def canonical_lines(
    lines: List[Dict[str, Any]],
) -> Tuple[Tuple[str, str, int], ...]:
    """Order-insensitive content signature for same-revision idempotence.

    Two resends of the current revision succeed exactly when every line id
    still maps to the same flow and amount, regardless of physical line order.
    """
    return tuple(
        sorted(
            (line["line_id"], str(line["flow_id"]), int(line["signed_cents"]))
            for line in lines
        )
    )

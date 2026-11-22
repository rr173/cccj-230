"""Strict parser/validator for the bound ``detail.csv`` payload.

Contracted format (only this exact shape is accepted):

* uncompressed UTF-8 CSV, header row exactly
  ``entry_id,account_id,kind,cents``;
* same-day ``entry_id`` values are unique;
* both ids are non-empty ASCII alphanumeric strings;
* ``kind`` is ``bill`` or ``payment``;
* ``cents`` is a positive integer (no sign, no decimals) not exceeding
  10**12;
* a header alone means "no records for this day", never "missing file".

Anything else raises :class:`InvalidDetailCsv` and the whole application is
rejected before any database write happens.
"""

from __future__ import annotations

import csv
import io
from typing import List, Sequence

from .engine import Entry
from .errors import InvalidDetailCsv

HEADER = ["entry_id", "account_id", "kind", "cents"]
MAX_CENTS = 10**12
_KINDS = ("bill", "payment")


def _is_ascii_alnum(value: str) -> bool:
    if not value:
        return False
    try:
        encoded = value.encode("ascii")
    except UnicodeEncodeError:
        return False
    return all(ch.isalnum() for ch in encoded.decode("ascii"))


def parse_detail_csv(data: bytes, business_day: str) -> List[Entry]:
    """Parse the exact bytes referenced by a closed binding.

    ``business_day`` is the already-normalised ``YYYY-MM-DD`` date stamped on
    every returned :class:`Entry`.
    """
    if not isinstance(data, (bytes, bytearray)):
        raise InvalidDetailCsv("detail payload must be raw bytes")
    try:
        text = bytes(data).decode("utf-8")
    except UnicodeDecodeError as exc:
        raise InvalidDetailCsv(f"detail.csv is not valid UTF-8: {exc}") from exc
    if text.startswith("﻿"):
        raise InvalidDetailCsv("detail.csv must not carry a UTF-8 BOM")

    # newline="" per the csv module docs so embedded line endings are handled
    # by the parser rather than the universal-newline layer.
    reader = csv.reader(io.StringIO(text, newline=""))
    try:
        rows = list(reader)
    except csv.Error as exc:
        raise InvalidDetailCsv(f"detail.csv is not parseable CSV: {exc}") from exc

    if not rows:
        raise InvalidDetailCsv("detail.csv is empty (header row required)")
    header, body = rows[0], rows[1:]
    if header != HEADER:
        raise InvalidDetailCsv(
            f"detail.csv header must be {HEADER!r}, got {header!r}"
        )

    entries: List[Entry] = []
    seen_entry_ids: set[str] = set()
    for line_no, row in enumerate(body, start=2):
        # Blank lines are tolerated by csv.reader as empty lists; treat any
        # structurally blank row as malformed rather than silently skipping.
        if len(row) != len(HEADER):
            raise InvalidDetailCsv(
                f"line {line_no}: expected {len(HEADER)} columns, got {len(row)}"
            )
        entry_id, account_id, kind, cents_text = row
        if entry_id in seen_entry_ids:
            raise InvalidDetailCsv(
                f"line {line_no}: duplicate entry_id {entry_id!r} within the day"
            )
        if not _is_ascii_alnum(entry_id):
            raise InvalidDetailCsv(
                f"line {line_no}: entry_id must be non-empty ASCII alphanumeric"
            )
        if not _is_ascii_alnum(account_id):
            raise InvalidDetailCsv(
                f"line {line_no}: account_id must be non-empty ASCII alphanumeric"
            )
        if kind not in _KINDS:
            raise InvalidDetailCsv(
                f"line {line_no}: kind must be one of {_KINDS}, got {kind!r}"
            )
        if not cents_text or any(ch not in "0123456789" for ch in cents_text):
            raise InvalidDetailCsv(
                f"line {line_no}: cents must be a positive integer, got "
                f"{cents_text!r}"
            )
        cents = int(cents_text)
        if not 1 <= cents <= MAX_CENTS:
            raise InvalidDetailCsv(
                f"line {line_no}: cents {cents} outside [1, 10^12]"
            )
        seen_entry_ids.add(entry_id)
        entries.append(
            Entry(
                day=business_day,
                entry_id=entry_id,
                account_id=account_id,
                kind=kind,
                cents=cents,
            )
        )
    return entries


def group_by_account(entries: Sequence[Entry]) -> dict[str, List[Entry]]:
    """Split a day's entries into per-account lists (each sorted by entry_id)."""
    grouped: dict[str, List[Entry]] = {}
    for entry in entries:
        grouped.setdefault(entry.account_id, []).append(entry)
    for rows in grouped.values():
        rows.sort(key=lambda e: e.entry_id)
    return grouped

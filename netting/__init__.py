"""Correction-safe per-entry netting fact table.

Public interface:
    NettingService      -- apply explicit closed revisions, read facts
    ClosedDetail        -- bound detail.csv coordinates/bytes value object
    Entry / Fact        -- engine value objects
    OpenState           -- resumable per-account open bill/payment state
    advance_day         -- pure one-account, one-day matching step
    replay_account      -- pure from-scratch recomputation of one account
    parse_detail_csv    -- strict validator for the contracted CSV
    exceptions defined in .errors
"""

from .csv_detail import group_by_account, parse_detail_csv
from .engine import (
    Entry,
    Fact,
    OpenBill,
    OpenPayment,
    OpenState,
    advance_day,
    replay_account,
)
from .errors import (
    BindingInvalid,
    InvalidBusinessDate,
    InvalidDetailCsv,
    NettingError,
    RevisionNotAvailable,
    StaleRevision,
)
from .service import NettingService
from .source import ClosedDetail, load_closed_detail, normalise_business_day

__all__ = [
    "NettingService",
    "ClosedDetail",
    "Entry",
    "Fact",
    "OpenBill",
    "OpenPayment",
    "OpenState",
    "advance_day",
    "replay_account",
    "parse_detail_csv",
    "group_by_account",
    "load_closed_detail",
    "normalise_business_day",
    "NettingError",
    "InvalidBusinessDate",
    "RevisionNotAvailable",
    "BindingInvalid",
    "InvalidDetailCsv",
    "StaleRevision",
]

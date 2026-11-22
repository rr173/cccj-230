"""Local netting-posting handoff.

Public interface:
    PostingService   -- preview / confirm / read-batch on a netting SQLite db
    exceptions defined in .errors

The package stores its tables in the same SQLite file as the netting service
and attaches change-tracking triggers to the netting ``facts`` table.  It
never connects to an external ledger and never changes a netting matching
rule or any netting interface's business semantics.
"""

from .errors import (
    BatchNotFound,
    InvalidPostingDay,
    NothingToPost,
    PostingError,
    PreviewStale,
    UnknownPreview,
)
from .service import PostingService

__all__ = [
    "PostingService",
    "PostingError",
    "InvalidPostingDay",
    "UnknownPreview",
    "PreviewStale",
    "NothingToPost",
    "BatchNotFound",
]

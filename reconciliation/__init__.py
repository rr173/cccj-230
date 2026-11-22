"""Local general-ledger receipt reconciliation.

Public interface:
    ReconciliationService -- import receipt versions / query batch closure
    exceptions defined in .errors

The local posting library (the SQLite file provisioned by the ``posting``
package) is opened strictly read-only; receipts are persisted in a separate
SQLite file.  Nothing is ever written to a general ledger, no published batch
is modified and no posting flow is regenerated.
"""

from .errors import (
    BatchNotFound,
    MalformedReceipt,
    ReconciliationError,
    StaleRevision,
    UnreadablePostingLibrary,
)
from .service import ReconciliationService

__all__ = [
    "ReconciliationService",
    "ReconciliationError",
    "MalformedReceipt",
    "StaleRevision",
    "BatchNotFound",
    "UnreadablePostingLibrary",
]

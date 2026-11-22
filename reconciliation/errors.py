"""Exceptions for the local general-ledger receipt reconciliation service.

Every failure is local: nothing is ever written to the general ledger and no
confirmed posting batch is ever modified.  A rejected receipt file never
changes the currently effective version of its voucher group.
"""


class ReconciliationError(Exception):
    """Base class for every reconciliation-side error."""


class MalformedReceipt(ReconciliationError):
    """The receipt document is not a well-formed, complete new version.

    Raised for bad top-level shape/fields, illegal lines (bad ids, zero or
    non-integer amounts, duplicated line ids) or a line referring to a flow
    that is not a confirmed local posting flow.  The whole group stays on its
    previously effective version.
    """


class StaleRevision(ReconciliationError):
    """A lower revision was sent, or the same revision carried different
    content.  The stored current version is left untouched."""


class BatchNotFound(ReconciliationError):
    """No confirmed posting batch exists with the requested batch number.

    A non-existent batch must never be reported as closeable.
    """


class UnreadablePostingLibrary(ReconciliationError):
    """The fixed local posting SQLite library cannot be opened read-only."""

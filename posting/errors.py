"""Exceptions for the local netting-posting handoff service.

The handoff never talks to an external ledger; every failure is local and
leaves published batches untouched.
"""


class PostingError(Exception):
    """Base class for every posting-side error."""


class InvalidPostingDay(PostingError):
    """The posting day is not a strict ``YYYY-MM-DD`` date or moves backwards."""


class UnknownPreview(PostingError):
    """The referenced preview id does not exist in this database."""


class PreviewStale(PostingError):
    """A preview can no longer be confirmed.

    It is stale when a committed application actually changed netting facts
    after the preview was built (even if the facts later changed back), or
    when another preview was confirmed first.  Row-only rewrites that keep
    every pairing identity and amount, revision-only re-applications and
    failed applications do not stale a preview.
    """


class NothingToPost(PostingError):
    """The preview carries no entries; an empty batch must never be published."""


class BatchNotFound(PostingError):
    """No confirmed batch exists with the requested batch number."""

"""Exceptions for the per-entry netting fact service.

A rejected application (binding missing/non-closed, malformed detail CSV,
stale revision) raises one of these.  Rejections never mutate the published
state: the caller fixes the input and retries.
"""


class NettingError(Exception):
    """Base class for every netting-side error."""


class InvalidBusinessDate(NettingError):
    """The business day argument is not a strict ``YYYY-MM-DD`` date."""


class RevisionNotAvailable(NettingError):
    """The requested day/revision does not exist or is not a ``CLOSED`` binding."""


class BindingInvalid(NettingError):
    """The closed binding does not carry a usable plain ``detail.csv``."""


class InvalidDetailCsv(NettingError):
    """The bound ``detail.csv`` bytes fail the contracted CSV/row rules."""


class StaleRevision(NettingError):
    """A newer revision of the same business day is already applied."""

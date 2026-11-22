"""Domain exceptions.

Business validation failures on received files (bad digest, oversize, path
escape, ...) are *recorded* and returned as result dictionaries rather than
raised.  Exceptions are reserved for programming/configuration errors and for
operations that cannot legally be performed in the current state (claiming an
unclosed revision, re-claiming an active claim, ...).
"""


class BatchClosureError(Exception):
    pass


class BadContract(BatchClosureError):
    pass


class UnknownContract(BatchClosureError):
    pass


class UnknownMember(BatchClosureError):
    pass


class PathEscape(BatchClosureError):
    pass


class SizeExceeded(BatchClosureError):
    pass


class UnsupportedCompression(BatchClosureError):
    pass


class NotClosed(BatchClosureError):
    pass


class AlreadyClaimed(BatchClosureError):
    pass


class InvalidToken(BatchClosureError):
    pass


class LateArrivalRejected(BatchClosureError):
    pass

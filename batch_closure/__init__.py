"""Daily settlement batch closure service.

Public interface:
    BatchClosureService  -- persistence + closure/revision/claim/evidence API
    compile_contract     -- declarative arrival contract compiler
    ReferenceMachine     -- independent pure state machine for cross checking
    exceptions defined in .errors
"""

from .contract import compile_contract, CompiledContract, deadline_for
from .errors import (
    BatchClosureError,
    BadContract,
    UnknownContract,
    UnknownMember,
    PathEscape,
    SizeExceeded,
    UnsupportedCompression,
    NotClosed,
    AlreadyClaimed,
    InvalidToken,
    LateArrivalRejected,
)
from .service import BatchClosureService
from .state_machine import ReferenceMachine

__all__ = [
    "BatchClosureService",
    "compile_contract",
    "CompiledContract",
    "deadline_for",
    "ReferenceMachine",
    "BatchClosureError",
    "BadContract",
    "UnknownContract",
    "UnknownMember",
    "PathEscape",
    "SizeExceeded",
    "UnsupportedCompression",
    "NotClosed",
    "AlreadyClaimed",
    "InvalidToken",
    "LateArrivalRejected",
]

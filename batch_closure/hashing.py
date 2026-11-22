"""Streaming hashing helpers.

Files are read in fixed-size chunks so that a multi-gigabyte member never has
to be held in memory.  A sha256 content hash is always computed (it is used as
the content-addressed blob name) in addition to whatever digest algorithms the
contract declares.
"""

from __future__ import annotations

import hashlib
from typing import BinaryIO, Dict, Iterable, Optional, Tuple

CHUNK_SIZE = 64 * 1024


class StreamSizeExceeded(Exception):
    """Raised mid-stream when more than ``max_size`` bytes were consumed."""

    def __init__(self, max_size: int, actual: int):
        super().__init__(f"stream size {actual} exceeds limit {max_size}")
        self.max_size = max_size
        self.actual = actual


def stream_digest(
    source: BinaryIO,
    algorithms: Iterable[str],
    max_size: Optional[int] = None,
) -> Tuple[int, Dict[str, str]]:
    """Hash ``source`` incrementally.

    Returns ``(size_in_bytes, {algorithm: hex_digest})``.  sha256 is always
    included even if not requested.  Raises :class:`StreamSizeExceeded` as
    soon as the running size crosses ``max_size``.
    """
    algs = set(algorithms) | {"sha256"}
    hashers = {a: hashlib.new(a) for a in algs}
    size = 0
    while True:
        chunk = source.read(CHUNK_SIZE)
        if not chunk:
            break
        size += len(chunk)
        if max_size is not None and size > max_size:
            raise StreamSizeExceeded(max_size, size)
        for hasher in hashers.values():
            hasher.update(chunk)
    return size, {a: h.hexdigest() for a, h in hashers.items()}


def digest_file(path, algorithms: Iterable[str]) -> Tuple[int, Dict[str, str]]:
    with open(path, "rb") as fh:
        return stream_digest(fh, algorithms, max_size=None)


def gzip_digest(path, algorithms: Iterable[str]) -> Tuple[int, Dict[str, str]]:
    """Hash the *decompressed* payload of a gzip file, streaming."""
    import gzip

    algs = set(algorithms) | {"sha256"}
    hashers = {a: hashlib.new(a) for a in algs}
    size = 0
    with gzip.open(path, "rb") as fh:
        while True:
            chunk = fh.read(CHUNK_SIZE)
            if not chunk:
                break
            size += len(chunk)
            for hasher in hashers.values():
                hasher.update(chunk)
    return size, {a: h.hexdigest() for a, h in hashers.items()}


def is_gzip(path) -> bool:
    with open(path, "rb") as fh:
        magic = fh.read(2)
    return magic == b"\x1f\x8b"


# Magic signatures of compression/archive formats the service can recognise.
# Anything matched here is treated as a *compressed* payload by the arrival
# contract, even if the format itself is not supported.
_COMPRESSION_MAGIC = (
    (b"\x1f\x8b", "gzip"),
    (b"BZh", "bzip2"),
    (b"\xfd7zXZ\x00", "xz"),
    (b"PK\x03\x04", "zip"),
    (b"PK\x05\x06", "zip"),  # empty archive
)


def sniff_compression(path) -> Optional[str]:
    """Return a recognised compression name, or ``None`` for plain bytes."""
    with open(path, "rb") as fh:
        head = fh.read(8)
    for magic, name in _COMPRESSION_MAGIC:
        if head.startswith(magic):
            return name
    return None

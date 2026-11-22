"""Path containment checks.

Member paths are untrusted input.  A member path must always resolve to a
location inside the declared inbound root: absolute paths and ``..`` traversal
attempts are rejected before any filesystem operation uses the path.
"""

from __future__ import annotations

import os
from pathlib import Path

from .errors import PathEscape


def safe_resolve(root, relpath: str) -> Path:
    """Resolve ``relpath`` under ``root`` or raise :class:`PathEscape`.

    The check is purely lexical; the target does not need to exist.
    """
    if relpath is None or relpath == "":
        raise PathEscape("empty member path")
    if "\x00" in relpath:
        raise PathEscape("nul byte in member path")
    p = Path(relpath)
    if p.is_absolute() or p.anchor:
        raise PathEscape(f"absolute member path not allowed: {relpath!r}")
    # Pure lexical rejection first, then a resolved containment check so that
    # symlink-style escapes are also caught.
    if ".." in p.parts:
        raise PathEscape(f"parent traversal not allowed: {relpath!r}")
    root_resolved = Path(root).resolve()
    target = (root_resolved / p).resolve()
    if target != root_resolved and root_resolved not in target.parents:
        raise PathEscape(f"path escapes inbound root: {relpath!r}")
    return target


def relative_blob_name(sha256_hex: str) -> str:
    return os.path.join("blobs", sha256_hex[:2], sha256_hex)

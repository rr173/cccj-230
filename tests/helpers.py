"""Shared test helpers: controllable clock, file fixtures, cross-checking."""

from __future__ import annotations

import gzip
import hashlib
import json
import shutil
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

from batch_closure import BatchClosureService, compile_contract
from batch_closure.contract import deadline_for, late_window_end

UTC = timezone.utc


class FixedClock:
    def __init__(self, start: datetime):
        self.t = start

    def __call__(self) -> datetime:
        return self.t

    def set(self, t: datetime) -> None:
        self.t = t

    def advance(self, **kwargs) -> None:
        self.t += timedelta(**kwargs)


def make_workdir() -> Path:
    return Path(tempfile.mkdtemp(prefix="bctest_"))


def cleanup(path: Path) -> None:
    shutil.rmtree(path, ignore_errors=True)


def write_member(inbound: Path, name: str, content: bytes) -> Path:
    target = inbound / name
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(content)
    return target


def gzip_bytes(content: bytes) -> bytes:
    import io

    buf = io.BytesIO()
    with gzip.GzipFile(fileobj=buf, mode="wb", mtime=0) as gz:
        gz.write(content)
    return buf.getvalue()


def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def base_contract(**overrides):
    decl = {
        "contract_id": "daily-settle",
        "version": "1",
        "batch_key": ["business_date"],
        "max_file_size": 1024,
        "deadline": {"type": "relative", "offset_days": 1, "hour": 12, "minute": 0},
        "late_window_minutes": 60,
        "on_missing_required": "reject",
        "supported_compression": ["gzip"],
        "members": {
            "manifest.json": {
                "required": True,
                "role": "manifest",
                "digest_algorithms": ["sha256"],
            },
            "detail.csv": {
                "required": True,
                "role": "data",
                "digest_algorithms": ["sha256"],
            },
            "control.json": {
                "required": False,
                "role": "summary",
                "digest_algorithms": ["sha256"],
            },
            "extra.txt": {
                "required": False,
                "role": "data",
                "digest_algorithms": ["sha256"],
            },
        },
    }
    decl.update(overrides)
    return decl


def business_day() -> datetime:
    return datetime(2026, 11, 22, tzinfo=UTC)


def day_base(day: datetime) -> datetime:
    # deadline = business day + 1 at 12:00 UTC
    return datetime(day.year, day.month, day.day, tzinfo=UTC) + timedelta(days=1)


def make_service(workdir: Path, clock):
    inbound = workdir / "inbound"
    inbound.mkdir(parents=True, exist_ok=True)
    svc = BatchClosureService(workdir / "data", inbound, clock=clock)
    return svc, inbound


def assert_replay_matches(testcase, svc, contract, key):
    """Service decisions must match the independent reference machine."""
    report = svc.replay(contract, key)
    testcase.assertTrue(
        report["matches"],
        msg=f"reference machine disagreement:\n{json.dumps(report, indent=2, default=str)}",
    )
    return report


def expected_closure_sequences(report):
    return [(c["revision"], c["state"]) for c in report["stored_closures"]], [
        (c["revision"], c["state"]) for c in report["derived_closures"]
    ]

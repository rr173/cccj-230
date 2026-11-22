"""Fixtures for netting tests: build real CLOSED revisions via the closure
service, in temporary directories, with no large data files.
"""

from __future__ import annotations

import hashlib
import json
import shutil
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple

from batch_closure import BatchClosureService

UTC = timezone.utc

CONTRACT_ID = "netting-source"
CONTRACT_VERSION = "1"


def contract_declaration() -> dict:
    # detail.csv is declared plain (compression omitted => none): the netting
    # contract only accepts uncompressed UTF-8 CSV.
    return {
        "contract_id": CONTRACT_ID,
        "version": CONTRACT_VERSION,
        "batch_key": ["business_date"],
        "business_date_field": "business_date",
        "max_file_size": 16 * 1024 * 1024,
        # Deadline is day+1 at 12:00; fixtures evaluate with an explicit
        # timestamp past it, so no wall-clock waiting is needed.
        "deadline": {"type": "relative", "offset_days": 1, "hour": 12,
                     "minute": 0},
        "late_window_minutes": 60 * 24 * 365,
        "on_missing_required": "reject",
        "supported_compression": [],
        "members": {
            "manifest.json": {
                "required": True, "role": "manifest",
                "digest_algorithms": ["sha256"],
            },
            "detail.csv": {
                "required": True, "role": "data",
                "digest_algorithms": ["sha256"],
            },
        },
    }


class TickingClock:
    """Monotonic clock; every read is one second later so ordering is stable."""

    def __init__(self, start: datetime):
        self.t = start

    def __call__(self) -> datetime:
        self.t += timedelta(seconds=1)
        return self.t


def csv_bytes(rows: Sequence[Tuple[str, str, str, int]]) -> bytes:
    """Render rows ``(entry_id, account_id, kind, cents)`` as contracted CSV."""
    lines = ["entry_id,account_id,kind,cents"]
    lines.extend(f"{e},{a},{k},{c}" for e, a, k, c in rows)
    return ("\n".join(lines) + "\n").encode("utf-8")


def header_only_csv() -> bytes:
    return b"entry_id,account_id,kind,cents\n"


class ClosureFixture:
    """A temp-dir closure service preloaded with the netting source contract."""

    def __init__(self):
        self.workdir = Path(tempfile.mkdtemp(prefix="netting_fixture_"))
        self.inbound = self.workdir / "inbound"
        self.inbound.mkdir()
        self.data_dir = self.workdir / "closure"
        self.clock = TickingClock(datetime(2026, 11, 1, 9, 0, tzinfo=UTC))
        self.service = BatchClosureService(
            self.data_dir, self.inbound, clock=self.clock
        )
        self.contract = self.service.compile_contract(contract_declaration())

    def cleanup(self) -> None:
        self.service.close()
        shutil.rmtree(self.workdir, ignore_errors=True)

    def _key(self, day: str) -> dict:
        return {"business_date": day}

    def _write_detail(self, day: str, payload: bytes, name: str) -> Path:
        day_dir = self.inbound / day
        day_dir.mkdir(parents=True, exist_ok=True)
        path = day_dir / name
        path.write_bytes(payload)
        return path

    def publish_day(
        self,
        day: str,
        detail_payload: bytes,
        expect_revision: Optional[int] = None,
    ) -> int:
        """Receive manifest + detail for a day and evaluate to a CLOSED revision.

        Returns the resulting revision number.  Calling again with new detail
        content creates the next revision.
        """
        manifest = {"members": [
            {"name": "manifest.json", "path": "manifest.json"},
            {"name": "detail.csv", "path": "detail.csv"},
        ]}
        manifest_path = self._write_detail(day, json.dumps(manifest).encode(),
                                           "manifest.json")
        self.service.receive_file(
            self.contract, self._key(day), "manifest.json", manifest_path
        )
        detail_path = self._write_detail(day, detail_payload, "detail.csv")
        self.service.receive_file(
            self.contract, self._key(day), "detail.csv", detail_path,
            expected_digests={
                "sha256": hashlib.sha256(detail_payload).hexdigest()
            },
        )
        # Evaluate explicitly past the relative deadline (day+1 12:00 UTC).
        day_date = datetime.fromisoformat(day).replace(tzinfo=UTC)
        evaluate_at = day_date + timedelta(days=2)
        result = self.service.evaluate_closure(
            self.contract, self._key(day), at=evaluate_at
        )
        assert result["closed"], f"day {day} did not close: {result}"
        assert result["state"] == "CLOSED", result
        if expect_revision is not None:
            assert result["revision"] == expect_revision, result
        return int(result["revision"])

    def raw_store_handle(self) -> BatchClosureService:
        return self.service


def make_fixture() -> ClosureFixture:
    return ClosureFixture()


def make_gzip_fixture() -> "GzipClosureFixture":
    return GzipClosureFixture()


class GzipClosureFixture(ClosureFixture):
    """A closure service whose detail.csv member is contracted as gzip."""

    GZIP_CONTRACT_ID = "netting-gzip-source"

    def __init__(self):
        super().__init__()
        self.gzip_contract = self.service.compile_contract({
            "contract_id": self.GZIP_CONTRACT_ID,
            "version": "1",
            "batch_key": ["business_date"],
            "business_date_field": "business_date",
            "max_file_size": 16 * 1024 * 1024,
            "deadline": {"type": "relative", "offset_days": 1,
                         "hour": 12, "minute": 0},
            "late_window_minutes": 60 * 24 * 365,
            "on_missing_required": "reject",
            "supported_compression": ["gzip"],
            "members": {
                "manifest.json": {"required": True, "role": "manifest"},
                "detail.csv": {"required": True, "role": "data",
                               "compression": "gzip"},
            },
        })

    def publish_gzip_day(self, day: str, gz_payload: bytes) -> int:
        manifest = {"members": [
            {"name": "manifest.json", "path": "manifest.json"},
            {"name": "detail.csv", "path": "detail.csv"},
        ]}
        day_dir = self.inbound / day
        day_dir.mkdir(parents=True, exist_ok=True)
        (day_dir / "manifest.json").write_bytes(json.dumps(manifest).encode())
        (day_dir / "detail.csv").write_bytes(gz_payload)
        self.service.receive_file(
            self.gzip_contract, self._key(day), "manifest.json",
            day_dir / "manifest.json")
        self.service.receive_file(
            self.gzip_contract, self._key(day), "detail.csv",
            day_dir / "detail.csv")
        evaluate_at = datetime.fromisoformat(day).replace(
            tzinfo=UTC) + timedelta(days=2)
        result = self.service.evaluate_closure(
            self.gzip_contract, self._key(day), at=evaluate_at)
        assert result["closed"] and result["state"] == "CLOSED", result
        return int(result["revision"])

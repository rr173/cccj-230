"""Streaming / memory tests: large files never need to be fully buffered."""

import hashlib
import os
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from batch_closure.hashing import CHUNK_SIZE, StreamSizeExceeded, stream_digest
from batch_closure.service import BatchClosureService
from batch_closure.contract import compile_contract

from helpers import base_contract, make_service, FixedClock, write_member
from datetime import datetime, timedelta, timezone


class CountingStream:
    """File-like object that records the largest single read requested."""

    def __init__(self, path):
        self._fh = open(path, "rb")
        self.max_read = 0
        self.total = 0

    def read(self, n=-1):
        if n is not None and n >= 0:
            self.max_read = max(self.max_read, n)
        chunk = self._fh.read(n)
        self.total += len(chunk)
        return chunk

    def close(self):
        self._fh.close()


class StreamingTests(unittest.TestCase):
    def setUp(self):
        from helpers import make_workdir

        self.workdir = make_workdir()
        self.clock = FixedClock(datetime(2026, 11, 24, tzinfo=timezone.utc))
        self.svc, self.inbound = make_service(self.workdir, self.clock)

    def tearDown(self):
        self.svc.close()
        import shutil

        shutil.rmtree(self.workdir, ignore_errors=True)

    def test_digest_uses_bounded_chunks(self):
        path = self.inbound / "big.dat"
        size = 5 * 1024 * 1024 + 123
        with open(path, "wb") as fh:
            fh.write(os.urandom(size))
        stream = CountingStream(path)
        got_size, digests = stream_digest(stream, ["sha256", "sha1"])
        stream.close()
        self.assertEqual(got_size, size)
        self.assertLessEqual(stream.max_read, CHUNK_SIZE)
        self.assertEqual(
            digests["sha256"], hashlib.sha256(path.read_bytes()).hexdigest()
        )

    def test_size_cap_aborts_mid_stream(self):
        path = self.inbound / "oversized.dat"
        with open(path, "wb") as fh:
            fh.write(b"z" * (128 * 1024))
        stream = CountingStream(path)
        with self.assertRaises(StreamSizeExceeded) as ctx:
            stream_digest(stream, ["sha256"], max_size=4096)
        stream.close()
        self.assertEqual(ctx.exception.max_size, 4096)
        self.assertGreater(ctx.exception.actual, 4096)
        # It stopped shortly after crossing the cap, never buffering the file.
        self.assertLess(stream.total, 4096 + CHUNK_SIZE * 2)

    def test_large_file_round_trip_through_service(self):
        decl = base_contract(max_file_size=64 * 1024 * 1024)
        contract = self.svc.compile_contract(decl)
        key = {"business_date": "2026-11-23T00:00:00+00:00"}
        path = write_member(self.inbound, "detail.csv",
                            b"7" * (8 * 1024 * 1024))
        result = self.svc.receive_file(
            contract, key, "detail.csv", path,
            {"sha256": hashlib.sha256(path.read_bytes()).hexdigest()},
        )
        self.assertTrue(result["accepted"])
        self.assertEqual(result["size"], 8 * 1024 * 1024)
        stored = self.svc.store.blob_path(result["blob_sha256"])
        self.assertEqual(stored.stat().st_size, 8 * 1024 * 1024)


if __name__ == "__main__":
    unittest.main(verbosity=2)

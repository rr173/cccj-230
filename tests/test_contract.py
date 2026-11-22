"""Contract compiler tests."""

import hashlib
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from batch_closure import BadContract, compile_contract, deadline_for
from batch_closure.contract import canonical_json, late_window_end

from helpers import base_contract, business_day


class ContractTests(unittest.TestCase):
    def test_valid_contract_compiles_and_has_stable_digest(self):
        c1 = compile_contract(base_contract())
        c2 = compile_contract(base_contract())
        self.assertEqual(c1.digest, c2.digest)
        self.assertEqual(
            c1.digest,
            hashlib.sha256(c1.canonical_json.encode("utf-8")).hexdigest(),
        )

    def test_field_order_does_not_change_digest(self):
        a = base_contract()
        b = dict(reversed(list(a.items())))
        self.assertEqual(compile_contract(a).digest, compile_contract(b).digest)

    def test_version_change_changes_digest(self):
        a = compile_contract(base_contract(version="1"))
        b = compile_contract(base_contract(version="2"))
        self.assertNotEqual(a.digest, b.digest)

    def test_deadline_relative_and_absolute(self):
        c = compile_contract(base_contract())
        dl = deadline_for(c, {"business_date": "2026-11-22T00:00:00+00:00"})
        self.assertEqual(dl.isoformat(), "2026-11-23T12:00:00+00:00")
        end = late_window_end(c, dl)
        self.assertEqual(end.isoformat(), "2026-11-23T13:00:00+00:00")

        abs_decl = base_contract(
            deadline={"type": "absolute", "at": "2026-12-01T08:30:00Z"}
        )
        ca = compile_contract(abs_decl)
        self.assertEqual(
            deadline_for(ca, {"business_date": "2026-11-22"}).isoformat(),
            "2026-12-01T08:30:00+00:00",
        )

    def test_rejects_bad_inputs(self):
        cases = [
            {},
            base_contract(contract_id=""),
            base_contract(members={}),
            base_contract(batch_key=["other"]),  # business_date_field missing
            base_contract(max_file_size=0),
            base_contract(late_window_minutes=-1),
            base_contract(on_missing_required="nope"),
            base_contract(supported_compression=["rar"]),
        ]
        for decl in cases:
            with self.assertRaises(BadContract):
                compile_contract(decl)

    def test_gzip_member_requires_declared_support(self):
        decl = base_contract(supported_compression=[])
        decl["members"]["detail.csv"] = {
            "required": True,
            "compression": "gzip",
        }
        with self.assertRaises(BadContract):
            compile_contract(decl)

    def test_unknown_algorithm_rejected(self):
        decl = base_contract()
        decl["members"]["detail.csv"]["digest_algorithms"] = ["sha3-512"]
        with self.assertRaises(BadContract):
            compile_contract(decl)

    def test_member_catalog_accessors(self):
        c = compile_contract(base_contract())
        self.assertEqual(c.required_names(), ["detail.csv", "manifest.json"])
        self.assertEqual(c.optional_names(), ["control.json", "extra.txt"])
        self.assertEqual(c.member_limit("detail.csv"), 1024)
        with self.assertRaises(Exception):
            c.member("not-declared")


if __name__ == "__main__":
    unittest.main(verbosity=2)

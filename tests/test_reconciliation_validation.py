"""Unit tests for strict receipt document validation."""

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from reconciliation.errors import MalformedReceipt  # noqa: E402
from reconciliation.validation import (  # noqa: E402
    canonical_lines,
    is_ascii_alnum,
    parse_receipt,
)


def doc(receipt_id="G1", revision=1, lines=None):
    if lines is None:
        lines = [{"line_id": "a", "flow_id": "7", "signed_cents": 10}]
    return {"receipt_id": receipt_id, "revision": revision, "lines": lines}


class ValidationTests(unittest.TestCase):
    def test_minimal_valid_document(self):
        rid, rev, lines = parse_receipt(doc())
        self.assertEqual((rid, rev), ("G1", 1))
        self.assertEqual(lines[0]["line_id"], "a")

    def test_empty_lines_is_valid_withdrawal(self):
        _, rev, lines = parse_receipt(doc(lines=[]))
        self.assertEqual(rev, 1)
        self.assertEqual(lines, [])

    def test_first_revision_need_not_be_one(self):
        _, rev, _ = parse_receipt(doc(revision=42))
        self.assertEqual(rev, 42)

    def test_rejects_wrong_root_shape(self):
        for bad in ([], "x", 1, None):
            with self.assertRaises(MalformedReceipt):
                parse_receipt(bad)

    def test_rejects_unknown_top_level_keys(self):
        bad = doc()
        bad["extra"] = 1
        with self.assertRaises(MalformedReceipt):
            parse_receipt(bad)

    def test_rejects_missing_top_level_keys(self):
        with self.assertRaises(MalformedReceipt):
            parse_receipt({"receipt_id": "G1", "revision": 1})

    def test_receipt_id_rules(self):
        for bad in ("", "has space", "dash-x", "under_x", "点"):
            with self.assertRaises(MalformedReceipt):
                parse_receipt(doc(receipt_id=bad))
        self.assertTrue(is_ascii_alnum("Abc123"))
        self.assertFalse(is_ascii_alnum(""))
        self.assertFalse(is_ascii_alnum(12))

    def test_revision_must_be_positive_plain_int(self):
        for bad in (0, -1, 1.5, True, "2", None):
            with self.assertRaises(MalformedReceipt):
                parse_receipt(doc(revision=bad))

    def test_lines_must_be_array(self):
        for bad in ({}, "x", 1):
            with self.assertRaises(MalformedReceipt):
                parse_receipt(doc(lines=bad))

    def test_line_id_rules_and_uniqueness(self):
        with self.assertRaises(MalformedReceipt):
            parse_receipt(doc(lines=[
                {"line_id": "", "flow_id": "1", "signed_cents": 1}]))
        with self.assertRaises(MalformedReceipt):
            parse_receipt(doc(lines=[
                {"line_id": "x-y", "flow_id": "1", "signed_cents": 1}]))
        with self.assertRaises(MalformedReceipt):
            parse_receipt(doc(lines=[
                {"line_id": "x", "flow_id": "1", "signed_cents": 1},
                {"line_id": "x", "flow_id": "2", "signed_cents": 2}]))

    def test_flow_id_rules(self):
        for bad in ("", "a.b", "a b", "中"):
            with self.assertRaises(MalformedReceipt):
                parse_receipt(doc(lines=[
                    {"line_id": "x", "flow_id": bad,
                     "signed_cents": 1}]))

    def test_signed_cents_must_be_non_zero_plain_int(self):
        for bad in (0, 1.0, True, False, "5", None):
            with self.assertRaises(MalformedReceipt):
                parse_receipt(doc(lines=[
                    {"line_id": "x", "flow_id": "1",
                     "signed_cents": bad}]))

    def test_negative_amounts_are_legal(self):
        _, _, lines = parse_receipt(doc(lines=[
            {"line_id": "x", "flow_id": "1", "signed_cents": -120}]))
        self.assertEqual(lines[0]["signed_cents"], -120)

    def test_unknown_line_keys_rejected(self):
        with self.assertRaises(MalformedReceipt):
            parse_receipt(doc(lines=[
                {"line_id": "x", "flow_id": "1", "signed_cents": 1,
                 "note": "nope"}]))

    def test_canonical_signature_ignores_order(self):
        a = [
            {"line_id": "x", "flow_id": "1", "signed_cents": 10},
            {"line_id": "y", "flow_id": "2", "signed_cents": -5},
        ]
        b = list(reversed(a))
        self.assertEqual(canonical_lines(a), canonical_lines(b))
        c = [dict(row) for row in a]
        c[0]["signed_cents"] = 11
        self.assertNotEqual(canonical_lines(a), canonical_lines(c))
        d = [dict(row) for row in a]
        d[0]["flow_id"] = "9"
        self.assertNotEqual(canonical_lines(a), canonical_lines(d))


if __name__ == "__main__":
    unittest.main()

"""Strict CSV validation tests for the contracted detail format."""

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from netting import InvalidDetailCsv, parse_detail_csv  # noqa: E402

DAY = "2026-11-01"


class CsvValidationTests(unittest.TestCase):
    def test_header_only_means_zero_records(self):
        self.assertEqual(parse_detail_csv(b"entry_id,account_id,kind,cents\n", DAY),
                         [])

    def test_valid_rows_parse_with_stamped_day(self):
        rows = parse_detail_csv(
            b"entry_id,account_id,kind,cents\n"
            b"e1,acct1,bill,100\n"
            b"e2,acct2,payment,1000000000000\n",
            DAY,
        )
        self.assertEqual([(r.entry_id, r.account_id, r.kind, r.cents)
                          for r in rows],
                         [("e1", "acct1", "bill", 100),
                          ("e2", "acct2", "payment", 10**12)])
        self.assertTrue(all(r.day == DAY for r in rows))

    def test_rejects_non_utf8(self):
        with self.assertRaises(InvalidDetailCsv):
            parse_detail_csv(b"entry_id,account_id,kind,cents\n\xff\xfe\n", DAY)

    def test_rejects_bom(self):
        with self.assertRaises(InvalidDetailCsv):
            parse_detail_csv(
                "﻿entry_id,account_id,kind,cents\n".encode("utf-8"), DAY)

    def test_rejects_bad_header(self):
        with self.assertRaises(InvalidDetailCsv):
            parse_detail_csv(b"id,account_id,kind,cents\ne1,A,bill,1\n", DAY)

    def test_rejects_wrong_column_count(self):
        with self.assertRaises(InvalidDetailCsv):
            parse_detail_csv(b"entry_id,account_id,kind,cents\ne1,A,bill\n", DAY)
        with self.assertRaises(InvalidDetailCsv):
            parse_detail_csv(
                b"entry_id,account_id,kind,cents\ne1,A,bill,1,extra\n", DAY)

    def test_rejects_empty_or_non_alnum_ids(self):
        for bad in (
            b"entry_id,account_id,kind,cents\n,A,bill,1\n",
            b"entry_id,account_id,kind,cents\ne-1,A,bill,1\n",
            b"entry_id,account_id,kind,cents\ne1,,bill,1\n",
            b"entry_id,account_id,kind,cents\ne1,ac ct,bill,1\n",
        ):
            with self.assertRaises(InvalidDetailCsv):
                parse_detail_csv(bad, DAY)

    def test_rejects_unicode_alnum_and_digits(self):
        # Unicode letter in an id, and a Unicode superscript digit in cents.
        with self.assertRaises(InvalidDetailCsv):
            parse_detail_csv(
                "entry_id,account_id,kind,cents\né1,A,bill,1\n".encode("utf-8"),
                DAY)
        with self.assertRaises(InvalidDetailCsv):
            parse_detail_csv(
                "entry_id,account_id,kind,cents\ne1,A,bill,²\n".encode("utf-8"),
                DAY)

    def test_rejects_bad_kind(self):
        with self.assertRaises(InvalidDetailCsv):
            parse_detail_csv(
                b"entry_id,account_id,kind,cents\ne1,A,refund,1\n", DAY)

    def test_rejects_non_positive_or_oversize_cents(self):
        for bad in (
            b"entry_id,account_id,kind,cents\ne1,A,bill,0\n",
            b"entry_id,account_id,kind,cents\ne1,A,bill,-5\n",
            b"entry_id,account_id,kind,cents\ne1,A,bill,1.5\n",
            b"entry_id,account_id,kind,cents\ne1,A,bill,1000000000001\n",
        ):
            with self.assertRaises(InvalidDetailCsv):
                parse_detail_csv(bad, DAY)

    def test_rejects_duplicate_entry_id_within_day(self):
        with self.assertRaises(InvalidDetailCsv):
            parse_detail_csv(
                b"entry_id,account_id,kind,cents\n"
                b"e1,A,bill,1\ne1,B,payment,2\n", DAY)

    def test_same_entry_id_on_different_days_is_fine(self):
        # Uniqueness is scoped to one business day; the parser itself only
        # sees one day, so two independent parses both succeed.
        a = parse_detail_csv(b"entry_id,account_id,kind,cents\ne1,A,bill,1\n",
                             "2026-11-01")
        b = parse_detail_csv(b"entry_id,account_id,kind,cents\ne1,A,bill,1\n",
                             "2026-11-02")
        self.assertNotEqual(a[0].day, b[0].day)


if __name__ == "__main__":
    unittest.main(verbosity=2)

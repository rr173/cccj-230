#!/usr/bin/env python3
"""End-to-end demonstration of the correction-safe netting fact table.

Run with:  python3 -I netting_demo.py

All input is generated at runtime through the real batch-closure service
(no large data files are committed).  The walkthrough shows:

1. importing later business days first, then backfilling an earlier batch;
2. a small from-scratch-computed example whose every pairing is printed
   before and after an old-day correction, including cross-day partial
   application, payment expiry and the disappearance of a deleted bill's
   pairings;
3. a bad revision being rejected without changing any published result;
4. a long stable prefix plus an unrelated account, proving an old-day
   correction neither replays the prefix nor rewrites the bypass account.
"""

import shutil
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.path.insert(0, str(Path(__file__).resolve().parent / "tests"))

from netting import NettingService  # noqa: E402
from netting.errors import InvalidDetailCsv  # noqa: E402
from netting_helpers import (  # noqa: E402
    CONTRACT_ID,
    CONTRACT_VERSION,
    csv_bytes,
    header_only_csv,
    make_fixture,
)


def hr(title: str) -> None:
    print("\n" + "=" * 72)
    print(title)
    print("=" * 72)


def print_facts(net: NettingService, title: str) -> None:
    print(f"\n{title}")
    facts = net.read_facts()
    if not facts:
        print("  (no pairings)")
    for f in facts:
        print(f"  {f['account_id']:>4} | match {f['match_day']} | "
              f"bill {f['bill_event']:<16} <- payment "
              f"{f['payment_event']:<16} | {f['cents']:>6} cents")


def main() -> None:
    workdir = Path(tempfile.mkdtemp(prefix="netting_demo_"))
    fixture = make_fixture()
    netting_db = workdir / "netting.db"
    net = NettingService(netting_db, closure=fixture.raw_store_handle())
    cid, cver = CONTRACT_ID, CONTRACT_VERSION
    try:
        # ------------------------------------------------------------- 1
        hr("1. Backfill out of order: later days imported before earlier ones")
        fixture.publish_day("2026-11-01",
                            csv_bytes([("b1", "A", "bill", 1000)]), 1)
        fixture.publish_day("2026-11-02",
                            csv_bytes([("p1", "A", "payment", 300)]), 1)
        fixture.publish_day("2026-11-03",
                            csv_bytes([("p2", "A", "payment", 700)]), 1)
        for day in ("2026-11-03", "2026-11-02"):
            print(" ", net.apply_revision(day, 1, cid, cver)["outcome"], day)
        print("  facts before the early day is known:")
        print_facts(net, "  maintained facts")
        print(" ", net.apply_revision("2026-11-01", 1, cid, cver)["outcome"],
              "2026-11-01 (backfilled)")
        print_facts(net, "maintained facts after backfill")
        print("  matches full from-scratch replay:",
              net.read_facts() == net.full_replay())

        # Fresh database *and* fresh closure fixture for the correction
        # walkthrough so the output stays small and every pairing is easy
        # to follow (the first fixture already closed these calendar days).
        net.close()
        fixture.cleanup()
        fixture = make_fixture()
        netting_db.unlink()
        for side in (".db-wal", ".db-shm"):
            p = netting_db.with_name(netting_db.name + side)
            if p.exists():
                p.unlink()
        net = NettingService(netting_db, closure=fixture.raw_store_handle())

        # ------------------------------------------------------------- 2
        hr("2. Correction on an old day redirects an already-used payment")
        # Revision 1:
        #   D1  bill b1 1000, payment p1 600      -> p1 partially pays b1
        #   D2  payment p2 400                     -> rest of b1
        #   D3  bill b3 500
        #   D4  payment p4 200 (usable D4..D6)     -> 200 onto b3, then expires
        #   D5,D6 empty; D7 bill b6 300            -> b6 stays open
        revision1 = {
            "2026-11-01": [("b1", "A", "bill", 1000),
                           ("p1", "A", "payment", 600)],
            "2026-11-02": [("p2", "A", "payment", 400)],
            "2026-11-03": [("b3", "A", "bill", 500)],
            "2026-11-04": [("p4", "A", "payment", 200)],
            "2026-11-05": [],
            "2026-11-06": [],
            "2026-11-07": [("b6", "A", "bill", 300)],
        }
        for day, rows in revision1.items():
            fixture.publish_day(day, csv_bytes(rows) if rows
                                else header_only_csv(), 1)
            net.apply_revision(day, 1, cid, cver)
        print_facts(net, "BEFORE correction (revision 1 of every day)")
        print("  open state after D7:",
              net.open_state_checkpoint("A", "2026-11-07"))

        # Revision 2 of D1 deletes bill b1 but keeps payment p1.  p1 used to
        # settle b1; it must now be redirected onto the later bill b3, which
        # changes pairings on D3/D4 as well even though those days are untouched.
        fixture.publish_day("2026-11-01",
                            csv_bytes([("p1", "A", "payment", 600)]), 2)
        result = net.apply_revision("2026-11-01", 2, cid, cver)
        print("\napply 2026-11-01 revision 2 ->", result["outcome"],
              "| affected accounts:", result["affected_accounts"],
              "| facts rewritten:", result["facts_inserted"])
        print_facts(net, "AFTER correction (b1 deleted, p1 redirected to b3)")
        print("  open state after D7:",
              net.open_state_checkpoint("A", "2026-11-07"))
        print("  matches full from-scratch replay:",
              net.read_facts() == net.full_replay())

        # ------------------------------------------------------------- 3
        hr("3. A bad revision is rejected and cannot change published facts")
        published = net.read_facts()
        fixture.publish_day("2026-11-08",
                            b"entry_id,account_id,kind,cents\nrow,A,bill,\n", 1)
        try:
            net.apply_revision("2026-11-08", 1, cid, cver)
        except InvalidDetailCsv as exc:
            print("  rejected malformed detail.csv:", exc)
        print("  published facts unchanged:", net.read_facts() == published)
        print("  selected revisions unchanged (D8 absent):",
              all(r["business_day"] != "2026-11-08"
                  for r in net.selected_revisions()))
        print("  audit log outcomes:",
              sorted({r["outcome"] for r in net.application_log()}))

        # ------------------------------------------------------------- 4
        hr("4. Long prefix + unrelated account: no prefix replay, no bypass rewrite")
        net.close()
        fixture.cleanup()
        fixture = make_fixture()
        netting_db.unlink()
        for side in (".db-wal", ".db-shm"):
            p = netting_db.with_name(netting_db.name + side)
            if p.exists():
                p.unlink()
        net = NettingService(netting_db, closure=fixture.raw_store_handle())

        prefix = [f"2026-10-{d:02d}" for d in range(1, 13)]
        for i, day in enumerate(prefix):
            fixture.publish_day(day, csv_bytes([
                (f"b{i}", "A", "bill", 100),
                (f"p{i}", "A", "payment", 100),
                ("bx", "B", "bill", 10),
                ("px", "B", "payment", 10),
            ]), 1)
            net.apply_revision(day, 1, cid, cver)

        corrected = prefix[1]
        a_pre_before = _fact_ids(net, "A", corrected)
        b_ids_before = _fact_ids(net, "B", "9999-12-31")
        b_facts_before = net.read_facts(account_id="B")

        # Revision 2 changes only A's amount on the second prefix day.
        fixture.publish_day(corrected, csv_bytes([
            ("b1", "A", "bill", 250),
            ("bx", "B", "bill", 10),
            ("px", "B", "payment", 10),
        ]), 2)
        r = net.apply_revision(corrected, 2, cid, cver)
        print("  affected accounts (B unchanged, so absent):",
              r["affected_accounts"])
        print("  A's pre-correction fact rowids preserved:",
              _fact_ids(net, "A", corrected) == a_pre_before)
        print("  B's fact rowids preserved (bypass not rewritten):",
              _fact_ids(net, "B", "9999-12-31") == b_ids_before)
        print("  B's facts identical:",
              net.read_facts(account_id="B") == b_facts_before)
        print("  matches full from-scratch replay:",
              net.read_facts() == net.full_replay())

        hr("All demonstrations completed successfully")
    finally:
        net.close()
        fixture.cleanup()
        shutil.rmtree(workdir, ignore_errors=True)


def _fact_ids(net: NettingService, account: str, before_day: str):
    rows = net.store.read_conn.execute(
        "SELECT id FROM facts WHERE account_id=? AND match_day<? ORDER BY id",
        (account, before_day),
    ).fetchall()
    return [r["id"] for r in rows]


if __name__ == "__main__":
    main()

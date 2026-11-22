#!/usr/bin/env python3
"""End-to-end demonstration of the local netting posting handoff.

Run with:  python3 -I posting_demo.py

Everything is built at runtime through the real closure + netting services
in a temporary directory; no external ledger is ever contacted.  The
walkthrough follows the finance team's scenario:

1. a 100-cent payment first covers 60 cents of an old bill and then 40 cents
   of a new bill; the first handoff registers exactly those two forwards;
2. after correcting the old bill (30) and the new bill (70) -- the payment
   total is unchanged -- the next handoff still delivers two full reversals
   and two new forwards, each reversal naming its live forward flow;
3. applying the correction *before* the first handoff registers only the two
   current pairings (nothing downstream ever saw the old amounts);
4. a correction landing between preview and confirm invalidates the old
   preview even when facts are changed back, the re-preview is correct, and
   retrying a successful confirmation never writes twice;
5. an unrelated historical pairing is never scanned by a later handoff.
"""

import shutil
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.path.insert(0, str(Path(__file__).resolve().parent / "tests"))

from netting import NettingService  # noqa: E402
from netting_helpers import (  # noqa: E402
    CONTRACT_ID,
    CONTRACT_VERSION,
    csv_bytes,
    make_fixture,
)
from posting import NothingToPost, PostingService, PreviewStale  # noqa: E402


def hr(title: str) -> None:
    print("\n" + "=" * 74)
    print(title)
    print("=" * 74)


def show_preview(preview: dict) -> None:
    print(f"  preview {preview['preview_id'][:8]}.. for posting day "
          f"{preview['posting_day']} ({preview['entry_count']} lines)")
    for e in preview["entries"]:
        target = f" -> reverses flow {e['reverses_flow_id']}" \
            if e["reverses_flow_id"] else ""
        print(f"    {e['entry_no']:>2}. {e['line_type']:<8} "
              f"acct={e['account_id']} match={e['match_day']} "
              f"bill={e['bill_event']} payment={e['payment_event']} "
              f"{e['cents']:>4} cents{target}")


def show_batch(post: PostingService, batch_no: int) -> None:
    batch = post.read_batch(batch_no)
    print(f"  batch {batch['batch_no']} (posting day {batch['posting_day']}, "
          f"confirmed {batch['confirmed_at'][:19]}, "
          f"{batch['entry_count']} lines)")
    for e in batch["entries"]:
        target = f" -> reverses flow {e['reverses_flow_id']}" \
            if e["reverses_flow_id"] else ""
        print(f"    flow {e['flow_id']:>3} | {e['line_type']:<8} "
              f"acct={e['account_id']} match={e['match_day']} "
              f"bill={e['bill_event']} payment={e['payment_event']} "
              f"{e['cents']:>4} cents{target}")


def open_services(workdir: Path, fixture):
    db = workdir / "netting.db"
    net = NettingService(db, closure=fixture.raw_store_handle())
    post = PostingService(db)
    return db, net, post


def main() -> None:
    workdir = Path(tempfile.mkdtemp(prefix="posting_demo_"))
    fixture = make_fixture()
    cid, cver = CONTRACT_ID, CONTRACT_VERSION
    db, net, post = open_services(workdir, fixture)
    try:
        # ----------------------------------------------------------- 1
        hr("1. First handoff: 100-cent payment covers 60 old + 40 new")
        fixture.publish_day("2026-11-01", csv_bytes([
            ("b1", "A", "bill", 60),
            ("p1", "A", "payment", 100),
        ]), 1)
        net.apply_revision("2026-11-01", 1, cid, cver)
        fixture.publish_day("2026-11-02", csv_bytes([
            ("b2", "A", "bill", 40),
        ]), 1)
        net.apply_revision("2026-11-02", 1, cid, cver)
        print("  current netting facts:")
        for f in net.read_facts():
            print(f"    match={f['match_day']} bill={f['bill_event']} "
                  f"payment={f['payment_event']} {f['cents']} cents")

        preview = post.preview("2026-11-03")
        show_preview(preview)
        post.confirm(preview["preview_id"])
        show_batch(post, 1)

        # ----------------------------------------------------------- 2
        hr("2. Correction to 30/70: equal total still gives 2 reversals + 2 "
           "forwards")
        fixture.publish_day("2026-11-01", csv_bytes([
            ("b1", "A", "bill", 30),
            ("p1", "A", "payment", 100),
        ]), 2)
        net.apply_revision("2026-11-01", 2, cid, cver)
        fixture.publish_day("2026-11-02", csv_bytes([
            ("b2", "A", "bill", 70),
        ]), 2)
        net.apply_revision("2026-11-02", 2, cid, cver)
        print("  corrected facts (payment total still 100):")
        for f in net.read_facts():
            print(f"    match={f['match_day']} bill={f['bill_event']} "
                  f"payment={f['payment_event']} {f['cents']} cents")

        preview2 = post.preview("2026-11-04")
        show_preview(preview2)
        batch2 = post.confirm(preview2["preview_id"])
        show_batch(post, 2)
        reversals = [e for e in batch2["entries"]
                     if e["line_type"] == "reversal"]
        batch1_flows = {e["flow_id"] for e in post.read_batch(1)["entries"]}
        print("  every reversal points at a batch-1 live flow:",
              all(e["reverses_flow_id"] in batch1_flows for e in reversals))
        print("  batch 1 is still on record and unchanged:")
        show_batch(post, 1)
        try:
            post.preview("2026-11-05")
        except NothingToPost:
            print("  no further changes -> no empty batch is published")

        # ----------------------------------------------------------- 3
        hr("3. Correction before the first handoff registers current facts "
           "only")
        post.close()
        net.close()
        fixture.cleanup()
        shutil.rmtree(workdir, ignore_errors=True)
        workdir = Path(tempfile.mkdtemp(prefix="posting_demo_"))
        fixture = make_fixture()
        db, net, post = open_services(workdir, fixture)

        fixture.publish_day("2026-11-01", csv_bytes([
            ("b1", "A", "bill", 60), ("p1", "A", "payment", 100)]), 1)
        net.apply_revision("2026-11-01", 1, cid, cver)
        fixture.publish_day("2026-11-02", csv_bytes([
            ("b2", "A", "bill", 40)]), 1)
        net.apply_revision("2026-11-02", 1, cid, cver)
        # Corrections land before any preview/confirm has happened.
        fixture.publish_day("2026-11-01", csv_bytes([
            ("b1", "A", "bill", 30), ("p1", "A", "payment", 100)]), 2)
        net.apply_revision("2026-11-01", 2, cid, cver)
        fixture.publish_day("2026-11-02", csv_bytes([
            ("b2", "A", "bill", 70)]), 2)
        net.apply_revision("2026-11-02", 2, cid, cver)

        first_preview = post.preview("2026-11-05")
        show_preview(first_preview)
        print("  only forwards, no reversals (the 60/40 state was never "
              "handed over):",
              all(e["line_type"] == "forward"
                  for e in first_preview["entries"]))
        post.confirm(first_preview["preview_id"])

        # ----------------------------------------------------------- 4
        hr("4. Correction between preview and confirm: stale, re-preview, "
           "idempotent retry")
        fixture.publish_day("2026-11-03", csv_bytes([
            ("b3", "A", "bill", 25), ("p3", "A", "payment", 25)]), 1)
        net.apply_revision("2026-11-03", 1, cid, cver)
        pending = post.preview("2026-11-06")
        show_preview(pending)

        # Interim change, then change back to the exact same facts.
        fixture.publish_day("2026-11-03", csv_bytes([
            ("b3", "A", "bill", 26), ("p3", "A", "payment", 26)]), 2)
        net.apply_revision("2026-11-03", 2, cid, cver)
        fixture.publish_day("2026-11-03", csv_bytes([
            ("b3", "A", "bill", 25), ("p3", "A", "payment", 25)]), 3)
        net.apply_revision("2026-11-03", 3, cid, cver)
        print("  facts are back at 25/25, but an application really did "
              "change them in between.")
        try:
            post.confirm(pending["preview_id"])
        except PreviewStale as exc:
            print("  old preview rejected:", str(exc).split(";")[0])

        fresh = post.preview("2026-11-06")
        show_preview(fresh)
        batch = post.confirm(fresh["preview_id"])
        again = post.confirm(fresh["preview_id"])
        print("  retry returns the same batch:",
              again["batch_no"] == batch["batch_no"],
              "| identical flow ids:",
              [e["flow_id"] for e in again["entries"]]
              == [e["flow_id"] for e in batch["entries"]])

        # ----------------------------------------------------------- 5
        hr("5. Unrelated historical pairing is never scanned")
        # The history above is already published; add a brand-new pairing.
        before = post.handoff_state()["last_change_seq"]
        fixture.publish_day("2026-11-10", csv_bytes([
            ("bn", "N", "bill", 99), ("pn", "N", "payment", 99)]), 1)
        net.apply_revision("2026-11-10", 1, cid, cver)
        new_rows = post.store.change_rows_after(before)
        print("  change rows produced by the new application:",
              [(r["op"], r["account_id"], r["cents"]) for r in new_rows])
        print("  all of them belong to the new account N:",
              all(r["account_id"] == "N" for r in new_rows))
        preview5 = post.preview("2026-11-11")
        show_preview(preview5)
        post.confirm(preview5["preview_id"])
        print("  published batches:",
              [b["batch_no"] for b in post.list_batches()])

        hr("All demonstrations completed successfully")
    finally:
        post.close()
        net.close()
        fixture.cleanup()
        shutil.rmtree(workdir, ignore_errors=True)


if __name__ == "__main__":
    main()

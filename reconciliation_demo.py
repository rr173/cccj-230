#!/usr/bin/env python3
"""End-to-end demonstration of local general-ledger receipt reconciliation.

Run with:  python3 -I reconciliation_demo.py

All data is generated at runtime through the real netting + posting services
in a temporary directory.  The local posting library (one SQLite file also
holding the netting facts) is only ever *read* by the reconciliation
service; receipts persist in a separate SQLite file and survive a restart.

The walkthrough follows the finance team's questions:

1. a 120-cent forward flow is booked by two voucher groups as 45 + 75 cents,
   and a -120-cent reversal in another batch is fully booked too;
2. after the 75-cent voucher is moved onto another flow, querying either batch
   shows the original flow underbooked and the reversal -- still receiving the
   full -120 -- cannot close because its named original forward is unmatched;
3. a flow expecting 120 cents receives +130 and -10: equal net does not make
   it match;
4. a correction referencing an unknown flow is rejected as a whole group and
   the previously effective voucher stays in force.

Idempotent resends, lower-reversion rejection, withdrawal, restart
persistence and the two database files' separation are shown as well.
"""

import json
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
from posting import PostingService  # noqa: E402
from reconciliation import (  # noqa: E402
    MalformedReceipt,
    ReconciliationService,
    StaleRevision,
)
from reconciliation.cli import render_batch_text  # noqa: E402


def hr(title: str) -> None:
    print("\n" + "=" * 76)
    print(title)
    print("=" * 76)


def write_receipt(workdir: Path, name: str, payload: dict) -> str:
    path = workdir / name
    path.write_text(json.dumps(payload, ensure_ascii=False),
                    encoding="utf-8")
    return str(path)


def show_batch(svc: ReconciliationService, batch_no: int) -> dict:
    status = svc.batch_status(batch_no)
    print(render_batch_text(status))
    return status


def build_posting_library(workdir: Path, fixture):
    """Generate three confirmed posting batches via the genuine services.

    Batch 1: two forwards for account A (60 + 40 = 100) and one 120 forward
             for account C.
    Batch 2: a 50 forward for account D and the -120 reversal of C's forward
             (the pairing disappears from the corrected netting facts).
    Batch 3: the replacement 200 forward for account C plus a new 70 forward
             for account E.
    """
    db = workdir / "netting.db"
    net = NettingService(db, closure=fixture.raw_store_handle())
    post = PostingService(db)
    cid, cver = CONTRACT_ID, CONTRACT_VERSION

    # --- day 1: A gets 60, C gets 120 -------------------------------------
    fixture.publish_day("2026-11-01", csv_bytes([
        ("bA", "A", "bill", 60),
        ("pA", "A", "payment", 60),
        ("bC", "C", "bill", 120),
        ("pC", "C", "payment", 120),
    ]), 1)
    net.apply_revision("2026-11-01", 1, cid, cver)
    fixture.publish_day("2026-11-02", csv_bytes([
        ("bA2", "A", "bill", 40),
        ("pA2", "A", "payment", 40),
    ]), 1)
    net.apply_revision("2026-11-02", 1, cid, cver)

    batch1 = post.confirm(post.preview("2026-11-03")["preview_id"])

    # --- correction on C: the 120 pairing disappears, a 200 one appears ----
    fixture.publish_day("2026-11-01", csv_bytes([
        ("bA", "A", "bill", 60),
        ("pA", "A", "payment", 60),
        ("bC", "C", "bill", 200),
        ("pC", "C", "payment", 200),
    ]), 2)
    net.apply_revision("2026-11-01", 2, cid, cver)
    # A new, independent pairing on D.
    fixture.publish_day("2026-11-04", csv_bytes([
        ("bD", "D", "bill", 50),
        ("pD", "D", "payment", 50),
    ]), 1)
    net.apply_revision("2026-11-04", 1, cid, cver)

    batch2 = post.confirm(post.preview("2026-11-05")["preview_id"])

    # Another genuinely new pairing, so the next preview is not empty.
    fixture.publish_day("2026-11-07", csv_bytes([
        ("bE", "E", "bill", 70),
        ("pE", "E", "payment", 70),
    ]), 1)
    net.apply_revision("2026-11-07", 1, cid, cver)
    batch3 = post.confirm(post.preview("2026-11-08")["preview_id"])

    flows = {}
    for batch in (batch1, batch2, batch3):
        for entry in post.read_batch(batch["batch_no"])["entries"]:
            flows[(entry["line_type"], entry["account_id"],
                   entry["cents"])] = entry
    post.close()
    net.close()
    return db, flows


def main() -> None:
    workdir = Path(tempfile.mkdtemp(prefix="recon_demo_"))
    fixture = make_fixture()
    try:
        posting_db, flows = build_posting_library(workdir, fixture)
        receipt_db = workdir / "receipts.db"

        fwd_120 = flows[("forward", "C", 120)]["flow_id"]
        reversal_120 = flows[("reversal", "C", 120)]["flow_id"]
        fwd_50 = flows[("forward", "D", 50)]["flow_id"]
        fwd_200 = flows[("forward", "C", 200)]["flow_id"]
        fwd_a60 = flows[("forward", "A", 60)]["flow_id"]
        fwd_a40 = flows[("forward", "A", 40)]["flow_id"]

        print(f"本地过账库: {posting_db}")
        print(f"回执持久库: {receipt_db}")
        print("关键流水: 120 正向 =", fwd_120,
              "| -120 冲销 =", reversal_120,
              "| 200 新正向 =", fwd_200)

        svc = ReconciliationService(str(posting_db), str(receipt_db))

        # ============================================================ 1
        hr("1. 45 + 75 两组凭证记齐 120 正向；另一批次 -120 冲销也足额")
        write_receipt(workdir, "g1.json", {
            "receipt_id": "VGRP45", "revision": 1,
            "lines": [{"line_id": "l1", "flow_id": str(fwd_120),
                       "signed_cents": 45}],
        })
        write_receipt(workdir, "g2.json", {
            "receipt_id": "VGRP75", "revision": 1,
            "lines": [{"line_id": "l1", "flow_id": str(fwd_120),
                       "signed_cents": 75}],
        })
        write_receipt(workdir, "g3.json", {
            "receipt_id": "VGRPREV", "revision": 1,
            "lines": [{"line_id": "l1", "flow_id": str(reversal_120),
                       "signed_cents": -120}],
        })
        for name in ("g1.json", "g2.json", "g3.json"):
            result = svc.import_receipt_file(workdir / name)
            print(f"  导入 {name}: {result['outcome']}, "
                  f"组 {result['receipt_id']} 版本 {result['revision']}, "
                  f"{result['line_count']} 行")
        # The other flows of batches 1 and 2 also need receipts before the
        # batches can close.
        write_receipt(workdir, "g4.json", {
            "receipt_id": "VGRPA", "revision": 1,
            "lines": [
                {"line_id": "a60", "flow_id": str(fwd_a60),
                 "signed_cents": 60},
                {"line_id": "a40", "flow_id": str(fwd_a40),
                 "signed_cents": 40},
            ],
        })
        write_receipt(workdir, "g5.json", {
            "receipt_id": "VGRPD", "revision": 1,
            "lines": [{"line_id": "d50", "flow_id": str(fwd_50),
                       "signed_cents": 50}],
        })
        write_receipt(workdir, "g6.json", {
            "receipt_id": "VGRPC200", "revision": 1,
            "lines": [{"line_id": "c200", "flow_id": str(fwd_200),
                       "signed_cents": 200}],
        })
        svc.import_receipt_file(workdir / "g4.json")
        svc.import_receipt_file(workdir / "g5.json")
        svc.import_receipt_file(workdir / "g6.json")

        status1 = show_batch(svc, 1)
        status2 = show_batch(svc, 2)
        print("  结论: 批次1 可关账 =", status1["closeable"],
              "| 批次2 可关账 =", status2["closeable"])
        assert status1["closeable"] and status2["closeable"]

        # ============================================================ 2
        hr("2. 75 分凭证改挂到 200 分新流水后，两个批次都不能关账")
        # Same-revision resend with different content must be rejected; the
        # correction therefore arrives as a strictly higher revision.
        bad_resend = write_receipt(workdir, "g2_bad_resend.json", {
            "receipt_id": "VGRP75", "revision": 1,
            "lines": [{"line_id": "l1", "flow_id": str(fwd_200),
                       "signed_cents": 75}],
        })
        try:
            svc.import_receipt_file(bad_resend)
        except StaleRevision as exc:
            print("  同版改内容被拒绝:", exc)
        write_receipt(workdir, "g2_v2.json", {
            "receipt_id": "VGRP75", "revision": 2,
            "lines": [{"line_id": "l1", "flow_id": str(fwd_200),
                       "signed_cents": 75}],
        })
        moved = svc.import_receipt_file(workdir / "g2_v2.json")
        print("  更高版本替换成功:", moved["outcome"],
              "版本", moved["revision"])

        status1 = show_batch(svc, 1)
        status2 = show_batch(svc, 2)
        print("  结论: 批次1 可关账 =", status1["closeable"],
              "| 批次2 可关账 =", status2["closeable"])
        assert not status1["closeable"] and not status2["closeable"]
        b1_120 = next(f for f in status1["flows"]
                      if f["flow_id"] == fwd_120)
        assert b1_120["same_direction_cents"] == 45
        assert b1_120["blockers"][0]["reason"] == "amount_mismatch"
        b2_rev = next(f for f in status2["flows"]
                      if f["flow_id"] == reversal_120)
        assert b2_rev["amount_matched"]
        assert b2_rev["blockers"][0]["reason"] == \
            "reversed_original_unmatched"
        assert b2_rev["blockers"][0]["original_flow_id"] == fwd_120

        # ============================================================ 3
        hr("3. 应记 120 的流水收到 +130 与 -10：净额相等也不对平")
        # Withdraw VGRP75 entirely (empty array) and turn the remaining 45
        # group's new revision into a +130/-10 pair against the 120 flow:
        # the net is 120, exactly the expectation, yet opposite-direction
        # booking must prevent the flow from matching.
        write_receipt(workdir, "g2_withdraw.json", {
            "receipt_id": "VGRP75", "revision": 3, "lines": [],
        })
        svc.import_receipt_file(workdir / "g2_withdraw.json")
        write_receipt(workdir, "g1_v2.json", {
            "receipt_id": "VGRP45", "revision": 2,
            "lines": [
                {"line_id": "l1", "flow_id": str(fwd_120),
                 "signed_cents": 130},
                {"line_id": "l2", "flow_id": str(fwd_120),
                 "signed_cents": -10},
            ],
        })
        svc.import_receipt_file(workdir / "g1_v2.json")
        status1 = show_batch(svc, 1)
        b1_120 = next(f for f in status1["flows"]
                      if f["flow_id"] == fwd_120)
        print("  120 流水: 同向 =", b1_120["same_direction_cents"],
              "反向 =", b1_120["opposite_direction_cents"],
              "| 对平 =", b1_120["amount_matched"])
        assert b1_120["same_direction_cents"] == 130
        assert b1_120["opposite_direction_cents"] == 10
        assert not b1_120["amount_matched"]
        assert not status1["closeable"]

        # ============================================================ 4
        hr("4. 带未知流水的更正整组拒绝，旧凭证继续有效")
        unknown_correction = write_receipt(workdir, "g1_bad.json", {
            "receipt_id": "VGRP45", "revision": 3,
            "lines": [
                {"line_id": "l1", "flow_id": str(fwd_120),
                 "signed_cents": 45},
                {"line_id": "ghost", "flow_id": "999999",
                 "signed_cents": 85},
            ],
        })
        try:
            svc.import_receipt_file(unknown_correction)
        except MalformedReceipt as exc:
            print("  整组拒绝:", exc)
        current = svc.get_group("VGRP45")
        print("  VGRP45 当前仍为版本", current["current_revision"],
              "行内容:",
              [(l["line_id"], l["flow_id"], l["signed_cents"])
               for l in current["lines"]])
        assert current["current_revision"] == 2
        status1 = svc.batch_status(1)
        b1_120 = next(f for f in status1["flows"]
                      if f["flow_id"] == fwd_120)
        assert b1_120["same_direction_cents"] == 130
        assert b1_120["opposite_direction_cents"] == 10

        # ============================================================ 5
        hr("5. 版本纪律、幂等、撤回与重启持久化")
        svc.close()
        lower = write_receipt(workdir, "g2_old.json", {
            # VGRP75 is currently at revision 3 (withdrawn); an older revision
            # must never roll it back.
            "receipt_id": "VGRP75", "revision": 2,
            "lines": [{"line_id": "l1", "flow_id": str(fwd_200),
                       "signed_cents": 75}],
        })
        try:
            svc = ReconciliationService(str(posting_db), str(receipt_db))
            svc.import_receipt_file(lower)
        except StaleRevision as exc:
            print("  更低版本被拒绝:", exc)
        # Idempotent resend: reordered lines, same flow+amount per line id.
        write_receipt(workdir, "g4_resend.json", {
            "receipt_id": "VGRPA", "revision": 1,
            "lines": [
                {"line_id": "a40", "flow_id": str(fwd_a40),
                 "signed_cents": 40},
                {"line_id": "a60", "flow_id": str(fwd_a60),
                 "signed_cents": 60},
            ],
        })
        again = svc.import_receipt_file(workdir / "g4_resend.json")
        print("  同版乱序重送:", again["outcome"],
              "（不重复计入）")
        assert again["outcome"] == "idempotent"
        print("  审计日志:")
        for row in svc.import_log():
            print(f"    #{row['id']} {row['receipt_id']} rev"
                  f"{row['revision']} -> {row['outcome']}"
                  + (f" ({row['reason']})" if row["reason"] else ""))

        svc.close()
        svc2 = ReconciliationService(str(posting_db), str(receipt_db))
        reopened = svc2.batch_status(2)
        print("  重启后批次2 仍可查询，可关账 =", reopened["closeable"])
        assert not reopened["closeable"]
        svc2.close()

        # The posting library file was never modified by reconciliation.
        print("  回执库独立文件:", receipt_db.name,
              "| 过账库保持独立:", posting_db.name)
        hr("全部演示完成")
    finally:
        fixture.cleanup()
        shutil.rmtree(workdir, ignore_errors=True)


if __name__ == "__main__":
    main()

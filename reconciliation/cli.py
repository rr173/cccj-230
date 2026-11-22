"""Command line interface so separate OS processes can drive reconciliation.

Arguments come from a UTF-8 JSON file and the script runs under
``python -I``::

    python -I reconciliation/cli.py --args import.json
    python -I reconciliation/cli.py --args batch.json

Commands:
  import       import one receipt document (file path or inline document)
  batch        reconcile one posting batch and report closeability
  groups       list currently effective voucher groups
  group        show one group's current version and lines
  log          show the append-only import audit log

Common args:
  posting_db   fixed local posting SQLite library (opened read-only)
  receipt_db   separate SQLite file persisting receipt versions

Exit codes: 0 success, 2 usage/args error, 3 business rejection
(stale revision, malformed receipt, unknown batch).
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from reconciliation import (  # noqa: E402
    BatchNotFound,
    MalformedReceipt,
    ReconciliationError,
    ReconciliationService,
    StaleRevision,
)


def _load(path: str) -> dict:
    return json.loads(Path(path).read_text(encoding="utf-8"))


def _emit(payload: dict) -> None:
    print(json.dumps(payload, ensure_ascii=False, indent=2))


def render_batch_text(status: dict) -> str:
    """Human-readable close report for a finance review."""
    lines = [
        f"批次 {status['batch_no']}  过账日 {status['posting_day']}  "
        f"确认于 {status['confirmed_at'][:19]}",
        f"能否关账: {'可以关账' if status['closeable'] else '不可关账'}  "
        f"（流水 {status['flow_count']} 条，金额对平 "
        f"{status['amount_matched_count']} 条）",
    ]
    for flow in status["flows"]:
        kind = "正向" if flow["line_type"] == "forward" else "冲销"
        lines.append(
            f"  - 流水 {flow['flow_id']} [{kind}] 应记 "
            f"{flow['expected_signed_cents']:+d} 分 | 同向 "
            f"{flow['same_direction_cents']} 分 | 反向 "
            f"{flow['opposite_direction_cents']} 分 | "
            f"金额对平: {'是' if flow['amount_matched'] else '否'}"
        )
        for contrib in flow["contributions"]:
            lines.append(
                f"      凭证组 {contrib['receipt_id']} 版本 "
                f"{contrib['revision']} 行 {contrib['line_id']}: "
                f"{contrib['signed_cents']:+d} 分"
            )
        for blocker in flow["blockers"]:
            if blocker["reason"] == "amount_mismatch":
                lines.append(
                    "      阻止原因: 金额未对平（"
                    f"问题={blocker['problem']}，应记绝对值 "
                    f"{blocker['expected_abs_cents']}，同向 "
                    f"{blocker['same_direction_cents']}，反向 "
                    f"{blocker['opposite_direction_cents']}）"
                )
            else:
                lines.append(
                    "      阻止原因: 冲销引用的原正向流水 "
                    f"{blocker['original_flow_id']} 尚未金额对平"
                    f"（同向 {blocker['original_same_direction_cents']}，"
                    f"反向 {blocker['original_opposite_direction_cents']}）"
                )
    if status["referenced_origins"]:
        lines.append("  跨批次引用的原正向流水:")
        for origin in status["referenced_origins"]:
            lines.append(
                f"      原流水 {origin['flow_id']}（所在批次 "
                f"{origin['batch_no']}）应记 "
                f"{origin['expected_signed_cents']:+d}，同向 "
                f"{origin['same_direction_cents']}，反向 "
                f"{origin['opposite_direction_cents']}，对平: "
                f"{'是' if origin['amount_matched'] else '否'}"
            )
    return "\n".join(lines)


def main(argv=None) -> int:
    argv = argv if argv is not None else sys.argv[1:]
    if len(argv) != 2 or argv[0] != "--args":
        print(json.dumps({"ok": False,
                          "error": "usage: cli --args args.json"}))
        return 2
    try:
        args = _load(argv[1])
    except (OSError, json.JSONDecodeError) as exc:
        print(json.dumps({"ok": False, "error": "args_unreadable",
                          "message": str(exc)}))
        return 2

    if "posting_db" not in args or "receipt_db" not in args \
            or "command" not in args:
        print(json.dumps({"ok": False,
                          "error": "posting_db, receipt_db and command "
                                   "required"}))
        return 2

    service = ReconciliationService(args["posting_db"], args["receipt_db"])
    try:
        command = args["command"]

        if command == "import":
            if "receipt_file" in args:
                result = service.import_receipt_file(args["receipt_file"])
            elif "receipt" in args:
                result = service.import_receipt(args["receipt"])
            else:
                print(json.dumps(
                    {"ok": False,
                     "error": "import needs receipt_file or receipt"}))
                return 2
            _emit({"ok": True, "import": result})
            return 0

        if command == "batch":
            status = service.batch_status(int(args["batch_no"]))
            if args.get("format") == "text":
                # Plain-text finance report; deliberately not a JSON envelope.
                print(render_batch_text(status))
            else:
                _emit({"ok": True, "batch_status": status})
            return 0
        if command == "groups":
            _emit({"ok": True, "groups": service.list_groups()})
            return 0

        if command == "group":
            result = service.get_group(args["receipt_id"])
            if result is None:
                print(json.dumps(
                    {"ok": False, "error": "UnknownGroup",
                     "message": f"no group {args['receipt_id']!r}"}))
                return 3
            _emit({"ok": True, "group": result})
            return 0

        if command == "log":
            _emit({"ok": True, "import_log": service.import_log()})
            return 0

        print(json.dumps({"ok": False,
                          "error": f"unknown command {command!r}"}))
        return 2
    except (MalformedReceipt, StaleRevision, BatchNotFound) as exc:
        print(json.dumps(
            {"ok": False, "error": type(exc).__name__,
             "message": str(exc)}, ensure_ascii=False))
        return 3
    except ReconciliationError as exc:
        print(json.dumps(
            {"ok": False, "error": type(exc).__name__,
             "message": str(exc)}, ensure_ascii=False))
        return 2
    finally:
        service.close()


if __name__ == "__main__":
    raise SystemExit(main())

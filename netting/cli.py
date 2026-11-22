"""Command line interface so separate OS processes can drive netting.

Arguments come from a JSON file (mirroring the closure service CLI) and the
script runs under ``python -I``::

    python -I netting/cli.py --args apply.json
    python -I netting/cli.py --args read.json

Commands:
  apply       select an explicit (business_date, revision) and fold it in
  read        print maintained facts (optional account_id / business_day)
  revisions   list the selected revision of every applied day
  log         print the application audit log
  replay      print a from-scratch recomputation for cross checking
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from netting import NettingError, NettingService  # noqa: E402


def _load(path: str) -> dict:
    return json.loads(Path(path).read_text(encoding="utf-8"))


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
    command = args.pop("command")

    if "netting_db" not in args or "closure_data_dir" not in args:
        print(json.dumps({"ok": False,
                          "error": "netting_db and closure_data_dir required"}))
        return 2

    service = NettingService(
        args["netting_db"],
        closure_data_dir=args["closure_data_dir"],
        closure_inbound_dir=args.get("closure_inbound_dir"),
    )
    try:
        if command == "apply":
            try:
                result = service.apply_revision(
                    business_day=args["business_day"],
                    revision=args["revision"],
                    contract_id=args["contract_id"],
                    contract_version=args.get("contract_version"),
                    contract_digest=args.get("contract_digest"),
                )
                print(json.dumps({"ok": True, "result": result}, ensure_ascii=False))
                return 0
            except NettingError as exc:
                print(json.dumps(
                    {"ok": False, "error": type(exc).__name__,
                     "message": str(exc)}, ensure_ascii=False))
                return 3

        if command == "read":
            rows = service.read_facts(
                account_id=args.get("account_id"),
                business_day=args.get("business_day"),
            )
            print(json.dumps({"ok": True, "facts": rows}, ensure_ascii=False))
            return 0

        if command == "revisions":
            print(json.dumps({"ok": True,
                              "revisions": service.selected_revisions()},
                             ensure_ascii=False))
            return 0

        if command == "log":
            print(json.dumps(
                {"ok": True,
                 "log": service.application_log(args.get("business_day"))},
                ensure_ascii=False))
            return 0

        if command == "replay":
            print(json.dumps({"ok": True, "facts": service.full_replay()},
                             ensure_ascii=False))
            return 0

        print(json.dumps({"ok": False,
                          "error": f"unknown command {command!r}"}))
        return 2
    finally:
        service.close()


if __name__ == "__main__":
    raise SystemExit(main())

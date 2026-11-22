"""Command line interface so separate OS processes can drive the service.

Every command takes its arguments from a JSON file so no untrusted input is
parsed from a shell command line.  The script inserts its own package
directory on ``sys.path`` and is intended to run under ``python -I``.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from batch_closure import BatchClosureService, compile_contract  # noqa: E402
from batch_closure.errors import BatchClosureError  # noqa: E402
from batch_closure.evidence import verify_bundle  # noqa: E402


def _load(path):
    return json.loads(Path(path).read_text(encoding="utf-8"))


def main(argv=None) -> int:
    argv = argv if argv is not None else sys.argv[1:]
    if len(argv) != 2 or argv[0] != "--args":
        print(json.dumps({"ok": False, "error": "usage: cli --args args.json"}))
        return 2
    args = _load(argv[1])
    command = args.pop("command")

    if command == "verify-bundle":
        print(json.dumps(verify_bundle(args["out_dir"]), indent=2))
        return 0

    svc = BatchClosureService(args["data_dir"], args.get("inbound"))
    try:
        if command == "compile":
            contract = svc.compile_contract(args["contract"])
            print(json.dumps({"ok": True, "digest": contract.digest}))
            return 0

        contract = svc.load_contract(
            args["contract_id"], args.get("contract_version")
        )
        key = args["key"]

        if command == "receive":
            result = svc.receive_file(
                contract, key, args["member"], args["source"],
                expected_digests=args.get("expected_digests"),
            )
            print(json.dumps({"ok": True, "result": result}))
            return 0
        if command == "status":
            print(json.dumps({"ok": True, "status": svc.get_status(contract, key)}))
            return 0
        if command == "evaluate":
            print(json.dumps({"ok": True, "result": svc.evaluate_closure(contract, key)}))
            return 0
        if command == "claim":
            try:
                result = svc.claim(contract, key, processor=args.get("processor", "cli"))
                print(json.dumps({"ok": True, "claim": result}))
                return 0
            except BatchClosureError as exc:
                print(json.dumps({"ok": False, "error": type(exc).__name__,
                                  "message": str(exc)}))
                return 3
        if command == "verify-token":
            try:
                print(json.dumps({"ok": True, "result": svc.verify_token(args["token"])}))
                return 0
            except BatchClosureError as exc:
                print(json.dumps({"ok": False, "error": type(exc).__name__,
                                  "message": str(exc)}))
                return 3
        if command == "complete":
            print(json.dumps({"ok": True,
                              "result": svc.complete_claim(args["token"],
                                                           args.get("status", "COMPLETED"))}))
            return 0
        if command == "evidence":
            out = svc.export_evidence(contract, key, args["out_dir"])
            print(json.dumps({"ok": True, "out_dir": str(out)}))
            return 0
        if command == "replay":
            print(json.dumps({"ok": True, "result": svc.replay(contract, key)}))
            return 0
        print(json.dumps({"ok": False, "error": f"unknown command {command!r}"}))
        return 2
    finally:
        svc.close()


if __name__ == "__main__":
    raise SystemExit(main())

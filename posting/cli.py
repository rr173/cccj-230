"""Command line interface so separate OS processes can drive the handoff.

Arguments come from a JSON file and the script runs under ``python -I``::

    python -I posting/cli.py --args preview.json
    python -I posting/cli.py --args confirm.json
    python -I posting/cli.py --args read_batch.json

Commands:
  preview      build a preview for posting_day
  confirm      publish a preview (idempotent for an already confirmed one)
  read_batch   print one confirmed batch by batch_no
  batches      list all confirmed batch headers
  state        print the handoff watermark snapshot
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from posting import NothingToPost, PostingError, PostingService  # noqa: E402


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

    if "netting_db" not in args or "command" not in args:
        print(json.dumps({"ok": False,
                          "error": "netting_db and command required"}))
        return 2

    service = PostingService(args["netting_db"])
    try:
        command = args["command"]

        if command == "preview":
            try:
                result = service.preview(args["posting_day"])
                print(json.dumps({"ok": True, "preview": result},
                                 ensure_ascii=False))
                return 0
            except NothingToPost as exc:
                # Business-level "nothing to do": distinct error name, not a
                # failure of the database.
                print(json.dumps(
                    {"ok": True, "nothing_to_post": True,
                     "message": str(exc)}, ensure_ascii=False))
                return 0

        if command == "confirm":
            result = service.confirm(args["preview_id"])
            print(json.dumps({"ok": True, "batch": result}, ensure_ascii=False))
            return 0

        if command == "read_batch":
            result = service.read_batch(int(args["batch_no"]))
            print(json.dumps({"ok": True, "batch": result}, ensure_ascii=False))
            return 0

        if command == "batches":
            print(json.dumps({"ok": True,
                              "batches": service.list_batches()},
                             ensure_ascii=False))
            return 0

        if command == "state":
            print(json.dumps({"ok": True,
                              "state": service.handoff_state()},
                             ensure_ascii=False))
            return 0

        print(json.dumps({"ok": False,
                          "error": f"unknown command {command!r}"}))
        return 2
    except PostingError as exc:
        print(json.dumps(
            {"ok": False, "error": type(exc).__name__,
             "message": str(exc)}, ensure_ascii=False))
        return 3
    finally:
        service.close()


if __name__ == "__main__":
    raise SystemExit(main())

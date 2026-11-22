import hashlib
import json
import shutil
import sys
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from batch_closure import BatchClosureService, compile_contract

tmp = Path(tempfile.mkdtemp(prefix="smoke_"))
inbound = tmp / "inbound"
inbound.mkdir()

contract = compile_contract({
    "contract_id": "smoke",
    "version": "1",
    "batch_key": ["business_date"],
    "members": {
        "manifest.json": {"required": True, "role": "manifest",
                          "digest_algorithms": ["sha256"]},
        "detail.csv": {"required": True, "role": "data",
                       "digest_algorithms": ["sha256"]},
        "summary.json": {"required": False, "role": "summary",
                         "digest_algorithms": ["sha256"]},
    },
    "max_file_size": 10 * 1024 * 1024,
    "deadline": {"type": "relative", "offset_days": 1, "hour": 12, "minute": 0},
    "late_window_minutes": 60,
    "on_missing_required": "reject",
})
key = {"business_date": "2026-11-22T00:00:00+00:00"}
base = datetime(2026, 11, 23, tzinfo=timezone.utc)

manifest = {"members": [
    {"name": "manifest.json", "path": "manifest.json"},
    {"name": "detail.csv", "path": "detail.csv"},
    {"name": "summary.json", "path": "summary.json"},
]}
(inbound / "manifest.json").write_bytes(json.dumps(manifest).encode())
(inbound / "detail.csv").write_bytes(b"a,b\n1,2\n")
(inbound / "summary.json").write_bytes(json.dumps({"digests": {}}).encode())

class Clock:
    def __init__(self, t): self.t = t
    def __call__(self): return self.t

clock = Clock(base + timedelta(hours=9))
svc = BatchClosureService(tmp / "data", inbound, clock=clock)
svc.compile_contract(contract.to_dict())

r1 = svc.receive_file(contract, key, "manifest.json", inbound / "manifest.json")
r2 = svc.receive_file(contract, key, "detail.csv", inbound / "detail.csv")
print("received:", r1["accepted"], r2["accepted"])

ev = svc.evaluate_closure(contract, key)
print("early eval (before deadline):", ev["closed"], ev["reason"])

clock.t = base + timedelta(hours=12, minutes=1)
ev = svc.evaluate_closure(contract, key)
print("deadline eval:", ev["closed"], ev["state"], ev["revision"])

# optional late file inside window
clock.t = base + timedelta(hours=12, minutes=30)
r3 = svc.receive_file(contract, key, "summary.json", inbound / "summary.json")
print("optional late:", r3["accepted"])
ev2 = svc.evaluate_closure(contract, key)
print("revision:", ev2["closed"], ev2["revision"], ev2["impact"])

claim = svc.claim(contract, key)
print("claim rev:", claim["revision"], "token len", len(claim["token"]))
print("verify:", svc.verify_token(claim["token"])["valid"])

status = svc.get_status(contract, key)
print("state:", status["state"], "order:", status["arrival_order"])

replay = svc.replay(contract, key)
print("replay matches:", replay["matches"], "closures:", len(replay["stored_closures"]))

out = svc.export_evidence(contract, key, tmp / "evidence")
print("evidence files:", sorted(p.name for p in out.iterdir()))
svc.close()

# reopen: recovery
svc2 = BatchClosureService(tmp / "data", inbound, clock=clock)
st2 = svc2.get_status(contract, key)
print("recovered state:", st2["state"], st2["current_revision"])
shutil.rmtree(tmp)
print("SMOKE OK")

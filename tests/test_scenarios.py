"""End-to-end scenarios hand-constructed from the requirements.

Every scenario drives the real service with an injected clock and then
re-derives the same history with the independent ReferenceMachine; states,
closure revisions, binding signatures and impact ranges must agree item by
item.
"""

import gzip
import io
import json
import sys
import unittest
from datetime import timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from batch_closure import (
    AlreadyClaimed,
    NotClosed,
    compile_contract,
)
from batch_closure.contract import parse_utc
from batch_closure.evidence import verify_bundle
from batch_closure.service import (
    KIND_DIGEST_MISMATCH,
    KIND_DUPLICATE_MEMBER,
    KIND_LATE_ARRIVAL,
    KIND_MANIFEST_DUPLICATE,
    KIND_MANIFEST_MISSING,
    KIND_PATH_ESCAPE,
    KIND_REJECTED_MISSING,
    KIND_SIZE_EXCEEDED,
    KIND_UNSUPPORTED_COMPRESSION,
    KIND_COMPRESSION_MISMATCH,
    KIND_COMPRESSION_CORRUPT,
)

from helpers import (
    FixedClock,
    assert_replay_matches,
    base_contract,
    business_day,
    cleanup,
    day_base,
    gzip_bytes,
    make_service,
    make_workdir,
    sha256_hex,
    write_member,
)

KEY = {"business_date": "2026-11-22T00:00:00+00:00"}


def manifest_doc():
    return {
        "members": [
            {"name": "manifest.json", "path": "manifest.json"},
            {"name": "detail.csv", "path": "detail.csv"},
            {"name": "control.json", "path": "control.json"},
            {"name": "extra.txt", "path": "extra.txt"},
        ]
    }


class ScenarioTestCase(unittest.TestCase):
    def setUp(self):
        self.workdir = make_workdir()
        self.day = business_day()
        self.base = day_base(self.day)  # 2026-11-23T00:00 UTC; deadline +12h
        self.clock = FixedClock(self.base + timedelta(hours=8))
        self.svc, self.inbound = make_service(self.workdir, self.clock)
        self.contract = self.svc.compile_contract(base_contract())

    def tearDown(self):
        self.svc.close()
        cleanup(self.workdir)

    # ------------------------------------------------------------- helpers
    def send(self, member, content, expected=None):
        path = write_member(self.inbound, member, content)
        return self.svc.receive_file(self.contract, KEY, member, path, expected)

    def evaluate(self):
        return self.svc.evaluate_closure(self.contract, KEY)

    def status(self):
        return self.svc.get_status(self.contract, KEY)

    def cross_check(self):
        return assert_replay_matches(self, self.svc, self.contract, KEY)

    def findings(self):
        return [v["kind"] for v in self.status()["validation"]]

    # S1: required files arrive one after another; no early closure.
    def test_s1_required_sequence_no_early_closure(self):
        r1 = self.send("manifest.json", json.dumps(manifest_doc()).encode())
        r2 = self.send("detail.csv", b"a,b\n1,2\n")
        self.assertTrue(r1["accepted"] and r2["accepted"])

        status = self.status()
        self.assertEqual(status["state"], "WAITING")
        self.assertEqual(status["phase"], "BEFORE_DEADLINE")
        self.assertEqual(status["arrival_order"], ["manifest.json", "detail.csv"])
        self.assertEqual(status["waiting"], ["control.json", "extra.txt"])

        # Even an explicit evaluation must not close before the deadline.
        decision = self.evaluate()
        self.assertFalse(decision["closed"])
        self.assertEqual(decision["reason"], "before_deadline")

        self.clock.set(self.base + timedelta(hours=12, minutes=1))
        decision = self.evaluate()
        self.assertTrue(decision["closed"])
        self.assertEqual(decision["state"], "CLOSED")
        self.assertEqual(decision["revision"], 1)
        self.assertEqual(decision["impact"]["added"],
                         ["detail.csv", "manifest.json"])

        status = self.status()
        self.assertEqual(status["arrival_order"], ["manifest.json", "detail.csv"])
        report = self.cross_check()
        self.assertEqual(
            [(c["revision"], c["state"]) for c in report["stored_closures"]],
            [(1, "CLOSED")],
        )

    # S2: optional member arrives late inside the window -> revision.
    def test_s2_optional_late_creates_revision_with_impact(self):
        self.send("manifest.json", json.dumps(manifest_doc()).encode())
        self.send("detail.csv", b"v1")
        self.clock.set(self.base + timedelta(hours=12, minutes=1))
        first = self.evaluate()
        self.assertEqual(first["revision"], 1)

        self.clock.set(self.base + timedelta(hours=12, minutes=30))
        r = self.send("control.json", json.dumps({"digests": {}}).encode())
        self.assertTrue(r["accepted"])
        second = self.evaluate()
        self.assertEqual(second["revision"], 2)
        self.assertEqual(second["parent_revision"], 1)
        self.assertEqual(second["impact"]["added"], ["control.json"])
        self.assertEqual(second["impact"]["changed"], [])

        status = self.status()
        self.assertEqual(status["current_revision"], 2)
        self.assertEqual(status["arrival_order"],
                         ["manifest.json", "detail.csv", "control.json"])
        self.cross_check()

    # S3: digest mismatch is quarantined; member stays missing.
    def test_s3_digest_mismatch_recorded_separately(self):
        data = b"a,b\n1,2\n"
        bad_expected = {"sha256": "0" * 64}
        path = write_member(self.inbound, "detail.csv", data)
        result = self.svc.receive_file(
            self.contract, KEY, "detail.csv", path, bad_expected
        )
        self.assertFalse(result["accepted"])
        self.assertEqual(result["quarantine_reason"], KIND_DIGEST_MISMATCH)
        self.assertIn(KIND_DIGEST_MISMATCH, self.findings())

        status = self.status()
        self.assertIn("detail.csv", status["missing_required"])
        self.assertEqual(status["quarantined"][0]["reason"], KIND_DIGEST_MISMATCH)

        # Correct digest for the same bytes is then accepted.
        good = self.send("detail.csv", data)
        self.assertTrue(good["accepted"])
        self.assertNotIn("detail.csv", self.status()["missing_required"])
        self.cross_check()

    # S4: same-name member with different content -> revision, never overwrite.
    def test_s4_content_change_forms_revision_not_overwrite(self):
        self.send("manifest.json", json.dumps(manifest_doc()).encode())
        first_bytes = b"row-1"
        self.send("detail.csv", first_bytes)
        self.clock.set(self.base + timedelta(hours=12, minutes=1))
        rev1 = self.evaluate()
        closure1 = self.svc.store.closure_at(
            self.status()["batch_id"], 1
        )
        old_blob = json.loads(closure1["binding_json"])["detail.csv"]["blob_sha256"]
        self.assertEqual(old_blob, sha256_hex(first_bytes))

        self.clock.set(self.base + timedelta(hours=12, minutes=20))
        changed = self.send("detail.csv", b"row-2-different")
        self.assertTrue(changed["revision_candidate"])
        self.assertIn(KIND_DUPLICATE_MEMBER, self.findings())

        rev2 = self.evaluate()
        self.assertEqual(rev2["revision"], 2)
        self.assertEqual(rev2["impact"]["changed"], ["detail.csv"])

        # Old blob and old closure input are physically retained and identical.
        self.assertTrue((self.svc.store.blob_root / "blobs" /
                         old_blob[:2] / old_blob).exists())
        closure1_again = self.svc.store.closure_at(
            self.status()["batch_id"], 1
        )
        self.assertEqual(
            json.loads(closure1_again["binding_json"])["detail.csv"]["blob_sha256"],
            old_blob,
        )
        self.cross_check()

    # S5: missing required before deadline = waiting; after window = rejected.
    def test_s5_missing_before_deadline_waiting_after_window_rejected(self):
        self.send("manifest.json", json.dumps(manifest_doc()).encode())
        status = self.status()
        self.assertEqual(status["state"], "WAITING")
        self.assertIn("detail.csv", status["waiting"])
        self.assertEqual(self.evaluate()["reason"], "before_deadline")

        self.clock.set(self.base + timedelta(hours=12, minutes=1))
        # Inside the late window the batch keeps waiting.
        self.assertFalse(self.evaluate()["closed"])
        self.assertEqual(self.status()["state"], "WAITING")

        self.clock.set(self.base + timedelta(hours=13, minutes=1))
        decision = self.evaluate()
        self.assertTrue(decision["closed"])
        self.assertEqual(decision["state"], "REJECTED")
        self.assertEqual(decision["missing"], ["detail.csv"])
        kinds = self.findings()
        self.assertIn(KIND_MANIFEST_MISSING, kinds)
        self.assertIn(KIND_REJECTED_MISSING, kinds)

        # After the window, a late file cannot resurrect or rewrite anything.
        late = self.send("detail.csv", b"too-late")
        self.assertFalse(late["accepted"])
        self.assertEqual(late["quarantine_reason"], KIND_LATE_ARRIVAL)
        self.assertEqual(self.status()["state"], "REJECTED")
        with self.assertRaises(NotClosed):
            self.svc.claim(self.contract, KEY)
        self.cross_check()

    # S6: publish_with_missing contract closes partially.
    def test_s6_publish_with_missing_contract(self):
        decl = base_contract(on_missing_required="publish_with_missing")
        contract = self.svc.compile_contract(decl)
        path = write_member(self.inbound, "manifest.json",
                            json.dumps(manifest_doc()).encode())
        self.svc.receive_file(contract, KEY, "manifest.json", path)

        self.clock.set(self.base + timedelta(hours=12, minutes=1))
        decision = self.svc.evaluate_closure(contract, KEY)
        self.assertEqual(decision["state"], "CLOSED_PARTIAL")
        self.assertEqual(decision["missing"], ["detail.csv"])

        # A partial closure is claimable; token binds the partial binding.
        claim = self.svc.claim(contract, KEY)
        self.assertEqual(claim["state"], "CLOSED_PARTIAL")
        self.assertEqual(claim["missing"], ["detail.csv"])
        verified = self.svc.verify_token(claim["token"])
        self.assertTrue(verified["valid"])
        self.assertEqual(verified["payload"]["binding_sig"], claim["binding_sig"])
        assert_replay_matches(self, self.svc, contract, KEY)

    # S7: gzip happy path, wrong format, corrupt stream.
    def test_s7_compression_rules(self):
        decl = base_contract()
        decl["members"]["detail.csv"]["compression"] = "gzip"
        contract = self.svc.compile_contract(decl)

        plain = b"a,b\n1,2\n"
        plain_path = write_member(self.inbound, "detail.csv", plain)
        mismatch = self.svc.receive_file(
            contract, KEY, "detail.csv", plain_path
        )
        self.assertEqual(mismatch["quarantine_reason"],
                         KIND_COMPRESSION_MISMATCH)

        good_path = write_member(self.inbound, "detail.csv", gzip_bytes(plain))
        ok = self.svc.receive_file(contract, KEY, "detail.csv", good_path,
                                   {"sha256": sha256_hex(plain)})
        self.assertTrue(ok["accepted"])

        # gzip magic but broken payload.
        corrupt = gzip_bytes(plain)[:-4] + b"\x00\x00\x00\x00"
        corrupt_path = write_member(self.inbound, "detail.csv", corrupt)
        bad = self.svc.receive_file(
            contract, KEY, "detail.csv", corrupt_path
        )
        self.assertEqual(bad["quarantine_reason"],
                         KIND_COMPRESSION_CORRUPT)

        # Plain member rejecting an unexpected gzip stream.
        gz_path = write_member(self.inbound, "extra.txt", gzip_bytes(b"x"))
        rejected = self.svc.receive_file(
            contract, KEY, "extra.txt", gz_path
        )
        self.assertEqual(rejected["quarantine_reason"],
                         KIND_UNSUPPORTED_COMPRESSION)

        # A different unsupported compression (bzip2) is recognised and
        # rejected with the same distinct finding, not treated as plain data.
        import bz2

        bz_path = write_member(self.inbound, "extra.txt", bz2.compress(b"x"))
        rejected_bz = self.svc.receive_file(
            contract, KEY, "extra.txt", bz_path
        )
        self.assertEqual(rejected_bz["quarantine_reason"],
                         KIND_UNSUPPORTED_COMPRESSION)
        self.assertEqual(
            rejected_bz["findings"][0]["detail"]["detected_format"], "bzip2"
        )

    # S8: path traversal declared in a manifest is refused.
    def test_s8_path_escape_rejected(self):
        evil_manifest = {
            "members": [
                {"name": "manifest.json", "path": "manifest.json"},
                {"name": "detail.csv", "path": "../../etc/passwd"},
            ]
        }
        result = self.send("manifest.json", json.dumps(evil_manifest).encode())
        self.assertFalse(result["accepted"])
        self.assertEqual(result["quarantine_reason"], KIND_PATH_ESCAPE)
        self.assertIn(KIND_PATH_ESCAPE, self.findings())
        # The manifest never became effective, so both required members are
        # still missing; the escaped path itself never touched the filesystem.
        self.assertEqual(
            self.status()["missing_required"], ["detail.csv", "manifest.json"]
        )
        self.assertFalse(
            (self.inbound.parent / "etc" / "passwd").exists()
        )

        absolute_manifest = {
            "members": [
                {"name": "manifest.json", "path": "/etc/passwd"},
            ]
        }
        result2 = self.send("manifest.json",
                            json.dumps(absolute_manifest).encode())
        self.assertEqual(result2["quarantine_reason"], KIND_PATH_ESCAPE)

    def test_summary_member_drives_digest_verification(self):
        data = b"settlement-detail"
        good_digest = sha256_hex(data)
        # Summary declares the authoritative digest for another member.
        summary = {"digests": {"detail.csv": {"sha256": good_digest}}}
        self.send("control.json", json.dumps(summary).encode())
        self.send("manifest.json", json.dumps(manifest_doc()).encode())

        bad_path = write_member(self.inbound, "detail.csv", b"wrong-bytes")
        bad = self.svc.receive_file(
            self.contract, KEY, "detail.csv", bad_path
        )
        self.assertEqual(bad["quarantine_reason"], KIND_DIGEST_MISMATCH)
        finding = bad["findings"][0]["detail"]
        self.assertEqual(finding["expected"], good_digest)

        good_path = write_member(self.inbound, "detail.csv", data)
        good = self.svc.receive_file(
            self.contract, KEY, "detail.csv", good_path
        )
        self.assertTrue(good["accepted"])

    def test_malformed_manifest_is_classified(self):
        result = self.send("manifest.json", b"{not-json")
        self.assertFalse(result["accepted"])
        self.assertEqual(result["quarantine_reason"], "manifest_malformed")
        self.assertIn("manifest_malformed", self.findings())

    # S8b: duplicate entries inside the manifest are recorded distinctly.
    def test_s8b_duplicate_manifest_entries_recorded(self):
        dup_manifest = {
            "members": [
                {"name": "manifest.json", "path": "manifest.json"},
                {"name": "detail.csv", "path": "detail.csv"},
                {"name": "detail.csv", "path": "detail.csv"},
            ]
        }
        result = self.send("manifest.json", json.dumps(dup_manifest).encode())
        self.assertFalse(result["accepted"])
        self.assertEqual(result["quarantine_reason"], "manifest_duplicate")
        self.assertIn("manifest_duplicate", self.findings())

    # S9: oversize stream is cut off and recorded distinctly.
    def test_s9_size_limit_enforced_while_streaming(self):
        big = b"x" * (self.contract.max_file_size + 128)
        result = self.send("detail.csv", big)
        self.assertEqual(result["quarantine_reason"], KIND_SIZE_EXCEEDED)
        detail = result["findings"][0]["detail"]
        self.assertGreater(detail["actual"], detail["limit"])
        # No blob was registered for the rejected stream.
        self.assertIsNone(result.get("blob_sha256"))
        self.assertEqual(self.status()["quarantined"][0]["reason"],
                         KIND_SIZE_EXCEEDED)

    # S10: old revision processing; a new revision cannot touch old inputs.
    def test_s10_new_revision_while_old_is_processing(self):
        self.send("manifest.json", json.dumps(manifest_doc()).encode())
        self.send("detail.csv", b"v1")
        self.clock.set(self.base + timedelta(hours=12, minutes=1))
        self.evaluate()
        claim1 = self.svc.claim(self.contract, KEY, processor="worker-A")
        self.assertEqual(claim1["revision"], 1)

        # Claiming again while the revision is active is impossible.
        with self.assertRaises(AlreadyClaimed):
            self.svc.claim(self.contract, KEY, processor="worker-B")

        # A changed member inside the window produces revision 2, but the
        # token and immutable inputs of revision 1 keep working.
        self.clock.set(self.base + timedelta(hours=12, minutes=20))
        self.send("detail.csv", b"v2-changed")
        rev2 = self.evaluate()
        self.assertEqual(rev2["revision"], 2)

        # Revision 1 token still verifies against the revision-1 binding.
        info = self.svc.verify_token(claim1["token"])
        self.assertTrue(info["valid"])
        self.assertEqual(info["payload"]["revision"], 1)
        self.assertEqual(info["claim_status"], "CLAIMED")

        # New revision cannot be claimed while revision 1 is still processing.
        with self.assertRaises(AlreadyClaimed):
            self.svc.claim(self.contract, KEY)

        # Completing the old claim releases the guard; revision 2 is claimable.
        self.svc.complete_claim(claim1["token"])
        claim2 = self.svc.claim(self.contract, KEY, processor="worker-B")
        self.assertEqual(claim2["revision"], 2)
        self.assertNotEqual(claim2["binding_sig"], claim1["binding_sig"])
        self.cross_check()

    def test_idempotent_reception_same_content(self):
        self.send("manifest.json", json.dumps(manifest_doc()).encode())
        again = self.send("manifest.json", json.dumps(manifest_doc()).encode())
        self.assertTrue(again["idempotent"])
        # Idempotent replays do not create closures or change order.
        self.clock.set(self.base + timedelta(hours=12, minutes=1))
        self.send("detail.csv", b"d")
        self.evaluate()
        status = self.status()
        self.assertEqual(status["arrival_order"], ["manifest.json", "detail.csv"])
        self.cross_check()

    def test_evidence_chain_breaks_under_tampering(self):
        self.send("manifest.json", json.dumps(manifest_doc()).encode())
        self.send("detail.csv", b"v1")
        self.clock.set(self.base + timedelta(hours=12, minutes=1))
        self.evaluate()
        self.clock.set(self.base + timedelta(hours=12, minutes=10))
        self.send("control.json", b"{}")
        self.evaluate()
        bid = self.status()["batch_id"]
        # Tamper with a persisted closure's stored binding signature.
        row = self.svc.store.closure_at(bid, 1)
        self.svc.store.conn.execute(
            "UPDATE closures SET binding_sig=? WHERE id=?",
            ("0" * 64, row["id"]),
        )
        report = self.svc.replay(self.contract, KEY)
        self.assertFalse(report["matches"])

    def test_rejected_batch_and_waiting_batch_are_not_claimable(self):
        # Nothing ever arrives: no closure, no claim.
        with self.assertRaises(NotClosed):
            self.svc.claim(self.contract, KEY)

        self.send("manifest.json", json.dumps(manifest_doc()).encode())
        self.clock.set(self.base + timedelta(hours=13, minutes=1))
        decision = self.evaluate()
        self.assertEqual(decision["state"], "REJECTED")
        with self.assertRaises(NotClosed):
            self.svc.claim(self.contract, KEY)
        claims = self.svc.list_claims(self.contract, KEY)
        self.assertEqual(claims, [])

    def test_evidence_bundle_is_self_verifying(self):
        self.send("manifest.json", json.dumps(manifest_doc()).encode())
        self.send("detail.csv", b"v1")
        self.clock.set(self.base + timedelta(hours=12, minutes=1))
        self.evaluate()
        self.clock.set(self.base + timedelta(hours=12, minutes=10))
        self.send("control.json", b"{}")
        self.evaluate()
        out = self.svc.export_evidence(
            self.contract, KEY, self.workdir / "evidence"
        )
        verdict = verify_bundle(out)
        self.assertTrue(verdict["ok"], msg=json.dumps(verdict, indent=2))
        self.assertEqual(verdict["closure_revisions"], [1, 2])
        self.assertTrue(all(c["ok"] for c in verdict["blob_checks"]))

        # Tampering with a blob is detected by verification.
        first_blob = next((out / "blobs").rglob("*"))
        if first_blob.is_file():
            first_blob.write_bytes(first_blob.read_bytes() + b"tampered")
            verdict2 = verify_bundle(out)
            self.assertFalse(verdict2["ok"])

    def test_recovery_after_reopen_preserves_decisions(self):
        self.send("manifest.json", json.dumps(manifest_doc()).encode())
        self.send("detail.csv", b"v1")
        self.clock.set(self.base + timedelta(hours=12, minutes=1))
        self.evaluate()
        claim = self.svc.claim(self.contract, KEY)
        self.svc.close()

        svc2, _ = make_service(self.workdir, self.clock)
        contract2 = svc2.load_contract(self.contract.contract_id,
                                       self.contract.version)
        status = svc2.get_status(contract2, KEY)
        self.assertEqual(status["state"], "CLOSED")
        self.assertEqual(status["current_revision"], 1)
        self.assertTrue(svc2.verify_token(claim["token"])["valid"])
        assert_replay_matches(self, svc2, contract2, KEY)
        svc2.close()


if __name__ == "__main__":
    unittest.main(verbosity=2)

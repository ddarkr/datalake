"""Synthetic regression tests for vehicle raw CAN pipeline (no hardware).

Covers: frame codec provenance, segment rotation/recovery, MF4 bus-logging
round-trip via MF4Reader (payload/FD/RTR/error, pre-rename verify gate,
crash-resume reuse, torn-tail salvage vs mid-corruption quarantine),
deterministic remote keys, triple-upload ack rule (all locals kept on any
failure, manifest-pin drift fail-closed to quarantine, partial sets
invisible), replay safety gate.
"""
import json
import os
import sys
import tempfile
import unittest

from scripts.vehicle.raw import raw_recorder as rec
from scripts.vehicle.raw import raw_upload as upl

SYN = [{"seq": i + 1, "twall_ns": 1_700_000_000_000_000_000 + i * 1_000_000,
        "tcan": 1700000000.0 + i * 0.001, "bus": "can0",
        "id": 0x100 + (i % 7), "ext": bool(i % 2), "rtr": False,
        "err": False, "fd": False, "brs": False, "esi": False,
        "dlc": 8, "data": bytes([(i + b) % 256 for b in range(8)])}
       for i in range(25)]
SYN[3] = dict(SYN[3], rtr=True, dlc=4, data=b"")  # RTR: requested len only
SYN[5] = dict(SYN[5], id=0x1FFFFFFF, ext=True, dlc=3, data=b"\x01\x02\x03")
FD = {"seq": 101, "twall_ns": 1_700_000_000_100_000_000, "tcan": 1700000000.1,
      "bus": "can1", "id": 0x123, "ext": True, "rtr": False, "err": False,
      "fd": True, "brs": True, "esi": False, "dlc": 64,
      "data": bytes(range(64))}
ERR = {"seq": 102, "twall_ns": 1_700_000_000_200_000_000, "tcan": 1700000000.2,
       "bus": "can0", "id": 0x200, "ext": False, "rtr": False, "err": True,
       "fd": False, "brs": False, "esi": False, "dlc": 8,
       "data": b"\xaa" * 8}


class CodecTest(unittest.TestCase):
    def test_roundtrip_preserves_provenance(self):
        for f in SYN:
            line = rec.encode_frame(f["seq"], f["twall_ns"], f["tcan"],
                                    f["bus"], f["id"], f["ext"], f["rtr"],
                                    f["err"], f["fd"], f["brs"], f["esi"],
                                    f["dlc"], f["data"])
            d = rec.decode_frame(json.loads(json.dumps(line)))
            for k in ("seq", "twall_ns", "tcan", "bus", "id", "ext",
                      "rtr", "err", "fd", "brs", "esi", "dlc"):
                self.assertEqual(d[k], f[k], k)
            self.assertEqual(d["data"], f["data"])

    def test_decode_rejects_schema_and_dlc(self):
        with self.assertRaises(ValueError):
            rec.decode_frame({"v": 999})
        bad = json.loads(json.dumps(rec.encode_frame(
            1, 2, 0.0, "can0", 1, False, False, False,
            False, False, False, 2, b"\x01\x02")))
        bad["dlc"] = 8
        with self.assertRaises(ValueError):
            rec.decode_frame(bad)


class SegmentTest(unittest.TestCase):
    def test_rotate_and_recover(self):
        with tempfile.TemporaryDirectory() as spool:
            rec.ensure_dirs(spool)
            w = rec.SegWriter(spool, "m3", 600, 200, 1000, 9999.0)
            closed = None
            for f in SYN:
                line = json.dumps(rec.encode_frame(
                    f["seq"], f["twall_ns"], f["tcan"], f["bus"], f["id"],
                    f["ext"], f["rtr"], f["err"], f["fd"], f["brs"],
                    f["esi"], f["dlc"], f["data"]), separators=(",", ":"))
                r = w.write(line)
                if r is not None:
                    closed = r
            tail = w.rotate()
            self.assertTrue(closed or tail)
            names = os.listdir(os.path.join(spool, "closed"))
            self.assertTrue(any(n.endswith(".jsonl") for n in names))
            self.assertEqual(os.listdir(os.path.join(spool, "active")), [])

    def test_crash_recovery(self):
        with tempfile.TemporaryDirectory() as spool:
            rec.ensure_dirs(spool)
            dirs = rec.seg_dirs(spool)
            with open(os.path.join(dirs["active"], "m3_x.jsonl"), "w") as fh:
                fh.write('{"v":1}\n')
            with open(os.path.join(dirs["active"], "torn.tmp"), "w") as fh:
                fh.write("partial")
            recovered, swept = rec.recover_spool(spool)
            self.assertEqual((recovered, swept), (1, 1))
            self.assertTrue(os.path.exists(
                os.path.join(dirs["closed"], "m3_x.jsonl")))


def _write_ingress(cp, frames):
    with open(cp, "w", encoding="utf-8") as fh:
        for f in frames:
            fh.write(json.dumps(rec.encode_frame(
                f["seq"], f["twall_ns"], f["tcan"], f["bus"],
                f["id"], f["ext"], f["rtr"], f["err"], f["fd"],
                f["brs"], f["esi"], f["dlc"], f["data"]),
                separators=(",", ":")) + "\n")


class MF4Test(unittest.TestCase):
    def test_finalize_roundtrip(self):
        try:
            import can.io.mf4  # noqa
        except ImportError:
            self.skipTest("python-can/asammdf not installed")
        with tempfile.TemporaryDirectory() as spool:
            rec.ensure_dirs(spool)
            dirs = rec.seg_dirs(spool)
            stem = "m3_20260921T000000Z_deadbeef_001"
            cp = os.path.join(dirs["closed"], stem + ".jsonl")
            _write_ingress(cp, SYN)
            out = rec.finalize_segment(cp, dirs["sealed"], spool=spool)
            self.assertTrue(out.endswith(".mf4") and os.path.exists(out))
            # finalize never deletes: the closed JSONL stays until remote ack.
            self.assertTrue(os.path.exists(cp))
            sidecar = os.path.join(dirs["sealed"], stem + ".ingress.jsonl.gz")
            self.assertTrue(os.path.exists(sidecar))
            with open(out + ".manifest.json", encoding="utf-8") as fh:
                man = json.load(fh)
            self.assertEqual(man["frames"], len(SYN))
            self.assertEqual(
                (man["seq_first"], man["seq_last"]), (1, len(SYN)))
            # Exact per-frame provenance: sidecar gunzips to the closed bytes.
            import gzip
            with open(sidecar, "rb") as fh:
                body = gzip.decompress(fh.read())
            with open(cp, "rb") as fh:
                self.assertEqual(body, fh.read())
            # Standard bus logging: MF4Reader must replay every payload byte.
            from can.io.mf4 import MF4Reader
            reader = MF4Reader(out)
            try:
                got = list(reader)
            finally:
                reader.stop()
            self.assertEqual(len(got), len(SYN))
            for m, f in zip(got, SYN):
                self.assertEqual(m.arbitration_id, f["id"])
                self.assertEqual(m.is_extended_id, f["ext"])
                self.assertEqual(m.is_remote_frame, f["rtr"])
                self.assertEqual(m.is_error_frame, f["err"])
                if f["rtr"]:
                    self.assertEqual(m.dlc, f["dlc"])
                    self.assertEqual(bytes(m.data), b"")
                else:
                    self.assertEqual(bytes(m.data), bytes(f["data"]))
                self.assertAlmostEqual(m.timestamp, f["twall_ns"] / 1e9,
                                       places=3)

    def test_finalize_fd_error_rtr_roundtrip(self):
        try:
            import can.io.mf4  # noqa
        except ImportError:
            self.skipTest("python-can/asammdf not installed")
        frames = [FD, ERR, dict(SYN[3])]
        with tempfile.TemporaryDirectory() as spool:
            rec.ensure_dirs(spool)
            dirs = rec.seg_dirs(spool)
            stem = "m3_20260921T000001Z_deadbeef_002"
            cp = os.path.join(dirs["closed"], stem + ".jsonl")
            _write_ingress(cp, frames)
            out = rec.finalize_segment(cp, dirs["sealed"], spool=spool)
            from can.io.mf4 import MF4Reader
            reader = MF4Reader(out)
            try:
                got = {m.arbitration_id: m for m in reader}
            finally:
                reader.stop()
            fd = got[0x123]
            self.assertTrue(fd.is_fd)
            self.assertTrue(fd.bitrate_switch)
            self.assertEqual(bytes(fd.data), bytes(range(64)))  # payload64
            err = got[0x200]
            self.assertTrue(err.is_error_frame)
            self.assertEqual(bytes(err.data), b"\xaa" * 8)
            rtr = got[0x103]
            self.assertTrue(rtr.is_remote_frame)
            self.assertEqual(rtr.dlc, 4)
            self.assertEqual(bytes(rtr.data), b"")

    def test_finalize_never_publishes_unverified(self):
        try:
            import can.io.mf4  # noqa
        except ImportError:
            self.skipTest("python-can/asammdf not installed")
        with tempfile.TemporaryDirectory() as spool:
            rec.ensure_dirs(spool)
            dirs = rec.seg_dirs(spool)
            stem = "m3_20260921T000002Z_deadbeef_003"
            cp = os.path.join(dirs["closed"], stem + ".jsonl")
            _write_ingress(cp, SYN)
            real_verify = rec.verify_sealed_mf4
            try:
                def boom(path, frames):
                    raise ValueError("simulated verify failure")
                rec.verify_sealed_mf4 = boom
                with self.assertRaises(ValueError):
                    rec.finalize_segment(cp, dirs["sealed"], spool=spool)
            finally:
                rec.verify_sealed_mf4 = real_verify
            sealed = os.listdir(dirs["sealed"])
            self.assertFalse(any(n.endswith(".mf4") for n in sealed),
                             sealed)  # uploader sees nothing
            self.assertTrue(os.path.exists(cp))  # ingress kept for retry

    def test_finalize_resume_reuses_verified_triple(self):
        try:
            import can.io.mf4  # noqa
        except ImportError:
            self.skipTest("python-can/asammdf not installed")
        with tempfile.TemporaryDirectory() as spool:
            rec.ensure_dirs(spool)
            dirs = rec.seg_dirs(spool)
            stem = "m3_20260921T000003Z_deadbeef_004"
            cp = os.path.join(dirs["closed"], stem + ".jsonl")
            _write_ingress(cp, SYN)
            out = rec.finalize_segment(cp, dirs["sealed"], spool=spool)
            # Crash between MF4 rename and manifest write: delete manifest +
            # sidecar, keep orphan MF4 + closed JSONL. Resume must rebuild
            # from the closed JSONL (never trust the orphan bytes, never
            # refuse forever).
            os.unlink(out + ".manifest.json")
            os.unlink(os.path.join(dirs["sealed"],
                                   stem + ".ingress.jsonl.gz"))
            out2 = rec.finalize_segment(cp, dirs["sealed"], spool=spool)
            self.assertEqual(out2, out)
            self.assertTrue(os.path.exists(out + ".manifest.json"))
            self.assertTrue(os.path.exists(
                os.path.join(dirs["sealed"], stem + ".ingress.jsonl.gz")))
            rec.verify_sealed_mf4(out2, SYN)  # rebuilt bytes verify clean
            # Verified triple present: next call reuses the immutable MF4.
            mtime = os.path.getmtime(out2)
            out3 = rec.finalize_segment(cp, dirs["sealed"], spool=spool)
            self.assertEqual(out3, out)
            self.assertEqual(os.path.getmtime(out), mtime)
            self.assertTrue(os.path.exists(cp))  # still kept for remote ack

    def test_torn_tail_salvaged_mid_corruption_quarantined(self):
        with tempfile.TemporaryDirectory() as spool:
            rec.ensure_dirs(spool)
            dirs = rec.seg_dirs(spool)
            good = [json.dumps(rec.encode_frame(
                f["seq"], f["twall_ns"], f["tcan"], f["bus"], f["id"],
                f["ext"], f["rtr"], f["err"], f["fd"], f["brs"], f["esi"],
                f["dlc"], f["data"]), separators=(",", ":")) for f in SYN[:5]]
            cp = os.path.join(dirs["closed"], "m3_torn.jsonl")
            with open(cp, "wb") as fh:
                fh.write(("\n".join(good) + "\n{truncated").encode())
            frames, torn = rec.read_ingress_frames(cp)
            self.assertTrue(torn)
            self.assertEqual(len(frames), 5)
            # Newline-terminated malformed tail is corruption, not a tear.
            cp3 = os.path.join(dirs["closed"], "m3_term.jsonl")
            with open(cp3, "wb") as fh:
                fh.write(("\n".join(good) + "\n{bad}\n").encode())
            frames3, torn3 = rec.read_ingress_frames(cp3)
            self.assertIsNone(frames3)
            self.assertFalse(torn3)
            cp2 = os.path.join(dirs["closed"], "m3_mid.jsonl")
            with open(cp2, "w", encoding="utf-8") as fh:
                fh.write(good[0] + "\n{bad}\n" + good[1] + "\n")
            frames2, torn2 = rec.read_ingress_frames(cp2)
            self.assertIsNone(frames2)
            self.assertFalse(torn2)
            try:
                import can.io.mf4  # noqa
            except ImportError:
                return
            with self.assertRaises(ValueError):
                rec.finalize_segment(cp2, dirs["sealed"], spool=spool)
            self.assertTrue(os.path.exists(
                os.path.join(spool, "quarantine", "m3_mid.jsonl")))
            self.assertFalse(os.path.exists(cp2))
            cp4 = os.path.join(dirs["closed"], "m3_utf8.jsonl")
            damaged = (good[0] + "\n").encode() + b"\xff\n"
            with open(cp4, "wb") as fh:
                fh.write(damaged)
            with self.assertRaises(ValueError):
                rec.finalize_segment(cp4, dirs["sealed"], spool=spool)
            with open(os.path.join(spool, "quarantine", "m3_utf8.jsonl"),
                      "rb") as fh:
                self.assertEqual(fh.read(), damaged)
            self.assertEqual(os.listdir(dirs["sealed"]), [])

    def test_finalize_empty_whitespace_torn_tail_returns_none(self):
        try:
            import can.io.mf4  # noqa
        except ImportError:
            self.skipTest("python-can/asammdf not installed")
        with tempfile.TemporaryDirectory() as spool:
            rec.ensure_dirs(spool)
            dirs = rec.seg_dirs(spool)
            for name, body in (
                    ("m3_empty.jsonl", b""),
                    ("m3_blank.jsonl", b"  \n\n \t\n"),
                    ("m3_torn_only.jsonl", b'{"v":'),
            ):
                cp = os.path.join(dirs["closed"], name)
                with open(cp, "wb") as fh:
                    fh.write(body)
                stats = {}
                out = rec.finalize_segment(cp, dirs["sealed"], spool=spool,
                                           stats=stats)
                self.assertIsNone(out)  # prior empty policy: None, no triple
                self.assertFalse(os.path.exists(cp))  # empty closed consumed
                stem = name[:-len(".jsonl")]
                for suffix in (".mf4", ".ingress.jsonl.gz",
                               ".mf4.manifest.json", ".stage.tmp"):
                    self.assertFalse(os.path.exists(
                        os.path.join(dirs["sealed"], stem + suffix)))
                leftovers = [n for n in os.listdir(dirs["sealed"])
                             if n.startswith(stem + ".")]
                self.assertEqual(leftovers, [])

    def test_finalize_combined_tmp_budget_fails_closed(self):
        try:
            import can.io.mf4  # noqa
        except ImportError:
            self.skipTest("python-can/asammdf not installed")
        with tempfile.TemporaryDirectory() as spool:
            rec.ensure_dirs(spool)
            dirs = rec.seg_dirs(spool)
            frames = BoundedMemoryTest()._frames(60)
            stem = "m3_20260921T000017Z_deadbeef_017"
            cp = os.path.join(dirs["closed"], stem + ".jsonl")
            _write_ingress(cp, frames)
            # Calibrate on the same spill path (mem_frames=1): full
            # finalize succeeds, so every partial sum fits a huge budget.
            from unittest.mock import patch
            stats = {}
            match_bytes = []
            note_live = rec.SortedFrameStore._note_live

            def measure_live(store, extra_paths=()):
                total = note_live(store, extra_paths)
                stage_size = sum(os.path.getsize(p) for p in extra_paths)
                match_bytes.append(total - stage_size)
                return total

            with patch.object(rec.SortedFrameStore, "_note_live", measure_live):
                out = rec.finalize_segment(cp, dirs["sealed"], spool=spool,
                                           mem_frames=1, stats=stats)
            combined = int(stats["peak_temp_bytes"])
            staged_bytes = os.path.getsize(out)
            for suffix in (".mf4", ".ingress.jsonl.gz",
                           ".mf4.manifest.json"):
                os.unlink(os.path.join(dirs["sealed"], stem + suffix))
            store = rec.SortedFrameStore(dirs["sealed"], stem, 1, 2 << 30)
            try:
                for f in frames:
                    store.add(f)
                store.finish_staging()
                sort_only = rec._enforce_tmp_budget(store.dir, 2 << 30)
            finally:
                store.destroy()
            # Keep both partials below the cap, with room for MF4 metadata
            # size variation between writes; the combined footprint exceeds it.
            partial_peak = max(sort_only + staged_bytes, max(match_bytes))
            cap = (partial_peak + combined) // 2
            self.assertLess(sort_only + staged_bytes, cap)
            self.assertLess(max(match_bytes), cap)
            with self.assertRaises(rec.NoSpace):
                rec.finalize_segment(cp, dirs["sealed"], spool=spool,
                                     mem_frames=1, tmp_max_bytes=cap)
            self.assertTrue(os.path.exists(cp))  # source kept for retry
            self.assertFalse(os.path.exists(
                os.path.join(dirs["sealed"], stem + ".mf4")))
            self.assertFalse(os.path.exists(
                os.path.join(dirs["sealed"], stem + ".mf4.manifest.json")))
            self.assertFalse(os.path.exists(
                os.path.join(dirs["sealed"], stem + ".ingress.jsonl.gz")))
            self.assertFalse(os.path.exists(
                os.path.join(dirs["sealed"], stem + ".stage.tmp")))

class _Missing(Exception):
    """Mimics botocore ClientError for a missing key (NoSuchKey)."""
    def __init__(self):
        super().__init__("missing key")
        self.response = {"Error": {"Code": "NoSuchKey"},
                         "ResponseMetadata": {"HTTPStatusCode": 404}}


class FakeBody:
    def __init__(self, data):
        self.data = data
        self.closed = False

    def iter_chunks(self, chunk_size=1 << 20):
        yield self.data

    def close(self):
        self.closed = True


class FakeS3:
    def __init__(self, corrupt=False, fail_put=0, unreadable=False):
        self.store = {}
        self.corrupt = corrupt
        self.fail_put = fail_put
        self.unreadable = unreadable
        self.puts = 0

    def put_object(self, Bucket, Key, Body, ContentLength, Metadata):
        if self.fail_put > 0:
            self.fail_put -= 1
            raise ConnectionError("boom")
        self.puts += 1
        self.store[(Bucket, Key)] = Body.read()

    def get_object(self, Bucket, Key):
        if self.unreadable:
            raise ConnectionError("denied")
        try:
            data = self.store[(Bucket, Key)]
        except KeyError:
            raise _Missing()
        if self.corrupt:
            data = b"tampered" + data
        return {"Body": FakeBody(data), "ContentLength": len(data)}


class UploadTest(unittest.TestCase):
    def _triple(self, spool, name="m3_20260921T140000Z_deadbeef_001",
                tweak=None):
        try:
            import can.io.mf4  # noqa
        except ImportError:
            self.skipTest("python-can/asammdf not installed")
        rec.ensure_dirs(spool)
        dirs = rec.seg_dirs(spool)
        cp = os.path.join(dirs["closed"], name + ".jsonl")
        frames = SYN if tweak is None else [dict(f) for f in SYN]
        if tweak is not None:
            frames[0] = dict(frames[0], data=bytes([tweak]) + frames[0]["data"][1:])
        with open(cp, "w", encoding="utf-8") as fh:
            for f in frames:
                fh.write(json.dumps(rec.encode_frame(
                    f["seq"], f["twall_ns"], f["tcan"], f["bus"],
                    f["id"], f["ext"], f["rtr"], f["err"], f["fd"],
                    f["brs"], f["esi"], f["dlc"], f["data"]),
                    separators=(",", ":")) + "\n")
        out = rec.finalize_segment(cp, dirs["sealed"], spool=spool)
        stem = os.path.basename(out)[:-len(".mf4")]
        sealed = dirs["sealed"]
        return (out, os.path.join(sealed, stem + ".ingress.jsonl.gz"),
                out + ".manifest.json", cp)

    def test_deterministic_key(self):
        md = "ab" * 32
        k1 = upl.remote_key("/s/m3_20260921T140000Z_deadbeef_001.mf4",
                            "m3", md)
        k2 = upl.remote_key("/s/m3_20260921T140000Z_deadbeef_001.mf4",
                            "m3", md)
        self.assertEqual(k1, k2)
        self.assertEqual(k1,
                         "raw/vehicle/m3/can/2026/09/21/" + md + "/"
                         "m3_20260921T140000Z_deadbeef_001.mf4")
        other = upl.remote_key("/s/m3_20260921T140000Z_deadbeef_001.mf4",
                               "m3", "cd" * 32)
        self.assertNotEqual(k1, other)  # same stem, changed triple: new key
        self.assertTrue(other.endswith(
            "m3_20260921T140000Z_deadbeef_001.mf4"))  # stem unchanged

    def test_verified_triple_deletes_all_locals(self):
        with tempfile.TemporaryDirectory() as spool:
            p, s, m, cp = self._triple(spool)
            md = upl.sha256_file(m)
            cli = FakeS3()
            n = upl.run_once(cli, "b", spool, "m3", 0)
            self.assertEqual(n, 1)
            for gone in (p, s, m, cp):  # full remote ack: all locals go
                self.assertFalse(os.path.exists(gone))
            key = upl.remote_key(p, "m3", md)
            self.assertIn(("b", key), cli.store)
            self.assertIn(("b", upl.sidecar_key(key)), cli.store)
            self.assertIn(("b", upl.manifest_key(key)), cli.store)

    def test_mismatch_keeps_all_locals(self):
        with tempfile.TemporaryDirectory() as spool:
            p, s, m, cp = self._triple(spool)
            cli = FakeS3(corrupt=True)
            n = upl.run_once(cli, "b", spool, "m3", 0)
            self.assertEqual(n, 1)
            for kept in (p, s, m, cp):  # unacked: everything stays
                self.assertTrue(os.path.exists(kept))

    def test_manifest_leg_failure_keeps_all(self):
        class ManifestDown(FakeS3):
            def put_object(self, Bucket, Key, Body, ContentLength, Metadata):
                if Key.endswith(".manifest.json"):
                    raise ConnectionError("manifest down")
                return super().put_object(Bucket, Key, Body, ContentLength,
                                          Metadata)
        with tempfile.TemporaryDirectory() as spool:
            p, s, m, cp = self._triple(spool)
            cli = ManifestDown()
            n = upl.run_once(cli, "b", spool, "m3", 0)
            self.assertEqual(n, 1)
            for kept in (p, s, m, cp):  # MF4 acked but triple not: all stay
                self.assertTrue(os.path.exists(kept))

    def test_sealed_drift_quarantines_never_uploads(self):
        with tempfile.TemporaryDirectory() as spool:
            p, s, m, cp = self._triple(spool)
            with open(p, "r+b") as fh:  # post-seal corruption on disk
                fh.seek(100)
                fh.write(b"\xff")
            cli = FakeS3()
            n = upl.run_once(cli, "b", spool, "m3", 0)
            self.assertEqual(n, 1)
            self.assertEqual(cli.store, {})  # nothing backed up as truth
            for q in (p, s, m, cp):
                self.assertFalse(os.path.exists(q))  # moved, not deleted
                self.assertTrue(os.path.exists(
                    os.path.join(spool, "quarantine", os.path.basename(q))))

    def test_partial_set_invisible(self):
        with tempfile.TemporaryDirectory() as spool:
            sealed = os.path.join(spool, "sealed")
            os.makedirs(sealed)
            p = os.path.join(sealed, "m3_lonely.mf4")
            with open(p, "wb") as fh:
                fh.write(os.urandom(64))
            cli = FakeS3()
            self.assertEqual(upl.pending(spool), [])
            self.assertEqual(upl.run_once(cli, "b", spool, "m3", 0), 0)
            self.assertTrue(os.path.exists(p))  # unverified: never touched
            self.assertEqual(cli.store, {})

    def test_retry_then_success(self):
        with tempfile.TemporaryDirectory() as spool:
            sealed = os.path.join(spool, "sealed")
            os.makedirs(sealed)
            p = os.path.join(sealed, "m3_retry.mf4")
            with open(p, "wb") as fh:
                fh.write(os.urandom(64))
            digest = upl.sha256_file(p)
            cli = FakeS3(fail_put=2)
            self.assertTrue(upl.upload_verified(
                cli, "b", upl.remote_key(p, "m3", "ab" * 32), p, digest, 3))

    def test_conflicting_live_object_kept_never_overwritten(self):
        with tempfile.TemporaryDirectory() as spool:
            p, s, m, cp = self._triple(spool)
            cli = FakeS3()
            key = upl.remote_key(p, "m3", upl.sha256_file(m))
            for leg_path, leg_key in ((p, key),
                                      (s, upl.sidecar_key(key)),
                                      (m, upl.manifest_key(key))):
                with open(leg_path, "rb") as fh:
                    cli.store[("b", leg_key)] = b"CONFLICT" + fh.read()
            before = dict(cli.store)
            self.assertEqual(upl.run_once(cli, "b", spool, "m3", 0), 1)
            self.assertEqual(cli.store, before)  # conflict: bytes untouched
            for kept in (p, s, m, cp):
                self.assertTrue(os.path.exists(kept))

    def test_unreadable_live_object_retried_never_deleted(self):
        with tempfile.TemporaryDirectory() as spool:
            p, s, m, cp = self._triple(spool)
            cli = FakeS3(unreadable=True)
            self.assertEqual(upl.run_once(cli, "b", spool, "m3", 0), 1)
            self.assertEqual(cli.store, {})  # denied reads: nothing uploaded
            for kept in (p, s, m, cp):
                self.assertTrue(os.path.exists(kept))

    def test_identical_live_object_reused_without_put(self):
        with tempfile.TemporaryDirectory() as spool:
            p, s, m, cp = self._triple(spool)
            saved = {}
            for f in (p, s, m, cp):
                with open(f, "rb") as fh:
                    saved[f] = fh.read()
            cli = FakeS3()
            self.assertEqual(upl.run_once(cli, "b", spool, "m3", 0), 1)
            for gone in (p, s, m, cp):
                self.assertFalse(os.path.exists(gone))
            for f, blob in saved.items():  # crash between upload+delete
                with open(f, "wb") as fh:
                    fh.write(blob)
            puts = cli.puts
            self.assertEqual(upl.run_once(cli, "b", spool, "m3", 0), 1)
            self.assertEqual(cli.puts, puts)  # identical: reused, no PUT
            for gone in (p, s, m, cp):
                self.assertFalse(os.path.exists(gone))

    def test_same_stem_changed_triple_new_key_old_archive_kept(self):
        with tempfile.TemporaryDirectory() as spool:
            p, s, m, cp = self._triple(spool)
            md1 = upl.sha256_file(m)
            cli = FakeS3()
            self.assertEqual(upl.run_once(cli, "b", spool, "m3", 0), 1)
            key1 = upl.remote_key(p, "m3", md1)
            before = dict(cli.store)
            p2, s2, m2, cp2 = self._triple(spool, tweak=0xFF)  # same stem
            md2 = upl.sha256_file(m2)
            self.assertNotEqual(md1, md2)  # one frame byte changed
            self.assertEqual(upl.run_once(cli, "b", spool, "m3", 0), 1)
            key2 = upl.remote_key(p2, "m3", md2)
            self.assertNotEqual(key1, key2)
            self.assertTrue(key2.endswith(os.path.basename(p2)))
            for k in (key1, upl.sidecar_key(key1), upl.manifest_key(key1)):
                self.assertEqual(cli.store[("b", k)], before[("b", k)])
            for k in (key2, upl.sidecar_key(key2), upl.manifest_key(key2)):
                self.assertIn(("b", k), cli.store)
            for gone in (p2, s2, m2, cp2):
                self.assertFalse(os.path.exists(gone))


class EndpointGateTest(unittest.TestCase):
    def test_plaintext_and_hostless_endpoints_refused(self):
        old = os.environ.get("RAW_S3_ENDPOINT_URL")
        try:
            for bad in ("http://s3.example.com", "https://",
                        "s3.example.com", "", "http://127.0.0.1:9000"):
                os.environ["RAW_S3_ENDPOINT_URL"] = bad
                with self.assertRaises(SystemExit, msg=bad):
                    upl.make_client()
        finally:
            if old is None:
                os.environ.pop("RAW_S3_ENDPOINT_URL", None)
            else:
                os.environ["RAW_S3_ENDPOINT_URL"] = old


class ReplayGateTest(unittest.TestCase):
    def test_gate(self):
        old = os.environ.get("RAW_REPLAY_ALLOW")
        try:
            os.environ["RAW_REPLAY_ALLOW"] = "0"
            ok, _ = rec.replay_allowed("vcan0")
            self.assertFalse(ok)
            os.environ["RAW_REPLAY_ALLOW"] = "1"
            ok, _ = rec.replay_allowed("can0")
            self.assertFalse(ok)  # real bus TX forbidden
            ok, _ = rec.replay_allowed("vcan0")
            self.assertTrue(ok)
        finally:
            if old is None:
                os.environ.pop("RAW_REPLAY_ALLOW", None)
            else:
                os.environ["RAW_REPLAY_ALLOW"] = old


class BoundedMemoryTest(unittest.TestCase):
    def _frames(self, n, start=1):
        out = []
        for i in range(n):
            out.append({"seq": start + i,
                        "twall_ns": 1_700_000_000_000_000_000
                        + (n - 1 - i) * 1_000_000,
                        "tcan": 1700000000.0 + i * 0.001, "bus": "can0",
                        "id": 0x100 + (i % 3), "ext": bool(i % 2),
                        "rtr": False, "err": False, "fd": False,
                        "brs": False, "esi": False, "dlc": 8,
                        "data": bytes([(i + b) % 256 for b in range(8)])})
        return out

    def test_stream_torn_tail_only(self):
        with tempfile.TemporaryDirectory() as spool:
            cp = os.path.join(spool, "s.jsonl")
            good = [json.dumps(rec.encode_frame(
                f["seq"], f["twall_ns"], f["tcan"], f["bus"], f["id"],
                f["ext"], f["rtr"], f["err"], f["fd"], f["brs"], f["esi"],
                f["dlc"], f["data"]), separators=(",", ":")) for f in SYN[:3]]
            with open(cp, "wb") as fh:
                fh.write(("\n".join(good) + "\n{truncated").encode())
            stream = rec.IngressStream(cp)
            self.assertEqual(len(list(stream)), 3)
            self.assertTrue(stream.torn)
            with open(cp, "wb") as fh:
                fh.write(("\n".join(good) + "\n{bad}\n").encode())
            with self.assertRaises(rec.CorruptSegment):
                list(rec.IngressStream(cp))
            with open(cp, "w", encoding="utf-8") as fh:
                fh.write(good[0] + "\n{bad}\n" + good[1] + "\n")
            with self.assertRaises(rec.CorruptSegment):
                list(rec.IngressStream(cp))
            # Chunk-split multibyte UTF-8 decodes, it never fails a split.
            with open(cp, "w", encoding="utf-8") as fh:
                fh.write(good[0] + "\n" + good[1] + "\n")
            self.assertEqual(len(list(rec.IngressStream(cp))), 2)

    def test_stream_rejects_non_utf8(self):
        with tempfile.TemporaryDirectory() as spool:
            cp = os.path.join(spool, "s.jsonl")
            with open(cp, "wb") as fh:
                fh.write(b"\xff\xfe\n")
            with self.assertRaises(rec.CorruptSegment):
                list(rec.IngressStream(cp))

    def test_finalize_spills_and_verifies(self):
        try:
            import can.io.mf4  # noqa
        except ImportError:
            self.skipTest("python-can/asammdf not installed")
        with tempfile.TemporaryDirectory() as spool:
            rec.ensure_dirs(spool)
            dirs = rec.seg_dirs(spool)
            frames = self._frames(300)
            stem = "m3_20260921T000010Z_deadbeef_010"
            cp = os.path.join(dirs["closed"], stem + ".jsonl")
            _write_ingress(cp, frames)
            out = rec.finalize_segment(cp, dirs["sealed"], spool=spool,
                                       mem_frames=50)
            with open(out + ".manifest.json", encoding="utf-8") as fh:
                man = json.load(fh)
            self.assertEqual(man["frames"], 300)
            # Shuffled arrival, chronological manifest bounds.
            self.assertEqual(man["twall_ns_first"],
                             1_700_000_000_000_000_000)
            self.assertEqual(man["twall_ns_last"],
                             1_700_000_000_000_000_000 + 299 * 1_000_000)
            self.assertEqual((man["seq_first"], man["seq_last"]), (300, 1))
            # No temp runs leak after success.
            for n in os.listdir(dirs["sealed"]):
                self.assertFalse(n.endswith(".tmp") and ".run-" in n, n)
                p = os.path.join(dirs["sealed"], n)
                if os.path.isdir(p):
                    self.assertFalse(any(
                        x.startswith("run-") for x in os.listdir(p)), n)
            rec.verify_sealed_mf4(out, frames)  # list path parity

    def test_duplicate_frames_match_chronologically(self):
        try:
            import can.io.mf4  # noqa
        except ImportError:
            self.skipTest("python-can/asammdf not installed")
        with tempfile.TemporaryDirectory() as spool:
            rec.ensure_dirs(spool)
            dirs = rec.seg_dirs(spool)
            base = {"tcan": 1700000000.0, "bus": "can0", "id": 0x100,
                    "ext": False, "rtr": False, "err": False, "fd": False,
                    "brs": False, "esi": False, "dlc": 8,
                    "data": b"\x01" * 8}
            frames = [dict(base, seq=i + 1,
                           twall_ns=1_700_000_000_000_000_000 + i * 1_000)
                      for i in range(10)]
            stem = "m3_20260921T000011Z_deadbeef_011"
            cp = os.path.join(dirs["closed"], stem + ".jsonl")
            _write_ingress(cp, frames)
            out = rec.finalize_segment(cp, dirs["sealed"], spool=spool,
                                       mem_frames=3)
            seen = [f["seq"] for _m, f in rec.matched_mf4_frames(out, frames)]
            self.assertEqual(sorted(seen), [f["seq"] for f in frames])

    def test_tmp_budget_fails_closed(self):
        try:
            import can.io.mf4  # noqa
        except ImportError:
            self.skipTest("python-can/asammdf not installed")
        with tempfile.TemporaryDirectory() as spool:
            rec.ensure_dirs(spool)
            dirs = rec.seg_dirs(spool)
            frames = self._frames(100)
            stem = "m3_20260921T000012Z_deadbeef_012"
            cp = os.path.join(dirs["closed"], stem + ".jsonl")
            _write_ingress(cp, frames)
            with self.assertRaises(rec.NoSpace):
                rec.finalize_segment(cp, dirs["sealed"], spool=spool,
                                     mem_frames=5, tmp_max_bytes=10)
            self.assertTrue(os.path.exists(cp))  # ingress kept, retry later
            self.assertFalse(os.path.exists(
                os.path.join(dirs["sealed"], stem + ".mf4")))

    def test_many_runs_single_fd_and_small_budget(self):
        try:
            import can.io.mf4  # noqa
        except ImportError:
            self.skipTest("python-can/asammdf not installed")
        with tempfile.TemporaryDirectory() as spool:
            rec.ensure_dirs(spool)
            dirs = rec.seg_dirs(spool)
            frames = self._frames(2500)
            stem = "m3_20260921T000014Z_deadbeef_014"
            cp = os.path.join(dirs["closed"], stem + ".jsonl")
            _write_ingress(cp, frames)
            stats = {}
            out = rec.finalize_segment(cp, dirs["sealed"], spool=spool,
                                       mem_frames=50, stats=stats)
            self.assertTrue(os.path.exists(out))
            # One sqlite file, never a run-file list: FD count cannot grow.
            leftovers = [n for n in os.listdir(dirs["sealed"])
                         if n.endswith(".tmp")]
            self.assertEqual(leftovers, [])
            with open(out + ".manifest.json", encoding="utf-8") as fh:
                man = json.load(fh)
            self.assertEqual(man["frames"], 2500)
            self.assertLessEqual(stats["peak_resident_frames"], 50)
            self.assertEqual(stats["budget_frames"], 50)
            self.assertGreaterEqual(stats["peak_temp_bytes"], 0)
            # Chronological identity match still wins on the real path.
            seen = [f["seq"] for _m, f in
                    rec.matched_mf4_frames(out, frames)]
            self.assertEqual(sorted(seen), [f["seq"] for f in frames])

    def test_match_disk_budget_enforced(self):
        try:
            import can.io.mf4  # noqa
        except ImportError:
            self.skipTest("python-can/asammdf not installed")
        with tempfile.TemporaryDirectory() as spool:
            rec.ensure_dirs(spool)
            dirs = rec.seg_dirs(spool)
            frames = self._frames(200)
            stem = "m3_20260921T000015Z_deadbeef_015"
            cp = os.path.join(dirs["closed"], stem + ".jsonl")
            _write_ingress(cp, frames)
            store = rec.SortedFrameStore(dirs["sealed"], stem, 50, 2 << 30)
            try:
                for f in frames:
                    store.add(f)
                store.max_bytes = 10  # shrink only for the match stage
                with self.assertRaises(rec.NoSpace):
                    list(rec._matched_mf4_store(
                        os.path.join(dirs["sealed"], stem + ".missing.mf4"),
                        store))
            finally:
                store.destroy()

    def test_budgets_reject_nonpositive(self):
        for bad in (0, -1):
            with self.assertRaises(ValueError):
                rec.SortedFrameStore(tempfile.gettempdir(), "stem", bad,
                                     2 << 30)
        with tempfile.TemporaryDirectory() as spool:
            rec.ensure_dirs(spool)
            dirs = rec.seg_dirs(spool)
            stem = "m3_20260921T000016Z_deadbeef_016"
            cp = os.path.join(dirs["closed"], stem + ".jsonl")
            _write_ingress(cp, self._frames(2))
            with self.assertRaises(ValueError):
                rec.finalize_segment(cp, dirs["sealed"], spool=spool,
                                     mem_frames=0)
            with self.assertRaises(ValueError):
                rec.finalize_segment(cp, dirs["sealed"], spool=spool,
                                     tmp_max_bytes=0)

    def test_oversize_line_quarantines_and_whitespace_passes(self):
        with tempfile.TemporaryDirectory() as spool:
            cp = os.path.join(spool, "s.jsonl")
            with open(cp, "w", encoding="utf-8") as fh:
                fh.write(" " * (rec._LINE_MAX_BYTES + 8) + "\n")
                fh.write(json.dumps(rec.encode_frame(
                    1, 1_700_000_000_000_000_000, 1700000000.0, "can0",
                    0x100, False, False, False, False, False, False, 8,
                    b"\x01" * 8), separators=(",", ":")) + "\n")
            self.assertEqual(len(list(rec.IngressStream(cp))), 1)
            with open(cp, "w", encoding="utf-8") as fh:
                fh.write("x" * (rec._LINE_MAX_BYTES + 8) + "\n")
            with self.assertRaises(rec.CorruptSegment):
                list(rec.IngressStream(cp))
            # Damaged unterminated tail still quarantines, never salvages.
            with open(cp, "wb") as fh:
                fh.write(b"\xff\xfe")
            with self.assertRaises(rec.CorruptSegment):
                list(rec.IngressStream(cp))
            stream = rec.IngressStream(cp)
            try:
                list(stream)
            except rec.CorruptSegment:
                pass
            self.assertFalse(stream.torn)

    def test_interrupted_tmp_swept_and_restarted(self):
        try:
            import can.io.mf4  # noqa
        except ImportError:
            self.skipTest("python-can/asammdf not installed")
        with tempfile.TemporaryDirectory() as spool:
            rec.ensure_dirs(spool)
            dirs = rec.seg_dirs(spool)
            frames = self._frames(120)
            stem = "m3_20260921T000013Z_deadbeef_013"
            cp = os.path.join(dirs["closed"], stem + ".jsonl")
            _write_ingress(cp, frames)
            stale = os.path.join(dirs["sealed"], stem + ".xyz")
            os.makedirs(stale)
            with open(os.path.join(stale, "run-00000.tmp"), "w") as fh:
                fh.write("interrupted")
            out = rec.finalize_segment(cp, dirs["sealed"], spool=spool,
                                       mem_frames=10)
            self.assertTrue(os.path.exists(out))
            self.assertFalse(os.path.exists(stale))
            # Restart recovery sweeps sort dirs too, keeping the JSONL.
            stale2 = os.path.join(dirs["sealed"], stem + ".abc")
            os.makedirs(stale2)
            with open(os.path.join(stale2, "frames.sqlite3"), "w") as fh:
                fh.write("interrupted")
            _recovered, swept = rec.recover_spool(spool)
            self.assertGreaterEqual(swept, 1)
            self.assertFalse(os.path.exists(stale2))
            self.assertTrue(os.path.exists(cp))

    def test_gunzip_chunk_boundaries(self):
        import gzip
        import hashlib
        with tempfile.TemporaryDirectory() as spool:
            body = b'{"v":1}\n' * 500
            sc = os.path.join(spool, "s.gz")
            with open(sc, "wb") as raw:
                with gzip.GzipFile(filename="", mode="wb", fileobj=raw,
                                   mtime=0) as gz:
                    gz.write(body)
            want = hashlib.sha256(body).hexdigest()
            ok, _ = upl.gunzip_compare(sc, want, None, chunk_bytes=7)
            self.assertTrue(ok)
            ok, reason = upl.gunzip_compare(sc, "0" * 64, None,
                                            chunk_bytes=7)
            self.assertFalse(ok)
            self.assertIn("drifted", reason)
            # Truncated member fails closed, never a partial pass.
            with open(sc, "r+b") as fh:
                fh.truncate(os.path.getsize(sc) - 5)
            ok, reason = upl.gunzip_compare(sc, want, None, chunk_bytes=7)
            self.assertFalse(ok)
            self.assertIn("gunzip failed", reason)
            # Multi-member gzip: every member verified.
            mm = os.path.join(spool, "m.gz")
            with open(mm, "wb") as fh:
                fh.write(gzip.compress(b"AAA\n", mtime=0)
                         + gzip.compress(b"BBB\n", mtime=0))
            ok, _ = upl.gunzip_compare(
                mm, hashlib.sha256(b"AAA\nBBB\n").hexdigest(), None,
                chunk_bytes=3)
            self.assertTrue(ok)
            # Closed comparison is byte-exact across chunk splits.
            cp = os.path.join(spool, "c.jsonl")
            with open(cp, "wb") as fh:
                fh.write(body)
            sc2 = os.path.join(spool, "s2.gz")
            with open(cp, "rb") as src, open(sc2, "wb") as raw:
                with gzip.GzipFile(filename="", mode="wb", fileobj=raw,
                                   mtime=0) as gz:
                    gz.write(body)
            ok, _ = upl.gunzip_compare(sc2, want, cp, chunk_bytes=11)
            self.assertTrue(ok)
            with open(cp, "r+b") as fh:
                fh.seek(10)
                fh.write(b"\xff")
            ok, reason = upl.gunzip_compare(sc2, want, cp, chunk_bytes=11)
            self.assertFalse(ok)
            self.assertIn("drifted", reason)

    def test_preflight_drift_never_uploads_never_deletes(self):
        with tempfile.TemporaryDirectory() as spool:
            p, s, m, cp = UploadTest()._triple(spool)
            with open(s, "r+b") as fh:  # post-seal sidecar corruption
                fh.seek(20)
                fh.write(b"\xff")
            cli = FakeS3()
            self.assertEqual(upl.run_once(cli, "b", spool, "m3", 0), 1)
            self.assertEqual(cli.store, {})
            for kept_moved in (p, s, m, cp):
                self.assertFalse(os.path.exists(kept_moved))
                self.assertTrue(os.path.exists(
                    os.path.join(spool, "quarantine",
                                 os.path.basename(kept_moved))))


if __name__ == "__main__":
    sys.exit(0 if unittest.main(exit=False).result.wasSuccessful() else 1)

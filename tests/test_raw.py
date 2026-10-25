"""Synthetic regression tests for vehicle raw CAN pipeline (no hardware).

Covers: frame codec provenance, segment rotation/recovery, MF4 bus-logging
round-trip via MF4Reader (payload/FD/RTR/error, pre-rename verify gate,
crash-resume reuse, torn-tail salvage vs mid-corruption quarantine),
deterministic remote keys, triple-upload ack rule (all locals deleted only
on server-ChecksumSHA256 + HEAD size + Metadata-sha256 ack of all three
legs; all locals kept on any failure, manifest-pin drift fail-closed to
quarantine, partial sets invisible), replay safety gate.
"""
import json
import os
import sys
import tempfile
import unittest
from unittest import mock

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
    """Mimics botocore ClientError for a missing key."""
    def __init__(self, code="NoSuchKey"):
        super().__init__("missing key")
        self.response = {"Error": {"Code": code},
                         "ResponseMetadata": {"HTTPStatusCode": 404}}


class _HeadError(Exception):
    """Mimics a non-missing botocore failure (never means missing)."""
    def __init__(self, code, status):
        super().__init__("head failed: %s" % code)
        self.response = {"Error": {"Code": code},
                         "ResponseMetadata": {"HTTPStatusCode": status}}


class FakeS3:
    """HEAD + PUT fake with server ChecksumSHA256 semantics.

    store maps (Bucket, Key) -> (bytes, metadata, checksum_b64) where the
    checksum is the actual SHA256 of the stored bytes. put_object demands
    ChecksumSHA256 and rejects a corrupt upload body like a server would;
    head_object reports the checksum of the (possibly corrupted) bytes.
    """
    def __init__(self, corrupt=False, fail_put=0, unreadable=False,
                 missing_code="NoSuchKey", head_error=None,
                 no_checksum=False, wrong_checksum=False,
                 unsupported_checksum=False, same_size_corrupt=False,
                 put_no_echo=False):
        self.store = {}
        self.corrupt = corrupt
        self.fail_put = fail_put
        self.unreadable = unreadable
        self.missing_code = missing_code
        self.head_error = head_error
        self.no_checksum = no_checksum
        self.wrong_checksum = wrong_checksum
        self.unsupported_checksum = unsupported_checksum
        self.same_size_corrupt = same_size_corrupt
        self.put_no_echo = put_no_echo
        self.puts = 0
        self.gets = 0

    @staticmethod
    def _b64(data):
        import base64 as _b64
        import hashlib as _hl
        return _b64.b64encode(_hl.sha256(data).digest()).decode("ascii")

    def put_object(self, Bucket, Key, Body, ContentLength=None, Metadata=None,
                   ChecksumSHA256=None):
        if self.fail_put > 0:
            self.fail_put -= 1
            raise ConnectionError("boom")
        if self.unsupported_checksum:
            raise _HeadError("InvalidRequest", 400)
        blob = Body.read()
        actual = self._b64(blob)
        if ChecksumSHA256 != actual:
            # Server rejects an upload whose bytes fail checksum validation.
            raise IOError("checksum mismatch on PUT")
        self.puts += 1
        self.store[(Bucket, Key)] = (blob, dict(Metadata or {}), actual)
        if self.put_no_echo:
            return {}
        return {"ChecksumSHA256": actual}

    def head_object(self, Bucket, Key, ChecksumMode=None):
        if self.unreadable:
            raise ConnectionError("denied")
        if self.head_error is not None:
            raise self.head_error
        try:
            data, meta, csum = self.store[(Bucket, Key)]
        except KeyError:
            raise _Missing(self.missing_code)
        # MinIO-proven semantics: checksum returned only with ENABLED.
        enabled = (ChecksumMode == "ENABLED")
        if self.corrupt:
            data = b"tampered" + data  # wrong length: HEAD size mismatches
        elif self.same_size_corrupt:
            data = bytes(b ^ 0xFF for b in data)  # equal size, wrong bytes
        out = {"ContentLength": len(data), "Metadata": dict(meta)}
        if enabled:
            out["ChecksumSHA256"] = self._b64(data)
        if self.no_checksum:
            out.pop("ChecksumSHA256", None)
        elif self.wrong_checksum:
            out["ChecksumSHA256"] = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="
        return out

    def get_object(self, Bucket, Key):  # tripwire: readback must never happen
        self.gets += 1
        raise AssertionError("GET body readback is forbidden in raw upload")


class UploadTest(unittest.TestCase):
    def test_inventory_disabled_upload_and_replay_remain_verified(self):
        with tempfile.TemporaryDirectory() as spool:
            paths = self._triple(spool)
            cli = FakeS3()
            with mock.patch.object(upl, "_bucket_inventory") as inventory:
                upl.inventory_loop(cli, "b", 0, [])
                # Simulate a crash after remote ACK but before local GC.
                with mock.patch.object(upl.os, "unlink"):
                    self.assertEqual(upl.run_once(cli, "b", spool, "m3", 0), 1)
                self.assertEqual(cli.puts, 3)
                env = {"VEHICLE_ID": "m3", "RAW_BUCKET": "b",
                       "RAW_SPOOL_DIR": spool,
                       "RAW_S3_ENDPOINT_URL": "https://s3.example.test",
                       "RAW_S3_ACCESS_KEY_ID": "test",
                       "RAW_S3_SECRET_ACCESS_KEY": "test"}
                callbacks = []
                run_once = upl.run_once

                def sweep(*args):
                    result = run_once(*args)
                    callbacks[0]()
                    return result

                with mock.patch.dict(os.environ, env, clear=True), \
                        mock.patch.object(upl.threading, "Thread") as thread, \
                        mock.patch.object(upl.signal, "signal",
                                          side_effect=lambda _, cb: callbacks.append(cb)), \
                        mock.patch.object(upl, "make_client", return_value=cli) as make, \
                        mock.patch.object(upl, "wrap_client", side_effect=lambda c: c), \
                        mock.patch.object(upl, "run_once", side_effect=sweep):
                    self.assertEqual(upl.main([]), 0)
                make.assert_called_once_with()
                self.assertEqual(thread.call_count, 1)  # metrics only
                inventory.assert_not_called()
                self.assertEqual(cli.puts, 3)  # replay reuses verified HEADs
                self.assertEqual(cli.gets, 0)
                for path in paths:
                    self.assertFalse(os.path.exists(path))

    def test_inventory_positive_cadence_and_negative_startup(self):
        stop, sleeps = [], []

        def sleep(seconds):
            sleeps.append(seconds)
            if len(sleeps) == 4:
                stop.append(1)

        with mock.patch.object(upl, "refresh_inventory") as refresh, \
                mock.patch.object(upl.time, "sleep", side_effect=sleep):
            upl.inventory_loop(object(), "b", 1, stop)
        self.assertEqual(refresh.call_count, 2)
        self.assertEqual(sleeps, [0.5] * 4)
        with mock.patch.dict(os.environ, {"RAW_INVENTORY_INTERVAL_SEC": "-1"}), \
                mock.patch.object(upl.threading, "Thread") as thread, \
                mock.patch.object(upl, "make_client") as make, \
                mock.patch.object(upl.os, "makedirs") as mkdir:
            self.assertEqual(upl.main([]), 1)
        thread.assert_not_called()
        make.assert_not_called()
        mkdir.assert_not_called()

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
            cli = FakeS3(corrupt=True)  # post-PUT HEAD size mismatches
            n = upl.run_once(cli, "b", spool, "m3", 0)
            self.assertEqual(n, 1)
            for kept in (p, s, m, cp):  # unacked: everything stays
                self.assertTrue(os.path.exists(kept))

    def test_manifest_leg_failure_keeps_all(self):
        class ManifestDown(FakeS3):
            def put_object(self, Bucket, Key, Body, ContentLength, Metadata,
                           ChecksumSHA256=None):
                if Key.endswith(".manifest.json"):
                    raise ConnectionError("manifest down")
                return super().put_object(Bucket, Key, Body, ContentLength,
                                          Metadata,
                                          ChecksumSHA256=ChecksumSHA256)
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
                    blob = fh.read()
                # Same byte length, wrong digest: HEAD metadata conflicts.
                clashing = bytes(b ^ 0xFF for b in blob)
                cli.store[("b", leg_key)] = (
                    clashing, {"sha256": "0" * 64}, FakeS3._b64(clashing))
            before = dict(cli.store)
            self.assertEqual(upl.run_once(cli, "b", spool, "m3", 0), 1)
            self.assertEqual(cli.store, before)  # conflict: bytes untouched
            for kept in (p, s, m, cp):
                self.assertTrue(os.path.exists(kept))

    def test_missing_metadata_fails_closed(self):
        with tempfile.TemporaryDirectory() as spool:
            p, s, m, cp = self._triple(spool)
            cli = FakeS3()
            key = upl.remote_key(p, "m3", upl.sha256_file(m))
            for leg_path, leg_key in ((p, key),
                                      (s, upl.sidecar_key(key)),
                                      (m, upl.manifest_key(key))):
                with open(leg_path, "rb") as fh:
                    blob = fh.read()
                cli.store[("b", leg_key)] = (blob, {}, FakeS3._b64(blob))
            before = dict(cli.store)
            self.assertEqual(upl.run_once(cli, "b", spool, "m3", 0), 1)
            self.assertEqual(cli.store, before)  # no digest: never trusted
            for kept in (p, s, m, cp):
                self.assertTrue(os.path.exists(kept))

    def test_wrong_size_fails_closed(self):
        with tempfile.TemporaryDirectory() as spool:
            p, s, m, cp = self._triple(spool)
            cli = FakeS3()
            key = upl.remote_key(p, "m3", upl.sha256_file(m))
            with open(p, "rb") as fh:
                blob = fh.read() + b"\x00"
            cli.store[("b", key)] = (blob, {"sha256": upl.sha256_file(p)},
                                     FakeS3._b64(blob))
            self.assertEqual(upl.run_once(cli, "b", spool, "m3", 0), 1)
            for kept in (p, s, m, cp):
                self.assertTrue(os.path.exists(kept))

    def test_head_404_code_means_missing(self):
        with tempfile.TemporaryDirectory() as spool:
            p, s, m, cp = self._triple(spool)
            cli = FakeS3(missing_code="404")  # botocore HEAD 404: upload
            self.assertEqual(upl.run_once(cli, "b", spool, "m3", 0), 1)
            for gone in (p, s, m, cp):
                self.assertFalse(os.path.exists(gone))

    def test_head_nonmissing_error_never_deleted(self):
        for err in (_HeadError("Forbidden", 403),
                    _HeadError("NoSuchBucket", 404)):
            with tempfile.TemporaryDirectory() as spool:
                p, s, m, cp = self._triple(spool)
                cli = FakeS3(head_error=err)  # never means missing
                self.assertEqual(upl.run_once(cli, "b", spool, "m3", 0), 1)
                self.assertEqual(cli.store, {})
                for kept in (p, s, m, cp):
                    self.assertTrue(os.path.exists(kept))

    def test_failed_upload_logs_no_private_key_path_or_remote_error(self):
        import contextlib
        import io
        with tempfile.TemporaryDirectory() as spool:
            paths = self._triple(spool, name="private_vehicle_marker_segment")
            cli = FakeS3(head_error=_HeadError("private_remote_error_marker", 403))
            captured = io.StringIO()
            with contextlib.redirect_stderr(captured):
                upl.run_once(cli, "b", spool, "private_vehicle_marker", 0)
            for marker in (spool, "private_vehicle_marker", "private_remote_error_marker"):
                self.assertNotIn(marker, captured.getvalue())
            self.assertEqual(cli.store, {})
            self.assertTrue(all(os.path.exists(path) for path in paths))

    def test_unreadable_live_object_retried_never_deleted(self):
        with tempfile.TemporaryDirectory() as spool:
            p, s, m, cp = self._triple(spool)
            cli = FakeS3(unreadable=True)
            self.assertEqual(upl.run_once(cli, "b", spool, "m3", 0), 1)
            self.assertEqual(cli.store, {})  # denied HEAD: nothing uploaded
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

    def test_equal_size_corruption_fails_closed_never_deletes(self):
        # Same ContentLength and same metadata digest, but the stored bytes
        # differ: only the server checksum distinguishes them. Ack must
        # refuse and every local must stay.
        with tempfile.TemporaryDirectory() as spool:
            p, s, m, cp = self._triple(spool)
            cli = FakeS3(same_size_corrupt=True)
            n = upl.run_once(cli, "b", spool, "m3", 0)
            self.assertEqual(n, 1)
            for kept in (p, s, m, cp):
                self.assertTrue(os.path.exists(kept))
            self.assertEqual(cli.gets, 0)  # no GET body readback, ever

    def test_metadata_match_without_server_checksum_fails_closed(self):
        # A live object with matching size+metadata but no server checksum
        # (checksum-less backend) must NOT ack or delete locals.
        with tempfile.TemporaryDirectory() as spool:
            p, s, m, cp = self._triple(spool)
            key = upl.remote_key(p, "m3", upl.sha256_file(m))
            cli = FakeS3(no_checksum=True)
            for leg_path, leg_key in ((p, key),
                                      (s, upl.sidecar_key(key)),
                                      (m, upl.manifest_key(key))):
                with open(leg_path, "rb") as fh:
                    blob = fh.read()
                cli.store[("b", leg_key)] = (
                    blob, {"sha256": upl.sha256_file(leg_path)},
                    FakeS3._b64(blob))
            before = dict(cli.store)
            self.assertEqual(upl.run_once(cli, "b", spool, "m3", 0), 1)
            self.assertEqual(cli.store, before)
            for kept in (p, s, m, cp):
                self.assertTrue(os.path.exists(kept))

    def test_unsupported_checksum_fails_closed_keeps_locals(self):
        # Backend rejecting ChecksumSHA256 is an explicit failure: no trust,
        # no GET fallback, all locals retained on every leg.
        with tempfile.TemporaryDirectory() as spool:
            p, s, m, cp = self._triple(spool)
            cli = FakeS3(unsupported_checksum=True)
            self.assertEqual(upl.run_once(cli, "b", spool, "m3", 0), 1)
            self.assertEqual(cli.store, {})
            self.assertEqual(cli.gets, 0)
            for kept in (p, s, m, cp):
                self.assertTrue(os.path.exists(kept))

    def test_put_response_without_checksum_keeps_locals(self):
        # PUT accepted bytes but returned no checksum echo: unverified.
        with tempfile.TemporaryDirectory() as spool:
            p, s, m, cp = self._triple(spool)
            cli = FakeS3(put_no_echo=True)
            # _put_checked alone: missing echo raises through retries.
            digest = upl.sha256_file(p)
            with self.assertRaises(IOError):
                upl._put_checked(cli, "b", "k", p, os.path.getsize(p),
                                 digest)
            for kept in (p, s, m, cp):
                self.assertTrue(os.path.exists(kept))

    def test_replay_after_verified_ack_needs_no_put_no_readback(self):
        # Fresh complete triple acks; a crash-restored replay reuses all
        # three legs with no extra PUT and no GET body read.
        with tempfile.TemporaryDirectory() as spool:
            p, s, m, cp = self._triple(spool)
            saved = {}
            for f in (p, s, m):
                with open(f, "rb") as fh:
                    saved[f] = fh.read()
            cli = FakeS3()
            key = upl.remote_key(p, "m3", upl.sha256_file(m))
            for (k, path), digest in zip(
                    ((key, p), (upl.sidecar_key(key), s),
                     (upl.manifest_key(key), m)),
                    [upl.sha256_file(x) for x in (p, s, m)]):
                self.assertTrue(
                    upl.upload_verified(cli, "b", k, path, digest, 0))
            puts = cli.puts
            for (k, path), digest in zip(
                    ((key, p), (upl.sidecar_key(key), s),
                     (upl.manifest_key(key), m)),
                    [upl.sha256_file(x) for x in (p, s, m)]):
                self.assertTrue(
                    upl.upload_verified(cli, "b", k, path, digest, 0))
            self.assertEqual(cli.puts, puts)  # replay: reuse, no PUT
            self.assertEqual(cli.gets, 0)  # replay: no body readback

    def test_any_leg_unsupported_checksum_preserves_all_locals(self):
        # Manifest leg PUT rejected for checksum support: MF4/sidecar may
        # already be stored, but the triple is unacked -- all locals stay.
        class ManifestUnsupported(FakeS3):
            def put_object(self, Bucket, Key, Body, ContentLength,
                           Metadata, ChecksumSHA256=None):
                if Key.endswith(".manifest.json"):
                    raise _HeadError("InvalidRequest", 400)
                return super().put_object(
                    Bucket, Key, Body, ContentLength, Metadata,
                    ChecksumSHA256=ChecksumSHA256)
        with tempfile.TemporaryDirectory() as spool:
            p, s, m, cp = self._triple(spool)
            cli = ManifestUnsupported()
            self.assertEqual(upl.run_once(cli, "b", spool, "m3", 0), 1)
            for kept in (p, s, m, cp):
                self.assertTrue(os.path.exists(kept))
            self.assertEqual(cli.gets, 0)



class RawBindingTest(unittest.TestCase):
    def _triple(self, spool):
        import gzip
        import hashlib
        sealed = os.path.join(spool, "sealed")
        closed = os.path.join(spool, "closed")
        os.makedirs(sealed)
        os.makedirs(closed)
        stem = "m3_20260921T140000Z_binding"
        p = os.path.join(sealed, stem + ".mf4")
        s = os.path.join(sealed, stem + ".ingress.jsonl.gz")
        m = p + ".manifest.json"
        cp = os.path.join(closed, stem + ".jsonl")
        ingress = b'{"seq":1}\n'
        blobs = {p: b"sealed MF4 bytes", s: gzip.compress(ingress, mtime=0),
                 cp: ingress}
        blobs[m] = json.dumps({
            "mf4_sha256": hashlib.sha256(blobs[p]).hexdigest(),
            "ingress_sha256": hashlib.sha256(ingress).hexdigest(),
            "ingress_sidecar_sha256": hashlib.sha256(blobs[s]).hexdigest(),
        }, sort_keys=True).encode()
        for path, blob in blobs.items():
            with open(path, "wb") as fh:
                fh.write(blob)
        return (p, s, m, cp), blobs

    def test_preflight_returns_exact_pins_and_parses_manifest_once(self):
        from unittest.mock import patch
        with tempfile.TemporaryDirectory() as spool:
            paths, blobs = self._triple(spool)
            p, s, m, cp = paths
            with patch.object(upl, "_load_manifest", wraps=upl._load_manifest) as load:
                ok, reason, snap = upl.fail_closed_check(*paths)
                self.assertTrue(ok, reason)
                self.assertEqual(snap["manifest_bytes"], blobs[m])
                self.assertEqual(snap["manifest_digest"], upl.sha256_file(m))
                self.assertEqual(snap["mf4_digest"], upl.sha256_file(p))
                self.assertEqual(snap["sidecar_digest"], upl.sha256_file(s))
                self.assertEqual(snap["sizes"], tuple(len(blobs[x]) for x in (p, s, m)))
                ok, reason, checked = upl.fail_closed_check(*paths, snap=snap)
                self.assertTrue(ok, reason)
                self.assertIs(checked, snap)
                self.assertEqual(load.call_count, 1)

    def test_post_preflight_drift_never_acks_or_deletes_fresh_or_replay(self):
        from unittest.mock import patch
        for replay in (False, True):
            for timing in ("preflight", "last_ack"):
                for leg in range(4):
                    with self.subTest(replay=replay, timing=timing, leg=leg):
                        with tempfile.TemporaryDirectory() as spool:
                            paths, blobs = self._triple(spool)
                            p, s, m, cp = paths
                            cli = FakeS3()
                            key = upl.remote_key(p, "m3", upl.sha256_file(m))
                            keys = (key, upl.sidecar_key(key), upl.manifest_key(key))
                            if replay:
                                for k, path in zip(keys, paths[:3]):
                                    self.assertTrue(upl.upload_verified(
                                        cli, "b", k, path, upl.sha256_file(path), 0))
                            before = dict(cli.store)
                            puts = cli.puts
                            uploaded = upl._M["uploaded_files"]
                            failed = upl._M["failed"]
                            target = paths[leg]
                            # Whitespace-only manifest drift preserves every
                            # JSON pin while changing its content address.
                            changed = (blobs[target] + b" " if target == m else
                                       bytes([blobs[target][0] ^ 0xFF]) +
                                       blobs[target][1:])
                            real_check = upl.fail_closed_check
                            real_head = cli.head_object

                            def drift():
                                with open(target, "wb") as fh:
                                    fh.write(changed)

                            def check_then_drift(*args, **kwargs):
                                result = real_check(*args, **kwargs)
                                if result[0] and kwargs.get("snap") is None:
                                    drift()
                                return result

                            def head_then_drift(**kwargs):
                                result = real_head(**kwargs)
                                if kwargs["Key"] == keys[2]:
                                    drift()
                                return result

                            if timing == "preflight":
                                hook = patch.object(upl, "fail_closed_check",
                                                    side_effect=check_then_drift)
                            else:
                                hook = patch.object(cli, "head_object",
                                                    side_effect=head_then_drift)
                            with hook:
                                self.assertEqual(upl.run_once(cli, "b", spool, "m3", 0), 1)
                            self.assertEqual(upl._M["uploaded_files"], uploaded)
                            self.assertGreater(upl._M["failed"], failed)
                            self.assertEqual(cli.gets, 0)
                            for path in paths:
                                kept = path if os.path.exists(path) else os.path.join(
                                    spool, "quarantine", os.path.basename(path))
                                self.assertTrue(os.path.exists(kept))
                                with open(kept, "rb") as fh:
                                    self.assertEqual(fh.read(), changed if path == target
                                                     else blobs[path])
                            # Any remote legs must contain the original pinned
                            # bytes, including the exact manifest serialization.
                            for (_, k), (data, meta, checksum) in cli.store.items():
                                path = paths[keys.index(k)]
                                self.assertEqual(data, blobs[path])
                                self.assertEqual(checksum, FakeS3._b64(blobs[path]))
                            if replay:
                                self.assertEqual(cli.puts, puts)
                                self.assertEqual(cli.store, before)
                            elif timing == "last_ack":
                                self.assertEqual(cli.puts, 3)


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

    def test_stale_shm_tmp_swept(self):
        with tempfile.TemporaryDirectory() as spool:
            rec.ensure_dirs(spool)
            dirs = rec.seg_dirs(spool)
            stem = "m3_20260921T000018Z_deadbeef_018"
            stale = os.path.join(dirs["sealed"], stem + ".crash")
            os.makedirs(stale)
            with open(os.path.join(stale, "frames.sqlite3-shm"),
                      "w") as fh:
                fh.write("interrupted")
            self.assertEqual(rec._sweep_stale_tmp(dirs["sealed"], stem), 1)
            self.assertFalse(os.path.exists(stale))
            stale2 = os.path.join(dirs["sealed"], "other." + stem)
            os.makedirs(stale2)
            with open(os.path.join(stale2, "match.sqlite3-shm"),
                      "w") as fh:
                fh.write("interrupted")
            _recovered, swept = rec.recover_spool(spool)
            self.assertGreaterEqual(swept, 1)
            self.assertFalse(os.path.exists(stale2))

    def test_identical_timestamps_match_by_seq(self):
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
                    "data": b"\x02" * 8,
                    "twall_ns": 1_700_000_000_000_000_000}
            frames = [dict(base, seq=i + 1) for i in range(10)]
            stem = "m3_20260921T000019Z_deadbeef_019"
            cp = os.path.join(dirs["closed"], stem + ".jsonl")
            _write_ingress(cp, frames)
            out = rec.finalize_segment(cp, dirs["sealed"], spool=spool,
                                       mem_frames=3)
            with open(out + ".manifest.json", encoding="utf-8") as fh:
                man = json.load(fh)
            self.assertEqual(man["frames"], 10)
            self.assertEqual(man["twall_ns_first"], man["twall_ns_last"])
            seen = [f["seq"] for _m, f in rec.matched_mf4_frames(out, frames)]
            self.assertEqual(seen, [f["seq"] for f in frames])

    def test_temp_dir_enospc_fails_closed(self):
        import errno
        from unittest.mock import patch
        try:
            import can.io.mf4  # noqa
        except ImportError:
            self.skipTest("python-can/asammdf not installed")
        with tempfile.TemporaryDirectory() as spool:
            rec.ensure_dirs(spool)
            dirs = rec.seg_dirs(spool)
            stem = "m3_20260921T000020Z_deadbeef_020"
            cp = os.path.join(dirs["closed"], stem + ".jsonl")
            _write_ingress(cp, self._frames(10))
            with self.assertRaises(rec.NoSpace):
                with patch.object(rec.tempfile, "mkdtemp", side_effect=OSError(
                        errno.ENOSPC, "No space left on device")):
                    rec.finalize_segment(cp, dirs["sealed"], spool=spool,
                                         mem_frames=5)
            self.assertTrue(os.path.exists(cp))  # ingress kept, retry later
            sealed = os.listdir(dirs["sealed"])
            self.assertFalse(any(n.endswith(".mf4") for n in sealed), sealed)
            self.assertFalse(any(n.endswith(".manifest.json")
                                 for n in sealed), sealed)
            self.assertFalse(any(os.path.isdir(os.path.join(dirs["sealed"], n))
                                 for n in sealed), sealed)


class StorageBudgetTest(unittest.TestCase):
    def test_segment_admission_reserves_closed_ingress_finalization(self):
        with tempfile.TemporaryDirectory() as spool:
            rec.ensure_dirs(spool)
            writer = rec.SegWriter(spool, "v", 600, 64, 1, 1)
            segment = 64 + rec._LINE_MAX_BYTES
            old_admission = segment + 2 * 1024
            with mock.patch.dict(os.environ, {
                    "RAW_TMP_MAX_BYTES": "1024",
                    "RAW_SPOOL_MAX_BYTES": str(old_admission)}):
                with self.assertRaises(rec.NoSpace):
                    writer.write("frame")
            self.assertEqual(rec.spool_stats(spool), (0, 0))

    def test_empty_spool_makes_no_remote_requests(self):
        client = mock.Mock()
        with tempfile.TemporaryDirectory() as spool:
            self.assertEqual(upl.run_once(client, "bucket", spool, "v", 1), 0)
        self.assertEqual(client.mock_calls, [])
        self.assertEqual(upl._M["pending_files"], 0)
        self.assertEqual(upl._M["pending_bytes"], 0)

    def test_spool_counts_quarantine_and_refuses_new_segment(self):
        with tempfile.TemporaryDirectory() as spool:
            rec.ensure_dirs(spool)
            os.mkdir(os.path.join(spool, "quarantine"))
            path = os.path.join(spool, "quarantine", "retained")
            with open(path, "wb") as stream:
                stream.write(b"history")
            self.assertEqual(rec.spool_stats(spool), (1, 7))
            writer = rec.SegWriter(spool, "v", 600, 64, 1, 1)
            with mock.patch.dict(os.environ, {"RAW_SPOOL_MAX_BYTES": "7"}):
                with self.assertRaises(rec.NoSpace):
                    writer.write("new")
            with open(path, "rb") as stream:
                self.assertEqual(stream.read(), b"history")


if __name__ == "__main__":
    sys.exit(0 if unittest.main(exit=False).result.wasSuccessful() else 1)

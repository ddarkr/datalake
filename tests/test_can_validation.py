#!/usr/bin/env python3
"""Behavior tests for observational CAN validation (no hardware, no TX).

Covers: known/unknown/truncated/out-of-range frames, configured checksum
good/bad, expected-signal absence, async cross-signal tolerance, render
absence semantics, config rejection, and validator failure never blocking
Raw sealing. No source-text or shape-only tests.
"""

import hashlib
import importlib.util
import json
import os
import sys
import tempfile
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
SCRIPTS = os.path.join(os.path.dirname(HERE), "scripts")


def load(name):
    spec = importlib.util.spec_from_file_location(
        "cv_" + name, os.path.join(SCRIPTS, name + ".py"))
    mod = importlib.util.module_from_spec(spec)
    sys.modules["cv_" + name] = mod
    spec.loader.exec_module(mod)
    return mod


cv = load("can_validation")
rec = load("raw_recorder")

PRIMARY = """VERSION ""
NS_ :
BS_:
BU_: Vector__XXX
BO_ 256 Msg100: 8 Vector__XXX
 SG_ PackVoltage : 0|16@1+ (0.1,0) [200|450] "V" Vector__XXX
 SG_ PackCurrent : 16|16@1+ (1,0) [0|500] "A" Vector__XXX
 SG_ Cksum : 56|8@1+ (1,0) [0|255] "" Vector__XXX
BO_ 512 Msg200: 8 Vector__XXX
 SG_ PackPower : 0|16@1+ (1,0) [0|20000] "W" Vector__XXX
"""

SUPP = """VERSION ""
NS_ :
BS_:
BU_: Vector__XXX
BO_ 768 Msg300: 8 Vector__XXX
 SG_ Dummy : 0|8@1+ (1,0) [0|255] "" Vector__XXX
"""

CKSUM = {(0x100, False): {"algorithm": "sum8", "checksum_byte": 7,
                          "start": 0, "end": 7, "id_bytes": 0}}

POWER_REL = {"pack_power": {"result": "PackPower", "left": "PackVoltage",
                            "right": "PackCurrent", "op": "product",
                            "factor": 1.0, "abs_tolerance": 100.0,
                            "rel_tolerance": 0.02, "max_age_seconds": 1.0}}

T0 = 1_700_000_000_000_000_000


def frame(cid, data, i=0, err=False, rtr=False, dt_ms=0, ext=False):
    return {"seq": i + 1, "twall_ns": T0 + dt_ms * 1_000_000,
            "tcan": 1700000000.0 + dt_ms / 1000.0, "bus": "can0",
            "id": cid, "ext": ext, "rtr": rtr, "err": err,
            "fd": False, "brs": False, "esi": False,
            "dlc": len(data) if not rtr else 8, "data": bytes(data)}


def msg100(volts=400.0, amps=10.0, corrupt_ck=False):
    body = bytearray(8)
    raw_v = int(round(volts / 0.1))
    raw_i = int(round(amps))
    body[0:2] = (raw_v & 0xFFFF).to_bytes(2, "little")
    body[2:4] = (raw_i & 0xFFFF).to_bytes(2, "little")
    body[7] = sum(body[0:7]) & 0xFF
    if corrupt_ck:
        body[0] ^= 0xFF  # break payload, keep stale checksum
    return bytes(body)


def msg200(watts=4000.0):
    return int(round(watts)).to_bytes(2, "little") + b"\x00" * 6


def db_and_epoch(tmp):
    ddir = os.path.join(tmp, "data")
    os.makedirs(os.path.join(ddir, "dbc"))
    pp = os.path.join(ddir, "dbc/primary.dbc")
    sp = os.path.join(ddir, "dbc/supplemental.dbc")
    with open(pp, "w") as fh:
        fh.write(PRIMARY)
    with open(sp, "w") as fh:
        fh.write(SUPP)
    man = {"decode_epoch": "test1",
           "artifacts": [
               {"role": "primary",
                "sha256": hashlib.sha256(PRIMARY.encode()).hexdigest()},
               {"role": "supplemental",
                "sha256": hashlib.sha256(SUPP.encode()).hexdigest()}]}
    with open(os.path.join(ddir, "manifest.json"), "w") as fh:
        json.dump(man, fh)
    return cv.load_db(ddir)


def has_cantools():
    try:
        import cantools  # noqa
        return True
    except ImportError:
        return False


class ValidateTest(unittest.TestCase):
    def test_counts_known_unknown_truncated_range_err_remote(self):
        if not has_cantools():
            self.skipTest("cantools not installed")
        with tempfile.TemporaryDirectory() as tmp:
            db, epoch = db_and_epoch(tmp)
            self.assertEqual(epoch, "test1")
            frames = [
                frame(0x100, msg100(), i=0),                     # known good
                frame(0x999, b"\x01\x02\x03", i=1),              # unknown ID
                frame(0x100, b"\xA0\x0F", i=2),                  # truncated
                frame(0x100, msg100(volts=500.0), i=3),          # DBC range
                frame(0x100, b"\x00" * 8, i=4, err=True),        # link error
                frame(0x100, b"", i=5, rtr=True),                # remote
            ]
            res = cv.validate_window(frames, db, {}, {}, [], {})
            self.assertEqual(
                res["frames"],
                {"known": 3, "unknown": 1, "error": 1, "remote": 1})
            self.assertEqual(res["decode_errors"], 1)
            self.assertEqual(res["invalid_values"], 1)
            self.assertEqual(res["unknown_ids"], 1)
            self.assertAlmostEqual(res["coverage"], 1 / 2)
            self.assertIsNone(res["missing"])  # unconfigured: absent
            self.assertEqual(res["cross"], {})

    def test_checksum_good_bad_and_err_excluded(self):
        if not has_cantools():
            self.skipTest("cantools not installed")
        with tempfile.TemporaryDirectory() as tmp:
            db, _ = db_and_epoch(tmp)
            good = cv.validate_window([frame(0x100, msg100(), i=0)],
                                      db, CKSUM, {}, [], {})
            self.assertEqual(good["checksum_failures"], 0)
            bad = cv.validate_window(
                [frame(0x100, msg100(corrupt_ck=True), i=0)], db,
                CKSUM, {}, [], {})
            self.assertEqual(bad["checksum_failures"], 1)
            # Link errors never become app checksum verdicts.
            mix = cv.validate_window(
                [frame(0x100, msg100(corrupt_ck=True), i=0),
                 frame(0x100, b"\xff" * 8, i=1, err=True)], db,
                CKSUM, {}, [], {})
            self.assertEqual(mix["checksum_failures"], 1)
            self.assertEqual(mix["frames"]["error"], 1)

    def test_expected_signal_absence(self):
        if not has_cantools():
            self.skipTest("cantools not installed")
        with tempfile.TemporaryDirectory() as tmp:
            db, _ = db_and_epoch(tmp)
            only100 = cv.validate_window([frame(0x100, msg100(), i=0)],
                                         db, {}, {}, ["PackPower"], {})
            self.assertEqual(only100["missing"], 1)
            both = cv.validate_window(
                [frame(0x100, msg100(), i=0),
                 frame(0x200, msg200(), i=1, dt_ms=5)], db,
                {}, {}, ["PackPower"], {})
            self.assertEqual(both["missing"], 0)

    def test_cross_signal_pass_fail_stale(self):
        if not has_cantools():
            self.skipTest("cantools not installed")
        with tempfile.TemporaryDirectory() as tmp:
            db, _ = db_and_epoch(tmp)
            ok_frames = [frame(0x100, msg100(), i=0),
                         frame(0x200, msg200(), i=1, dt_ms=5)]
            ok = cv.validate_window(ok_frames, db, {}, {}, [], POWER_REL)
            self.assertEqual(ok["cross"], {})
            bad_frames = [frame(0x100, msg100(), i=0),
                          frame(0x200, msg200(9999.0), i=1, dt_ms=5)]
            bad = cv.validate_window(bad_frames, db, {}, {}, [], POWER_REL)
            self.assertEqual(bad["cross"], {"pack_power": 1})
            # Same inconsistent pair, but 5 s apart on an async bus: skipped.
            stale = [frame(0x100, msg100(), i=0),
                     frame(0x200, msg200(9999.0), i=1, dt_ms=5000)]
            skipped = cv.validate_window(stale, db, {}, {}, [], POWER_REL)
            self.assertEqual(skipped["cross"], {})

    def test_calibrated_range_supplements_dbc(self):
        if not has_cantools():
            self.skipTest("cantools not installed")
        with tempfile.TemporaryDirectory() as tmp:
            db, _ = db_and_epoch(tmp)
            msgs, sigs = db
            ranges = cv.parse_ranges({"PackVoltage": {"min": 300.0,
                                                      "max": 420.0}}, sigs)
            res = cv.validate_window([frame(0x100, msg100(volts=250.0), i=0)],
                                     db, {}, ranges, [], {})
            self.assertEqual(res["invalid_values"], 1)  # DBC-clean, cal-bad


EXT_PRIMARY = """VERSION ""
NS_ :
BS_:
BU_: Vector__XXX
BO_ 2147483904 MsgExt: 8 Vector__XXX
 SG_ ExtSig : 0|8@1+ (1,0) [0|10] "" Vector__XXX
"""

EXT_SUPP = """VERSION ""
NS_ :
BS_:
BU_: Vector__XXX
BO_ 256 MsgStd: 8 Vector__XXX
 SG_ StdSig : 0|8@1+ (1,0) [0|10] "" Vector__XXX
"""

OVER_PRIMARY = """VERSION ""
NS_ :
BS_:
BU_: Vector__XXX
BO_ 256 MsgP: 8 Vector__XXX
 SG_ Shared : 0|16@1+ (1,0) [0|10] "" Vector__XXX
"""

# Same wire identity as primary, conflicting schema: primary must win.
OVER_SUPP = """VERSION ""
NS_ :
BS_:
BU_: Vector__XXX
BO_ 256 MsgS: 8 Vector__XXX
 SG_ Shared : 0|16@1+ (0.1,0) [0|1000] "" Vector__XXX
"""

WIDE_PRIMARY = PRIMARY

WIDE_SUPP = """VERSION ""
NS_ :
BS_:
BU_: Vector__XXX
BO_ 512 MsgWide: 8 Vector__XXX
 SG_ SharedWide : 0|16@1+ (1,0) [0|100] "" Vector__XXX
BO_ 513 MsgNarrow: 8 Vector__XXX
 SG_ SharedWide : 0|16@1+ (1,0) [0|10] "" Vector__XXX
"""


def write_gen(tmp, primary, supp, epoch="test1"):
    ddir = os.path.join(tmp, "data")
    os.makedirs(os.path.join(ddir, "dbc"))
    with open(os.path.join(ddir, "dbc/primary.dbc"), "w") as fh:
        fh.write(primary)
    with open(os.path.join(ddir, "dbc/supplemental.dbc"), "w") as fh:
        fh.write(supp)
    with open(os.path.join(ddir, "manifest.json"), "w") as fh:
        json.dump({"decode_epoch": epoch,
                   "artifacts": [
                       {"role": "primary",
                        "sha256": hashlib.sha256(
                            primary.encode()).hexdigest()},
                       {"role": "supplemental",
                        "sha256": hashlib.sha256(
                            supp.encode()).hexdigest()}]}, fh)
    return ddir


class IdentityTest(unittest.TestCase):
    def test_std_and_extended_are_distinct_identities(self):
        if not has_cantools():
            self.skipTest("cantools not installed")
        keys = cv.parse_checksums(
            {"256": {"algorithm": "sum8", "checksum_byte": 0},
             "ext:256": {"algorithm": "sum8", "checksum_byte": 0}})
        self.assertIn((256, False), keys)
        self.assertIn((256, True), keys)
        # 29-bit wire IDs must say so; a bare >11-bit key is ambiguous.
        with self.assertRaises(cv.ConfigError):
            cv.parse_checksums({"0x1000": {"algorithm": "sum8",
                                           "checksum_byte": 0}})
        with tempfile.TemporaryDirectory() as tmp:
            db, _ = cv.load_db(write_gen(tmp, EXT_PRIMARY, EXT_SUPP))
            std = frame(0x100, b"\x05" + b"\x00" * 7, i=0, ext=False)
            ext = frame(0x100, b"\x05" + b"\x00" * 7, i=1, ext=True)
            res = cv.validate_window([std, ext], db, {}, {}, [], {})
            # Supplemental std 256 decodes StdSig; the std frame is known.
            # The ext frame matches the primary ext message, not the std one.
            self.assertEqual(res["frames"]["known"], 2)
            self.assertEqual(res["decode_errors"], 0)
            # Same numeric ID, wrong frame type: unknown, not misdecoded.
            res2 = cv.validate_window(
                [frame(0x100, b"\x05" + b"\x00" * 7, i=0, ext=True)],
                cv.load_db(write_gen(tmp + "_x", PRIMARY, SUPP))[0],
                {}, {}, [], {})
            self.assertEqual(res2["frames"]["unknown"], 1)
            self.assertEqual(res2["frames"]["known"], 0)

    def test_primary_wins_overlapping_wire_identity(self):
        if not has_cantools():
            self.skipTest("cantools not installed")
        with tempfile.TemporaryDirectory() as tmp:
            # Must not disable validation: supplemental overlap is ignored.
            db, epoch = cv.load_db(write_gen(tmp, OVER_PRIMARY, OVER_SUPP))
            self.assertEqual(epoch, "test1")
            msgs, sigs = db
            self.assertIn((256, False), msgs)
            self.assertEqual(len(msgs), 1)  # no merged duplicate
            # Raw 50: valid under the supplemental schema [0|1000]@0.1,
            # out of range under the primary schema [0|10]@1. Primary wins.
            raw50 = (50).to_bytes(2, "little") + b"\x00" * 6
            res = cv.validate_window([frame(0x100, raw50, i=0)],
                                     db, {}, {}, [], {})
            self.assertEqual(res["frames"]["known"], 1)
            self.assertEqual(res["invalid_values"], 1)

    def test_bounds_come_from_decoded_message_not_global_name(self):
        if not has_cantools():
            self.skipTest("cantools not installed")
        with tempfile.TemporaryDirectory() as tmp:
            db, _ = cv.load_db(write_gen(tmp, WIDE_PRIMARY, WIDE_SUPP))
            raw50 = (50).to_bytes(2, "little") + b"\x00" * 6
            # SharedWide=50 is inside MsgWide's own [0|100]: clean.
            res = cv.validate_window([frame(0x200, raw50, i=0)],
                                     db, {}, {}, [], {})
            self.assertEqual(res["invalid_values"], 0)
            # Same value on MsgNarrow [0|10]: one violation.
            res2 = cv.validate_window([frame(0x201, raw50, i=0)],
                                      db, {}, {}, [], {})
            self.assertEqual(res2["invalid_values"], 1)


class NanTest(unittest.TestCase):
    def test_nonfinite_values_count_invalid_never_samples(self):
        class FakeSignal:
            name = "S"
            minimum, maximum = 0.0, 10.0
        for bad in (float("nan"), float("inf"), float("-inf")):
            class FakeMsg:
                signals = [FakeSignal()]
                def decode(self, data, decode_choices=False, scaling=True):
                    return {"S": bad}
            rel = {"r": {"result": "S", "left": "S", "right": None,
                         "op": "equal", "factor": 1.0, "abs_tolerance": 0.0,
                         "rel_tolerance": 0.0, "max_age_seconds": 1.0}}
            fdb = ({(1, False): FakeMsg()}, {"S"})
            res = cv.validate_window([frame(1, b"\x00", i=0)], fdb,
                                     {}, {}, ["S"], rel)
            self.assertEqual(res["invalid_values"], 1, bad)
            self.assertEqual(res["missing"], 1)  # not a usable sample
            self.assertEqual(res["cross"], {})  # skipped, never failed
    def test_latest_sample_wins_within_float_tick(self):
        if not has_cantools():
            self.skipTest("cantools not installed")
        # Two PackPower samples closer than float64 timestamp resolution:
        # integer-ns ordering must pick the later one.
        d = next(d for d in range(1, 10000)
                 if float((T0 + d) / 1e9) == float(T0 / 1e9))
        with tempfile.TemporaryDirectory() as tmp:
            db, _ = db_and_epoch(tmp)
            v = dict(frame(0x100, msg100(), i=0), twall_ns=T0)
            p_ok = dict(frame(0x200, msg200(), i=1), twall_ns=T0)
            p_bad = dict(frame(0x200, msg200(9999.0), i=2), twall_ns=T0 + d)
            res = cv.validate_window([v, p_ok, p_bad], db,
                                     {}, {}, [], POWER_REL)
            self.assertEqual(res["cross"], {"pack_power": 1})


class EpochResetTest(unittest.TestCase):
    def _res(self, known):
        return {"frames": {"known": known, "unknown": 0, "error": 0,
                           "remote": 0},
                "unknown_ids": 0, "coverage": 1.0, "decode_errors": 0,
                "invalid_values": 0, "checksum_failures": 0, "missing": None,
                "cross": {}, "success_ts": 1700000000}

    def test_vehicle_epoch_change_resets_counters(self):
        state = {}
        cv.apply_success(state, "m3", "e1", self._res(2),
                         {"has_checksums": False, "has_expected": False,
                          "rules": []})
        cv.apply_success(state, "m3", "e2", self._res(5),
                         {"has_checksums": False, "has_expected": False,
                          "rules": []})
        self.assertEqual(state["c"]["known"], 5)  # not 7
        self.assertEqual(state["successes"], 1)
        out = cv.render(state)
        self.assertIn('decode_epoch="e2"', out)
        self.assertIn('classification="known"} 5', out)
        self.assertNotIn("e1", out)


class SymlinkTest(unittest.TestCase):
    def test_current_symlink_resolves(self):
        if not has_cantools():
            self.skipTest("cantools not installed")
        with tempfile.TemporaryDirectory() as tmp:
            gen = os.path.join(tmp, "generations", "e9")
            os.makedirs(os.path.join(gen, "dbc"))
            with open(os.path.join(gen, "dbc/primary.dbc"), "w") as fh:
                fh.write(PRIMARY)
            with open(os.path.join(gen, "dbc/supplemental.dbc"), "w") as fh:
                fh.write(SUPP)
            with open(os.path.join(gen, "manifest.json"), "w") as fh:
                json.dump({"decode_epoch": "e9",
                           "artifacts": [
                               {"role": "primary",
                                "sha256": hashlib.sha256(
                                    PRIMARY.encode()).hexdigest()},
                               {"role": "supplemental",
                                "sha256": hashlib.sha256(
                                    SUPP.encode()).hexdigest()}]}, fh)
            link = os.path.join(tmp, "current")
            os.symlink(gen, link)
            _db, epoch = cv.load_db(link)
            self.assertEqual(epoch, "e9")

class ConfigTest(unittest.TestCase):
    def test_rejects_bad_calibration(self):
        with self.assertRaises(cv.ConfigError):
            cv.parse_checksums({"256": {"algorithm": "crc16",
                                        "checksum_byte": 7}})
        with self.assertRaises(cv.ConfigError):
            cv.parse_checksums({"nope": {"algorithm": "sum8",
                                         "checksum_byte": 7}})
        with self.assertRaises(cv.ConfigError):
            cv.parse_ranges({"Nope": {"min": 0.0}}, {"PackVoltage"})
        with self.assertRaises(cv.ConfigError):
            cv.parse_relations({"r": {"result": "A", "left": "B",
                                      "op": "xor"}}, {"A", "B"})
        with self.assertRaises(cv.ConfigError):
            cv.parse_expected("PackVoltage")

    def test_drifted_dbc_is_unavailable_not_zero(self):
        if not has_cantools():
            self.skipTest("cantools not installed")
        with tempfile.TemporaryDirectory() as tmp:
            ddir = os.path.join(tmp, "data")
            os.makedirs(os.path.join(ddir, "dbc"))
            with open(os.path.join(ddir, "dbc/primary.dbc"), "w") as fh:
                fh.write(PRIMARY + "\n")
            with open(os.path.join(ddir, "dbc/supplemental.dbc"), "w") as fh:
                fh.write(SUPP)
            with open(os.path.join(ddir, "manifest.json"), "w") as fh:
                json.dump({"decode_epoch": "test1",
                           "artifacts": [{"role": "primary",
                                          "sha256": "0" * 64},
                                         {"role": "supplemental",
                                          "sha256": hashlib.sha256(
                                              SUPP.encode()).hexdigest()}]},
                          fh)
            with self.assertRaises(cv.Unavailable):
                cv.load_db(ddir)


class RenderTest(unittest.TestCase):
    def test_absence_semantics_and_bounded_labels(self):
        state = {}
        self.assertEqual(cv.render(state), "")
        cv.apply_failure(state, "m3")
        out = cv.render(state)
        self.assertIn('can_validation_configured{vehicle="m3",'
                      'decode_epoch="none"} 0', out)
        self.assertNotIn("frames_total", out)  # no healthy-looking zeros
        cv.apply_success(state, "m3", "e1",
                         {"frames": {"known": 2, "unknown": 1, "error": 0,
                                     "remote": 0},
                          "unknown_ids": 1, "coverage": 0.5,
                          "decode_errors": 0, "invalid_values": 0,
                          "checksum_failures": 0, "missing": None,
                          "cross": {}, "success_ts": 1700000000},
                         {"has_checksums": False, "has_expected": False,
                          "rules": []})
        out = cv.render(state)
        self.assertIn('can_validation_frames_total{vehicle="m3",'
                      'decode_epoch="e1",classification="known"} 2', out)
        self.assertIn("can_validation_known_id_coverage_ratio", out)
        self.assertNotIn("checksum_failures", out)  # unconfigured: absent
        self.assertNotIn("missing_expected", out)
        self.assertNotIn("cross_signal", out)
        self.assertNotIn("999", out)  # no per-frame IDs in labels


class SealSurvivesTest(unittest.TestCase):
    def test_validator_failure_cannot_block_sealing(self):
        try:
            import can.io.mf4  # noqa
        except ImportError:
            self.skipTest("python-can/asammdf not installed")
        old = os.environ.get("CAN_VALIDATION_DATA_DIR")
        try:
            with tempfile.TemporaryDirectory() as spool:
                rec.ensure_dirs(spool)
                dirs = rec.seg_dirs(spool)
                stem = "m3_20260921T000009Z_deadbeef_009"
                cp = os.path.join(dirs["closed"], stem + ".jsonl")
                with open(cp, "w", encoding="utf-8") as fh:
                    for i in range(5):
                        fh.write(json.dumps(rec.encode_frame(
                            i + 1, T0 + i * 1_000_000, 1700000000.0,
                            "can0", 0x100, False, False, False,
                            False, False, False, 8,
                            bytes([i] * 8)),
                            separators=(",", ":")) + "\n")
                # No manifest here: validation must fail, sealing must not.
                os.environ["CAN_VALIDATION_DATA_DIR"] = os.path.join(
                    spool, "no-such-data")
                out = rec.finalize_segment(cp, dirs["sealed"], spool=spool)
                self.assertTrue(out.endswith(".mf4"))
                self.assertTrue(os.path.exists(cp))  # ingress kept
                text = rec.metrics_text()
                self.assertIn("can_validation_configured", text)
                self.assertIn(" 0", text)
        finally:
            if old is None:
                os.environ.pop("CAN_VALIDATION_DATA_DIR", None)
            else:
                os.environ["CAN_VALIDATION_DATA_DIR"] = old


if __name__ == "__main__":
    sys.exit(0 if unittest.main(exit=False).result.wasSuccessful() else 1)

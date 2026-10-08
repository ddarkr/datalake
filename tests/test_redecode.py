"""Offline redecode regression: real MF4 bytes + simple DBC/VSS mapping.

Proves original-frame event_time (never collector now), math + named-enum
transforms through the real pinned upstream Mapper, duplicate-rerun
idempotence, and new-epoch coexistence. No CAN socket is ever opened.
"""

import gzip
import hashlib
import json
import os
import struct
import sys
import tempfile
import unittest
import urllib.request

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
from scripts.vehicle import vss_recorder as vr
from scripts.vehicle.raw import redecode as rd

# eclipse-kuksa/kuksa-can-provider tag 0.5.0 (same pin as redecode-deps).
KUKSA_COMMIT = "d03dd7db364dd1ce9f7d0c614d80ebf6642ad167"
KUKSA_BASE = ("https://raw.githubusercontent.com/eclipse-kuksa/"
              "kuksa-can-provider/" + KUKSA_COMMIT + "/dbcfeederlib/")
KUKSA_HASHES = {
    "__init__.py": "c1e13294c563beb313ed3a19a36e4839a0a4f98133ae4df5410c6c84be1786e6",
    "dbcparser.py": "2f478a0bd6032426ef589f4b479b589a72be7fb50b195da4facce042cc387774",
    "dbc2vssmapper.py": "9dfc995b906c58ccd4b3ff89ae6cab78b061ebb737bee5ab5c7b2a06e4ab84cb",
}

PRIMARY_DBC = """VERSION "redecode-test"

NS_ :
BS_:
BU_: Vector__XXX

BO_ 256 Drive: 8 Vector__XXX
 SG_ TSpeed : 0|16@1+ (0.1,0) [0|300] "km/h" Vector__XXX
 SG_ TSoc : 16|8@1+ (0.5,0) [0|100] "%" Vector__XXX
"""

SUPPL_DBC = """VERSION "redecode-test"

NS_ :
BS_:
BU_: Vector__XXX

BO_ 512 Charge: 8 Vector__XXX
 SG_ TCharging : 0|8@1+ (1,0) [0|1] "" Vector__XXX
 SG_ TPower : 8|16@1+ (0.01,0) [0|300] "kW" Vector__XXX

VAL_ 512 TCharging 0 "NotCharging" 1 "Charging" ;
"""

T0 = 1700000000.0  # fixed past stamp: history, never "now"
EPOCH1, EPOCH2 = "test-e1", "test-e2"


def mapping_tree():
    return {"Vehicle": {"type": "branch", "description": "t", "children": {
        "Speed": {"type": "sensor", "datatype": "float", "description": "s",
                  "unit": "km/h", "dbc2vss": {
                      "signal": "TSpeed", "interval_ms": 0,
                      "transform": {"math": "x + 0.5"}}},
        "Soc": {"type": "sensor", "datatype": "float", "description": "s",
                "unit": "percent", "dbc2vss": {
                    "signal": "TSoc", "interval_ms": 0}},
        "Charging": {"type": "sensor", "datatype": "boolean",
                     "description": "c", "dbc2vss": {
                         "signal": "TCharging", "interval_ms": 0,
                         "transform": {"mapping": [
                             {"from": "NotCharging", "to": False},
                             {"from": "Charging", "to": True}]}}},
        "ChargePower": {"type": "sensor", "datatype": "float",
                        "description": "p", "unit": "kW", "dbc2vss": {
                            "signal": "TPower", "interval_ms": 0}},
    }}}


def speed_data(phys, soc):
    return (struct.pack("<H", round(phys / 0.1))
            + bytes([round(soc / 0.5)]) + b"\x00" * 5)


def charge_data(charging, kw):
    return (bytes([charging]) + struct.pack("<H", round(kw / 0.01))
            + b"\x00" * 5)


def need_deps():
    try:
        import can  # noqa: F401
        import asammdf  # noqa: F401
        import cantools  # noqa: F401
        import py_expression_eval  # noqa: F401
    except ImportError as ex:
        raise unittest.SkipTest(f"pinned decode dep missing: {ex}")


def ensure_kuksa_src(workdir):
    """Pinned upstream source dir: deployed volume, installed package,
    else pinned download + SHA256 verify into the temp dir."""
    for cand in (os.environ.get("KUKSA_SRC_DIR", "").strip(),
                 "/opt/venv/kuksa_verified"):
        if cand and os.path.isfile(
                os.path.join(cand, "dbcfeederlib", "__init__.py")):
            return cand
    try:
        __import__("dbcfeederlib.dbc2vssmapper")
        return workdir
    except ImportError:
        pass
    dest = os.path.join(workdir, "kuksa_verified", "dbcfeederlib")
    os.makedirs(dest, exist_ok=True)
    for name, want in KUKSA_HASHES.items():
        with urllib.request.urlopen(KUKSA_BASE + name, timeout=120) as r:
            blob = r.read()
        got = hashlib.sha256(blob).hexdigest()
        if got.lower() != want.lower():
            raise AssertionError(f"pinned kuksa {name} hash drift: {got}")
        with open(os.path.join(dest, name), "wb") as f:
            f.write(blob)
    return os.path.join(workdir, "kuksa_verified")


def write_capture(path, records):
    closed = os.path.splitext(path)[0] + ".jsonl"
    with open(closed, "w", encoding="utf-8") as fh:
        for record in records:
            fh.write(json.dumps(record) + "\n")
    return rd.rr.finalize_segment(closed, os.path.dirname(path))


def sha_file(path):
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(8 << 20), b""):
            h.update(chunk)
    return h.hexdigest()




def write_generation(d, primary_text, suppl_text, mapping_obj):
    """Vehicle-setup-shaped generation dir: DBCs + mapping + manifest
    with truthful artifacts[].sha256 pins."""
    os.makedirs(os.path.join(d, "cur"), exist_ok=True)
    primary = os.path.join(d, "cur", "primary.dbc")
    suppl = os.path.join(d, "cur", "supplemental.dbc")
    mapping = os.path.join(d, "cur", "vss.json")
    with open(primary, "w", encoding="utf-8") as fh:
        fh.write(primary_text)
    with open(suppl, "w", encoding="utf-8") as fh:
        fh.write(suppl_text)
    with open(mapping, "w", encoding="utf-8") as fh:
        json.dump(mapping_obj, fh)
    manifest = os.path.join(d, "cur", "manifest.json")
    with open(manifest, "w", encoding="utf-8") as fh:
        json.dump({"vehicle_firmware": "t-fw", "decode_epoch": EPOCH1,
                   "vss_version": "t-vss", "dbc_primary_commit": "c1",
                   "dbc_supplemental_commit": "c2", "mapping_revision": "r1",
                   "artifacts": [
                       {"role": "primary", "sha256": sha_file(primary)},
                       {"role": "supplemental", "sha256": sha_file(suppl)},
                       {"role": "mapping", "sha256": sha_file(mapping)}]},
                  fh)
    return manifest, mapping, [primary, suppl]


def rows_by_path(conn):
    out = {}
    for path, ets, num, boolean, epoch in conn.execute(
            "SELECT path,event_time,value_num,value_bool,decode_epoch"
            " FROM outbox ORDER BY event_time,path"):
        out.setdefault(path, []).append((ets, num, boolean, epoch))
    return out


class RedecodeTest(unittest.TestCase):
    def setUp(self):
        need_deps()
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        d = self._tmp.name
        self.manifest, self.mapping, self.dbcs = write_generation(
            d, PRIMARY_DBC, SUPPL_DBC, mapping_tree())
        old = os.environ.get("KUKSA_SRC_DIR")
        os.environ["KUKSA_SRC_DIR"] = ensure_kuksa_src(d)
        self.addCleanup(lambda: os.environ.__setitem__(
            "KUKSA_SRC_DIR", old) if old is not None
            else os.environ.pop("KUKSA_SRC_DIR", None))
        self.meta = {"vehicle_firmware": "t-fw", "decode_epoch": EPOCH1,
                     "vss_version": "t-vss", "dbc_primary_commit": "c1",
                     "dbc_supplemental_commit": "c2", "mapping_revision": "r1",
                     "collector_version": "redecode-test"}
        _, self.units = vr.mapped_paths(self.mapping)
        self.conn = vr.open_outbox(os.path.join(d, "o.sqlite"))
        self.addCleanup(self.conn.close)
        self.twall = [int((T0 + off) * 1e9) for off in (0, 0.2, 0.4, 0.6)]
        payloads = [(0x100, speed_data(82.5, 40.0)),
                    (0x200, charge_data(1, 12.34)),
                    (0x100, speed_data(82.5, 40.0)),
                    (0x200, charge_data(0, 0.0))]
        records = [rd.rr.encode_frame(
            i + 1, ns, 0, "can0", can_id, False, False, False,
            False, False, False, 8, data)
            for i, ((can_id, data), ns) in enumerate(zip(payloads, self.twall))]
        self.mf4 = write_capture(os.path.join(d, "in.mf4"), records)
        _, self.frames = rd.verify_sealed_triple(self.mf4)

    def run_epoch(self, epoch):
        mapper = rd.load_mapper(self.mapping, self.dbcs)
        meta = dict(self.meta, decode_epoch=epoch)
        return rd.redecode_mf4(self.mf4, mapper, "smoke-veh", epoch,
                               meta, self.units, self.conn, {}, self.frames)

    def test_history_math_and_enum(self):
        n, skipped = self.run_epoch(EPOCH1)
        self.assertEqual(skipped, 0)
        self.assertEqual(n, 8)  # 4 frames x 2 signals, nothing dropped
        rows = rows_by_path(self.conn)
        self.assertEqual(sorted(rows), ["Vehicle.ChargePower",
                                        "Vehicle.Charging",
                                        "Vehicle.Soc", "Vehicle.Speed"])
        lo, hi = int(T0 * 1e9) - 10**6, int((T0 + 0.6) * 1e9) + 10**6
        for path, cells in rows.items():
            for ets, _num, _b, epoch in cells:
                # past frame time preserved, never collector now (~1.78e18)
                self.assertTrue(lo <= ets <= hi, (path, ets))
                self.assertEqual(epoch, EPOCH1)
        got_ns = sorted({ets for cells in rows.values()
                         for ets, _n, _b, _e in cells})
        self.assertEqual(got_ns, sorted(self.twall))  # exact capture ns
        speeds = rows["Vehicle.Speed"]
        self.assertEqual(len(speeds), 2)
        for ets, num, _b, _e in speeds:
            self.assertAlmostEqual(num, 83.0)  # 82.5 + math 0.5
        self.assertNotEqual(speeds[0][0], speeds[1][0])  # distinct samples
        charging = sorted(rows["Vehicle.Charging"])  # time order: True then False
        self.assertEqual([b for _e, _n, b, _ep in charging], [1, 0])
        socs = rows["Vehicle.Soc"]
        self.assertTrue(all(abs(num - 40.0) < 1e-9 for _e, num, _b, _ep in socs))
        powers = sorted(rows["Vehicle.ChargePower"])  # time order: 12.34 then 0.0
        self.assertAlmostEqual(powers[0][1], 12.34)
        self.assertAlmostEqual(powers[1][1], 0.0)

    def test_duplicate_rerun_is_idempotent(self):
        n1, _ = self.run_epoch(EPOCH1)
        self.assertEqual(n1, 8)
        n2, _ = self.run_epoch(EPOCH1)  # fresh mapper, same epoch
        self.assertEqual(n2, 0)
        count = self.conn.execute("SELECT COUNT(*) FROM outbox").fetchone()[0]
        self.assertEqual(count, 8)

    def test_new_epoch_coexists(self):
        self.assertEqual(self.run_epoch(EPOCH1)[0], 8)
        self.assertEqual(self.run_epoch(EPOCH2)[0], 8)
        count = self.conn.execute("SELECT COUNT(*) FROM outbox").fetchone()[0]
        self.assertEqual(count, 16)
        epochs = self.conn.execute(
            "SELECT DISTINCT decode_epoch FROM outbox ORDER BY 1").fetchall()
        self.assertEqual(epochs, [(EPOCH1,), (EPOCH2,)])
        dupes = self.conn.execute(
            "SELECT path,event_time,COUNT(*) FROM outbox"
            " GROUP BY path,event_time HAVING COUNT(*)!=2").fetchall()
        self.assertEqual(dupes, [])  # every sample kept under both epochs

    def test_sealed_triple_missing_manifest_fails(self):
        os.unlink(self.mf4 + ".manifest.json")
        with self.assertRaises(SystemExit):
            rd.verify_sealed_triple(self.mf4)

    def test_sealed_triple_missing_sidecar_fails(self):
        os.unlink(os.path.join(
            os.path.dirname(self.mf4),
            "in.ingress.jsonl.gz"))
        with self.assertRaises(SystemExit):
            rd.verify_sealed_triple(self.mf4)

    def test_sealed_triple_mf4_corruption_fails(self):
        with open(self.mf4, "r+b") as fh:
            fh.seek(-8, os.SEEK_END)
            fh.write(b"\xff" * 8)
        with self.assertRaises(SystemExit):
            rd.verify_sealed_triple(self.mf4)

    def test_sealed_triple_sidecar_corruption_fails(self):
        sidecar = os.path.join(os.path.dirname(self.mf4),
                               "in.ingress.jsonl.gz")
        with open(sidecar, "r+b") as fh:
            fh.seek(-4, os.SEEK_END)
            fh.write(b"\x00" * 4)
        with self.assertRaises(SystemExit):
            rd.verify_sealed_triple(self.mf4)

    def test_stale_dbc_bytes_fail_provenance(self):
        with open(self.dbcs[0], "a", encoding="utf-8") as fh:
            fh.write("\n")
        with self.assertRaises(SystemExit):
            rd.check_vehicle_artifacts(
                self.manifest, self.mapping, self.dbcs)

    def test_stale_mapping_bytes_fail_provenance(self):
        with open(self.mapping, "a", encoding="utf-8") as fh:
            fh.write(" ")
        with self.assertRaises(SystemExit):
            rd.check_vehicle_artifacts(
                self.manifest, self.mapping, self.dbcs)

    def test_bad_batch_limits_fail_closed(self):
        for bad in ("0", "-3", "abc", "4.5"):
            with self.assertRaises(SystemExit):
                vr._positive_int("REDECODE_BATCH_N", bad, 500)

    def test_s3_fetch_refuses_plaintext_endpoint_before_boto(self):
        old = {k: os.environ.get(k) for k in
               ("RAW_S3_ENDPOINT_URL", "RAW_S3_REGION")}
        try:
            for bad in ("http://s3.invalid", "https://", "",
                        "s3.invalid", "http://127.0.0.1:9000"):
                os.environ["RAW_S3_ENDPOINT_URL"] = bad
                with self.assertRaises(SystemExit, msg=bad):
                    rd.fetch_bytes("s3://b/k")
        finally:
            for k, v in old.items():
                if v is None:
                    os.environ.pop(k, None)
                else:
                    os.environ[k] = v

    def test_resolve_spool_verifies_and_returns_exact_times(self):
        import shutil
        spool = os.path.join(self._tmp.name, "spool", "sealed")
        os.makedirs(spool)
        for src in (self.mf4, self.mf4 + ".manifest.json",
                    os.path.join(os.path.dirname(self.mf4),
                                 "in.ingress.jsonl.gz")):
            shutil.copy(src, os.path.join(spool, os.path.basename(src)))
        old_spool, old_url = (os.environ.get("REDECODE_SPOOL_DIR"),
                              os.environ.get("REDECODE_INPUT_URL"))
        os.environ["REDECODE_SPOOL_DIR"] = os.path.dirname(spool)
        os.environ.pop("REDECODE_INPUT_URL", None)
        try:
            got = rd.resolve_inputs(os.path.join(self._tmp.name, "work"))
        finally:
            if old_spool is None:
                os.environ.pop("REDECODE_SPOOL_DIR", None)
            else:
                os.environ["REDECODE_SPOOL_DIR"] = old_spool
            if old_url is not None:
                os.environ["REDECODE_INPUT_URL"] = old_url
        self.assertEqual(len(got), 1)
        self.assertEqual([f["twall_ns"] for f in got[0][1]], self.twall)

    def test_float_equal_mixed_groups_preserve_capture_times(self):
        base = 1_789_983_110_123_456_780
        records = [
            rd.rr.encode_frame(1, base + 1, 0, "can0", 0x200, False,
                               False, True, False, False, False, 8, b"\0" * 8),
            rd.rr.encode_frame(2, base + 2, 0, "can0", 0x100, False,
                               False, False, False, False, False, 8,
                               speed_data(82.5, 40.0)),
            rd.rr.encode_frame(3, base + 3, 0, "can0", 0x300, False,
                               True, False, False, False, False, 8, b""),
            rd.rr.encode_frame(4, base + 4, 0, "can0", 0x200, False,
                               False, False, False, False, False, 8,
                               charge_data(1, 12.34)),
        ]
        path = write_capture(os.path.join(self._tmp.name, "mixed.mf4"), records)
        _, frames = rd.verify_sealed_triple(path)
        self.assertEqual(list(rd.iter_frames(path, frames)), [
            (0x100, speed_data(82.5, 40.0), base + 2),
            (0x200, charge_data(1, 12.34), base + 4),
        ])

    def test_applied_override_manifest_drives_effective_row_provenance(self):
        # The override artifact replaces the supplemental file: provenance
        # check hashes primary + override, and redecoded rows carry the
        # override version with a NULL supplemental commit.
        with open(self.manifest, encoding="utf-8") as fh:
            man = json.load(fh)
        over_sha = sha_file(self.dbcs[1])
        man["artifacts"].append({"role": "override", "sha256": over_sha})
        man["override"] = {"applied": True, "sha256": over_sha,
                           "version": "ov-7", "commit": None}
        man["dbc_supplemental_commit"] = None
        man["dbc_override_version"] = "ov-7"
        man["dbc_override_commit"] = None
        with open(self.manifest, "w", encoding="utf-8") as fh:
            json.dump(man, fh)
        rd.check_vehicle_artifacts(self.manifest, self.mapping, self.dbcs)
        meta = vr.load_manifest(self.manifest)
        mapper = rd.load_mapper(self.mapping, self.dbcs)
        n, skipped = rd.redecode_mf4(self.mf4, mapper, "smoke-veh", EPOCH1,
                                     meta, self.units, self.conn, {},
                                     self.frames)
        self.assertEqual(skipped, 0)
        self.assertEqual(n, 8)
        prov = self.conn.execute(
            "SELECT DISTINCT dbc_primary_commit, dbc_supplemental_commit,"
            " dbc_override_version, dbc_override_commit FROM outbox").fetchall()
        self.assertEqual(prov, [("c1", None, "ov-7", None)])



if __name__ == "__main__":
    unittest.main()

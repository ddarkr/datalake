"""Regression tests for vehicle_setup helpers (stdlib only).

Run: python3 tests/test_vss_setup.py  (do NOT run here; parent validates)
Covers, with real behavior (no source-text asserts):
- upstream VSS JSON "children" trees map to paths without a "children"
  segment; flat overlay-style trees keep working
- same-file and cross-file duplicate CAN IDs, plus cross-message
  signal-name reuse, are detected (fail closed)
- override is a complete supplemental replacement (no regex merge);
  empty/invalid override rejected
- reused decode_epoch with different inputs collides across full history
  (A->B->A fails); identical re-runs and new epochs pass
- failed publish leaves previous generation and current pointer untouched;
  successful publish swaps one symlink so the whole generation goes live
- invalid inputs (missing env, bad epoch name) fail before any I/O
"""

import importlib.util
import os
import sys
import tempfile
import unittest

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def load(name):
    spec = importlib.util.spec_from_file_location(
        name, os.path.join(REPO, "scripts", name + ".py"))
    mod = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod
    spec.loader.exec_module(mod)
    return mod


setup = load("vehicle_setup")

DBC_A = """VERSION ""

NS_ :
BS_ :
BU_ :

BO_ 100 MsgA: 8 Vector__XXX
 SG_ SigA : 0|8@1+ (1,0) [0|255] "" Vector__XXX
 SG_ Shared : 8|8@1+ (1,0) [0|255] "" Vector__XXX

BO_ 200 MsgB: 8 Vector__XXX
 SG_ SigB : 0|8@1+ (1,0) [0|255] "" Vector__XXX
"""

DBC_B = """VERSION ""

NS_ :
BS_ :
BU_ :

BO_ 300 MsgC: 8 Vector__XXX
 SG_ SigC : 0|8@1+ (1,0) [0|255] "" Vector__XXX
"""

# Upstream VSS JSON shape: branches nest under "children"
# (eclipse-kuksa/kuksa-can-provider mapping/vss_4.0/vss_dbc.json).
CHILDREN_TREE = {
    "Vehicle": {
        "children": {
            "Speed": {
                "datatype": "float",
                "type": "sensor",
                "unit": "km/h",
                "dbc2vss": {"signal": "SigA"},
            },
            "Body": {
                "type": "branch",
                "children": {
                    "Mirrors": {
                        "type": "branch",
                        "children": {
                            "DriverSide": {
                                "type": "branch",
                                "children": {
                                    "Pan": {
                                        "datatype": "int8",
                                        "type": "actuator",
                                        "unit": "percent",
                                        "dbc2vss": {
                                            "signal": "SigB",
                                            "interval_ms": 100,
                                        },
                                        "vss2dbc": {"signal": "SigB_wanted"},
                                    }
                                },
                            }
                        },
                    }
                },
            },
        }
    }
}

# Flat overlay-style tree (no "children" key) must keep working.
FLAT_TREE = {
    "Vehicle": {
        "Speed": {
            "datatype": "float",
            "type": "sensor",
            "dbc2vss": {"signal": "SigA"},
        }
    }
}


class ChildrenMappingTest(unittest.TestCase):
    def test_children_segments_never_in_paths(self):
        refs = setup.find_mappings(CHILDREN_TREE["Vehicle"], "Vehicle")
        got = {(p, s) for p, s, k in refs if k == "dbc2vss"}
        self.assertEqual(got, {
            ("Vehicle.Speed", "SigA"),
            ("Vehicle.Body.Mirrors.DriverSide.Pan", "SigB"),
        })
        for path, _, _ in refs:
            self.assertNotIn("children", path.split("."))

    def test_actuation_entry_reported_not_applied(self):
        refs = setup.find_mappings(CHILDREN_TREE["Vehicle"], "Vehicle")
        actuated = sorted({p for p, _, k in refs if k == "vss2dbc"})
        self.assertEqual(actuated, ["Vehicle.Body.Mirrors.DriverSide.Pan"])

    def test_flat_tree_still_works(self):
        refs = setup.find_mappings(FLAT_TREE["Vehicle"], "Vehicle")
        self.assertEqual(
            [(p, s) for p, s, k in refs if k == "dbc2vss"],
            [("Vehicle.Speed", "SigA")])

    def test_leaf_type_and_unit_visible_to_recorder(self):
        nodes = dict(setup.iter_nodes(CHILDREN_TREE["Vehicle"], "Vehicle"))
        leaf = nodes["Vehicle.Speed"]
        self.assertEqual(leaf["datatype"], "float")
        self.assertEqual(leaf["unit"], "km/h")

    def test_validate_mapping_roundtrip_ok(self):
        _, sigs_a = setup.parse_dbc(DBC_A)
        refs, actuated = setup.validate_mapping(
            CHILDREN_TREE["Vehicle"], sigs_a | {"SigB_wanted"})
        self.assertEqual(len(refs), 2)
        self.assertEqual(actuated, ["Vehicle.Body.Mirrors.DriverSide.Pan"])

    def test_validate_mapping_rejects_unknown_signal(self):
        _, sigs_a = setup.parse_dbc(DBC_A)
        tree = {"Speed": {"datatype": "float", "type": "sensor",
                          "dbc2vss": {"signal": "Nope"}}}
        with self.assertRaises(ValueError):
            setup.validate_mapping(tree, sigs_a)

    def test_validate_mapping_rejects_untyped_leaf(self):
        _, sigs_a = setup.parse_dbc(DBC_A)
        tree = {"Speed": {"dbc2vss": {"signal": "SigA"}}}  # no datatype/type
        with self.assertRaises(ValueError):
            setup.validate_mapping(tree, sigs_a)


class DbcConflictTest(unittest.TestCase):
    def test_same_file_duplicate_id_detected(self):
        dupe = DBC_A + ("\nBO_ 100 MsgA2: 8 Vector__XXX\n"
                        " SG_ SigA2 : 0|8@1+ (1,0) [0|255] \"\" Vector__XXX\n")
        issues = setup.dbc_issues(dupe, "primary")
        self.assertEqual(len(issues), 1)
        self.assertIn("100", issues[0])
    def test_cross_file_same_can_id_is_dupe(self):
        pa, _ = setup.parse_dbc(DBC_A)  # IDs 100, 200
        other = DBC_B.replace("BO_ 300 MsgC", "BO_ 200 MsgX")
        po, _ = setup.parse_dbc(other)  # ID 200 collides with primary
        self.assertIn(200, setup.cross_file_dupes(pa, po))
        clean, _ = setup.parse_dbc(
            DBC_B.replace("BO_ 300 MsgC", "BO_ 400 MsgD"))
        self.assertEqual(setup.cross_file_dupes(pa, clean), [])
    def test_signal_reused_across_ids_is_conflict(self):
        moved = DBC_B.replace("SG_ SigC ", "SG_ SigA ")
        conflicts = setup.signal_conflicts(
            [("primary", DBC_A), ("supplemental", moved)])
        self.assertIn("SigA", conflicts)

    def test_same_signal_same_id_is_not_conflict(self):
        # Same signal name under the SAME can id (e.g. present in both files
        # only if the id also collides) is not itself a signal conflict;
        # the id collision is reported separately by cross_file_dupes.
        conflicts = setup.signal_conflicts([("primary", DBC_A)])
        self.assertEqual(conflicts, {})


class OverridePolicyTest(unittest.TestCase):
    OVERRIDE = """VERSION ""

NS_ :
BS_ :
BU_ :

BO_ 300 MsgCfixed: 8 Vector__XXX
 SG_ SigCfixed : 0|8@1+ (1,0) [0|255] "" Vector__XXX
"""

    def test_override_replaces_supplemental_verbatim(self):
        text, info = setup.resolve_supplemental(DBC_B, self.OVERRIDE.encode(), "")
        self.assertTrue(info["applied"])
        self.assertEqual(text, self.OVERRIDE)
        _, signals = setup.parse_dbc(text)
        self.assertIn("SigCfixed", signals)
        self.assertNotIn("SigC", signals)

    def test_applied_override_records_version_without_commit(self):
        _text, info = setup.resolve_supplemental(
            DBC_B, self.OVERRIDE.encode(), "", "ov-7")
        self.assertTrue(info["applied"])
        self.assertEqual(info["version"], "ov-7")
        self.assertIsNone(info["commit"])  # version never invents a commit

    def test_no_override_uses_supplemental_verbatim(self):
        text, info = setup.resolve_supplemental(DBC_B, None, "")
        self.assertFalse(info["applied"])
        self.assertEqual(text, DBC_B)

    def test_empty_override_rejected(self):
        with self.assertRaises(ValueError):
            setup.resolve_supplemental(DBC_B, b'VERSION ""\n\nNS_ :\n', "")


class EpochAndPublishTest(unittest.TestCase):
    def ident(self, **kw):
        base = {"decode_epoch": "7", "vehicle_firmware": "fw1",
                "vss_version": "4.0", "primary_sha256": "a",
                "supplemental_sha256": "b", "mapping_sha256": "c",
                "override_applied": False, "override_sha256": "",
                "override_version": ""}
        base.update(kw)
        return base

    def gen(self, d, epoch, ident):
        path = os.path.join(d, "generations", epoch)
        os.makedirs(os.path.join(path, "dbc"))
        with open(os.path.join(path, "manifest.json"), "w") as f:
            import json as _j
            _j.dump({"decode_epoch": epoch, "inputs": ident}, f)

    def test_reused_epoch_collides_across_history(self):
        # A -> B -> A with different content must fail: history scan sees
        # the stale generation even though it is not the latest.
        with tempfile.TemporaryDirectory() as d:
            self.gen(d, "A", self.ident(decode_epoch="A"))
            self.gen(d, "B", self.ident(decode_epoch="B"))
            existing = setup.list_generation_manifests(d)
            with self.assertRaises(setup.EpochCollision):
                setup.check_epoch_history(
                    existing, self.ident(decode_epoch="A",
                                        vehicle_firmware="fw2"))

    def test_identical_rerun_passes(self):
        with tempfile.TemporaryDirectory() as d:
            self.gen(d, "A", self.ident(decode_epoch="A"))
            existing = setup.list_generation_manifests(d)
            setup.check_epoch_history(
                existing, self.ident(decode_epoch="A"))  # must not raise

    def test_new_epoch_passes(self):
        with tempfile.TemporaryDirectory() as d:
            self.gen(d, "A", self.ident(decode_epoch="A"))
            existing = setup.list_generation_manifests(d)
            setup.check_epoch_history(
                existing, self.ident(decode_epoch="B"))

    def test_first_boot_passes(self):
        setup.check_epoch_history([], self.ident())

    def test_inputs_validated_before_any_output(self):
        with self.assertRaises(setup.ConfigError):
            setup.load_config({})  # nothing downloaded, nothing written

    def test_bad_epoch_name_rejected_before_io(self):
        with self.assertRaises(setup.ConfigError):
            setup.load_config({"DATA_DIR": "/tmp/x",
                               "DBC_PRIMARY_URL": "u", "DBC_PRIMARY_SHA256": "s",
                               "DBC_PRIMARY_COMMIT": "1" * 40,
                               "DBC_SUPPLEMENTAL_URL": "u",
                               "DBC_SUPPLEMENTAL_SHA256": "s",
                               "DBC_SUPPLEMENTAL_COMMIT": "2" * 40,
                               "VSS_MAPPING_URL": "u", "VSS_MAPPING_SHA256": "s",
                               "VEHICLE_FIRMWARE": "f", "DECODE_EPOCH": "..",
                               "VSS_VERSION": "v"})

    def test_source_commits_and_overlay_version_are_required(self):
        cfg = {"DBC_PRIMARY_URL": "u", "DBC_PRIMARY_SHA256": "1" * 64,
               "DBC_PRIMARY_COMMIT": "1" * 40,
               "DBC_SUPPLEMENTAL_URL": "u", "DBC_SUPPLEMENTAL_SHA256": "2" * 64,
               "DBC_SUPPLEMENTAL_COMMIT": "2" * 40,
               "VSS_MAPPING_URL": "u", "VSS_MAPPING_SHA256": "3" * 64,
               "VEHICLE_FIRMWARE": "f", "DECODE_EPOCH": "epoch", "VSS_VERSION": "6.0"}
        for key in ("DBC_PRIMARY_COMMIT", "DBC_SUPPLEMENTAL_COMMIT"):
            with self.subTest(key=key), self.assertRaises(setup.ConfigError):
                setup.load_config(dict(cfg, **{key: ""}))
        with self.assertRaises(setup.ConfigError):
            setup.load_config(dict(cfg, DBC_PRIMARY_COMMIT="main"))
        with self.assertRaises(setup.ConfigError):
            setup.load_config(dict(cfg, DBC_OVERRIDE_URL="u", DBC_OVERRIDE_SHA256="4" * 64))

    def test_failed_publish_leaves_current_and_history_untouched(self):
        with tempfile.TemporaryDirectory() as d:
            setup.publish_generation(d, "A", {
                "manifest.json": b'{"decode_epoch": "A"}',
                "dbc/primary.dbc": b"old",
            })
            before = setup.current_target(d)
            with self.assertRaises(TypeError):
                setup.publish_generation(d, "B", {
                    "manifest.json": b'{"decode_epoch": "B"}',
                    "dbc/primary.dbc": "not-bytes",  # fails during staging
                })
            # single-pointer swap: current still aims at A, B never appeared.
            self.assertEqual(setup.current_target(d), before)
            self.assertFalse(
                os.path.exists(os.path.join(d, "generations", "B")))
            with open(os.path.join(
                    d, "generations", "A", "dbc", "primary.dbc"), "rb") as f:
                self.assertEqual(f.read(), b"old")

    def test_successful_publish_swaps_single_pointer(self):
        with tempfile.TemporaryDirectory() as d:
            setup.publish_generation(d, "A", {
                "manifest.json": b'{"decode_epoch": "A"}',
                "dbc/primary.dbc": b"a",
                "dbc/supplemental.dbc": b"s",
                "mapping/vss_dbc.json": b"{}",
            })
            self.assertEqual(setup.current_target(d),
                             os.path.join("generations", "A"))
            setup.publish_generation(d, "B", {
                "manifest.json": b'{"decode_epoch": "B"}',
                "dbc/primary.dbc": b"b",
                "dbc/supplemental.dbc": b"s",
                "mapping/vss_dbc.json": b"{}",
            })
            self.assertEqual(setup.current_target(d),
                             os.path.join("generations", "B"))
            # both generations persist for redecode/audit
            for epoch, want in (("A", b"a"), ("B", b"b")):
                with open(os.path.join(
                        d, "generations", epoch, "dbc", "primary.dbc"),
                        "rb") as f:
                    self.assertEqual(f.read(), want)

    def test_offline_generation_never_switches_live_configuration(self):
        with tempfile.TemporaryDirectory() as d:
            setup.publish_generation(d, "B", {
                "manifest.json": b'{"decode_epoch": "B"}',
                "dbc/primary.dbc": b"offline-first",
            }, activate=False)
            self.assertIsNone(setup.current_target(d))
            setup.publish_generation(d, "A", {
                "manifest.json": b'{"decode_epoch": "A"}',
                "dbc/primary.dbc": b"live",
            })
            setup.publish_generation(d, "C", {
                "manifest.json": b'{"decode_epoch": "C"}',
                "dbc/primary.dbc": b"offline-next",
            }, activate=False)
            with open(os.path.join(d, "current", "dbc", "primary.dbc"), "rb") as f:
                self.assertEqual(f.read(), b"live")
            with open(os.path.join(d, "generations", "C", "dbc", "primary.dbc"), "rb") as f:
                self.assertEqual(f.read(), b"offline-next")

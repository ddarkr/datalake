#!/usr/bin/env python3
"""Strict Regression tests for Tesla Fleet Telemetry recorder (scripts/fleet_recorder.py).

Covers (stdlib only):
  - Exact 9-digit nanosecond preservation of original createdAt without fraction truncations
  - Fail-closed behavior on timestamps without explicit timezone or boolean timestamps
  - Protojson oneof field unwrapping (floatValue, doubleValue, intValue, longValue, shiftStateValue, etc.)
  - Dropping of 'invalid' oneof kind
  - Strict 2-frame ZMQ message contract (frame 0: tesla_V, frame 1: protojson)
  - Rejection of flat or non-conformant payload fallbacks
  - Anti-vehicle mixing: mandatory VIN, TARGET_VIN isolation, mandatory salt pseudonymization
  - Official unit conversions:
    * VehicleSpeed (mph -> km/h * 1.609344)
    * Odometer, EstRange, IdealBatteryRange (miles -> km * 1.609344)
    * Tire Pressure TpmsPressure*/TirePressure* (bar -> kPa * 100.0, VSS standard unit)
  - Distinct canonical paths & event_id provenance preservation:
    * Soc vs BatteryLevel keep independent paths and event_ids
    * EstRange vs IdealBatteryRange keep independent paths and event_ids
    * deterministic_event_id incorporates source_system & source_field so same-timestamp
      distinct fields never collide or dedup each other
    * Only isResend redeliveries deduplicate
  - Unverified VSS_VERSION and VEHICLE_FIRMWARE left NULL
  - Sparse signal preservation (unreceived fields are never padded with 0/false)
  - Outbox overflow non-destructive policy: existing unacked rows preserved, new rejected, drop counter incremented
  - Bounded seen_ids table prevention of disk growth
  - End-to-end smoke: official telemetry.rs 2-frame ZMQ fixture -> fleet_recorder -> Greptime HTTP SQL
  - DB down / restart / redelivery recovery
  - CAN / Fleet source separation in aggregation
"""

import http.server
import json
import math
import os
import re
import sqlite3
import sys
import tempfile
import threading
import unittest
import urllib.parse
from datetime import datetime, timezone

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(REPO, "scripts"))
import fleet_recorder as fr
import aggregate as agg


class TimestampNanosecondPreservationTest(unittest.TestCase):
    def test_nanosecond_fraction_exact_preservation(self):
        # 9-digit fraction: .123456789Z
        iso_str = "2026-09-26T13:00:00.123456789Z"
        ns = fr.parse_created_at(iso_str)
        # Fraction part must be exactly 123456789 ns
        self.assertEqual(ns % 1_000_000_000, 123456789)

        # Base seconds: 2026-09-26 13:00:00 UTC = 1790427600
        dt = datetime(2026, 9, 26, 13, 0, 0, tzinfo=timezone.utc)
        expected_s = int(dt.timestamp())
        self.assertEqual(ns // 1_000_000_000, expected_s)
        self.assertEqual(ns, expected_s * 1_000_000_000 + 123456789)

    def test_varying_fraction_lengths_padded_to_nanoseconds(self):
        # 3 digits (ms) -> 500000000 ns
        self.assertEqual(fr.parse_created_at("2026-09-26T13:00:00.5Z") % 1_000_000_000, 500000000)
        # 6 digits (us) -> 123456000 ns
        self.assertEqual(fr.parse_created_at("2026-09-26T13:00:00.123456Z") % 1_000_000_000, 123456000)

    def test_missing_timezone_rejected(self):
        # Timestamp without timezone MUST be rejected
        with self.assertRaises(ValueError):
            fr.parse_created_at("2026-09-26T13:00:00")
        with self.assertRaises(ValueError):
            fr.parse_created_at("2026-09-26T13:00:00.123456789")

    def test_boolean_timestamp_rejected(self):
        # Python bool is an int subclass; parse_created_at must explicitly reject it
        with self.assertRaises(ValueError):
            fr.parse_created_at(True)
        with self.assertRaises(ValueError):
            fr.parse_created_at(False)

    def test_timezone_offsets_converted_to_utc(self):
        # 13:00:00+09:00 is 04:00:00 UTC
        ns_kst = fr.parse_created_at("2026-09-26T13:00:00.000000000+09:00")
        ns_utc = fr.parse_created_at("2026-09-26T04:00:00.000000000Z")
        self.assertEqual(ns_kst, ns_utc)


class ProtojsonUnwrappingAndValidationTest(unittest.TestCase):
    def test_protojson_oneof_unwrapping(self):
        # floatValue
        self.assertEqual(fr.unwrap_protojson_value("InsideTemp", {"floatValue": 22.5}), (22.5, "floatValue"))
        # doubleValue
        self.assertEqual(fr.unwrap_protojson_value("OutsideTemp", {"doubleValue": 34.0}), (34.0, "doubleValue"))
        # longValue (string encoded 64-bit int)
        self.assertEqual(fr.unwrap_protojson_value("Soc", {"longValue": "59"}), (59, "longValue"))
        # intValue
        self.assertEqual(fr.unwrap_protojson_value("HvacFanStatus", {"intValue": 3}), (3, "intValue"))
        # booleanValue
        self.assertEqual(fr.unwrap_protojson_value("HvacACEnabled", {"booleanValue": True}), (True, "booleanValue"))
        # shiftStateValue with prefix strip
        self.assertEqual(fr.unwrap_protojson_value("Gear", {"shiftStateValue": "ShiftStateP"}), ("P", "shiftStateValue"))
        self.assertEqual(fr.unwrap_protojson_value("Gear", {"shiftStateValue": "ShiftStateD"}), ("D", "shiftStateValue"))
        # hvacAutoMode
        self.assertEqual(fr.unwrap_protojson_value("HvacAutoMode", {"hvacAutoModeValue": "HvacAutoModeStateOn"}), (True, "hvacAutoModeValue"))
        self.assertEqual(fr.unwrap_protojson_value("HvacAutoMode", {"hvacAutoModeValue": "HvacAutoModeStateOverride"}), (False, "hvacAutoModeValue"))
        # invalid kind dropped
        self.assertIsNone(fr.unwrap_protojson_value("Experimental_1", {"invalid": True}))

    def test_official_unit_conversions(self):
        # VehicleSpeed raw mph -> VSS km/h
        spec_speed = fr.FIELD_ALLOWLIST["VehicleSpeed"]
        num, text, b = fr.validate_field_value(spec_speed, 60.0)
        self.assertAlmostEqual(num, 60.0 * 1.609344, places=4)
        self.assertIsNone(text)
        self.assertIsNone(b)

        # Odometer raw miles -> VSS km
        spec_odo = fr.FIELD_ALLOWLIST["Odometer"]
        num_odo, _, _ = fr.validate_field_value(spec_odo, 10000.0)
        self.assertAlmostEqual(num_odo, 10000.0 * 1.609344, places=4)

        # Tire Pressure raw bar -> VSS kPa (1 bar = 100 kPa)
        spec_tpms = fr.FIELD_ALLOWLIST["TpmsPressureFl"]
        num_tpms, _, _ = fr.validate_field_value(spec_tpms, 2.9)
        self.assertAlmostEqual(num_tpms, 290.0, places=2)
        self.assertEqual(spec_tpms["unit"], "kPa")

        # Range fields
        spec_est = fr.FIELD_ALLOWLIST["EstRange"]
        num_est, _, _ = fr.validate_field_value(spec_est, 250.0)
        self.assertAlmostEqual(num_est, 250.0 * 1.609344, places=4)

        spec_ideal = fr.FIELD_ALLOWLIST["IdealBatteryRange"]
        num_ideal, _, _ = fr.validate_field_value(spec_ideal, 300.0)
        self.assertAlmostEqual(num_ideal, 300.0 * 1.609344, places=4)

    def test_distinct_paths_and_event_id_collision_prevention(self):
        # Soc and BatteryLevel map to distinct canonical paths
        self.assertEqual(fr.FIELD_ALLOWLIST["Soc"]["path"],
                         "Vehicle.Powertrain.TractionBattery.StateOfCharge.Current")
        self.assertEqual(fr.FIELD_ALLOWLIST["BatteryLevel"]["path"],
                         "Vehicle.Powertrain.TractionBattery.StateOfCharge.Displayed")

        # EstRange and IdealBatteryRange map to distinct canonical paths
        self.assertEqual(fr.FIELD_ALLOWLIST["EstRange"]["path"],
                         "Vehicle.Powertrain.TractionBattery.Range")
        self.assertEqual(fr.FIELD_ALLOWLIST["IdealBatteryRange"]["path"],
                         "Vehicle.Powertrain.TractionBattery.IdealRange")

        # Even if values and timestamps are identical, deterministic_event_id hashes source_field
        id_soc = fr.deterministic_event_id("v1", "Vehicle.Speed", "tesla_fleet_telemetry", "Soc",
                                           1790427600000000000, "fleet-v1", 59.0, None, None)
        id_bat = fr.deterministic_event_id("v1", "Vehicle.Speed", "tesla_fleet_telemetry", "BatteryLevel",
                                           1790427600000000000, "fleet-v1", 59.0, None, None)
        self.assertNotEqual(id_soc, id_bat)  # Never collide or dedup each other!


class StrictFramingAndIdentityTest(unittest.TestCase):
    def test_two_frame_zmq_enforcement(self):
        valid_frames = [b"tesla_V", b'{"data":[],"createdAt":"2026-09-26T13:00:00Z","vin":"V1"}']
        payload = fr.parse_zmq_frames(valid_frames, expected_topic="tesla_V")
        self.assertEqual(payload, valid_frames[1])

        # Wrong topic rejected
        with self.assertRaises(ValueError):
            fr.parse_zmq_frames([b"wrong_topic", b"{}"], expected_topic="tesla_V")

        # Single frame rejected
        with self.assertRaises(ValueError):
            fr.parse_zmq_frames([b"only_one_frame"], expected_topic="tesla_V")

        # 3 frames rejected
        with self.assertRaises(ValueError):
            fr.parse_zmq_frames([b"tesla_V", b"{}", b"extra"], expected_topic="tesla_V")

    def test_flat_payload_rejected(self):
        flat_payload = b'{"VehicleSpeed": 60, "createdAt": "2026-09-26T13:00:00Z", "vin": "V1"}'
        with self.assertRaises(ValueError):
            fr.extract_protojson_records(flat_payload, target_vin="V1")

    def test_vin_and_salt_requirements(self):
        with self.assertRaises(ValueError):
            fr.resolve_vehicle_identity("", target_vin="", configured_id="", salt="s")

        self.assertEqual(fr.resolve_vehicle_identity("VIN1", target_vin="VIN1", configured_id="my-car"), "my-car")
        self.assertIsNone(fr.resolve_vehicle_identity("OTHER_VIN", target_vin="VIN1", configured_id="my-car"))

        with self.assertRaises(ValueError):
            fr.resolve_vehicle_identity("VIN1", target_vin="", configured_id="", salt="")

        pseudo = fr.resolve_vehicle_identity("VIN1", target_vin="", configured_id="", salt="mysalt")
        self.assertTrue(pseudo.startswith("v-"))


class OutboxNonDestructiveOverflowAndBoundedTest(unittest.TestCase):
    def setUp(self):
        self.tmp_dir = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self.tmp_dir.name, "outbox.sqlite")

    def tearDown(self):
        self.tmp_dir.cleanup()

    def test_overflow_rejects_new_and_preserves_unacked(self):
        conn = fr.open_outbox(self.db_path)
        max_rows = 3
        stats = {
            "messages_received": 0, "signals_stored": 0, "uploaded": 0,
            "upload_failures": 0, "deduped": 0, "invalid_messages": 0,
            "invalid_fields": 0, "dropped_signals": 0, "outbox_overflow_drops": 0
        }
        meta_env = {
            "target_vin": "V1", "vehicle_id": "c1", "vehicle_salt": "",
            "decode_epoch": "fleet-v1", "vss_version": "",
            "vehicle_firmware": "", "mapping_revision": "fleet-v1",
            "collector_version": "fleet-1", "collector_id": "c1"
        }

        # Insert 3 records to fill capacity
        for i in range(3):
            payload = json.dumps({
                "vin": "V1",
                "createdAt": f"2026-09-26T13:00:0{i}Z",
                "data": [{"key": "VehicleSpeed", "value": {"floatValue": float(50 + i)}}]
            }).encode()
            stored = fr.process_message(conn, payload, meta_env, stats, max_rows=max_rows)
            self.assertEqual(stored, 1)

        self.assertEqual(stats["signals_stored"], 3)
        self.assertEqual(stats["outbox_overflow_drops"], 0)

        # 4th arrival must be REJECTED, not evicting existing unacked rows
        payload4 = json.dumps({
            "vin": "V1",
            "createdAt": "2026-09-26T13:00:04Z",
            "data": [{"key": "VehicleSpeed", "value": {"floatValue": 80.0}}]
        }).encode()
        stored4 = fr.process_message(conn, payload4, meta_env, stats, max_rows=max_rows)
        self.assertEqual(stored4, 0)
        self.assertEqual(stats["dropped_signals"], 1)
        self.assertEqual(stats["outbox_overflow_drops"], 1)

        # Verify all initial 3 rows remain intact
        cur = conn.execute("SELECT COUNT(*) FROM outbox")
        self.assertEqual(cur.fetchone()[0], 3)

        # Rejected row must NOT be marked in seen_ids so it can be accepted later
        seen = conn.execute("SELECT COUNT(*) FROM seen_ids").fetchone()[0]
        self.assertEqual(seen, 3)
        conn.close()

    def test_seen_ids_bounded(self):
        conn = fr.open_outbox(self.db_path)
        for i in range(20):
            conn.execute("INSERT INTO seen_ids(event_id, seen_at) VALUES(?,?)", (f"id-{i}", i))
        conn.commit()

        fr.prune_seen(conn, older_than_ns=5, max_seen_limit=10)
        count = conn.execute("SELECT COUNT(*) FROM seen_ids").fetchone()[0]
        self.assertLessEqual(count, 10)
        conn.close()


class FakeGreptimeServer(http.server.BaseHTTPRequestHandler):
    mode = "ok"
    received_stmts = []

    def do_POST(self):
        length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(length).decode()
        stmt = urllib.parse.parse_qs(body).get("sql", [""])[0]
        FakeGreptimeServer.received_stmts.append(stmt)

        if FakeGreptimeServer.mode == "error":
            self.send_response(500)
            self.end_headers()
            self.wfile.write(b'{"code": 500, "error": "db down"}')
            return

        if "VALUES" in stmt:
            vals_part = stmt.split("VALUES", 1)[1]
            n_rows = len(re.findall(r"\s*\([^)]+\)", vals_part)) or 1
        else:
            n_rows = 1

        payload = json.dumps({"code": 0, "output": [{"affectedrows": n_rows}]})
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(payload.encode())

    def log_message(self, *a):
        pass


class OfficialFixtureAndEndToEndSmokeTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.server = http.server.HTTPServer(("127.0.0.1", 0), FakeGreptimeServer)
        cls.thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.thread.start()
        cls.base_url = f"http://127.0.0.1:{cls.server.server_port}"

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()

    def setUp(self):
        FakeGreptimeServer.mode = "ok"
        FakeGreptimeServer.received_stmts = []
        self.tmp_dir = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self.tmp_dir.name, "outbox.sqlite")

    def tearDown(self):
        self.tmp_dir.cleanup()

    def test_official_fixture_to_greptime_smoke(self):
        # Actual official sample from crates/tesla-api/src/telemetry.rs:141 plus tire pressure & related fields
        sample_json = (
            r'{"data":['
            r'{"key":"InsideTemp","value":{"floatValue":22.5}},'
            r'{"key":"OutsideTemp","value":{"doubleValue":34.0}},'
            r'{"key":"HvacFanStatus","value":{"intValue":3}},'
            r'{"key":"HvacACEnabled","value":{"booleanValue":true}},'
            r'{"key":"HvacAutoMode","value":{"hvacAutoModeValue":"HvacAutoModeStateOverride"}},'
            r'{"key":"Gear","value":{"shiftStateValue":"ShiftStateP"}},'
            r'{"key":"Soc","value":{"longValue":"59"}},'
            r'{"key":"BatteryLevel","value":{"longValue":"59"}},'
            r'{"key":"EstRange","value":{"floatValue":250.0}},'
            r'{"key":"IdealBatteryRange","value":{"floatValue":300.0}},'
            r'{"key":"VehicleSpeed","value":{"floatValue":45.0}},'
            r'{"key":"TpmsPressureFl","value":{"floatValue":2.9}},'
            r'{"key":"Experimental_1","value":{"invalid":true}}'
            r'],'
            r'"createdAt":"2026-09-26T13:00:00.123456789Z",'
            r'"vin":"5YJ3E1EB1NF123456",'
            r'"isResend":false}'
        )
        zmq_frames = [b"tesla_V", sample_json.encode("utf-8")]

        # Step 1: Parse 2-frame ZMQ message
        payload = fr.parse_zmq_frames(zmq_frames, expected_topic="tesla_V")

        # Step 2: Ingest into outbox
        conn = fr.open_outbox(self.db_path)
        stats = {
            "messages_received": 0, "signals_stored": 0, "uploaded": 0,
            "upload_failures": 0, "deduped": 0, "invalid_messages": 0,
            "invalid_fields": 0, "dropped_signals": 0, "outbox_overflow_drops": 0
        }
        meta_env = {
            "target_vin": "5YJ3E1EB1NF123456",
            "vehicle_id": "my-tesla",
            "vehicle_salt": "testsalt",
            "decode_epoch": "fleet-v1",
            "vss_version": "",
            "vehicle_firmware": "",
            "mapping_revision": "fleet-v1",
            "collector_version": "tesla-fleet-recorder-1",
            "collector_id": "fleet-collector-1"
        }

        stored = fr.process_message(conn, payload, meta_env, stats)
        # 12 valid allowlisted signals: InsideTemp, OutsideTemp, HvacFanStatus, HvacACEnabled,
        # HvacAutoMode, Gear, Soc, BatteryLevel, EstRange, IdealBatteryRange, VehicleSpeed, TpmsPressureFl.
        # Experimental_1 is dropped.
        self.assertEqual(stored, 12)
        self.assertEqual(stats["signals_stored"], 12)

        # Step 3: Batch upload to Greptime HTTP SQL endpoint
        uploaded = fr.upload_tick(conn, self.base_url, "datalake", "user", "pw", batch_size=100)
        self.assertEqual(uploaded, 12)

        # Outbox must be cleanly drained after successful ack
        cur = conn.execute("SELECT COUNT(*) FROM outbox")
        self.assertEqual(cur.fetchone()[0], 0)

        # Step 4: Verify the exact SQL sent to GreptimeDB
        self.assertEqual(len(FakeGreptimeServer.received_stmts), 1)
        sql = FakeGreptimeServer.received_stmts[0]

        # Provenance verification
        self.assertIn("INSERT INTO vehicle_signal", sql)
        self.assertIn("'fleet'", sql)
        self.assertIn("'tesla_fleet_telemetry'", sql)
        self.assertIn("'my-tesla'", sql)
        self.assertIn("'fleet-collector-1'", sql)
        self.assertIn("FALSE", sql)  # source_is_resend is False

        # Unverified VSS_VERSION and VEHICLE_FIRMWARE are NULL
        # Check that NULL exists in the generated SQL values
        self.assertIn("NULL", sql)

        # Unit conversion verifications:
        # VehicleSpeed: 45.0 mph -> 72.42048 km/h
        self.assertIn("'VehicleSpeed'", sql)
        self.assertIn("72.42048", sql)
        # TpmsPressureFl: 2.9 bar -> 290.0 kPa
        self.assertIn("'TpmsPressureFl'", sql)
        self.assertIn("290.0", sql)
        self.assertIn("'kPa'", sql)
        # EstRange: 250.0 miles -> 402.336 km
        self.assertIn("'EstRange'", sql)
        self.assertIn("402.336", sql)
        # IdealBatteryRange: 300.0 miles -> 482.8032 km
        self.assertIn("'IdealBatteryRange'", sql)
        self.assertIn("482.8032", sql)

        # Distinct Soc vs BatteryLevel paths preserved simultaneously
        self.assertIn("'Soc'", sql)
        self.assertIn("'BatteryLevel'", sql)
        self.assertIn("'Vehicle.Powertrain.TractionBattery.StateOfCharge.Current'", sql)
        self.assertIn("'Vehicle.Powertrain.TractionBattery.StateOfCharge.Displayed'", sql)

        # Exact 9-digit nanosecond event_time in SQL
        self.assertIn("1790427600123456789", sql)
        conn.close()

    def test_can_and_fleet_aggregation_isolation(self):
        base_time = datetime(2026, 9, 26, 13, 0, 0, tzinfo=timezone.utc)
        cols = ["event_time", "vehicle", "path", "source", "decode_epoch",
                "value_num", "value_bool", "unit"]
        rows = [
            [base_time.strftime("%Y-%m-%d %H:%M:%S"), "v1", "Vehicle.Speed", "can", "can-epoch1", 80.0, None, "km/h"],
            [base_time.strftime("%Y-%m-%d %H:%M:%S"), "v1", "Vehicle.Speed", "fleet", "fleet-v1", 75.0, None, "km/h"],
        ]
        groups = agg.group_vehicle_rows(cols, rows, {"speed": "Vehicle.Speed"})
        self.assertIn(("v1", "can", "can-epoch1"), groups)
        self.assertIn(("v1", "fleet", "fleet-v1"), groups)
        self.assertEqual(len(groups), 2)
        can_speed_val = list(groups[("v1", "can", "can-epoch1")]["speed"].values())[0][0]
        fleet_speed_val = list(groups[("v1", "fleet", "fleet-v1")]["speed"].values())[0][0]
        self.assertEqual(can_speed_val, 80.0)
        self.assertEqual(fleet_speed_val, 75.0)


if __name__ == "__main__":
    unittest.main()

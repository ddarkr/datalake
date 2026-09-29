#!/usr/bin/env python3
"""Strict Regression tests for Tesla Fleet Telemetry recorder (scripts/fleet_recorder.py).

Covers (stdlib only):
  - Exact 9-digit nanosecond preservation of original createdAt without fraction truncations
  - Fail-closed behavior on timestamps without explicit timezone or boolean timestamps
  - Protojson oneof field unwrapping (floatValue, doubleValue, intValue, longValue, shiftStateValue, etc.)
  - Tombstone storage for 'invalid'/undecodable/non-finite (quality 'invalid', values NULL)
  - Tombstone storage for out-of-range numerics (quality 'range_rejected', values NULL)
  - Raw passthrough for undocumented-unit battery fields (quality 'unit_unverified')
  - Four-topic dispatch: tesla_V -> signals, tesla_alerts/errors/connectivity -> events
  - Official alert/error/connectivity envelope shapes (camelCase protojson, no invented fields)
  - Active iff endedAt absent; invalid endedAt never implies active; unknown-start retained
  - Privacy: no raw VIN, body, tags, or connection secrets stored
  - Anti-vehicle mixing: mandatory VIN, TARGET_VIN isolation, mandatory salt pseudonymization
  - Official unit conversions (mph -> km/h, miles -> km, bar -> kPa) + kWh battery counters
  - Distinct canonical paths & event_id provenance preservation
  - Outbox overflow non-destructive TOTAL-bound policy across signals + events
  - Bounded seen_ids table prevention of disk growth
  - Old-DB migration preserving pending rows; restart redelivery recovery
  - End-to-end smoke: official 2-frame ZMQ fixtures -> recorder -> Greptime HTTP SQL
  - CAN / Fleet source separation in aggregation
"""

import hashlib
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
        num, text, b, q = fr.validate_field_value(spec_speed, 60.0)
        self.assertAlmostEqual(num, 60.0 * 1.609344, places=4)
        self.assertIsNone(text)
        self.assertIsNone(b)
        self.assertIsNone(q)

        # Odometer raw miles -> VSS km
        spec_odo = fr.FIELD_ALLOWLIST["Odometer"]
        num_odo, _, _, q_odo = fr.validate_field_value(spec_odo, 10000.0)
        self.assertAlmostEqual(num_odo, 10000.0 * 1.609344, places=4)
        self.assertIsNone(q_odo)

        # Tire Pressure raw bar -> VSS kPa (1 bar = 100 kPa)
        spec_tpms = fr.FIELD_ALLOWLIST["TpmsPressureFl"]
        num_tpms, _, _, q_tpms = fr.validate_field_value(spec_tpms, 2.9)
        self.assertAlmostEqual(num_tpms, 290.0, places=2)
        self.assertEqual(spec_tpms["unit"], "kPa")
        self.assertIsNone(q_tpms)

        # Range fields
        spec_est = fr.FIELD_ALLOWLIST["EstRange"]
        num_est, _, _, _ = fr.validate_field_value(spec_est, 250.0)
        self.assertAlmostEqual(num_est, 250.0 * 1.609344, places=4)

        spec_ideal = fr.FIELD_ALLOWLIST["IdealBatteryRange"]
        num_ideal, _, _, _ = fr.validate_field_value(spec_ideal, 300.0)
        self.assertAlmostEqual(num_ideal, 300.0 * 1.609344, places=4)

    def test_tombstone_and_unverified_qualities(self):
        # Undecodable numeric -> 'invalid' tombstone (values NULL)
        spec = fr.FIELD_ALLOWLIST["VehicleSpeed"]
        num, text, b, q = fr.validate_field_value(spec, None)
        self.assertEqual((num, text, b, q), (None, None, None, "invalid"))
        num2, _, _, q2 = fr.validate_field_value(spec, float("nan"))
        self.assertIsNone(num2)
        self.assertEqual(q2, "invalid")
        # Type-ok but out of range -> 'range_rejected' tombstone
        num3, _, _, q3 = fr.validate_field_value(spec, 10000.0)
        self.assertIsNone(num3)
        self.assertEqual(q3, "range_rejected")
        # Undocumented-unit battery field -> raw numeric, 'unit_unverified'
        for field, value in (("BrickVoltageMin", 3.7),
                             ("PackCurrent", -1200.0),
                             ("PackVoltage", 1200.0)):
            bspec = fr.FIELD_ALLOWLIST[field]
            self.assertIsNone(bspec["unit"])
            raw, _, _, bq = fr.validate_field_value(bspec, value)
            self.assertAlmostEqual(raw, value)
            self.assertEqual(bq, "unit_unverified")
        # kWh counters are authoritative per available-data docs
        for field in ("DCChargingEnergyIn", "ACChargingEnergyIn",
                      "EnergyRemaining", "LifetimeEnergyUsed"):
            self.assertEqual(fr.FIELD_ALLOWLIST[field]["unit"], "kWh")
        self.assertEqual(fr.FIELD_ALLOWLIST["ChargeLimitSoc"]["unit"], "%")
        # Enum-typed battery fields stored without reinterpretation
        _, etext, _, eq = fr.validate_field_value(fr.FIELD_ALLOWLIST["BMSState"], "Active")
        self.assertEqual(etext, "Active")
        self.assertIsNone(eq)
        _, _, ebb, ebq = fr.validate_field_value(fr.FIELD_ALLOWLIST["BatteryHeaterOn"], True)
        self.assertEqual(ebb, 1)
        self.assertIsNone(ebq)

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
        topic, payload = fr.parse_zmq_frames(valid_frames)
        self.assertEqual((topic, payload), ("tesla_V", valid_frames[1]))

        # Allowlist dispatch: each official topic accepted, unknown rejected
        for known in ("tesla_V", "tesla_alerts", "tesla_errors", "tesla_connectivity"):
            t, _ = fr.parse_zmq_frames([known.encode(), b"{}"])
            self.assertEqual(t, known)
        with self.assertRaises(ValueError):
            fr.parse_zmq_frames([b"wrong_topic", b"{}"])
        with self.assertRaises(ValueError):
            fr.parse_zmq_frames([b"tesla_V", b"{}"], topics=("tesla_alerts",))

        # Single frame rejected
        with self.assertRaises(ValueError):
            fr.parse_zmq_frames([b"only_one_frame"])

        # 3 frames rejected
        with self.assertRaises(ValueError):
            fr.parse_zmq_frames([b"tesla_V", b"{}", b"extra"])

    def test_topic_allowlist_resolution(self):
        self.assertEqual(fr.resolve_topics(""), list(fr.DEFAULT_TOPICS))
        self.assertEqual(fr.resolve_topics("tesla_V"), ["tesla_V"])
        self.assertEqual(fr.resolve_topics("tesla_V, tesla_alerts"),
                         ["tesla_V", "tesla_alerts"])
        # Unknown names dropped; all-unknown falls back to full default
        self.assertEqual(fr.resolve_topics("tesla_V,nope"), ["tesla_V"])
        self.assertEqual(fr.resolve_topics("nope"), list(fr.DEFAULT_TOPICS))
        # No singular FLEET_ZMQ_TOPIC fallback exists anymore
        self.assertFalse(hasattr(fr, "DEFAULT_TOPIC"))

    def test_flat_payload_rejected(self):
        flat_payload = b'{"VehicleSpeed": 60, "createdAt": "2026-09-26T13:00:00Z", "vin": "V1"}'
        with self.assertRaises(ValueError):
            fr.extract_protojson_records(flat_payload, target_vin="V1")

    def test_vin_and_salt_requirements(self):
        # Missing or empty VIN strictly raises ValueError
        with self.assertRaises(ValueError):
            fr.resolve_vehicle_identity("", target_vin="", configured_id="", salt="s")
        with self.assertRaises(ValueError):
            fr.resolve_vehicle_identity("   ", target_vin="VIN1", configured_id="c1", salt="s")

        # target_vin matches and configured_id provided
        self.assertEqual(fr.resolve_vehicle_identity("VIN1", target_vin="VIN1", configured_id="my-car"), "my-car")
        self.assertEqual(fr.resolve_vehicle_identity("VIN1", target_vin="VIN1", configured_id="my-car", salt="salt"), "my-car")

        # target_vin matches, configured_id empty, salt provided -> uses salted hash
        pseudo_target = fr.resolve_vehicle_identity("5YJ3E1EB123456789", target_vin="5YJ3E1EB123456789", configured_id="", salt="mysalt")
        self.assertTrue(pseudo_target.startswith("v-"))
        self.assertNotEqual(pseudo_target, "v-5YJ3E1EB")  # Raw VIN prefix must NEVER be exposed
        expected_hash = hashlib.sha256(b"mysalt5YJ3E1EB123456789").hexdigest()[:16]
        self.assertEqual(pseudo_target, f"v-{expected_hash}")

        # target_vin matches, configured_id empty, salt empty -> FAIL CLOSED
        with self.assertRaises(ValueError):
            fr.resolve_vehicle_identity("VIN1", target_vin="VIN1", configured_id="", salt="")
        with self.assertRaises(ValueError):
            fr.resolve_vehicle_identity("VIN1", target_vin="VIN1", configured_id="   ", salt="   ")

        # target_vin mismatch -> drop (return None)
        self.assertIsNone(fr.resolve_vehicle_identity("OTHER_VIN", target_vin="VIN1", configured_id="my-car"))
        self.assertIsNone(fr.resolve_vehicle_identity("OTHER_VIN", target_vin="VIN1", configured_id="", salt="mysalt"))

        # No target_vin and no salt -> FAIL CLOSED
        with self.assertRaises(ValueError):
            fr.resolve_vehicle_identity("VIN1", target_vin="", configured_id="", salt="")
        with self.assertRaises(ValueError):
            fr.resolve_vehicle_identity("VIN1", target_vin="", configured_id="my-car", salt="")

        # No target_vin with salt -> salted hash
        pseudo_notarget = fr.resolve_vehicle_identity("5YJ3E1EB123456789", target_vin="", configured_id="", salt="mysalt")
        self.assertEqual(pseudo_notarget, f"v-{expected_hash}")
        self.assertNotEqual(pseudo_notarget, "v-5YJ3E1EB")


class OutboxNonDestructiveOverflowAndBoundedTest(unittest.TestCase):
    def setUp(self):
        self.tmp_dir = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self.tmp_dir.name, "outbox.sqlite")

    def tearDown(self):
        self.tmp_dir.cleanup()

    META = {
        "target_vin": "V1", "vehicle_id": "c1", "vehicle_salt": "",
        "decode_epoch": "fleet-v1", "vss_version": "",
        "vehicle_firmware": "", "mapping_revision": "fleet-v1",
        "collector_version": "fleet-1", "collector_id": "c1",
        "config_version": "",
    }

    def _stats(self):
        return fr.default_stats()

    def _signal_payload(self, sec, speed):
        return json.dumps({
            "vin": "V1",
            "createdAt": f"2026-09-26T13:00:{sec:02d}Z",
            "data": [{"key": "VehicleSpeed", "value": {"floatValue": float(speed)}}]
        }).encode()

    def _alert_payload(self, name, started="2026-09-26T13:00:00Z", ended=None):
        alert = {"name": name, "audiences": ["Customer"], "startedAt": started}
        if ended is not None:
            alert["endedAt"] = ended
        return json.dumps({
            "vin": "V1", "createdAt": "2026-09-26T13:00:05Z", "alerts": [alert],
        }).encode()

    def test_overflow_rejects_new_and_preserves_unacked(self):
        conn = fr.open_outbox(self.db_path)
        max_rows = 3
        stats = self._stats()

        # Insert 3 records to fill capacity
        for i in range(3):
            stored = fr.process_message(conn, self._signal_payload(i, 50 + i),
                                        self.META, stats, max_rows=max_rows)
            self.assertEqual(stored, 1)

        self.assertEqual(stats["signals_stored"], 3)
        self.assertEqual(stats["outbox_overflow_drops"], 0)

        # 4th arrival must be REJECTED, not evicting existing unacked rows
        stored4 = fr.process_message(
            conn, self._signal_payload(4, 80.0), self.META, stats,
            max_rows=max_rows)
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

    def test_total_bound_spans_signals_and_events(self):
        # MAX_OUTBOX_ROWS bounds TOTAL pending signals + events, not each.
        conn = fr.open_outbox(self.db_path)
        max_rows = 3
        stats = self._stats()
        for i in range(2):
            self.assertEqual(fr.process_message(
                conn, self._signal_payload(i, 50 + i), self.META, stats,
                max_rows=max_rows), 1)
        self.assertEqual(fr.process_frame(
            conn, "tesla_alerts", self._alert_payload("bms_a035"),
            self.META, stats, max_rows=max_rows), 1)
        self.assertEqual(stats["events_stored"], 1)
        # Total is now 3: one more signal must overflow even though the
        # signal table alone holds only 2 rows.
        self.assertEqual(fr.process_message(
            conn, self._signal_payload(9, 80.0), self.META, stats,
            max_rows=max_rows), 0)
        self.assertEqual(stats["outbox_overflow_drops"], 1)
        total = conn.execute("SELECT COUNT(*) FROM outbox").fetchone()[0]
        total += conn.execute("SELECT COUNT(*) FROM outbox_events").fetchone()[0]
        self.assertEqual(total, 3)
        conn.close()

    def test_event_overflow_keeps_signals(self):
        conn = fr.open_outbox(self.db_path)
        stats = self._stats()
        for i in range(3):
            fr.process_message(conn, self._signal_payload(i, 50 + i),
                               self.META, stats, max_rows=3)
        stored = fr.process_frame(conn, "tesla_alerts",
                                  self._alert_payload("bms_a035"),
                                  self.META, stats, max_rows=3)
        self.assertEqual(stored, 0)
        self.assertEqual(stats["dropped_events"], 1)
        self.assertEqual(stats["event_overflow_drops"], 1)
        self.assertEqual(conn.execute("SELECT COUNT(*) FROM outbox").fetchone()[0], 3)
        self.assertEqual(conn.execute("SELECT COUNT(*) FROM outbox_events").fetchone()[0], 0)
        conn.close()

    def test_old_db_migrates_without_losing_pending(self):
        # Durable rows written by the old tesla_V-only schema keep their data
        # and gain the new provenance columns on open.
        conn = sqlite3.connect(self.db_path, timeout=30)
        conn.execute("CREATE TABLE outbox(event_id TEXT PRIMARY KEY,"
                     " event_time INTEGER NOT NULL, vehicle TEXT NOT NULL,"
                     " path TEXT NOT NULL, source TEXT NOT NULL,"
                     " decode_epoch TEXT NOT NULL, value_num REAL,"
                     " value_text TEXT, value_bool INTEGER, unit TEXT,"
                     " vss_version TEXT, vehicle_firmware TEXT,"
                     " dbc_primary_commit TEXT, dbc_supplemental_commit TEXT,"
                     " dbc_override_version TEXT, dbc_override_commit TEXT,"
                     " mapping_revision TEXT, collector_version TEXT,"
                     " ingest_time INTEGER NOT NULL, source_system TEXT,"
                     " source_field TEXT, collector_id TEXT,"
                     " source_is_resend INTEGER)")
        conn.execute("INSERT INTO outbox(event_id, event_time, vehicle, path,"
                     " source, decode_epoch, value_num, ingest_time)"
                     " VALUES('old-id', 1790427600000000000, 'c1', 'Vehicle.Speed',"
                     " 'fleet', 'fleet-v1', 72.4, 1790427601000000000)")
        conn.commit()
        conn.close()
        conn = fr.open_outbox(self.db_path)
        row = conn.execute("SELECT event_id, value_num, quality, envelope_id,"
                           " config_version FROM outbox").fetchone()
        self.assertEqual(row[0], "old-id")
        self.assertAlmostEqual(row[1], 72.4)
        self.assertIsNone(row[2])
        self.assertIsNone(row[3])
        self.assertIsNone(row[4])
        stats = self._stats()
        self.assertEqual(fr.process_message(
            conn, self._signal_payload(1, 51.0), self.META, stats,
            max_rows=50000), 1)
        self.assertEqual(conn.execute("SELECT COUNT(*) FROM outbox").fetchone()[0], 2)
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

    META = {
        "target_vin": "5YJ3E1EB1NF123456",
        "vehicle_id": "my-tesla",
        "vehicle_salt": "testsalt",
        "decode_epoch": "fleet-v1",
        "vss_version": "",
        "vehicle_firmware": "",
        "mapping_revision": "fleet-v1",
        "collector_version": "tesla-fleet-recorder-1",
        "collector_id": "fleet-collector-1",
        "config_version": "",
    }

    def test_official_fixture_to_greptime_smoke(self):
        # Actual official sample matching telemetry-config.sh:675-682 (8 official fields:
        # Gear, DriverSeatOccupied, InsideTemp, OutsideTemp, HvacFanStatus, HvacACEnabled, HvacAutoMode, Soc)
        # plus additional fleet allowlist fields (BatteryLevel, EstRange, IdealBatteryRange, VehicleSpeed, TpmsPressureFl).
        sample_json = (
            r'{"data":['
            r'{"key":"InsideTemp","value":{"floatValue":22.5}},'
            r'{"key":"OutsideTemp","value":{"doubleValue":34.0}},'
            r'{"key":"HvacFanStatus","value":{"intValue":3}},'
            r'{"key":"HvacACEnabled","value":{"booleanValue":true}},'
            r'{"key":"HvacAutoMode","value":{"hvacAutoModeValue":"HvacAutoModeStateOverride"}},'
            r'{"key":"Gear","value":{"shiftStateValue":"ShiftStateP"}},'
            r'{"key":"DriverSeatOccupied","value":{"booleanValue":true}},'
            r'{"key":"Soc","value":{"longValue":"59"}},'
            r'{"key":"BatteryLevel","value":{"longValue":"59"}},'
            r'{"key":"EstRange","value":{"floatValue":250.0}},'
            r'{"key":"IdealBatteryRange","value":{"floatValue":300.0}},'
            r'{"key":"VehicleSpeed","value":{"floatValue":45.0}},'
            r'{"key":"TpmsPressureFl","value":{"floatValue":2.9}},'
            r'{"key":"DCChargingEnergyIn","value":{"floatValue":12.5}},'
            r'{"key":"BrickVoltageMin","value":{"floatValue":3.7}},'
            r'{"key":"Experimental_1","value":{"invalid":true}}'
            r'],'
            r'"createdAt":"2026-09-26T13:00:00.123456789Z",'
            r'"vin":"5YJ3E1EB1NF123456",'
            r'"isResend":false}'
        )
        zmq_frames = [b"tesla_V", sample_json.encode("utf-8")]

        # Step 1: Parse 2-frame ZMQ message (topic dispatch included)
        topic, payload = fr.parse_zmq_frames(zmq_frames)
        self.assertEqual(topic, "tesla_V")

        # Step 2: Ingest into outbox
        conn = fr.open_outbox(self.db_path)
        stats = fr.default_stats()

        stored = fr.process_frame(conn, topic, payload, self.META, stats)
        # 15 allowlisted signals: 8 from telemetry-config.sh (InsideTemp,
        # OutsideTemp, HvacFanStatus, HvacACEnabled, HvacAutoMode, Gear,
        # DriverSeatOccupied, Soc) plus BatteryLevel, EstRange,
        # IdealBatteryRange, VehicleSpeed, TpmsPressureFl,
        # DCChargingEnergyIn, BrickVoltageMin. Experimental_1 is not
        # allowlisted and is ignored silently (no row, no counter).
        self.assertEqual(stored, 15)
        self.assertEqual(stats["signals_stored"], 15)
        raw = conn.execute(
            "SELECT value_num, quality FROM outbox WHERE source_field='BrickVoltageMin'"
        ).fetchone()
        self.assertAlmostEqual(raw[0], 3.7)
        self.assertEqual(raw[1], "unit_unverified")
        self.assertEqual(conn.execute(
            "SELECT COUNT(*) FROM outbox WHERE source_field='Experimental_1'").fetchone()[0], 0)

        # Step 3: Batch upload to Greptime HTTP SQL endpoint
        uploaded = fr.upload_tick(conn, self.base_url, "datalake", "user", "pw", batch_size=100)
        self.assertEqual(uploaded, 15)

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

        # All 8 telemetry-config.sh fields are verified in output
        self.assertIn("'Gear'", sql)
        self.assertIn("'P'", sql)
        self.assertIn("'DriverSeatOccupied'", sql)
        self.assertIn("'Vehicle.Tesla.DriverSeatOccupied'", sql)
        self.assertIn("'InsideTemp'", sql)
        self.assertIn("22.5", sql)
        self.assertIn("'OutsideTemp'", sql)
        self.assertIn("34.0", sql)
        self.assertIn("'HvacFanStatus'", sql)
        self.assertIn("'HvacACEnabled'", sql)
        self.assertIn("'HvacAutoMode'", sql)
        self.assertIn("'Soc'", sql)
        self.assertIn("59.0", sql)

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

    def test_alert_errors_connectivity_dispatch_to_events(self):
        conn = fr.open_outbox(self.db_path)
        stats = fr.default_stats()
        # Active alert (no endedAt) + ended alert with duration.
        alert_payload = json.dumps({
            "vin": "5YJ3E1EB1NF123456",
            "createdAt": "2026-09-26T13:00:00.123456789Z",
            "alerts": [
                {"name": "bms_a035",
                 "audiences": ["Customer"],
                 "startedAt": "2026-09-26T12:00:00Z"},
                {"name": "bms_a079",
                 "audiences": ["Service", "ServiceFix"],
                 "startedAt": "2026-09-26T11:00:00Z",
                 "endedAt": "2026-09-26T11:30:00Z"},
            ],
        }).encode()
        t, p = fr.parse_zmq_frames([b"tesla_alerts", alert_payload])
        self.assertEqual(t, "tesla_alerts")
        self.assertEqual(fr.process_frame(conn, t, p, self.META, stats), 2)
        self.assertEqual(stats["events_stored"], 2)
        active = conn.execute(
            "SELECT name, event_type, audience, is_active, duration_s,"
            " quality, episode_id, envelope_id FROM outbox_events"
            " WHERE name='bms_a035'").fetchone()
        self.assertEqual(active[1], "alerts")
        self.assertEqual(active[2], "Customer")
        self.assertEqual(active[3], 1)
        self.assertIsNone(active[4])
        self.assertIsNone(active[5])
        self.assertIsNotNone(active[6])  # episode links on valid start
        ended = conn.execute(
            "SELECT is_active, duration_s, quality, audience FROM outbox_events"
            " WHERE name='bms_a079'").fetchone()
        self.assertEqual(ended[0], 0)
        self.assertAlmostEqual(ended[1], 1800.0)
        self.assertIsNone(ended[2])
        self.assertEqual(ended[3], "Service,ServiceFix")
        # Same envelope id rides both rows of the envelope.
        envs = {r[0] for r in conn.execute("SELECT envelope_id FROM outbox_events").fetchall()}
        self.assertEqual(len(envs), 1)

        # Errors: per-element time, body presence only, no body/tags stored.
        err_payload = json.dumps({
            "vin": "5YJ3E1EB1NF123456",
            "createdAt": "2026-09-26T13:00:00Z",
            "errors": [{"createdAt": "2026-09-26T12:59:00Z",
                        "name": "PCS_a019",
                        "tags": {"ecu": "pcs"},
                        "body": "secret-stack-trace"}],
        }).encode()
        self.assertEqual(fr.process_frame(
            conn, "tesla_errors", err_payload, self.META, stats), 1)
        erow = conn.execute(
            "SELECT event_type, event_time, body_redacted, quality FROM"
            " outbox_events WHERE name='PCS_a019'").fetchone()
        self.assertEqual(erow[0], "errors")
        self.assertEqual(erow[1], fr.parse_created_at("2026-09-26T12:59:00Z"))
        self.assertEqual(erow[2], 1)
        self.assertIsNone(erow[3])
        cols = [r[1] for r in conn.execute("PRAGMA table_info(outbox_events)").fetchall()]
        for banned in ("body", "tags", "connection_id", "network_interface", "vin"):
            self.assertNotIn(banned, cols)

        # Connectivity stays separate; disconnected never closes alerts.
        for status in ("CONNECTED", "DISCONNECTED"):
            disc_payload = json.dumps({
                "vin": "5YJ3E1EB1NF123456",
                "createdAt": "2026-09-26T13:05:00Z",
                "connection_id": "opaque-conn-id",
                "network_interface": "cellular",
                "status": status,
            }).encode()
            self.assertEqual(fr.process_frame(
                conn, "tesla_connectivity", disc_payload, self.META,
                stats), 1)
        crows = conn.execute(
            "SELECT connectivity, is_active, episode_id FROM outbox_events"
            " WHERE event_type='connectivity' ORDER BY event_time").fetchall()
        self.assertEqual([r[0] for r in crows], ["CONNECTED", "DISCONNECTED"])
        self.assertEqual([r[1] for r in crows], [1, 0])
        # Same socket -> same episode; a concurrent socket gets its own, so a
        # wifi DISCONNECTED never reads as the cellular socket going away.
        self.assertIsNotNone(crows[0][2])
        self.assertEqual(crows[0][2], crows[1][2])
        other = json.dumps({
            "vin": "5YJ3E1EB1NF123456", "createdAt": "2026-09-26T13:06:00Z",
            "connectionId": "second-socket", "status": "CONNECTED",
        }).encode()
        self.assertEqual(fr.process_frame(
            conn, "tesla_connectivity", other, self.META, stats), 1)
        second = conn.execute(
            "SELECT episode_id FROM outbox_events WHERE event_type='connectivity'"
            " AND event_time=?", (fr.parse_created_at("2026-09-26T13:06:00Z"),)).fetchone()[0]
        self.assertNotIn(second, (None, crows[0][2]))
        leak = conn.execute(
            "SELECT COUNT(*) FROM outbox_events WHERE connectivity LIKE '%opaque%'"
            " OR connectivity LIKE '%cellular%' OR episode_id LIKE '%opaque%'"
            " OR episode_id LIKE '%second-socket%'").fetchone()[0]
        self.assertEqual(leak, 0)
        # Earlier alert rows untouched by connectivity arrivals.
        self.assertEqual(conn.execute(
            "SELECT COUNT(*) FROM outbox_events WHERE event_type='alerts'").fetchone()[0], 2)

        # Upload routes events to vehicle_event (only table with rows here).
        uploaded = fr.upload_tick(conn, self.base_url, "datalake", "user", "pw",
                                  batch_size=100, stats=stats)
        self.assertEqual(uploaded, 6)
        self.assertEqual(stats["events_uploaded"], 6)
        self.assertEqual(stats["uploaded"], 6)
        stmts = FakeGreptimeServer.received_stmts
        self.assertEqual(len(stmts), 1)
        self.assertIn("INSERT INTO vehicle_event", stmts[0])
        self.assertIn("'alerts'", stmts[0])
        self.assertIn("'errors'", stmts[0])
        self.assertIn("'connectivity'", stmts[0])
        self.assertNotIn("secret-stack-trace", stmts[0])
        self.assertNotIn("5YJ3E1EB1NF123456", stmts[0])
        self.assertEqual(conn.execute("SELECT COUNT(*) FROM outbox_events").fetchone()[0], 0)
        conn.close()

    def test_unknown_start_and_bad_end_adversarial(self):
        conn = fr.open_outbox(self.db_path)
        stats = fr.default_stats()
        # Missing start retains the warning; duration/episode excluded.
        no_start = json.dumps({
            "vin": "5YJ3E1EB1NF123456", "createdAt": "2026-09-26T13:00:00Z",
            "alerts": [{"name": "bms_w164", "audiences": ["Customer"]}],
        }).encode()
        self.assertEqual(fr.process_frame(
            conn, "tesla_alerts", no_start, self.META, stats), 1)
        row = conn.execute(
            "SELECT quality, duration_s, episode_id, event_id, is_active"
            " FROM outbox_events").fetchone()
        self.assertEqual(row[0], "unknown_start")
        self.assertIsNone(row[1])
        self.assertIsNone(row[2])
        self.assertIsNone(row[4])  # no start: activity unknown, warning kept
        first_id = row[3]
        # Invalid endedAt: kept as quality 'error', ended NULL, NOT active.
        bad_end = json.dumps({
            "vin": "5YJ3E1EB1NF123456", "createdAt": "2026-09-26T13:00:00Z",
            "alerts": [{"name": "bms_w164",
                        "startedAt": "2026-09-26T12:00:00Z",
                        "endedAt": "not-a-time"}],
        }).encode()
        self.assertEqual(fr.process_frame(
            conn, "tesla_alerts", bad_end, self.META, stats), 1)
        bad = conn.execute(
            "SELECT quality, ended_at, is_active FROM outbox_events"
            " ORDER BY rowid DESC LIMIT 1").fetchone()
        self.assertEqual(bad[0], "error")
        self.assertIsNone(bad[1])
        self.assertIsNone(bad[2])  # parse failure never implies active
        # Conflicting end (end < start): both kept, no duration, new identity.
        conflict = json.dumps({
            "vin": "5YJ3E1EB1NF123456", "createdAt": "2026-09-26T13:00:00Z",
            "alerts": [{"name": "bms_w164",
                        "startedAt": "2026-09-26T12:00:00Z",
                        "endedAt": "2026-09-26T11:00:00Z"}],
        }).encode()
        self.assertEqual(fr.process_frame(
            conn, "tesla_alerts", conflict, self.META, stats), 1)
        conf = conn.execute(
            "SELECT started_at, ended_at, duration_s, event_id FROM outbox_events"
            " ORDER BY rowid DESC LIMIT 1").fetchone()
        self.assertLess(conf[1], conf[0])
        self.assertIsNone(conf[2])
        self.assertNotEqual(conf[3], first_id)
        # No invented top-level name/status fallback: envelope without the
        # official array fails closed and counts invalid.
        bogus = json.dumps({"vin": "5YJ3E1EB1NF123456",
                            "createdAt": "2026-09-26T13:00:00Z",
                            "name": "bms_a035"}).encode()
        before = stats["invalid_events"]
        self.assertEqual(fr.process_frame(
            conn, "tesla_alerts", bogus, self.META, stats), 0)
        self.assertEqual(stats["invalid_events"], before + 1)
        conn.close()

    def test_restart_redelivery_and_config_provenance(self):
        conn = fr.open_outbox(self.db_path)
        stats = fr.default_stats()
        payload = json.dumps({
            "vin": "5YJ3E1EB1NF123456",
            "createdAt": "2026-09-26T13:00:00.123456789Z",
            "data": [{"key": "Soc", "value": {"longValue": "59"}}],
        }).encode()
        alert = json.dumps({
            "vin": "5YJ3E1EB1NF123456",
            "createdAt": "2026-09-26T13:00:00Z",
            "alerts": [{"name": "bms_a035",
                        "startedAt": "2026-09-26T12:00:00Z"}],
        }).encode()
        meta = dict(self.META, config_version="cfg-7")
        self.assertEqual(fr.process_frame(conn, "tesla_V", payload, meta, stats), 1)
        self.assertEqual(fr.process_frame(conn, "tesla_alerts", alert, meta, stats), 1)
        # Simulate restart: reopen DB, redeliver identical frames -> deduped.
        conn.close()
        conn = fr.open_outbox(self.db_path)
        self.assertEqual(fr.process_frame(conn, "tesla_V", payload, meta, stats), 0)
        self.assertEqual(fr.process_frame(conn, "tesla_alerts", alert, meta, stats), 0)
        self.assertEqual(stats["deduped"], 2)
        self.assertEqual(conn.execute("SELECT COUNT(*) FROM outbox").fetchone()[0], 1)
        self.assertEqual(conn.execute("SELECT COUNT(*) FROM outbox_events").fetchone()[0], 1)
        # Operator-supplied config version rides rows; never invented.
        self.assertEqual(conn.execute(
            "SELECT config_version FROM outbox").fetchone()[0], "cfg-7")
        self.assertEqual(conn.execute(
            "SELECT config_version FROM outbox_events").fetchone()[0], "cfg-7")
        sig_id = conn.execute("SELECT event_id FROM outbox").fetchone()[0]
        ev_id = conn.execute("SELECT event_id FROM outbox_events").fetchone()[0]
        self.assertNotEqual(sig_id, ev_id)
        uploaded = fr.upload_tick(conn, self.base_url, "datalake", "user", "pw", batch_size=100)
        self.assertEqual(uploaded, 2)
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

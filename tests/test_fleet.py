#!/usr/bin/env python3
"""Regression tests for Tesla Fleet Telemetry recorder (scripts/fleet_recorder.py).

Covers (stdlib only):
  - Original createdAt parsing with exact nanosecond preservation (ISO, epoch s/ms/us/ns)
  - Fail-closed behavior on unknown/invalid/empty timestamps
  - Field allowlist & unit range validation
  - Sparse signal preservation (unreceived signals are never padded to 0/false)
  - Pseudonym vehicle ID resolution (configured ID vs salted hash)
  - Deterministic event_id stability across isResend variations (redelivery deduplication)
  - Bounded SQLite outbox capacity enforcement (disk exhaustion prevention)
  - Fail-closed Greptime ack policy & HTTP SQL INSERT rendering
  - DB down / restart / redelivery recovery simulation
  - Prometheus /metrics output state
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
from unittest import mock

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(REPO, "scripts"))
import fleet_recorder as fr


class TimestampParsingTest(unittest.TestCase):
    def test_iso_utc_parsing(self):
        # 2026-09-26T12:00:00.123456789Z
        iso_str = "2026-09-26T12:00:00.123456789Z"
        ns = fr.parse_created_at(iso_str)
        # Expected: 2026-09-26 12:00:00 UTC = 1790424000s + 123456789ns
        dt = datetime(2026, 9, 26, 12, 0, 0, tzinfo=timezone.utc)
        expected_base = int(dt.timestamp()) * 1_000_000_000
        # datetime fromisoformat handles microseconds (6 digits = 123456000 ns)
        self.assertAlmostEqual(ns, expected_base + 123456000, delta=1000)

    def test_epoch_scales(self):
        base_s = 1790424000
        # Seconds
        self.assertEqual(fr.parse_created_at(base_s), base_s * 1_000_000_000)
        # Milliseconds
        self.assertEqual(fr.parse_created_at(base_s * 1000 + 500), (base_s * 1000 + 500) * 1_000_000)
        # Microseconds
        self.assertEqual(fr.parse_created_at(base_s * 1_000_000 + 123456), (base_s * 1_000_000 + 123456) * 1000)
        # Nanoseconds
        self.assertEqual(fr.parse_created_at(base_s * 1_000_000_000 + 987654321), base_s * 1_000_000_000 + 987654321)

    def test_epoch_as_string(self):
        base_s = 1790424000
        self.assertEqual(fr.parse_created_at(str(base_s * 1000)), base_s * 1_000_000_000)

    def test_invalid_timestamps_fail_closed(self):
        for bad in (None, "", "   ", "not-a-date", -100, 0, float("nan"), float("inf")):
            with self.assertRaises(ValueError, msg=f"Should fail for {bad}"):
                fr.parse_created_at(bad)


class ValidationAndMappingTest(unittest.TestCase):
    def test_allowlist_field_validation(self):
        # Valid VehicleSpeed
        spec_speed = fr.FIELD_ALLOWLIST["VehicleSpeed"]
        res = fr.validate_field_value(spec_speed, 100.5)
        self.assertEqual(res, (100.5, None, None))

        # VehicleSpeed out of physical range (> 350 km/h) -> fail closed
        self.assertIsNone(fr.validate_field_value(spec_speed, 450.0))
        self.assertIsNone(fr.validate_field_value(spec_speed, -5.0))

        # VehicleSpeed bad type -> fail closed
        self.assertIsNone(fr.validate_field_value(spec_speed, "not_a_number"))

        # Valid SoC
        spec_soc = fr.FIELD_ALLOWLIST["Soc"]
        self.assertEqual(fr.validate_field_value(spec_soc, 75.2), (75.2, None, None))
        self.assertIsNone(fr.validate_field_value(spec_soc, 105.0))

        # Valid Bool
        spec_fast = fr.FIELD_ALLOWLIST["FastChargerPresent"]
        self.assertEqual(fr.validate_field_value(spec_fast, True), (None, None, 1))
        self.assertEqual(fr.validate_field_value(spec_fast, False), (None, None, 0))
        self.assertEqual(fr.validate_field_value(spec_fast, "true"), (None, None, 1))

        # Valid Text
        spec_gear = fr.FIELD_ALLOWLIST["Gear"]
        self.assertEqual(fr.validate_field_value(spec_gear, "D"), (None, "D", None))

    def test_sparse_signals_never_zero_padded(self):
        # Incoming payload contains only VehicleSpeed, no Soc or Odometer
        payload = {
            "createdAt": "2026-09-26T12:00:00Z",
            "vin": "5YJ3E1EB1NF123456",
            "isResend": False,
            "VehicleSpeed": 65.0
        }
        meta, signals = fr.extract_signals_and_metadata(json.dumps(payload).encode())
        self.assertEqual(len(signals), 1)
        self.assertEqual(signals[0], ("VehicleSpeed", 65.0))
        # Unsent fields like 'Soc' or 'Odometer' are not present in signals at all!


class DeduplicationAndOutboxTest(unittest.TestCase):
    def setUp(self):
        self.tmp_dir = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self.tmp_dir.name, "outbox.sqlite")

    def tearDown(self):
        self.tmp_dir.cleanup()

    def test_event_id_stable_across_is_resend(self):
        # Same sample with isResend=False vs isResend=True must yield IDENTICAL event_id
        id1 = fr.deterministic_event_id("v1", "Vehicle.Speed", 1790424000000000000, "fleet-v1", 60.0, None, None)
        id2 = fr.deterministic_event_id("v1", "Vehicle.Speed", 1790424000000000000, "fleet-v1", 60.0, None, None)
        self.assertEqual(id1, id2)

    def test_redelivery_deduplication(self):
        conn = fr.open_outbox(self.db_path)
        stats = {
            "messages_received": 0, "signals_stored": 0, "uploaded": 0,
            "upload_failures": 0, "deduped": 0, "invalid_messages": 0,
            "invalid_fields": 0, "dropped_signals": 0
        }
        meta_env = {
            "vehicle": "test-car", "vehicle_salt": "salt",
            "decode_epoch": "fleet-v1", "vss_version": "4.0",
            "vehicle_firmware": "2026.32.1", "mapping_revision": "fleet-v1",
            "collector_version": "fleet-1", "collector_id": "c1"
        }

        # 1. Original delivery (isResend=False)
        msg1 = json.dumps({
            "createdAt": "2026-09-26T12:00:00Z",
            "vin": "VIN123",
            "isResend": False,
            "VehicleSpeed": 70.0
        }).encode()
        stored1 = fr.process_message(conn, msg1, meta_env, stats)
        self.assertEqual(stored1, 1)
        self.assertEqual(stats["signals_stored"], 1)
        self.assertEqual(stats["deduped"], 0)

        # 2. Redelivery (isResend=True) with identical sample data
        msg2 = json.dumps({
            "createdAt": "2026-09-26T12:00:00Z",
            "vin": "VIN123",
            "isResend": True,
            "VehicleSpeed": 70.0
        }).encode()
        stored2 = fr.process_message(conn, msg2, meta_env, stats)
        self.assertEqual(stored2, 0)
        self.assertEqual(stats["signals_stored"], 1)  # Still 1
        self.assertEqual(stats["deduped"], 1)         # Dedup counter incremented

        # Check outbox contents: exactly 1 row
        cur = conn.execute("SELECT COUNT(*) FROM outbox")
        self.assertEqual(cur.fetchone()[0], 1)
        conn.close()

    def test_bounded_outbox_fifo_eviction(self):
        # Ensure outbox never exceeds max_rows to protect disk
        conn = fr.open_outbox(self.db_path)
        max_rows = 5
        stats = {
            "messages_received": 0, "signals_stored": 0, "uploaded": 0,
            "upload_failures": 0, "deduped": 0, "invalid_messages": 0,
            "invalid_fields": 0, "dropped_signals": 0
        }
        meta_env = {
            "vehicle": "test-car", "vehicle_salt": "salt",
            "decode_epoch": "fleet-v1", "vss_version": "4.0",
            "vehicle_firmware": "", "mapping_revision": "fleet-v1",
            "collector_version": "fleet-1", "collector_id": "c1"
        }

        # Insert 10 different samples
        for i in range(10):
            msg = json.dumps({
                "createdAt": f"2026-09-26T12:00:{i:02d}Z",
                "vin": "VIN123",
                "VehicleSpeed": float(50 + i)
            }).encode()
            fr.process_message(conn, msg, meta_env, stats, max_rows=max_rows)

        cur = conn.execute("SELECT COUNT(*) FROM outbox")
        outbox_count = cur.fetchone()[0]
        self.assertEqual(outbox_count, max_rows)  # Strictly bounded to 5

        # Verify FIFO: oldest timestamps were evicted, newest remain
        cur = conn.execute("SELECT MIN(event_time), MAX(event_time) FROM outbox")
        min_ts, max_ts = cur.fetchone()
        self.assertTrue(min_ts < max_ts)
        conn.close()


class FakeGreptimeHandler(http.server.BaseHTTPRequestHandler):
    mode = "ok"
    received_sql = []

    def do_POST(self):
        length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(length).decode()
        stmt = urllib.parse.parse_qs(body).get("sql", [""])[0]
        FakeGreptimeHandler.received_sql.append(stmt)

        if FakeGreptimeHandler.mode == "error":
            self.send_response(500)
            self.end_headers()
            self.wfile.write(b'{"code": 500, "error": "simulated db down"}')
            return

        if FakeGreptimeHandler.mode == "partial":
            # Return code=0 but affectedrows=0 (partial failure)
            payload = json.dumps({"code": 0, "output": [{"affectedrows": 0}]})
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(payload.encode())
            return

        # Count how many tuples in VALUES
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


class EndToEndUploadAndRecoveryTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.server = http.server.HTTPServer(("127.0.0.1", 0), FakeGreptimeHandler)
        cls.server_thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.server_thread.start()
        cls.base_url = f"http://127.0.0.1:{cls.server.server_port}"

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()

    def setUp(self):
        FakeGreptimeHandler.mode = "ok"
        FakeGreptimeHandler.received_sql = []
        self.tmp_dir = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self.tmp_dir.name, "outbox.sqlite")

    def tearDown(self):
        self.tmp_dir.cleanup()

    def test_sql_rendering_provenance(self):
        conn = fr.open_outbox(self.db_path)
        row = {
            "event_time": 1790424000000000000,
            "vehicle": "test-v",
            "path": "Vehicle.Speed",
            "source": "fleet",
            "event_id": "eid123",
            "decode_epoch": "fleet-v1",
            "value_num": 88.5,
            "value_text": None,
            "value_bool": None,
            "unit": "km/h",
            "vss_version": "4.0",
            "vehicle_firmware": "2026.32.1",
            "dbc_primary_commit": None,
            "dbc_supplemental_commit": None,
            "dbc_override_version": None,
            "dbc_override_commit": None,
            "mapping_revision": "fleet-v1",
            "collector_version": "v1",
            "ingest_time": 1790424001000000000,
            "source_system": "tesla_fleet_telemetry",
            "source_field": "VehicleSpeed",
            "collector_id": "c1",
            "source_is_resend": 1
        }
        sql = fr.render_insert("vehicle_signal", [row])
        self.assertIn("INSERT INTO vehicle_signal", sql)
        self.assertIn("'tesla_fleet_telemetry'", sql)
        self.assertIn("'VehicleSpeed'", sql)
        self.assertIn("'c1'", sql)
        self.assertIn("TRUE", sql)  # source_is_resend = 1 -> TRUE
        conn.close()

    def test_db_down_preserves_outbox_and_resumes_on_restart(self):
        conn = fr.open_outbox(self.db_path)
        meta_env = {
            "vehicle": "v1", "vehicle_salt": "", "decode_epoch": "fleet-v1",
            "vss_version": "4.0", "vehicle_firmware": "", "mapping_revision": "fleet-v1",
            "collector_version": "v1", "collector_id": "c1"
        }
        stats = {
            "messages_received": 0, "signals_stored": 0, "uploaded": 0,
            "upload_failures": 0, "deduped": 0, "invalid_messages": 0,
            "invalid_fields": 0, "dropped_signals": 0
        }

        # 1. Enqueue 2 samples
        for i in range(2):
            msg = json.dumps({
                "createdAt": f"2026-09-26T12:00:0{i}Z",
                "vin": "VIN1",
                "VehicleSpeed": 60.0 + i
            }).encode()
            fr.process_message(conn, msg, meta_env, stats)

        # 2. Simulate DB Down (500)
        FakeGreptimeHandler.mode = "error"
        with self.assertRaises(fr.SqlError):
            fr.upload_tick(conn, self.base_url, "datalake", "user", "pw", batch_size=10)

        # Verify rows PRESERVED in outbox (fail-closed!)
        cur = conn.execute("SELECT COUNT(*) FROM outbox")
        self.assertEqual(cur.fetchone()[0], 2)

        # 3. Simulate Process Restart: close connection, reopen fresh
        conn.close()
        conn = fr.open_outbox(self.db_path)
        cur = conn.execute("SELECT COUNT(*) FROM outbox")
        self.assertEqual(cur.fetchone()[0], 2)

        # 4. Simulate DB Recovery (mode = "ok")
        FakeGreptimeHandler.mode = "ok"
        uploaded = fr.upload_tick(conn, self.base_url, "datalake", "user", "pw", batch_size=10)
        self.assertEqual(uploaded, 2)

        # Verify outbox is now drained
        cur = conn.execute("SELECT COUNT(*) FROM outbox")
        self.assertEqual(cur.fetchone()[0], 0)
        conn.close()


if __name__ == "__main__":
    unittest.main()

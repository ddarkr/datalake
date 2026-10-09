"""Synthetic actual HTTP/SQLite regression; no CAN device or production secrets.

Run: python -m unittest discover -s tests -p test_can_receiver.py
"""
import base64
import contextlib
import gzip
from http.server import BaseHTTPRequestHandler, HTTPServer
import json
from pathlib import Path
import tempfile
import sys
import threading
import time
import unittest
from unittest.mock import patch
import urllib.error
import urllib.parse
import urllib.request

from scripts.ingest.can.can_decoder import Decoder
from scripts.ingest.can.can_receiver import Archive, COLUMNS, ConfigurationError, DEFAULT_MAX_BODY_BYTES, DownstreamError, Greptime, Receiver, Rejection, Worker, _dirty_inserts, _ns_timestamp, archive_status, candidate_sql, encode_body, render_insert
from scripts.ingest.can.can_otlp_wire import MAX_REQUEST_BYTES, WireError, check_response, decode_batch, encode_batch


def synthetic_decoder(directory, revision="synthetic-v1"):
    """Entirely invented definitions: no vehicle DBC or observation evidence."""
    dbc = Path(directory) / "synthetic.dbc"
    definitions = Path(directory) / "synthetic.json"
    dbc.write_text('VERSION "synthetic"\n\nNS_ :\n\nBS_:\n\nBU_: Fixture\n\nBO_ 291 Sample: 2 Fixture\n SG_ Power : 0|16@1- (0.5,0) [-16384|16383.5] "kW" Fixture\n', encoding="utf-8")
    definitions.write_text(json.dumps({"revision": revision, "signals": [{
        "source": "synthetic", "id": "0x123", "signal": "Power",
        "source_signal": "Power", "kind": "data", "start_bit": 0,
        "bit_length": 16, "byte_order": "little_endian", "signed": True,
        "scale": 0.5, "offset": 0, "is_multiplexer": False,
        "multiplexer_signal": None, "multiplexer_ids": None,
        "actual_dbc_length": 2, "unit": "kW", "source_unit": "kW",
        "choices": {}, "evidence": "synthetic@0123456789abcdef"
    }]}), encoding="utf-8")
    return Decoder(dbc, definitions)


class SQLSink(BaseHTTPRequestHandler):
    """A downstream outage/partial-ACK server counting submitted VALUES rows per request."""
    def log_message(self, *args):
        pass

    def do_POST(self):
        length = int(self.headers["Content-Length"])
        payload = self.rfile.read(length)
        mode = self.server.mode
        if mode == "outage":
            body = b"sensitive downstream error body"
            self.send_response(503)
        else:
            try:
                params = urllib.parse.parse_qs(payload.decode("ascii"))
                submitted = params["sql"][0].count("),(") + 1
            except (ValueError, KeyError, IndexError):
                submitted = -1
            if submitted < 0:
                body = b"sensitive downstream error body"
                self.send_response(503)
            else:
                reported = 0 if mode == "partial" else submitted
                body = json.dumps({"code": 0, "output": [{"affectedrows": reported}]}).encode()
                self.send_response(200)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


def wait_for(predicate):
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.02)
    raise AssertionError("worker did not reach expected durable state")


class ReceiverBehavior(unittest.TestCase):
    def test_wire_record_and_byte_boundaries(self):
        meta = {"schema_version": 1, "vehicle": "synthetic", "collector_id": "fixture",
                "session_id": "boundary", "started_ns": 1800000000000000000,
                "vehicle_firmware": "synthetic"}
        chunks = [{"seq": seq, "offset_ns": seq, "phase": "capture", "data": b"x"}
                  for seq in range(10000)]
        payload = encode_batch(meta, chunks)
        self.assertEqual(decode_batch(payload), (meta, chunks))
        with self.assertRaisesRegex(WireError, "record count"):
            encode_batch(meta, chunks + [dict(chunks[-1], seq=10000, offset_ns=10000)])
        from opentelemetry.proto.collector.logs.v1.logs_service_pb2 import ExportLogsServiceRequest
        request = ExportLogsServiceRequest.FromString(payload)
        records = request.resource_logs[0].scope_logs[0].log_records
        records.add().CopyFrom(records[-1])
        with self.assertRaisesRegex(WireError, "instrumentation scope"):
            decode_batch(request.SerializeToString())
        with self.assertRaisesRegex(WireError, "byte limit"):
            encode_batch(meta, [dict(chunks[seq], data=b"x" * 65536) for seq in range(32)])
        with self.assertRaisesRegex(WireError, "request size"):
            decode_batch(b"x" * (2 * 1024 * 1024 + 1))

    def test_archive_rejects_workspace_before_creating_database(self):
        workspace = Path(__file__).resolve().parents[1]
        with tempfile.TemporaryDirectory(dir=workspace) as inside, tempfile.TemporaryDirectory() as outside:
            private = Path(inside)
            alias = Path(outside) / "workspace"
            alias.symlink_to(private, target_is_directory=True)
            paths = (workspace / "raw.sqlite",
                     private / "raw.sqlite",
                     private / "missing" / ".." / "raw.sqlite",
                     alias / "raw.sqlite")
            for path in paths:
                for open_archive in (Archive, archive_status):
                    with self.assertRaisesRegex(ConfigurationError, "^archive_must_be_outside_workspace$"):
                        open_archive(path)
            self.assertEqual(list(private.iterdir()), [])
            archive = Archive(Path(outside) / "raw.sqlite", disk_reserve_bytes=0)
            self.assertEqual(archive.status()["raw_chunks"], 0)

    def test_decode_resumes_in_arrival_order_without_rescanning_history(self):
        meta = {"schema_version": 1, "vehicle": "synthetic", "collector_id": "fixture",
                "session_id": "old-session", "started_ns": 1800000000000000000,
                "vehicle_firmware": "synthetic"}
        with tempfile.TemporaryDirectory() as directory:
            decoder = synthetic_decoder(directory)
            archive = Archive(Path(directory) / "raw.sqlite", disk_reserve_bytes=0)
            archive.register_epoch(decoder)
            archive.accept(meta, [
                {"seq": seq, "offset_ns": seq, "phase": "capture", "data": b"\r"}
                for seq in range(6000)
            ])
            archive.decode_once(decoder, limit=1)
            # A completed, retained prefix with no decoded signals or partial frame.
            with archive.connect() as conn:
                state = json.loads(conn.execute("SELECT state_json FROM decode_states").fetchone()[0])
                state.update(next_seq=6000, last_offset_ns=5999)
                conn.execute("UPDATE decode_states SET next_seq=6000,state_json=?", (json.dumps(state),))
                conn.commit()
            archive.accept(dict(meta, session_id="new-session"), [
                {"seq": 0, "offset_ns": 7, "phase": "capture", "data": b"t12320200\r"}
            ])
            archive.accept(meta, [
                {"seq": 6000, "offset_ns": 6000, "phase": "capture", "data": b"t12320400\r"}
            ])
            connect = archive.connect

            @contextlib.contextmanager
            def bounded_connect():
                with connect() as conn:
                    steps = 0

                    def budget():
                        nonlocal steps
                        steps += 1000
                        return steps > 20000

                    # Bound SQLite work, not wall time or a particular query plan.
                    conn.set_progress_handler(budget, 1000)
                    yield conn

            with patch.object(archive, "connect", bounded_connect):
                self.assertTrue(archive.decode_once(decoder, limit=2))
                self.assertFalse(archive.decode_once(decoder))
            with archive.connect() as conn:
                rows = [json.loads(row[0]) for row in conn.execute("SELECT row_json FROM outbox ORDER BY id")]
            self.assertEqual([(row["event_time"], row["value_num"]) for row in rows],
                             [(meta["started_ns"] + 7, 1.0), (meta["started_ns"] + 6000, 2.0)])
            replay = synthetic_decoder(directory, revision="synthetic-v2")
            archive.register_epoch(replay, explicit=True)
            with patch.object(archive, "connect", bounded_connect):
                self.assertTrue(archive.decode_once(replay, limit=1))
            with archive.connect() as conn:
                cursor = conn.execute("SELECT next_seq FROM decode_states WHERE epoch=?", (replay.epoch,)).fetchone()[0]
                self.assertEqual(cursor, 1)
                self.assertEqual(conn.execute("SELECT COUNT(*) FROM raw_chunks").fetchone()[0], 6002)

    def test_batch_failure_rolls_back_cursors_and_outbox_before_restart(self):
        meta = {"schema_version": 1, "vehicle": "synthetic", "collector_id": "fixture",
                "session_id": "batch-session", "started_ns": 1800000000000000000,
                "vehicle_firmware": "synthetic"}
        chunks = [{"seq": seq, "offset_ns": seq, "phase": "capture", "data": data}
                  for seq, data in enumerate([b"t12320200\r", b"t123204", b"00\r"])]
        with tempfile.TemporaryDirectory() as directory:
            decoder = synthetic_decoder(directory)
            path = Path(directory) / "raw.sqlite"
            archive = Archive(path, disk_reserve_bytes=0)
            archive.register_epoch(decoder)
            archive.accept(meta, chunks)
            before = archive.status()
            decode_some = decoder.decode_some

            def fail_second(meta, chunk, state, budget):
                if chunk["seq"] == 1:
                    raise ValueError("injected decoder failure")
                return decode_some(meta, chunk, state, budget)

            with patch.object(decoder, "decode_some", fail_second), patch(
                    "scripts.ingest.can.can_receiver.time.monotonic", return_value=0):
                with self.assertRaisesRegex(ValueError, "injected decoder failure"):
                    archive.decode_once(decoder)
            after = archive.status()
            for key in ("sessions", "raw_chunks", "raw_bytes", "pending_rows", "epochs",
                        "errors", "oldest_pending_id", "backlog_chunks", "freshness", "counters"):
                self.assertEqual(after[key], before[key], key)
            archive = Archive(path, disk_reserve_bytes=0)
            with patch("scripts.ingest.can.can_receiver.time.monotonic", return_value=0):
                self.assertEqual(archive.decode_once(decoder, limit=2), 2)
            self.assertEqual(archive.status()["epochs"][0]["remaining_chunks"], 1)
            archive = Archive(path, disk_reserve_bytes=0)
            self.assertEqual(archive.decode_once(decoder), 1)
            with archive.connect() as conn:
                rows = [json.loads(r[0]) for r in conn.execute("SELECT row_json FROM outbox ORDER BY id")]
            self.assertEqual([(r["event_time"], r["value_num"]) for r in rows],
                             [(meta["started_ns"], 1.0), (meta["started_ns"] + 2, 2.0)])
            self.assertEqual(archive.status()["raw_chunks"], 3)
            self.assertEqual(archive.status()["epochs"][0]["remaining_chunks"], 0)

    def test_decode_continues_during_blocked_upload_and_shutdown_joins_uploader(self):
        meta = {"schema_version": 1, "vehicle": "synthetic", "collector_id": "fixture",
                "session_id": "blocked-upload", "started_ns": 1800000000000000000,
                "vehicle_firmware": "synthetic"}
        chunks = [{"seq": seq, "offset_ns": seq, "phase": "capture", "data": b"t12320200\r"}
                  for seq in range(500)]
        entered, release = threading.Event(), threading.Event()

        class HeldSink(SQLSink):
            def do_POST(self):
                entered.set()
                release.wait(10)
                super().do_POST()

        with tempfile.TemporaryDirectory() as directory:
            decoder = synthetic_decoder(directory)
            archive = Archive(Path(directory) / "raw.sqlite", disk_reserve_bytes=0)
            archive.register_epoch(decoder)
            archive.accept(meta, chunks[:1])
            archive.decode_once(decoder)
            sink = HTTPServer(("127.0.0.1", 0), HeldSink)
            sink.mode = "outage"
            sink_thread = threading.Thread(target=sink.serve_forever, daemon=True)
            sink_thread.start()
            greptime = Greptime(f"http://127.0.0.1:{sink.server_port}", "synthetic", "test", "test", timeout=10)
            worker = Worker(archive, decoder, greptime, interval=0.01)
            worker.start()
            try:
                wait_for(entered.is_set)
                archive.accept(meta, chunks[1:])
                wait_for(lambda: archive.status()["epochs"][0]["remaining_chunks"] == 0)
                self.assertEqual(archive.status()["pending_rows"], 500)
                worker.stop_event.set()
                worker.join(0.05)
                self.assertTrue(worker.is_alive())  # Exclusive archive ownership must outlive the pending upload.
                release.set()
                worker.join(5)
                self.assertFalse(worker.is_alive())
                self.assertEqual(archive.status()["pending_rows"], 500)
                self.assertEqual(archive.status()["raw_chunks"], 500)
            finally:
                release.set()
                worker.stop_event.set()
                worker.join(10)
                sink.shutdown()
                sink.server_close()
                sink_thread.join(3)

    def test_durable_auth_dedup_split_restart_and_downstream_retry(self):
        meta = {"schema_version": 1, "vehicle": "synthetic", "collector_id": "fixture",
                "session_id": "fixture-session", "started_ns": 1800000000000000000,
                "vehicle_firmware": "synthetic 雪 'quoted' \\ label"}
        frame = b"t1232FFFF\r"  # Invented signed 16-bit Power = -0.5 kW.
        chunks = [{"seq": 0, "offset_ns": 11, "phase": "capture", "data": frame[:9]},
                  {"seq": 1, "offset_ns": 23, "phase": "capture", "data": frame[9:]},
                  {"seq": 2, "offset_ns": 25, "phase": "close_drain", "data": b"not-a-frame\r"}]
        auth = "Basic " + base64.b64encode(b"fixture-user:fixture-password").decode()
        with tempfile.TemporaryDirectory() as directory:
            decoder = synthetic_decoder(directory)
            self.assertEqual(decode_batch(encode_batch(meta, chunks)), (meta, chunks))
            database = Path(directory) / "raw.sqlite"
            archive = Archive(database, disk_reserve_bytes=0)
            archive.register_epoch(decoder)
            server = Receiver(("127.0.0.1", 0), archive, "synthetic", "fixture", "fixture-user", "fixture-password", timeout=1)
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            endpoint = f"http://127.0.0.1:{server.server_port}"

            def post(batch=None, metadata=meta, credentials=auth, compressed=None):
                if compressed is None:
                    compressed = gzip.compress(encode_batch(metadata, batch))
                request = urllib.request.Request(endpoint + "/v1/logs", data=compressed, headers={
                    "Authorization": credentials, "Content-Type": "application/x-protobuf", "Content-Encoding": "gzip"})
                try:
                    with urllib.request.urlopen(request, timeout=2) as response:
                        return response.status, response.read()
                except urllib.error.HTTPError as exc:
                    return exc.code, exc.read()

            worker = None
            sink = None
            sink_thread = None
            try:
                code, body = post([chunks[0]])
                self.assertEqual(code, 200)
                check_response(body)
                self.assertEqual(post([chunks[0]])[0], 200)  # Lost ACK retry.
                self.assertEqual(archive.status()["raw_chunks"], 1)
                before = archive.status()
                changed = dict(chunks[0], data=b"different")
                cases = [
                    (post([chunks[0]], credentials="Basic Zm9vOmJhcg=="), 401),
                    (post([chunks[0]], metadata=dict(meta, vehicle="other")), 403),
                    (post([chunks[0]], metadata=dict(meta, collector_id="other")), 403),
                    (post([chunks[0]], metadata=dict(meta, vehicle_firmware="changed")), 409),
                    (post([changed, chunks[1]]), 409),
                    (post([chunks[2]], metadata=dict(meta, session_id="gap-session")), 409),
                    (post(compressed=b"not gzip"), 400),
                    (post(compressed=gzip.compress(b"x" * (MAX_REQUEST_BYTES + 1))), 413),
                ]
                for response, expected in cases:
                    self.assertEqual(response[0], expected)
                after = archive.status()
                for key in ("sessions", "raw_chunks", "raw_bytes", "pending_rows", "epochs",
                            "errors", "oldest_pending_id", "backlog_chunks", "freshness", "counters"):
                    self.assertEqual(after[key], before[key])  # No partial durable state.
                self.assertTrue(archive.decode_once(decoder))
                self.assertEqual(archive.status()["pending_rows"], 0)
                with archive.connect() as conn:
                    state = json.loads(conn.execute("SELECT state_json FROM decode_states").fetchone()[0])
                    self.assertEqual(bytes.fromhex(state["tail_hex"]), chunks[0]["data"])
                server.shutdown()
                server.server_close()
                thread.join()

                # Restart archive and real receiver, preserving partial parser bytes and checkpoint.
                archive = Archive(database, disk_reserve_bytes=0)
                decoder = synthetic_decoder(directory)
                archive.register_epoch(decoder)
                server = Receiver(("127.0.0.1", 0), archive, "synthetic", "fixture", "fixture-user", "fixture-password", timeout=1)
                thread = threading.Thread(target=server.serve_forever, daemon=True)
                thread.start()
                endpoint = f"http://127.0.0.1:{server.server_port}"
                self.assertEqual(post(chunks[1:])[0], 200)
                self.assertEqual(post(chunks)[0], 200)
                self.assertTrue(archive.decode_once(decoder))
                self.assertFalse(archive.decode_once(decoder))
                with archive.connect() as conn:
                    row = json.loads(conn.execute("SELECT row_json FROM outbox").fetchone()[0])
                first_rows, first_state, _, first_done = decoder.decode_some(meta, chunks[0], None, 2000)
                self.assertTrue(first_done)
                self.assertEqual(first_rows, [])
                expected_rows, _, _, second_done = decoder.decode_some(meta, chunks[1], first_state, 2000)
                self.assertEqual(row["event_time"], meta["started_ns"] + 23)
                self.assertEqual(row["value_num"], -0.5)
                self.assertEqual(row["value_text"], None)
                self.assertEqual(row["quality"], "reported_unverified")
                self.assertEqual(row["path"], "Vehicle.CAN.x123.Power")
                self.assertEqual(row["vehicle_firmware"], meta["vehicle_firmware"])
                self.assertEqual(archive.status()["raw_chunks"], 3)
                self.assertEqual(archive.status()["pending_rows"], 1)

                sink = HTTPServer(("127.0.0.1", 0), SQLSink)
                sink.mode = "outage"
                sink_thread = threading.Thread(target=sink.serve_forever, daemon=True)
                sink_thread.start()
                greptime = Greptime(f"http://127.0.0.1:{sink.server_port}", "synthetic", "test", "test", timeout=1)
                worker = Worker(archive, decoder, greptime, interval=0.02)
                worker.start()
                wait_for(lambda: "greptime_failure" in archive.status()["errors"])
                self.assertEqual(archive.status()["pending_rows"], 1)
                sink.mode = "partial"
                wait_for(lambda: "greptime_partial_ack" in archive.status()["errors"])
                self.assertEqual(archive.status()["pending_rows"], 1)
                sink.mode = "success"
                wait_for(lambda: archive.status()["pending_rows"] == 0)
                self.assertNotIn("sensitive", json.dumps(archive.status()))
                self.assertNotIn("tail_hex", json.dumps(archive.status()))
                self.assertEqual(archive.status()["epochs"][0]["processed_chunks"], 3)
                worker.stop_event.set()
                worker.join(3)
                replay = synthetic_decoder(directory, revision="synthetic-v2")
                self.assertNotEqual(replay.epoch, decoder.epoch)
                with self.assertRaisesRegex(ConfigurationError, "^new_decoder_epoch_requires_re-decode$"):
                    archive.register_epoch(replay)
                archive.register_epoch(replay, explicit=True)
                while archive.decode_once(replay):
                    pass
                with archive.connect() as conn:
                    replay_row = json.loads(conn.execute("SELECT row_json FROM outbox").fetchone()[0])
                    stored = conn.execute("SELECT data FROM raw_chunks ORDER BY seq").fetchall()
                self.assertEqual([record[0] for record in stored], [chunk["data"] for chunk in chunks])
                self.assertNotEqual(replay_row["event_id"], row["event_id"])
                self.assertEqual(replay_row["event_time"], row["event_time"])
                self.assertEqual(replay_row["quality"], row["quality"])
            finally:
                if worker:
                    worker.stop_event.set()
                    worker.join(3)
                server.shutdown()
                server.server_close()
                thread.join(3)
                if sink:
                    sink.shutdown()
                    sink.server_close()
                    sink_thread.join(3)
    def test_oversized_head_retained_and_fixed_error_categories(self):
        base = {c: None for c in COLUMNS}
        base.update({"event_time": 1800000000000000000, "vehicle": "synthetic", "path": "Vehicle.CAN.x123.Power",
                     "source": "can", "decode_epoch": "synthetic-v1", "ingest_time": 1800000000000000001,
                     "source_system": None, "source_field": None, "collector_id": "fixture",
                     "source_is_resend": None, "quality": "reported_unverified", "envelope_id": None,
                     "config_version": None, "connectivity": None, "value_num": None, "value_bool": None,
                     "unit": "kW", "vss_version": None, "vehicle_firmware": "synthetic",
                     "dbc_primary_commit": None, "dbc_supplemental_commit": None, "dbc_override_version": None,
                     "dbc_override_commit": None, "mapping_revision": "synthetic-v1", "collector_version": None})

        def row(event_id, text):
            return dict(base, event_id=event_id, value_text=text)

        with tempfile.TemporaryDirectory() as directory:
            decoder = synthetic_decoder(directory)
            archive = Archive(Path(directory) / "raw.sqlite", disk_reserve_bytes=0)
            archive.register_epoch(decoder)
            big = row("oversized-head", "x" * 4000)
            with archive.connect() as conn:
                conn.execute("INSERT INTO outbox(event_id,epoch,row_json) VALUES(?,?,?)",
                             (big["event_id"], decoder.epoch, json.dumps(big)))
                conn.commit()
            small_budget = len(encode_body(render_insert([big]))) - 1
            stuck = Greptime("http://127.0.0.1:9", "synthetic", "test", "test", max_body_bytes=small_budget)
            with self.assertRaisesRegex(DownstreamError, "^greptime_row_too_large$"):
                archive.flush_once(stuck)
            # Durable: head row is kept, never skipped or deleted, and surfaces a fixed code.
            self.assertEqual(archive.status()["pending_rows"], 1)
            with archive.connect() as conn:
                stored = json.loads(conn.execute("SELECT row_json FROM outbox").fetchone()[0])
            self.assertEqual(stored["event_id"], "oversized-head")
            with self.assertRaisesRegex(ValueError, "invalid error category"):
                archive.record_error("adaptive_suppressed")
            archive.record_error("greptime_row_too_large")
            archive.record_error("greptime_timeout")

    def test_prefix_flush_keeps_remainder_and_timeout_retains_batch(self):
        import socket
        import urllib.parse
        base = {c: None for c in COLUMNS}
        base.update({"event_time": 1800000000000000000, "vehicle": "synthetic", "path": "p",
                     "source": "can", "decode_epoch": "synthetic-v1", "ingest_time": 1800000000000000001,
                     "collector_id": "fixture", "quality": "reported_unverified", "mapping_revision": "synthetic-v1",
                     "vehicle_firmware": "synthetic"})
        rows = [dict(base, event_id=f"e{i}", value_text=f"text-{i}") for i in range(3)]
        two = len(encode_body(render_insert(rows[:2])))
        three = len(encode_body(render_insert(rows)))
        self.assertLess(two, three)
        budget = two  # Exact cap: first two rows fit, all three do not.

        seen = {}

        class CountingSink(SQLSink):
            def do_POST(self):
                length = int(self.headers["Content-Length"])
                seen["bytes"] = self.rfile.read(length)
                params = urllib.parse.parse_qs(seen["bytes"].decode("ascii"))
                count = params["sql"][0].count("),(") + 1
                body = json.dumps({"code": 0, "output": [{"affectedrows": count}]}).encode()
                self.send_response(200)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

        with tempfile.TemporaryDirectory() as directory:
            decoder = synthetic_decoder(directory)
            archive = Archive(Path(directory) / "raw.sqlite", disk_reserve_bytes=0)
            archive.register_epoch(decoder)
            with archive.connect() as conn:
                conn.executemany("INSERT INTO outbox(event_id,epoch,row_json) VALUES(?,?,?)",
                                 [(r["event_id"], decoder.epoch, json.dumps(r)) for r in rows])
                conn.commit()
            sink = HTTPServer(("127.0.0.1", 0), CountingSink)
            thread = threading.Thread(target=sink.serve_forever, daemon=True)
            thread.start()
            try:
                greptime = Greptime(f"http://127.0.0.1:{sink.server_port}", "synthetic", "test", "test",
                                    timeout=60, max_body_bytes=budget)
                self.assertEqual(archive.flush_once(greptime), 2)
                self.assertEqual(archive.status()["pending_rows"], 1)
                with archive.connect() as conn:
                    leftover = json.loads(conn.execute("SELECT row_json FROM outbox").fetchone()[0])
                self.assertEqual(leftover["event_id"], "e2")  # Ordered prefix; remainder IDs untouched.
                # Partial ACK keeps the sent prefix durable.
                sink.mode = "partial"
                with patch.object(CountingSink, "do_POST", SQLSink.do_POST):
                    partial = Greptime(f"http://127.0.0.1:{sink.server_port}", "synthetic", "test", "test",
                                       timeout=60, max_body_bytes=budget)
                    sink.mode = "partial"
                    with self.assertRaisesRegex(DownstreamError, "^greptime_partial_ack$"):
                        archive.flush_once(partial)
                self.assertEqual(archive.status()["pending_rows"], 1)
                # Timeout/unknown outcome retains the batch without adaptation.
                held = Greptime("http://127.0.0.1:9", "synthetic", "test", "test", timeout=60,
                                max_body_bytes=DEFAULT_MAX_BODY_BYTES)
                with patch.object(Greptime, "_connect", side_effect=socket.timeout):
                    with self.assertRaisesRegex(DownstreamError, "^greptime_timeout$"):
                        archive.flush_once(held)
                self.assertEqual(archive.status()["pending_rows"], 1)
                with self.assertRaises(ValueError):
                    Greptime("http://127.0.0.1:9", "synthetic", "test", "test", timeout=float("inf"))
                with self.assertRaises(ValueError):
                    Greptime("http://127.0.0.1:9", "synthetic", "test", "test", timeout=0)
                with self.assertRaises(ValueError):
                    Greptime("http://127.0.0.1:9", "synthetic", "test", "test", max_body_bytes=0)
            finally:
                sink.shutdown()
                sink.server_close()
                thread.join(3)

    def test_boundary_matrix_shutdown_timeout_partial_resume_counters_slow(self):
        """Writes, holds, shutdown, timeout, partial ACK, oversized head, resume, migration, slow clients."""
        import http.client
        import socket
        import urllib.parse
        meta = {"schema_version": 1, "vehicle": "synthetic", "collector_id": "fixture",
                "session_id": "matrix", "started_ns": 1800000000000000000, "vehicle_firmware": "synthetic"}
        frame = b"t12320200\r"
        with tempfile.TemporaryDirectory() as directory:
            decoder = synthetic_decoder(directory)
            path = Path(directory) / "raw.sqlite"
            archive = Archive(path, disk_reserve_bytes=0)
            archive.register_epoch(decoder)
            # Writes: exact-ordered prefix, remainder untouched, counters durable.
            archive.accept(meta, [{"seq": seq, "offset_ns": seq, "phase": "capture", "data": frame}
                                  for seq in range(3)])
            self.assertEqual(archive.status()["raw_chunks"], 3)
            self.assertEqual(archive.status()["counters"]["decoded_rows_total"], 0)
            self.assertIsNone(archive.status()["freshness"]["last_decode_ns"])
            archive.decode_once(decoder)
            self.assertEqual(archive.status()["pending_rows"], 3)
            self.assertEqual(archive.status()["counters"]["decoded_rows_total"], 3)
            self.assertIsNotNone(archive.status()["freshness"]["last_decode_ns"])
            # Replay of an ACKed raw prefix is overlap-legal and changes nothing.
            archive.accept(meta, [{"seq": 0, "offset_ns": 0, "phase": "capture", "data": frame}])
            self.assertEqual(archive.status()["raw_chunks"], 3)
            # Partial ACK retains the whole fitting prefix; timeout retains the batch.
            sink = HTTPServer(("127.0.0.1", 0), SQLSink)
            sink.mode = "partial"
            thread = threading.Thread(target=sink.serve_forever, daemon=True)
            thread.start()
            try:
                partial = Greptime(f"http://127.0.0.1:{sink.server_port}", "synthetic", "test", "test", timeout=5)
                with self.assertRaisesRegex(DownstreamError, "^greptime_partial_ack$"):
                    archive.flush_once(partial)
                self.assertEqual(archive.status()["pending_rows"], 3)
                held = Greptime("http://127.0.0.1:9", "synthetic", "test", "test", timeout=5)
                with patch.object(Greptime, "_connect", side_effect=socket.timeout):
                    with self.assertRaisesRegex(DownstreamError, "^greptime_timeout$"):
                        archive.flush_once(held)
                self.assertEqual(archive.status()["pending_rows"], 3)
                # Slow downstream (held connection) still joins on graceful shutdown.
                entered, release = threading.Event(), threading.Event()

                class SlowSink(SQLSink):
                    def do_POST(self):
                        entered.set()
                        release.wait(10)
                        super().do_POST()
                slow = HTTPServer(("127.0.0.1", 0), SlowSink)
                slow.mode = "outage"
                slow_thread = threading.Thread(target=slow.serve_forever, daemon=True)
                slow_thread.start()
                try:
                    blocked = Greptime(f"http://127.0.0.1:{slow.server_port}", "synthetic", "test", "test", timeout=10)
                    worker = Worker(archive, decoder, blocked, interval=0.01)
                    worker.start()
                    self.assertTrue(entered.wait(5))
                    worker.stop_event.set()
                    worker.join(0.05)
                    self.assertTrue(worker.is_alive())
                    release.set()
                    worker.join(10)
                    self.assertFalse(worker.is_alive())
                    self.assertEqual(archive.status()["pending_rows"], 3)
                finally:
                    release.set()
                    slow.shutdown()
                    slow.server_close()
                    slow_thread.join(3)
                # Oversized head stays durable; dirty failure retains the whole batch.
                base = {c: None for c in COLUMNS}
                base.update({"event_time": 1800000000000000000, "vehicle": "synthetic", "path": "p",
                             "source": "can", "decode_epoch": decoder.epoch, "ingest_time": 1800000000000000001,
                             "collector_id": "fixture", "quality": "reported_unverified",
                             "mapping_revision": decoder.mapping_revision, "vehicle_firmware": "synthetic"})
                with archive.connect() as conn:
                    conn.execute("INSERT INTO outbox(event_id,epoch,row_json) VALUES(?,?,?)",
                                 ("zz-oversized", decoder.epoch, json.dumps(dict(base, event_id="zz-oversized",
                                            value_text="x" * 5000))))
                    conn.commit()
                tiny = Greptime("http://127.0.0.1:9", "synthetic", "test", "test", max_body_bytes=64)
                with self.assertRaises(DownstreamError):
                    archive.flush_once(tiny)
                with archive.connect() as conn:
                    conn.execute("DELETE FROM outbox WHERE event_id='zz-oversized'")
                    conn.commit()
                sink.mode = "success"
                # Dirty failure retains the whole prefix: hold the dirty ACK by
                # failing only the dirty-table POST at the HTTP layer.
                real_post = SQLSink.do_POST

                def fail_dirty_table(self):
                    length = int(self.headers["Content-Length"])
                    payload = self.rfile.read(length)
                    if b"vehicle_signal_dirty" in payload:
                        body = b"sensitive downstream error body"
                        self.send_response(503)
                        self.send_header("Content-Length", str(len(body)))
                        self.end_headers()
                        self.wfile.write(body)
                        return
                    params = urllib.parse.parse_qs(payload.decode("ascii"))
                    reported = params["sql"][0].count("),(") + 1
                    body = json.dumps({"code": 0, "output": [{"affectedrows": reported}]}).encode()
                    self.send_response(200)
                    self.send_header("Content-Length", str(len(body)))
                    self.end_headers()
                    self.wfile.write(body)
                with patch.object(SQLSink, "do_POST", fail_dirty_table):
                    with self.assertRaisesRegex(DownstreamError, "^dirty_notify_failure$"):
                        archive.flush_once(partial)
                # Slow clients: trickled headers/body stay within the whole-request deadline.
                server = Receiver(("127.0.0.1", 0), archive, "synthetic", "fixture",
                                  "fixture-user", "fixture-password", timeout=1)
                slow_thread = threading.Thread(target=server.serve_forever, daemon=True)
                slow_thread.start()
                try:
                    # Keep-alive: two sequential 200s reuse one socket (end-to-end with Go MaxConns=1).
                    import http.client
                    keep = http.client.HTTPConnection("127.0.0.1", server.server_port, timeout=5)
                    try:
                        auth = "Basic " + base64.b64encode(b"fixture-user:fixture-password").decode()
                        connection = None
                        for _ in range(2):
                            keep.request("GET", "/status", headers={"Authorization": auth})
                            response = keep.getresponse()
                            self.assertEqual(response.status, 200)
                            if connection is None:
                                connection = keep.sock
                            else:
                                self.assertIs(keep.sock, connection)
                            response.read()
                        keep.request("GET", "/missing", headers={"Authorization": auth})
                        missing = keep.getresponse()
                        self.assertEqual(missing.status, 404)
                        self.assertEqual(missing.getheader("Connection", ""), "close")
                        missing.read()
                    finally:
                        keep.close()
                    sock = socket.create_connection(("127.0.0.1", server.server_port), timeout=5)
                    try:
                        sock.sendall(b"POST /v1/logs HTTP/1.1\r\nHost: x\r\n")
                        time.sleep(1.5)
                        sock.settimeout(3)
                        try:
                            self.assertEqual(sock.recv(64), b"")
                        except ConnectionResetError:
                            pass  # Reset and EOF both release the slow client's slot.
                        healthy = urllib.request.Request(
                            f"http://127.0.0.1:{server.server_port}/status",
                            headers={"Authorization": auth})
                        with urllib.request.urlopen(healthy, timeout=3) as response:
                            self.assertEqual(response.status, 200)
                            self.assertEqual(json.load(response)["raw_chunks"], 3)
                    finally:
                        sock.close()
                finally:
                    server.shutdown()
                    server.server_close()
                    slow_thread.join(3)
                # Resume: partial chunk state survives a crash; row IDs/counts continue exactly.
                archive.accept(meta, [{"seq": 3, "offset_ns": 3, "phase": "capture", "data": frame * 40}])
                real = decoder.decode_some
                calls = {"n": 0}

                def fail_after_first(m, c, s, budget):
                    calls["n"] += 1
                    rows, state, counts, done = real(m, c, s, 1)
                    if calls["n"] == 1 and not done:
                        raise RuntimeError("injected crash before commit")
                    return rows, state, counts, done
                with patch.object(decoder, "decode_some", fail_after_first):
                    with self.assertRaises(RuntimeError):
                        archive.decode_once(decoder)
                while archive.decode_once(decoder):
                    pass
                with archive.connect() as conn:
                    ids = [r[0] for r in conn.execute("SELECT row_json FROM outbox ORDER BY id")]
                self.assertEqual(len({json.loads(r)["event_id"] for r in ids}), len(ids))
                # Dense chunk followed by another chunk: every row exact, none
                # skipped after restart, across the partial boundary.
                dense_meta = dict(meta, session_id="dense-session")
                archive.accept(dense_meta, [{"seq": 0, "offset_ns": 0, "phase": "capture", "data": frame * 600},
                                            {"seq": 1, "offset_ns": 1, "phase": "capture", "data": frame}])
                while archive.decode_once(decoder):
                    pass
                with archive.connect() as conn:
                    dense_ids = [json.loads(r[0])["event_id"] for r in
                                 conn.execute("SELECT row_json FROM outbox ORDER BY id").fetchall()[-601:]]
                self.assertEqual(len(dense_ids), 601)
                self.assertEqual(len(set(dense_ids)), 601)
                outlet = Archive(path, disk_reserve_bytes=0)
                self.assertFalse(outlet.decode_once(decoder))
                with outlet.connect() as conn:
                    again = [json.loads(r[0])["event_id"] for r in
                             conn.execute("SELECT row_json FROM outbox ORDER BY id").fetchall()[-601:]]
                self.assertEqual(again, dense_ids)
            finally:
                sink.shutdown()
                sink.server_close()
                thread.join(3)
            # Counter migration: a legacy DB without archive_meta gains counters without rescanning per call.
            with archive.connect() as conn:
                conn.execute("DELETE FROM archive_meta")
                conn.commit()
            reopened = Archive(path, disk_reserve_bytes=0)
            status = reopened.status()
            with archive.connect() as conn:
                expected_raw = conn.execute("SELECT COUNT(*) FROM raw_chunks").fetchone()[0]
            self.assertEqual(status["raw_chunks"], expected_raw)
            self.assertIn("decoded_rows_total", status["counters"])
            # Dirty buckets: hour floor from original event_time; generation hashes exact
            # canonical row bytes (identity+content), stable across retries/order.
            first = _ns_timestamp(1800000000000000000)
            rows = [(json.dumps(dict(base, event_id=f"e{i}", event_time=1800000000000000000 + i), sort_keys=True),
                     dict(base, event_id=f"e{i}", event_time=1800000000000000000 + i)) for i in range(2)]
            first_inserts = _dirty_inserts(rows, first)
            self.assertEqual(len(first_inserts), 1)
            self.assertEqual(first_inserts, _dirty_inserts(list(reversed(rows)), first))
            changed = dict(rows[0][1], value_text="changed")
            corrected = [(json.dumps(changed, sort_keys=True), changed)] + rows[1:]
            self.assertNotEqual(first_inserts, _dirty_inserts(corrected, first))
            # Disk budget: staged amplification reserves row bytes + per-chunk page/WAL
            # allowance before commit; low space commits nothing and raw is retained.
            import shutil as _shutil
            with reopened.connect() as conn:
                next_seq, last_offset = conn.execute(
                    "SELECT next_seq,last_offset_ns FROM sessions").fetchone()
            reopened.accept(meta, [{"seq": next_seq, "offset_ns": last_offset + 1,
                                    "phase": "capture", "data": frame}])
            low = _shutil.disk_usage(path.parent)
            scarce = type(low)(total=low.total, used=low.total - 1, free=1)
            pending_before = reopened.status()["pending_rows"]
            with patch("scripts.ingest.can.can_receiver.shutil.disk_usage", return_value=scarce):
                with self.assertRaises(Rejection) as rejected:
                    reopened.decode_once(decoder)
            self.assertEqual(rejected.exception.status, 503)
            self.assertEqual(reopened.status()["pending_rows"], pending_before)
            reopened.record_error("archive_disk_reserve")
            self.assertIsNotNone(reopened.status()["errors"].get("archive_disk_reserve"))
            live = Greptime("http://127.0.0.1:9", "synthetic", "t", "t")
            self.assertIsNone(live._connection)
            live.close()

    def test_pending_index_reawakens_epochs_resume_and_rebuild(self):
        """Pending membership drives selection; completion/receive/epoch/crash/rebuild stay consistent."""
        meta = {"schema_version": 1, "vehicle": "synthetic", "collector_id": "fixture",
                "session_id": "pending-a", "started_ns": 1800000000000000000,
                "vehicle_firmware": "synthetic"}
        frame = b"t12320200\r"
        with tempfile.TemporaryDirectory() as directory:
            decoder = synthetic_decoder(directory)
            path = Path(directory) / "raw.sqlite"
            archive = Archive(path, disk_reserve_bytes=0)
            archive.register_epoch(decoder)
            archive.accept(meta, [{"seq": seq, "offset_ns": seq, "phase": "capture", "data": frame}
                                  for seq in range(2)])
            archive.accept(dict(meta, session_id="pending-b"), [
                {"seq": 0, "offset_ns": 0, "phase": "capture", "data": frame}])
            with archive.connect() as conn:
                self.assertEqual(conn.execute("SELECT COUNT(*) FROM decode_pending WHERE epoch=?",
                                              (decoder.epoch,)).fetchone()[0], 2)
                # Pending-driven, not plan-text: hiding one member from the
                # shipped head query must divert the merge to the survivor
                # without touching any raw row.
                full = conn.execute(candidate_sql(), (decoder.epoch, decoder.epoch)).fetchall()
                self.assertEqual(len(full), 2)
                victim = conn.execute("SELECT session FROM decode_pending WHERE epoch=? LIMIT 1",
                                      (decoder.epoch,)).fetchone()[0]
                survivor = conn.execute("SELECT session FROM decode_pending WHERE epoch=? AND session!=?",
                                        (decoder.epoch, victim)).fetchone()[0]
                conn.execute("DELETE FROM decode_pending WHERE epoch=? AND session=?", (decoder.epoch, victim))
                picked = conn.execute(candidate_sql(), (decoder.epoch, decoder.epoch)).fetchall()
                self.assertEqual([row["session"] for row in picked], [survivor])
                conn.execute("INSERT OR IGNORE INTO decode_pending(session,epoch) VALUES(?,?)", (victim, decoder.epoch))
                # Operational boundedness: actual VM steps of the shipped
                # head lookup, not plan wording that shifts with aliases.
                steps = {"n": 0}

                def counter():
                    steps["n"] += 1
                    return False
                conn.set_progress_handler(counter, 1000)
                conn.execute(candidate_sql(), (decoder.epoch, decoder.epoch)).fetchall()
                conn.set_progress_handler(None, 0)
                self.assertLessEqual(steps["n"], 5)
            # All sessions complete, so the pending set drains to empty.
            while archive.decode_once(decoder):
                pass
            with archive.connect() as conn:
                self.assertEqual(conn.execute("SELECT COUNT(*) FROM decode_pending WHERE epoch=?",
                                              (decoder.epoch,)).fetchone()[0], 0)
                self.assertFalse(archive.decode_once(decoder))
                outbox_before = [r[0] for r in conn.execute("SELECT row_json FROM outbox ORDER BY id")]
                events_before = {(json.loads(r)["event_id"], json.loads(r)["decode_epoch"]) for r in outbox_before}
                state_before = {(r["session"], r["epoch"], r["next_seq"], r["state_json"], r["counts_json"], r["rows"])
                                for r in conn.execute("SELECT session,epoch,next_seq,state_json,counts_json,rows FROM decode_states")}
                self.assertEqual(conn.execute("SELECT COUNT(*) FROM decode_partial").fetchone()[0], 0)
            # Completed session reawakens on new raw; resend overlap does not duplicate rows.
            archive.accept(meta, [{"seq": 2, "offset_ns": 2, "phase": "capture", "data": frame}])
            with archive.connect() as conn:
                self.assertEqual(conn.execute("SELECT COUNT(*) FROM decode_pending WHERE epoch=?",
                                              (decoder.epoch,)).fetchone()[0], 1)
            self.assertEqual(archive.accept(meta, [{"seq": 2, "offset_ns": 2, "phase": "capture", "data": frame}]), 0)
            with self.assertRaises(Rejection):
                archive.accept(meta, [{"seq": 2, "offset_ns": 2, "phase": "capture", "data": b"t12320500\r"}])
            with archive.connect() as conn:
                self.assertEqual(conn.execute("SELECT COUNT(*) FROM decode_pending WHERE epoch=?",
                                              (decoder.epoch,)).fetchone()[0], 1)
            while archive.decode_once(decoder):
                pass
            with archive.connect() as conn:
                outbox_after = [r[0] for r in conn.execute("SELECT row_json FROM outbox ORDER BY id")]
                self.assertEqual(len(outbox_after), len(outbox_before) + 1)
                self.assertEqual(len({json.loads(r)["event_id"] for r in outbox_after}), len(outbox_after))
                self.assertTrue(events_before.issubset({(json.loads(r)["event_id"], json.loads(r)["decode_epoch"])
                                                        for r in outbox_after}))
                cursors_after = {r["session"]: r["next_seq"] for r in
                                 conn.execute("SELECT session,next_seq FROM decode_states WHERE epoch=?", (decoder.epoch,))}
                cursors_before = {s: n for (s, e, n, _, _, _) in state_before if e == decoder.epoch}
                self.assertEqual(set(cursors_after), set(cursors_before))
                advanced = [s for s in cursors_after if cursors_after[s] == cursors_before[s] + 1]
                held = [s for s in cursors_after if cursors_after[s] == cursors_before[s]]
                self.assertEqual(len(advanced), 1)
                self.assertEqual(len(held), len(cursors_after) - 1)
                self.assertEqual(conn.execute("SELECT COUNT(*) FROM decode_partial").fetchone()[0], 0)
            # New epoch replays all originals regardless of prior completion.
            replay = synthetic_decoder(directory, revision="synthetic-v2")
            archive.register_epoch(replay, explicit=True)
            with archive.connect() as conn:
                self.assertEqual(conn.execute("SELECT COUNT(*) FROM decode_pending WHERE epoch=?",
                                              (replay.epoch,)).fetchone()[0], 2)
                with self.assertRaises(ConfigurationError):
                    archive.register_epoch(synthetic_decoder(directory, revision="synthetic-v3"))
            while archive.decode_once(replay):
                pass
            with archive.connect() as conn:
                self.assertEqual(conn.execute("SELECT COUNT(*) FROM decode_pending WHERE epoch=?",
                                              (replay.epoch,)).fetchone()[0], 0)
                replay_rows = conn.execute("SELECT COUNT(*) FROM outbox WHERE epoch=?", (replay.epoch,)).fetchone()[0]
                self.assertEqual(replay_rows, 4)
                replay_ids = [json.loads(r[0])["event_id"] for r in
                              conn.execute("SELECT row_json FROM outbox WHERE epoch=? ORDER BY id", (replay.epoch,))]
                self.assertEqual(len(set(replay_ids)), 4)
            # Crash before commit leaves pending and data in agreement.
            baseline = archive.status()
            archive.accept(dict(meta, session_id="pending-c"), [
                {"seq": 0, "offset_ns": 0, "phase": "capture", "data": frame}])

            def crash(meta_arg, chunk, state, budget):
                raise RuntimeError("injected crash before commit")
            with patch.object(decoder, "decode_some", crash):
                with self.assertRaises(RuntimeError):
                    archive.decode_once(decoder)
            with archive.connect() as conn:
                self.assertEqual(conn.execute("SELECT COUNT(*) FROM decode_pending WHERE epoch=?",
                                              (decoder.epoch,)).fetchone()[0], 1)
                crashed = archive.status()
                self.assertEqual(crashed["sessions"], baseline["sessions"] + 1)
                self.assertEqual(crashed["raw_chunks"], baseline["raw_chunks"] + 1)
                for key in ("pending_rows", "errors", "oldest_pending_id", "counters"):
                    self.assertEqual(crashed[key], baseline[key], key)
            while archive.decode_once(decoder):
                pass
            while archive.decode_once(replay):
                pass
            # Outside-writer archive migrates and rebuilds to the exact pending set
            # (all sessions complete here, so the set is empty).
            with archive.connect() as conn:
                conn.execute("DELETE FROM decode_pending")
                conn.execute("DELETE FROM archive_meta WHERE key='pending_migrated'")
                conn.commit()
            reopened = Archive(path, disk_reserve_bytes=0)
            with reopened.connect() as conn:
                got = {(r[0], r[1]) for r in conn.execute("SELECT session,epoch FROM decode_pending")}
                want = set()
                for (epoch,) in conn.execute("SELECT epoch FROM epochs"):
                    for (sid,) in conn.execute(
                        "SELECT s.id FROM sessions s LEFT JOIN decode_states d ON d.session=s.id AND d.epoch=? "
                        "WHERE EXISTS(SELECT 1 FROM raw_chunks c WHERE c.session=s.id AND c.seq>=COALESCE(d.next_seq,0))",
                        (epoch,)):
                        want.add((sid, epoch))
                self.assertEqual(got, want)
                self.assertEqual(got, set())
            reopened.rebuild_pending()
            with reopened.connect() as conn:
                again = {(r[0], r[1]) for r in conn.execute("SELECT session,epoch FROM decode_pending")}
                self.assertEqual(again, want)

    def test_prefetch_merge_keeps_arrival_order_split_resume_and_conflict(self):
        """Heap merge stages global raw-id order, not per-session pages; partial resume chains in batch."""
        frame = b"t12320200\r"
        split = (b"t12320", b"200\r")
        base = {"schema_version": 1, "vehicle": "synthetic", "collector_id": "fixture",
                "started_ns": 1800000000000000000, "vehicle_firmware": "synthetic"}
        first, second = dict(base, session_id="merge-a"), dict(base, session_id="merge-b")
        with tempfile.TemporaryDirectory() as directory:
            decoder = synthetic_decoder(directory)
            path = Path(directory) / "raw.sqlite"
            archive = Archive(path, disk_reserve_bytes=0)
            archive.register_epoch(decoder)
            # Interleaved arrival: A0, B0, A1(split head), A2(split tail).
            archive.accept(first, [{"seq": 0, "offset_ns": 0, "phase": "capture", "data": frame}])
            archive.accept(second, [{"seq": 0, "offset_ns": 0, "phase": "capture", "data": frame}])
            archive.accept(first, [{"seq": 1, "offset_ns": 1, "phase": "capture", "data": split[0]}])
            archive.accept(first, [{"seq": 2, "offset_ns": 2, "phase": "capture", "data": split[1]}])
            self.assertEqual(archive.decode_once(decoder, limit=4), 4)
            with archive.connect() as conn:
                rows = [json.loads(row[0]) for row in conn.execute("SELECT row_json FROM outbox ORDER BY id")]
                cursors = {row["session"]: row["next_seq"] for row in
                           conn.execute("SELECT session,next_seq FROM decode_states WHERE epoch=?", (decoder.epoch,))}
                sessions = {row["session_id"]: row["id"] for row in
                            conn.execute("SELECT id,session_id FROM sessions")}
            # Sequential oracle: A0, B0, then the split A1/A2 chain (same parser state).
            state_a, state_b = None, None
            want_ids = []
            for meta, chunk, slot in ((first, {"seq": 0, "offset_ns": 0, "phase": "capture", "data": frame}, "a"),
                                      (second, {"seq": 0, "offset_ns": 0, "phase": "capture", "data": frame}, "b"),
                                      (first, {"seq": 1, "offset_ns": 1, "phase": "capture", "data": split[0]}, "a"),
                                      (first, {"seq": 2, "offset_ns": 2, "phase": "capture", "data": split[1]}, "a")):
                if slot == "a":
                    chunk_rows, state_a, _, done = decoder.decode_some(meta, chunk, state_a, 2000)
                else:
                    chunk_rows, state_b, _, done = decoder.decode_some(meta, chunk, state_b, 2000)
                self.assertTrue(done)
                want_ids.extend(row["event_id"] for row in chunk_rows)
            self.assertEqual([row["event_id"] for row in rows], want_ids)
            self.assertEqual(cursors, {sessions["merge-a"]: 3, sessions["merge-b"]: 1})
            self.assertEqual(archive.decode_once(decoder), 0)
            # Concurrent append during CPU work cannot slip into this batch:
            # the commit CAS sees the moved cursor and rolls everything back.
            archive.accept(dict(base, session_id="merge-d"), [
                {"seq": 0, "offset_ns": 0, "phase": "capture", "data": frame}])
            before = archive.status()
            real_decode = decoder.decode_some

            def append_during_decode(meta, chunk, state, budget):
                rows, state, counts, done = real_decode(meta, chunk, state, budget)
                archive.accept(dict(base, session_id="merge-d"), [
                    {"seq": 1, "offset_ns": 1, "phase": "capture", "data": frame}])
                with archive.connect() as conn:
                    conn.execute("INSERT INTO decode_states(session,epoch,next_seq,state_json,counts_json,rows) "
                                 "VALUES((SELECT id FROM sessions WHERE session_id='merge-d'),?,99,'{}','{}',0) "
                                 "ON CONFLICT(session,epoch) DO UPDATE SET next_seq=99", (decoder.epoch,))
                    conn.commit()
                return rows, state, counts, done
            with patch.object(decoder, "decode_some", append_during_decode):
                with self.assertRaisesRegex(ValueError, "decode cursor moved during decode"):
                    archive.decode_once(decoder, limit=1)
            after = archive.status()
            # Raw/outbox/counters keep atomicity: the concurrent raw append
            # persists (accept committed it), but the decode batch commits
            # nothing, so outbox rows and decoded counters are unchanged.
            self.assertEqual(after["raw_chunks"], before["raw_chunks"] + 1)
            for key in ("pending_rows", "counters"):
                self.assertEqual(after[key], before[key], key)
            with archive.connect() as conn:
                conn.execute("DELETE FROM decode_states WHERE session="
                             "(SELECT id FROM sessions WHERE session_id='merge-d') AND epoch=?",
                             (decoder.epoch,))
                conn.commit()
            while archive.decode_once(decoder):
                pass
            with archive.connect() as conn:
                tail_ids = [json.loads(row[0])["event_id"] for row in
                            conn.execute("SELECT row_json FROM outbox ORDER BY id")]
            self.assertEqual(len(tail_ids), len(set(tail_ids)))

    def test_prefetch_page_and_byte_boundaries_preserve_order_and_partial_completion(self):
        frame = b"t12320200\r"
        base = {"schema_version": 1, "vehicle": "synthetic", "collector_id": "fixture",
                "started_ns": 1800000000000000000, "vehicle_firmware": "synthetic"}
        for boundary in ("page", "bytes"):
            with self.subTest(boundary=boundary), tempfile.TemporaryDirectory() as directory:
                decoder = synthetic_decoder(directory)
                archive = Archive(Path(directory) / "raw.sqlite", disk_reserve_bytes=0)
                archive.register_epoch(decoder)
                metas = {name: dict(base, session_id=name) for name in ("a", "b", "dense")}
                data = frame if boundary == "page" else frame + b"\r" * (65536 - len(frame))
                a = [{"seq": i, "offset_ns": i, "phase": "capture", "data": data}
                     for i in range(80 if boundary == "page" else 20)]
                b = {"seq": 0, "offset_ns": 1000, "phase": "capture", "data": frame}
                arrival = ([("a", c) for c in a] + [("b", b)] if boundary == "page" else
                           [("a", a[0]), ("b", b)] + [("a", c) for c in a[1:]])
                dense = [{"seq": i, "offset_ns": i, "phase": "capture", "data": payload}
                         for i, payload in enumerate((frame * 3, frame * 6000, frame))]
                arrival.extend(("dense", c) for c in dense)
                with patch("scripts.ingest.can.can_receiver.time.time_ns", return_value=1800000000000000001):
                    for name, c in arrival:
                        archive.accept(metas[name], [c])
                    # Force row/byte boundaries, not machine-dependent timing.
                    with patch("scripts.ingest.can.can_receiver.time.monotonic", return_value=0):
                        while archive.decode_once(decoder):
                            pass
                    expected, states, totals = [], {}, {}
                    for name, c in arrival:
                        rows, state, counts = decoder.decode(metas[name], c, states.get(name))
                        expected.extend(rows)
                        states[name] = state
                        own = totals.setdefault(name, {})
                        for key, value in counts.items():
                            own[key] = value if key == "tail_bytes" else own.get(key, 0) + value
                with archive.connect() as conn:
                    actual = [json.loads(r[0]) for r in conn.execute("SELECT row_json FROM outbox ORDER BY id")]
                    self.assertEqual(actual, expected)
                    for row in conn.execute(
                            "SELECT s.session_id,d.state_json,d.counts_json,d.rows FROM decode_states d "
                            "JOIN sessions s ON s.id=d.session WHERE d.epoch=?", (decoder.epoch,)):
                        self.assertEqual(json.loads(row[1]), states[row[0]])
                        self.assertEqual(json.loads(row[2]), totals[row[0]])
                        self.assertEqual(row[3], totals[row[0]]["rows"])
                    self.assertEqual(conn.execute("SELECT COUNT(*) FROM decode_partial").fetchone()[0], 0)
                    self.assertEqual(conn.execute("SELECT COUNT(*) FROM decode_pending").fetchone()[0], 0)

if __name__ == "__main__":
    unittest.main()

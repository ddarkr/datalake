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
import urllib.request

from scripts.ingest.can.can_decoder import Decoder
from scripts.ingest.can.can_receiver import Archive, COLUMNS, ConfigurationError, DEFAULT_MAX_BODY_BYTES, DownstreamError, Greptime, Receiver, Worker, archive_status, encode_body, render_insert, select_prefix_count
from scripts.ingest.can.can_otlp_wire import MAX_REQUEST_BYTES, check_response, decode_batch, encode_batch


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
    """A downstream outage/partial-ACK server, not an echo of submitted rows."""
    def log_message(self, *args):
        pass

    def do_POST(self):
        self.rfile.read(int(self.headers["Content-Length"]))
        mode = self.server.mode
        body = (b"sensitive downstream error body" if mode == "outage" else
                json.dumps({"code": 0, "output": [{"affectedrows": 0 if mode == "partial" else 1}]}).encode())
        self.send_response(503 if mode == "outage" else 200)
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
            decode = decoder.decode

            def fail_second(meta, chunk, state):
                if chunk["seq"] == 1:
                    raise ValueError("injected decoder failure")
                return decode(meta, chunk, state)

            with patch.object(decoder, "decode", fail_second), patch(
                    "scripts.ingest.can.can_receiver.time.monotonic", return_value=0):
                with self.assertRaisesRegex(ValueError, "injected decoder failure"):
                    archive.decode_once(decoder)
            self.assertEqual(archive.status(), before)
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
                self.assertEqual(archive.status(), before)  # No failed request creates partial raw/session state.
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
                _, uninterrupted_state, _ = decoder.decode(meta, chunks[0], None)
                expected_rows, _, _ = decoder.decode(meta, chunks[1], uninterrupted_state)
                self.assertEqual(row["event_id"], expected_rows[0]["event_id"])
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
                self.assertEqual(archive.status()["raw_chunks"], 3)
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
    def test_exact_body_cap_multibyte_escaping_and_oversized_head(self):
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
        rows = [row("e1", "plain"), row("e2", "雪 'quoted' \\ +,&=+%20"), row("e3", "☃" * 400)]
        greptime = Greptime("http://127.0.0.1:9", "synthetic", "test", "test")
        full = len(greptime.body_for(rows))
        tiny = Greptime("http://127.0.0.1:9", "synthetic", "test", "test", max_body_bytes=full - 1)
        self.assertLess(tiny.fit(rows), 3)
        # Brute-force oracle: largest ordered prefix that fits, without touching rows/IDs.
        expected = 0
        for end in range(1, 4):
            if len(encode_body(render_insert(rows[:end]))) <= tiny.max_body_bytes:
                expected = end
        self.assertEqual(tiny.fit(rows), expected)

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
                self.assertLessEqual(len(seen["bytes"]), budget)
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
                with patch.object(held.opener, "open", side_effect=socket.timeout):
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


if __name__ == "__main__":
    unittest.main()
